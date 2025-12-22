from __future__ import annotations
from loguru import logger
from pathlib import Path
from typing import Union
from operator import ge
from operator import ne
from operator import eq
import pyarrow as pa
import orjson
import duckdb
import typer
import lzma

logger.add(Path("./dbssert.log"))

def init(conn: object) -> None:
  schemas: list[str] = [
    """\
CREATE TABLE IF NOT EXISTS SOURCES (
  SOURCE_ID INTEGER PRIMARY KEY,
  SOURCE_NAME VARCHAR,
  SOURCE_VERSION FLOAT,
  NLP_LEVEL INTEGER
);
    """,
    """\
CREATE TABLE IF NOT EXISTS CATEGORIES (
  CATEGORY_ID INTEGER PRIMARY KEY,
  CATEGORY_NAME VARCHAR
);
    """,
    """\
CREATE TABLE IF NOT EXISTS CURIES (
  CURIE_ID INTEGER PRIMARY KEY,
  CURIE VARCHAR,
  PREFERRED_NAME VARCHAR,
  CATEGORY_ID INTEGER REFERENCES(CATEGORIES),
  TAXON_ID INTEGER
);
    """,
    """\
CREATE TABLE IF NOT EXISTS SYNONYMS (
  CURIE_ID INTEGER REFERENCES(CURIES),
  SOURCE_ID INTEGER REFERENCES(SOURCES),
  SYNONYM VARCHAR
);
    """]
  for op in schemas:
    conn.execute(op)

def index(conn: object) -> None:
  indexes: list[str] = [
    "CREATE INDEX CURIE_SYNONYMS ON SYNONYMS (SYNONYM);",
    "CREATE INDEX CATEGORY_NAMES ON CATEGORIES (CATEGORY_NAME);",
    "CREATE INDEX CURIE_TAXON ON CURIES (TAXON);"
  ]
  for op in indexes:
    conn.execute(op)

def remove_problematic(x: str) -> bool:
  if "INCHIKEY" in x:
    return False
  elif eq(x, "uncharacterized protein") or eq(x, "hypothetical protein"):
    return False
  else:
    return True

def clean(x: str) -> str:
  cleaned: str = x.strip()
  if ne(cleaned, x):
    return clean(cleaned)
  elif eq(x[0], "\'") and eq(x[-1], "\'"):
    return clean(x[1:-1])
  elif eq(x[0], '"') and eq(x[-1], '"'):
    return clean(x[1:-1])
  else:
    return x

def bulk_insert(conn: object, batch: list[dict[str, dict[str, Union[str, list[str], int]]]], table: str) -> None:
  arrow: object = pa.Table.from_pylist(batch)
  # ! Somehow DuckDB picks up on the python variable...
  conn.execute(f"INSERT INTO {table} SELECT * FROM arrow")

def build(
  synonyms: list[Path],
  conn: object,
  table: dict[str, list[str]],
  regex: str = r"\W+",
  max_batch: int = 2_000_000
) -> None:
  categories: dict[str, int] = {}
  curie_batch: list[dict[str, dict[str, Union[str, int]]]] = []
  synonym_batch: list[dict[str, dict[str, Union[str, list[str], int]]]] = []
  idx: int = 0

  for p in synonyms:
    with lzma.open(p, "rb") as f:
      logger.warning(f"01 | STARTED ADDING {p}")

      for line in f:
        r: object = orjson.loads(line)

        curie: str = r["curie"]
        aliases: list[str] = r["names"]
        aliases.append(curie)

        if curie in table:
          aliases.extend(table[curie])

        preferred: str = r["preferred_name"]
        preferred = clean(preferred)

        category: str = r["types"][0]
        if category in categories:
          category_id: int = categories[category]
        else:
          category_id: int = len(categories)
          categories.update({category: category_id})

        taxon: int = r["taxa"][0][10:] if "taxa" in r else 0

        zero: list[str] = [clean(a).lower() for a in aliases]
        zero = list(filter(remove_problematic, zero))
        one: list[str] = [a.replace(regex, "") for a in aliases]

        curie_data: dict[str, dict[str, Union[str, int]]] = {
            "CURIE_ID": idx,
            "CURIE": curie,
            "PREFERRED_NAME": preferred,
            "CATEGORY_ID": category_id,
            "TAXON_ID": taxon
        }

        synonym_data: list[dict[str, dict[str, Union[str, list[str], int]]]] = [
          {
            "CURIE_ID": idx,
            "SOURCE_ID": 0,
            "SYNONYM": x
          }
          for x in zero
        ] + [
          {
            "CURIE_ID": idx,
            "SOURCE_ID": 1,
            "SYNONYM": x
          }
          for x in one
        ]

        curie_batch.append(curie_data)
        synonym_batch.extend(synonym_data)
        idx += 1

        if ge(len(synonym_batch), max_batch):
          bulk_insert(conn, synonym_batch, "SYNONYMS")
          synonym_batch = {}

          bulk_insert(conn, curie_batch, "CURIES")
          curie_batch = {}

          logger.debug(f"02 | ADDED {idx} TO DUCKDB")

      # * If anything is left over
      bulk_insert(conn, synonym_batch, "SYNONYMS")
      synonym_batch = {}

      bulk_insert(conn, curie_batch, "CURIES")
      curie_batch = {}

  # * Add categories
  bulk_insert(conn, [{"CATEGORY_ID": v, "CATEGORY_NAME": k} for k, v in categories], "CATEGORIES")

  # * Add sources
  bulk_insert(
    conn,
    [
      {
        "SOURCE_ID": i,
        "SOURCE_NAME": "BABEL",
        "SOURCE_VERSION": 2025.07,
        "NLP_LEVEL": i
      } 
      for i in range(1)
    ],
    "SOURCES"
  )

def lookup(classes: list[Path]) -> dict[str, list[str]]:
  table: dict[str, list[str]] = {}

  for p in classes:
    with lzma.open(p, "rb") as f:
      for line in f:
        r: object = orjson.loads(line)
        curie, *aliases = r["equivalent_identifiers"]
        cleaned: list[str] = [clean(a) for a in aliases] if aliases else []
        cleaned = list(filter(remove_problematic, cleaned))
        table.update({curie: cleaned})

  return table

CLI: object = typer.Typer(pretty_exceptions_show_locals=False)

@CLI.command()
def main(
  synonyms: list[Path] = typer.Option(..., "-s", "--synonyms", help="synonyms.txt.xz"),
  classes: list[Path] = typer.Option(..., "-c", "--classes", help="classes.ndjson.xz"),
  export: Path = typer.Option(Path("./dbssert.duckdb"), "-e", "--export", help="name.duckdb")
) -> None:
  try:
    with duckdb.connect(export) as conn:
      init(conn)
      table: dict[str, list[str]] = lookup(classes)
      build(synonyms, conn, table)
      index(conn)

  finally:
    conn.close()
