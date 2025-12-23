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
import sys
import re

logger.remove()
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
  CATEGORY_ID INTEGER,
  TAXON_ID INTEGER
);
    """,
    """\
CREATE TABLE IF NOT EXISTS SYNONYMS (
  CURIE_ID INTEGER,
  SOURCE_ID INTEGER,
  SYNONYM VARCHAR
);
    """]
  for op in schemas:
    conn.execute(op)

def index(conn: object) -> None:
  indexes: list[str] = [
    "CREATE INDEX CURIE_SYNONYMS ON SYNONYMS (SYNONYM);",
    "CREATE INDEX CATEGORY_NAMES ON CATEGORIES (CATEGORY_NAME);",
    "CREATE INDEX CURIE_TAXON ON CURIES (TAXON_ID);"
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
  if not cleaned:
    return sys.intern("")
  elif ne(cleaned, x):
    return clean(cleaned)
  elif eq(x[0], "\'") and eq(x[-1], "\'"):
    return clean(x[1:-1])
  elif eq(x[0], '"') and eq(x[-1], '"'):
    return clean(x[1:-1])
  else:
    return sys.intern(x)

def bulk_insert(conn: object, batch: list[dict[str, dict[str, Union[str, list[str], int]]]], table: str) -> None:
  arrow: object = pa.Table.from_pylist(batch)
  # ! Somehow DuckDB picks up on the python variable...
  conn.execute(f"INSERT INTO {table} SELECT * FROM arrow")

REGEX: object = re.compile(r"\W+")

def build(
  synonyms: list[Path],
  conn: object,
  table: dict[str, tuple[str]],
  max_batch: int = 50_000_000,
  log: float = 2_000_000
) -> None:
  categories: dict[str, int] = {}
  curie_batch: list[dict[str, dict[str, Union[str, int]]]] = []
  synonym_batch: list[dict[str, dict[str, Union[str, list[str], int]]]] = []
  idx: int = 0

  for p in synonyms:
    with lzma.open(p, "rb") as f:
      logger.warning(f"04 | {p} | STARTED ADDING")

      for line in f:
        line: object = line.strip()

        if not line:
          continue

        r: object = orjson.loads(line)

        curie: str = r["curie"]
        curie = sys.intern(curie)

        aliases: list[str] = r["names"]
        aliases.append(curie)

        if curie in table:
          aliases.extend(table[curie])

        aliases = list(filter(remove_problematic, aliases))

        preferred: str = r["preferred_name"]
        preferred = clean(preferred)

        category: str = r["types"][0]
        if category in categories:
          category_id: int = categories[category]
        else:
          category_id: int = len(categories)
          categories.update({category: category_id})

        taxon: Union[int, str] = r.get("taxa", [])
        taxon = str(taxon[0]) if taxon else "0"
        taxon = int(taxon[10:]) if "NCBITaxon:" in taxon else int(taxon)

        zero: list[str] = list(set(clean(a).lower() for a in aliases))
        one: list[str] = list(set(REGEX.sub("", a) for a in zero))

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

        if eq((idx % log), 0) and ne(idx(idx % max_batch), 0):
          logger.debug(f"05 | {p} | PROCESSED {idx} TO ADD")

        if ge(len(synonym_batch), max_batch):
          bulk_insert(conn, synonym_batch, "SYNONYMS")
          synonym_batch = []

          bulk_insert(conn, curie_batch, "CURIES")
          curie_batch = []

          logger.debug(f"06 | {p} | ADDED {idx} TO DUCKDB")

      # * If anything is left over
      bulk_insert(conn, synonym_batch, "SYNONYMS")
      synonym_batch = []

      bulk_insert(conn, curie_batch, "CURIES")
      curie_batch = []

      logger.debug(f"07 | {p} | ADDED {idx} TO DUCKDB")

  # * Add categories
  bulk_insert(conn, [{"CATEGORY_ID": v, "CATEGORY_NAME": k} for k, v in categories.items()], "CATEGORIES")

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
      for i in range(2)
    ],
    "SOURCES"
  )

def lookup(classes: list[Path], log: float = 2_000_000) -> dict[str, tuple[str]]:
  table: dict[str, tuple[str]] = {}
  for p in classes:

    with lzma.open(p, "rb") as f:
      logger.warning(f"01 | {p} | STARTED MAPPING")

      for idx, line in enumerate(f, start=1):
        line: object = line.strip()

        if not line:
          continue

        r: object = orjson.loads(line)
        curie, *aliases = r["equivalent_identifiers"]
        curie: str = sys.intern(curie)

        cleaned: tuple[str] = tuple(set(filter(remove_problematic, (clean(a) for a in aliases)))) if aliases else tuple()

        table.update({curie: cleaned})

        if eq((idx % log), 0):
          logger.debug(f"02 | {p} | MAPPED {idx}")

    logger.debug(f"03 | {p} | MAPPED {idx}")
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
      table: list[dict[str, tuple[str]]] = lookup(classes)
      build(synonyms, conn, table)
      index(conn)

  finally:
    conn.close()
