{pkgs, lib, config, ...}:
let
  py = pkgs.python313Packages;
  dbssert = py.buildPythonApplication rec {
    pname = "dbssert";
    version = "1.0.0";
    format = "pyproject";
    src = ../.;
    build-system = (with py; [
      setuptools
      wheel
    ]);
    propagatedBuildInputs = (with py; [
      backports-zstd
      pyarrow
      loguru
      duckdb
      orjson
      typer
    ]);
    doCheck = false;
  };
in {
  devShells.default = pkgs.mkShell {
    packages = (with py; [
      python
      flake8
    ]) ++ (with pkgs; [
      duckdb
    ]) ++ ([
      dbssert
    ]);
  };
}