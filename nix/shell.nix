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
      pyarrow
      loguru
      duckdb
      orjson
      typer
    ]) ++ (with pkgs; [
      chromium
    ]);
    nativeBuildInputs = [
      pkgs.makeWrapper
    ];
    makeWrapperArgs = [
      "--set CHROMIUM_PATH ${pkgs.chromium}/bin/chromium"
      "--set PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD 1"
    ];
    doCheck = false;
  };
in {
  devShells.default = pkgs.mkShell {
    packages = (with py; [
      python
      flake8
    ]) ++ ([
      dbssert
    ]);
  };
}