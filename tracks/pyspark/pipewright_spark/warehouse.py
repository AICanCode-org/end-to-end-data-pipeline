"""Register the Parquet gold tables in the DuckDB warehouse (schema gold), typed exactly as the contract says, so
the checks and the dashboard read the same tables whichever track built them."""

from __future__ import annotations

from pathlib import Path

import duckdb


def register(duckdb_path: Path, parquet: dict[str, Path], contract: dict) -> None:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        for name, path in parquet.items():
            cols = contract["gold"][name]["columns"]
            select = ", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t in cols.items())
            con.execute(f"CREATE OR REPLACE TABLE gold.{name} AS SELECT {select} FROM read_parquet('{path}/*.parquet')")
    finally:
        con.close()
