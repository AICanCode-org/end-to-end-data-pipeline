#!/usr/bin/env python3
"""Export the silver tables of a SQL-track build to canonical CSVs (the silver oracle both tracks are tested against).

    python3 scripts/export_silver.py <warehouse.duckdb> contract/golden/fixture-silver
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

from pipewright import testkit


def main(argv: list[str]) -> int:
    db, out = Path(argv[0]), Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    for table, (_, cols) in testkit.SILVER.items():
        (out / f"{table}.csv").write_text(testkit.canonical(con.execute(testkit.silver_sql(table)).fetchall(), cols), encoding="utf-8")
    con.close()
    print(f"exported {len(testkit.SILVER)} silver tables to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
