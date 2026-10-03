"""Canonical CSV export of the gold tables (the same format as scripts/parity.py and contract/golden)."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from . import contract as contract_mod


def _fmt(value, typ: str) -> str:
    if value is None:
        return ""
    if typ == "DOUBLE":
        return f"{value:.12g}"
    if typ == "BOOLEAN":
        return "true" if value else "false"
    if typ == "TIMESTAMP":
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def export(db: Path, out: Path, contract_path: Path) -> list[Path]:
    gold = contract_mod.load(contract_path)["gold"]
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    written = []
    try:
        for table, spec in gold.items():
            cols = list(spec["columns"])
            quoted = ", ".join(f'"{c}"' for c in cols)
            order = ", ".join(f'"{k}"' for k in spec["key"])
            rows = con.execute(f"SELECT {quoted} FROM gold.{table} ORDER BY {order}").fetchall()
            path = out / f"{table}.csv"
            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(cols)
                w.writerows([_fmt(v, spec["columns"][c]) for v, c in zip(row, cols, strict=True)] for row in rows)
            written.append(path)
    finally:
        con.close()
    return written
