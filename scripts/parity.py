#!/usr/bin/env python3
"""Cross-track parity: do two builds (or a build and the golden files) contain exactly the same gold tables?

    python3 scripts/parity.py export <warehouse.duckdb> <out_dir>      # canonical CSV + fingerprints.json
    python3 scripts/parity.py compare <A> <B> [--tables t1,t2]          # A, B: a .duckdb file or an exported dir

Canonical form: every gold table of contract/data-contract.yaml, rows sorted by the table's key, NULL as an
empty field, dates ISO, timestamps 'YYYY-MM-DD HH:MM:SS', booleans true/false, doubles with 12 significant digits.
Integers and text must match exactly; doubles within 1e-9 (relative). A golden directory may hold only
fingerprints for the big tables (rows + sha256 of the canonical CSV); those are compared by fingerprint.
Needs duckdb (already in every track's environment) and PyYAML. PARITY_CONTRACT=<file> compares with the tables
and columns of another contract version (1.1 backcompat check: contract/history/data-contract-1.0.0.yaml).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONTRACT_FILE = Path(os.environ.get("PARITY_CONTRACT") or ROOT / "contract" / "data-contract.yaml")
CONTRACT = yaml.safe_load(CONTRACT_FILE.read_text(encoding="utf-8"))
GOLD = CONTRACT["gold"]


def fmt(value, typ: str) -> str:
    if value is None:
        return ""
    if typ == "DOUBLE":
        return f"{value:.12g}"
    if typ == "BOOLEAN":
        return "true" if value else "false"
    if typ == "TIMESTAMP":
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def canonical(con, table: str) -> str:
    spec = GOLD[table]
    cols = list(spec["columns"])
    order = ", ".join(f'"{k}"' for k in spec["key"])
    rows = con.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM gold.{table} ORDER BY {order}').fetchall()
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(cols)
    for row in rows:
        w.writerow([fmt(v, spec["columns"][c]) for v, c in zip(row, cols, strict=True)])
    return buf.getvalue()


def load(source: Path) -> dict[str, dict]:
    """{table: {"csv": text or None, "rows": n, "sha256": hex}} from a warehouse file or an exported directory."""
    out = {}
    if source.suffix == ".duckdb":
        import duckdb

        con = duckdb.connect(str(source), read_only=True)
        try:
            for t in GOLD:
                text = canonical(con, t)
                out[t] = {"csv": text, "rows": text.count("\n") - 1, "sha256": hashlib.sha256(text.encode()).hexdigest()}
        finally:
            con.close()
        return out
    fps = json.loads((source / "fingerprints.json").read_text()) if (source / "fingerprints.json").exists() else {}
    for t in GOLD:
        f = source / f"{t}.csv"
        if f.exists():
            text = f.read_text(encoding="utf-8")
            out[t] = {"csv": text, "rows": text.count("\n") - 1, "sha256": hashlib.sha256(text.encode()).hexdigest()}
        elif t in fps:
            out[t] = {"csv": None, **fps[t]}
    return out


def export(db: Path, out_dir: Path, csv_tables: set[str] | None = None) -> None:
    data = load(db)
    out_dir.mkdir(parents=True, exist_ok=True)
    fps = {}
    for t, d in data.items():
        fps[t] = {"rows": d["rows"], "sha256": d["sha256"]}
        if csv_tables is None or t in csv_tables:
            (out_dir / f"{t}.csv").write_text(d["csv"], encoding="utf-8")
    (out_dir / "fingerprints.json").write_text(json.dumps(fps, indent=1, sort_keys=True) + "\n")
    print(f"exported {len(data)} gold tables from {db} to {out_dir}")


def diff_csv(a: str, b: str, types: dict[str, str]) -> list[str]:
    ra, rb = list(csv.reader(io.StringIO(a))), list(csv.reader(io.StringIO(b)))
    if ra[:1] != rb[:1]:
        return [f"columns differ: {ra[:1]} vs {rb[:1]}"]
    if len(ra) != len(rb):
        return [f"row counts differ: {len(ra) - 1} vs {len(rb) - 1}"]
    problems = []
    cols = ra[0]
    for i, (x, y) in enumerate(zip(ra[1:], rb[1:], strict=True), 1):
        for c, u, v in zip(cols, x, y, strict=True):
            if u == v:
                continue
            if types[c] == "DOUBLE" and u and v and math.isclose(float(u), float(v), rel_tol=1e-9, abs_tol=1e-12):
                continue
            problems.append(f"row {i} column {c}: {u!r} vs {v!r}")
            if len(problems) >= 5:
                return problems
    return problems


def compare(a: Path, b: Path, tables: list[str] | None = None) -> int:
    da, db_ = load(a), load(b)
    bad = 0
    for t in tables or list(GOLD):
        x, y = da.get(t), db_.get(t)
        if x is None or y is None:
            print(f"MISSING {t}: present in {'B' if x is None else 'A'} only" if (x or y) else f"MISSING {t}: in neither")
            bad += 1
            continue
        if x["csv"] is not None and y["csv"] is not None:
            problems = diff_csv(x["csv"], y["csv"], GOLD[t]["columns"])
        else:
            problems = [] if (x["rows"], x["sha256"]) == (y["rows"], y["sha256"]) else [
                f"fingerprint differs: {x['rows']} rows {x['sha256'][:12]} vs {y['rows']} rows {y['sha256'][:12]}"]
        print(f"{'MATCH' if not problems else 'DIFF '} {t} ({x['rows']} rows)")
        for p in problems:
            print(f"    {p}")
        bad += bool(problems)
    print("PARITY OK" if not bad else f"PARITY FAILED: {bad} table(s) differ")
    return 1 if bad else 0


def main(argv: list[str]) -> int:
    if len(argv) >= 3 and argv[0] == "export":
        export(Path(argv[1]), Path(argv[2]), set(argv[3].split(",")) if len(argv) > 3 else None)
        return 0
    if len(argv) >= 3 and argv[0] == "compare":
        tables = argv[4].split(",") if len(argv) > 4 and argv[3] == "--tables" else None
        return compare(Path(argv[1]), Path(argv[2]), tables)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
