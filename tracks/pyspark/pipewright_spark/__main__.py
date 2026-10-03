"""python -m pipewright_spark --run-date 2026-08-31 --lake lake --duckdb warehouse/pipewright_build.duckdb"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from . import pipeline, warehouse
from .session import get_spark


def main(argv: list[str] | None = None) -> int:
    root = Path(os.environ.get("PIPEWRIGHT_HOME", Path(__file__).resolve().parents[3]))
    ap = argparse.ArgumentParser(prog="python -m pipewright_spark", description="Build silver + gold with PySpark.")
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--lake", type=Path, default=Path(os.environ.get("PIPEWRIGHT_LAKE_DIR", root / "lake")))
    ap.add_argument("--duckdb", type=Path, default=Path(os.environ.get("PIPEWRIGHT_DUCKDB", root / "warehouse/pipewright_build.duckdb")))
    ap.add_argument("--parquet", type=Path, default=None, help="where gold Parquet goes (default <lake>/gold)")
    ap.add_argument("--contract", type=Path, default=root / "contract" / "data-contract.yaml")
    ap.add_argument("--previous", type=Path, default=None, help="1.1: folder with the previous published facts (incremental)")
    ap.add_argument("--since", default=None, help="1.1: rebuild the orders updated on or after this date (with --previous)")
    args = ap.parse_args(argv)
    t0 = time.time()
    contract = pipeline.load_contract(args.contract)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.previous and not args.since:
            ap.error("--previous needs --since")
        previous = pipeline.read_previous(spark, args.previous, contract) if args.previous else None
        tables = pipeline.build(spark, args.lake.resolve(), args.contract.parent / "seeds", args.run_date, previous, args.since)
        paths = pipeline.write_parquet(tables, (args.parquet or args.lake / "gold").resolve(), contract)
    except pipeline.ContractError as exc:
        print(f"CONTRACT VIOLATION: {exc}", file=sys.stderr)
        return 2
    finally:
        spark.stop()
    warehouse.register(args.duckdb, paths, contract)
    mode = f"incremental since {args.since}" if args.previous else "full"
    print(f"pyspark: built {len(paths)} gold tables ({mode}) for {args.run_date} in {time.time() - t0:.1f}s -> {args.duckdb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
