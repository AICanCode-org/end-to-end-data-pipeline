"""Helpers the track test suites share: a lake ingested from the committed 3-day fixture, and the silver oracle.

Both tracks build silver + gold from the same lake, so both are checked against the same expected files:
contract/golden/fixture/*.csv (gold) and contract/golden/fixture-silver/*.csv (silver, flat columns only).
"""

from __future__ import annotations

import csv
import io
import sys
import threading
from pathlib import Path

from . import config, ingest

ROOT = Path(__file__).resolve().parents[2] if (Path(__file__).resolve().parents[2] / "contract").is_dir() else config.repo_root()
FIXTURE = ROOT / "contract" / "fixture" / "source"
GOLDEN = ROOT / "contract" / "golden"
DAYS = ["2026-06-01", "2026-06-02", "2026-06-03"]
RUN_DATE = DAYS[-1]

# silver table -> (key, flat columns). Nested columns (stg_orders.items) are checked through stg_order_items.
SILVER: dict[str, tuple[list[str], list[str]]] = {
    "stg_orders": (["order_id"], ["order_id", "customer_id", "status", "created_at_utc", "updated_at_utc", "order_date",
                                  "city", "region", "payment_method"]),
    "stg_order_items": (["order_id", "line_number"], ["order_id", "line_number", "sku", "qty", "unit_price_paise"]),
    "stg_customers": (["customer_id"], ["customer_id", "signup_date", "city", "region", "acquisition_channel"]),
    "stg_payments": (["event_id"], ["event_id", "event_type", "customer_id", "order_id", "subscription_id", "plan",
                                    "amount_paise", "occurred_at_utc", "event_date", "period_start", "period_end"]),
    "stg_marketing_spend": (["spend_date", "channel"], ["spend_date", "channel", "spend_paise"]),
    "stg_products": (["sku"], ["sku", "product_name", "category", "price_paise", "first_seen_date"]),
}


def settings_for(tmp: Path, url: str, **extra: str) -> config.Settings:
    return config.load({"PIPEWRIGHT_HOME": str(ROOT), "SOURCE_API_URL": url, "PIPEWRIGHT_DROPS_DIR": str(FIXTURE / "drops"),
                        "PIPEWRIGHT_LAKE_DIR": str(tmp / "lake"), "PIPEWRIGHT_WAREHOUSE": str(tmp / "warehouse" / "pipewright.duckdb"),
                        "PIPEWRIGHT_PAGE_SIZE": "50", **extra})


def ingest_fixture(tmp: Path, **extra: str) -> config.Settings:
    """Serve the fixture with the API stub, ingest the three days into tmp/lake and return the settings."""
    sys.path.insert(0, str(ROOT / "source"))
    from bazaarbox_source.api import serve

    server = serve(FIXTURE, port=0, now="follow")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        settings = settings_for(tmp, f"http://127.0.0.1:{server.server_address[1]}", **extra)
        for day in DAYS:
            ingest.ingest_all(settings, config.parse_date(day))
    finally:
        server.shutdown()
        server.server_close()
    return settings


def canonical(rows: list[tuple], cols: list[str]) -> str:
    """CSV text of already-stringified rows (None -> empty), sorted as given."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(cols)
    for row in rows:
        w.writerow(["" if v is None else str(v) for v in row])
    return buf.getvalue()


def silver_sql(table: str) -> str:
    """DuckDB query giving a silver table in canonical form (timestamps as 'YYYY-MM-DD HH:MM:SS')."""
    key, cols = SILVER[table]
    sel = ", ".join(f"strftime({c}, '%Y-%m-%d %H:%M:%S')" if c.endswith("_utc") else f"CAST({c} AS VARCHAR)" for c in cols)
    return f"SELECT {sel} FROM silver.{table} ORDER BY {', '.join(key)}"


def expected_silver(table: str) -> str:
    return (GOLDEN / "fixture-silver" / f"{table}.csv").read_text(encoding="utf-8")


def first_diff(actual: str, expected: str) -> str:
    """A short description of the first differing line, for assertion messages."""
    a, e = actual.splitlines(), expected.splitlines()
    for i, (x, y) in enumerate(zip(a, e, strict=False)):
        if x != y:
            return f"line {i + 1}: got {x!r}, expected {y!r}"
    return f"row counts differ: got {len(a) - 1}, expected {len(e) - 1}"


def golden_warehouse(path: Path, golden: Path | None = None) -> Path:
    """A warehouse whose gold tables are loaded from golden CSVs (default: the fixture), typed as the contract says;
    lets the checks, publishing, the runner and the dashboard be tested without running a track."""
    import duckdb
    import yaml

    golden = golden or GOLDEN / "fixture"
    spec = yaml.safe_load((ROOT / "contract" / "data-contract.yaml").read_text(encoding="utf-8"))
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    for table, t in spec["gold"].items():
        cols = ", ".join(f"NULLIF({c}, '')::{typ} AS {c}" if typ != "VARCHAR" else c for c, typ in t["columns"].items())
        con.execute(f"CREATE OR REPLACE TABLE gold.{table} AS SELECT {cols} FROM read_csv('{golden / (table + '.csv')}', all_varchar=true, header=true)")
    con.close()
    return path
