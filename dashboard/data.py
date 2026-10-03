"""Everything the dashboard reads, as plain functions (tested without Streamlit).

The dashboard reads only the published warehouse: gold tables plus ops.build_info / ops.dq_results, always with
explicit column lists, so a newer warehouse (extra tables or columns, v1.1) still works with this version.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def rupees(paise: int | float | None, decimals: int = 0) -> str:
    """Integer paise -> '₹12,34,567' (Indian digit grouping). None -> '—'."""
    if paise is None or (isinstance(paise, float) and pd.isna(paise)):
        return "—"
    sign, value = ("-" if paise < 0 else ""), abs(int(paise))
    whole, frac = divmod(value, 100)
    digits = str(whole)
    if len(digits) > 3:
        head, tail = digits[:-3], digits[-3:]
        groups = []
        while len(head) > 2:
            groups.insert(0, head[-2:])
            head = head[:-2]
        digits = ",".join([head, *groups, tail]) if head else ",".join([*groups, tail])
    return f"{sign}₹{digits}" + (f".{frac:02d}" if decimals else "")


@dataclass
class Status:
    run_date: dt.date | None
    track: str | None
    contract_version: str | None
    checks: pd.DataFrame
    latest_report: dict | None  # the newest dq report on disk (may be newer than the published data)

    @property
    def last_run_blocked(self) -> bool:
        return bool(self.latest_report) and not self.latest_report["summary"]["publishable"]

    def age_days(self, today: dt.date | None = None) -> int | None:
        if self.run_date is None:
            return None
        today = today or dt.datetime.now(IST).date()
        return (today - self.run_date).days


def connect(warehouse: Path) -> duckdb.DuckDBPyConnection:
    """A read-only connection to the published file (publishing swaps the file; open connections keep the old one)."""
    return duckdb.connect(str(warehouse), read_only=True)


def status(con: duckdb.DuckDBPyConnection, reports_dir: Path) -> Status:
    try:
        run_date, track, version = con.execute("SELECT run_date, track, contract_version FROM ops.build_info").fetchone()
    except (duckdb.Error, TypeError):
        run_date = track = version = None
    try:
        checks = con.execute(
            "SELECT check_id, severity, status, observed, threshold, detail FROM ops.dq_results ORDER BY status = 'pass', severity, check_id"
        ).df()
    except duckdb.Error:
        checks = pd.DataFrame(columns=["check_id", "severity", "status", "observed", "threshold", "detail"])
    reports = sorted(reports_dir.glob("dq-*.json")) if reports_dir.is_dir() else []
    latest = json.loads(reports[-1].read_text(encoding="utf-8")) if reports else None
    return Status(run_date, track, version, checks, latest)


def revenue_daily(con, regions: list[str] | None = None, categories: list[str] | None = None) -> pd.DataFrame:
    where, params = [], []
    if regions:
        where.append(f"region IN ({', '.join('?' * len(regions))})")
        params += regions
    if categories:
        where.append(f"category IN ({', '.join('?' * len(categories))})")
        params += categories
    sql = "SELECT order_date, region, category, orders, units, revenue_paise FROM gold.mart_revenue_daily"
    if where:
        sql += " WHERE " + " AND ".join(where)
    return con.execute(sql + " ORDER BY order_date, region, category", params).df()


def dimension_values(con, column: str) -> list[str]:
    assert column in ("region", "category")
    return [r[0] for r in con.execute(f"SELECT DISTINCT {column} FROM gold.mart_revenue_daily ORDER BY 1").fetchall()]


def cac_monthly(con) -> pd.DataFrame:
    return con.execute("SELECT month, channel, spend_paise, new_customers, cac_paise FROM gold.mart_cac_monthly ORDER BY month, channel").df()


def mrr_monthly(con) -> pd.DataFrame:
    return con.execute(
        "SELECT month, active_subscriptions, monthly_subscriptions, annual_subscriptions, mrr_paise, new_mrr_paise, churned_mrr_paise "
        "FROM gold.mart_mrr_monthly ORDER BY month"
    ).df()


def top_products(con) -> pd.DataFrame:
    return con.execute(
        "SELECT rank, sku, product_name, category, units, revenue_paise, as_of_date FROM gold.mart_top_products_30d ORDER BY rank"
    ).df()


def cohorts(con) -> pd.DataFrame:
    return con.execute(
        "SELECT cohort_month, customers, mature_customers, retained_30d, retention_rate FROM gold.mart_cohort_retention ORDER BY cohort_month"
    ).df()
