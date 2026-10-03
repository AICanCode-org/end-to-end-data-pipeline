"""Milestone 3: the gold DataFrames, written as Parquet and registered in DuckDB, equal the expected files; the
contract is enforced; and the trickiest rules have small hand-made cases (same as the dbt unit tests)."""

from __future__ import annotations

import datetime as dt

import parity
import pytest

from pipewright import testkit
from pipewright_spark import gold as g
from pipewright_spark import pipeline, warehouse

pytestmark = pytest.mark.group("Phase 3 M3 - gold models (the five reports)")
CONTRACT = pipeline.load_contract(testkit.ROOT / "contract" / "data-contract.yaml")


@pytest.fixture(scope="module")
def built(gold, lake):
    paths = pipeline.write_parquet(gold, lake.lake_dir / "gold", CONTRACT)
    db = lake.build_warehouse
    warehouse.register(db, paths, CONTRACT)
    return parity.load(db)


@pytest.mark.parametrize("table", list(parity.GOLD))
def test_gold_table_matches_golden(built, table):
    expected = parity.load(testkit.GOLDEN / "fixture")[table]
    problems = parity.diff_csv(built[table]["csv"], expected["csv"], parity.GOLD[table]["columns"])
    assert not problems, f"gold.{table}: " + "; ".join(problems)


def test_enforce_rejects_missing_column(gold):
    with pytest.raises(pipeline.ContractError, match="columns"):
        pipeline.enforce("dim_products", gold["dim_products"].drop("category"), CONTRACT)


def test_enforce_rejects_wrong_type(gold):
    df = gold["mart_revenue_daily"].withColumn("orders", gold["mart_revenue_daily"]["orders"].cast("string"))
    with pytest.raises(pipeline.ContractError, match="wrong types"):
        pipeline.enforce("mart_revenue_daily", df, CONTRACT)


def test_cac_rounds_down_and_is_null_without_customers(spark):
    spend = spark.createDataFrame([(dt.date(2026, 6, 3), "meta_ads", 1000), (dt.date(2026, 6, 9), "google_ads", 500)],
                                  "spend_date date, channel string, spend_paise bigint")
    customers = spark.createDataFrame([("C-1", dt.date(2026, 6, 5), "meta_ads"), ("C-2", dt.date(2026, 6, 7), "meta_ads"),
                                       ("C-3", dt.date(2026, 6, 8), "meta_ads"), ("C-4", None, "google_ads")],
                                      "customer_id string, first_order_date date, acquisition_channel string")
    rows = {r["channel"]: r for r in g.mart_cac_monthly(spend, customers).collect()}
    assert rows["meta_ads"]["cac_paise"] == 333
    assert rows["google_ads"]["cac_paise"] is None and rows["google_ads"]["new_customers"] == 0


def test_mrr_counts_annual_as_one_twelfth(spark):
    payments = spark.createDataFrame([
        ("subscription.charged", "S-1", "prime_monthly", 14900, dt.date(2026, 6, 10), dt.date(2026, 7, 10)),
        ("subscription.charged", "S-2", "prime_annual", 142800, dt.date(2026, 6, 15), dt.date(2027, 6, 15)),
        ("payment.captured", None, None, 99900, None, None),
    ], "event_type string, subscription_id string, plan string, amount_paise bigint, period_start date, period_end date")
    rows = [r.asDict() for r in g.mart_mrr_monthly(payments, "2026-07-31").orderBy("month").collect()]
    assert [(r["mrr_paise"], r["new_mrr_paise"], r["churned_mrr_paise"]) for r in rows] == [(26800, 26800, 0), (11900, 0, 14900)]
