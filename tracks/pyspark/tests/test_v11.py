"""1.1: incremental merge of the facts, reading CSVs whose header grew, rto and coupon discounts (hand cases,
the same as the dbt unit tests)."""

from __future__ import annotations

import datetime as dt

import pytest
from conftest import canonical_df
from pyspark.sql import functions as F

from pipewright import testkit
from pipewright_spark import bronze, pipeline
from pipewright_spark import gold as g

pytestmark = pytest.mark.group("Phase 7 - incremental builds")
CONTRACT = pipeline.load_contract(testkit.ROOT / "contract" / "data-contract.yaml")
KEYS = {"fct_orders": ["order_id"], "fct_order_items": ["order_id", "line_number"]}


def canon(df, name):
    return canonical_df(df, KEYS[name], list(CONTRACT["gold"][name]["columns"]))


def test_incremental_merge_rebuilds_only_recent_and_unknown_orders(silver, gold):
    full_items, full_orders = gold["fct_order_items"], gold["fct_orders"]
    recent = {r.order_id for r in full_orders.where(F.col("updated_at_utc") >= F.lit("2026-06-02 18:30:00")).collect()}  # IST 06-03
    old = sorted({r.order_id for r in full_orders.collect()} - recent)
    assert recent and len(old) >= 2
    # a stale previous state: recent orders still "placed", one old line wrongly 'unknown', one old order with a marker
    prev_orders = full_orders.withColumn("status", F.when(F.col("order_id").isin(recent), F.lit("placed")).otherwise(F.col("status")))
    prev_orders = prev_orders.withColumn("gross_paise", F.when(F.col("order_id") == old[0], F.lit(1).cast("bigint")).otherwise(F.col("gross_paise")))
    prev_items = full_items.withColumn("category", F.when(F.col("order_id") == old[1], F.lit("unknown")).otherwise(F.col("category")))
    items, orders = pipeline.facts(silver, {"fct_orders": prev_orders, "fct_order_items": prev_items}, "2026-06-03")
    assert canon(items, "fct_order_items") == canon(full_items, "fct_order_items")  # recent + unknown lines rebuilt
    marker = orders.where(F.col("order_id") == old[0]).first()
    assert marker.gross_paise == 1  # an order not updated since `since` is kept as it was (that is incremental)
    fixed = orders.withColumn("gross_paise", F.when(F.col("order_id") == old[0], full_orders.where(F.col("order_id") == old[0]).first().gross_paise)
                              .otherwise(F.col("gross_paise")))
    assert canon(fixed, "fct_orders") == canon(full_orders, "fct_orders")


def test_read_previous_types_like_the_contract(spark, gold, tmp_path):
    for name in ("fct_orders", "fct_order_items"):
        gold[name].write.parquet(str(tmp_path / f"{name}.parquet"))
    prev = pipeline.read_previous(spark, tmp_path, CONTRACT)
    for name, df in prev.items():
        assert pipeline.enforce(name, df, CONTRACT) is df


def test_csv_files_with_a_grown_header_are_read_together(spark, tmp_path):
    base = tmp_path / "raw" / "marketing_spend"
    (base / "dt=2026-09-14").mkdir(parents=True)
    (base / "dt=2026-09-15").mkdir(parents=True)
    (base / "dt=2026-09-14" / "spend_2026-09-14.csv").write_text("date,channel,spend,currency\n2026-09-14,meta_ads,10.00,INR\n")
    (base / "dt=2026-09-15" / "spend_2026-09-15.csv").write_text("date,channel,spend,currency,campaign_id\n2026-09-15,influencer,5.50,INR,C-7\n")
    rows = sorted(bronze.read(spark, tmp_path, "marketing_spend", "2026-09-30").collect(), key=lambda r: r.date)
    assert [(r.channel, r.campaign_id, str(r.dt)) for r in rows] == [("meta_ads", None, "2026-09-14"), ("influencer", "C-7", "2026-09-15")]
    assert bronze.read(spark, tmp_path, "products", "2026-09-30").count() == 0  # no file yet: empty, right columns


def test_rto_and_cancelled_orders_are_not_valid(spark):
    customers = spark.createDataFrame([("C-1", dt.date(2026, 9, 1), "Pune", "West", "influencer"), ("C-2", dt.date(2026, 9, 2), "Pune", "West", "organic")],
                                      "customer_id string, signup_date date, city string, region string, acquisition_channel string")
    orders = spark.createDataFrame([("O-1", "C-1", dt.date(2026, 9, 3), "rto"), ("O-2", "C-1", dt.date(2026, 9, 5), "delivered"),
                                    ("O-3", "C-2", dt.date(2026, 9, 4), "rto"), ("O-4", "C-2", dt.date(2026, 9, 6), "cancelled")],
                                   "order_id string, customer_id string, order_date date, status string")
    got = {r.customer_id: (r.first_order_date, r.valid_orders) for r in g.dim_customers(customers, orders).collect()}
    assert got == {"C-1": (dt.date(2026, 9, 5), 1), "C-2": (None, 0)}


def test_net_is_gross_minus_the_coupon_discount(spark):
    ts = dt.datetime(2026, 9, 16, 4, 0)
    orders = spark.createDataFrame(
        [("O-1", "C-1", dt.date(2026, 9, 16), ts, ts, "delivered", "Pune", "West", "prepaid", 0),
         ("O-2", "C-1", dt.date(2026, 9, 16), ts, ts, "rto", "Pune", "West", "cod", 1500)],
        "order_id string, customer_id string, order_date date, created_at_utc timestamp_ntz, updated_at_utc timestamp_ntz, status string, "
        "city string, region string, payment_method string, discount_paise bigint")
    items = spark.createDataFrame([("O-1", 2, 20000), ("O-2", 1, 15000)], "order_id string, qty bigint, line_paise bigint")
    got = {r.order_id: (r.gross_paise, r.discount_paise, r.net_paise, r.is_revenue) for r in g.fct_orders(orders, items).collect()}
    assert got == {"O-1": (20000, 0, 20000, True), "O-2": (15000, 1500, 13500, False)}
