"""Silver: cleaned, typed, de-duplicated DataFrames (contract/transform-spec.md §3)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .parsing import clean_key, ist_date, to_paise, utc_timestamp


def latest(df: DataFrame, key: str, *order: F.Column) -> DataFrame:
    """Keep one row per `key`: the first by `order` (row_number = 1)."""
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def map_city(df: DataFrame, cities: DataFrame, raw_col: str) -> DataFrame:
    m = cities.select(F.col("raw_city").alias("_raw"), F.col("city").alias("_city"), F.col("region").alias("_region"))
    return (
        df.join(m, clean_key(F.col(raw_col)) == F.col("_raw"), "left")
        .withColumn("city", F.coalesce("_city", F.lit("Unknown")))
        .withColumn("region", F.coalesce("_region", F.lit("Unknown")))
        .drop("_raw", "_city", "_region")
    )


def stg_orders(raw: DataFrame, cities: DataFrame) -> DataFrame:
    """§3.1 The latest version of every non-test order."""
    parsed = raw.withColumn("created_at_utc", utc_timestamp(F.col("created_at"))).withColumn(
        "updated_at_utc", utc_timestamp(F.col("updated_at"))
    )
    df = latest(parsed, "order_id", F.col("updated_at_utc").desc(), F.col("dt").desc())
    df = map_city(df, cities, "ship_city")
    return df.where(~F.col("is_test")).select(
        "order_id",
        "customer_id",
        F.lower(F.trim("status")).alias("status"),
        "created_at_utc",
        "updated_at_utc",
        ist_date(F.col("created_at_utc")).alias("order_date"),
        "city",
        "region",
        F.lower(F.trim("payment_method")).alias("payment_method"),
        "items",
        "coupon_code",  # 1.1 optional
        F.coalesce("discount_paise", F.lit(0)).cast("bigint").alias("discount_paise"),
    )


def stg_order_items(orders: DataFrame) -> DataFrame:
    """§3.2 Kept lines (qty > 0) with their 1-based position in the items array."""
    exploded = orders.select("order_id", F.posexplode("items").alias("pos", "item"))
    return exploded.where(F.col("item.qty") > 0).select(
        "order_id",
        (F.col("pos") + 1).cast("int").alias("line_number"),
        F.col("item.sku").alias("sku"),
        F.col("item.qty").alias("qty"),
        F.col("item.unit_price_paise").alias("unit_price_paise"),
    )


def stg_customers(raw: DataFrame, cities: DataFrame) -> DataFrame:
    """§3.3 The latest profile of every non-test customer."""
    df = latest(raw, "customer_id", utc_timestamp(F.col("updated_at")).desc(), F.col("dt").desc())
    df = map_city(df.withColumnRenamed("city", "raw_city_value"), cities, "raw_city_value")
    return df.where(~F.col("is_test")).select(
        "customer_id",
        ist_date(utc_timestamp(F.col("signup_at"))).alias("signup_date"),
        "city",
        "region",
        F.lower(F.trim("acquisition_channel")).alias("acquisition_channel"),
    )


def stg_payments(raw: DataFrame) -> DataFrame:
    """§3.4 One row per event_id (the first delivery; re-deliveries are identical)."""
    df = latest(raw, "event_id", F.col("dt").asc())
    return df.select(
        "event_id",
        F.col("type").alias("event_type"),
        "customer_id",
        "order_id",
        "subscription_id",
        "plan",
        "amount_paise",
        utc_timestamp(F.col("occurred_at")).alias("occurred_at_utc"),
        ist_date(utc_timestamp(F.col("occurred_at"))).alias("event_date"),
        "period_start",
        "period_end",
    )


def stg_marketing_spend(raw: DataFrame, run_date: str) -> DataFrame:
    """§3.5 Spend per day and channel in integer paise; the latest delivery of a day wins."""
    df = raw.select(
        F.col("date").cast("date").alias("spend_date"),
        F.lower(F.trim("channel")).alias("channel"),
        to_paise(F.col("spend")).alias("spend_paise"),
        "dt",
    ).where(F.col("spend_date") <= F.lit(run_date).cast("date"))
    w = Window.partitionBy("spend_date", "channel").orderBy(F.col("dt").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "dt")


def stg_products(raw: DataFrame) -> DataFrame:
    """§3.6 The latest catalogue snapshot, with the first date each SKU appeared."""
    first_seen = raw.groupBy("sku").agg(F.min("dt").alias("first_seen_date"))
    last = raw.agg(F.max("dt").alias("_last"))
    snap = raw.join(last, F.col("dt") == F.col("_last"))
    return snap.join(first_seen, "sku").select(
        "sku",
        F.trim("name").alias("product_name"),
        F.translate(F.lower(F.trim("category")), "é", "e").alias("category"),
        to_paise(F.col("price_inr")).alias("price_paise"),
        "first_seen_date",
    )
