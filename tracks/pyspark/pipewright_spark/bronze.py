"""Read the bronze files of the lake with explicit schemas (never inferred: inference changes with the data)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

S = T.StringType()
ITEM = T.StructType([T.StructField("sku", S), T.StructField("qty", T.LongType()), T.StructField("unit_price_paise", T.LongType())])

SCHEMAS = {
    "orders": T.StructType(
        [T.StructField(n, S) for n in ("order_id", "customer_id", "status", "created_at", "updated_at", "ship_city", "payment_method")]
        + [T.StructField("is_test", T.BooleanType()), T.StructField("items", T.ArrayType(ITEM))]
    ),
    "customers": T.StructType(
        [T.StructField(n, S) for n in ("customer_id", "signup_at", "acquisition_channel")]
        + [T.StructField("is_test", T.BooleanType())]
        + [T.StructField(n, S) for n in ("city", "updated_at")]
    ),
    "payments": T.StructType(
        [T.StructField(n, S) for n in ("event_id", "type", "customer_id", "order_id", "subscription_id", "plan")]
        + [T.StructField("amount_paise", T.LongType()), T.StructField("occurred_at", S)]
        + [T.StructField("period_start", T.DateType()), T.StructField("period_end", T.DateType())]
    ),
    # CSV: required columns only, as text; new columns may only be appended at the end (contract rule)
    "marketing_spend": T.StructType([T.StructField(n, S) for n in ("date", "channel", "spend", "currency")]),
    "products": T.StructType([T.StructField(n, S) for n in ("sku", "name", "category", "price_inr", "active")]),
}


def read(spark: SparkSession, lake: Path, source: str, run_date: str) -> DataFrame:
    """All partitions of `source` up to and including run_date, with the partition date as column `dt`."""
    base = str(lake / "raw" / source)
    reader = spark.read.schema(SCHEMAS[source]).option("basePath", base)
    if source in ("marketing_spend", "products"):
        df = reader.option("header", True).option("mode", "FAILFAST").csv(f"{base}/dt=*/*.csv")
    else:
        df = reader.option("mode", "FAILFAST").json(f"{base}/dt=*/*.jsonl")
    return df.withColumn("dt", F.col("dt").cast("date")).where(F.col("dt") <= F.lit(run_date).cast("date"))


def city_map(spark: SparkSession, seeds: Path) -> DataFrame:
    schema = T.StructType([T.StructField(n, S) for n in ("raw_city", "city", "region")])
    return spark.read.schema(schema).option("header", True).csv(str(seeds / "city_map.csv"))
