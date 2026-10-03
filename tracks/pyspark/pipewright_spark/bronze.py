"""Read the bronze files of the lake with explicit schemas (never inferred: inference changes with the data)."""

from __future__ import annotations

import csv
from functools import reduce
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
        # 1.1 optional keys (from 2026-09-15); a v1.0 line simply has them null
        + [T.StructField("coupon_code", S), T.StructField("discount_paise", T.LongType())]
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
    # CSV: the columns we read, as text. 1.1: campaign_id is optional (appended from 2026-09-15)
    "marketing_spend": T.StructType([T.StructField(n, S) for n in ("date", "channel", "spend", "currency", "campaign_id")]),
    "products": T.StructType([T.StructField(n, S) for n in ("sku", "name", "category", "price_inr", "active")]),
}


def _csv_groups(base: str) -> dict[tuple[str, ...], list[str]]:
    """Group the CSV files of a source by their header line.

    1.1 lesson: one FAILFAST schema cannot read files with 4 and 5 columns at once (Spark calls the shorter rows
    malformed), so each header group is read with its own schema and the groups are unioned by column name."""
    groups: dict[tuple[str, ...], list[str]] = {}
    for path in sorted(Path(base).glob("dt=*/*.csv")):
        with path.open(encoding="utf-8", newline="") as f:
            header = tuple(next(csv.reader(f), []))
        groups.setdefault(header, []).append(str(path))
    return groups


def _read_csv(spark: SparkSession, base: str, schema: T.StructType) -> DataFrame:
    wanted = [f.name for f in schema.fields]
    parts = []
    for header, files in _csv_groups(base).items():
        file_schema = T.StructType([T.StructField(n, S) for n in header])
        df = spark.read.schema(file_schema).option("header", True).option("mode", "FAILFAST").option("basePath", base).csv(files)
        parts.append(df.select(*[F.col(n) if n in header else F.lit(None).cast(S).alias(n) for n in wanted], "dt"))
    if not parts:  # no file yet: an empty frame with the right columns
        return spark.createDataFrame([], T.StructType([*schema.fields, T.StructField("dt", S)]))
    return reduce(DataFrame.unionByName, parts)


def read(spark: SparkSession, lake: Path, source: str, run_date: str) -> DataFrame:
    """All partitions of `source` up to and including run_date, with the partition date as column `dt`."""
    base = str(lake / "raw" / source)
    if source in ("marketing_spend", "products"):
        df = _read_csv(spark, base, SCHEMAS[source])
    else:
        df = spark.read.schema(SCHEMAS[source]).option("basePath", base).option("mode", "FAILFAST").json(f"{base}/dt=*/*.jsonl")
    return df.withColumn("dt", F.col("dt").cast("date")).where(F.col("dt") <= F.lit(run_date).cast("date"))


def city_map(spark: SparkSession, seeds: Path) -> DataFrame:
    schema = T.StructType([T.StructField(n, S) for n in ("raw_city", "city", "region")])
    return spark.read.schema(schema).option("header", True).csv(str(seeds / "city_map.csv"))
