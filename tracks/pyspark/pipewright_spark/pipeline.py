"""Build every silver and gold DataFrame for one run date, enforce the gold contract, write and register."""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import bronze, gold, staging
from .parsing import ist_date

SPARK_TYPES = {  # contract (DuckDB) type -> Spark simpleString
    "VARCHAR": "string", "DATE": "date", "BIGINT": "bigint", "INTEGER": "int", "TIMESTAMP": "timestamp_ntz",
    "BOOLEAN": "boolean", "DOUBLE": "double",
}


class ContractError(Exception):
    pass


def build_silver(spark: SparkSession, lake: Path, seeds: Path, run_date: str) -> dict[str, DataFrame]:
    """The six silver (staging) DataFrames; stg_orders keeps its nested `items` for stg_order_items."""
    cities = bronze.city_map(spark, seeds).cache()
    orders = staging.stg_orders(bronze.read(spark, lake, "orders", run_date), cities).cache()
    return {
        "stg_orders": orders,
        "stg_order_items": staging.stg_order_items(orders),
        "stg_customers": staging.stg_customers(bronze.read(spark, lake, "customers", run_date), cities),
        "stg_payments": staging.stg_payments(bronze.read(spark, lake, "payments", run_date)),
        "stg_marketing_spend": staging.stg_marketing_spend(bronze.read(spark, lake, "marketing_spend", run_date), run_date),
        "stg_products": staging.stg_products(bronze.read(spark, lake, "products", run_date)).cache(),
    }


def facts(silver: dict[str, DataFrame], previous: dict[str, DataFrame] | None = None, since: str | None = None) -> tuple[DataFrame, DataFrame]:
    """fct_order_items and fct_orders: a full build, or (1.1) an incremental merge into the previous published facts.

    Incremental, like the dbt models (delete+insert by order_id): recompute the orders updated on or after `since`
    and the lines that were 'unknown' last time, then replace those orders' rows in the previous tables."""
    orders, products = silver["stg_orders"], silver["stg_products"]
    all_items = gold.fct_order_items(silver["stg_order_items"], orders, products)
    if previous is None:
        order_items = all_items
        return order_items, gold.fct_orders(orders.drop("items"), order_items)
    prev_items, prev_orders = previous["fct_order_items"], previous["fct_orders"]
    changed = orders.where(ist_date(F.col("updated_at_utc")) >= F.lit(since).cast("date")).select("order_id")
    unknown = prev_items.where(F.col("category") == "unknown").select("order_id")
    new_items = all_items.join(changed.unionByName(unknown).distinct(), "order_id", "left_semi")
    order_items = prev_items.join(new_items.select("order_id").distinct(), "order_id", "left_anti").unionByName(new_items)
    new_orders = gold.fct_orders(orders.drop("items").join(changed, "order_id", "left_semi"), order_items)
    fct_orders = prev_orders.join(new_orders.select("order_id"), "order_id", "left_anti").unionByName(new_orders)
    return order_items, fct_orders


def build_gold(
    silver: dict[str, DataFrame], run_date: str, previous: dict[str, DataFrame] | None = None, since: str | None = None
) -> dict[str, DataFrame]:
    """The nine gold DataFrames (two dimensions, two facts, five reports) from the silver ones."""
    products = silver["stg_products"]
    order_items, fct_orders = facts(silver, previous, since)
    order_items, fct_orders = order_items.cache(), fct_orders.cache()
    dim_customers = gold.dim_customers(silver["stg_customers"], fct_orders).cache()
    return {
        "dim_customers": dim_customers,
        "dim_products": gold.dim_products(products),
        "fct_orders": fct_orders,
        "fct_order_items": order_items,
        "mart_revenue_daily": gold.mart_revenue_daily(order_items),
        "mart_cac_monthly": gold.mart_cac_monthly(silver["stg_marketing_spend"], dim_customers),
        "mart_mrr_monthly": gold.mart_mrr_monthly(silver["stg_payments"], run_date),
        "mart_top_products_30d": gold.mart_top_products_30d(order_items, products, run_date),
        "mart_cohort_retention": gold.mart_cohort_retention(fct_orders, run_date),
    }


def build(
    spark: SparkSession, lake: Path, seeds: Path, run_date: str, previous: dict[str, DataFrame] | None = None, since: str | None = None
) -> dict[str, DataFrame]:
    return build_gold(build_silver(spark, lake, seeds, run_date), run_date, previous, since)


def read_previous(spark: SparkSession, folder: Path, contract: dict) -> dict[str, DataFrame]:
    """The previous published facts (exported to Parquet by `pipewright transform`), typed as the contract says."""
    out = {}
    for name in ("fct_orders", "fct_order_items"):
        df = spark.read.parquet(str(folder / f"{name}.parquet"))
        out[name] = df.select(*[F.col(c).cast(SPARK_TYPES[t]).alias(c) for c, t in contract["gold"][name]["columns"].items()])
    return out


def load_contract(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def enforce(name: str, df: DataFrame, contract: dict) -> DataFrame:
    """The Spark equivalent of an enforced dbt contract: exact column names, order and types, or fail."""
    want = contract["gold"][name]["columns"]
    got = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    expected = [(col, SPARK_TYPES[typ]) for col, typ in want.items()]
    if [c for c, _ in got] != [c for c, _ in expected]:
        raise ContractError(f"{name}: columns {[c for c, _ in got]} != contract {[c for c, _ in expected]}")
    wrong = [(c, g, e) for (c, g), (_, e) in zip(got, expected, strict=True) if g != e]
    if wrong:
        raise ContractError(f"{name}: wrong types (column, got, contract): {wrong}")
    return df


def write_parquet(tables: dict[str, DataFrame], out: Path, contract: dict) -> dict[str, Path]:
    """Write every table to <name>.next first, then swap the folders: no table is overwritten while another
    one may still read it lazily, and a failed build leaves the previous Parquet intact."""
    staged = {}
    for name, df in tables.items():
        target = out / f"{name}.next"
        enforce(name, df, contract).coalesce(1).write.mode("overwrite").parquet(str(target))
        staged[name] = target
    paths = {}
    for name, target in staged.items():
        final = out / name
        shutil.rmtree(final, ignore_errors=True)
        target.rename(final)
        paths[name] = final
    return paths
