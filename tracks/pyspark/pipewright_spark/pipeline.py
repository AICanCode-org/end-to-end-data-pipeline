"""Build every silver and gold DataFrame for one run date, enforce the gold contract, write and register."""

from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession

from . import bronze, gold, staging

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


def build_gold(silver: dict[str, DataFrame], run_date: str) -> dict[str, DataFrame]:
    """The nine gold DataFrames (two dimensions, two facts, five reports) from the silver ones."""
    orders, products = silver["stg_orders"], silver["stg_products"]
    order_items = gold.fct_order_items(silver["stg_order_items"], orders, products).cache()
    fct_orders = gold.fct_orders(orders.drop("items"), order_items).cache()
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


def build(spark: SparkSession, lake: Path, seeds: Path, run_date: str) -> dict[str, DataFrame]:
    return build_gold(build_silver(spark, lake, seeds, run_date), run_date)


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
    paths = {}
    for name, df in tables.items():
        target = out / name
        enforce(name, df, contract).coalesce(1).write.mode("overwrite").parquet(str(target))
        paths[name] = target
    return paths
