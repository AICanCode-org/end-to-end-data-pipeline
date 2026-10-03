"""Gold: the tables the dashboard reads (contract/transform-spec.md §4-§5)."""

from __future__ import annotations

from pyspark.sql import DataFrame

REVENUE_STATUS = ["placed", "shipped", "delivered"]


def dim_products(products: DataFrame) -> DataFrame:
    """§4.1"""
    # TODO(Phase 3 M3): implement dim_products.
    # §4.1: the latest catalogue, one row per SKU.
    raise NotImplementedError("Phase 3 M3: dim_products (see the TODO above)")


def fct_order_items(items: DataFrame, orders: DataFrame, products: DataFrame) -> DataFrame:
    """§4.2"""
    # TODO(Phase 3 M3): implement fct_order_items.
    # §4.2: one row per order line with order_date, status, region, category; line_paise = qty * unit price.
    raise NotImplementedError("Phase 3 M3: fct_order_items (see the TODO above)")


def fct_orders(orders: DataFrame, order_items: DataFrame) -> DataFrame:
    """§4.3"""
    # TODO(Phase 3 M3): implement fct_orders.
    # §4.3: one row per order with items_count, units, gross_paise and is_revenue.
    raise NotImplementedError("Phase 3 M3: fct_orders (see the TODO above)")


def dim_customers(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """§4.4"""
    # TODO(Phase 3 M3): implement dim_customers.
    # §4.4: the customer profile + first_order_date and valid_orders (status != cancelled).
    raise NotImplementedError("Phase 3 M3: dim_customers (see the TODO above)")


def mart_revenue_daily(order_items: DataFrame) -> DataFrame:
    """§5.1 Report 1"""
    # TODO(Phase 3 M3): implement mart_revenue_daily.
    # §5.1 Report 1: revenue lines (revenue statuses) by order_date, region, category.
    raise NotImplementedError("Phase 3 M3: mart_revenue_daily (see the TODO above)")


def mart_cac_monthly(spend: DataFrame, customers: DataFrame) -> DataFrame:
    """§5.2 Report 2: CAC in integer paise, rounded down; NULL when nobody was acquired."""
    # TODO(Phase 3 M3): implement mart_cac_monthly.
    # §5.2 Report 2: spend / new customers per month and channel, rounded DOWN; NULL without customers.
    raise NotImplementedError("Phase 3 M3: mart_cac_monthly (see the TODO above)")


def mart_mrr_monthly(payments: DataFrame, run_date: str) -> DataFrame:
    """§5.3 Report 3: Prime MRR at each month end (or run_date for the current month)."""
    # TODO(Phase 3 M3): implement mart_mrr_monthly.
    # §5.3 Report 3: active Prime subscriptions at each month end (or run_date); annual = amount div 12.
    raise NotImplementedError("Phase 3 M3: mart_mrr_monthly (see the TODO above)")


def mart_top_products_30d(order_items: DataFrame, products: DataFrame, run_date: str) -> DataFrame:
    """§5.4 Report 4: top 10 products by revenue in the 30 days ending on run_date."""
    # TODO(Phase 3 M3): implement mart_top_products_30d.
    # §5.4 Report 4: top 10 SKUs by revenue in the 30 days ending run_date; ties by sku.
    raise NotImplementedError("Phase 3 M3: mart_top_products_30d (see the TODO above)")


def mart_cohort_retention(orders: DataFrame, run_date: str) -> DataFrame:
    """§5.5 Report 5: 30-day repeat rate by monthly cohort, mature customers only."""
    # TODO(Phase 3 M3): implement mart_cohort_retention.
    # §5.5 Report 5: monthly cohorts by first valid order; retained = another valid order within 30 days; mature only.
    raise NotImplementedError("Phase 3 M3: mart_cohort_retention (see the TODO above)")
