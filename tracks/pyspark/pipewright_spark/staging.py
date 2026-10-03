"""Silver: cleaned, typed, de-duplicated DataFrames (contract/transform-spec.md §3)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def latest(df: DataFrame, key: str, *order: F.Column) -> DataFrame:
    """Keep one row per `key`: the first by `order` (row_number = 1)."""
    # TODO(Phase 3 M2): implement latest.
    # row_number() over (partition by key order by *order) = 1; drop the helper column.
    raise NotImplementedError("Phase 3 M2: latest (see the TODO above)")


def map_city(df: DataFrame, cities: DataFrame, raw_col: str) -> DataFrame:
    # TODO(Phase 3 M2): implement map_city.
    # Left join the city map on clean_key(raw) = raw_city; unmatched -> city 'Unknown', region 'Unknown'.
    raise NotImplementedError("Phase 3 M2: map_city (see the TODO above)")


def stg_orders(raw: DataFrame, cities: DataFrame) -> DataFrame:
    """§3.1 The latest version of every non-test order."""
    # TODO(Phase 3 M2): implement stg_orders.
    # transform-spec §3.1: the latest version per order_id (updated_at, then dt), test orders removed, timestamps
    # in UTC, order_date = IST date of created_at, city/region from the map; keep `items` for stg_order_items.
    raise NotImplementedError("Phase 3 M2: stg_orders (see the TODO above)")


def stg_order_items(orders: DataFrame) -> DataFrame:
    """§3.2 Kept lines (qty > 0) with their 1-based position in the items array."""
    # TODO(Phase 3 M2): implement stg_order_items.
    # §3.2: posexplode the items of stg_orders; line_number starts at 1.
    raise NotImplementedError("Phase 3 M2: stg_order_items (see the TODO above)")


def stg_customers(raw: DataFrame, cities: DataFrame) -> DataFrame:
    """§3.3 The latest profile of every non-test customer."""
    # TODO(Phase 3 M2): implement stg_customers.
    # §3.3: the latest profile per customer_id, no test customers, signup_date in IST.
    raise NotImplementedError("Phase 3 M2: stg_customers (see the TODO above)")


def stg_payments(raw: DataFrame) -> DataFrame:
    """§3.4 One row per event_id (the first delivery; re-deliveries are identical)."""
    # TODO(Phase 3 M2): implement stg_payments.
    # §3.4: one row per event_id (re-deliveries are identical: keep the first).
    raise NotImplementedError("Phase 3 M2: stg_payments (see the TODO above)")


def stg_marketing_spend(raw: DataFrame, run_date: str) -> DataFrame:
    """§3.5 Spend per day and channel in integer paise; the latest delivery of a day wins."""
    # TODO(Phase 3 M2): implement stg_marketing_spend.
    # §3.5: spend in paise per (date, channel); the latest delivery of a day wins.
    raise NotImplementedError("Phase 3 M2: stg_marketing_spend (see the TODO above)")


def stg_products(raw: DataFrame) -> DataFrame:
    """§3.6 The latest catalogue snapshot, with the first date each SKU appeared."""
    # TODO(Phase 3 M2): implement stg_products.
    # §3.6: the latest catalogue snapshot + first_seen_date per SKU (category: lower, 'é' -> 'e').
    raise NotImplementedError("Phase 3 M2: stg_products (see the TODO above)")
