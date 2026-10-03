"""Gold: the tables the dashboard reads (contract/transform-spec.md §4-§5)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REVENUE_STATUS = ["placed", "shipped", "delivered"]


def dim_products(products: DataFrame) -> DataFrame:
    """§4.1"""
    return products.select("sku", "product_name", "category", "price_paise", "first_seen_date")


def fct_order_items(items: DataFrame, orders: DataFrame, products: DataFrame) -> DataFrame:
    """§4.2"""
    o = orders.select("order_id", "order_date", "region", "status")
    p = products.select("sku", F.col("category").alias("_category"))
    return (
        items.join(o, "order_id")
        .join(p, "sku", "left")
        .select(
            "order_id",
            "line_number",
            "order_date",
            "sku",
            F.coalesce("_category", F.lit("unknown")).alias("category"),
            "region",
            "status",
            "qty",
            "unit_price_paise",
            (F.col("qty") * F.col("unit_price_paise")).alias("line_paise"),
        )
    )


def fct_orders(orders: DataFrame, order_items: DataFrame) -> DataFrame:
    """§4.3"""
    lines = order_items.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("items_count"), F.sum("qty").alias("units"), F.sum("line_paise").alias("gross_paise")
    )
    return orders.join(lines, "order_id", "left").select(
        "order_id",
        "customer_id",
        "order_date",
        "created_at_utc",
        "updated_at_utc",
        "status",
        "city",
        "region",
        "payment_method",
        F.coalesce("items_count", F.lit(0)).cast("bigint").alias("items_count"),
        F.coalesce("units", F.lit(0)).cast("bigint").alias("units"),
        F.coalesce("gross_paise", F.lit(0)).cast("bigint").alias("gross_paise"),
        F.col("status").isin(REVENUE_STATUS).alias("is_revenue"),
    )


def dim_customers(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """§4.4"""
    valid = (
        orders.where(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(F.min("order_date").alias("first_order_date"), F.count(F.lit(1)).alias("valid_orders"))
    )
    return customers.join(valid, "customer_id", "left").select(
        "customer_id",
        "signup_date",
        "city",
        "region",
        "acquisition_channel",
        "first_order_date",
        F.coalesce("valid_orders", F.lit(0)).cast("bigint").alias("valid_orders"),
    )


def mart_revenue_daily(order_items: DataFrame) -> DataFrame:
    """§5.1 Report 1"""
    return (
        order_items.where(F.col("status").isin(REVENUE_STATUS))
        .groupBy("order_date", "region", "category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("qty").alias("units"),
            F.sum("line_paise").alias("revenue_paise"),
        )
    )


def mart_cac_monthly(spend: DataFrame, customers: DataFrame) -> DataFrame:
    """§5.2 Report 2: CAC in integer paise, rounded down; NULL when nobody was acquired."""
    s = spend.groupBy(F.trunc("spend_date", "month").alias("month"), "channel").agg(F.sum("spend_paise").alias("spend_paise"))
    a = (
        customers.where(F.col("first_order_date").isNotNull())
        .groupBy(F.trunc("first_order_date", "month").alias("month"), F.col("acquisition_channel").alias("channel"))
        .agg(F.count(F.lit(1)).alias("new_customers"))
    )
    j = s.join(a, ["month", "channel"], "full_outer").select(
        "month",
        "channel",
        F.coalesce("spend_paise", F.lit(0)).cast("bigint").alias("spend_paise"),
        F.coalesce("new_customers", F.lit(0)).cast("bigint").alias("new_customers"),
    )
    return j.withColumn("cac_paise", F.when(F.col("new_customers") > 0, F.expr("spend_paise div new_customers")))


def mart_mrr_monthly(payments: DataFrame, run_date: str) -> DataFrame:
    """§5.3 Report 3: Prime MRR at each month end (or run_date for the current month)."""
    rd = F.lit(run_date).cast("date")
    charges = payments.where(F.col("event_type") == "subscription.charged").select(
        "subscription_id",
        "plan",
        "period_start",
        "period_end",
        F.when(F.col("plan") == "prime_annual", F.expr("amount_paise div 12")).otherwise(F.col("amount_paise")).alias("mrr_paise"),
    )
    bounds = charges.agg(F.trunc(F.min("period_start"), "month").alias("first"))
    months = bounds.select(F.explode(F.sequence("first", F.trunc(rd, "month"), F.expr("INTERVAL 1 MONTH"))).alias("month"))
    months = months.withColumn("as_of", F.least(F.last_day("month"), rd))
    active = months.join(charges, (charges.period_start <= months.as_of) & (months.as_of < charges.period_end)).select(
        "month", "subscription_id", "plan", "mrr_paise"
    )
    previous = active.select(F.add_months("month", 1).alias("month"), "subscription_id", "mrr_paise")
    new = (
        active.join(previous.select("month", "subscription_id"), ["month", "subscription_id"], "left_anti")
        .groupBy("month")
        .agg(F.sum("mrr_paise").alias("new_mrr_paise"))
    )
    churned = (
        previous.join(active.select("month", "subscription_id"), ["month", "subscription_id"], "left_anti")
        .groupBy("month")
        .agg(F.sum("mrr_paise").alias("churned_mrr_paise"))
    )
    totals = active.groupBy("month").agg(
        F.count(F.lit(1)).alias("active_subscriptions"),
        F.count(F.when(F.col("plan") == "prime_monthly", 1)).alias("monthly_subscriptions"),
        F.count(F.when(F.col("plan") == "prime_annual", 1)).alias("annual_subscriptions"),
        F.sum("mrr_paise").alias("mrr_paise"),
    )
    out = months.select("month").join(totals, "month", "left").join(new, "month", "left").join(churned, "month", "left")
    zero = F.lit(0)
    return out.select(
        "month",
        *[F.coalesce(c, zero).cast("bigint").alias(c) for c in (
            "active_subscriptions", "monthly_subscriptions", "annual_subscriptions", "mrr_paise", "new_mrr_paise", "churned_mrr_paise",
        )],
    )


def mart_top_products_30d(order_items: DataFrame, products: DataFrame, run_date: str) -> DataFrame:
    """§5.4 Report 4: top 10 products by revenue in the 30 days ending on run_date."""
    rd = F.lit(run_date).cast("date")
    sales = (
        order_items.where(F.col("status").isin(REVENUE_STATUS) & F.col("order_date").between(F.date_sub(rd, 29), rd))
        .groupBy("sku")
        .agg(F.sum("qty").alias("units"), F.sum("line_paise").alias("revenue_paise"))
    )
    ranked = sales.withColumn("rank", F.row_number().over(Window.orderBy(F.col("revenue_paise").desc(), F.col("sku").asc())))
    p = products.select("sku", "product_name", F.col("category").alias("_category"))
    return (
        ranked.where(F.col("rank") <= 10)
        .join(p, "sku", "left")
        .select(
            rd.alias("as_of_date"),
            F.col("rank").cast("int").alias("rank"),
            "sku",
            F.coalesce("product_name", F.lit("Unknown product")).alias("product_name"),
            F.coalesce("_category", F.lit("unknown")).alias("category"),
            "units",
            "revenue_paise",
        )
    )


def mart_cohort_retention(orders: DataFrame, run_date: str) -> DataFrame:
    """§5.5 Report 5: 30-day repeat rate by monthly cohort, mature customers only."""
    rd = F.lit(run_date).cast("date")
    w = Window.partitionBy("customer_id").orderBy("created_at_utc", "order_id")
    valid = orders.where(F.col("status") != "cancelled").withColumn("n", F.row_number().over(w))
    firsts = valid.where("n = 1").select(
        "customer_id", F.col("order_date").alias("first_date"), (F.date_add("order_date", 30) <= rd).alias("is_mature")
    )
    later = valid.where("n > 1").select("customer_id", "order_date")
    retained = (
        firsts.join(later, "customer_id")
        .where(F.col("order_date") <= F.date_add("first_date", 30))
        .select("customer_id")
        .distinct()
        .withColumn("is_retained", F.lit(True))
    )
    j = firsts.join(retained, "customer_id", "left")
    mature = F.col("is_mature")
    agg = j.groupBy(F.trunc("first_date", "month").alias("cohort_month")).agg(
        F.count(F.lit(1)).alias("customers"),
        F.count(F.when(mature, 1)).alias("mature_customers"),
        F.count(F.when(mature & F.col("is_retained"), 1)).alias("retained_30d"),
    )
    return agg.withColumn(
        "retention_rate",
        F.when(F.col("mature_customers") > 0, F.col("retained_30d").cast("double") / F.col("mature_customers")),
    )
