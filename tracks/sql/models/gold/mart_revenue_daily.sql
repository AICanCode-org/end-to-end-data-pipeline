-- Report 1 (§5.1): revenue by day, region and product category. Revenue = lines of orders whose latest status is
-- placed, shipped or delivered (cancelled and returned orders earn nothing).
-- TODO(Phase 3 M3): implement mart_revenue_daily.
-- §5.1 Report 1: revenue lines (statuses placed, shipped, delivered) per order_date,
-- region and category: orders (distinct), units, revenue_paise.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as date) as order_date,
    cast(null as varchar) as region,
    cast(null as varchar) as category,
    cast(null as bigint) as orders,
    cast(null as bigint) as units,
    cast(null as bigint) as revenue_paise
where false
