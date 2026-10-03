-- §4.3 One row per (non-test) order in its latest state, with totals from its kept lines.
-- TODO(Phase 3 M3): implement fct_orders.
-- §4.3: one row per order with items_count, units, gross_paise (sum of its lines) and is_revenue.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as varchar) as order_id,
    cast(null as varchar) as customer_id,
    cast(null as date) as order_date,
    cast(null as timestamp) as created_at_utc,
    cast(null as timestamp) as updated_at_utc,
    cast(null as varchar) as status,
    cast(null as varchar) as city,
    cast(null as varchar) as region,
    cast(null as varchar) as payment_method,
    cast(null as bigint) as items_count,
    cast(null as bigint) as units,
    cast(null as bigint) as gross_paise,
    cast(null as boolean) as is_revenue
where false
