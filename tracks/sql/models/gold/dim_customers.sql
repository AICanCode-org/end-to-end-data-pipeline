-- §4.4 One row per (non-test) customer, with the date of their first valid (not cancelled) order.
-- TODO(Phase 3 M3): implement dim_customers.
-- §4.4: every customer + first_order_date and valid_orders (orders not cancelled).
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as varchar) as customer_id,
    cast(null as date) as signup_date,
    cast(null as varchar) as city,
    cast(null as varchar) as region,
    cast(null as varchar) as acquisition_channel,
    cast(null as date) as first_order_date,
    cast(null as bigint) as valid_orders
where false
