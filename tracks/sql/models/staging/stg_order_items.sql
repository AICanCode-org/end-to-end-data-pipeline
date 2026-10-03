-- One row per order line of the latest order version (§3.2). Lines with a non-positive quantity are dropped;
-- line_number is the 1-based position in the original items array, so it stays stable.
-- TODO(Phase 3 M2): implement stg_order_items.
-- §3.2: one row per element of stg_orders.items (unnest with its position);
-- line_number starts at 1, qty and unit_price_paise as BIGINT.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as varchar) as order_id,
    cast(null as integer) as line_number,
    cast(null as varchar) as sku,
    cast(null as bigint) as qty,
    cast(null as bigint) as unit_price_paise
where false
