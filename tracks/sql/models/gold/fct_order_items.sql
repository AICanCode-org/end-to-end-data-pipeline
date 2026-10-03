-- §4.2 One row per kept order line, with the order's date, region and status and the product's category
-- ('unknown' for a SKU that is not in the catalogue).
-- TODO(Phase 3 M3): implement fct_order_items.
-- §4.2: one row per order line: order_date, sku, category (unknown SKU -> 'unknown'),
-- region, status, qty, unit_price_paise and line_paise = qty * unit_price_paise.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as varchar) as order_id,
    cast(null as integer) as line_number,
    cast(null as date) as order_date,
    cast(null as varchar) as sku,
    cast(null as varchar) as category,
    cast(null as varchar) as region,
    cast(null as varchar) as status,
    cast(null as bigint) as qty,
    cast(null as bigint) as unit_price_paise,
    cast(null as bigint) as line_paise
where false
