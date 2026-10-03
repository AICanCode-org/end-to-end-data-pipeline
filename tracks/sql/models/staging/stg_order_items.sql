-- One row per order line of the latest order version (§3.2). Lines with a non-positive quantity are dropped;
-- line_number is the 1-based position in the original items array, so it stays stable.
with lines as (
    select
        order_id,
        unnest(generate_series(1, len(items))) as line_number,
        unnest(items) as item
    from {{ ref('stg_orders') }}
)

select
    order_id,
    cast(line_number as integer) as line_number,
    item.sku,
    item.qty,
    item.unit_price_paise
from lines
where item.qty > 0
