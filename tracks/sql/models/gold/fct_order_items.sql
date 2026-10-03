-- §4.2 One row per kept order line, with the order's date, region and status and the product's category
-- ('unknown' for a SKU that is not in the catalogue).
-- 1.1: incremental. Each run rebuilds the lines of orders updated since incremental_since(), plus the lines that
-- were 'unknown' last time (a late product in the catalogue fixes them: a late-arriving dimension).
-- A full refresh (`pipewright transform --full-refresh`) must give exactly the same table.
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert', on_schema_change='fail') }}
select
    items.order_id,
    items.line_number,
    orders.order_date,
    items.sku,
    coalesce(products.category, 'unknown') as category,
    orders.region,
    orders.status,
    items.qty,
    items.unit_price_paise,
    items.qty * items.unit_price_paise as line_paise
from {{ ref('stg_order_items') }} as items
inner join {{ ref('stg_orders') }} as orders on items.order_id = orders.order_id
left join {{ ref('stg_products') }} as products on items.sku = products.sku
{% if is_incremental() %}
    where
        {{ ist_date('orders.updated_at_utc') }} >= {{ incremental_since() }}
        or items.order_id in (select previous.order_id from {{ this }} as previous where previous.category = 'unknown')
{% endif %}
