-- §4.2 One row per kept order line, with the order's date, region and status and the product's category
-- ('unknown' for a SKU that is not in the catalogue).
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
