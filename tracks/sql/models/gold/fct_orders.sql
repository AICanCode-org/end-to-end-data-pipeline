-- §4.3 One row per (non-test) order in its latest state, with totals from its kept lines.
with lines as (
    select
        order_id,
        count(*) as items_count,
        cast(sum(qty) as bigint) as units,
        cast(sum(line_paise) as bigint) as gross_paise
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.created_at_utc,
    orders.updated_at_utc,
    orders.status,
    orders.city,
    orders.region,
    orders.payment_method,
    coalesce(lines.items_count, 0) as items_count,
    coalesce(lines.units, 0) as units,
    coalesce(lines.gross_paise, 0) as gross_paise,
    orders.status in ('placed', 'shipped', 'delivered') as is_revenue
from {{ ref('stg_orders') }} as orders
left join lines on orders.order_id = lines.order_id
