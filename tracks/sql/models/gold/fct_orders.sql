-- §4.3 One row per (non-test) order in its latest state, with totals from its kept lines.
-- 1.1: discount_paise and net_paise appended; incremental (orders updated since incremental_since()).
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert', on_schema_change='fail') }}
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
    orders.status in ('placed', 'shipped', 'delivered') as is_revenue,
    orders.discount_paise,
    coalesce(lines.gross_paise, 0) - orders.discount_paise as net_paise
from {{ ref('stg_orders') }} as orders
left join lines on orders.order_id = lines.order_id
{% if is_incremental() %}
    where {{ ist_date('orders.updated_at_utc') }} >= {{ incremental_since() }}
{% endif %}
