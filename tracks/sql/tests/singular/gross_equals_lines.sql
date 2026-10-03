-- Every order's gross equals the sum of its kept lines.
select orders.order_id, orders.gross_paise, coalesce(sum(items.line_paise), 0) as lines_paise
from {{ ref('fct_orders') }} as orders
left join {{ ref('fct_order_items') }} as items on orders.order_id = items.order_id
group by orders.order_id, orders.gross_paise
having orders.gross_paise != coalesce(sum(items.line_paise), 0)
