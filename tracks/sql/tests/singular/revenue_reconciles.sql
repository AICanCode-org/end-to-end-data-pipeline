-- Report 1 must add up to exactly the revenue lines of fct_order_items: no line lost or counted twice.
with mart as (select coalesce(sum(revenue_paise), 0) as total from {{ ref('mart_revenue_daily') }}),

lines as (
    select coalesce(sum(line_paise), 0) as total
    from {{ ref('fct_order_items') }}
    where status in ('placed', 'shipped', 'delivered')
)

select mart.total as mart_total, lines.total as lines_total
from mart cross join lines
where mart.total != lines.total
