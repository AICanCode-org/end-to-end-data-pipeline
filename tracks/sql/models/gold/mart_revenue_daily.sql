-- Report 1 (§5.1): revenue by day, region and product category. Revenue = lines of orders whose latest status is
-- placed, shipped or delivered (cancelled and returned orders earn nothing).
select
    order_date,
    region,
    category,
    count(distinct order_id) as orders,
    cast(sum(qty) as bigint) as units,
    cast(sum(line_paise) as bigint) as revenue_paise
from {{ ref('fct_order_items') }}
where status in ('placed', 'shipped', 'delivered')
group by order_date, region, category
