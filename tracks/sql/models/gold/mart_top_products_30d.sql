-- Report 4 (§5.4): the 10 products with the most revenue in the 30 days ending on run_date
-- (ties: lower SKU first).
with sales as (
    select
        sku,
        cast(sum(qty) as bigint) as units,
        cast(sum(line_paise) as bigint) as revenue_paise
    from {{ ref('fct_order_items') }}
    where
        status in ('placed', 'shipped', 'delivered')
        and order_date between {{ run_date() }} - interval 29 day and {{ run_date() }}
    group by sku
),

ranked as (
    select
        sales.*,
        cast(row_number() over (order by sales.revenue_paise desc, sales.sku asc) as integer) as rank
    from sales
)

select
    {{ run_date() }} as as_of_date,
    ranked.rank,
    ranked.sku,
    coalesce(products.product_name, 'Unknown product') as product_name,
    coalesce(products.category, 'unknown') as category,
    ranked.units,
    ranked.revenue_paise
from ranked
left join {{ ref('dim_products') }} as products on ranked.sku = products.sku
where ranked.rank <= 10
