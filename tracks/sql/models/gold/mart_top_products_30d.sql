-- Report 4 (§5.4): the 10 products with the most revenue in the 30 days ending on run_date
-- (ties: lower SKU first).
-- TODO(Phase 3 M3): implement mart_top_products_30d.
-- §5.4 Report 4: the 10 SKUs with the most revenue in the 30 days ending run_date
-- (ties broken by sku), with product name and category.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as date) as as_of_date,
    cast(null as integer) as rank,
    cast(null as varchar) as sku,
    cast(null as varchar) as product_name,
    cast(null as varchar) as category,
    cast(null as bigint) as units,
    cast(null as bigint) as revenue_paise
where false
