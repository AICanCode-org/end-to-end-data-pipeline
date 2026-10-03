-- §4.1 One row per SKU in the latest catalogue snapshot.
-- TODO(Phase 3 M3): implement dim_products.
-- §4.1: one row per SKU of the latest catalogue.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as varchar) as sku,
    cast(null as varchar) as product_name,
    cast(null as varchar) as category,
    cast(null as bigint) as price_paise,
    cast(null as date) as first_seen_date
where false
