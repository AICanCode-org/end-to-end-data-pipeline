-- The product catalogue (§3.6): the latest daily snapshot on or before run_date, plus the first date each SKU
-- appeared in any snapshot.
-- TODO(Phase 3 M2): implement stg_products.
-- §3.6: the latest catalogue snapshot on or before run_date + the first date each SKU
-- appeared; category lower-cased with 'é' -> 'e'; price in paise.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
with snapshots as (
    select
        *,
        cast(dt as date) as snapshot_date
    from
        read_csv(
            '{{ lake_glob("products", "csv") }}',
            header = true,
            all_varchar = true,
            hive_partitioning = true,
            union_by_name = true
        )
    where cast(dt as date) <= {{ run_date() }}
)

select
    cast(null as varchar) as sku,
    cast(null as varchar) as product_name,
    cast(null as varchar) as category,
    cast(null as bigint) as price_paise,
    cast(null as date) as first_seen_date
from snapshots
where false
