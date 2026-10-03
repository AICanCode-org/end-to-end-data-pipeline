-- The product catalogue (§3.6): the latest daily snapshot on or before run_date, plus the first date each SKU
-- appeared in any snapshot.
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
),

first_seen as (
    select
        sku,
        min(snapshot_date) as first_seen_date
    from snapshots
    group by sku
),

latest_day as (
    select max(snapshot_date) as snapshot_date
    from snapshots
)

select
    snapshots.sku,
    trim(snapshots.name) as product_name,
    translate(lower(trim(snapshots.category)), 'é', 'e') as category,
    {{ to_paise('snapshots.price_inr') }} as price_paise,
    first_seen.first_seen_date
from snapshots
inner join latest_day on snapshots.snapshot_date = latest_day.snapshot_date
inner join first_seen on snapshots.sku = first_seen.sku
