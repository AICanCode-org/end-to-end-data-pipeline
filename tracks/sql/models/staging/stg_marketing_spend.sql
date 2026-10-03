-- One row per (date, channel) of marketing spend (§3.5), money parsed from text to integer paise.
-- If a day's file is delivered again (a correction), the latest delivery wins.
with raw as (
    select *
    from
        read_csv(
            '{{ lake_glob("marketing_spend", "csv") }}',
            header = true,
            all_varchar = true,
            hive_partitioning = true,
            union_by_name = true
        )
    where cast(dt as date) <= {{ run_date() }}
)

select
    cast(date as date) as spend_date,
    lower(trim(channel)) as channel,
    {{ to_paise('spend') }} as spend_paise
from raw
where cast(date as date) <= {{ run_date() }}
qualify row_number() over (partition by cast(date as date), lower(trim(channel)) order by cast(dt as date) desc) = 1
