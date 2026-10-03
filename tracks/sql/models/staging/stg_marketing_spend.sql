-- One row per (date, channel) of marketing spend (§3.5), money parsed from text to integer paise.
-- If a day's file is delivered again (a correction), the latest delivery wins.
-- TODO(Phase 3 M2): implement stg_marketing_spend.
-- §3.5: spend per (date, channel) in integer paise with to_paise();
-- if a day's file arrives again, the latest delivery (dt) wins.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
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
    cast(null as date) as spend_date,
    cast(null as varchar) as channel,
    cast(null as bigint) as spend_paise
from raw
where false
