-- One row per payment event (§3.4). Delivery is at-least-once: the same event_id can appear twice in one file
-- or again in the next day's file, always with identical content, so we keep the first delivery.
with raw as (
    select *
    from
        read_json(
            '{{ lake_glob("payments", "jsonl") }}',
            format = 'newline_delimited',
            hive_partitioning = true,
            columns = {
                event_id: 'VARCHAR',
                type: 'VARCHAR',
                customer_id: 'VARCHAR',
                order_id: 'VARCHAR',
                subscription_id: 'VARCHAR',
                plan: 'VARCHAR',
                amount_paise: 'BIGINT',
                occurred_at: 'VARCHAR',
                period_start: 'DATE',
                period_end: 'DATE'
            }
        )
    where cast(dt as date) <= {{ run_date() }}
),

deduped as (
    select *
    from raw
    qualify row_number() over (partition by event_id order by cast(dt as date)) = 1
)

select
    event_id,
    type as event_type,
    customer_id,
    order_id,
    subscription_id,
    plan,
    amount_paise,
    {{ utc_timestamp('occurred_at') }} as occurred_at_utc,
    {{ ist_date(utc_timestamp('occurred_at')) }} as event_date,
    period_start,
    period_end
from deduped
