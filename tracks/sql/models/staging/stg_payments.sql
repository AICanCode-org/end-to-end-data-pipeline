-- One row per payment event (§3.4). Delivery is at-least-once: the same event_id can appear twice in one file
-- or again in the next day's file, always with identical content, so we keep the first delivery.
-- TODO(Phase 3 M2): implement stg_payments.
-- §3.4: one row per event_id (the drops re-send some events: keep the first delivery),
-- occurred_at in UTC + its IST event_date; subscription periods as DATE.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
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
)

select
    cast(null as varchar) as event_id,
    cast(null as varchar) as event_type,
    cast(null as varchar) as customer_id,
    cast(null as varchar) as order_id,
    cast(null as varchar) as subscription_id,
    cast(null as varchar) as plan,
    cast(null as bigint) as amount_paise,
    cast(null as timestamp) as occurred_at_utc,
    cast(null as date) as event_date,
    cast(null as date) as period_start,
    cast(null as date) as period_end
from raw
where false
