-- One row per customer: the latest profile version (§3.3). PII (email, phone) never reached the lake.
-- TODO(Phase 3 M2): implement stg_customers.
-- §3.3: the latest profile per customer_id, test customers removed, signup_date in IST,
-- city/region from the city map, acquisition_channel lower-cased and trimmed.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
with raw as (
    select *
    from
        read_json(
            '{{ lake_glob("customers", "jsonl") }}',
            format = 'newline_delimited',
            hive_partitioning = true,
            columns = {
                customer_id: 'VARCHAR',
                signup_at: 'VARCHAR',
                acquisition_channel: 'VARCHAR',
                is_test: 'BOOLEAN',
                city: 'VARCHAR',
                updated_at: 'VARCHAR'
            }
        )
    where cast(dt as date) <= {{ run_date() }}
)

select
    cast(null as varchar) as customer_id,
    cast(null as date) as signup_date,
    cast(null as varchar) as city,
    cast(null as varchar) as region,
    cast(null as varchar) as acquisition_channel
from raw
where false
