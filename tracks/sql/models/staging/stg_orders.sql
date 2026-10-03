-- One row per order: its latest version (contract/transform-spec.md §3.1). An order whose status changes later
-- is extracted again in a later window, so the bronze layer holds several versions of it.
-- TODO(Phase 3 M2): implement stg_orders.
-- transform-spec §3.1: keep the latest version of each order_id (latest updated_at, then latest dt),
-- drop test orders, parse timestamps with utc_timestamp(), order_date = ist_date(created_at_utc),
-- city/region from the city_map seed (unknown spellings -> 'Unknown'). Keep `items` as it is.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
with raw as (
    select *
    from
        read_json(
            '{{ lake_glob("orders", "jsonl") }}',
            format = 'newline_delimited',
            hive_partitioning = true,
            columns = {
                order_id: 'VARCHAR',
                customer_id: 'VARCHAR',
                status: 'VARCHAR',
                created_at: 'VARCHAR',
                updated_at: 'VARCHAR',
                ship_city: 'VARCHAR',
                payment_method: 'VARCHAR',
                is_test: 'BOOLEAN',
                items: 'STRUCT(sku VARCHAR, qty BIGINT, unit_price_paise BIGINT)[]'
            }
        )
    where cast(dt as date) <= {{ run_date() }}
)

select
    cast(null as varchar) as order_id,
    cast(null as varchar) as customer_id,
    cast(null as varchar) as status,
    cast(null as timestamp) as created_at_utc,
    cast(null as timestamp) as updated_at_utc,
    cast(null as date) as order_date,
    cast(null as varchar) as city,
    cast(null as varchar) as region,
    cast(null as varchar) as payment_method,
    cast(null as struct(sku varchar, qty bigint, unit_price_paise bigint)[]) as items
from raw
where false
