-- One row per order: its latest version (contract/transform-spec.md §3.1). An order whose status changes later
-- is extracted again in a later window, so the bronze layer holds several versions of it.
-- 1.1: coupon_code / discount_paise are optional (sent from 2026-09-15); absent = no coupon, discount 0.
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
                items: 'STRUCT(sku VARCHAR, qty BIGINT, unit_price_paise BIGINT)[]',
                coupon_code: 'VARCHAR',
                discount_paise: 'BIGINT'
            }
        )
    where cast(dt as date) <= {{ run_date() }}
),

parsed as (
    select
        *,
        cast(dt as date) as extract_date,
        {{ utc_timestamp('created_at') }} as created_at_utc,
        {{ utc_timestamp('updated_at') }} as updated_at_utc
    from raw
),

latest as (
    select *
    from parsed
    qualify row_number() over (partition by order_id order by updated_at_utc desc, extract_date desc) = 1
)

select
    latest.order_id,
    latest.customer_id,
    lower(trim(latest.status)) as status,
    latest.created_at_utc,
    latest.updated_at_utc,
    {{ ist_date('latest.created_at_utc') }} as order_date,
    coalesce(city_map.city, 'Unknown') as city,
    coalesce(city_map.region, 'Unknown') as region,
    lower(trim(latest.payment_method)) as payment_method,
    latest.items,
    latest.coupon_code,
    coalesce(latest.discount_paise, 0) as discount_paise
from latest
left join {{ ref('city_map') }} as city_map on lower(trim(latest.ship_city)) = city_map.raw_city
where not latest.is_test
