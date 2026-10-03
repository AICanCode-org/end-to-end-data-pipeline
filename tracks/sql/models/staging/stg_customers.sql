-- One row per customer: the latest profile version (§3.3). PII (email, phone) never reached the lake.
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
),

latest as (
    select
        *,
        {{ utc_timestamp('signup_at') }} as signup_at_utc
    from raw
    qualify row_number() over (
        partition by customer_id order by {{ utc_timestamp('updated_at') }} desc, cast(dt as date) desc
    ) = 1
)

select
    latest.customer_id,
    {{ ist_date('latest.signup_at_utc') }} as signup_date,
    coalesce(city_map.city, 'Unknown') as city,
    coalesce(city_map.region, 'Unknown') as region,
    lower(trim(latest.acquisition_channel)) as acquisition_channel
from latest
left join {{ ref('city_map') }} as city_map on lower(trim(latest.city)) = city_map.raw_city
where not latest.is_test
