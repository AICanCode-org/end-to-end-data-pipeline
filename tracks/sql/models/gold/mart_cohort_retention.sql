-- Report 5 (§5.5): 30-day repeat rate by monthly cohort. A customer's cohort is the month of their first valid
-- order; they are retained if they placed another valid order within 30 days of it; they count as mature only
-- when those 30 days are over by run_date (immature customers are not counted as lost).
with valid as (
    select
        customer_id,
        order_id,
        order_date,
        created_at_utc,
        row_number() over (partition by customer_id order by created_at_utc, order_id) as n
    from {{ ref('fct_orders') }}
    where status != 'cancelled'
),

firsts as (
    select
        customer_id,
        order_date as first_date,
        first_date + 30 <= {{ run_date() }} as is_mature
    from valid
    where n = 1
),

retained as (
    select distinct firsts.customer_id
    from firsts
    inner join valid on firsts.customer_id = valid.customer_id
    where valid.n > 1 and valid.order_date <= firsts.first_date + 30
)

select
    cast(date_trunc('month', firsts.first_date) as date) as cohort_month,
    count(*) as customers,
    count(*) filter (where firsts.is_mature) as mature_customers,
    count(retained.customer_id) filter (where firsts.is_mature) as retained_30d,
    case
        when count(*) filter (where firsts.is_mature) > 0
            then
                (count(retained.customer_id) filter (where firsts.is_mature))
                / (count(*) filter (where firsts.is_mature))
    end as retention_rate
from firsts
left join retained on firsts.customer_id = retained.customer_id
group by 1
