-- §4.4 One row per (non-test) customer, with the date of their first valid (not cancelled) order.
with valid as (
    select
        customer_id,
        min(order_date) as first_order_date,
        count(*) as valid_orders
    from {{ ref('fct_orders') }}
    where status != 'cancelled'
    group by customer_id
)

select
    customers.customer_id,
    customers.signup_date,
    customers.city,
    customers.region,
    customers.acquisition_channel,
    valid.first_order_date,
    coalesce(valid.valid_orders, 0) as valid_orders
from {{ ref('stg_customers') }} as customers
left join valid on customers.customer_id = valid.customer_id
