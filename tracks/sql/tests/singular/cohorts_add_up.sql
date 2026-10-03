-- Every customer with a valid order belongs to exactly one cohort.
with cohorts as (select coalesce(sum(customers), 0) as n from {{ ref('mart_cohort_retention') }}),

customers as (select count(*) as n from {{ ref('dim_customers') }} where first_order_date is not null)

select cohorts.n as in_cohorts, customers.n as with_orders from cohorts cross join customers
where cohorts.n != customers.n
