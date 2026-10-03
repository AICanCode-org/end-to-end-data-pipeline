-- Report 2 (§5.2): customer acquisition cost by month and channel = spend / customers whose first valid order
-- was in that month, in integer paise rounded down; NULL when the channel acquired nobody that month.
with spend as (
    select
        date_trunc('month', spend_date) as month,
        channel,
        cast(sum(spend_paise) as bigint) as spend_paise
    from {{ ref('stg_marketing_spend') }}
    group by 1, 2
),

acquired as (
    select
        date_trunc('month', first_order_date) as month,
        acquisition_channel as channel,
        count(*) as new_customers
    from {{ ref('dim_customers') }}
    where first_order_date is not null
    group by 1, 2
),

joined as (
    select
        cast(coalesce(spend.month, acquired.month) as date) as month,
        coalesce(spend.channel, acquired.channel) as channel,
        coalesce(spend.spend_paise, 0) as spend_paise,
        coalesce(acquired.new_customers, 0) as new_customers
    from spend
    full outer join acquired on spend.month = acquired.month and spend.channel = acquired.channel
)

select
    month,
    channel,
    spend_paise,
    new_customers,
    case when new_customers > 0 then spend_paise // new_customers end as cac_paise
from joined
