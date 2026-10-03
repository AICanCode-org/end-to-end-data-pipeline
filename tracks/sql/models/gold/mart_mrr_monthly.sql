-- Report 3 (§5.3): monthly recurring revenue of BazaarBox Prime. A subscription is active at an instant T when a
-- subscription.charged event covers it (period_start <= T < period_end). Each month is measured at its last day,
-- or at run_date for the current month. Annual plans count 1/12 of their price (integer paise, rounded down).
with charges as (
    select
        subscription_id,
        plan,
        period_start,
        period_end,
        case when plan = 'prime_annual' then amount_paise // 12 else amount_paise end as mrr_paise
    from {{ ref('stg_payments') }}
    where event_type = 'subscription.charged'
),

months as (
    select
        cast(series.range as date) as month,
        least(cast(series.range + interval 1 month - interval 1 day as date), {{ run_date() }}) as as_of
    from range(
        (select date_trunc('month', min(period_start)) from charges),
        date_trunc('month', {{ run_date() }}) + interval 1 day,
        interval 1 month
    ) as series
),

active as (
    select
        months.month,
        charges.subscription_id,
        charges.plan,
        charges.mrr_paise
    from months
    inner join charges on charges.period_start <= months.as_of and months.as_of < charges.period_end
),

previous as (
    select
        cast(month + interval 1 month as date) as month,
        subscription_id,
        mrr_paise
    from active
),

new_subs as (
    select
        active.month,
        cast(sum(active.mrr_paise) as bigint) as new_mrr_paise
    from active
    left join previous on active.month = previous.month and active.subscription_id = previous.subscription_id
    where previous.subscription_id is null
    group by active.month
),

churned as (
    select
        previous.month,
        cast(sum(previous.mrr_paise) as bigint) as churned_mrr_paise
    from previous
    left join active on previous.month = active.month and previous.subscription_id = active.subscription_id
    where active.subscription_id is null
    group by previous.month
)

select
    months.month,
    count(active.subscription_id) as active_subscriptions,
    count(active.subscription_id) filter (where active.plan = 'prime_monthly') as monthly_subscriptions,
    count(active.subscription_id) filter (where active.plan = 'prime_annual') as annual_subscriptions,
    cast(coalesce(sum(active.mrr_paise), 0) as bigint) as mrr_paise,
    coalesce(any_value(new_subs.new_mrr_paise), 0) as new_mrr_paise,
    coalesce(any_value(churned.churned_mrr_paise), 0) as churned_mrr_paise
from months
left join active on months.month = active.month
left join new_subs on months.month = new_subs.month
left join churned on months.month = churned.month
group by months.month
