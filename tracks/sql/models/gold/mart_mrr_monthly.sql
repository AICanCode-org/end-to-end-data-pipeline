-- Report 3 (§5.3): monthly recurring revenue of BazaarBox Prime. A subscription is active at an instant T when a
-- subscription.charged event covers it (period_start <= T < period_end). Each month is measured at its last day,
-- or at run_date for the current month. Annual plans count 1/12 of their price (integer paise, rounded down).
-- TODO(Phase 3 M3): implement mart_mrr_monthly.
-- §5.3 Report 3: Prime subscriptions active at each month's last day (or run_date for the
-- current month); annual plans count amount // 12; new and churned MRR vs the month before.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as date) as month,
    cast(null as bigint) as active_subscriptions,
    cast(null as bigint) as monthly_subscriptions,
    cast(null as bigint) as annual_subscriptions,
    cast(null as bigint) as mrr_paise,
    cast(null as bigint) as new_mrr_paise,
    cast(null as bigint) as churned_mrr_paise
where false
