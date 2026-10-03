-- Report 5 (§5.5): 30-day repeat rate by monthly cohort. A customer's cohort is the month of their first valid
-- order; they are retained if they placed another valid order within 30 days of it; they count as mature only
-- when those 30 days are over by run_date (immature customers are not counted as lost).
-- TODO(Phase 3 M3): implement mart_cohort_retention.
-- §5.5 Report 5: cohorts by month of first valid order; mature = first order 30+ days
-- before run_date; retained = another valid order within 30 days; rate = retained / mature.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as date) as cohort_month,
    cast(null as bigint) as customers,
    cast(null as bigint) as mature_customers,
    cast(null as bigint) as retained_30d,
    cast(null as double) as retention_rate
where false
