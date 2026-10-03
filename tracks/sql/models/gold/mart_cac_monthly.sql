-- Report 2 (§5.2): customer acquisition cost by month and channel = spend / customers whose first valid order
-- was in that month, in integer paise rounded down; NULL when the channel acquired nobody that month.
-- TODO(Phase 3 M3): implement mart_cac_monthly.
-- §5.2 Report 2: spend and new customers (by first valid order month) per month and
-- channel; cac_paise = spend // new_customers, NULL when nobody was acquired.
-- Until then this stub returns no rows, with exactly the columns and types the next models expect.
select
    cast(null as date) as month,
    cast(null as varchar) as channel,
    cast(null as bigint) as spend_paise,
    cast(null as bigint) as new_customers,
    cast(null as bigint) as cac_paise
where false
