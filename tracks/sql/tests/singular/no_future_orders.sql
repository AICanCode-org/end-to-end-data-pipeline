-- Nothing after run_date may leak into a build for run_date (backfills rebuild the past exactly).
select order_id, order_date from {{ ref('fct_orders') }} where order_date > {{ run_date() }}
