-- §4.1 One row per SKU in the latest catalogue snapshot.
select
    sku,
    product_name,
    category,
    price_paise,
    first_seen_date
from {{ ref('stg_products') }}
