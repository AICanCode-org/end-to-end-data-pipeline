# Transformation spec (contract 1.0.0)

The business rules both tracks implement, numbered so code can cite them (`-- §3.1 …`, `"""§4.2"""`). The data
contract (`data-contract.yaml`) says **what** the tables look like; this page says **how** each value is computed.
When a rule here and the code disagree, the code is wrong. Change a rule with a pull request that updates this page,
the contract version, the golden files and both tracks together.

## 1. Conventions
- **Business date** = an IST calendar day (Asia/Kolkata, UTC+05:30, no daylight saving). "Run date" = the last
  business date a build includes; every model ignores lake partitions after it (`dt <= run_date`).
- **Money** is integer **paise** (₹1 = 100 paise) everywhere after bronze. Never a float. Rates (retention) are the
  only doubles.
- **Bronze is immutable**: the lake keeps every delivery exactly as received, partitioned by delivery date
  `dt=YYYY-MM-DD`. Silver and gold are always rebuilt from bronze, so a fix in a rule fixes history too.
- **Test records** (`is_test = true`) are dropped in silver. **PII** (customer email, phone) is dropped at ingestion
  and never reaches the lake.

## 2. Parsing (macros `utc_timestamp`, `ist_date`, `to_paise`; PySpark `parsing.py`)
- `utc_timestamp(text)`: ISO-8601 with an offset, `YYYY-MM-DDTHH:MM:SS` followed by `Z` or `±HH:MM`, to a UTC
  timestamp without time zone. Parse the offset by hand (do not rely on the session time zone): subtract it.
  `2026-06-01T09:15:00+05:30` → `2026-06-01 03:45:00`; `2026-06-01T03:45:00Z` → the same.
- `ist_date(ts)`: the IST calendar date of a UTC timestamp: `date(ts + 5 h 30 min)`.
  `2026-06-01 19:00:00` UTC → `2026-06-02`.
- `to_paise(text)`: rupees written as text to integer paise, exactly: strip `₹`, thousands separators (`,`) and
  spaces, then whole part × 100 + the fraction padded to two digits. `₹12,345.50` → 1234550; `12,345.5` → 1234550;
  `499` → 49900.
- Text keys are compared after `lower(trim(x))`.

## 3. Silver (staging): one clean row per business entity
### 3.1 `stg_orders`
The API is extracted by `updated_at` windows, so an order whose status changes later is delivered again: bronze
holds several versions. Keep **the latest version** per `order_id`: highest `updated_at` (as UTC), then the latest
`dt`. Drop test orders. `status` and `payment_method` lower-cased and trimmed; `created_at_utc`, `updated_at_utc`
via §2; `order_date = ist_date(created_at_utc)`. `city`, `region` from the `city_map` seed on
`lower(trim(ship_city))`; a spelling not in the seed → `'Unknown'` for both. Keep the nested `items` array for §3.2.

### 3.2 `stg_order_items`
One row per element of the latest order version's `items`, **only if `qty > 0`** (zero and negative quantities are
data-entry errors). `line_number` = the element's 1-based position in the original array (stable even when an
earlier line is dropped). `qty`, `unit_price_paise` as BIGINT.

### 3.3 `stg_customers`
The latest profile per `customer_id` (highest `updated_at`, then latest `dt`); test customers dropped.
`signup_date = ist_date(utc_timestamp(signup_at))`; `city`/`region` via the city map as in §3.1;
`acquisition_channel` lower-cased and trimmed.

### 3.4 `stg_payments`
Payment events arrive **at least once**: the same `event_id` can appear twice in one file or again the next day,
always with identical content. Keep one row per `event_id` (the first delivery). `event_type` = `type`;
`occurred_at_utc` via §2 and `event_date` its IST date; `period_start`/`period_end` are dates (subscriptions only).

### 3.5 `stg_marketing_spend`
One row per (`spend_date`, `channel`): `spend_paise = to_paise(spend)`, channel lower-cased and trimmed. If a day's
file is delivered again (a correction), the latest delivery (`dt`) wins. Ignore rows dated after the run date.

### 3.6 `stg_products`
The catalogue is a daily full snapshot. Take the latest snapshot on or before the run date; `product_name` trimmed;
`category` lower-cased, trimmed and `é` → `e` (`Décor` → `decor`); `price_paise = to_paise(price_inr)`;
`first_seen_date` = the first snapshot date in which the SKU appeared.

## 4. Gold: dimensions and facts
### 4.1 `dim_products`
One row per SKU of the latest snapshot (§3.6): `sku, product_name, category, price_paise, first_seen_date`.

### 4.2 `fct_order_items`
One row per kept order line (§3.2) with the order's `order_date`, `region`, `status` and the product's `category`
(`'unknown'` when the SKU is not in the catalogue: a warning, not an error, see the `item_sku_known` check).
`line_paise = qty × unit_price_paise`.

### 4.3 `fct_orders`
One row per (non-test) order in its latest state: the §3.1 columns plus `items_count`, `units`, `gross_paise` from
its kept lines (0 when it has none) and `is_revenue = status ∈ {placed, shipped, delivered}`.

### 4.4 `dim_customers`
Every (non-test) customer (§3.3) with `first_order_date` = the `order_date` of their first **valid** order and
`valid_orders` = how many they have. A valid order is any order that is not `cancelled`. No valid order →
`first_order_date` NULL, `valid_orders` 0.

## 5. Gold: the CEO's five reports
### 5.1 `mart_revenue_daily`: revenue by category and region
Per (`order_date`, `region`, `category`): `orders` (distinct orders), `units`, `revenue_paise` = sum of
`line_paise` of lines whose order's latest status is placed, shipped or delivered. Cancelled and returned orders earn
nothing; a later cancellation removes revenue from the day the order was placed.

### 5.2 `mart_cac_monthly`: customer acquisition cost by channel
Per (`month`, `channel`): `spend_paise` = the month's spend; `new_customers` = customers whose first valid order
(§4.4) falls in that month, by their `acquisition_channel`; `cac_paise = spend_paise // new_customers` (integer
division, rounded down), NULL when `new_customers` = 0. A channel with customers but no spend (organic)
appears with `spend_paise` 0 and `cac_paise` 0.

### 5.3 `mart_mrr_monthly`: Prime monthly recurring revenue
A subscription is **active at instant T** when a `subscription.charged` event covers it:
`period_start <= T < period_end`. Each month is measured at its last day, or at the run date for the current month.
MRR of a charge = `amount_paise` (monthly plan) or `amount_paise // 12` (annual plan). Per month:
`active_subscriptions`, `monthly_subscriptions`, `annual_subscriptions`, `mrr_paise` (sum), `new_mrr_paise`
(subscriptions active this month but not the previous one) and `churned_mrr_paise` (active the previous month, not
this one), from the first month with a charge to the run date's month.

### 5.4 `mart_top_products_30d`: top 10 products
The 10 SKUs with the most revenue (§5.1 rule) in the 30 days ending on the run date (`run_date - 29 … run_date`),
ties broken by the lower SKU: `as_of_date, rank (1..10), sku, product_name, category, units, revenue_paise`. A SKU
missing from the catalogue shows `Unknown product` / `unknown`.

### 5.5 `mart_cohort_retention`: 30-day repeat rate
A customer's cohort is the month of their first valid order (ordered by `created_at_utc`, then `order_id`). They are
**retained** if another valid order has `order_date <= first order_date + 30`. They are **mature** only when
`first order_date + 30 <= run_date` (immature customers are neither retained nor lost yet). Per cohort month:
`customers`, `mature_customers`, `retained_30d` (mature and retained), `retention_rate = retained_30d /
mature_customers` (a double; NULL with no mature customer).
