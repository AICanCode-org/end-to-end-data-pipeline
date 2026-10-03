# Requirements: BazaarBox CEO dashboard (Pipewright v1.0)

Status: accepted · Owner: data engineering · Reviewed with: CEO, Head of Marketing, Head of Category, Finance

## 1. Stakeholders and decisions
| Who | Which decision, how often | Which report(s) |
|---|---|---|
| CEO | Where to push the team this week: which region or category is slipping; whether growth is healthy (daily, 07:00 review) | R1, R3, R5 |
| Head of Marketing | Move ad budget between channels at the start of each month; pause a channel whose CAC doubled | R2 |
| Head of Category | Re-order stock and plan promotions for the best sellers (weekly) | R1, R4 |
| Finance | Prime revenue forecast and monthly board numbers | R3 (and R1 for the monthly close) |

## 2. The five reports
All dates are **IST calendar days**; all money is **integer paise** (shown as ₹ with Indian digit grouping);
test orders and test customers never count. The exact rules are in `contract/transform-spec.md` (cited as §).

### R1 Revenue by category and region (`mart_revenue_daily`, §5.1)
- Grain: one row per (order date, region, category).
- Measures: distinct orders, units, revenue = Σ `qty × unit_price` of order lines whose order's **latest** status is
  placed, shipped or delivered. Cancelled and returned orders earn nothing, also retroactively: a cancellation on
  4 June removes revenue from 1 June when the order was placed on 1 June.
- Region from the shipping city via a reviewed mapping (`contract/seeds/city_map.csv`); an unmapped spelling is
  `Unknown` and is watched (warning above 1 % of orders).
- Example (fixture, 2026-06-01): one row per region × category with that day's revenue; the sum over all rows
  equals the revenue lines of `fct_order_items` (reconciliation, NFR-2).

### R2 CAC by channel (`mart_cac_monthly`, §5.2)
- Grain: one row per (month, acquisition channel).
- CAC = the month's ad spend of the channel ÷ the customers whose **first valid order** fell in that month, rounded
  down to the paisa; undefined (NULL) with no new customer. Channels without spend (organic) show 0; referral spend is the referral bonuses paid.
- Decision recorded: a "new customer" is a first *order*, not a signup (a signup who never orders cost money but is
  not acquired revenue; Marketing agreed).

### R3 Prime MRR (`mart_mrr_monthly`, §5.3)
- Grain: one row per month, measured at the month's last day (the run date for the current month).
- A subscription is active while a charge covers it (`period_start <= T < period_end`); an annual plan contributes
  1/12 of its charge, rounded down. Also: active / monthly / annual counts, new and churned MRR.
- Payment events arrive at least once: each event counts once.

### R4 Top 10 products, last 30 days (`mart_top_products_30d`, §5.4)
- The 10 SKUs with the most revenue (R1 rule) from run date − 29 to the run date; ties → the lower SKU.
- A SKU missing from the catalogue still appears (as "Unknown product") rather than silently disappearing.

### R5 30-day cohort retention (`mart_cohort_retention`, §5.5)
- Cohort = month of the first valid order. Retained = another valid order within 30 days of the first.
- Only **mature** customers (first order at least 30 days before the run date) are in the rate; the dashboard
  shows the mature count next to the rate so a young cohort is never read as a bad one.

## 3. User stories and acceptance criteria
**US-1 (CEO)** As the CEO, I want yesterday's five reports on one page by 06:30 IST, so that I can run the 07:00
review from one set of numbers.
- Given the sources delivered by 05:00 IST, when the 06:00 run finishes, then the dashboard shows business date
  D-1 and the run log records a `published` run before 06:30.
- Given any blocking check fails, when the run finishes, then the dashboard still shows the last good business
  date, labelled with that date, and an incident report exists for the failed day.

**US-2 (CEO)** As the CEO, I want cancelled orders removed from revenue, so that revenue is money we keep.
- Given an order placed on 1 June and cancelled on 4 June, when the run for 4 June publishes, then 1 June's revenue
  no longer includes it, and `fct_orders` holds the order once, with status `cancelled`.

**US-3 (Finance)** As Finance, I want each payment event counted once, so that MRR matches the payment provider.
- Given the same `event_id` delivered twice (same day, or the next day's file), when both days are processed, then
  MRR counts it once.

**US-4 (Marketing)** As the Head of Marketing, I want CAC per channel per month that matches the ads export, so
that I can move budget with confidence.
- Given spend written as `"₹6,485.40"` and `"6,110.68"`, when the day is processed, then the spend is 648540 and
  611068 paise exactly.
- Given a month in which a channel acquired no customer, then its CAC is empty, not zero or infinite.

**US-5 (Data engineer on call)** As the on-call engineer, I want a late file to delay the run, not corrupt it.
- Given the payments file for D is missing at 06:00, when the run starts, then the check-drops step fails with exit
  code 3, is retried every 15 minutes for up to 3 hours, and nothing is published for D until the file arrives.

**US-6 (Data engineer on call)** As the on-call engineer, I want a new, unknown value to stop publishing.
- Given orders with a status the contract does not know, when the build is checked, then `status_accepted` fails
  (severity error), the build is not published, and the report names the unexpected value (an aggregate, never a row).

**US-7 (Head of Category)** As the Head of Category, I want the top 10 to include products missing from the
catalogue, so that a catalogue glitch does not hide a best seller.
- Given order lines for a SKU absent from the catalogue, then the SKU can rank, as "Unknown product", and the
  `item_sku_known` warning is raised.

**US-8 (Security)** As the company, I want no customer email or phone stored by the pipeline.
- Given customers with email and phone in the API, when they are ingested, then no lake file, table, log or report
  contains either field.

## 4. Non-functional requirements
| Id | Requirement | Measure | How it is tested |
|---|---|---|---|
| NFR-1 | Freshness | D-1 published by 06:30 IST on ≥ 95 % of days; the dashboard always shows its business date | run log timestamps; from v1.1 a freshness/SLA monitor with alerts |
| NFR-2 | Correctness | every report reconciles to the order lines; gold matches the golden tables exactly | contract checks (`reconcile`, `accepted_values`, `relationship`, ...) before publishing; parity with `contract/golden` in CI |
| NFR-3 | Idempotency, reproducibility | same input → same tables; re-running a day replaces it; two implementations agree row by row | re-run tests; SQL vs PySpark parity on the full history |
| NFR-4 | Privacy | no email/phone anywhere after the API response; prompts contain aggregates only | ingestion tests; explainer guardrail tests |
| NFR-5 | Footprint, cost | runs on an 8 GB laptop and free CI; no paid service or key required | Docker Compose memory budget; CI on `ubuntu-latest` |
| NFR-6 | Operability | one command per task (`pipewright ingest/transform/dq/publish/run`), clear exit codes, logs without secrets | CLI tests; Docker smoke test greps logs for a fake key |

## 5. Data sources
| Source | Delivery | When | What goes wrong |
|---|---|---|---|
| Orders API (`/v1/orders`) | REST, bearer token, cursor pagination, `updated_since/before` windows | always available | status changes re-deliver orders; mixed `Z`/`+05:30`; city spellings; test orders; qty ≤ 0; unknown SKUs; 429/503 |
| Customers API (`/v1/customers`) | same | always | profile versions; email/phone (PII); test accounts |
| Payments drop | `payments/payments_YYYY-MM-DD.jsonl` | by 05:00 IST | duplicates (same or next file); subscriptions, refunds, cancellations |
| Ads spend drop | `marketing_spend/spend_YYYY-MM-DD.csv` | by 05:00 IST | amounts as text with ₹ and commas; corrections re-deliver a day |
| Catalogue drop | `products/products_YYYY-MM-DD.csv` (full snapshot) | by 05:00 IST | category spelling; prices as rupee text; SKUs appear late |

## 6. Out of scope (v1.0)
- Intraday or real-time numbers (the decisions are daily or monthly).
- Forecasts, targets and alerts on business metrics (v1.0 alerts on *data* problems only; business alerts later).
- Customer-level views and exports (privacy; not needed for the five decisions).
- Refund-adjusted revenue (returns are excluded by status; partial refunds are a v2 question for Finance).
- A hosted BI tool (Streamlit on the laptop / one container is enough for one reader; see design).

## 7. Open questions
1. Should revenue be net of coupon discounts once coupons launch? (Marketing plans coupons for September → v1.1.)
2. Is 06:30 still right when a partner file is regularly late? What is the escalation path?
3. Does Finance want MRR in the month's last day or average over the month? (v1.0: last day, agreed for now.)
