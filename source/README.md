# source/: BazaarBox's systems, offline

`bazaarbox_source` is a seeded generator (standard library only, seed `20260601`) and a small HTTP server that
plays BazaarBox's orders & customers API. Nothing leaves your machine.

```bash
cd source
python -m bazaarbox_source generate --out ../source-data --check ../contract/source-manifest-1.0.json
python -m bazaarbox_source serve --data ../source-data --port 18140      # SOURCE_NOW=follow for realistic windows
```

## Output of `generate`
```text
source-data/
├── api/orders.jsonl                       one line per order with its full status history (served by the API)
├── api/customers.jsonl                    one line per customer with its profile versions (served by the API)
├── drops/payments/payments_YYYY-MM-DD.jsonl     payment events ("Stripe-like"), delivered at least once
├── drops/marketing_spend/spend_YYYY-MM-DD.csv   the ads export: date, channel, spend (text!), currency
├── drops/products/products_YYYY-MM-DD.csv       the full catalogue every day: sku, name, category, price_inr, active
├── late/...                               v1.1 only: files that arrive late in the Phase 7 drill
└── MANIFEST.json                          sha256 of every file (compared with contract/source-manifest-*.json)
```
`--version 1.0` covers 2026-06-01..2026-08-31; `--version 1.1` adds September (coupons, a new order status `rto`,
a new channel `influencer`, a `campaign_id` column in the spend file, one late file). The June-August data is
identical in both versions.

## The API
All responses are JSON; errors are RFC 9457 `application/problem+json`.

| Endpoint | Parameters |
|---|---|
| `GET /health` | none, no auth |
| `GET /v1/orders` | `updated_since`, `updated_before` (ISO-8601 instants, half-open window), `limit` (1..500), `cursor` |
| `GET /v1/customers` | same |

- **Auth:** `Authorization: Bearer <token>`; the token comes from `SOURCE_API_TOKEN` (default
  `bazaarbox-dev-token`, a public local value, not a secret). Without it: 401.
- **Pagination:** results are ordered by (`updated_at` instant, id); follow `next_cursor` until it is `null`.
- **The source clock:** `SOURCE_NOW` unset = the final state of everything; `follow` = each request sees the world
  as of its `updated_before`, so an order that is delivered three days later shows up again in a later window,
  exactly the case incremental extraction must handle; an ISO instant freezes the clock there.
- **Faults:** `SOURCE_FAULT_RATE=0.2` makes 20 % of `/v1` calls answer 429 (`Retry-After: 1`) or 503 from a seeded
  random stream (`SOURCE_FAULT_SEED`), so retries can be tested reproducibly.

Records carry `updated_at` (the latest status change), times as ISO-8601 strings with either `Z` or `+05:30`
(both on purpose), and customers carry `email` and `phone`, which the contract tells ingestion to drop.
