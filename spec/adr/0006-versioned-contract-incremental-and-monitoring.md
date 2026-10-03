# ADR-0006: Versioned contract, incremental facts and monitoring for v1.1

- Status: accepted
- Date: 2026-10-03
- Context document: `docs/postmortem-2026-09.md`

## Context
September brought new values (`influencer`, `rto`), new optional fields (coupons) and a new CSV column
(`campaign_id`), a late file, and a blocked publish that nobody was told about. The v1.0 contract had no way to
change without breaking v1.0 readers, every daily build re-read the whole lake, and nothing watched freshness or the
06:30 SLA.

## Options considered
**Schema evolution.** (a) Loosen the checks (accept any channel or status): never blocks again, but the next unknown
status could silently count as revenue. (b) Version the contract and allow only additive changes in minor versions:
new enum values, optional fields, columns appended at the end; anything else is 2.0.0. (c) A schema registry: too
heavy for five sources and one team.

**Incremental builds.** (a) Keep full rebuilds (about 25 s today, linear in history). (b) dbt `incremental` /
a Spark merge on the facts with a lookback window, full refresh on any doubt, and a check that incremental = full.
(c) Incremental everything, marts included: the 30-day, cohort and month-end marts depend on whole windows, so the
saving is small and the risk large.

**Monitoring.** (a) Airflow e-mail on failure only: misses late files and stale-but-green runs. (b) A `monitor`
command driven by the contract (freshness per source, volume vs the 7-day median, publish SLA, DQ history), run by
its own DAG before and after the deadline, alerts to a log and an optional webhook. (c) A hosted observability tool:
paid, and not offline.

## Decision
Schema (b), incremental (b), monitoring (b):
- Contract **1.1.0**, additive; `contract/history/data-contract-1.0.0.yaml` kept; `fct_orders.discount_paise` and
  `net_paise` appended. `rto` is a final status excluded like `cancelled` (agreed with Finance).
- Facts rebuild orders updated in the last `lookback_days` (3) plus lines whose category was `unknown`; delete +
  insert by `order_id`. Full refresh when there is no published warehouse, the contract version or the track changed,
  or on request. `scripts/incremental-check.sh` must MATCH every gold table on both tracks.
- `pipewright backfill --start --end` re-ingests a range and builds as of the later of its end and the published
  date, so a backfill never moves the dashboard back.
- `pipewright monitor` + DAG `pipewright_watch` at 05:45 (heads-up: drop files due at 05:00) and 06:45 (after the
  06:30 SLA: errors). Results and runs in `warehouse/ops.duckdb`, alerts in `warehouse/reports/alerts.jsonl`, a
  webhook at or above `ALERT_MIN_SEVERITY` whose URL is never logged.

## Consequences
- Backward compatibility is now tested, not hoped for: 1.1 on 1.0 data reproduces golden 1.0; the 1.0 dashboard reads
  a 1.1 warehouse; the 1.0 pipeline on 1.1 data refuses to publish (`make backcompat`, CI job `v11`).
- A status change older than 3 days is missed by the daily build until a backfill or full refresh: documented in
  the runbook; the source delivers status changes within 3 days today. Watch it if that changes.
- More moving parts (ops database, a second DAG, alert settings) and one more thing to keep equal across tracks
  (the incremental merge), covered by the incremental check.
- Next: when history outgrows one DuckDB file, the same rules map to partitioned Parquet or a table format
  (Delta/Iceberg) with MERGE; the contract and the checks do not change.
