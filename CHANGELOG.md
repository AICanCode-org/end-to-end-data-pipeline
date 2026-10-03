# Changelog

All notable changes to Pipewright are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/):
the **data contract** (`contract/data-contract.yaml`) has its own version, and a change that alters a published
number or a gold column is never a patch release.

## [Unreleased]

## [1.1.0] - 2026-09-25
### Added
- The September data (June to September 2026) and its golden files (`contract/golden/v1.1/`).
- Data contract 1.1.0 (additive): order status `rto`, channel `influencer`, optional order fields `coupon_code` and
  `discount_paise`, optional spend column `campaign_id`, `fct_orders.discount_paise` and `net_paise` (appended),
  the `monitoring` section (freshness, volume, publish SLA, incremental lookback); 1.0.0 kept in
  `contract/history/`. Transformation spec 1.1 with §6 incremental builds.
- Incremental facts on both tracks (orders updated in the last 3 days, plus lines whose category was `unknown`),
  with automatic full refreshes; `scripts/incremental-check.sh` proves incremental = full refresh on every table.
- `pipewright backfill --start --end` (never moves the published date back) and `pipewright monitor`
  (freshness, volume, SLA, DQ history; `ops.duckdb`; `warehouse/reports/alerts.jsonl`; optional webhook whose URL is
  never logged); the Airflow DAG `pipewright_watch` (05:45 and 06:45 IST); in the dashboard, coupon discounts and
  net revenue, and the monitoring history in the Data health tab.
- `scripts/late-file-drill.sh` (21 September replayed), `scripts/backcompat-check.sh`, the `v11` CI job,
  `docs/postmortem-2026-09.md`, ADR-0006, the monitoring section of the runbook.
### Changed
- `rto` orders are excluded like cancelled ones (not revenue, not a valid order).
- The PySpark track reads CSV files with different headers (4- and 5-column spend files) in one run.
### Compatibility
- The 1.1 pipeline on 1.0 data reproduces every 1.0 gold table; the 1.0 dashboard renders a 1.1 warehouse; the 1.0
  pipeline on 1.1 data refuses to publish September and leaves 31 August on the dashboard (`make backcompat`).

## [1.0.0] - 2026-08-31
### Added
- The BazaarBox pipeline for the five CEO reports, contract 1.0.0, in two tracks with identical gold tables:
  **SQL · dbt** (dbt-duckdb: staging, silver, gold, marts, enforced model contracts, data and unit tests) and
  **Python · PySpark** (local mode, explicit schemas, the same spec), proven equal on every gold table
  (`scripts/parity-run.sh`, `contract/golden/v1.0/`).
- `pipewright`, the shared Python package: API ingestion (bearer token, cursor pagination, `updated_since` windows,
  retries with backoff on 429/503), file drops with manifests, PII dropped before the lake, a high-water mark per
  source, atomic partition writes, quarantine of bad rows; the data-quality gate (15 contract checks, error/warn
  severities), atomic publishing only of a checked build, incident notes; one CLI with documented exit codes.
- Requirements, design and ADRs 0001-0005; the transformation spec; the 3-day fixture and golden files.
- Tests: package unit tests (coverage gate 85 %), dbt unit tests and data tests, PySpark unit tests against the
  expected silver and gold files, Airflow DAG tests, dashboard tests.
- CI: path filters, the contract job (fixture, manifests, scripts, CMS export), lint + unit per part, DAG tests,
  parity of both tracks on the full data, dashboard tests, the Docker job; Dependabot.
- v1.0 operations: the Airflow DAG `pipewright_daily` (06:00 IST, retries for late drops, no retries for the gate),
  the Streamlit dashboard (five reports, the data date, a Data health tab), the optional AI explanation of a
  data-quality report (OpenAI-compatible, off by default, guardrails, deterministic template fallback),
  Dockerfiles (non-root uid 10001), `docker-compose.yml` (about 5 GB with everything up), `scripts/smoke-test.sh`,
  `scripts/docker-check.sh`, `docs/runbook.md`.
