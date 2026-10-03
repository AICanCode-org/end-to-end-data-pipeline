# Changelog

All notable changes to Pipewright are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/):
the **data contract** (`contract/data-contract.yaml`) has its own version, and a change that alters a published
number or a gold column is never a patch release.

## [Unreleased]

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
