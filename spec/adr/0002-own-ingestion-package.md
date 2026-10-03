# ADR-0002: A small, tested ingestion package instead of a connector platform (Airbyte)

- Status: accepted
- Date: 2026-10-03

## Context
Sources: one REST API (orders, customers; bearer token, cursor pagination, `updated_at` windows, 429/503) and three
daily file drops. Airbyte needs several GB of RAM and around ten containers, and its connectors hide the mechanics
the project must teach and test: windows, cursors, state, retries, idempotent writes, PII handling.

## Options considered
1. Airbyte (open source) with an HTTP connector built in its low-code builder.
2. A generic library (e.g. dlt, Singer taps).
3. `pipewright.ingest`: a few hundred lines of standard-library Python + DuckDB, driven by `data-contract.yaml`.

## Decision
Option 3. One function per concern (`fetch_window`, `conform`, `ingest_api`, `ingest_file`, `validate_file`), an
HTTP client with backoff and `Retry-After`, partition writes by temp-dir + rename, manifests and state files.

## Consequences
- Every behaviour is unit-tested against the offline stub (faults included) and readable by a learner.
- Memory: tens of MB instead of GBs; nothing to operate.
- We own the connector code: a new source means new code and tests, not a click. For dozens of SaaS sources a
  platform (Airbyte, Fivetran, dlt) would win; the design notes this as the scaling path.
