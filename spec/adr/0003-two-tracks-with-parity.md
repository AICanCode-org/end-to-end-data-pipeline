# ADR-0003: Two transformation tracks (dbt SQL and PySpark) that must agree row by row

- Status: accepted
- Date: 2026-10-03

## Context
Learners come with SQL or with Python. At work, the same choice is real: dbt on a warehouse or Spark jobs. A second
implementation is also the strongest test of the spec: if two independent implementations agree on every row, the
rules are unambiguous and both are probably right.

## Options considered
1. One track (dbt only).
2. Two tracks with "similar" results.
3. Two tracks, shared everything else (ingestion, checks, DAG, dashboard), and **exact parity** of every gold table.
4. A third track (pandas/Polars, or a TypeScript service).

## Decision
Option 3. `scripts/parity.py` exports every gold table to canonical CSV (sorted by key, fixed formats); integers and
strings must match exactly, doubles to 1e-9. Parity is checked on the fixture (each track vs `contract/golden/fixture`)
and on the full history (each track vs `contract/golden/v1.x`, and the tracks against each other) in CI.

## Consequences
- A rule change touches both tracks, the spec and the golden files in one PR (enforced by CI).
- No third track: pandas/Polars would be a third copy of the same transformations at a size where SQL vs Spark is the
  relevant real-world choice; a non-Python ingestion service would duplicate shared code without a new idea.
- PySpark needs Java; the SQL track stays the recommended, lighter path.
