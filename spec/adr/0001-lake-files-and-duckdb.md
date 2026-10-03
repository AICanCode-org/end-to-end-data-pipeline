# ADR-0001: A lake of immutable files and an embedded DuckDB warehouse, not Delta Lake on a Spark cluster

- Status: accepted
- Date: 2026-10-03

## Context
The old design used Delta Lake on a Spark cluster. The requirements ask for a pipeline that runs offline on an
8 GB laptop and in free CI, with reproducible numbers. The data is small (about 20,000 orders over four months),
but the ideas must scale: immutable raw data, partitions, rebuilds, atomic publishing.

## Options considered
1. Delta Lake tables on a Spark cluster (standalone master + workers).
2. Delta Lake with PySpark in local mode.
3. Plain files in a partitioned lake (`lake/raw/<source>/dt=D/`) + one DuckDB file as the warehouse.

## Decision
Option 3. Bronze is a directory of files exactly as delivered, partitioned by delivery date; silver and gold live
in a DuckDB file that is rebuilt and published atomically. PySpark is kept as a transformation **track** (ADR-0003)
that reads the same lake and registers its Parquet output in the same warehouse.

## Consequences
- Runs anywhere with `pip install`; no cluster, no JVM for readers, no jars downloaded at runtime (works offline).
- Atomic publishing is a file rename (ADR-0004) instead of table-format transactions.
- We give up Delta's time travel and concurrent writers; the immutable lake plus rebuilds give reproducibility, and
  there is one writer by design (`max_active_runs=1`).
- At 100x the data, the lake layout stays and the warehouse could move to a table format or a cloud warehouse; the
  contract and the checks do not change.
