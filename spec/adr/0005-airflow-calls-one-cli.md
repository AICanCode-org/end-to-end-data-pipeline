# ADR-0005: Airflow schedules the pipeline by calling one CLI

- Status: accepted
- Date: 2026-10-03

## Context
The pipeline must run daily at 06:00 IST with retries and waiting for late files (Airflow), but also in CI, in tests,
in Docker and on a 4 GB laptop without Airflow. Airflow's dependency pins conflict with dbt's.

## Options considered
1. Pipeline logic inside Airflow operators (PythonOperator importing the code).
2. A dbt-specific integration (e.g. Cosmos) plus custom operators for ingestion.
3. One CLI (`pipewright ingest|check-drops|transform|dq|publish|run`) with clear exit codes; Airflow tasks are
   `BashOperator`s calling it from its own virtualenv.

## Decision
Option 3, with Airflow 3.3 in `standalone` mode (Postgres metadata DB in Docker). Exit codes say what
happened: 3 a drop is missing (retry every 15 min, up to 3 h), 4 the source API failed after its retries, 5 a
contract violation, 6 the transformation failed, 1 a blocking check failed or publishing was refused.

## Consequences
- The same command runs everywhere; the DAG stays small and testable (structure tests without running tasks).
- Airflow and dbt never share an environment: no dependency conflicts.
- Less Airflow-native lineage per model; acceptable for one pipeline. With dozens of dbt models per team, a per-model
  integration would be worth revisiting.
- The dashboard is Streamlit, not Metabase: a hosted BI tool is a separate decision for when there is more than one
  reader (see design §7).
