"""Pipewright's daily DAG (Phase 3, milestone M4): BazaarBox's numbers for yesterday (IST), every morning.

    check_drops -> ingest_<source> (x5, in parallel) -> transform -> dq -> publish

Every task calls the `pipewright` command line, so the DAG holds no business logic: what runs in Airflow is exactly
what `pipewright run` does on a laptop or in CI. tests/test_dag.py says what "done" means.
"""

from __future__ import annotations

import datetime as dt
import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PIPEWRIGHT = os.environ.get("PIPEWRIGHT_BIN", "pipewright")
SOURCES = ("orders", "customers", "payments", "marketing_spend", "products")
# TODO(M4): the business date: the `run_date` param of a manual run, else the IST day before the run.
#   Hint: with a trigger timetable, the logical date `ds` is the day the run fires, and Airflow has macros.ds_add.
RUN_DATE = "{{ params.run_date }}"

default_args = {
    "owner": "data-platform",
    # TODO(M4): retries with a delay (and backoff) for flaky steps; a time limit per task.
}

with DAG(
    dag_id="pipewright_daily",
    description="BazaarBox daily pipeline: ingest -> transform -> data quality -> publish",
    # TODO(M4): every day at 06:00 IST. Hint: airflow.timetables.trigger.CronTriggerTimetable, cron in UTC.
    schedule=None,
    start_date=dt.datetime(2026, 6, 2, tzinfo=dt.UTC),
    catchup=False,
    # TODO(M4): never two runs at once (the build file and the lake state are shared).
    default_args=default_args,
    params={"run_date": Param(None, type=["null", "string"], description="business date YYYY-MM-DD (default: yesterday IST)")},
    tags=["pipewright", "bazaarbox"],
    doc_md=__doc__,
) as dag:
    check_drops = BashOperator(task_id="check_drops", bash_command=f"{PIPEWRIGHT} check-drops --date {RUN_DATE}")
    # TODO(M4): check_drops exits 3 while a file drop is missing: retry it every 15 minutes for up to 3 hours.
    # TODO(M4): one ingest_<source> task per source, then transform, dq (never retried: a failed check is not
    #           flaky) and publish (never retried either), wired check_drops >> ingests >> transform >> dq >> publish.
