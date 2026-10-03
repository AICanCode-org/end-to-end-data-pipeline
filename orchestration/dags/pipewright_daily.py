"""Pipewright's DAGs: BazaarBox's numbers for yesterday (IST), on the CEO's desk by 06:30 IST (the SLA).

pipewright_daily (06:00 IST):
    check_drops -> ingest_<source> (x5, in parallel) -> transform -> dq -> publish -> monitor (always runs)
pipewright_watch (05:45 and 06:45 IST): `pipewright monitor` alone, so a missing file drop raises an alert before
the daily run starts, and a missed SLA raises one even if the daily run is stuck retrying.

Every task calls the `pipewright` command line (installed in its own virtualenv, see PIPEWRIGHT_BIN), so the DAG
holds no business logic: what runs in Airflow is exactly what `pipewright run` does on a laptop or in CI.

* Schedule: 00:30 UTC = 06:00 IST (CronTriggerTimetable). The business date is the IST day before the run (ds - 1), or the `run_date`
  param of a manual run (a backfill of one day).
* check_drops waits for late file drops by retrying (exit 3 = "not there yet") for up to 3 hours.
* transform is incremental (1.1): it rebuilds the orders updated in the last 3 days on top of the published
  warehouse. A past range is rebuilt with `pipewright backfill --start .. --end ..` (or a manual run with run_date).
* dq exits 1 when a blocking check fails: it writes reports/incident-<date>.md and publish never runs, so the
  dashboard keeps yesterday's data instead of showing wrong numbers.
* monitor (trigger rule all_done) records freshness / volume / SLA / data-quality results in ops.duckdb and raises
  alerts (reports/alerts.jsonl, optional ALERT_WEBHOOK_URL); it fails the task when an error-level alert is raised.
"""

from __future__ import annotations

import datetime as dt
import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

PIPEWRIGHT = os.environ.get("PIPEWRIGHT_BIN", "pipewright")
SOURCES = ("orders", "customers", "payments", "marketing_spend", "products")
RUN_DATE = "{{ params.run_date or macros.ds_add(ds, -1) }}"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": dt.timedelta(minutes=30),
}

with DAG(
    dag_id="pipewright_daily",
    description="BazaarBox daily pipeline: ingest -> transform -> data quality -> publish",
    # A trigger timetable: the logical date (ds) is the moment the run fires, 00:30 UTC = 06:00 IST, so the business
    # date is simply ds - 1. (A plain cron string would give data intervals, and ds would already be the day before.)
    schedule=CronTriggerTimetable("30 0 * * *", timezone="UTC"),
    start_date=dt.datetime(2026, 6, 2, tzinfo=dt.UTC),
    catchup=False,  # backfills are explicit: `pipewright run --start .. --end ..` or a manual run with run_date
    max_active_runs=1,  # the build file and the lake state are shared, so never two runs at once
    default_args=default_args,
    params={"run_date": Param(None, type=["null", "string"], description="business date YYYY-MM-DD (default: yesterday IST)")},
    tags=["pipewright", "bazaarbox"],
    doc_md=__doc__,
) as dag:
    check_drops = BashOperator(
        task_id="check_drops",
        bash_command=f"{PIPEWRIGHT} check-drops --date {RUN_DATE}",
        retries=12,
        retry_delay=dt.timedelta(minutes=15),
        retry_exponential_backoff=False,
    )
    ingests = [
        BashOperator(task_id=f"ingest_{s}", bash_command=f"{PIPEWRIGHT} ingest --source {s} --date {RUN_DATE}") for s in SOURCES
    ]
    transform = BashOperator(task_id="transform", bash_command=f"{PIPEWRIGHT} transform --date {RUN_DATE}")
    quality = BashOperator(task_id="dq", bash_command=f"{PIPEWRIGHT} dq --date {RUN_DATE}", retries=0)
    publish = BashOperator(task_id="publish", bash_command=f"{PIPEWRIGHT} publish", retries=0)
    monitor = BashOperator(
        task_id="monitor", bash_command=f"{PIPEWRIGHT} monitor --date {RUN_DATE}", retries=0, trigger_rule=TriggerRule.ALL_DONE
    )

    check_drops >> ingests >> transform >> quality >> publish >> monitor

with DAG(
    dag_id="pipewright_watch",
    description="Freshness / SLA watch for yesterday's data, independent of the daily run",
    schedule=CronTriggerTimetable("15 0,1 * * *", timezone="UTC"),  # 05:45 and 06:45 IST
    start_date=dt.datetime(2026, 6, 2, tzinfo=dt.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={**default_args, "retries": 0},
    params={"run_date": Param(None, type=["null", "string"], description="business date YYYY-MM-DD (default: yesterday IST)")},
    tags=["pipewright", "bazaarbox", "monitoring"],
) as watch:
    BashOperator(task_id="monitor", bash_command=f"{PIPEWRIGHT} monitor --date {RUN_DATE}")
