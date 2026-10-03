"""The DAG loads without import errors and has the agreed shape, schedule and business-date logic."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.group("Phase 3 M4 - runner and orchestration")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def module():
    """Import the DAG file the way the scheduler would (any exception here = an import error in the UI)."""
    spec = importlib.util.spec_from_file_location("pipewright_daily", DAGS / "pipewright_daily.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.dag.validate()
    module.watch.validate()
    return module


@pytest.fixture(scope="module")
def dag(module):
    return module.dag


def test_schedule_is_0600_ist(dag):
    from airflow.timetables.trigger import CronTriggerTimetable

    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert dag.timetable.serialize()["expression"] == "30 0 * * *"
    assert dag.timetable.serialize()["timezone"] == "UTC"
    assert dag.catchup is False and dag.max_active_runs == 1


def test_task_graph(dag):
    ingests = {f"ingest_{s}" for s in ("orders", "customers", "payments", "marketing_spend", "products")}
    assert set(dag.task_dict) == {"check_drops", "transform", "dq", "publish", "monitor", *ingests}
    assert dag.get_task("check_drops").downstream_task_ids == ingests
    for t in ingests:
        assert dag.get_task(t).downstream_task_ids == {"transform"}
    assert dag.get_task("transform").downstream_task_ids == {"dq"}
    assert dag.get_task("dq").downstream_task_ids == {"publish"}
    assert dag.get_task("publish").downstream_task_ids == {"monitor"}


@pytest.mark.group("Phase 7 - monitoring")
def test_monitor_always_runs_and_watch_dag(module):
    """1.1: monitoring runs even when the build failed, and a separate watch DAG checks before and after the run."""
    monitor = module.dag.get_task("monitor")
    assert monitor.trigger_rule == "all_done" and monitor.retries == 0
    assert monitor.bash_command.startswith("pipewright monitor --date ")
    watch = module.watch
    assert watch.timetable.serialize()["expression"] == "15 0,1 * * *"  # 05:45 and 06:45 IST
    assert set(watch.task_dict) == {"monitor"} and watch.catchup is False


def test_quality_gate_is_not_retried(dag):
    assert dag.get_task("dq").retries == 0 and dag.get_task("publish").retries == 0
    assert dag.get_task("check_drops").retries >= 6  # late file drops are waited for


def test_commands_use_the_cli_and_business_date(dag):
    cmd = dag.get_task("ingest_orders").bash_command
    assert cmd.startswith("pipewright ingest --source orders --date ")
    assert "macros.ds_add(ds, -1)" in cmd and "params.run_date" in cmd


def test_business_date_is_yesterday_ist():
    """06:00 IST on 2 June (00:30 UTC) builds 1 June: ds_add(ds, -1)."""
    from airflow.sdk.execution_time import macros

    run = dt.datetime(2026, 6, 2, 0, 30, tzinfo=dt.UTC)
    assert macros.ds_add(run.strftime("%Y-%m-%d"), -1) == "2026-06-01"
