"""1.1 incremental builds and backfills: when a build is incremental, what it starts from, how the runner calls it."""

from __future__ import annotations

import datetime as dt
import json

import duckdb
import pytest

from pipewright import cli, config, monitor, runner, transform, warehouse

from .conftest import DAYS
from .helpers import golden_warehouse

pytestmark = pytest.mark.group("Phase 7 - incremental builds")


def published(settings, day=DAYS[-1], version="1.1.0", track=None):
    golden_warehouse(settings.warehouse)
    con = duckdb.connect(str(settings.warehouse))
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE OR REPLACE TABLE ops.build_info AS SELECT ?::DATE AS run_date, ? AS track, ? AS contract_version, true AS publishable",
                [day, track or settings.track, version])
    con.close()


def with_track(settings, track):
    return type(settings)(**{**settings.__dict__, "track": track})


def test_plan(settings):
    day = dt.date(2026, 6, 10)
    assert transform.plan(settings, day).reason == "no published warehouse yet"
    published(settings, version="1.0.0")
    p = transform.plan(settings, day)
    assert not p.incremental and p.reason == "contract changed 1.0.0 -> 1.1.0"
    published(settings, track="pyspark")
    assert "built by the pyspark track" in transform.plan(settings, day).reason
    published(settings)
    p = transform.plan(settings, day)
    assert p.incremental and p.since == dt.date(2026, 6, 7)  # lookback_days: 3 (contract monitoring.incremental)
    assert transform.plan(settings, day, since=dt.date(2026, 6, 1)).since == dt.date(2026, 6, 1)
    assert transform.plan(settings, day, full_refresh=True).reason == "full refresh requested"


def test_commands_carry_the_plan(settings):
    day = DAYS[-1]
    sql = with_track(settings, "sql")
    inc = transform.Plan(True, dt.date(2026, 6, 1), "orders updated since 2026-06-01")
    cmd, _, _ = transform.command(sql, day, inc)
    dbt_vars = json.loads(cmd[cmd.index("--vars") + 1])
    assert dbt_vars == {"run_date": "2026-06-03", "lookback_days": 3, "since": "2026-06-01"} and "--full-refresh" not in cmd
    cmd, _, _ = transform.command(sql, day, transform.Plan(False, None, "full refresh requested"))
    assert cmd[-1] == "--full-refresh"
    spark = with_track(settings, "pyspark")
    cmd, _, _ = transform.command(spark, day, inc)
    assert cmd[-4:] == ["--previous", str(transform.previous_dir(spark)), "--since", "2026-06-01"]


def test_an_incremental_build_starts_from_the_published_warehouse_unchecked(settings):
    spark = with_track(settings, "pyspark")
    published(spark)
    transform._prepare(spark, transform.plan(spark, DAYS[-1]))
    con = duckdb.connect(str(spark.build_warehouse), read_only=True)
    assert con.execute("SELECT count(*) FROM gold.fct_orders").fetchone()[0] > 0
    assert con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'build_info'").fetchone()[0] == 0
    con.close()
    assert {p.name for p in transform.previous_dir(spark).iterdir()} == {"fct_orders.parquet", "fct_order_items.parquet"}
    with pytest.raises(warehouse.PublishBlocked, match="not been checked"):
        warehouse.publish(spark)  # the copy must pass dq again before it can be published
    transform._prepare(spark, transform.Plan(False, None, "full refresh requested"))
    assert not spark.build_warehouse.exists()


@pytest.fixture
def env(settings, monkeypatch):
    for k, v in {"PIPEWRIGHT_HOME": str(settings.home), "SOURCE_API_URL": settings.source_url,
                 "PIPEWRIGHT_DROPS_DIR": str(settings.drops_dir), "PIPEWRIGHT_LAKE_DIR": str(settings.lake_dir),
                 "PIPEWRIGHT_WAREHOUSE": str(settings.warehouse), "PIPEWRIGHT_TRACK": "pyspark"}.items():
        monkeypatch.setenv(k, v)
    return config.load()


@pytest.fixture
def calls(monkeypatch):
    seen = []

    def fake_run(settings, day, since=None, full_refresh=False):
        seen.append((day, since, full_refresh))
        return golden_warehouse(settings.build_warehouse)

    monkeypatch.setattr(transform, "run", fake_run)
    return seen


def test_backfill_rebuilds_from_its_first_day_and_is_logged(env, calls):
    assert cli.main(["backfill", "--start", "2026-06-01", "--end", "2026-06-03", "--full-refresh"]) == 0
    assert calls == [(DAYS[-1], dt.date(2026, 5, 31), True)]  # min(first day, last day - lookback 3)
    assert monitor.read_ops(env, "SELECT kind, status FROM run_log") == [("backfill", "published")]
    assert json.loads((env.reports_dir / "last-run.json").read_text())["kind"] == "backfill"
    with pytest.raises(SystemExit):
        cli.main(["backfill", "--date", "2026-06-01"])


def test_a_short_range_still_uses_the_lookback(env, calls):
    runner.run_range(env, DAYS[-1:], lambda _: None)
    assert calls == [(DAYS[-1], DAYS[-1] - dt.timedelta(days=3), False)]


def test_failed_and_blocked_runs_are_logged(env, monkeypatch):
    def boom(*_a, **_k):
        raise transform.TransformError("pyspark track failed (exit 2)")

    monkeypatch.setattr(transform, "run", boom)
    with pytest.raises(transform.TransformError):
        runner.build_and_publish(env, DAYS[0], lambda _: None)

    def broken(settings, *_a, **_k):
        golden_warehouse(settings.build_warehouse)
        con = duckdb.connect(str(settings.build_warehouse))
        con.execute("UPDATE gold.fct_orders SET discount_paise = gross_paise + 1")  # breaks discount_within_gross
        con.close()

    monkeypatch.setattr(transform, "run", broken)
    with pytest.raises(runner.RunFailed):
        runner.build_and_publish(env, DAYS[0], lambda _: None)
    assert monitor.read_ops(env, "SELECT status FROM run_log ORDER BY started_at") == [("failed",), ("blocked",)]


def test_cli_transform_reports_the_plan(env, monkeypatch, capsys):
    monkeypatch.setattr(transform, "run", lambda *_a, **_k: env.build_warehouse)
    assert cli.main(["transform", "--date", "2026-06-03", "--since", "2026-06-01"]) == 0
    assert "full: no published warehouse yet" in capsys.readouterr().out


def test_a_backfill_of_a_past_range_never_moves_the_published_date_back(env, calls):
    published(env, day=dt.date(2026, 6, 10))
    runner.run_range(env, DAYS[:2], lambda _: None, kind="backfill")
    assert calls == [(dt.date(2026, 6, 10), DAYS[0], False)]  # as of the published date, from the range's first day
