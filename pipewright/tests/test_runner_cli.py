"""The runner and the CLI end to end, with the track replaced by a build of the golden fixture."""

import datetime as dt
import json

import duckdb
import pytest

from pipewright import cli, config, runner, transform

from .conftest import DAYS, FIXTURE, ROOT
from .helpers import golden_warehouse

pytestmark = pytest.mark.group("Phase 3 M4 - runner and orchestration")


@pytest.fixture
def fake_track(monkeypatch):
    calls = []

    def fake_run(settings, day):
        calls.append(day)
        return golden_warehouse(settings.build_warehouse)

    monkeypatch.setattr(transform, "run", fake_run)
    return calls


@pytest.fixture
def env(settings, monkeypatch):
    for k, v in {"PIPEWRIGHT_HOME": str(settings.home), "SOURCE_API_URL": settings.source_url,
                 "PIPEWRIGHT_DROPS_DIR": str(settings.drops_dir), "PIPEWRIGHT_LAKE_DIR": str(settings.lake_dir),
                 "PIPEWRIGHT_WAREHOUSE": str(settings.warehouse), "PIPEWRIGHT_TRACK": "pyspark"}.items():
        monkeypatch.setenv(k, v)
    return config.load()


def test_run_range_ingests_every_day_then_builds_once(env, fake_track):
    logs = []
    summary = runner.run_range(env, DAYS, logs.append)
    assert fake_track == [DAYS[-1]]
    assert summary["loads"] == 15 and summary["dq"]["publishable"]
    assert env.warehouse.exists() and (env.reports_dir / "last-run.json").exists()
    assert any("published" in line for line in logs)


@pytest.mark.group("Phase 4 - data-quality checks")  # needs the real checks
def test_blocking_failure_writes_an_incident_note_and_does_not_publish(env, monkeypatch):
    def broken(settings, day):
        golden_warehouse(settings.build_warehouse)
        con = duckdb.connect(str(settings.build_warehouse))
        con.execute("UPDATE gold.fct_orders SET status = 'rto'")
        con.close()

    monkeypatch.setattr(transform, "run", broken)
    with pytest.raises(runner.RunFailed) as exc:
        runner.run_range(env, DAYS[:1], lambda _: None)
    assert exc.value.report["summary"]["publishable"] is False
    note = (env.reports_dir / f"incident-{DAYS[0]}.md").read_text()
    assert "status_accepted" in note and "Source: template" in note
    assert not env.warehouse.exists()


def test_cli_run_and_explain_and_export(env, fake_track, capsys, tmp_path):
    assert cli.main(["run", "--start", "2026-06-01", "--end", "2026-06-03"]) == 0
    assert cli.main(["explain"]) == 0
    assert "Source: template" in capsys.readouterr().out
    assert cli.main(["export", "--out", str(tmp_path / "csv")]) == 0
    want = (ROOT / "contract" / "golden" / "fixture" / "mart_revenue_daily.csv").read_text()
    assert (tmp_path / "csv" / "mart_revenue_daily.csv").read_text() == want


def test_cli_ingest_one_and_all(env, capsys):
    assert cli.main(["ingest", "--source", "orders", "--date", "2026-06-02"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[0])["source"] == "orders"
    assert cli.main(["ingest", "--all", "--date", "2026-06-02"]) == 0
    assert len(capsys.readouterr().out.splitlines()) == 5


def test_cli_dq_and_publish(env, capsys):
    golden_warehouse(env.build_warehouse)
    assert cli.main(["dq", "--date", "2026-06-03"]) == 0
    assert cli.main(["publish"]) == 0
    assert cli.main(["publish"]) == 1  # nothing to publish any more
    assert "not published" in capsys.readouterr().err


@pytest.mark.group("Phase 4 - data-quality checks")  # needs the real checks
def test_cli_dq_failure_exit_code(env):
    golden_warehouse(env.build_warehouse)
    con = duckdb.connect(str(env.build_warehouse))
    con.execute("DELETE FROM gold.fct_orders")
    con.close()
    assert cli.main(["dq", "--date", "2026-06-03"]) == 1
    assert (env.reports_dir / "incident-2026-06-03.md").exists()


def test_cli_exit_codes(env, monkeypatch, tmp_path, capsys):
    assert cli.main(["check-drops", "--date", "2026-06-03"]) == 0
    assert cli.main(["check-drops", "--date", "2026-06-04"]) == 3
    assert cli.main(["ingest", "--source", "payments", "--date", "2026-06-04"]) == 3
    monkeypatch.setenv("SOURCE_API_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("PIPEWRIGHT_MAX_ATTEMPTS", "1")
    assert cli.main(["ingest", "--source", "orders", "--date", "2026-06-01"]) == 4
    monkeypatch.setenv("PIPEWRIGHT_TRACK", "pandas")
    assert cli.main(["publish"]) == 2
    monkeypatch.setenv("PIPEWRIGHT_TRACK", "sql")
    bad = tmp_path / "drops" / "marketing_spend"
    bad.mkdir(parents=True)
    (bad / "spend_2026-06-01.csv").write_text("date,channel\n2026-06-01,organic\n")
    monkeypatch.setenv("PIPEWRIGHT_DROPS_DIR", str(tmp_path / "drops"))
    assert cli.main(["ingest", "--source", "marketing_spend", "--date", "2026-06-01"]) == 5
    assert cli.main(["explain"]) == 2


def test_cli_transform_failure_exit_code(env, monkeypatch):
    def boom(settings, day):
        raise transform.TransformError("sql track failed (exit 1)")

    monkeypatch.setattr(transform, "run", boom)
    assert cli.main(["transform", "--date", "2026-06-03"]) == 6


def test_cli_needs_dates(env):
    with pytest.raises(SystemExit):
        cli.main(["transform"])
    with pytest.raises(SystemExit):
        cli.main(["ingest", "--date", "2026-06-01"])


def test_transform_commands(env):
    sql = type(env)(**{**env.__dict__, "track": "sql"})
    cmd, run_env, cwd = transform.command(sql, dt.date(2026, 6, 3))
    assert cmd[1:2] == ["build"] and '"run_date": "2026-06-03"' in " ".join(cmd) and cwd.name == "sql"
    assert run_env["PIPEWRIGHT_DUCKDB"].endswith("pipewright_build.duckdb")
    cmd, run_env, cwd = transform.command(env, dt.date(2026, 6, 3))
    assert cmd[1:3] == ["-m", "pipewright_spark"] and cwd.name == "pyspark"


def test_transform_failure_shows_the_tail_of_the_log(env, monkeypatch):
    monkeypatch.setattr(transform, "command", lambda s, d: (["python3", "-c", "import sys; print('boom'); sys.exit(3)"], {}, FIXTURE))
    with pytest.raises(transform.TransformError, match="exit 3"):
        transform.run(env, DAYS[0])
