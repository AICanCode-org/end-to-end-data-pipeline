"""1.1 monitoring: freshness, volume, SLA and data-quality results, alerts, history and the webhook."""

from __future__ import annotations

import datetime as dt
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb
import pytest

from pipewright import cli, config, dq, ingest, lake, monitor

from .conftest import DAYS
from .helpers import golden_warehouse

pytestmark = pytest.mark.group("Phase 7 - monitoring")
IST = config.IST


def at(day: dt.date, hhmm: str) -> dt.datetime:
    """A moment on the morning after business date `day`, IST."""
    h, m = (int(x) for x in hhmm.split(":"))
    return dt.datetime.combine(day + dt.timedelta(days=1), dt.time(h, m), IST)


def by_id(results):
    return {r.check_id: r for r in results}


def publish_golden(settings, day: dt.date, version: str = "1.1.0") -> None:
    golden_warehouse(settings.warehouse)
    con = duckdb.connect(str(settings.warehouse))
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE OR REPLACE TABLE ops.build_info AS SELECT ?::DATE AS run_date, ? AS track, ? AS contract_version, true AS publishable",
                [day, settings.track, version])
    con.close()


def test_freshness_is_pending_before_the_deadline_and_fails_after(settings):
    day = DAYS[0]
    early = by_id(monitor.evaluate(settings, day, at(day, "05:45")))
    assert early["freshness:payments"].status == "pass"  # the drop file has arrived
    assert early["freshness:orders"].status == "pending"  # API data: due 06:15, not ingested yet
    assert early["sla:publish"].status == "pending" and early["dq:gate"].status == "pending"
    late = by_id(monitor.evaluate(settings, day, at(day, "07:00")))
    assert late["freshness:orders"].status == "fail" and late["freshness:orders"].severity == "error"
    assert late["sla:publish"].status == "fail" and "nothing" in late["sla:publish"].detail
    missing = type(settings)(**{**settings.__dict__, "drops_dir": settings.drops_dir.parent / "nowhere"})
    spend = by_id(monitor.evaluate(missing, day, at(day, "05:45")))["freshness:marketing_spend"]
    assert (spend.status, spend.severity) == ("fail", "warn") and "has not arrived" in spend.detail


def test_after_a_good_run_everything_passes(settings):
    for d in DAYS:
        ingest.ingest_all(settings, d)
    publish_golden(settings, DAYS[-1])
    results = by_id(monitor.evaluate(settings, DAYS[-1], at(DAYS[-1], "07:00")))
    assert all(r.status == "pass" for k, r in results.items() if k.startswith(("freshness", "sla")))
    assert results["volume:orders"].status == "pending"  # only 2 days of history: no verdict yet


def test_volume_compares_with_the_7_day_median(settings):
    day = dt.date(2026, 6, 10)
    for i in range(1, 8):
        lake.write_manifest(settings.lake_dir, "orders", day - dt.timedelta(days=i), {"rows": 100})
    lake.write_manifest(settings.lake_dir, "orders", day, {"rows": 30})
    r = by_id(monitor.check_volume(settings, day, {"orders": {"min_ratio": 0.5, "max_ratio": 2.0, "severity": "warn"}}))["volume:orders"]
    assert (r.status, r.severity) == ("fail", "warn") and "ratio 0.3" in r.detail
    lake.write_manifest(settings.lake_dir, "orders", day, {"rows": 120})
    assert monitor.check_volume(settings, day, {"orders": {"min_ratio": 0.5, "max_ratio": 2.0}})[0].status == "pass"


def test_dq_results_and_history(settings):
    s = type(settings)(**{**settings.__dict__, "track": "pyspark"})  # no dbt: checks from the contract only
    golden_warehouse(s.build_warehouse)
    report = dq.run(s, DAYS[-1])
    rows = monitor.read_ops(s, "SELECT count(*), min(contract_version) FROM dq_history")
    assert rows == [(len(report["checks"]), "1.1.0")]
    assert monitor.check_dq(s, DAYS[-1]).status in ("pass", "fail")
    path = s.reports_dir / f"dq-{DAYS[-1]}.json"
    report["summary"].update(failed_error=2, failed_warn=0)
    path.write_text(json.dumps(report))
    r = monitor.check_dq(s, DAYS[-1])
    assert (r.check_id, r.status, r.severity) == ("dq:gate", "fail", "error")
    report["summary"].update(failed_error=0, failed_warn=1)
    path.write_text(json.dumps(report))
    assert (monitor.check_dq(s, DAYS[-1]).check_id, monitor.check_dq(s, DAYS[-1]).severity) == ("dq:warnings", "warn")


class Hook(BaseHTTPRequestHandler):
    received: list = []  # noqa: RUF012
    code = 200

    def do_POST(self):
        Hook.received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self.send_response(Hook.code)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def hook():
    server = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    Hook.received, Hook.code = [], 200
    yield f"http://127.0.0.1:{server.server_address[1]}/hooks/T000/SECRET-TOKEN"
    server.shutdown()


def test_alerts_are_recorded_and_sent_without_leaking_the_url(settings, hook):
    logs = []
    results, alerts = monitor.run(settings, DAYS[0], at(DAYS[0], "07:00"), {"ALERT_WEBHOOK_URL": hook}, logs.append)
    assert {a["check_id"] for a in alerts} >= {"freshness:orders", "freshness:customers", "sla:publish"}
    lines = (settings.reports_dir / "alerts.jsonl").read_text().splitlines()
    assert len(lines) == len(alerts) and json.loads(lines[0])["run_date"] == "2026-06-01"
    assert monitor.read_ops(settings, "SELECT count(*) FROM alerts")[0][0] == len(alerts)
    assert monitor.read_ops(settings, "SELECT count(*) FROM monitor_results")[0][0] == len(results)
    assert len(Hook.received) == 1 and "[ERROR] 2026-06-01 sla:publish" in Hook.received[0]["text"]
    assert logs == ["webhook delivered (HTTP 200)"] and not any("SECRET" in line for line in logs)
    # a second evaluation replaces the day's results instead of piling them up
    monitor.run(settings, DAYS[0], at(DAYS[0], "07:00"), {}, logs.append)
    assert monitor.read_ops(settings, "SELECT count(*) FROM monitor_results")[0][0] == len(results)


def test_webhook_failures_never_break_the_run(settings, hook):
    Hook.code = 500
    assert monitor.send_webhook(hook, [{"severity": "error", "run_date": "d", "check_id": "c", "message": "m"}]) == "webhook delivery failed (HTTP 500)"
    assert "failed (URLError)" in monitor.send_webhook("http://127.0.0.1:9/x", [], timeout=1)
    assert monitor.send_webhook("file:///etc/passwd", []).startswith("webhook skipped")


def test_warn_alerts_only_reach_the_webhook_when_asked(settings, hook):
    missing = type(settings)(**{**settings.__dict__, "drops_dir": settings.drops_dir.parent / "nowhere"})
    _, alerts = monitor.run(missing, DAYS[0], at(DAYS[0], "05:45"), {"ALERT_WEBHOOK_URL": hook}, lambda _: None)
    assert alerts and all(a["severity"] in ("warn", "error") for a in alerts)
    sent_default = len(Hook.received)
    monitor.run(missing, DAYS[0], at(DAYS[0], "05:45"), {"ALERT_WEBHOOK_URL": hook, "ALERT_MIN_SEVERITY": "warn"}, lambda _: None)
    assert len(Hook.received) == sent_default + 1 and "[WARN]" in Hook.received[-1]["text"]


def test_record_run_and_cli_monitor(settings, monkeypatch, capsys):
    started = dt.datetime.now(dt.UTC)
    monitor.record_run(settings, DAYS[0], "backfill", "published", started, "ok", True, DAYS[0])
    assert monitor.read_ops(settings, "SELECT kind, status, incremental FROM run_log") == [("backfill", "published", True)]
    for k, v in {"PIPEWRIGHT_HOME": str(settings.home), "PIPEWRIGHT_DROPS_DIR": str(settings.drops_dir),
                 "PIPEWRIGHT_LAKE_DIR": str(settings.lake_dir), "PIPEWRIGHT_WAREHOUSE": str(settings.warehouse)}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert cli.main(["monitor", "--date", "2026-06-01", "--now", "2026-06-02T05:45+05:30"]) == 0  # only pending / warn so far
    assert cli.main(["monitor", "--date", "2026-06-01", "--now", "2026-06-02T07:00+05:30", "--json"]) == 1
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert json.loads(out)["alerts"]
    with pytest.raises(SystemExit):
        cli.main(["monitor", "--date", "2026-06-01", "--now", "2026-06-02T07:00"])
    assert monitor.published_date(settings) is None
