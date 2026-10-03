"""The dashboard renders every tab from a warehouse, handles 'no data yet' and a blocked latest run, and the
explainer button works with no AI provider at all."""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
APP = str(ROOT / "dashboard" / "app.py")
sys.path.insert(0, str(ROOT / "dashboard"))
import data  # noqa: E402

from pipewright.testkit import golden_warehouse  # noqa: E402

pytestmark = pytest.mark.group("Phase 6 - dashboard")


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    path = golden_warehouse(tmp_path / "warehouse" / "pipewright.duckdb")
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA ops")
    con.execute("CREATE TABLE ops.build_info AS SELECT DATE '2026-06-03' AS run_date, 'sql' AS track, '1.0.0' AS contract_version, true AS publishable")
    con.execute("CREATE TABLE ops.dq_results AS SELECT DATE '2026-06-03' AS run_date, 'orders_not_empty' AS check_id, 'row_count_min' AS kind, "
                "'fct_orders' AS table_name, 'error' AS severity, 'pass' AS status, 46.0 AS observed, 1.0 AS threshold, 'ok' AS detail")
    con.close()
    monkeypatch.setenv("PIPEWRIGHT_WAREHOUSE", str(path))
    monkeypatch.setenv("PIPEWRIGHT_HOME", str(ROOT))
    monkeypatch.setenv("AI_PROVIDER", "template")
    return path


def _report(path: Path, day: str, publishable: bool) -> None:
    path.mkdir(parents=True, exist_ok=True)
    checks = [] if publishable else [{"id": "status_accepted", "kind": "accepted_values", "table": "fct_orders", "severity": "error",
                                      "status": "fail", "observed": 12, "threshold": 0, "detail": "12 row(s) outside", "unexpected_values": ["rto"]}]
    report = {"run_date": day, "track": "sql", "contract_version": "1.0.0", "checks": checks,
              "summary": {"checks": 1, "passed": int(publishable), "failed_error": int(not publishable), "failed_warn": 0, "publishable": publishable}}
    (path / f"dq-{day}.json").write_text(json.dumps(report))


def run_app():
    from streamlit.testing.v1 import AppTest

    return AppTest.from_file(APP, default_timeout=60).run()


def test_renders_all_reports(warehouse):
    at = run_app()
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Revenue", "CAC by channel", "Prime MRR", "Top 10 products (30d)", "30-day retention", "Data health"]
    assert at.metric[0].label == "Revenue" and at.metric[0].value.startswith("₹")
    assert "2026-06-03" in at.caption[0].value
    assert len(at.dataframe) >= 5


def test_filters_change_revenue(warehouse):
    at = run_app()
    total = at.metric[0].value
    at.multiselect[0].select("South").run()
    assert not at.exception and at.metric[0].value != total


def test_no_data_yet(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPEWRIGHT_WAREHOUSE", str(tmp_path / "nothing.duckdb"))
    at = run_app()
    assert not at.exception and "No published data yet" in at.warning[0].value


def test_blocked_run_banner_and_offline_explainer(warehouse, monkeypatch):
    _report(warehouse.parent / "reports", "2026-06-04", publishable=False)
    monkeypatch.setenv("AI_PROVIDER", "openai")
    monkeypatch.setenv("AI_BASE_URL", "http://127.0.0.1:9")  # nothing listens: the provider is unreachable
    monkeypatch.setenv("AI_MODEL", "any")
    at = run_app()
    assert "NOT published" in at.error[0].value
    at.button(key="explain").click().run()
    assert not at.exception
    assert any("Source: template (provider unavailable" in c.value for c in at.caption)


@pytest.mark.parametrize(("paise", "text"), [
    (0, "₹0"), (99, "₹0"), (12345, "₹123"), (123456789, "₹12,34,567"), (-500000, "-₹5,000"), (None, "—"), (1000000000, "₹1,00,00,000"),
])
def test_rupees(paise, text):
    assert data.rupees(paise) == text


def test_rupees_with_paise():
    assert data.rupees(1234550, decimals=2) == "₹12,345.50"


def test_status_age_and_blocked(warehouse):
    con = data.connect(warehouse)
    s = data.status(con, warehouse.parent / "reports")
    con.close()
    assert s.run_date == dt.date(2026, 6, 3) and s.age_days(dt.date(2026, 6, 5)) == 2
    assert not s.last_run_blocked and s.latest_report is None


def test_status_of_an_unchecked_file(tmp_path):
    path = golden_warehouse(tmp_path / "w.duckdb")
    con = data.connect(path)
    s = data.status(con, tmp_path / "none")
    con.close()
    assert s.run_date is None and s.age_days() is None and s.checks.empty
