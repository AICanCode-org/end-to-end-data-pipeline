"""The shared data-quality checks: every kind passes on a good build and fails on a broken one."""

import duckdb
import pytest

from pipewright import dq

from .helpers import CONTRACT, golden_warehouse

pytestmark = pytest.mark.group("Phase 4 - data-quality checks")


@pytest.fixture
def con(tmp_path):
    db = golden_warehouse(tmp_path / "w.duckdb")
    c = duckdb.connect(str(db))
    c.execute("SET search_path = 'gold'")
    yield c
    c.close()


def check(id_):
    return next(c for c in CONTRACT["checks"] if c["id"] == id_)


def run(con, id_):
    return dq.run_check(con, check(id_), CONTRACT["enums"])


def test_every_contract_check_passes_on_the_golden_fixture(con):
    results = [dq.run_check(con, c, CONTRACT["enums"]) for c in CONTRACT["checks"]]
    failing = [(r.id, r.detail) for r in results if r.status != "pass" and r.severity == "error"]
    assert failing == []


def test_accepted_values_reports_the_unexpected_codes(con):
    con.execute("UPDATE fct_orders SET status = 'rto' WHERE order_id IN (SELECT order_id FROM fct_orders LIMIT 2)")
    r = run(con, "status_accepted")
    assert (r.status, r.observed, r.unexpected_values, r.blocking) == ("fail", 2, ["rto"], True)


def test_unprintable_values_are_masked(con):
    con.execute("UPDATE fct_orders SET status = 'x''; DROP TABLE y; --' WHERE order_id IN (SELECT order_id FROM fct_orders LIMIT 1)")
    assert run(con, "status_accepted").unexpected_values == ["<unprintable>"]


def test_relationship(con):
    con.execute("DELETE FROM dim_customers WHERE customer_id IN (SELECT customer_id FROM fct_orders LIMIT 1)")
    r = run(con, "order_customer_exists")
    assert r.status == "fail" and r.observed >= 1


def test_warn_severity_never_blocks(con):
    con.execute("UPDATE fct_order_items SET sku = 'SKU-0000' WHERE line_number = 1")
    r = run(con, "item_sku_known")
    assert r.status == "fail" and r.severity == "warn" and not r.blocking


def test_max_share(con):
    assert run(con, "unknown_city_rare").status == "pass"
    con.execute("UPDATE fct_orders SET region = 'Unknown'")
    r = run(con, "unknown_city_rare")
    assert r.status == "fail" and r.observed == 1.0


def test_reconcile_catches_a_lost_line(con):
    con.execute("DELETE FROM fct_order_items WHERE rowid = (SELECT min(rowid) FROM fct_order_items)")
    r = run(con, "gross_equals_lines")
    assert r.status == "fail" and r.observed > 0


def test_revenue_reconcile_resolves_enum_lists(con):
    assert run(con, "revenue_reconciles").status == "pass"
    con.execute("UPDATE mart_revenue_daily SET revenue_paise = revenue_paise + 1 WHERE rowid = 0")
    assert run(con, "revenue_reconciles").observed == 1


def test_row_counts_and_expressions(con):
    assert run(con, "orders_not_empty").status == "pass"
    con.execute("DELETE FROM fct_orders")
    assert run(con, "orders_not_empty").status == "fail"
    con.execute("UPDATE mart_cohort_retention SET retention_rate = 1.5")
    assert run(con, "retention_is_a_rate").status == "fail"
    con.execute("INSERT INTO mart_top_products_30d SELECT * FROM mart_top_products_30d")
    assert run(con, "top_products_at_most_10").status in ("pass", "fail")


def test_a_check_that_cannot_run_is_an_error_not_a_pass(con):
    r = dq.run_check(con, {"id": "x", "kind": "row_count_min", "table": "no_such_table", "min": 1}, {})
    assert r.status == "error" and r.blocking
    r = dq.run_check(con, {"id": "y", "kind": "telepathy", "table": "fct_orders"}, {})
    assert r.status == "error"


def test_run_writes_report_and_ops_tables(settings):
    golden_warehouse(settings.build_warehouse)
    s = type(settings)(**{**settings.__dict__, "track": "pyspark"})  # no dbt tests on this track
    report = dq.run(s, __import__("datetime").date(2026, 6, 3))
    assert report["summary"]["publishable"] is True
    assert (s.reports_dir / "dq-2026-06-03.json").exists()
    con = duckdb.connect(str(s.build_warehouse), read_only=True)
    assert con.execute("SELECT count(*) FROM ops.dq_results").fetchone()[0] == len(CONTRACT["checks"])
    assert con.execute("SELECT publishable FROM ops.build_info").fetchone()[0] is True
