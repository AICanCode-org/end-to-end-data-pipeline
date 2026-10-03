import datetime as dt

import pytest

from pipewright import dq, warehouse

from .helpers import golden_warehouse

pytestmark = pytest.mark.group("Phase 3 M4 - runner and orchestration")


def pyspark(settings):
    return type(settings)(**{**settings.__dict__, "track": "pyspark"})


def test_publish_requires_a_checked_build(settings):
    with pytest.raises(warehouse.PublishBlocked, match="no build"):
        warehouse.publish(settings)
    golden_warehouse(settings.build_warehouse)
    with pytest.raises(warehouse.PublishBlocked, match="not been checked"):
        warehouse.publish(settings)


def test_publish_swaps_atomically_after_passing_checks(settings):
    s = pyspark(settings)
    golden_warehouse(s.build_warehouse)
    dq.run(s, dt.date(2026, 6, 3))
    assert warehouse.publish(s) == s.warehouse
    assert not s.build_warehouse.exists()
    assert warehouse.published_run_date(s) == "2026-06-03"


@pytest.mark.group("Phase 4 - data-quality checks")  # needs the real checks
def test_failed_checks_block_publishing_and_keep_the_last_good_build(settings):
    s = pyspark(settings)
    golden_warehouse(s.build_warehouse)
    dq.run(s, dt.date(2026, 6, 3))
    warehouse.publish(s)
    golden_warehouse(s.build_warehouse)
    import duckdb

    con = duckdb.connect(str(s.build_warehouse))
    con.execute("UPDATE gold.fct_orders SET status = 'lost'")
    con.close()
    assert dq.run(s, dt.date(2026, 6, 4))["summary"]["publishable"] is False
    with pytest.raises(warehouse.PublishBlocked, match="last good data"):
        warehouse.publish(s)
    assert warehouse.published_run_date(s) == "2026-06-03"


def test_published_run_date_without_warehouse(settings):
    assert warehouse.published_run_date(settings) is None
