"""Milestone 2: the staging (silver) models clean, deduplicate and type the raw data exactly as the spec says."""

from __future__ import annotations

import pytest

from pipewright import testkit

pytestmark = pytest.mark.group("Phase 3 M2 - silver (staging) models")


@pytest.mark.parametrize("table", sorted(testkit.SILVER))
def test_silver_matches_expected(con, table):
    _, cols = testkit.SILVER[table]
    actual = testkit.canonical(con.execute(testkit.silver_sql(table)).fetchall(), cols)
    expected = testkit.expected_silver(table)
    assert actual == expected, f"silver.{table}: {testkit.first_diff(actual, expected)}"


def test_orders_are_latest_version_only(con):
    dupes = con.execute("SELECT count(*) - count(DISTINCT order_id) FROM silver.stg_orders").fetchone()[0]
    assert dupes == 0, "stg_orders must keep one row per order (the latest updated_at)"


def test_payments_are_deduplicated(con):
    dupes = con.execute("SELECT count(*) - count(DISTINCT event_id) FROM silver.stg_payments").fetchone()[0]
    assert dupes == 0, "the payment drops contain re-sent events; stg_payments keeps each event_id once"


def test_money_is_integer_paise(con):
    types = dict(con.execute("SELECT table_name || '.' || column_name, data_type FROM information_schema.columns "
                             "WHERE table_schema = 'silver' AND column_name LIKE '%paise'").fetchall())
    assert types and set(types.values()) == {"BIGINT"}, types
