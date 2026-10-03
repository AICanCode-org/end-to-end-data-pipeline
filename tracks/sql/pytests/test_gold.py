"""Milestone 3: the gold models (two dimensions, two facts and the five CEO reports) equal the expected files."""

from __future__ import annotations

from pathlib import Path

import parity
import pytest

from pipewright import config, testkit, transform

pytestmark = pytest.mark.group("Phase 3 M3 - gold models (the five reports)")

GOLDEN = testkit.GOLDEN / "fixture"


@pytest.fixture(scope="module")
def actual(build):
    return parity.load(build)


@pytest.mark.parametrize("table", list(parity.GOLD))
def test_gold_table_matches_golden(actual, table):
    expected = parity.load(GOLDEN)[table]
    assert table in actual, f"gold.{table} was not built"
    problems = parity.diff_csv(actual[table]["csv"], expected["csv"], parity.GOLD[table]["columns"])
    assert not problems, f"gold.{table}: " + "; ".join(problems)


def test_gold_columns_follow_the_contract(con):
    for table, spec in parity.GOLD.items():
        got = dict(con.execute("SELECT column_name, data_type FROM information_schema.columns "
                               "WHERE table_schema = 'gold' AND table_name = ?", [table]).fetchall())
        assert got == spec["columns"], f"gold.{table} columns/types differ from contract/data-contract.yaml"


def test_rebuild_is_idempotent(lake, actual, tmp_path):
    """A second build from the same lake gives byte-identical gold tables."""
    again = config.Settings(**{**lake.__dict__, "warehouse": tmp_path / "pipewright.duckdb"})
    transform.run(again, config.parse_date(testkit.RUN_DATE))
    second = parity.load(Path(again.build_warehouse))
    assert {t: d["sha256"] for t, d in second.items()} == {t: d["sha256"] for t, d in actual.items()}
