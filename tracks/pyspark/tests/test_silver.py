"""Milestone 2: the staging (silver) DataFrames equal the expected files (the same ones the SQL track must match)."""

from __future__ import annotations

import pytest
from conftest import canonical_df

from pipewright import testkit

pytestmark = pytest.mark.group("Phase 3 M2 - silver (staging) models")


@pytest.mark.parametrize("table", sorted(testkit.SILVER))
def test_silver_matches_expected(silver, table):
    key, cols = testkit.SILVER[table]
    actual = canonical_df(silver[table], key, cols)
    expected = testkit.expected_silver(table)
    assert actual == expected, f"{table}: {testkit.first_diff(actual, expected)}"


def test_money_is_integer_paise(silver):
    for name, df in silver.items():
        for col, typ in df.dtypes:
            if col.endswith("paise"):
                assert typ == "bigint", f"{name}.{col} is {typ}"
