"""Milestone 2: the parsing rules of transform-spec.md §2 (same cases as tracks/sql/tests/singular/parsing_macros.sql)."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F

from pipewright_spark import parsing

pytestmark = pytest.mark.group("Phase 3 M2 - silver (staging) models")


@pytest.mark.parametrize(("text", "utc"), [
    ("2026-06-01T10:00:00Z", "2026-06-01 10:00:00"),
    ("2026-06-01T15:30:00+05:30", "2026-06-01 10:00:00"),
    ("2026-06-01T01:00:00-02:00", "2026-06-01 03:00:00"),
    ("2026-05-31T20:00:00+00:00", "2026-05-31 20:00:00"),
])
def test_utc_timestamp(spark, text, utc):
    got = spark.createDataFrame([(text,)], "s string").select(F.date_format(parsing.utc_timestamp(F.col("s")), "yyyy-MM-dd HH:mm:ss")).first()[0]
    assert got == utc


@pytest.mark.parametrize(("text", "day"), [
    ("2026-05-31T18:29:59Z", dt.date(2026, 5, 31)),   # 23:59:59 IST
    ("2026-05-31T18:30:00Z", dt.date(2026, 6, 1)),    # midnight IST: the next business day
    ("2026-06-01T23:59:00+05:30", dt.date(2026, 6, 1)),
])
def test_ist_date(spark, text, day):
    got = spark.createDataFrame([(text,)], "s string").select(parsing.ist_date(parsing.utc_timestamp(F.col("s")))).first()[0]
    assert got == day


@pytest.mark.parametrize(("text", "paise"), [
    ("₹12,345.50", 1234550), ("12,340.00", 1234000), ("499", 49900), ("499.5", 49950), (" 1,00,000.05 ", 10000005),
])
def test_to_paise(spark, text, paise):
    assert spark.createDataFrame([(text,)], "s string").select(parsing.to_paise(F.col("s"))).first()[0] == paise


def test_clean_key(spark):
    assert spark.createDataFrame([("  Bengaluru ",)], "s string").select(parsing.clean_key(F.col("s"))).first()[0] == "bengaluru"
