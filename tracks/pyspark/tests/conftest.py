"""PySpark track harness: one local SparkSession, the fixture lake ingested once, then silver and gold compared
with the same expected files as the SQL track (contract/golden/fixture-silver and contract/golden/fixture).

Run from tracks/pyspark:  pytest --cov   (needs Java 17+ and the shared package: pip install -e ../../pipewright)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pipewright import testkit

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
SEEDS = ROOT / "contract" / "seeds"

# Tests still to be switched on, one group per milestone (Phase 3: delete a line, watch the tests go red, make them
# green). Each test module declares its group with `pytestmark = pytest.mark.group("...")`.
PENDING: set[str] = {
    "Phase 4 - data-quality checks",
    "Phase 4 - data tests",
    "Phase 6 - AI incident explainer",
    "Phase 6 - dashboard",
}


def pytest_configure(config):
    config.addinivalue_line("markers", "group(name): the milestone a test belongs to (see PENDING in conftest.py)")


def pytest_collection_modifyitems(config, items):
    for item in items:
        mark = item.get_closest_marker("group")
        if mark and mark.args[0] in PENDING:
            item.add_marker(pytest.mark.skip(reason=f"pending: {mark.args[0]}"))


@pytest.fixture(scope="session")
def spark():
    from pipewright_spark.session import get_spark

    s = get_spark("pipewright-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def lake(tmp_path_factory):
    return testkit.ingest_fixture(tmp_path_factory.mktemp("pyspark"), PIPEWRIGHT_TRACK="pyspark")


@pytest.fixture(scope="session")
def silver(spark, lake):
    from pipewright_spark import pipeline

    return pipeline.build_silver(spark, lake.lake_dir, SEEDS, testkit.RUN_DATE)


@pytest.fixture(scope="session")
def gold(silver):
    from pipewright_spark import pipeline

    return pipeline.build_gold(silver, testkit.RUN_DATE)


def canonical_df(df, key: list[str], cols: list[str]) -> str:
    """A DataFrame as canonical CSV text: every value as text, timestamps 'YYYY-MM-DD HH:MM:SS', sorted by key."""
    from pyspark.sql import functions as F

    types = dict(df.dtypes)
    exprs = [F.date_format(c, "yyyy-MM-dd HH:mm:ss").alias(c) if types[c].startswith("timestamp") else F.col(c).cast("string").alias(c)
             for c in cols]
    rows = df.orderBy(*key).select(*exprs).collect()
    return testkit.canonical([tuple(r) for r in rows], cols)
