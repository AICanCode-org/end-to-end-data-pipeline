"""SQL track harness: ingest the committed 3-day fixture once, build it with dbt, and compare with the expected files.

Run from tracks/sql:  pytest pytests   (needs the shared `pipewright` package: pip install -e ../../pipewright)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

from pipewright import config, testkit, transform

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

# Tests still to be switched on, one group per milestone (Phase 3: delete a line, watch the tests go red, make them
# green). Each test module declares its group with `pytestmark = pytest.mark.group("...")`.
PENDING: set[str] = set()


def pytest_configure(config):
    config.addinivalue_line("markers", "group(name): the milestone a test belongs to (see PENDING in conftest.py)")


def pytest_collection_modifyitems(config, items):
    for item in items:
        mark = item.get_closest_marker("group")
        if mark and mark.args[0] in PENDING:
            item.add_marker(pytest.mark.skip(reason=f"pending: {mark.args[0]}"))


@pytest.fixture(scope="session")
def lake(tmp_path_factory):
    """Settings whose lake holds the three fixture days (bronze), ingested once per test session."""
    return testkit.ingest_fixture(tmp_path_factory.mktemp("sql"), PIPEWRIGHT_TRACK="sql")


@pytest.fixture(scope="session")
def build(lake):
    """The build warehouse after `dbt build` (seeds + models) as of the last fixture day."""
    transform.run(lake, config.parse_date(testkit.RUN_DATE))
    return lake.build_warehouse


@pytest.fixture
def con(build):
    c = duckdb.connect(str(build), read_only=True)
    yield c
    c.close()
