"""Airflow needs a home; tests use a throwaway one (SQLite) so they never touch a real installation."""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile

import pytest

_home = tempfile.mkdtemp(prefix="pipewright-airflow-tests-")
atexit.register(shutil.rmtree, _home, ignore_errors=True)
os.environ.setdefault("AIRFLOW_HOME", _home)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

PENDING: set[str] = {
    "Phase 3 M1 - ingestion",
    "Phase 3 M2 - silver (staging) models",
    "Phase 3 M3 - gold models (the five reports)",
    "Phase 3 M4 - runner and orchestration",
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
