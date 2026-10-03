"""Shared fixtures: the committed 3-day source fixture served by the API stub in a thread, and temporary lakes."""

from __future__ import annotations

import datetime as dt
import os
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "source"))
FIXTURE = ROOT / "contract" / "fixture" / "source"
DAYS = [dt.date(2026, 6, 1), dt.date(2026, 6, 2), dt.date(2026, 6, 3)]

# Tests still to be switched on, one group per milestone (Phase 3: delete a line, watch the tests go red, make them
# green). Each test module declares its group with `pytestmark = pytest.mark.group("...")`.
PENDING: set[str] = {
    "Phase 6 - AI incident explainer",
    "Phase 6 - dashboard",
}


def pytest_configure(config):
    config.addinivalue_line("markers", "group(name): the milestone a test belongs to (see PENDING in conftest.py)")
    os.environ.setdefault("SOURCE_QUIET", "1")


def pytest_collection_modifyitems(config, items):
    for item in items:
        mark = item.get_closest_marker("group")
        if mark and mark.args[0] in PENDING:
            item.add_marker(pytest.mark.skip(reason=f"pending: {mark.args[0]}"))


@pytest.fixture(scope="session")
def stub():
    """The BazaarBox API stub on the fixture data, clock 'follow' (late updates show up in later windows)."""
    from bazaarbox_source.api import serve

    server = serve(FIXTURE, port=0, now="follow")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()


@pytest.fixture
def settings(tmp_path, stub):
    from pipewright import config

    return config.load(
        {
            "PIPEWRIGHT_HOME": str(ROOT),
            "SOURCE_API_URL": f"http://127.0.0.1:{stub.server_address[1]}",
            "PIPEWRIGHT_DROPS_DIR": str(FIXTURE / "drops"),
            "PIPEWRIGHT_LAKE_DIR": str(tmp_path / "lake"),
            "PIPEWRIGHT_WAREHOUSE": str(tmp_path / "warehouse" / "pipewright.duckdb"),
            "PIPEWRIGHT_PAGE_SIZE": "5",
            "PIPEWRIGHT_MAX_ATTEMPTS": "3",
        }
    )
