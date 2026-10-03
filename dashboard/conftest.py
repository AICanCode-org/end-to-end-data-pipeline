"""Dashboard tests: PENDING groups, same mechanism as the other suites."""

from __future__ import annotations

import pytest

PENDING: set[str] = set()


def pytest_configure(config):
    config.addinivalue_line("markers", "group(name): the milestone a test belongs to (see PENDING in conftest.py)")


def pytest_collection_modifyitems(config, items):
    for item in items:
        mark = item.get_closest_marker("group")
        if mark and mark.args[0] in PENDING:
            item.add_marker(pytest.mark.skip(reason=f"pending: {mark.args[0]}"))
