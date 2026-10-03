"""Publishing: the dashboard only ever sees a complete build that passed its checks."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from .config import Settings


class PublishBlocked(Exception):
    pass


def publish(settings: Settings) -> Path:
    """Atomically swap the build into place, but only if its data-quality gate passed (ops.build_info)."""
    build = settings.build_warehouse
    if not build.exists():
        raise PublishBlocked(f"no build at {build}: run transform and dq first")
    con = duckdb.connect(str(build), read_only=True)
    try:
        row = con.execute("SELECT run_date, publishable FROM ops.build_info").fetchone()
    except duckdb.Error:
        row = None
    finally:
        con.close()
    if row is None:
        raise PublishBlocked("the build has not been checked: run `pipewright dq` first")
    if not row[1]:
        raise PublishBlocked(f"the build for {row[0]} failed blocking data-quality checks; the dashboard keeps the last good data")
    os.replace(build, settings.warehouse)  # atomic on the same file system
    return settings.warehouse


def published_run_date(settings: Settings) -> str | None:
    if not settings.warehouse.exists():
        return None
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    try:
        return str(con.execute("SELECT run_date FROM ops.build_info").fetchone()[0])
    except duckdb.Error:
        return None
    finally:
        con.close()
