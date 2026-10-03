"""Publishing: the dashboard only ever sees a complete build that passed its checks."""

from __future__ import annotations

from pathlib import Path

import duckdb

from .config import Settings


class PublishBlocked(Exception):
    pass


def publish(settings: Settings) -> Path:
    """Atomically swap the build into place, but only if its data-quality gate passed (ops.build_info)."""
    # TODO(Phase 3 M4): implement publish.
    # Read ops.build_info from the build file (read-only). No build, no build_info, or publishable = false ->
    # raise PublishBlocked with a clear message. Otherwise os.replace(build, warehouse): atomic on one file system.
    raise NotImplementedError("Phase 3 M4: publish (see the TODO above)")


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
