"""One day of the pipeline, end to end: ingest -> transform -> checks -> publish (or explain why not).

The Airflow DAG runs the same steps as separate tasks; `pipewright run` runs them in-process for CI, tests and
laptops without Airflow.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from pathlib import Path

from . import explainer, ingest
from .config import Settings


class RunFailed(Exception):
    def __init__(self, message: str, report: dict | None = None):
        super().__init__(message)
        self.report = report


def ingest_day(settings: Settings, day: dt.date, log: Callable[[str], None] = print) -> list[ingest.LoadResult]:
    loads = ingest.ingest_all(settings, day)
    log(f"[{day}] ingest: " + ", ".join(f"{r.source}={r.rows}" + (f" (rejected {r.rejected})" if r.rejected else "") for r in loads))
    return loads


def write_incident(settings: Settings, day: dt.date, report: dict) -> Path:
    """The incident note for a blocked build: what failed, explained (AI if configured, else the template)."""
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    path = settings.reports_dir / f"incident-{day}.md"
    path.write_text(explainer.render(explainer.explain(report)), encoding="utf-8")
    return path


def build_and_publish(settings: Settings, day: dt.date, log: Callable[[str], None] = print) -> dict:
    """Transform, check and publish the warehouse as of `day` (everything in the lake up to that date)."""
    # TODO(Phase 3 M4): implement build_and_publish.
    # transform.run -> dq.run -> if the report is publishable: warehouse.publish, else write_incident and raise
    # RunFailed. Log one line per step (use `log`, not print, so the CLI and tests can capture it).
    raise NotImplementedError("Phase 3 M4: build_and_publish (see the TODO above)")


def run_range(settings: Settings, days: list[dt.date], log: Callable[[str], None] = print) -> dict:
    """Ingest every day's window in order, then build, check and publish once, as of the last day.

    A v1.0 build is a full rebuild from the lake, so building every intermediate day of a backfill would give
    the same final warehouse, only slower."""
    # TODO(Phase 3 M4): implement run_range.
    # Ingest every day in order (ingest_day), then build_and_publish once, as of the last day (a v1.0 build is a
    # full rebuild from the lake). Write reports/last-run.json with start, end, track, loads, rows and the dq summary.
    raise NotImplementedError("Phase 3 M4: run_range (see the TODO above)")
