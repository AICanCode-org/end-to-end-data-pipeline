"""One day of the pipeline, end to end: ingest -> transform -> checks -> publish (or explain why not).

The Airflow DAG runs the same steps as separate tasks; `pipewright run` runs them in-process for CI, tests and
laptops without Airflow.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from collections.abc import Callable
from pathlib import Path

from . import dq, explainer, ingest, transform, warehouse
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
    t0 = time.time()
    transform.run(settings, day)
    log(f"[{day}] transform ({settings.track}): built {settings.build_warehouse.name}")
    report = dq.run(settings, day)
    s = report["summary"]
    log(f"[{day}] dq: {s['passed']}/{s['checks']} passed, {s['failed_error']} blocking, {s['failed_warn']} warnings")
    if not s["publishable"]:
        path = write_incident(settings, day, report)
        raise RunFailed(f"[{day}] blocking data-quality failures: not published (incident note: {path})", report)
    warehouse.publish(settings)
    log(f"[{day}] published {settings.warehouse} (build + checks {time.time() - t0:.1f}s)")
    return s


def run_range(settings: Settings, days: list[dt.date], log: Callable[[str], None] = print) -> dict:
    """Ingest every day's window in order, then build, check and publish once, as of the last day.

    A v1.0 build is a full rebuild from the lake, so building every intermediate day of a backfill would give
    the same final warehouse, only slower."""
    loads = [r.as_dict() for d in days for r in ingest_day(settings, d, log)]
    summary = {"start": days[0].isoformat(), "end": days[-1].isoformat(), "track": settings.track,
               "loads": len(loads), "rows": sum(r["rows"] for r in loads)}
    summary["dq"] = build_and_publish(settings, days[-1], log)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    (settings.reports_dir / "last-run.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
