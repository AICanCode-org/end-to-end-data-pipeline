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

from . import dq, explainer, ingest, monitor, transform, warehouse
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


def build_and_publish(settings: Settings, day: dt.date, log: Callable[[str], None] = print, since: dt.date | None = None,
                      full_refresh: bool = False, kind: str = "run") -> dict:
    """Transform, check and publish the warehouse as of `day` (everything in the lake up to that date).

    1.1: the build is incremental when it can be (see transform.plan), and every attempt is logged in ops.run_log."""
    t0, started = time.time(), dt.datetime.now(dt.UTC)
    build_plan = transform.plan(settings, day, since, full_refresh)
    try:
        transform.run(settings, day, since, full_refresh)
    except transform.TransformError as exc:
        monitor.record_run(settings, day, kind, "failed", started, str(exc).splitlines()[0], build_plan.incremental, build_plan.since)
        raise
    log(f"[{day}] transform ({settings.track}, {'incremental' if build_plan.incremental else 'full'}: {build_plan.reason}): "
        f"built {settings.build_warehouse.name}")
    report = dq.run(settings, day)
    s = report["summary"]
    log(f"[{day}] dq: {s['passed']}/{s['checks']} passed, {s['failed_error']} blocking, {s['failed_warn']} warnings")
    if not s["publishable"]:
        path = write_incident(settings, day, report)
        monitor.record_run(settings, day, kind, "blocked", started, f"{s['failed_error']} blocking check(s)", build_plan.incremental, build_plan.since)
        raise RunFailed(f"[{day}] blocking data-quality failures: not published (incident note: {path})", report)
    warehouse.publish(settings)
    monitor.record_run(settings, day, kind, "published", started, f"{s['passed']}/{s['checks']} checks passed", build_plan.incremental, build_plan.since)
    log(f"[{day}] published {settings.warehouse} (build + checks {time.time() - t0:.1f}s)")
    return s


def run_range(settings: Settings, days: list[dt.date], log: Callable[[str], None] = print, full_refresh: bool = False,
              kind: str = "run") -> dict:
    """Ingest every day's window in order, then build, check and publish once, as of the last day.

    Building every intermediate day would give the same final warehouse, only slower. 1.1: the incremental build
    covers every order updated since the first day of the range (or the usual lookback, whichever is earlier), so
    a backfill (`pipewright backfill`) that re-ingests a past range also rebuilds everything it touched. The build
    is as of the later of the range end and the published date: a backfill never moves the dashboard back in time."""
    loads = [r.as_dict() for d in days for r in ingest_day(settings, d, log)]
    summary = {"start": days[0].isoformat(), "end": days[-1].isoformat(), "track": settings.track, "kind": kind,
               "loads": len(loads), "rows": sum(r["rows"] for r in loads)}
    published = warehouse.published_run_date(settings)
    as_of = max(days[-1], dt.date.fromisoformat(published)) if published else days[-1]
    since = min(days[0], as_of - dt.timedelta(days=transform.lookback_days(settings)))
    summary["dq"] = build_and_publish(settings, as_of, log, since=since, full_refresh=full_refresh, kind=kind)
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    (settings.reports_dir / "last-run.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
