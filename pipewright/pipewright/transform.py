"""Run the chosen track's transformations (silver + gold) into the build warehouse.

* sql:     dbt seed + dbt run (models only; enforced contracts on gold). The dbt tests run in `pipewright dq`.
* pyspark: python -m pipewright_spark (gold written as Parquet, then registered in the same DuckDB file).

1.1: the two fact tables are incremental. A build starts from a copy of the published warehouse and rebuilds only
the orders updated on or after `since` (default: run date - monitoring.incremental.lookback_days). It falls back
to a full rebuild when there is no published warehouse, when it was built under another contract version or by
the other track, or when asked (`--full-refresh`). Both ways must give exactly the same tables (tested).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

from . import contract as contract_mod
from .config import Settings


class TransformError(Exception):
    pass


@dataclass(frozen=True)
class Plan:
    incremental: bool
    since: dt.date | None
    reason: str


def _bin(name: str) -> str:
    """An executable from the same virtualenv as this Python, else from PATH."""
    local = Path(sys.executable).with_name(name)
    return str(local) if local.exists() else (shutil.which(name) or name)


def lookback_days(settings: Settings) -> int:
    c = contract_mod.load(settings.contract)
    return int(c.get("monitoring", {}).get("incremental", {}).get("lookback_days", 3))


def _published_info(settings: Settings) -> tuple[str, str] | None:
    if not settings.warehouse.exists():
        return None
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    try:
        return con.execute("SELECT contract_version, track FROM ops.build_info").fetchone()
    except duckdb.Error:
        return None
    finally:
        con.close()


def plan(settings: Settings, day: dt.date, since: dt.date | None = None, full_refresh: bool = False) -> Plan:
    """Decide between an incremental build and a full rebuild (and say why)."""
    if full_refresh:
        return Plan(False, None, "full refresh requested")
    info = _published_info(settings)
    if info is None:
        return Plan(False, None, "no published warehouse yet")
    version = contract_mod.load(settings.contract)["version"]
    if info[0] != version:
        return Plan(False, None, f"contract changed {info[0]} -> {version}")
    if info[1] != settings.track:
        return Plan(False, None, f"published warehouse was built by the {info[1]} track")
    since = since or day - dt.timedelta(days=lookback_days(settings))
    return Plan(True, since, f"orders updated since {since}")


def previous_dir(settings: Settings) -> Path:
    return settings.warehouse.parent / "previous"


def command(settings: Settings, day: dt.date, build_plan: Plan | None = None) -> tuple[list[str], dict[str, str], Path]:
    build_plan = build_plan or Plan(False, None, "")
    env = {**os.environ, "PIPEWRIGHT_LAKE_DIR": str(settings.lake_dir), "PIPEWRIGHT_DUCKDB": str(settings.build_warehouse),
           "PIPEWRIGHT_HOME": str(settings.home), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DO_NOT_TRACK": "1"}
    if settings.track == "sql":
        project = settings.home / "tracks" / "sql"
        dbt_vars = {"run_date": day.isoformat(), "lookback_days": lookback_days(settings)}
        if build_plan.since:
            dbt_vars["since"] = build_plan.since.isoformat()
        cmd = [os.environ.get("PIPEWRIGHT_DBT", _bin("dbt")), "build", "--resource-type", "seed", "--resource-type", "model",
               "--project-dir", str(project), "--profiles-dir", str(project), "--vars", json.dumps(dbt_vars),
               "--target-path", str(settings.warehouse.parent / "dbt-target"), "--log-path", str(settings.warehouse.parent / "dbt-logs")]
        if not build_plan.incremental and build_plan.reason:
            cmd.append("--full-refresh")
        return cmd, env, project
    project = settings.home / "tracks" / "pyspark"
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(project), env.get("PYTHONPATH")]))
    cmd = [sys.executable, "-m", "pipewright_spark", "--run-date", day.isoformat(), "--lake", str(settings.lake_dir),
           "--duckdb", str(settings.build_warehouse), "--parquet", str(settings.lake_dir / "gold"),
           "--contract", str(settings.contract)]
    if build_plan.incremental and build_plan.since:
        cmd += ["--previous", str(previous_dir(settings)), "--since", build_plan.since.isoformat()]
    return cmd, env, project


def _prepare(settings: Settings, build_plan: Plan) -> None:
    """Start the build file: empty (full), or a copy of the published warehouse without its checks (incremental)."""
    build = settings.build_warehouse
    build.parent.mkdir(parents=True, exist_ok=True)
    for stale in (build, build.with_suffix(".duckdb.wal")):
        stale.unlink(missing_ok=True)
    if not build_plan.incremental:
        return
    shutil.copyfile(settings.warehouse, build)
    con = duckdb.connect(str(build))
    try:  # the copy must be checked again before it can be published
        con.execute("DROP TABLE IF EXISTS ops.build_info")
        con.execute("DROP TABLE IF EXISTS ops.dq_results")
        if settings.track == "pyspark":
            prev = previous_dir(settings)
            shutil.rmtree(prev, ignore_errors=True)
            prev.mkdir(parents=True)
            for name in ("fct_orders", "fct_order_items"):
                con.execute(f"COPY gold.{name} TO '{prev / (name + '.parquet')}' (FORMAT parquet)")
    finally:
        con.close()


def run(settings: Settings, day: dt.date, since: dt.date | None = None, full_refresh: bool = False) -> Path:
    build_plan = plan(settings, day, since, full_refresh)
    _prepare(settings, build_plan)
    cmd, env, cwd = command(settings, day, build_plan)
    proc = subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-25:])
        raise TransformError(f"{settings.track} track failed (exit {proc.returncode}):\n{tail}")
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    (settings.reports_dir / "last-transform.json").write_text(
        json.dumps({"run_date": day.isoformat(), "track": settings.track, "incremental": build_plan.incremental,
                    "since": build_plan.since.isoformat() if build_plan.since else None, "reason": build_plan.reason}) + "\n",
        encoding="utf-8")
    return settings.build_warehouse
