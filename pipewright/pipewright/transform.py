"""Run the chosen track's transformations (silver + gold) into the build warehouse.

* sql:     dbt seed + dbt run (models only; enforced contracts on gold). The dbt tests run in `pipewright dq`.
* pyspark: python -m pipewright_spark (gold written as Parquet, then registered in the same DuckDB file).

The build always starts from an empty file (v1.0: full rebuild every day), so a re-run is idempotent.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from .config import Settings


class TransformError(Exception):
    pass


def _bin(name: str) -> str:
    """An executable from the same virtualenv as this Python, else from PATH."""
    local = Path(sys.executable).with_name(name)
    return str(local) if local.exists() else (shutil.which(name) or name)


def command(settings: Settings, day: dt.date) -> tuple[list[str], dict[str, str], Path]:
    env = {**os.environ, "PIPEWRIGHT_LAKE_DIR": str(settings.lake_dir), "PIPEWRIGHT_DUCKDB": str(settings.build_warehouse),
           "PIPEWRIGHT_HOME": str(settings.home), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DO_NOT_TRACK": "1"}
    if settings.track == "sql":
        project = settings.home / "tracks" / "sql"
        cmd = [os.environ.get("PIPEWRIGHT_DBT", _bin("dbt")), "build", "--resource-type", "seed", "--resource-type", "model",
               "--project-dir", str(project), "--profiles-dir", str(project), "--vars", json.dumps({"run_date": day.isoformat()}),
               "--target-path", str(settings.warehouse.parent / "dbt-target"), "--log-path", str(settings.warehouse.parent / "dbt-logs")]
        return cmd, env, project
    project = settings.home / "tracks" / "pyspark"
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(project), env.get("PYTHONPATH")]))
    cmd = [sys.executable, "-m", "pipewright_spark", "--run-date", day.isoformat(), "--lake", str(settings.lake_dir),
           "--duckdb", str(settings.build_warehouse), "--parquet", str(settings.lake_dir / "gold"),
           "--contract", str(settings.contract)]
    return cmd, env, project


def run(settings: Settings, day: dt.date) -> Path:
    settings.build_warehouse.parent.mkdir(parents=True, exist_ok=True)
    for stale in (settings.build_warehouse, settings.build_warehouse.with_suffix(".duckdb.wal")):
        stale.unlink(missing_ok=True)
    cmd, env, cwd = command(settings, day)
    proc = subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-25:])
        raise TransformError(f"{settings.track} track failed (exit {proc.returncode}):\n{tail}")
    return settings.build_warehouse
