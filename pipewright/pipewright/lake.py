"""The lake (bronze layer): immutable partitions, written atomically, plus manifests and ingestion state.

    lake/raw/<source>/dt=<YYYY-MM-DD>/<file>
    lake/_manifests/<source>/<YYYY-MM-DD>.json
    lake/_state/<source>.json

Writing a partition replaces it as a whole (write a temporary directory, then swap), so re-running a day is
idempotent: the same input always leaves exactly the same files, never duplicates.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path


def partition_dir(lake: Path, source: str, key: str, day: dt.date) -> Path:
    return lake / "raw" / source / f"{key}={day.isoformat()}"


def write_partition(target: Path, files: dict[str, bytes]) -> None:
    """Replace directory `target` with exactly `files` ({name: content}); readers never see a half-written one."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    old = target.with_name(f".{target.name}.old-{os.getpid()}")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    for name, content in files.items():
        with (tmp / name).open("wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
    if target.exists():
        target.rename(old)
    tmp.rename(target)
    shutil.rmtree(old, ignore_errors=True)


def jsonl(rows: list[dict]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n" for r in rows).encode()


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def write_manifest(lake: Path, source: str, day: dt.date, manifest: dict) -> Path:
    path = lake / "_manifests" / source / f"{day.isoformat()}.json"
    write_json(path, manifest)
    return path


def read_state(lake: Path, source: str) -> dict:
    path = lake / "_state" / f"{source}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def update_state(lake: Path, source: str, day: dt.date) -> dict:
    """Remember the latest business date loaded (the high-water mark); never move it backwards (backfills)."""
    state = read_state(lake, source)
    last = state.get("last_loaded_date")
    if last is None or day.isoformat() > last:
        state["last_loaded_date"] = day.isoformat()
    write_json(lake / "_state" / f"{source}.json", state)
    return state


def partitions(lake: Path, source: str) -> list[dt.date]:
    base = lake / "raw" / source
    if not base.exists():
        return []
    return sorted(dt.date.fromisoformat(p.name.split("=", 1)[1]) for p in base.iterdir() if "=" in p.name and not p.name.startswith("."))
