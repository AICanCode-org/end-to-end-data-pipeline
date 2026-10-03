"""Ingestion (extract + load into the lake), the job Airbyte did in the old version of this project.

* API sources (orders, customers): incremental extraction by `updated_at` for one business-date window,
  cursor pagination, retries, PII dropped, required fields checked, then the window's partition is replaced.
* File sources (payments, marketing spend, products): the day's drop file is checked against the contract
  (required columns or keys) and copied byte for byte into the lake.

Every load writes a manifest (rows, sha256, contract version) and moves the source's high-water mark forward.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass

from . import contract as contract_mod
from . import lake
from .config import Settings
from .http import HttpClient


class MissingDrop(Exception):
    """The day's file has not arrived (yet). Orchestrators retry or alert on this."""


@dataclass
class LoadResult:
    source: str
    date: str
    rows: int
    rejected: int
    sha256: str
    path: str
    pages: int = 0
    retries: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


def _client(settings: Settings) -> HttpClient:
    return HttpClient(settings.source_url, settings.source_token, settings.http_timeout, settings.max_attempts)


def fetch_window(client: HttpClient, endpoint: str, day: dt.date, page_size: int) -> tuple[list[dict], int]:
    """Every record whose updated_at falls in the window of `day`, following next_cursor to the last page."""
    # TODO(Phase 3 M1): implement fetch_window.
    # GET the endpoint with updated_since / updated_before = config.window(day) and limit = page_size,
    # then follow next_cursor until it is null. Return (all rows, number of pages).
    raise NotImplementedError("Phase 3 M1: fetch_window (see the TODO above)")


def conform(rows: list[dict], spec: dict, day: dt.date) -> tuple[list[dict], list[dict]]:
    """Drop PII and check required fields; rows are otherwise kept exactly as received. Returns (good, rejected)."""
    # TODO(Phase 3 M1): implement conform.
    # For every row: drop the spec's pii_drop fields; a row missing a required field goes to `rejected`
    # (with the reason), everything else to `good`, otherwise unchanged (bronze keeps what the source sent).
    raise NotImplementedError("Phase 3 M1: conform (see the TODO above)")


def ingest_api(settings: Settings, source: str, day: dt.date, client: HttpClient | None = None) -> LoadResult:
    # TODO(Phase 3 M1): implement ingest_api.
    # fetch_window -> conform -> lake.write_partition(source, day, good) -> quarantine the rejected rows,
    # then _finish() writes the manifest and moves the high-water mark. Return a LoadResult.
    raise NotImplementedError("Phase 3 M1: ingest_api (see the TODO above)")


def ingest_file(settings: Settings, source: str, day: dt.date) -> LoadResult:
    # TODO(Phase 3 M1): implement ingest_file.
    # The drop for `day` is <drops>/<pattern with the date>. Missing -> raise MissingDrop (exit code 3: retry later).
    # Validate it against the contract (validate_file), then copy the bytes unchanged into the partition.
    raise NotImplementedError("Phase 3 M1: ingest_file (see the TODO above)")


def validate_file(content: bytes, spec: dict, name: str) -> int:
    """Check a drop file against the contract; extra columns/keys are allowed (additive schema evolution)."""
    # TODO(Phase 3 M1): implement validate_file.
    # CSV: the header must contain every required column (extra columns are fine: the contract allows additions).
    # JSON lines: every line must parse and carry the required keys. Raise ContractViolation with the file name.
    # Return the number of records.
    raise NotImplementedError("Phase 3 M1: validate_file (see the TODO above)")


def missing_drops(settings: Settings, day: dt.date) -> list[str]:
    c = contract_mod.load(settings.contract)
    return [
        name for name, spec in c["sources"].items()
        if spec["kind"] == "file" and not (settings.drops_dir / spec["pattern"].format(date=day.isoformat())).is_file()
    ]


def ingest(settings: Settings, source: str, day: dt.date, client: HttpClient | None = None) -> LoadResult:
    spec = contract_mod.source(contract_mod.load(settings.contract), source)
    return ingest_api(settings, source, day, client) if spec["kind"] == "api" else ingest_file(settings, source, day)


def ingest_all(settings: Settings, day: dt.date, client: HttpClient | None = None) -> list[LoadResult]:
    c = contract_mod.load(settings.contract)
    return [ingest(settings, name, day, client) for name in c["sources"]]


def _quarantine(settings: Settings, source: str, day: dt.date, bad: list[dict]) -> None:
    target = settings.lake_dir / "quarantine" / source / f"{day.isoformat()}.jsonl"
    if bad:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(lake.jsonl(bad))
    elif target.exists():
        target.unlink()


def _finish(settings: Settings, source: str, day: dt.date, result: LoadResult, version: str) -> None:
    manifest = {k: v for k, v in result.as_dict().items() if k not in ("path", "retries", "pages")}
    lake.write_manifest(settings.lake_dir, source, day, {**manifest, "contract_version": version})
    lake.update_state(settings.lake_dir, source, day)
