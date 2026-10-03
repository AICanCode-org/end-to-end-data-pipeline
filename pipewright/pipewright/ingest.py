"""Ingestion (extract + load into the lake), the job Airbyte did in the old version of this project.

* API sources (orders, customers): incremental extraction by `updated_at` for one business-date window,
  cursor pagination, retries, PII dropped, required fields checked, then the window's partition is replaced.
* File sources (payments, marketing spend, products): the day's drop file is checked against the contract
  (required columns or keys) and copied byte for byte into the lake.

Every load writes a manifest (rows, sha256, contract version) and moves the source's high-water mark forward.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
from dataclasses import asdict, dataclass

from . import contract as contract_mod
from . import lake
from .config import Settings, window
from .contract import ContractViolation
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
    since, before = window(day)
    params: dict[str, str | int] = {"updated_since": since, "updated_before": before, "limit": page_size}
    rows, pages = [], 0
    while True:
        body = client.get_json(endpoint, params)
        pages += 1
        rows.extend(body["data"])
        if not body.get("next_cursor"):
            return rows, pages
        params["cursor"] = body["next_cursor"]


def conform(rows: list[dict], spec: dict, day: dt.date) -> tuple[list[dict], list[dict]]:
    """Drop PII and check required fields; rows are otherwise kept exactly as received. Returns (good, rejected)."""
    good, bad = [], []
    for row in rows:
        row = {k: v for k, v in row.items() if k not in spec.get("pii_drop", [])}
        missing = [f for f in spec["required"] if row.get(f) is None]
        if missing:
            bad.append({"reason": f"missing required {missing}", "record": row})
            continue
        good.append(row)
    key = spec["primary_key"]
    good.sort(key=lambda r: str(r[key]))
    return good, bad


def ingest_api(settings: Settings, source: str, day: dt.date, client: HttpClient | None = None) -> LoadResult:
    c = contract_mod.load(settings.contract)
    spec = contract_mod.source(c, source)
    if spec["kind"] != "api":
        raise ValueError(f"{source} is not an API source")
    client = client or _client(settings)
    rows, pages = fetch_window(client, spec["endpoint"], day, settings.page_size)
    good, bad = conform(rows, spec, day)
    content = lake.jsonl(good)
    target = lake.partition_dir(settings.lake_dir, source, spec["partition_key"], day)
    lake.write_partition(target, {"part-0000.jsonl": content})
    _quarantine(settings, source, day, bad)
    result = LoadResult(source, day.isoformat(), len(good), len(bad), lake.sha256(content), str(target), pages, client.retries)
    _finish(settings, source, day, result, c["version"])
    return result


def ingest_file(settings: Settings, source: str, day: dt.date) -> LoadResult:
    c = contract_mod.load(settings.contract)
    spec = contract_mod.source(c, source)
    if spec["kind"] != "file":
        raise ValueError(f"{source} is not a file source")
    path = settings.drops_dir / spec["pattern"].format(date=day.isoformat())
    if not path.is_file():
        raise MissingDrop(f"{source}: {path.name} has not arrived in {settings.drops_dir}")
    content = path.read_bytes()
    rows = validate_file(content, spec, path.name)
    target = lake.partition_dir(settings.lake_dir, source, spec["partition_key"], day)
    lake.write_partition(target, {path.name: content})
    result = LoadResult(source, day.isoformat(), rows, 0, lake.sha256(content), str(target))
    _finish(settings, source, day, result, c["version"])
    return result


def validate_file(content: bytes, spec: dict, name: str) -> int:
    """Check a drop file against the contract; extra columns/keys are allowed (additive schema evolution)."""
    text = content.decode("utf-8")
    if spec["format"] == "csv":
        reader = csv.DictReader(io.StringIO(text))
        missing = [col for col in spec["required"] if col not in (reader.fieldnames or [])]
        if missing:
            raise ContractViolation(f"{name}: missing required column(s) {missing}")
        return sum(1 for _ in reader)
    count = 0
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ContractViolation(f"{name}: line {n} is not JSON ({exc.msg})") from None
        missing = [k for k in spec["required"] if obj.get(k) is None]
        if missing:
            raise ContractViolation(f"{name}: line {n} misses required key(s) {missing}")
        count += 1
    return count


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
