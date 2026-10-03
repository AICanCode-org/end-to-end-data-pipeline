"""Ingestion into the lake: windows, pagination, PII, idempotency, quarantine, file drops and the contract."""

import json
import shutil

import pytest

from pipewright import ingest, lake
from pipewright.contract import ContractViolation

from .conftest import DAYS, FIXTURE

pytestmark = pytest.mark.group("Phase 3 M1 - ingestion")


def read_rows(path):
    return [json.loads(line) for line in (path / "part-0000.jsonl").read_text().splitlines()]


def test_api_window_is_complete_across_pages(settings):
    r = ingest.ingest_api(settings, "orders", DAYS[2])
    assert r.pages > 2, "page size 5 must force pagination"
    rows = read_rows(lake.partition_dir(settings.lake_dir, "orders", "dt", DAYS[2]))
    assert len(rows) == r.rows > 10
    assert len({x["order_id"] for x in rows}) == len(rows)
    assert rows == sorted(rows, key=lambda x: x["order_id"])


def test_every_order_version_lands_in_the_window_of_its_updated_at(settings):
    for d in DAYS:
        ingest.ingest_api(settings, "orders", d)
    from bazaarbox_source.api import parse_ts

    for d in DAYS:
        for row in read_rows(lake.partition_dir(settings.lake_dir, "orders", "dt", d)):
            local = parse_ts(row["updated_at"]).astimezone(__import__("pipewright").config.IST)
            assert local.date() == d


def test_late_updates_reappear_in_later_windows(settings):
    for d in DAYS:
        ingest.ingest_api(settings, "orders", d)
    seen = {}
    for d in DAYS:
        for row in read_rows(lake.partition_dir(settings.lake_dir, "orders", "dt", d)):
            seen.setdefault(row["order_id"], []).append(row["status"])
    assert any(len(v) > 1 for v in seen.values()), "some orders must change status on a later day"


def test_pii_is_dropped_before_landing(settings):
    ingest.ingest_api(settings, "customers", DAYS[0])
    text = (lake.partition_dir(settings.lake_dir, "customers", "dt", DAYS[0]) / "part-0000.jsonl").read_text()
    assert "@example" not in text and "+91-" not in text
    assert all("email" not in r and "phone" not in r for r in map(json.loads, text.splitlines()))


def test_reingesting_a_day_is_idempotent(settings):
    first = ingest.ingest_api(settings, "orders", DAYS[1])
    path = lake.partition_dir(settings.lake_dir, "orders", "dt", DAYS[1])
    before = sorted(p.name for p in path.parent.iterdir())
    second = ingest.ingest_api(settings, "orders", DAYS[1])
    assert first.sha256 == second.sha256 and first.rows == second.rows
    assert sorted(p.name for p in path.parent.iterdir()) == before, "no temp or duplicate partitions left behind"


def test_manifest_and_state(settings):
    for d in (DAYS[1], DAYS[0]):  # a backfill of an older day must not move the high-water mark back
        ingest.ingest_api(settings, "orders", d)
    manifest = json.loads((settings.lake_dir / "_manifests" / "orders" / f"{DAYS[0]}.json").read_text())
    assert manifest["contract_version"] == "1.1.0" and manifest["loaded_at"].endswith("+00:00") and manifest["rows"] > 0 and len(manifest["sha256"]) == 64
    assert lake.read_state(settings.lake_dir, "orders")["last_loaded_date"] == str(DAYS[1])
    assert lake.partitions(settings.lake_dir, "orders") == [DAYS[0], DAYS[1]]


def test_records_missing_required_fields_are_quarantined(settings):
    spec = {"required": ["order_id", "status"], "primary_key": "order_id", "pii_drop": []}
    good, bad = ingest.conform([{"order_id": "O-1", "status": "placed"}, {"order_id": "O-2", "status": None}], spec, DAYS[0])
    assert [g["order_id"] for g in good] == ["O-1"]
    assert bad[0]["reason"] == "missing required ['status']"
    ingest._quarantine(settings, "orders", DAYS[0], bad)
    q = settings.lake_dir / "quarantine" / "orders" / f"{DAYS[0]}.jsonl"
    assert q.exists()
    ingest._quarantine(settings, "orders", DAYS[0], [])
    assert not q.exists(), "a clean re-run clears the quarantine for that day"


def test_file_drop_is_copied_byte_for_byte(settings):
    r = ingest.ingest_file(settings, "marketing_spend", DAYS[0])
    src = FIXTURE / "drops" / "marketing_spend" / f"spend_{DAYS[0]}.csv"
    dst = lake.partition_dir(settings.lake_dir, "marketing_spend", "dt", DAYS[0]) / src.name
    assert dst.read_bytes() == src.read_bytes()
    assert r.rows == 5


def test_missing_drop_raises_and_is_reported(settings, tmp_path):
    drops = tmp_path / "drops"
    shutil.copytree(FIXTURE / "drops", drops)
    (drops / "payments" / f"payments_{DAYS[2]}.jsonl").unlink()
    s = type(settings)(**{**settings.__dict__, "drops_dir": drops})
    assert ingest.missing_drops(s, DAYS[2]) == ["payments"]
    assert ingest.missing_drops(s, DAYS[1]) == []
    with pytest.raises(ingest.MissingDrop, match=r"payments_2026-06-03\.jsonl"):
        ingest.ingest_file(s, "payments", DAYS[2])


def test_csv_missing_a_required_column_is_a_contract_violation():
    spec = {"format": "csv", "required": ["date", "channel", "spend", "currency"]}
    with pytest.raises(ContractViolation, match="spend"):
        ingest.validate_file(b"date,channel,currency\n2026-06-01,organic,INR\n", spec, "spend.csv")


def test_extra_csv_columns_are_allowed():
    spec = {"format": "csv", "required": ["date", "channel"]}
    assert ingest.validate_file(b"date,channel,campaign_id\n2026-06-01,organic,X\n", spec, "s.csv") == 1


def test_jsonl_bad_line_and_missing_key():
    spec = {"format": "jsonl", "required": ["event_id"]}
    with pytest.raises(ContractViolation, match="line 2 is not JSON"):
        ingest.validate_file(b'{"event_id": "a"}\nnot json\n', spec, "p.jsonl")
    with pytest.raises(ContractViolation, match="line 1 misses"):
        ingest.validate_file(b'{"type": "x"}\n', spec, "p.jsonl")
    assert ingest.validate_file(b'{"event_id": "a"}\n\n', spec, "p.jsonl") == 1


def test_ingest_all_loads_every_source(settings):
    results = ingest.ingest_all(settings, DAYS[0])
    assert [r.source for r in results] == ["orders", "customers", "payments", "marketing_spend", "products"]
    assert all(r.rows > 0 for r in results)


def test_wrong_kind_and_unknown_source(settings):
    with pytest.raises(ValueError):
        ingest.ingest_api(settings, "payments", DAYS[0])
    with pytest.raises(ValueError):
        ingest.ingest_file(settings, "orders", DAYS[0])
    with pytest.raises(KeyError, match="unknown source"):
        ingest.ingest(settings, "stripe", DAYS[0])
