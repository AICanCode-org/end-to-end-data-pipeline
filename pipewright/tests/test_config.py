import datetime as dt

import pytest

from pipewright import config

pytestmark = pytest.mark.group("Phase 3 M1 - ingestion")


def test_window_is_an_ist_calendar_day():
    assert config.window(dt.date(2026, 6, 1)) == ("2026-06-01T00:00:00+05:30", "2026-06-02T00:00:00+05:30")


def test_date_range_inclusive_and_ordered():
    days = config.date_range(dt.date(2026, 6, 30), dt.date(2026, 7, 2))
    assert [d.isoformat() for d in days] == ["2026-06-30", "2026-07-01", "2026-07-02"]
    with pytest.raises(ValueError):
        config.date_range(dt.date(2026, 7, 2), dt.date(2026, 7, 1))


def test_parse_date_rejects_garbage():
    assert config.parse_date("2026-08-03") == dt.date(2026, 8, 3)
    with pytest.raises(ValueError, match="expected a date"):
        config.parse_date("03/08/2026")


def test_load_resolves_relative_paths_against_home(tmp_path):
    s = config.load({"PIPEWRIGHT_HOME": str(tmp_path), "PIPEWRIGHT_LAKE_DIR": "data/lake"})
    assert s.lake_dir == tmp_path / "data" / "lake"
    assert s.warehouse == tmp_path / "warehouse" / "pipewright.duckdb"
    assert s.build_warehouse.name == "pipewright_build.duckdb"
    assert s.track == "sql"


def test_unknown_track_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="PIPEWRIGHT_TRACK"):
        config.load({"PIPEWRIGHT_HOME": str(tmp_path), "PIPEWRIGHT_TRACK": "pandas"})


def test_token_never_appears_in_repr(tmp_path):
    s = config.load({"PIPEWRIGHT_HOME": str(tmp_path), "SOURCE_API_TOKEN": "s3cr3t-value"})
    assert "s3cr3t-value" not in repr(s)


def test_repo_root_found_from_cwd(tmp_path, monkeypatch):
    (tmp_path / "contract").mkdir()
    (tmp_path / "contract" / "data-contract.yaml").write_text("version: x\n")
    (tmp_path / "a" / "b").mkdir(parents=True)
    monkeypatch.delenv("PIPEWRIGHT_HOME", raising=False)
    monkeypatch.chdir(tmp_path / "a" / "b")
    assert config.repo_root() == tmp_path
