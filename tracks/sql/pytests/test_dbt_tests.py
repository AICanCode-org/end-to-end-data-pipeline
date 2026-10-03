"""Phase 4: the dbt data tests and unit tests all pass on the fixture build (warnings are allowed, errors are not)."""

from __future__ import annotations

import json
import subprocess

import pytest

from pipewright import config, testkit, transform

pytestmark = pytest.mark.group("Phase 4 - data tests")


def _dbt_test(settings, *selector: str) -> list[dict]:
    cmd, env, cwd = transform.command(settings, config.parse_date(testkit.RUN_DATE))
    cmd = [cmd[0], "test", *selector, *cmd[cmd.index("--project-dir"):]]
    subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603
    return json.loads((settings.warehouse.parent / "dbt-target" / "run_results.json").read_text())["results"]


def test_data_tests_pass(lake, build):
    results = _dbt_test(lake, "--exclude", "test_type:unit")
    assert len(results) >= 60, f"expected at least 60 dbt data tests, found {len(results)}"
    bad = [r["unique_id"] for r in results if r["status"] not in ("pass", "warn")]
    assert not bad, f"failing dbt tests: {bad}"


def test_unit_tests_pass(lake, build):
    results = _dbt_test(lake, "--select", "test_type:unit")
    assert len(results) >= 3, "expected the dbt unit tests (staging parsing, MRR, cohorts)"
    bad = [r["unique_id"] for r in results if r["status"] != "pass"]
    assert not bad, f"failing dbt unit tests: {bad}"


def test_gold_contracts_are_enforced():
    import yaml

    spec = yaml.safe_load((testkit.ROOT / "tracks" / "sql" / "models" / "gold" / "_gold.yml").read_text())
    for model in spec["models"]:
        assert model.get("config", {}).get("contract", {}).get("enforced") is True, f"{model['name']} has no enforced contract"
