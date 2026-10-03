"""Test helpers (golden_warehouse lives in pipewright.testkit so the track and dashboard suites can share it)."""

from __future__ import annotations

from pathlib import Path

import yaml

from pipewright.testkit import golden_warehouse

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = yaml.safe_load((ROOT / "contract" / "data-contract.yaml").read_text())

__all__ = ["CONTRACT", "ROOT", "golden_warehouse"]
