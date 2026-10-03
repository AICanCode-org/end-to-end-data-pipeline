"""Read contract/data-contract.yaml: the single source of truth for sources, the lake layout and the gold tables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml


class ContractViolation(Exception):
    """Raised when incoming data breaks the contract (missing required fields or columns)."""


@lru_cache(maxsize=8)
def load(path: Path) -> dict:
    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def source(contract: dict, name: str) -> dict:
    try:
        return contract["sources"][name]
    except KeyError:
        raise KeyError(f"unknown source {name!r}; known: {sorted(contract['sources'])}") from None
