"""Settings from environment variables (12-factor). Every variable is listed in .env.example."""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass, field
from pathlib import Path

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
TRACKS = ("sql", "pyspark")


def repo_root() -> Path:
    """PIPEWRIGHT_HOME, else the nearest parent of this file (or the cwd) that contains contract/."""
    env = os.environ.get("PIPEWRIGHT_HOME")
    if env:
        return Path(env).resolve()
    for start in (Path.cwd(), Path(__file__).resolve()):
        for p in (start, *start.parents):
            if (p / "contract" / "data-contract.yaml").is_file():
                return p
    return Path.cwd()


@dataclass(frozen=True)
class Settings:
    home: Path
    source_url: str = "http://127.0.0.1:18140"
    source_token: str = field(default="bazaarbox-dev-token", repr=False)  # never printed
    drops_dir: Path = Path("source-data/drops")
    lake_dir: Path = Path("lake")
    warehouse: Path = Path("warehouse/pipewright.duckdb")
    track: str = "sql"
    page_size: int = 200
    http_timeout: float = 10.0
    max_attempts: int = 5

    @property
    def contract(self) -> Path:
        return self.home / "contract" / "data-contract.yaml"

    @property
    def build_warehouse(self) -> Path:
        """Every run builds here; `publish` swaps it into place atomically."""
        return self.warehouse.with_name(self.warehouse.stem + "_build.duckdb")

    @property
    def reports_dir(self) -> Path:
        return self.warehouse.parent / "reports"


def load(env: dict[str, str] | None = None) -> Settings:
    env = dict(os.environ if env is None else env)
    home = Path(env["PIPEWRIGHT_HOME"]).resolve() if env.get("PIPEWRIGHT_HOME") else repo_root()

    def path(name: str, default: str) -> Path:
        p = Path(env.get(name) or default)
        return p if p.is_absolute() else home / p

    track = env.get("PIPEWRIGHT_TRACK", "sql")
    if track not in TRACKS:
        raise ValueError(f"PIPEWRIGHT_TRACK must be one of {TRACKS}, got {track!r}")
    return Settings(
        home=home,
        source_url=env.get("SOURCE_API_URL", "http://127.0.0.1:18140").rstrip("/"),
        source_token=env.get("SOURCE_API_TOKEN") or "bazaarbox-dev-token",
        drops_dir=path("PIPEWRIGHT_DROPS_DIR", "source-data/drops"),
        lake_dir=path("PIPEWRIGHT_LAKE_DIR", "lake"),
        warehouse=path("PIPEWRIGHT_WAREHOUSE", "warehouse/pipewright.duckdb"),
        track=track,
        page_size=int(env.get("PIPEWRIGHT_PAGE_SIZE", "200")),
        http_timeout=float(env.get("PIPEWRIGHT_HTTP_TIMEOUT", "10")),
        max_attempts=int(env.get("PIPEWRIGHT_MAX_ATTEMPTS", "5")),
    )


def parse_date(text: str) -> dt.date:
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"expected a date like 2026-08-03, got {text!r}") from exc


def window(day: dt.date) -> tuple[str, str]:
    """The extraction window for business date `day`: [day 00:00 IST, day+1 00:00 IST)."""
    start = dt.datetime.combine(day, dt.time(), IST)
    return start.isoformat(), (start + dt.timedelta(days=1)).isoformat()


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
