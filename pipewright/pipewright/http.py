"""A small HTTP client with retries: exponential backoff with jitter, Retry-After, timeouts. Standard library only."""

from __future__ import annotations

import random
import time
from collections.abc import Callable

RETRYABLE = {429, 500, 502, 503, 504}


class SourceError(Exception):
    """The source API failed in a way retrying will not fix (or retries ran out)."""


class HttpClient:
    def __init__(self, base_url: str, token: str, timeout: float = 10.0, max_attempts: int = 5,
                 backoff: float = 0.5, max_backoff: float = 8.0, sleep: Callable[[float], None] = time.sleep,
                 rng: random.Random | None = None):
        if not base_url.startswith(("http://", "https://")):
            raise SourceError(f"SOURCE_API_URL must be an http(s) URL, got {base_url!r}")
        self.base_url, self._token, self.timeout = base_url.rstrip("/"), token, timeout
        self.max_attempts, self.backoff, self.max_backoff = max_attempts, backoff, max_backoff
        self.sleep, self.rng = sleep, rng or random.Random()
        self.attempts = 0  # total HTTP attempts, for the run summary
        self.retries = 0

    def __repr__(self) -> str:  # never show the token
        return f"HttpClient({self.base_url!r})"

    def delay(self, attempt: int, retry_after: str | None) -> float:
        # TODO(Phase 3 M1): implement delay.
        # Retry-After (seconds, digits only) wins, capped at max_backoff.
        # Otherwise exponential backoff: base = min(backoff * 2 ** (attempt - 1), max_backoff), then 'equal jitter':
        # base / 2 + a random amount in [0, base / 2] from self.rng (tests pass a seeded random.Random).
        raise NotImplementedError("Phase 3 M1: delay (see the TODO above)")

    def get_json(self, path: str, params: dict[str, str | int]) -> dict:
        # TODO(Phase 3 M1): implement get_json.
        # Build the URL with urllib.parse.urlencode(params) and send the bearer token in the Authorization header.
        # Retry 429, 5xx, timeouts and connection errors up to max_attempts (count self.attempts and self.retries),
        # sleeping self.delay(attempt, Retry-After) in between; a 4xx other than 429 is a SourceError at once.
        # Never put the token in an exception message or a log line.
        raise NotImplementedError("Phase 3 M1: get_json (see the TODO above)")
