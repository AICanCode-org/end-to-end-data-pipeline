"""A small HTTP client with retries: exponential backoff with jitter, Retry-After, timeouts. Standard library only."""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
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
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), self.max_backoff)
        base = min(self.backoff * 2 ** (attempt - 1), self.max_backoff)
        return base / 2 + self.rng.uniform(0, base / 2)  # "equal jitter"

    def get_json(self, path: str, params: dict[str, str | int]) -> dict:
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"})  # noqa: S310  # scheme checked in __init__
        for attempt in range(1, self.max_attempts + 1):
            self.attempts += 1
            retry_after = None
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310  # scheme checked in __init__
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    raise SourceError("401 Unauthorized from the source API: check SOURCE_API_TOKEN") from None
                if exc.code not in RETRYABLE:
                    raise SourceError(f"{exc.code} from {path}: {exc.read()[:200].decode('utf-8', 'replace')}") from None
                retry_after, reason = exc.headers.get("Retry-After"), f"HTTP {exc.code}"
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                reason = f"{type(exc).__name__}: {getattr(exc, 'reason', exc)}"
            if attempt == self.max_attempts:
                raise SourceError(f"{path}: giving up after {attempt} attempts ({reason})")
            self.retries += 1
            self.sleep(self.delay(attempt, retry_after))
        raise AssertionError("unreachable")  # pragma: no cover
