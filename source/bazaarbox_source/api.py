"""Offline stub of BazaarBox's orders and customers API (standard library only).

    python -m bazaarbox_source serve --data source-data --port 18140

Endpoints (all JSON; errors are RFC 9457 problem+json):
    GET /health
    GET /v1/orders?updated_since=<ISO-8601>&updated_before=<ISO-8601>&limit=<1..500>&cursor=<opaque>
    GET /v1/customers?... (same parameters)

* ``Authorization: Bearer <token>`` is required on /v1/* (token from SOURCE_API_TOKEN, default
  ``bazaarbox-dev-token``; it is a local dev value, not a secret).
* Records are the state **as of the source clock** (SOURCE_NOW: an ISO instant, ``follow`` or unset = the end
  of the data; see SourceData). An order's
  ``updated_at`` is its latest status change, so an order whose status changes later shows up again in a later
  window: that is what incremental extraction by ``updated_at`` has to handle.
* Results are ordered by (updated_at instant, id) and paginated with ``next_cursor`` (null on the last page).
* SOURCE_FAULT_RATE (0..1) makes that share of /v1 requests fail with 429 (Retry-After: 1) or 503, from a seeded
  random stream (SOURCE_FAULT_SEED), so retry logic can be tested reproducibly.
"""

from __future__ import annotations

import argparse
import base64
import bisect
import contextlib
import datetime as dt
import json
import os
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_TOKEN = "bazaarbox-dev-token"  # noqa: S105  # a public demo token for a local stub, not a secret
MAX_LIMIT = 500


def parse_ts(text: str) -> dt.datetime:
    ts = dt.datetime.fromisoformat(text)
    if ts.tzinfo is None:
        raise ValueError(f"timestamp without offset: {text!r}")
    return ts.astimezone(dt.UTC)


class SourceData:
    """Orders and customers as of a clock, indexed for windowed, cursor-paginated reads.

    ``now`` is an ISO timestamp (the state at that instant), None (the final state) or ``"follow"``: each request
    sees the state as of its ``updated_before`` + 6 hours, i.e. as if the extraction ran at 06:00 the next
    morning. ``follow`` makes a backfill behave exactly like a series of daily runs (late status changes show up
    in later windows), and the result never depends on the wall clock.
    """

    FOLLOW_LAG = dt.timedelta(hours=6)

    def __init__(self, data_dir: Path, now: str | None = None):
        def load(name: str, events: str) -> list[tuple[dict, list[dt.datetime], list[dict]]]:
            out = []
            for line in (data_dir / "api" / name).read_text("utf-8").splitlines():
                rec = json.loads(line)
                evs = rec.pop(events)
                out.append((rec, [parse_ts(e["at"]) for e in evs], evs))
            return out

        self.orders = load("orders.jsonl", "history")
        self.customers = load("customers.jsonl", "versions")
        self._cache: dict[tuple[str, dt.datetime | None], tuple[list, list]] = {}
        self.set_clock(now)

    def set_clock(self, now: str | None) -> None:
        self.follow = now == "follow"
        self.now = None if self.follow or not now else parse_ts(now)
        self._cache.clear()

    def view(self, entity: str, clock: dt.datetime | None) -> tuple[list, list]:
        key = (entity, clock)
        if key not in self._cache:
            if len(self._cache) > 8:
                self._cache.clear()
            rows = []
            for rec, times, events in self.orders if entity == "orders" else self.customers:
                n = len(times) if clock is None else bisect.bisect_right(times, clock)
                if n == 0:
                    continue
                last = events[n - 1]
                if entity == "orders":
                    out = {**rec, "status": last["status"], "updated_at": last["at"]}
                    out = {k: out[k] for k in ORDER_FIELDS if k in out}
                    rows.append((times[n - 1], rec["order_id"], out))
                else:
                    rows.append((times[n - 1], rec["customer_id"], {**rec, "city": last["city"], "updated_at": last["at"]}))
            rows.sort(key=lambda r: (r[0], r[1]))
            self._cache[key] = ([(r[0], r[1]) for r in rows], [r[2] for r in rows])
        return self._cache[key]

    def page(self, entity: str, since: dt.datetime, before: dt.datetime | None, limit: int, cursor: str | None):
        clock = (before + self.FOLLOW_LAG if before else None) if self.follow else self.now
        keys, recs = self.view(entity, clock)
        start = bisect.bisect_left(keys, (since, ""))
        if cursor:
            at, rid = decode_cursor(cursor)
            start = max(start, bisect.bisect_right(keys, (at, rid)))
        stop = len(keys) if before is None else bisect.bisect_left(keys, (before, ""))
        chunk = list(range(start, min(stop, start + limit)))
        nxt = encode_cursor(*keys[chunk[-1]]) if chunk and chunk[-1] + 1 < stop else None
        return [recs[i] for i in chunk], nxt


ORDER_FIELDS = ["order_id", "customer_id", "status", "created_at", "updated_at", "ship_city", "payment_method",
                "is_test", "items", "coupon_code", "discount_paise"]


def encode_cursor(at: dt.datetime, rid: str) -> str:
    return base64.urlsafe_b64encode(f"{at.isoformat()}|{rid}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[dt.datetime, str]:
    at, rid = base64.urlsafe_b64decode(cursor.encode()).decode().split("|", 1)
    return parse_ts(at), rid


class Faults:
    def __init__(self, rate: float, seed: str):
        self.rate, self.rng, self.lock = rate, random.Random(seed), threading.Lock()

    def next(self) -> int | None:
        with self.lock:
            if self.rate <= 0 or self.rng.random() >= self.rate:
                return None
            return 429 if self.rng.random() < 0.5 else 503


def make_handler(data: SourceData, token: str, faults: Faults):
    class Handler(BaseHTTPRequestHandler):
        server_version = "BazaarBoxSourceStub/1.0"

        def log_message(self, fmt, *args):  # one short line per request, never the Authorization header
            if os.environ.get("SOURCE_QUIET") != "1":
                print(f"source-api {self.command} {urlparse(self.path).path} {args[1] if len(args) > 1 else ''}", flush=True)

        def send(self, status: int, body: dict, headers: dict | None = None, problem: bool = False):
            raw = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/problem+json" if problem else "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def problem(self, status: int, title: str, detail: str, headers: dict | None = None):
            self.send(status, {"type": "about:blank", "title": title, "status": status, "detail": detail}, headers, True)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/health":
                return self.send(200, {"status": "ok", "clock": "follow" if data.follow else (data.now.isoformat() if data.now else "end")})
            entity = {"/v1/orders": "orders", "/v1/customers": "customers"}.get(url.path)
            if entity is None:
                return self.problem(404, "Not Found", f"no route {url.path}")
            if self.headers.get("Authorization") != f"Bearer {token}":
                return self.problem(401, "Unauthorized", "missing or wrong bearer token")
            fault = faults.next()
            if fault == 429:
                return self.problem(429, "Too Many Requests", "rate limited, retry later", {"Retry-After": "1"})
            if fault == 503:
                return self.problem(503, "Service Unavailable", "upstream hiccup, retry later")
            q = {k: v[-1] for k, v in parse_qs(url.query).items()}
            try:
                since = parse_ts(q.get("updated_since", "1970-01-01T00:00:00Z"))
                before = parse_ts(q["updated_before"]) if "updated_before" in q else None
                limit = int(q.get("limit", "100"))
                if not 1 <= limit <= MAX_LIMIT:
                    raise ValueError(f"limit must be 1..{MAX_LIMIT}")
                rows, nxt = data.page(entity, since, before, limit, q.get("cursor"))
            except (ValueError, KeyError) as exc:
                return self.problem(400, "Bad Request", str(exc))
            return self.send(200, {"data": rows, "next_cursor": nxt})

    return Handler


def serve(data_dir: Path, host: str = "127.0.0.1", port: int = 18140, now: str | None = None,
          token: str | None = None, fault_rate: float = 0.0, fault_seed: str = "faults") -> ThreadingHTTPServer:
    """Create (not start) the server; call ``serve_forever()`` (tests run it in a thread)."""
    data = SourceData(data_dir, now)
    handler = make_handler(data, token or DEFAULT_TOKEN, Faults(fault_rate, fault_seed))
    server = ThreadingHTTPServer((host, port), handler)
    server.data = data  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m bazaarbox_source serve")
    ap.add_argument("--data", default=os.environ.get("SOURCE_DATA_DIR", "source-data"), type=Path)
    ap.add_argument("--host", default=os.environ.get("SOURCE_HOST", "127.0.0.1"))
    ap.add_argument("--port", default=int(os.environ.get("SOURCE_PORT", "18140")), type=int)
    args = ap.parse_args(argv)
    server = serve(args.data, args.host, args.port, os.environ.get("SOURCE_NOW") or None,
                   os.environ.get("SOURCE_API_TOKEN") or None, float(os.environ.get("SOURCE_FAULT_RATE", "0")),
                   os.environ.get("SOURCE_FAULT_SEED", "faults"))
    print(f"BazaarBox source API on http://{args.host}:{args.port} (data {args.data})", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()
    return 0
