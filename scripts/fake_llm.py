#!/usr/bin/env python3
"""A tiny OpenAI-compatible endpoint for tests and the smoke test (no model, no network, no key).

    python3 scripts/fake_llm.py --port 18146 [--mode good|url|numbers|slow]

good    : answers citing facts [1] and [2] (passes the guardrails)
url     : answers with a link (the guardrails must reject it and fall back to the template)
numbers : invents a number that is in no fact (rejected)
slow    : sleeps longer than any sane AI_TIMEOUT (the explainer must time out and fall back)
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ANSWERS = {
    "good": "The build was not published because a blocking check failed [1]. Look at the values it reports [2] and "
            "fix the upstream mapping before re-running the day.",
    "url": "A blocking check failed [1]. See https://example.com/runbook for details.",
    "numbers": "A blocking check failed [1]; about 98765 orders are affected.",
}


def make_handler(mode: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # quiet: never echo request headers (they may carry a key)
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            json.loads(self.rfile.read(length) or b"{}")
            if mode == "slow":
                time.sleep(30)
            body = json.dumps({"choices": [{"message": {"role": "assistant", "content": ANSWERS.get(mode, ANSWERS["good"])}}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.send_response(200 if self.path == "/health" else 404)
            self.end_headers()

    return Handler


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18146)
    ap.add_argument("--mode", default="good", choices=["good", "url", "numbers", "slow"])
    args = ap.parse_args()
    ThreadingHTTPServer((args.host, args.port), make_handler(args.mode)).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
