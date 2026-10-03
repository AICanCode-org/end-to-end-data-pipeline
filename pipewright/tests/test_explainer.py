"""The AI incident explainer: template fallback, guardrails, provider failures, and no secrets or rows sent."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from pipewright import explainer

pytestmark = pytest.mark.group("Phase 6 - AI incident explainer")


def report(failing=True):
    checks = [{"id": "orders_not_empty", "kind": "row_count_min", "table": "fct_orders", "severity": "error", "status": "pass",
               "observed": 10, "threshold": 1, "detail": "fct_orders has 10 rows (min 1)", "unexpected_values": []}]
    if failing:
        checks.append({"id": "status_accepted", "kind": "accepted_values", "table": "fct_orders", "severity": "error",
                       "status": "fail", "observed": 293, "threshold": 0,
                       "detail": "293 row(s) of fct_orders.status outside the accepted values", "unexpected_values": ["rto"]})
        checks.append({"id": "item_sku_known", "kind": "relationship", "table": "fct_order_items", "severity": "warn",
                       "status": "fail", "observed": 92, "threshold": 0, "detail": "92 row(s) have no match", "unexpected_values": []})
    n_fail = 2 if failing else 0
    return {"run_date": "2026-09-30", "track": "sql", "contract_version": "1.0.0", "checks": checks,
            "summary": {"checks": len(checks), "passed": len(checks) - n_fail, "failed_error": 1 if failing else 0,
                        "failed_warn": 1 if failing else 0, "publishable": not failing}}


class FakeLLM:
    def __init__(self, answer, status=200, delay=0.0):
        self.answer, self.status, self.delay, self.requests = answer, status, delay, []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                import time

                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.requests.append({"body": body, "auth": self.headers.get("Authorization")})
                time.sleep(outer.delay)
                raw = json.dumps({"choices": [{"message": {"content": outer.answer}}]}).encode()
                self.send_response(outer.status)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def env(self, **extra):
        return {"AI_PROVIDER": "openai", "AI_BASE_URL": self.url, "AI_MODEL": "fake-1", "AI_API_KEY": "sk-test-123",
                "AI_TIMEOUT": "2", **extra}


def test_template_is_the_default_and_cites_facts():
    e = explainer.explain(report(), env={})
    assert e.source == "template" and "status_accepted" in e.text and "[3]" in e.text
    assert "BLOCKED" in e.facts[1] and "unexpected values: rto" in e.facts[2]
    assert explainer.validate(e.text, e.facts) is None, "the template itself passes the guardrails"


def test_nothing_failed_needs_no_model():
    e = explainer.explain(report(failing=False), env={"AI_PROVIDER": "openai", "AI_BASE_URL": "http://x", "AI_MODEL": "m"})
    assert e.source == "template" and "All checks passed" in e.text


def test_missing_configuration_falls_back():
    assert "not set" in explainer.explain(report(), env={"AI_PROVIDER": "openai"}).reason


def test_good_llm_answer_is_used_and_only_facts_are_sent():
    llm = FakeLLM("Status codes changed: 293 orders carry rto [3]. Publishing is blocked [2]; version the contract [1].")
    e = explainer.explain(report(), env=llm.env())
    assert e.source == "llm", e.reason
    sent = llm.requests[0]
    assert sent["auth"] == "Bearer sk-test-123"
    prompt = json.dumps(sent["body"])
    assert "C-0" not in prompt and "O-0" not in prompt, "no customer or order ids leave the box"
    assert "sk-test-123" not in prompt and "sk-test-123" not in explainer.render(e)


@pytest.mark.parametrize(
    "answer, reason",
    [
        ("Everything is fine.", "cites no fact"),
        ("See fact [9].", "does not exist"),
        ("1234 orders were lost [3].", "numbers not in the facts"),
        ("Run DELETE FROM fct_orders to fix it [3].", "data-changing SQL"),
        ("Details at https://evil.example [3].", "URL"),
        ("[3] " + "x" * 3000, "too long"),
        ("", "empty"),
    ],
)
def test_guardrails_reject_bad_answers(answer, reason):
    e = explainer.explain(report(), env=FakeLLM(answer).env())
    assert e.source == "template" and reason in e.reason


def test_provider_errors_and_timeouts_fall_back():
    assert "provider unavailable" in explainer.explain(report(), env=FakeLLM("x", status=500).env()).reason
    slow = FakeLLM("fine [3]", delay=1.5)
    assert "provider unavailable" in explainer.explain(report(), env=slow.env(AI_TIMEOUT="0.3")).reason
    dead = {"AI_PROVIDER": "openai", "AI_BASE_URL": "http://127.0.0.1:9/v1", "AI_MODEL": "m", "AI_TIMEOUT": "0.5"}
    assert "provider unavailable" in explainer.explain(report(), env=dead).reason


def test_render_lists_facts_and_source():
    out = explainer.render(explainer.explain(report(), env={}))
    assert "Source: template" in out and "**Facts**" in out and "3. Check status_accepted" in out


def test_extra_facts_are_numbered_after_the_checks():
    facts, failed = explainer.facts_from(report(), ["Source payments is 2 days late."])
    assert facts[-1] == "Source payments is 2 days late." and len(failed) == 2
