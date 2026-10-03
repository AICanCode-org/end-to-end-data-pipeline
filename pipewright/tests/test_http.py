"""The retrying HTTP client against the real stub with fault injection, and against a closed port."""

import random
import threading

import pytest

from pipewright.http import HttpClient, SourceError

pytestmark = pytest.mark.group("Phase 3 M1 - ingestion")


@pytest.fixture
def faulty():
    from bazaarbox_source.api import serve

    from .conftest import FIXTURE

    server = serve(FIXTURE, port=0, fault_rate=0.5, fault_seed="test")
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def params():
    return {"updated_since": "2026-06-01T00:00:00+05:30", "updated_before": "2026-06-04T00:00:00+05:30", "limit": 5}


def test_retries_through_429_and_503(faulty):
    sleeps = []
    client = HttpClient(faulty, "bazaarbox-dev-token", max_attempts=10, sleep=sleeps.append, rng=random.Random(1))
    for _ in range(5):
        body = client.get_json("/v1/orders", params())
        assert len(body["data"]) == 5
    assert client.retries > 0 and len(sleeps) == client.retries
    assert all(0 < s <= 8 for s in sleeps)


def test_wrong_token_fails_fast_without_retrying(stub):
    client = HttpClient(f"http://127.0.0.1:{stub.server_address[1]}", "wrong", sleep=lambda s: pytest.fail("slept"))
    with pytest.raises(SourceError, match="SOURCE_API_TOKEN"):
        client.get_json("/v1/orders", params())
    assert client.attempts == 1


def test_bad_request_is_not_retried(stub):
    client = HttpClient(f"http://127.0.0.1:{stub.server_address[1]}", "bazaarbox-dev-token", sleep=lambda s: pytest.fail("slept"))
    with pytest.raises(SourceError, match="400"):
        client.get_json("/v1/orders", {"limit": 9999})


def test_gives_up_after_max_attempts_on_connection_errors():
    sleeps = []
    client = HttpClient("http://127.0.0.1:9", "t", timeout=0.5, max_attempts=3, sleep=sleeps.append)
    with pytest.raises(SourceError, match="giving up after 3 attempts"):
        client.get_json("/v1/orders", params())
    assert len(sleeps) == 2


def test_backoff_grows_and_respects_retry_after():
    client = HttpClient("http://x", "t", backoff=0.5, max_backoff=8, rng=random.Random(0))
    assert 0.25 <= client.delay(1, None) <= 0.5
    assert 2 <= client.delay(4, None) <= 4
    assert client.delay(10, None) <= 8
    assert client.delay(1, "3") == 3
    assert client.delay(1, "120") == 8


def test_repr_hides_the_token():
    assert "secret" not in repr(HttpClient("http://x", "secret"))
