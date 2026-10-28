"""Fixes from the third (six-part) code review: what one odd value could break (request log, content log,
web fetch, 2FA codes), "allow without key", the usage chunk QueueRouter asks for itself, rate-limit memory."""

import asyncio
import json
import time

import httpx
import pytest

from app import content_log, totp, websearch
from app.auth import ANONYMOUS, find_client
from app.config import Bootstrap, ClientKey, ClientPolicy
from app.ratelimit import RateLimiter
from app.state import load_snapshot, open_store
from app.store import hash_key
from app.upstream import UsageChunkFilter
from tests.test_store import SEED


def test_a_lone_surrogate_does_not_lose_the_request_log(tmp_path):
    """JSON allows "\\ud800", SQLite (UTF-8) doesn't: the value is kept with "?" and the rest of the write too."""
    store = open_store(Bootstrap(data_dir=str(tmp_path)), SEED)
    now = time.time()
    store.record_requests(
        [
            {
                "id": "qr-a",
                "ts": now,
                "client": "opencode",
                "model": "m",
                "outcome": "done",
                "stream": False,
                "retries": 0,
                "client_headers": json.dumps({"body.user": "\ud800"}, ensure_ascii=False),
            },
            {"id": "qr-b", "ts": now, "client": "client2", "model": "m\udfff", "outcome": "done", "stream": 0}
            | {"retries": 0},
        ]
    )
    rows = {r["id"]: r for r in store.list_requests()}
    assert set(rows) == {"qr-a", "qr-b"} and rows["qr-b"]["model"] == "m?"
    assert len(store.query("SELECT * FROM usage_daily")) == 2
    store.save_contents([("qr-a", now, "opencode", "req \ud800", "reply", False)])
    assert store.get_content("qr-a")["request"] == "req ?"


def test_content_log_reads_odd_streams():
    """Content as a list of parts, tool arguments as an object, a choice that isn't an object: all readable."""
    chunks = [
        {"choices": [{"delta": {"content": [{"type": "text", "text": "hi "}]}}]},
        {"choices": [{"delta": {"content": "there"}}]},
        {"choices": [{"delta": {"tool_calls": [{"id": "c1", "function": {"name": "f", "arguments": {"a": 1}}}]}}]},
        {"choices": ["odd", {"delta": None, "finish_reason": "stop"}]},
        {"choices": [], "usage": {"prompt_tokens": 1}},
    ]
    raw = "".join(f"data: {json.dumps(c)}\n\n" for c in chunks).encode()
    reply = json.loads(content_log.readable_reply(raw))
    assert reply["content"] == "hi there" and reply["finish_reason"] == "stop"
    assert reply["tool_calls"] == [{"id": "c1", "name": "f", "arguments": '{"a": 1}'}]
    assert "reasoning" not in reply and reply["usage"] == {"prompt_tokens": 1}
    assert content_log.clip("x\ud800", 100) == ("x?", False)


def test_usage_chunk_filter_leaves_out_only_the_usage_chunk():
    stream = (
        b'data: {"choices":[{"delta":{"content":"a"}}],"usage":null}\n\n'
        b'data: {"choices":[],"usage":{"prompt_tokens":1}}\n\n'
        b"data: [DONE]\n\n"
    )
    held = UsageChunkFilter()
    out = b"".join(held.feed(stream[i : i + 7]) for i in range(0, len(stream), 7)) + held.flush()
    assert out == b'data: {"choices":[{"delta":{"content":"a"}}],"usage":null}\n\n\ndata: [DONE]\n\n'


async def test_fetch_turns_every_bad_address_into_a_search_error(monkeypatch):
    async def resolved(host):
        return None  # connect by name: the mock transport answers

    monkeypatch.setattr(websearch, "public_host", resolved)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"abc", headers={"content-type": "text/plain; charset=base64"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        for url in ("http://[::1/", "http://example.com:99999/", "https://example.com/page"):
            with pytest.raises(websearch.SearchError):
                await websearch.fetch(client, url)

    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(2)
        return httpx.Response(200, text="late", headers={"content-type": "text/plain"})

    monkeypatch.setattr(websearch, "FETCH_TOTAL_S", 0.2)
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        with pytest.raises(websearch.SearchError, match="longer than"):
            await websearch.fetch(client, "https://example.com/slow")


def test_two_factor_codes_in_other_digits():
    secret = totp.new_secret()
    code = totp.code_at(secret, int(time.time() // totp.STEP_S))
    arabic = "".join("٠١٢٣٤٥٦٧٨٩"[int(d)] for d in code)
    assert totp.verify(secret, arabic) is not None
    assert totp.verify(secret, "٠" * 5) is None


def test_allow_without_key_only_while_no_client_exists(tmp_path):
    key = ClientKey("x", hash_key("k"), "normal", ClientPolicy())
    assert find_client(None, {}, True, any_clients=False) is ANONYMOUS
    assert find_client(None, {}, True, any_clients=True) is None  # every client disabled or expired
    assert find_client("k", {key.key_hash: key}, True, any_clients=True) is key
    store = open_store(Bootstrap(data_dir=str(tmp_path)), SEED)
    for client in store.list_clients():
        store.update_client(client.id, "test", enabled=False)
    snap = load_snapshot(store)
    assert snap.clients == {} and snap.any_clients


def test_rate_limit_history_is_pruned_for_clients_without_limits():
    limiter = RateLimiter()
    for t in range(5):
        limiter.record_request("free", now=float(t))
        limiter.record_tokens("free", 10, now=float(t))
    limiter.record_request("free", now=100_000.0)
    limiter.record_tokens("free", 10, now=100_000.0)
    assert len(limiter._requests["free"]) == 1 and len(limiter._tokens["free"]) == 1
