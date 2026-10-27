"""Response times: the provider's own timings per streamed request, the Response times page, and the
slow-first-token alert."""

import asyncio
import json
import time

import httpx

from app import latency
from app.upstream import UsageSniffer
from tests.test_admin import OWNER, login
from tests.test_usage import chat, flushed


def event(delta: dict, spaced: bool = False) -> bytes:
    chunk = {"choices": [{"index": 0, "delta": delta, "finish_reason": None}]}
    text = json.dumps(chunk) if spaced else json.dumps(chunk, separators=(",", ":"))
    return f"data: {text}\n\n".encode()


def test_sniffer_times_thinking_answer_speed_and_pauses():
    s = UsageSniffer()
    s.feed(event({"role": "assistant", "content": ""}), 10.2)  # an empty opener isn't a token
    s.feed(event({"reasoning_content": "Let me think"}), 11.0)
    s.feed(event({"content": "Hello", "reasoning": None}, spaced=True), 13.0)  # OpenRouter-style, spaced JSON
    s.feed(event({"tool_calls": [{"index": 0, "function": {"arguments": '{"a"'}}]}), 13.5)
    usage = {"choices": [], "usage": {"prompt_tokens": 100, "completion_tokens": 25}}
    s.feed(f"data: {json.dumps(usage)}\n\ndata: [DONE]\n\n".encode(), 14.0)
    assert s.timings(10.0) == {
        "first_token_ms": 1000,
        "answer_ms": 3000,
        "stall_ms": 2000,
        "tokens_per_s": 10.0,  # 25 tokens from 11.0 to 13.5
    }
    assert UsageSniffer().timings(10.0) == {}  # no tokens: nothing to say
    few = UsageSniffer()
    few.feed(event({"content": "x"}), 1.0)
    few.feed(event({"content": "y"}), 1.1)
    assert "tokens_per_s" not in few.timings(0.5)  # too short to tell a speed


def test_summaries_percentiles_and_slow_pairs():
    assert latency.percentile([], 50) is None
    assert latency.percentile([5, 1, 3], 50) == 3 and latency.percentile(list(range(1, 101)), 95) == 95

    def row(first, **extra):
        timings = {"headers_ms": 100, "first_token_ms": first, "answer_ms": None, "tokens_per_s": 50.0}
        base = {"ts": time.time(), "model": "m", "upstream": "u", "outcome": "done", "stall_ms": 10, "backoff_ms": None}
        return {**base, **timings, **extra}

    rows = [row(1000 * i) for i in range(1, 11)] + [row(None, outcome="error", backoff_ms=4000)]
    rows += [row(500, model="other", answer_ms=2500)]
    first, other = latency.summarize(rows)
    assert (first.model, first.requests, first.timed) == ("m", 11, 10)
    assert first.first_med == 5000 and first.first_p95 == 10000 and first.thinking_med is None
    assert first.retried == 1 and first.backoff_s == 4
    assert other.thinking_med == 2000  # it thinks: 2 s from its first token to the answer
    assert [(s.model, round(w)) for s, w in latency.slow_pairs(rows, 9)] == [("m", 10)]
    assert latency.slow_pairs(rows, 10) == []  # not past the limit
    assert latency.slow_pairs(rows[:3], 0) == []  # too few requests to say


async def test_streamed_requests_are_timed_and_shown(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        body = chat("t", stream=True, mock_think=3, mock_chunks=20, mock_delay=0.04)
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            _ = [x async for x in r.aiter_bytes()]
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("plain"))  # not streamed: not timed
        await flushed(stack, 2)
        plain, streamed = await asyncio.to_thread(store.list_requests, 10)
        assert plain["first_token_ms"] is None and plain["headers_ms"] is None
        assert streamed["headers_ms"] is not None
        assert 0 < streamed["first_token_ms"] < streamed["answer_ms"]  # it thought first
        assert streamed["tokens_per_s"] > 10 and streamed["stall_ms"] >= 0

        page = await c.get("/admin/response-times")
        assert page.status_code == 200 and "mock-model" in page.text and "tok/s" in page.text
        assert '<polyline class="line l1"' in page.text or '<circle class="dot l1"' in page.text
        assert (await c.get("/admin/response-times?window=1h")).status_code == 200
        assert (await c.get("/admin/response-times?window=forever")).status_code == 200  # falls back to 24h
        listing = (await c.get("/admin/requests")).text
        assert "tok/s" in listing and f"token {streamed['first_token_ms']}" in listing

        alerts = stack.app.state.alerts
        store.save_alert_rule("slow_first_token", True, 0.001, 3600, 3600, "t")
        alerts.reload()
        for _ in range(latency.ALERT_MIN_REQUESTS):
            async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
                _ = [x async for x in r.aiter_bytes()]
        await flushed(stack, 2 + latency.ALERT_MIN_REQUESTS)
        alerts._last_slow = 0  # as if the last check was long ago (they're 10 minutes apart)
        await alerts.check_slow()
        await alerts.drain()
        titles = [n["title"] for n in store.list_notifications()]
        assert any("mock-model" in t and "slow to start" in t for t in titles), titles
