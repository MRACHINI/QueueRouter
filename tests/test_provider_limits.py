"""Provider quota errors, per-upstream rules and retry limits, unknown models, the pricing table."""

import json

import httpx

from app.upstream import describe_429
from tests.mock_upstream import _google_429
from tests.test_admin import OWNER, csrf_of, login
from tests.test_fallback import add_chain

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


def test_describe_429():
    message, hard, wait = describe_429(
        json.dumps(_google_429("GenerateRequestsPerDayPerProjectPerModel-FreeTier", "45s")).encode()
    )
    assert hard and wait == 45 and "exceeded your current quota" in message
    _, hard, wait = describe_429(json.dumps(_google_429("GenerateRequestsPerMinutePerProjectPerModel", "7s")).encode())
    assert not hard and wait == 7
    openai_style = {"error": {"message": "You exceeded your current quota", "type": "insufficient_quota"}}
    assert describe_429(json.dumps(openai_style).encode())[1]
    assert describe_429(b"Too Many Requests") == ("Too Many Requests", False, None)
    assert describe_429(b'{"error": {"message": "slow down"}}') == ("slow down", False, None)


def set_upstream(stack, **fields):
    store = stack.app.state.store
    store.update_upstream(store.list_upstreams()[0].id, "test", **fields)
    stack.app.state.reload()


async def test_daily_quota_fails_fast(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        body = {**CHAT, "mock_429_models": {"mock-model": "daily"}}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=body)
        assert r.status_code == 429 and r.json()["error"]["type"] == "upstream_quota_exhausted"
        assert "exceeded your current quota" in r.json()["error"]["message"]
        sent = len(stack.mock["models"])
        # the next request doesn't even reach the provider
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)
        assert r.status_code == 429 and "used up" in r.json()["error"]["message"]
        assert len(stack.mock["models"]) == sent


async def test_daily_quota_falls_back(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "mock-model", ["other-model"], ["unavailable", "errors"])
        body = {**CHAT, "model": "smart", "mock_429_models": {"mock-model": "daily"}}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=body)
        assert r.status_code == 200 and r.headers["x-queuerouter-model"] == "other-model"


async def test_rate_limit_retries_and_upstream_limit(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        # a per-minute limit is retried (using the provider's suggested wait) and then succeeds
        body = {**CHAT, "stream": True, "mock_429_times": 2, "mock_key": "a"}
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as s:
            text = "".join([chunk async for chunk in s.aiter_text()])
        assert "HTTP 429 (You exceeded your current quota" in text and "t4" in text

        # the upstream's own retry limit wins: 1 retry, then the provider's 429 is passed through
        set_upstream(stack, max_retries=1)
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json={**CHAT, "mock_429_times": 5, "mock_key": "b"}
        )
        assert r.status_code == 429 and isinstance(r.json(), list)  # Google's own body
        assert stack.mock["fails"]["429:b"] == 2


async def test_upstream_parameter_rules(make_stack):
    stack = make_stack()
    set_upstream(stack, params=json.dumps({"strip": ["store"], "defaults": {"temperature": 0.3}}))
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "store": False})
        assert r.status_code == 200
    sent = json.loads(stack.mock["last_body"])
    assert "store" not in sent and sent["temperature"] == 0.3


async def test_unknown_model(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)  # model lists are known
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "model": "mock-modle"})
        assert r.status_code == 404 and r.json()["error"]["type"] == "model_not_found"
        assert "did you mean mock-model" in r.json()["error"]["message"]

        stack.app.state.store.set_tunables({"unknown_model_check": False}, "test")
        stack.app.state.reload()
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "model": "mock-modle"})
        assert r.status_code == 200  # sent on, as before


async def test_pricing_table(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    store.record_requests(
        [{"ts": __import__("time").time(), "client": "x", "model": "old-name", "outcome": "done", "stream": False}]
    )
    store.set_model_price("old-name", "test", None, 1.0, 2.0, None)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/models")).text
        row = page.split("old-name", 1)[1][:400]
        assert "not offered" in row and "Remove price" in page
        await c.post("/admin/prices", data={"csrf": csrf_of(page), "model": "old-name"})
        assert "old-name" not in store.list_model_prices()
