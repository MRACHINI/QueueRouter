"""Per-client policies and operating modes."""

import asyncio

import httpx

from app.config import ClientPolicy
from app.ratelimit import RateLimiter
from tests.conftest import eventually
from tests.test_admin import OWNER, csrf_of, login

# -- rate limiter units ------------------------------------------------------------------------


def test_request_limits_per_minute_and_day():
    r = RateLimiter()
    policy = ClientPolicy(rpm=2, rpd=3)
    assert r.wait_s("c", policy, now=1000) == 0
    r.record_request("c", now=1000)
    r.record_request("c", now=1010)
    assert r.wait_s("c", policy, now=1020) == 40  # oldest (t=1000) leaves the minute at 1060
    assert r.wait_s("c", policy, now=1061) == 0
    r.record_request("c", now=1061)
    assert r.wait_s("c", policy, now=1200) == 1000 + 86_400 - 1200  # daily limit now binds


def test_token_limits():
    r = RateLimiter()
    policy = ClientPolicy(tpm=1000)
    r.record_tokens("c", 600, now=0)
    assert r.wait_s("c", policy, now=1) == 0
    r.record_tokens("c", 600, now=10)
    assert r.wait_s("c", policy, now=20) == 40  # back under 1000 once the first 600 expire
    assert r.usage("c", now=20)["tokens_minute"] == 1200


def test_no_policy_never_waits():
    r = RateLimiter()
    for _ in range(100):
        r.record_request("c")
    assert r.wait_s("c", ClientPolicy()) == 0


# -- end to end --------------------------------------------------------------------------------


def chat(tag, **extra):
    return {"model": "mock-model", "messages": [{"role": "user", "content": tag}], "mock_tag": tag, **extra}


async def set_policy(c, stack, **values):
    client = next(x for x in stack.app.state.store.list_clients() if x.name == "client1")
    csrf = csrf_of((await c.get("/admin/clients")).text)
    form = {"csrf": csrf, "on_limit": "delay", **{k: str(v) for k, v in values.items()}}
    r = await c.post(f"/admin/clients/{client.id}/policy", data=form)
    assert "m=saved" in r.headers["location"], r.headers["location"]


async def set_mode(c, mode, message=""):
    csrf = csrf_of((await c.get("/admin")).text)
    r = await c.post("/admin/mode", data={"csrf": csrf, "mode": mode, "message": message})
    assert "m=saved" in r.headers["location"]


async def test_allowed_models(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, allowed_models="other-model")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("x"))
        assert r.status_code == 403 and r.json()["error"]["type"] == "model_not_allowed"
        assert (await c.get("/v1/models", headers=stack.headers)).json()["data"] == []
        await set_policy(c, stack, allowed_models="other-model, mock-model")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("y"))).status_code == 200
        assert [m["id"] for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]] == ["mock-model"]


async def test_rate_limit_reject_mode(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, rpm=1, on_limit="reject")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))).status_code == 200
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("b"))
        assert r.status_code == 429 and r.json()["error"]["type"] == "rate_limited"
        assert 0 < int(r.headers["retry-after"]) <= 60
        stats = (await c.get("/admin/api/stats")).json()
        assert stats["rate_limits"]["client1"]["requests_minute"] == 1


async def test_rate_limit_delay_mode_waits_in_queue(make_stack):
    stack = make_stack(bootstrap=OWNER, keepalive_interval_s=0.1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, rpm=1)
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))).status_code == 200
        waiting = asyncio.create_task(c.post("/v1/chat/completions", headers=stack.headers, json=chat("b")))
        await asyncio.sleep(0.5)
        stats = (await c.get("/admin/api/stats")).json()
        assert stats["queued"] == 1 and stats["active"] == 0  # delayed, not rejected
        await set_policy(c, stack, rpm="")  # lifting the limit releases it
        assert (await waiting).status_code == 200


async def test_max_queued_and_max_active(make_stack):
    stack = make_stack(limit=2, bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, max_active=1, max_queued=1)
        api = "/v1/chat/completions"
        first = asyncio.create_task(c.post(api, headers=stack.headers, json=chat("1", mock_delay=0.1)))
        await eventually(lambda: stack.mock["active"] == 1)
        second = asyncio.create_task(c.post(api, headers=stack.headers, json=chat("2", mock_delay=0.05)))
        await asyncio.sleep(0.2)
        third = await c.post(api, headers=stack.headers, json=chat("3"))
        assert third.status_code == 429 and third.json()["error"]["type"] == "client_queue_full"
        assert (await first).status_code == 200 and (await second).status_code == 200
    assert stack.mock["peak"] == 1  # max_active=1 even though the pool has 2 slots


async def test_total_timeout(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, total_timeout_s=1)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("slow", mock_delay=0.5))
        assert r.status_code == 504 and r.json()["error"]["type"] == "request_timeout"
        async with c.stream(
            "POST",
            "/v1/chat/completions",
            headers=stack.headers,
            json=chat("slow-stream", stream=True, mock_delay=0.4, mock_chunks=10),
        ) as s:
            text = "".join([chunk async for chunk in s.aiter_text()])
        assert "request_timeout" in text


async def test_modes(make_stack):
    stack = make_stack(bootstrap=OWNER)
    api = "/v1/chat/completions"
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)

        await set_mode(c, "paused")
        held = asyncio.create_task(c.post(api, headers=stack.headers, json=chat("held")))
        await asyncio.sleep(0.3)
        assert stack.mock["served"] == 0 and (await c.get("/admin/api/stats")).json()["queued"] == 1
        await set_mode(c, "running")
        assert (await held).status_code == 200

        await set_mode(c, "draining", "back soon")
        r = await c.post(api, headers=stack.headers, json=chat("refused"))
        assert r.status_code == 503 and r.json()["error"]["message"] == "back soon"

        await set_mode(c, "paused")
        stranded = asyncio.create_task(c.post(api, headers=stack.headers, json=chat("stranded")))
        await asyncio.sleep(0.3)
        await set_mode(c, "maintenance", "upgrading")
        r = await stranded
        assert r.status_code == 503 and r.json()["error"]["type"] == "maintenance"
        assert "upgrading" in (await c.get("/admin")).text
        await set_mode(c, "running")
        assert (await c.post(api, headers=stack.headers, json=chat("again"))).status_code == 200
