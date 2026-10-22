"""Choosing which models an upstream offers; provider error text on every failed request; the
dashboard's per-client tokens split."""

import asyncio

import httpx

from app.catalog import make_filter, parse_filter
from tests.test_admin import OWNER, csrf_of, login
from tests.test_aliases import add_alias


def test_filter_storage():
    listed = {"a", "b", "c"}
    assert make_filter(listed, {"a", "b", "c"}, True) == ""  # everything: the default
    assert parse_filter(make_filter(listed, {"a", "b"}, True)) == (True, frozenset({"c"}))  # hide c, offer new
    assert parse_filter(make_filter(listed, {"a"}, False)) == (False, frozenset({"a"}))  # only a
    assert parse_filter("") is None and parse_filter("junk") is None


async def choose(c, upstream_id, models, offer_new=True):
    page = (await c.get(f"/admin/upstreams/{upstream_id}/models")).text
    form = {"csrf": csrf_of(page), **{f"model_{i}": m for i, m in enumerate(models)}}
    if offer_new:
        form["offer_new"] = "on"
    r = await c.post(f"/admin/upstreams/{upstream_id}/models", data=form)
    assert "m=saved" in r.headers["location"], r.headers["location"]


async def model_ids(c, stack):
    return sorted(m["id"] for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"])


async def test_hidden_models_leave_every_list(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model", "other-model"]
    upstream = stack.app.state.store.list_upstreams()[0]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get(f"/admin/upstreams/{upstream.id}/models")).text
        assert page.count('type="checkbox" name="model_') == 3 and "3 of 3 offered" in page.replace("</span>", "")

        await choose(c, upstream.id, ["mock-model", "small-model"])
        assert parse_filter(stack.app.state.store.list_upstreams()[0].model_filter) == (
            True,
            frozenset({"other-model"}),
        )
        assert await model_ids(c, stack) == ["mock-model", "small-model"]
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert "other-model" not in config["provider"]["queuerouter"]["models"]
        chat = {"model": "other-model", "messages": [{"role": "user", "content": "hi"}]}
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat)).status_code == 404
        assert "2 of 3 models offered" in (await c.get("/admin/upstreams")).text

        await add_alias(c, "o", "other-model")  # an alias may still use it
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "model": "o"})
        assert r.status_code == 200 and stack.mock["models"][-1] == "other-model"
        assert "o" in await model_ids(c, stack)

        # new models: offered automatically, or not
        stack.mock["extra_models"].append("brand-new")
        assert "brand-new" in await model_ids(c, stack)
        await choose(c, upstream.id, ["mock-model"], offer_new=False)
        stack.mock["extra_models"].append("newer")
        assert await model_ids(c, stack) == ["mock-model", "o"]

        await choose(c, upstream.id, ["mock-model", "small-model", "other-model", "brand-new", "newer"])
        assert stack.app.state.store.list_upstreams()[0].model_filter == ""  # all ticked: back to the default


async def test_adding_an_upstream_opens_its_model_choice(make_stack, make_mock):
    stack = make_stack(bootstrap=OWNER)
    url, _ = make_mock()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        form = {
            "csrf": csrf,
            "preset": "custom",
            "name": "second",
            "base_url": url,
            "api_key": "k",
            "pool": "new",
            "max_concurrency": "1",
            "weight": "1",
        }
        r = await c.post("/admin/upstreams", data=form)
        assert "/admin/upstreams/" in r.headers["location"] and "/models?m=created" in r.headers["location"]


async def test_provider_error_text_recorded(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    base = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**base, "mock_status": 400})
        assert r.status_code == 400
        async with c.stream(
            "POST", "/v1/chat/completions", headers=stack.headers, json={**base, "mock_status": 422, "stream": True}
        ) as s:
            _ = [chunk async for chunk in s.aiter_bytes()]
        retried = {**base, "mock_key": "k1", "mock_fail": 1}  # a 503, then success
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=retried)).status_code == 200
        await login(c, stack.url)
        dashboard = (await c.get("/admin")).text
        assert dashboard.count("<th>In tokens</th><th>Out tokens</th>") == 2  # per client and recent requests
    for _ in range(50):
        rows = await asyncio.to_thread(store.list_requests, 10)
        if len(rows) == 3:
            break
        await asyncio.sleep(0.1)
    errors = sorted(r["error"] or "" for r in rows)
    assert errors == [
        "HTTP 400 from upstream: forced",
        "HTTP 422 from upstream: forced",
        "HTTP 503 from upstream: temporarily unavailable",
    ]
