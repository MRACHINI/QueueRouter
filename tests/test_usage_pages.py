"""Requests and Usage pages: what each request and model cost, paging, the brief live stream, and reads that
don't wait for the database lock."""

import asyncio
import json
import time

import httpx
import pytest

from app.pricing import PriceCatalog, model_costs, request_costs, summarize_usage, token_costs
from tests.test_admin import OWNER, csrf_of, login
from tests.test_usage import chat, flushed

RAW = {"zai": {"models": {"glm-4.7": {"cost": {"input": 1, "output": 2, "cache_read": 0.1}}}}}


@pytest.fixture
def catalog():
    prices = PriceCatalog()
    prices.use(PriceCatalog.slim(RAW), time.time())
    return prices


def usage_row(model, prompt, cached, completion, day="2026-10-31", client="ali"):
    return {
        "day": day,
        "client": client,
        "model": model,
        "requests": 1,
        "errors": 0,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "cached_tokens": cached,
    }


def request_row(rid, model, prompt, cached, completion, council=None):
    return {
        "id": rid,
        "council": council,
        "model": model,
        "target_model": None,
        "prompt_tokens": prompt,
        "cached_tokens": cached,
        "completion_tokens": completion,
    }


def test_token_costs_split_normal_cached_and_output(catalog):
    price = catalog.lookup("zai/glm-4.7")
    assert token_costs(price, 1_000_000, 500_000, 400_000) == pytest.approx((0.6, 0.04, 1.0))
    assert token_costs(price, 10, 0, 50) == pytest.approx((0.0, 1e-6, 0.0))  # never more cached than sent
    assert token_costs(None, 1, 1, 0) is None


def test_usage_rows_show_each_part(catalog):
    rows = [usage_row("glm-4.7", 1_000_000, 400_000, 500_000)]
    out, totals, _ = summarize_usage(rows, {}, catalog, by_model=False, plan_per_month=0, days=1)
    assert out[0]["input_tokens"] == 600_000 and out[0]["cached_tokens"] == 400_000
    assert (out[0]["cost_in"], out[0]["cost_cached"], out[0]["cost_out"]) == pytest.approx((0.6, 0.04, 1.0))
    assert totals["cost"] == pytest.approx(1.64) and out[0]["cost"] == pytest.approx(1.64)


def test_models_dearest_first_with_a_total(catalog):
    rows = [
        usage_row("unknown", 100, 0, 100),
        usage_row("glm-4.7", 1_000_000, 0, 0),
        usage_row("glm-4.7", 1_000_000, 0, 0, client="bilal"),
    ]
    models, total = model_costs(rows, {}, catalog)
    assert [m["model"] for m in models] == ["glm-4.7", "unknown"]
    assert models[0]["cost"] == pytest.approx(2) and models[0]["requests"] == 2 and models[0]["priced"]
    assert not models[1]["priced"] and total["cost"] == pytest.approx(2) and total["unpriced_tokens"] == 200


def test_a_council_adds_up_its_calls(catalog):
    rows = [request_row("c1", "my-council", 5, 0, 5, council="pick 2/3"), request_row("r1", "glm-4.7", 1_000_000, 0, 0)]
    calls = [
        {**request_row("m1", "glm-4.7", 1_000_000, 0, 0), "parent": "c1"},
        {**request_row("m2", "glm-4.7", 0, 0, 1_000_000), "parent": "c1"},
    ]
    council, plain = request_costs(rows, calls, {}, catalog)
    assert council["calls"] == 2 and council["cost"] == pytest.approx(3) and council["tokens_out"] == 1_000_000
    assert plain["calls"] == 0 and plain["cost"] == pytest.approx(1)


def test_requests_without_tokens_or_a_price(catalog):
    failed, unpriced = request_costs(
        [request_row("r1", "glm-4.7", None, None, None), request_row("r2", "unknown", 10, 0, 5)], [], {}, catalog
    )
    assert failed["tokens_in"] is None and failed["cost"] is None
    assert unpriced["tokens_in"] == 10 and unpriced["cost"] == 0 and unpriced["priced"] is False


def stored_row(rid, ts, **fields):
    return {"id": rid, "ts": ts, "client": "ali", "model": "m", "stream": 0, "retries": 0, "outcome": "done", **fields}


async def test_paging_keeps_rows_with_the_same_time(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    now = time.time()
    store.record_requests([stored_row(f"r{i}", now) for i in range(60)])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        first = (await c.get("/admin/api/requests")).json()
        assert len(first["rows"]) == 50 and first["more"] is True
        last = first["rows"][-1]
        rest = (await c.get(f"/admin/api/requests?before={last['ts']}&before_id={last['id']}")).json()
        assert len(rest["rows"]) == 10 and rest["more"] is False
        seen = [r["id"] for r in first["rows"] + rest["rows"]]
        assert sorted(seen) == sorted(f"r{i}" for i in range(60))


async def test_catching_up_says_when_rows_were_left_out(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    now = time.time()
    store.record_requests([stored_row(f"r{i}", now + i) for i in range(301)])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        caught_up = (await c.get(f"/admin/api/requests?after={now}")).json()
        assert len(caught_up["rows"]) == 300 and caught_up["more"] is True
        assert caught_up["rows"][0]["id"] == "r300"


async def test_rows_cost_and_details(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        rates = {"input": "1", "cached_input": "0.1", "output": "2"}
        await c.post("/admin/prices", data={"csrf": csrf, "model": "mock-model", "reference": "", **rates})
        body = chat("priced", mock_cached=4)
        await c.post("/v1/chat/completions", headers=stack.headers, json=body)
        await flushed(stack, 1)
        row = (await c.get("/admin/api/requests")).json()["rows"][0]
        assert (row["tokens_in"], row["tokens_cached"], row["tokens_out"]) == (6, 4, 5)
        assert row["cost_in"] == pytest.approx(6e-6) and row["cost_cached"] == pytest.approx(4e-7)
        assert row["cost_out"] == pytest.approx(1e-5) and row["cost"] == pytest.approx(1.64e-5)
        assert "client_headers" not in row and "timeline" not in row  # those wait for the row to be opened

        details = await c.get(f"/admin/api/requests/{row['id']}")
        assert details.status_code == 200 and details.json()["client_headers"] == {}
        assert "ip" not in details.json() and "timeline" not in details.json()
        assert (await c.get("/admin/api/requests/nope")).status_code == 404


async def test_filter_choices_are_kept_for_a_while(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        assert (await c.get("/admin/api/requests/filters")).json()["client"] == []
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))
        await flushed(stack, 1)
        assert (await c.get("/admin/api/requests/filters")).json()["client"] == []  # not scanned again yet


async def test_usage_body_and_model_range(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    day = 86_400
    now = time.time()
    store.record_requests(
        [
            stored_row("today", now, model="model-today", prompt_tokens=10, completion_tokens=1),
            stored_row("week", now - 3 * day, model="model-week", prompt_tokens=10, completion_tokens=1),
            stored_row("month", now - 20 * day, model="model-month", prompt_tokens=10, completion_tokens=1),
        ]
    )
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/usage")).text
        assert "<html" in page and all(f"model-{name}" in page for name in ("today", "week", "month"))
        assert 'data-model-days="30" aria-current="true"' in page  # the default

        body = (await c.get("/admin/usage/body?model_days=1")).text
        assert "<html" not in body and 'data-model-days="1" aria-current="true"' in body
        table = body.split("Cost per model")[1].split("Per day")[0]
        assert "model-today" in table and "model-week" not in table
        table = (await c.get("/admin/usage/body?model_days=7")).text.split("Cost per model")[1].split("Per day")[0]
        assert "model-week" in table and "model-month" not in table
        assert 'data-model-days="30" aria-current="true"' in (await c.get("/admin/usage/body?model_days=5")).text


async def test_a_period_counts_its_days_with_today(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    now = time.time()
    store.record_requests(
        [stored_row("a", now, prompt_tokens=1), stored_row("b", now - 86_400, prompt_tokens=1)],
    )
    assert len(store.usage(1)) == 1
    assert len(store.usage(2)) == 2


async def test_brief_stream_holds_only_what_the_pages_use(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        async with c.stream("GET", "/admin/api/live?view=brief") as live:
            async for line in live.aiter_lines():
                if line.startswith("data: "):
                    assert sorted(json.loads(line[6:])) == ["inflight", "usage_version"]
                    break


async def test_the_lists_dont_wait_for_the_write_lock(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    store.record_requests([stored_row("r", time.time())])
    with store._lock:  # what a long write or a backup holds
        rows = await asyncio.wait_for(asyncio.to_thread(store.list_requests, 10), 5)
        usage = await asyncio.wait_for(asyncio.to_thread(store.usage, 30), 5)
    assert [r["id"] for r in rows] == ["r"] and len(usage) == 1


async def test_the_read_connection_cant_write(make_stack):
    stack = make_stack(bootstrap=OWNER)
    with pytest.raises(Exception, match="readonly"):
        stack.app.state.store.read("DELETE FROM requests")
