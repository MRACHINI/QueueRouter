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
        "upstream": "syn",
        "requests": 1,
        "errors": 0,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "cached_tokens": cached,
        "cache_write_tokens": 0,
    }


def request_row(rid, model, prompt, cached, completion, council=None):
    return {
        "id": rid,
        "council": council,
        "model": model,
        "target_model": None,
        "upstream": "syn",
        "prompt_tokens": prompt,
        "cached_tokens": cached,
        "cache_write_tokens": 0,
        "completion_tokens": completion,
    }


def test_token_costs_split_normal_cached_and_output(catalog):
    price = catalog.lookup("zai/glm-4.7")
    assert token_costs(price, 1_000_000, 500_000, 400_000) == pytest.approx((0.6, 0.04, 1.0))
    assert token_costs(price, 10, 0, 50) == pytest.approx((0.0, 1e-6, 0.0))  # never more cached than sent
    assert token_costs(None, 1, 1, 0) is None


def test_usage_rows_show_each_part(catalog):
    rows = [usage_row("glm-4.7", 1_000_000, 400_000, 500_000)]
    out, totals, _ = summarize_usage(rows, {}, catalog, merged=False, plan_per_month=0, days=1)
    assert out[0]["input_tokens"] == 600_000 and out[0]["cached_tokens"] == 400_000
    assert (out[0]["cost_in"], out[0]["cost_cached"], out[0]["cost_out"]) == pytest.approx((0.6, 0.04, 1.0))
    assert totals["cost"] == pytest.approx(1.64) and out[0]["cost"] == pytest.approx(1.64)


def test_models_dearest_first_with_a_total(catalog):
    rows = [
        usage_row("unknown", 100, 0, 100),
        usage_row("glm-4.7", 1_000_000, 0, 0),
        usage_row("glm-4.7", 3_000_000, 0, 0, client="bilal"),
    ]
    models, total = model_costs(rows, {}, catalog)
    assert [m["model"] for m in models] == ["glm-4.7", "unknown"]
    assert models[0]["cost"] == pytest.approx(4) and models[0]["requests"] == 2 and models[0]["priced"]
    assert not models[1]["priced"] and total["cost"] == pytest.approx(4) and total["unpriced_tokens"] == 200
    # each model opens to the clients that used it, the dearest first, with their share of its cost
    clients = models[0]["clients"]
    assert [(c["client"], c["requests"], c["share"]) for c in clients] == [("bilal", 1, 0.75), ("ali", 1, 0.25)]
    assert models[1]["clients"][0]["share"] is None  # no price: no share


def test_clients_per_day_or_over_the_whole_period(catalog):
    rows = [
        usage_row("glm-4.7", 1_000_000, 0, 0, day="2026-10-30"),
        usage_row("glm-4.7", 2_000_000, 0, 0, day="2026-10-31"),
        usage_row("unknown", 10, 0, 0, day="2026-10-31"),
        usage_row("glm-4.7", 5_000_000, 0, 0, day="2026-10-30", client="bilal"),
    ]
    per_day, totals, _ = summarize_usage(rows, {}, catalog)
    assert [(r["day"], r["client"]) for r in per_day] == [
        ("2026-10-31", "ali"),
        ("2026-10-30", "bilal"),  # the dearest first within a day
        ("2026-10-30", "ali"),
    ]
    models = per_day[0]["models"]
    assert [(m["model"], m["share"]) for m in models] == [("glm-4.7", 1.0), ("unknown", 0.0)]
    whole, merged_totals, _ = summarize_usage(rows, {}, catalog, merged=True)
    assert [(r["day"], r["client"], r["requests"]) for r in whole] == [(None, "bilal", 1), (None, "ali", 3)]
    assert whole[1]["models"][0]["requests"] == 2 and merged_totals["cost"] == totals["cost"] == pytest.approx(8)


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
        assert details.json()["ip"] == "127.0.0.1" and "timeline" not in details.json()  # the address: admins
        assert (await c.get("/admin/api/requests/nope")).status_code == 404


async def test_filter_choices_are_read_once_then_kept_current(make_stack):
    """The table is scanned once (it takes seconds on a long log, and older rows waited behind it); what's written
    afterwards is added as it's written, and a purge has it read again, in the background."""
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    with store.tx() as db:  # there before the first look
        db.execute(
            "INSERT INTO requests (id, ts, client, model, tags) VALUES ('old', ?, 'before', 'm-old', 'x,y')",
            (time.time() - 3 * 86_400,),
        )
    scans = []
    scan = store._scan_choices
    store._scan_choices = lambda: (scans.append(1), scan())[1]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        choices = (await c.get("/admin/api/requests/filters")).json()
        assert choices["client"] == ["before"] and choices["tag"] == ["x", "y"] and choices["model"] == ["m-old"]
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))
        await flushed(stack, 2)
        await asyncio.sleep(0.2)  # written, then noted
        choices = (await c.get("/admin/api/requests/filters")).json()
        assert len(choices["client"]) == 2 and "done" in choices["outcome"] and len(scans) == 1  # at once, no scan

        assert store.prune_requests(1) == 1  # the old row goes
        assert "before" in (await c.get("/admin/api/requests/filters")).json()["client"]  # read again daily at most
        assert len(scans) == 1
        store._choices_at -= 2 * 86_400
        for _ in range(100):
            choices = (await c.get("/admin/api/requests/filters")).json()
            if "before" not in choices["client"]:
                break
            await asyncio.sleep(0.05)
        assert len(choices["client"]) == 1 and choices["tag"] == [] and len(scans) == 2


async def test_usage_body_and_its_period(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    day = 86_400
    now = time.time()
    store.record_requests(
        [
            stored_row("now", now, model="model-now", prompt_tokens=10, completion_tokens=1),
            stored_row("week", now - 3 * day, model="model-week", prompt_tokens=10, completion_tokens=1),
            stored_row("month", now - 20 * day, model="model-month", prompt_tokens=10, completion_tokens=1),
        ]
    )
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/usage")).text  # the last 24 hours
        assert "<html" in page and '<option value="24h" selected>' in page and "Tokens per hour" in page
        assert "model-now" in page and "model-week" not in page

        body = (await c.get("/admin/usage/body?range=7d")).text
        assert "<html" not in body and "model-week" in body and "model-month" not in body
        assert "Tokens per day" in body
        body = (await c.get("/admin/usage/body?range=30d")).text
        assert all(f"model-{name}" in body for name in ("now", "week", "month"))
        first = time.strftime("%Y-%m-%d", time.gmtime(now - 21 * day))
        custom = f"range=custom&from={first}&to={time.strftime('%Y-%m-%d', time.gmtime(now - 2 * day))}"
        body = (await c.get(f"/admin/usage/body?{custom}")).text
        assert "model-week" in body and "model-month" in body and "model-now" not in body
        assert f"From {first} 00:00 to " in body

        # a model opens to its clients, a client to its models; over the whole period, a row per client
        assert 'data-key="model:-:model-week"' in body
        assert '<tr class="part" data-of="model:-:model-week" hidden>' in body
        page = (await c.get("/admin/usage?range=30d&group=range")).text
        assert "Per client, whole period" in page and "<th>Day</th>" not in page
        assert page.count('data-of="client::ali"') == 3  # its three models
        assert 'data-group="range" aria-current="true"' in page
        assert 'href="/admin/usage.csv?range=30d&amp;group=range&amp;by=model"' in page
        csv_text = (await c.get("/admin/usage.csv?range=30d&group=range&by=model")).text
        rows = csv_text.strip().splitlines()[1:]
        assert len(rows) == 3 and all(r.split(",")[0].endswith("Z") and "/" in r.split(",")[0] for r in rows)


async def test_a_period_takes_its_hours(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    hour = int(time.time() // 3600) * 3600
    store.record_requests(
        [
            stored_row("a", hour + 1, prompt_tokens=1),
            stored_row("b", hour - 3600 + 5, prompt_tokens=2),
            stored_row("c", hour - 3600 + 9, prompt_tokens=4, client="bilal"),
        ],
    )
    rows = store.usage_between(hour, hour + 3600)
    assert [(r["client"], r["prompt_tokens"]) for r in rows] == [("ali", 1)]
    rows = store.usage_between(hour - 3600, hour + 3600)
    assert sorted((r["hour"][-2:], r["client"], r["prompt_tokens"]) for r in rows) == sorted(
        [
            (time.strftime("%H", time.gmtime(hour)), "ali", 1),
            (time.strftime("%H", time.gmtime(hour - 3600)), "ali", 2),
            (time.strftime("%H", time.gmtime(hour - 3600)), "bilal", 4),
        ]
    )
    assert all(r["day"] == r["hour"][:10] for r in rows)


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
        usage = await asyncio.wait_for(asyncio.to_thread(store.usage_between, 0, time.time() + 3600), 5)
    assert [r["id"] for r in rows] == ["r"] and len(usage) == 1


async def test_the_read_connection_cant_write(make_stack):
    stack = make_stack(bootstrap=OWNER)
    with pytest.raises(Exception, match="readonly"):
        stack.app.state.store.read("DELETE FROM requests")
