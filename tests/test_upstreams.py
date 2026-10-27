"""Several upstreams: pools, load balancing, failover and circuit breakers, driven via the admin UI."""

import asyncio

import httpx

from tests.test_admin import OWNER, csrf_of, login


def chat(tag, **extra):
    return {"model": "mock-model", "messages": [{"role": "user", "content": tag}], "mock_tag": tag, **extra}


async def add_upstream(c, name, base_url, max_concurrency=1, pool="new"):
    csrf = csrf_of((await c.get("/admin/upstreams")).text)
    form = {
        "csrf": csrf,
        "name": name,
        "preset": "custom",
        "base_url": base_url,
        "api_key": "k",
        "pool": pool,
        "max_concurrency": str(max_concurrency),
        "weight": "1",
    }
    r = await c.post("/admin/upstreams", data=form)
    assert "m=created" in r.headers["location"], r.headers["location"]


async def break_upstream(c, upstream_id):
    """Point an upstream at a closed port."""
    csrf = csrf_of((await c.get("/admin/upstreams")).text)
    form = {"csrf": csrf, "base_url": "http://127.0.0.1:9/v1", "enabled": "on", "weight": "1"}
    r = await c.post(f"/admin/upstreams/{upstream_id}", data=form)
    assert "m=saved" in r.headers["location"]


async def add_listless_upstream(c, make_mock, name="claude"):
    """An upstream whose model list can't be fetched (like a Claude account), once QueueRouter has tried."""
    url, state = make_mock(limit=4)
    state["models_status"] = 500
    await add_upstream(c, name, url, max_concurrency=4)
    for _ in range(100):  # its list is fetched in the background: wait until that has failed
        if "HTTP 500: no model list" in (await c.get("/admin/upstreams")).text:
            return state
        await asyncio.sleep(0.05)
    raise AssertionError("the model list didn't fail")


async def test_load_balancing_across_pools(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER)
    second_url, second = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url)
        page = await c.get("/admin/upstreams")
        assert "second" in page.text and "Limit pools" in page.text

        api = f"{stack.url}/v1/chat/completions"
        results = await asyncio.gather(
            *(c.post(api, headers=stack.headers, json=chat(f"r{i}", mock_delay=0.05)) for i in range(8))
        )
    assert all(r.status_code == 200 for r in results)
    # each account's limit (1 + 1) respected, and both were used
    assert stack.mock["peak"] == 1 and second["peak"] == 1
    assert stack.mock["rejected"] == 0 and second["rejected"] == 0
    assert stack.mock["served"] > 0 and second["served"] > 0
    assert stack.mock["served"] + second["served"] == 8


async def test_shared_pool_keys_share_the_limit(make_stack, make_mock):
    """Two upstream entries in one pool (same account) never exceed the pool's limit together."""
    stack = make_stack(limit=1, bootstrap=OWNER)
    second_url, second = make_mock(limit=5)
    in_flight, peak = [0], [0]

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        pool_id = stack.app.state.store.list_pools()[0].id
        await add_upstream(c, "same-account", second_url, pool=str(pool_id))
        api = f"{stack.url}/v1/chat/completions"

        async def watch():
            while True:
                in_flight[0] = stack.mock["active"] + second["active"]
                peak[0] = max(peak[0], in_flight[0])
                await asyncio.sleep(0.005)

        watcher = asyncio.create_task(watch())
        results = await asyncio.gather(
            *(c.post(api, headers=stack.headers, json=chat(f"r{i}", mock_delay=0.05)) for i in range(6))
        )
        watcher.cancel()
        stats = (await c.get("/admin/api/stats")).json()
    assert all(r.status_code == 200 for r in results)
    assert peak[0] == 1
    assert stats["max_concurrency"] == 1


async def test_failover_and_circuit_breaker(make_stack, make_mock):
    # "ordered" = always try the first upstream first, so the broken one really gets tried
    stack = make_stack(limit=1, bootstrap=OWNER, max_retries=3, circuit_threshold=2, lb_strategy="ordered")
    good_url, good = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "backup", good_url)
        original = stack.app.state.store.list_upstreams()[0]
        await break_upstream(c, original.id)

        api = f"{stack.url}/v1/chat/completions"
        for i in range(4):
            r = await c.post(api, headers=stack.headers, json=chat(f"f{i}"))
            assert r.status_code == 200, r.text
        stats = (await c.get("/admin/api/stats")).json()
        broken = next(u for u in stats["upstreams"] if u["id"] == original.id)
        assert broken["state"] == "open" and "ConnectError" in broken["last_error"]
        assert stats["outcomes"]["done"] == 4
        assert "open" in (await c.get("/admin/upstreams")).text
    assert good["served"] == 4


async def test_streaming_failover(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER, max_retries=3)
    good_url, good = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "backup", good_url)
        original = stack.app.state.store.list_upstreams()[0]
        await break_upstream(c, original.id)
        csrf = csrf_of((await c.get("/admin/settings")).text)
        await c.post("/admin/settings", data={"csrf": csrf, "lb_strategy": "ordered", "max_retries": "3"})
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=chat("s", stream=True)) as r:
            text = "".join([chunk async for chunk in r.aiter_text()])
    assert "failing over" in text and text.rstrip().endswith("data: [DONE]")
    assert good["served"] == 1


async def test_paused_upstream_gets_no_traffic_and_models_merge(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER)
    second_url, second = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url)
        models = (await c.get("/v1/models", headers=stack.headers)).json()
        assert [m["id"] for m in models["data"]] == ["mock-model"]  # merged, de-duplicated

        original = stack.app.state.store.list_upstreams()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        form = {"csrf": csrf, "base_url": "", "enabled": "on", "paused": "on", "weight": "1"}
        await c.post(f"/admin/upstreams/{original.id}", data=form)
        for i in range(3):
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat(f"p{i}"))
            assert r.status_code == 200
    assert stack.mock["served"] == 0 and second["served"] == 3


async def test_upstream_without_model_list_gets_no_listed_model(make_stack, make_mock):
    """An upstream whose model list can't be fetched (e.g. a Claude account) gets no model another upstream
    lists, even when the listing one is busy (it used to get council members' "hf:..." models and answer 404)."""
    from tests.test_aliases import add_alias  # it imports this module

    stack = make_stack(limit=1, bootstrap=OWNER, models_cache_s=0)  # lists fetched again when asked
    stack.mock["extra_models"] = ["hf:listed"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        claude = await add_listless_upstream(c, make_mock)
        claude["missing_models"] = ["hf:listed"]
        await add_alias(c, "member", "hf:listed")  # aliases and council members name models "raw"
        assert "hf:listed" in [m["id"] for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]]
        api = f"{stack.url}/v1/chat/completions"
        results = await asyncio.gather(
            *(
                c.post(api, headers=stack.headers, json=chat(f"l{i}", model=model, mock_delay=0.05))
                for i, model in enumerate(["hf:listed", "member"] * 3)
            )
        )
    assert all(r.status_code == 200 for r in results), [r.text for r in results]
    assert "hf:listed" not in claude.get("models", []) and stack.mock["served"] == 6


async def test_404_fails_over_to_another_upstream(make_stack, make_mock):
    """A model no list names goes to any upstream; one that answers 404 ("no such model") passes it on."""
    stack = make_stack(limit=1, bootstrap=OWNER, lb_strategy="ordered")
    stack.mock["missing_models"] = ["elsewhere"]
    other_url, other = make_mock(limit=1)
    other["models_status"] = 500
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "other", other_url)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("e", model="elsewhere"))
        assert r.status_code == 200, r.text
        stats = (await c.get("/admin/api/stats")).json()
    assert stack.mock["models"] == ["elsewhere"] and other["served"] == 1
    assert all(u["state"] == "closed" for u in stats["upstreams"])  # a 404 isn't a broken upstream


async def test_upstream_test_button_and_pool_rules(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        upstream = stack.app.state.store.list_upstreams()[0]
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        r = await c.post(f"/admin/upstreams/{upstream.id}/test", data={"csrf": csrf})
        assert "OK — 1 models" in r.text

        r = await c.post(f"/admin/pools/{pool.id}/delete", data={"csrf": csrf})
        assert "e=pool_in_use" in r.headers["location"]

        # an ArliAI plan sets the pool's limits; raising the limit applies live
        form = {"csrf": csrf, "plan": "Max", "max_concurrency": "", "context_cap": "", "min_spacing_ms": "0"}
        r = await c.post(f"/admin/pools/{pool.id}", data=form)
        assert "m=saved" in r.headers["location"]
        updated = stack.app.state.store.list_pools()[0]
        assert (updated.max_concurrency, updated.context_cap, updated.plan) == (6, 524_288, "ArliAI Max")
        assert (await c.get("/admin/api/stats")).json()["max_concurrency"] == 6

        bad_form = {"csrf": csrf, "name": "x", "preset": "custom", "base_url": "not a url", "api_key": "k"}
        bad = await c.post("/admin/upstreams", data={**bad_form, "pool": "new"})
        assert "e=invalid" in bad.headers["location"]


async def test_control_headers_not_forwarded(make_stack):
    stack = make_stack()
    headers = {**stack.headers, "X-QueueRouter-Priority": "low", "X-Session-Id": "abc", "X-Custom": "kept"}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=headers, json=chat("h"))
        assert r.status_code == 200
    seen = stack.mock["last_headers"]
    assert "x-queuerouter-priority" not in seen and "x-session-id" not in seen
    assert seen.get("x-custom") == "kept"


async def test_sticky_routing_returns_to_same_upstream(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER)
    second_url, second = make_mock(limit=1)
    api = "/v1/chat/completions"
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url)
        # occupy upstream 1 so the conversation starts on upstream 2
        blocker = asyncio.create_task(c.post(api, headers=stack.headers, json=chat("blocker", mock_delay=0.1)))
        await asyncio.sleep(0.2)
        convo = {"model": "mock-model", "messages": [{"role": "user", "content": "same conversation"}]}
        assert (await c.post(api, headers=stack.headers, json={**convo, "mock_tag": "turn1"})).status_code == 200
        await blocker
        # both free now: least-busy alone would pick upstream 1, sticky routing picks 2 again
        for turn in (2, 3):
            r = await c.post(api, headers=stack.headers, json={**convo, "mock_tag": f"turn{turn}"})
            assert r.status_code == 200
    second_tags = [tag for event, tag in second["log"] if event == "start"]
    assert second_tags == ["turn1", "turn2", "turn3"]


async def test_review_fixes_request_path(make_stack, make_mock):
    """Fable review: a half-open trial that hits a used-up quota and fails over is released (not reserved
    forever); dot-segment paths are refused; header text from clients can't break the response."""
    from app.main import header_value, odd_path

    stack = make_stack(limit=1, bootstrap=OWNER, circuit_threshold=1, circuit_cooldown_s=0.2, lb_strategy="ordered")
    good_url, good = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "backup", good_url)
        original = stack.app.state.store.list_upstreams()[0]
        circuits = stack.app.state.circuits
        circuits.failure(original.id, "test")  # open, then half-open after the cooldown
        await asyncio.sleep(0.3)
        assert circuits.state(original.id) == "half-open"
        stack.mock["daily_quota"] = True
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("trial"))
        assert r.status_code == 200 and good["served"] == 1
        assert circuits.allows(original.id)  # the trial isn't stuck

        r = await c.get("/v1/%2e%2e/admin", headers=stack.headers)
        assert r.status_code == 404
    assert odd_path("../admin") and odd_path("a/./b") and odd_path("a\\b") and not odd_path("models/x.y")
    assert header_value("m\r\nX-Evil: 1") == "mX-Evil: 1"


async def test_get_passthrough_needs_a_listed_path(make_stack):
    """Unknown /v1/ GETs are refused unless listed in Settings; "retrieve model" is answered by QueueRouter."""
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.get("/v1/credits", headers=stack.headers)
        assert r.status_code == 404 and "Settings" in r.json()["error"]["message"]
        stack.app.state.store.set_tunables({"passthrough_get_paths": " /v1/credits/ , other"}, "t")
        stack.app.state.reload()
        r = await c.get("/v1/credits", headers=stack.headers)
        assert r.status_code == 200 and r.json() == {"credits": 5, "key": "Bearer upstream-secret"}
        assert (await c.get("/v1/creditsx", headers=stack.headers)).status_code == 404

        model = (await c.get("/v1/models/mock-model", headers=stack.headers)).json()
        assert model["id"] == "mock-model" and model["max_model_len"] == 262144
        missing = await c.get("/v1/models/no/such-model", headers=stack.headers)
        assert missing.status_code == 404 and missing.json()["error"]["type"] == "model_not_found"
        upstream = stack.app.state.store.list_upstreams()[0].name
        pinned = (await c.get(f"/v1/models/mock-model@{upstream}", headers=stack.headers)).json()
        assert pinned["id"] == f"mock-model@{upstream}"
