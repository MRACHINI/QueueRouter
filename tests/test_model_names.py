"""A model on a provider is one entry: Provider|name everywhere, one opencode entry each, and its own custom name,
price, score, context and thinking levels. Also how far cancelled requests got, and proxies' Via headers."""

import asyncio
import json
import time

import httpx
import pytest
from cryptography.fernet import Fernet

from app.main import stopped_at
from app.names import EVERY, ModelNames
from app.pricing import (
    DEFAULT_MIX,
    Price,
    PriceCatalog,
    model_costs,
    price_text,
    request_costs,
    resolve,
    summarize_usage,
    token_mix,
)
from app.store import MIGRATIONS, Store
from app.upstream import client_identity, upstream_headers
from tests.test_admin import OWNER, csrf_of, login
from tests.test_upstreams import add_upstream
from tests.test_usage import chat, flushed

NAMES = ModelNames({1: "Synthetic", 2: "ArliAI"}, {(1, "hf:zai/GLM-4.7"): "GLM-4.7"})
PRICES = {"zai": {"models": {"glm-4.7": {"cost": {"input": 1, "output": 2, "cache_read": 0.1}}}}}


@pytest.fixture
def catalog():
    prices = PriceCatalog()
    prices.use(PriceCatalog.slim(PRICES), time.time())
    return prices


def test_labels():
    assert NAMES.label("hf:zai/GLM-4.7", "Synthetic") == "Synthetic|GLM-4.7"
    assert NAMES.label("hf:zai/GLM-4.7", "ArliAI") == "ArliAI|hf:zai/GLM-4.7"  # a name belongs to one provider
    assert NAMES.label("hf:zai/GLM-4.7@Synthetic") == "Synthetic|GLM-4.7"
    assert NAMES.label("hf:zai/GLM-4.7") == "hf:zai/GLM-4.7"  # no provider known
    assert NAMES.label("m", "-") == "m"  # usage without one
    assert NAMES.label("m", "Gone") == "Gone|m"  # old usage of a deleted upstream
    assert NAMES.label("m@nowhere") == "m@nowhere"  # not an upstream's name: part of the ID
    assert NAMES.name("hf:zai/GLM-4.7", "Synthetic") == "GLM-4.7" and NAMES.name("x") == "x"
    assert NAMES.id_of("ArliAI") == 2 and NAMES.id_of("Gone") == EVERY and NAMES.id_of(None) == EVERY


def test_a_choice_several_providers_may_serve():
    assert NAMES.label_on("m", ["Synthetic", "ArliAI"]) == "Synthetic, ArliAI|m"
    assert NAMES.label_on("hf:zai/GLM-4.7", ["ArliAI", "Synthetic"]) == "ArliAI, Synthetic|GLM-4.7"
    assert NAMES.label_on("m", ["a", "b", "c", "d", "e"]) == "a, b, c +2|m"
    assert NAMES.label_on("m", ["ArliAI"]) == "ArliAI|m" and NAMES.label_on("m", []) == "m"


def test_what_a_request_asked_for_and_got():
    assert NAMES.of_request("hf:zai/GLM-4.7", None, "Synthetic") == "Synthetic|GLM-4.7"
    assert NAMES.of_request("hf:zai/GLM-4.7@Synthetic", "hf:zai/GLM-4.7", "Synthetic") == "Synthetic|GLM-4.7"
    assert NAMES.of_request("hf:zai/GLM-4.7@Synthetic", None, None) == "Synthetic|GLM-4.7"  # never got a slot
    assert NAMES.of_request("fast", "hf:zai/GLM-4.7", "Synthetic") == "fast → Synthetic|GLM-4.7"
    assert NAMES.of_request("fast", "m", None) == "fast → m"
    assert NAMES.of_request("m", "m", None) == "m" and NAMES.of_request(None, None, None) == ""


def test_prices_per_provider(catalog):
    every = {"reference": None, "input": 5.0, "output": 6.0, "cached_input": None}
    own = {"reference": None, "input": 3.0, "output": 4.0, "cached_input": 0.5}
    overrides = {(EVERY, "m"): every, (1, "m"): own}
    assert resolve("m", overrides, catalog, 1).input == 3.0  # this provider's price wins
    assert resolve("m", overrides, catalog, 2).input == 5.0  # the rest: the one for every provider
    assert resolve("hf:zai/whatever", {}, catalog) is None
    assert resolve("hf:zai/whatever", {}, catalog, 1, "GLM-4.7").input == 1  # found by its custom name


def usage_row(model, upstream, prompt=1_000_000, day="2026-10-31", client="ali"):
    return {
        "day": day,
        "client": client,
        "model": model,
        "upstream": upstream,
        "requests": 1,
        "errors": 0,
        "prompt_tokens": prompt,
        "completion_tokens": 0,
        "cached_tokens": 0,
        "cache_write_tokens": 0,
    }


def test_usage_is_per_provider_at_its_price(catalog):
    own = {"reference": None, "input": 3.0, "output": 4.0, "cached_input": None}
    overrides = {(2, "hf:zai/GLM-4.7"): own}
    rows = [usage_row("hf:zai/GLM-4.7", "Synthetic"), usage_row("hf:zai/GLM-4.7", "ArliAI")]
    models, total = model_costs(rows, overrides, catalog, NAMES)
    assert [(m["label"], m["cost"]) for m in models] == [("ArliAI|hf:zai/GLM-4.7", 3.0), ("Synthetic|GLM-4.7", 1.0)]
    assert total["cost"] == 4.0
    out, totals, _ = summarize_usage(rows, overrides, catalog, True, 0, 1, NAMES)
    assert sorted(r["label"] for r in out) == ["ArliAI|hf:zai/GLM-4.7", "Synthetic|GLM-4.7"]
    assert sorted(r["cost"] for r in out) == [1.0, 3.0] and totals["cost"] == 4.0
    per_client, _, _ = summarize_usage(rows, overrides, catalog, False, 0, 1, NAMES)
    assert len(per_client) == 1 and per_client[0]["cost"] == 4.0


def test_request_rows_have_their_label_and_price(catalog):
    row = {
        "id": "r",
        "council": None,
        "model": "fast",
        "target_model": "hf:zai/GLM-4.7",
        "upstream": "Synthetic",
        "prompt_tokens": 1_000_000,
        "cached_tokens": 0,
        "cache_write_tokens": 0,
        "completion_tokens": 0,
    }
    costed = request_costs([row], [], {}, catalog, NAMES)[0]
    assert costed["label"] == "fast → Synthetic|GLM-4.7" and costed["cost"] == 1.0


def test_the_price_in_opencode_names():
    mix = token_mix(2_535_277_961, 2_102_288_531, 11_312_905)  # what a real month looked like
    assert [round(share, 3) for share in mix] == [0.17, 0.826, 0.004]
    assert price_text(Price(0.60, 2.20, 0.11, source="x"), mix) == "$0.203"
    assert price_text(Price(0.30, 1.20, 0.30, source="x"), mix) == "$0.304"  # no cache discount: dearer than it looks
    assert price_text(Price(1.0, 1.0, 1.0, source="x"), mix) == "$1.000"  # always three decimals
    assert price_text(Price(0.15, 0.60, 0.02, source="x"), mix) == "$0.045"  # cheap models stay readable
    assert price_text(Price(0.001, 0.002, 0.0001, source="x"), mix) == "$0.000"
    assert price_text(None, mix) == "$?"
    assert price_text(Price(0.0, 0.0, 0.0, source="plan"), mix) == "plan"
    assert token_mix(10, 5, 5) == DEFAULT_MIX  # too little usage to say


def test_how_far_a_cancelled_request_got():
    assert stopped_at({"queue_ms": None, "ttft_ms": None}) == "in queue"
    assert stopped_at({"queue_ms": 5, "ttft_ms": None}) == "before first byte"
    assert stopped_at({"queue_ms": 5, "ttft_ms": 9}) == "mid-answer"
    assert stopped_at({"queue_ms": 5, "ttft_ms": 9}, answered=True) == "after answer"


def test_a_proxys_via_header_stays_here():
    headers = {"Via": "1.1 Caddy", "X-Title": "app"}
    assert "via" not in {k.lower() for k in upstream_headers(headers, "k")}
    assert "via" not in json.loads(client_identity(headers, None))


def test_settings_made_before_become_ones_for_every_provider(tmp_path):
    store = Store(str(tmp_path / "old.db"), Fernet(Fernet.generate_key()))
    for version, script in enumerate(MIGRATIONS[:-1], 1):
        store._db.executescript(f"BEGIN;\n{script}\nPRAGMA user_version={version};\nCOMMIT;")
    store._db.execute("INSERT INTO model_prices (model, input, output, updated_at) VALUES ('m', 1, 2, 0)")
    store._db.execute("INSERT INTO model_scores (model, score, updated_at) VALUES ('m', 50, 0)")
    store._db.execute("INSERT INTO model_thinking (model, levels, updated_at) VALUES ('m', 'low', 0)")
    store.migrate()
    assert store.schema_version() == len(MIGRATIONS)
    assert store.list_model_prices()[(EVERY, "m")]["input"] == 1
    assert store.list_model_scores() == {(EVERY, "m"): 50}
    assert store.list_model_thinking() == {(EVERY, "m"): "low"}
    store.set_model_price("m", "t", None, 3.0, 4.0, None, upstream_id=7)
    assert set(store.list_model_prices()) == {(EVERY, "m"), (7, "m")}


async def two_providers(c, stack, make_mock):
    await login(c, stack.url)
    second_url, second = make_mock(3)
    await add_upstream(c, "second", second_url, max_concurrency=3)
    store = stack.app.state.store
    return second, {u.name: u.id for u in store.list_upstreams()}


async def opencode(c, stack, query=""):
    config = await c.get(f"/v1/opencode-config{query}", headers=stack.headers)
    return config.json()["provider"]["queuerouter"]["models"]


async def test_opencode_lists_each_provider_and_requests_go_there(make_stack, make_mock):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        second, ids = await two_providers(c, stack, make_mock)
        models = await opencode(c, stack)
        assert {k: v["name"] for k, v in models.items()} == {
            "mock-model@upstream": "$?|mock-model|upstream",
            "mock-model@second": "$?|mock-model|second",
        }
        before = stack.mock["served"]
        body = chat("hi", model="mock-model@second")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
        assert second["served"] == 1 and stack.mock["served"] == before
        assert json.loads(second["last_body"])["model"] == "mock-model"  # the provider gets the real ID
        plain = chat("hi")  # what an app with the plain ID sends still works, on either provider
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=plain)).status_code == 200

        await flushed(stack, 2)
        store = stack.app.state.store
        rows = await asyncio.to_thread(store.list_requests, 10)
        pinned = next(r for r in rows if r["model"] == "mock-model@second")
        assert pinned["target_model"] == "mock-model" and pinned["upstream"] == "second"
        usage = await asyncio.to_thread(store.usage, 1)
        assert {r["model"] for r in usage} == {"mock-model"} and "second" in {r["upstream"] for r in usage}
        listed = (await c.get("/admin/api/requests")).json()["rows"]
        assert any(r["label"] == "second|mock-model" for r in listed)


async def test_a_client_limited_to_a_model_sees_it_on_each_provider(make_stack, make_mock):
    import dataclasses

    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["other-model"]
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await two_providers(c, stack, make_mock)
        client = store.list_clients()[0]
        limited = dataclasses.replace(client.policy, allowed_models=("mock-model",))
        store.update_client(client.id, "t", policy=limited)
        stack.app.state.reload()
        assert sorted(await opencode(c, stack)) == ["mock-model@second", "mock-model@upstream"]
        body = chat("hi", model="mock-model@second")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
        refused = chat("hi", model="other-model@upstream")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=refused)).status_code == 403


async def test_a_custom_name_shows_everywhere_and_finds_score_and_price(make_stack, make_mock):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, opencode_rank=True)
    stack.app.state.prices.use(PriceCatalog.slim(PRICES), time.time())
    stack.app.state.scores.use(
        [{"name": "GLM 4.7", "slug": "glm-4-7", "creator": "", "scores": {"intelligence": 60.0}}], time.time()
    )
    stack.mock["extra_models"] = ["hf:zai/secret-tune"]
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await c.get("/v1/models", headers=stack.headers)
        upstream = store.list_upstreams()[0]
        page = (await c.get("/admin/models")).text
        assert 'id="names"' in page and "upstream|hf:zai/secret-tune" in page
        assert "no match" in page  # its real ID finds no price

        row = {"upstream_id": str(upstream.id), "model": "hf:zai/secret-tune"}
        form = {"csrf": csrf_of(page), **row, "name": "GLM-4.7"}
        assert "m=saved" in (await c.post("/admin/model-names", data=form)).headers["location"]

        models = await opencode(c, stack, "?plugin=2")
        key = "hf:zai/secret-tune@upstream"
        assert models[key]["name"] == "01|60.0|$0.299|GLM-4.7|upstream"  # the score and price, by the custom name
        assert models[key]["cost"]["input"] == 1  # the key is still the real ID

        body = chat("hi", model=key)
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
        assert json.loads(stack.mock["last_body"])["model"] == "hf:zai/secret-tune"
        await flushed(stack, 1)
        row = (await c.get("/admin/api/requests")).json()["rows"][0]
        assert row["label"] == "upstream|GLM-4.7" and row["cost"] and row["cost"] > 0
        details = (await c.get(f"/admin/api/requests/{row['id']}")).json()
        assert details["target_model"] == "hf:zai/secret-tune"  # the real ID stays one click away

        usage = (await c.get("/admin/usage?by=model")).text
        assert "upstream|GLM-4.7" in usage
        page = (await c.get("/admin/models")).text
        assert "upstream|GLM-4.7" in page

        form["name"] = ""  # empty: back to the ID
        assert "m=saved" in (await c.post("/admin/model-names", data=form)).headers["location"]
        models = await opencode(c, stack)
        assert models[key]["name"] == "01|$?|hf:zai/secret-tune|upstream"

        form.update(name="x", upstream_id="9999")
        assert "m=saved" not in (await c.post("/admin/model-names", data=form)).headers["location"]
        form.update(upstream_id="", name="y")  # a name always belongs to a provider
        assert "m=saved" not in (await c.post("/admin/model-names", data=form)).headers["location"]


async def test_each_provider_has_its_own_settings(make_stack, make_mock):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, opencode_rank=True)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        second, ids = await two_providers(c, stack, make_mock)
        await c.get("/v1/models", headers=stack.headers)
        page = (await c.get("/admin/models")).text
        csrf = csrf_of(page)
        assert "upstream|mock-model" in page and "second|mock-model" in page
        row = {"csrf": csrf, "model": "mock-model", "upstream_id": str(ids["second"])}

        prices = {**row, "reference": "", "input": "2", "cached_input": "0.5", "output": "3"}
        assert "m=saved" in (await c.post("/admin/prices", data=prices)).headers["location"]
        assert "m=saved" in (await c.post("/admin/model-scores", data={**row, "score": "77"})).headers["location"]
        levels = {**row, "levels": "low, high"}
        assert "m=saved" in (await c.post("/admin/model-thinking", data=levels)).headers["location"]
        limits = {**row, "context": "4096", "output": "1024"}
        assert "m=saved" in (await c.post("/admin/model-limits", data=limits)).headers["location"]

        models = await opencode(c, stack, "?plugin=2")
        mine, other = models["mock-model@second"], models["mock-model@upstream"]
        assert mine["name"].startswith("01|77.0|$0.825|") and mine["name"].endswith("|mock-model|second")
        assert other["name"] == "02|$?|mock-model|upstream"  # the other provider's row keeps what it had
        assert mine["cost"] == {"input": 2.0, "output": 3.0, "cache_read": 0.5} and "cost" not in other
        assert mine["limit"] == {"context": 4096, "output": 1024} and other["limit"]["context"] == 262_144
        assert "thinking" in mine and "thinking" not in other

        too_long = chat("x" * 20_000, model="mock-model@second")
        refused = await c.post("/v1/chat/completions", headers=stack.headers, json=too_long)
        assert refused.status_code == 400 and "4096 tokens" in refused.json()["error"]["message"]
        fits = chat("x" * 20_000, model="mock-model@upstream")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=fits)).status_code == 200

        store.set_model_name(ids["second"], "mock-model", "t", "Named")
        await c.post(f"/admin/upstreams/{ids['second']}/delete", data={"csrf": csrf})
        assert "second" not in {u.name for u in store.list_upstreams()}
        assert store.list_model_names() == {} and not store.list_model_scores()
        assert not store.list_model_prices() and not store.list_model_thinking()
        assert not [r for r in store.list_model_limits() if r["source"] == "manual"]


async def test_blank_also_clears_what_was_set_for_every_provider(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    store.set_model_score("mock-model", "t", 50)
    store.set_model_thinking("mock-model", "t", "low")
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        upstream = str(store.list_upstreams()[0].id)
        row = {"csrf": csrf, "model": "mock-model", "upstream_id": upstream}
        await c.post("/admin/model-scores", data={**row, "score": ""})
        await c.post("/admin/model-thinking", data={**row, "levels": ""})
    assert store.list_model_scores() == {} and store.list_model_thinking() == {}


async def test_every_page_with_a_model_opens_and_shows_labels(make_stack, make_mock):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        second, ids = await two_providers(c, stack, make_mock)
        await c.get("/v1/models", headers=stack.headers)
        store.set_model_name(ids["second"], "mock-model", "t", "Named")
        stack.app.state.reload()
        body = chat("t", model="mock-model@second", stream=True, mock_chunks=3)
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            _ = [x async for x in r.aiter_bytes()]
        await flushed(stack, 1)
        row = (await asyncio.to_thread(store.list_requests, 1))[0]

        assert "second|Named" in (await c.get("/admin/response-times")).text
        assert "second|Named" in (await c.get("/admin/usage?by=model")).text
        assert "second|Named" in (await c.get("/admin/models")).text
        assert "second|Named" in (await c.get(f"/admin/requests/{row['id']}/trace")).text
        assert "second|Named" in (await c.get("/admin/playground")).text
        assert "(Named)" in (await c.get(f"/admin/upstreams/{ids['second']}/models")).text
        new = (await c.get("/admin/aliases/new")).text
        assert '"label": "second|Named"' in new
        live = (await c.get("/admin/api/stats")).json()
        assert live["recent"][-1]["label"] == "second|Named"
        choices = (await c.get("/admin/api/requests/filters")).json()
        assert choices["labels"]["mock-model@second"] == "second|Named"
        assert "second|Named" in (await c.get("/admin/requests?model=mock-model@second")).text
        assert (await c.get("/admin/jobs")).status_code == 200
        csv_text = (await c.get("/admin/usage.csv?by=model")).text
        assert ",second,second|Named" in csv_text


async def test_opencode_names_carry_the_price(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, opencode_rank=True)
    stack.mock["extra_models"] = ["small-model"]
    store = stack.app.state.store
    store.set_model_score("mock-model", "t", 32.45)
    store.set_model_price("mock-model", "t", None, 1.0, 2.0, 0.1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        models = await opencode(c, stack)
        assert models["mock-model@upstream"]["name"] == "01|32.5|$0.299|mock-model|upstream"
        assert "~" not in models["mock-model@upstream"]["name"]
        assert models["small-model@upstream"]["name"] == "02|$?|small-model|upstream"

        pool = store.list_pools()[0]
        store.update_pool(pool.id, "t", monthly_price=20.0)
        stack.app.state.reload()
        models = await opencode(c, stack)
        assert models["small-model@upstream"]["name"] == "02|plan|small-model|upstream"  # nothing extra per request
        assert models["mock-model@upstream"]["name"] == "01|32.5|$0.299|mock-model|upstream"  # yours still shows

        await login(c, stack.url)
        page = (await c.get("/admin/models")).text
        assert "a typical mix until there" in page and "20.0% input" in page


async def test_prices_in_names_can_be_turned_off(make_stack):
    stack = make_stack(models_cache_s=0, opencode_rank=True, opencode_price=False)
    store = stack.app.state.store
    store.set_model_score("mock-model", "t", 40)
    store.set_model_price("mock-model", "t", None, 1.0, 2.0, 0.1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        models = await opencode(c, stack, "?plugin=2")
    entry = models["mock-model@upstream"]
    assert entry["name"] == "01|40.0|mock-model|upstream" and entry["cost"]["input"] == 1


async def test_token_totals(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    assert store.token_totals(30) == (0, 0, 0)
    now = time.time()
    tokens = {"prompt_tokens": 100, "cached_tokens": 40, "completion_tokens": 5}
    old = {"id": "old", "ts": now - 40 * 86_400, "stream": 0, "retries": 0, "outcome": "done", **tokens}
    store.record_requests([{**old, "id": "new", "ts": now}, old])
    assert store.token_totals(30) == (100, 40, 5)  # the last 30 days only


async def test_a_cancelled_stream_says_how_far_it_got(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        body = chat("t", stream=True, mock_chunks=200, mock_delay=0.05)
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            async for line in r.aiter_lines():
                if '"content"' in line:
                    break  # goes away in the middle of the answer
        await flushed(stack, 1)
        row = (await c.get("/admin/api/requests")).json()["rows"][0]
    assert row["outcome"] == "cancelled" and row["stopped"] == "mid-answer"


def test_resolve_keeps_a_reference_per_provider(catalog):
    overrides = {(2, "m"): {"reference": "zai/glm-4.7", "input": None, "output": None, "cached_input": None}}
    price = resolve("m", overrides, catalog, 2)
    assert isinstance(price, Price) and price.input == 1
    assert resolve("m", overrides, catalog, 1) is None
