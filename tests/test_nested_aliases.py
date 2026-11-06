"""Aliases inside aliases: an alias's model or fallbacks can be other aliases, and the request is forwarded to them,
handled the way that alias handles it (council, orchestrator, answer check, its fallbacks)."""

import asyncio
import json
from types import SimpleNamespace

import httpx

from app.fallback import loop_of
from tests.test_admin import OWNER, csrf_of, login
from tests.test_answer_check import INSTRUCTIONS, words
from tests.test_council import ASK
from tests.test_council import setup as council_alias
from tests.test_fallback import chat
from tests.test_policies import set_group


def alias(name, target, fallbacks=()):
    return SimpleNamespace(name=name, target_model=target, fallbacks=tuple(fallbacks))


def test_loops_are_found_and_named():
    a, b = alias("a", "b", ["m1"]), alias("b", "a", ["m2"])
    aliases = {"a": a, "b": b}
    assert loop_of(a, aliases) == ["a", "b", "a"]
    c = alias("c", "m", ["a"])
    assert loop_of(c, {**aliases, "c": c}) == ["a", "b", "a"]  # a loop it uses, even one it isn't in
    assert loop_of(alias("d", "m", ["m2"]), aliases) is None


async def save_alias(c, name, target, fallbacks=(), triggers=("errors",), alias_id=None, **extra):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "target_model": target, "fallbacks": ", ".join(fallbacks), **extra}
    form.update({f"on_{t}": "on" for t in triggers})
    if alias_id is None:
        r = await c.post("/admin/aliases", data={**form, "name": name})
        assert "m=created" in r.headers.get("location", ""), r.text[:300]
        return r
    return await c.post(f"/admin/aliases/{alias_id}", data={**form, "enabled": "on"})


async def finished(stack, count):
    """The newest ``count`` requests once written (a stream's row ends just after the app got all of it)."""
    for _ in range(100):
        rows = await asyncio.to_thread(stack.app.state.store.list_requests, 50)
        if len(rows) >= count:
            return rows
        await asyncio.sleep(0.05)
    raise AssertionError(f"only {len(rows)} requests written")


async def test_a_forwarded_alias_checks_its_own_answers(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["bad ", "answer"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        checked = {"check_answers": "on", "check_model": "mock-model", "check_instructions": INSTRUCTIONS}
        await save_alias(c, "checked", "strict-model", ["mock-model"], **checked)
        await save_alias(c, "outer", "checked")
        await set_group(c, stack, "All models", models="outer")  # the key may use outer: checked comes with it
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("outer"))
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "good answer"
        # checked's model, its checker (failed it), then its fallback
        assert stack.mock["models"] == ["strict-model", "mock-model", "mock-model"]
        refused = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("checked"))
        assert refused.status_code == 403  # asked for itself, it still isn't the key's
    rows = await finished(stack, 4)
    main = next(r for r in rows if r["model"] == "outer" and not r["role"])
    assert main["via"] == "outer → checked" and main["target_model"] == "mock-model"
    assert main["answer_check"].startswith("strict-model: failed")


async def test_a_fallback_alias_takes_over_a_stream(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "plain", "mock-model")
        await save_alias(c, "outer", "big-model", ["plain"])
        body = chat("outer", stream=True, mock_fail_models=["big-model"])
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            assert r.status_code == 200
            text = "".join([chunk async for chunk in r.aiter_text()])
        assert ": big-model " in text and "; trying plain" in text and '"content"' in text
        assert text.rstrip().endswith("data: [DONE]") and text.count("data: [DONE]") == 1
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("outer", mock_fail_models=["big-model"])
        )
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
    rows = await finished(stack, 4)
    forwarded = [r for r in rows if r["outcome"] == "forwarded"]
    assert len(forwarded) == 2 and all(r["model"] == "outer" for r in forwarded)
    assert forwarded[0]["fallback"].startswith("big-model -> plain")
    answered = [r for r in rows if r["via"] == "outer → plain"]
    assert {r["parent"] for r in answered} == {r["id"] for r in forwarded}
    assert all(r["model"] == "outer" and r["outcome"] == "done" for r in answered)
    usage = await asyncio.to_thread(stack.app.state.store.usage_between, 0, 4e9)
    assert sum(u["requests"] for u in usage) == 2  # the forwarded rows aren't usage: what they forwarded is


async def test_back_to_the_outer_alias_when_the_forwarded_one_runs_out(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "solo", "big-model", triggers=())  # no fallbacks of its own: outer decides
        await save_alias(c, "outer", "solo", ["mock-model"])
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("outer", mock_fail_models=["big-model"])
        )
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
        assert stack.mock["models"] == ["big-model", "big-model", "mock-model"]
    rows = await finished(stack, 2)
    solo = next(r for r in rows if r["via"] == "outer → solo")
    back = next(r for r in rows if r["via"] == "outer → solo → outer")
    assert solo["outcome"] == "forwarded" and back["parent"] == solo["id"] and back["target_model"] == "mock-model"


async def test_a_forwarded_council_alias_runs_its_council(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await council_alias(c, stack)  # "team": members mock-model and small-model
        await save_alias(c, "boss", "team")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "boss", "messages": ASK})
        assert r.status_code == 200, r.text
        assert r.headers["x-queuerouter-council"] == "pick 2/2: small-model"
        assert r.json()["choices"][0]["message"]["content"] == "answer from small-model"
    rows = await finished(stack, 4)
    council = next(r for r in rows if r["council"])
    assert council["model"] == "boss" and council["via"] == "boss → team"


async def test_the_editor_refuses_loops(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "a", "mock-model")
        await save_alias(c, "b", "a")
        a_id = next(a.id for a in store.list_aliases() if a.name == "a")
        r = await save_alias(c, "a", "mock-model", ["b"], alias_id=a_id)
        assert r.status_code == 200 and "These aliases use each other in a loop: a → b → a." in r.text
        assert next(a for a in store.list_aliases() if a.name == "a").fallbacks == ()  # not saved

        # a loop made around the editor (as by an older version) ends
        store.update_alias(a_id, "t", target_model="b")
        stack.app.state.reload()
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))
        assert r.status_code == 508 and "do they loop?" in r.json()["error"]["message"]


async def test_a_name_both_an_alias_and_a_model_means_the_alias(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "mock-model", "mock-model@upstream")  # hides the provider's own mock-model
        await save_alias(c, "uses", "mock-model")
        await save_alias(c, "named", "plain")  # an alias that comes later: a name until then
        await save_alias(c, "plain", "mock-model@upstream")
        page = (await c.get("/admin/models")).text
        assert "is an alias and also a model a provider lists: here it means the alias" in page
        assert "its providers: those of plain" in page and 'title="An alias: the request is forwarded' in page
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("uses"))
        assert r.status_code == 200 and json.loads(stack.mock["last_body"])["model"] == "mock-model"
        models = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert models["named"]["max_model_len"] == 262144  # the size of what it sends to first, through plain
