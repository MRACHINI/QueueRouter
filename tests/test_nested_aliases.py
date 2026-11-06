"""Aliases inside aliases: an alias's model or fallbacks can be other aliases, each standing for its own list."""

import json
from types import SimpleNamespace

import httpx

from app.fallback import Link, links_of, loop_of, triggers_of
from tests.test_admin import OWNER, csrf_of, login
from tests.test_fallback import chat

RULES = '{"overrides": {"temperature": 0.2}}'


def alias(name, target, fallbacks=(), upstream_ids=(), params="", fallback_on=("errors",)):
    return SimpleNamespace(
        name=name,
        target_model=target,
        fallbacks=tuple(fallbacks),
        upstream_ids=tuple(upstream_ids),
        params=params,
        fallback_on=tuple(fallback_on),
    )


def named(*aliases):
    return {a.name: a for a in aliases}


def test_an_alias_in_an_alias_stands_for_its_own_list():
    inner = alias("inner", "m1", ["m2"], upstream_ids=[2], params=RULES)
    outer = alias("outer", "inner", ["m3", "m2"], upstream_ids=[1])  # its ticks were for its model: inner has its own
    aliases = named(inner, outer)
    assert links_of(outer, aliases) == [Link("m1", (2,), (RULES,)), Link("m2", (), (RULES,)), Link("m3")]
    deeper = alias("deeper", "x@up", ["outer"])
    aliases["deeper"] = deeper
    assert [link.named for link in links_of(deeper, aliases)] == ["x@up", "m1", "m2", "m3"]
    assert links_of(alias("plain", "m1", ["m2"]), aliases) == [Link("m1"), Link("m2")]


def test_loops_are_cut_and_named():
    a, b = alias("a", "b", ["m1"]), alias("b", "a", ["m2"])
    aliases = named(a, b)
    assert [link.named for link in links_of(a, aliases)] == ["m2", "m1"]  # b's way back to a is skipped
    assert loop_of(a, aliases) == ["a", "b", "a"]
    c = alias("c", "m", ["a"])
    assert loop_of(c, {**aliases, "c": c}) == ["a", "b", "a"]  # a loop it uses, even one it isn't in
    assert loop_of(alias("d", "m", ["m2"]), aliases) is None
    only_loop = named(alias("x", "y"), alias("y", "x"))
    assert links_of(only_loop["x"], only_loop) == []


def test_when_it_moves_on():
    inner = alias("inner", "m1", ["m2"], fallback_on=("refused",))
    aliases = named(inner)
    assert triggers_of(alias("outer", "inner", fallback_on=()), aliases) == {"refused"}  # no fallbacks of its own
    assert triggers_of(alias("outer", "inner", ["m3"], fallback_on=("errors",)), aliases) == {"errors"}
    assert triggers_of(alias("lone", "m1"), aliases) == frozenset()


async def save_alias(c, name, target, fallbacks=(), triggers=("errors",), alias_id=None, **extra):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "target_model": target, "fallbacks": ", ".join(fallbacks), **extra}
    form.update({f"on_{t}": "on" for t in triggers})
    if alias_id is None:
        return await c.post("/admin/aliases", data={**form, "name": name})
    return await c.post(f"/admin/aliases/{alias_id}", data={**form, "enabled": "on"})


async def test_an_alias_as_another_alias_model(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=1)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        r = await save_alias(c, "inner", "big-model", ["mock-model"], params=RULES)
        assert "m=created" in r.headers["location"]
        r = await save_alias(c, "outer", "inner", triggers=())  # no fallbacks of its own: inner's ticks apply
        assert "m=created" in r.headers["location"]

        failing = {"mock_fail_models": ["big-model"]}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("outer", **failing))
        assert r.status_code == 200 and r.headers["x-queuerouter-model"] == "mock-model"
        assert stack.mock["models"] == ["big-model", "big-model", "mock-model"]  # inner's model, a retry, its fallback
        sent = json.loads(stack.mock["last_body"])
        assert sent["model"] == "mock-model" and sent["temperature"] == 0.2  # inner's rules for inner's models
        entry = list(stack.app.state.stats.recent)[-1]
        assert entry["model"] == "outer" and entry["target_model"] == "mock-model"
        assert entry["fallback"].startswith("big-model -> mock-model")

        await save_alias(c, "short", "smallish")
        await save_alias(c, "named", "inner2")  # an alias added later: a name until then
        await save_alias(c, "inner2", "mock-model")
        alias_id = next(a.id for a in store.list_aliases() if a.name == "short")
        r = await save_alias(c, "short", "big-model", ["inner2"], alias_id=alias_id)  # an alias as a fallback
        assert "m=saved" in r.headers["location"]
        before = len(stack.mock["models"])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("short", **failing))
        assert r.status_code == 200 and stack.mock["models"][before:] == ["big-model", "big-model", "mock-model"]

        models = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert models["named"]["max_model_len"] == 262144  # the size of what it sends to first, through inner2

        page = (await c.get("/admin/models")).text
        assert "its providers: those of inner" in page and 'title="An alias: its model' in page


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

        # an answer check counts the fallbacks of an alias it uses
        await save_alias(c, "fb", "big-model", ["mock-model"])
        r = await save_alias(c, "checked", "fb", check_answers="on", check_instructions="Fail it if it says bad.")
        assert "m=created" in r.headers["location"], r.text[:300]

        # a loop made around the editor (as by an older version): no model, said so
        store.update_alias(a_id, "t", target_model="b")
        stack.app.state.reload()
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a"))
        assert r.status_code == 404 and "lead back to it" in r.json()["error"]["message"]


async def test_a_name_both_an_alias_and_a_model_means_the_alias(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "mock-model", "mock-model@upstream")  # hides the provider's own mock-model
        await save_alias(c, "uses", "mock-model", params="")
        page = (await c.get("/admin/models")).text
        assert "is an alias and also a model a provider lists: here it means the alias" in page
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("uses"))
        assert r.status_code == 200 and json.loads(stack.mock["last_body"])["model"] == "mock-model"
