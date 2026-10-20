"""Model aliases: rewrite, pinning to upstreams, /v1/models entries, permissions."""

import json

import httpx

from tests.test_admin import OWNER, csrf_of, login
from tests.test_policies import set_policy
from tests.test_upstreams import add_upstream


def chat(model, tag="x", **extra):
    return {"model": model, "messages": [{"role": "user", "content": tag}], "mock_tag": tag, **extra}


async def add_alias(c, name, target, upstream_ids=()):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "name": name, "target_model": target, **{f"upstream_{u}": str(u) for u in upstream_ids}}
    r = await c.post("/admin/aliases", data=form)
    return r


async def test_alias_rewrites_model_and_appears_in_models(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        r = await add_alias(c, "smart", "mock-model")
        assert "m=created" in r.headers["location"]
        assert "smart" in (await c.get("/admin/models")).text

        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("smart"))
        assert r.status_code == 200
        assert json.loads(stack.mock["last_body"])["model"] == "mock-model"

        models = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert models["smart"]["queuerouter"] == {"type": "alias", "target": "mock-model"}
        assert models["smart"]["max_model_len"] == 262144  # copied from the target

        recent = list(stack.app.state.stats.recent)[-1]
        assert (recent["model"], recent["target_model"]) == ("smart", "mock-model")

        dup = await add_alias(c, "smart", "other")
        assert "e=exists" in dup.headers["location"]


async def test_alias_pinned_to_upstream(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER)
    second_url, second = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url)
        second_id = next(u.id for u in stack.app.state.store.list_upstreams() if u.name == "second")
        await add_alias(c, "pinned", "mock-model", upstream_ids=(second_id,))
        for i in range(3):
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("pinned", f"p{i}"))
            assert r.status_code == 200
    assert second["served"] == 3 and stack.mock["served"] == 0


async def test_alias_permissions_and_disable(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "fast", "mock-model")
        # allowing the alias name is enough
        await set_policy(c, stack, allowed_models="fast")
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("fast"))).status_code == 200
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("mock-model"))).status_code == 403
        ids = [m["id"] for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]]
        assert ids == ["fast"]

        alias = stack.app.state.store.list_aliases()[0]
        csrf = csrf_of((await c.get("/admin/models")).text)
        await c.post(f"/admin/aliases/{alias.id}", data={"csrf": csrf, "target_model": "mock-model"})  # disabled
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("fast"))
        # no longer rewritten, and no upstream offers "fast": refused instead of sent on
        assert r.status_code == 404 and r.json()["error"]["type"] == "model_not_found"

        bad = await add_alias(c, "bad name!", "x")
        assert "e=invalid" in bad.headers["location"]
