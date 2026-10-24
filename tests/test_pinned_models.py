"""model@upstream: a model name can name the upstream that must serve it (alias target, fallbacks, council
members, judge, summary model, or a request's own model). The upstream still gets the plain model name."""

import json

import httpx

from tests.test_admin import OWNER, csrf_of, login
from tests.test_aliases import add_alias
from tests.test_upstreams import add_upstream

CHAT = {"messages": [{"role": "user", "content": "hi"}]}


async def test_pinned_names_go_to_that_upstream(make_stack, make_mock):
    stack = make_stack(limit=2, bootstrap=OWNER, models_cache_s=0)
    second_url, second = make_mock(limit=2)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url, max_concurrency=2)
        await c.get("/v1/models", headers=stack.headers)  # both upstreams offer mock-model

        for _ in range(3):  # a request's own model
            r = await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "model": "mock-model@second"})
            assert r.status_code == 200
        assert second["served"] == 3 and stack.mock["served"] == 0
        assert json.loads(second["last_body"])["model"] == "mock-model"  # the plain name upstream

        await add_alias(c, "pinned", "mock-model@upstream")  # an alias target
        for _ in range(3):
            assert (
                await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "model": "pinned"})
            ).status_code == 200
        assert stack.mock["served"] == 3 and second["served"] == 3

        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "model": "mock-model@nowhere"})
        assert r.status_code == 404  # not an upstream's name: just an unknown model

        new = (await c.get("/admin/aliases/new")).text  # the picker offers one entry per provider
        assert '"id": "mock-model@second"' in new and '"id": "mock-model@upstream"' in new and "only second" in new
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()["provider"]["queuerouter"]["models"]
        assert config["pinned"]["name"] == "pinned → mock-model@upstream · upstream"
        assert config["pinned"]["limit"]["context"] == 262_144


async def test_pinned_council_member(make_stack, make_mock):
    from tests.test_council import ASK

    stack = make_stack(limit=2, bootstrap=OWNER, models_cache_s=0)
    second_url, second = make_mock(limit=2)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url, max_concurrency=2)
        await c.get("/v1/models", headers=stack.headers)

        csrf = csrf_of((await c.get("/admin/aliases/new")).text)
        form = {
            "csrf": csrf,
            "name": "team",
            "target_model": "mock-model@upstream",
            "council": "combine",
            "council_members": "mock-model@second",
            "council_judge": "mock-model@upstream",
            "council_when": "always",
        }
        assert "m=created" in (await c.post("/admin/aliases", data=form)).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200
        assert second["served"] == 1 and stack.mock["served"] == 1  # the member on second, the judge on upstream
