"""Allowed apps per client: the key only works for requests from the given User-Agents."""

import asyncio
import dataclasses

import httpx

from app.config import ClientPolicy
from tests.test_admin import OWNER, csrf_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


def test_policy_matching():
    policy = ClientPolicy(user_agents=("myapp/", "telegramgrouphelper/"))
    assert policy.allows_app("myapp/1.0") and policy.allows_app("myapp/2.3 (linux)")
    assert policy.allows_app("telegramgrouphelper/0.1")
    assert not policy.allows_app("python-httpx/0.28") and not policy.allows_app("")
    assert not policy.allows_app("xmyapp/1.0")  # a prefix, not "contains"
    assert ClientPolicy().allows_app("anything") and ClientPolicy().allows_app("")


async def test_requests_from_other_apps_are_refused(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    client = store.list_clients()[0]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        # no rule yet: any app works
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)).status_code == 200

        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)
        r = await c.post(
            f"/admin/clients/{client.id}/policy",
            data={"csrf": csrf, "on_limit": "delay", "user_agents": "myapp/, other/"},
        )
        assert "m=saved" in r.headers["location"]
        assert store.list_clients()[0].policy.user_agents == ("myapp/", "other/")
        assert 'value="myapp/, other/"' in (await c.get("/admin/clients")).text

        refused = await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)
        assert refused.status_code == 403 and refused.json()["error"]["type"] == "app_not_allowed"
        assert "myapp" not in refused.text  # the caller isn't told what's expected
        assert (await c.get("/v1/models", headers=stack.headers)).status_code == 403  # every API call

        ok = await c.post("/v1/chat/completions", headers={**stack.headers, "User-Agent": "myapp/1.4"}, json=CHAT)
        assert ok.status_code == 200
        assert (await c.get("/v1/models", headers={**stack.headers, "User-Agent": "other/9"})).status_code == 200

    for _ in range(50):
        rows = await asyncio.to_thread(store.list_requests, 10, outcome="refused")
        if len(rows) == 2:
            break
        await asyncio.sleep(0.1)
    assert len(rows) == 2 and rows[0]["user_agent"].startswith("python-httpx") and rows[0]["status"] == 403
    usage = await asyncio.to_thread(store.usage, 1)
    assert sum(u["requests"] for u in usage) == 2  # the two real requests; refusals aren't usage


async def test_rule_survives_other_policy_edits(make_stack):
    stack = make_stack()
    store = stack.app.state.store
    client = store.list_clients()[0]
    store.update_client(client.id, "t", policy=dataclasses.replace(client.policy, user_agents=("myapp/",)))
    stack.app.state.reload()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers={**stack.headers, "User-Agent": "myapp/1"}, json=CHAT)
        assert r.status_code == 200
