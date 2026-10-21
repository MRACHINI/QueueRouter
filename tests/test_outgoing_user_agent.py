"""Outgoing User-Agent: what the provider sees, set per client with an upstream default."""

import httpx

from app import __version__
from app.upstream import outgoing_user_agent
from tests.test_admin import OWNER, csrf_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}
VALUES = {"original": "myapp/1.0", "client": "bot", "priority": "high", "model": "m", "upstream": "u", "version": "9"}


def test_rule_resolution():
    keep = ("keep", "")
    assert outgoing_user_agent(keep, keep, VALUES) is None
    assert outgoing_user_agent(("remove", ""), keep, VALUES) == "queuerouter/9"
    assert outgoing_user_agent(keep, ("remove", ""), VALUES) == "queuerouter/9"  # upstream default
    client = ("template", "{original} ({client}; {priority}) {unknown}")
    assert outgoing_user_agent(client, ("remove", ""), VALUES) == "myapp/1.0 (bot; high) {unknown}"  # client wins
    assert outgoing_user_agent(("template", "  "), keep, VALUES) is None
    assert outgoing_user_agent(("template", "a\r\nb é"), keep, VALUES) == "a b ?"  # one ASCII line


async def set_client(c, client_id, mode, template=""):
    csrf = csrf_of((await c.get("/admin/clients")).text)
    data = {"csrf": csrf, "on_limit": "delay", "ua_mode": mode, "ua_template": template}
    r = await c.post(f"/admin/clients/{client_id}/policy", data=data)
    assert "m=saved" in r.headers["location"], r.headers["location"]


async def set_upstream(c, upstream_id, mode, template=""):
    csrf = csrf_of((await c.get("/admin/upstreams")).text)
    form = {"csrf": csrf, "base_url": "", "enabled": "on", "weight": "1", "ua_mode": mode, "ua_template": template}
    r = await c.post(f"/admin/upstreams/{upstream_id}", data=form)
    assert "m=saved" in r.headers["location"], r.headers["location"]


async def test_outgoing_user_agent_end_to_end(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    client = store.list_clients()[0]
    upstream = store.list_upstreams()[0]
    app_headers = {**stack.headers, "User-Agent": "myapp/1.0"}

    async def sent(headers=app_headers):
        r = await c.post("/v1/chat/completions", headers=headers, json=CHAT)
        assert r.status_code == 200, r.text
        return stack.mock["last_headers"].get("user-agent")

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        assert await sent() == "myapp/1.0"  # keep: passed through

        await set_upstream(c, upstream.id, "remove")
        assert store.list_upstreams()[0].ua_mode == "remove"
        assert await sent() == f"queuerouter/{__version__}"

        await set_client(c, client.id, "template", "{original} via {client}/{priority} to {upstream} for {model}")
        policy = store.list_clients()[0].policy
        assert policy.ua_mode == "template" and "{client}" in policy.ua_template
        assert await sent() == f"myapp/1.0 via {client.name}/normal to {upstream.name} for mock-model"
        low = {**app_headers, "X-QueueRouter-Priority": "low"}
        assert (await sent(low)).endswith(f"{client.name}/low to {upstream.name} for mock-model")
        page = (await c.get("/admin/clients")).text
        assert "{original} via {client}" in page

        await set_upstream(c, upstream.id, "template", "edge/{version}")
        await set_client(c, client.id, "keep")
        assert await sent() == f"edge/{__version__}"
        assert 'value="edge/{version}"' in (await c.get("/admin/upstreams")).text

        # a template mode needs a template
        csrf = csrf_of((await c.get("/admin/clients")).text)
        r = await c.post(
            f"/admin/clients/{client.id}/policy",
            data={"csrf": csrf, "on_limit": "delay", "ua_mode": "template", "ua_template": ""},
        )
        assert "m=saved" not in r.headers.get("location", "")
        assert store.list_clients()[0].policy.ua_mode == "keep"


async def test_allowed_apps_checks_the_original_user_agent(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    client = store.list_clients()[0]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)
        data = {"csrf": csrf, "on_limit": "delay", "user_agents": "myapp/", "ua_mode": "remove"}
        assert "m=saved" in (await c.post(f"/admin/clients/{client.id}/policy", data=data)).headers["location"]
        r = await c.post("/v1/chat/completions", headers={**stack.headers, "User-Agent": "myapp/2"}, json=CHAT)
        assert r.status_code == 200
        assert stack.mock["last_headers"]["user-agent"] == f"queuerouter/{__version__}"
        refused = await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)
        assert refused.status_code == 403
