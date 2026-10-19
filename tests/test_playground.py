"""Admin Playground: chat as a client through the normal pipeline."""

import asyncio
import json

import httpx

from tests.test_admin import OWNER, csrf_of, login
from tests.test_aliases import add_alias
from tests.test_policies import set_policy


async def send(c, csrf, **payload):
    return await c.post(
        "/admin/playground/send",
        headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
        content=json.dumps({"client": "client1", "messages": [{"role": "user", "content": "hi"}], **payload}),
    )


async def test_playground_as_a_client(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = await c.get("/admin/playground")
        assert page.status_code == 200 and "client1" in page.text
        csrf = csrf_of(page.text)

        r = await send(c, csrf, model="mock-model")
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"].startswith("hi")

        # the client's own rules and aliases apply
        await add_alias(c, "smart", "mock-model")
        await set_policy(c, stack, params=json.dumps({"overrides": {"temperature": 0.25}}))
        async with c.stream(
            "POST",
            "/admin/playground/send",
            headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
            content=json.dumps(
                {"client": "client1", "model": "smart", "stream": True, "messages": [{"role": "user", "content": "x"}]}
            ),
        ) as s:
            text = "".join([chunk async for chunk in s.aiter_text()])
        assert "t0 " in text and text.rstrip().endswith("data: [DONE]")
        sent = json.loads(stack.mock["last_body"])
        assert sent["model"] == "mock-model" and sent["temperature"] == 0.25
        assert "cookie" not in stack.mock["last_headers"] and "x-csrf-token" not in stack.mock["last_headers"]

        # tagged as playground in the request log
        store = stack.app.state.store
        for _ in range(100):
            if await asyncio.to_thread(store.list_requests, 10, tag="playground"):
                break
            await asyncio.sleep(0.05)
        assert await asyncio.to_thread(store.list_requests, 10, tag="playground")

        assert (await send(c, csrf, model="mock-model", client="nobody")).status_code == 404


async def test_playground_needs_operator_and_csrf(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as owner:
        await login(owner, stack.url)
        assert (await send(owner, "forged", model="mock-model")).status_code == 403
        csrf = csrf_of((await owner.get("/admin/users")).text)
        await owner.post(
            "/admin/users", data={"csrf": csrf, "username": "v", "role": "viewer", "password": "viewer password!"}
        )
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as viewer:
        await login(viewer, stack.url, "v", "viewer password!")
        assert (await viewer.get("/admin/playground")).status_code == 403
