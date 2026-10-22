"""Separate embedding slots per limit pool: /v1/embeddings don't take chat slots where the provider
limits them separately."""

import asyncio
import time

import httpx

from app.presets import ARLIAI_PLANS
from tests.test_admin import OWNER, csrf_of, login

EMBED = {"model": "mock-model", "input": "hello", "mock_delay": 0.3}
SLOW_CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}], "mock_delay": 0.4}  # ~2 s


async def set_pool(c, stack, **fields):
    pool = stack.app.state.store.list_pools()[0]
    csrf = csrf_of((await c.get("/admin/upstreams")).text)
    form = {"csrf": csrf, "plan": "", "max_concurrency": "1", "context_cap": "", "min_spacing_ms": "0", **fields}
    r = await c.post(f"/admin/pools/{pool.id}", data=form)
    assert "m=saved" in r.headers["location"], r.headers["location"]


async def run_together(c, stack) -> tuple[float, float]:
    """Start a slow chat, then two embeddings; (when the embeddings were done, when the chat was)."""
    start = time.monotonic()

    async def chat():
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=SLOW_CHAT)
        assert r.status_code == 200
        return time.monotonic() - start

    async def embed():
        r = await c.post("/v1/embeddings", headers=stack.headers, json=EMBED)
        assert r.status_code == 200, r.text
        return time.monotonic() - start

    chat_task = asyncio.ensure_future(chat())
    await asyncio.sleep(0.2)  # the chat has the only chat slot
    embedded = max(await asyncio.gather(embed(), embed()))
    return embedded, await chat_task


async def test_embeddings_get_their_own_slots(make_stack):
    stack = make_stack(limit=1, bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        embedded, chatted = await run_together(c, stack)  # shared (default): they wait for the chat
        assert embedded > chatted

        await set_pool(c, stack, embedding_concurrency="2")
        assert stack.app.state.store.list_pools()[0].embedding_concurrency == 2
        stack.mock["embed_peak"] = 0
        embedded, chatted = await run_together(c, stack)
        assert embedded < chatted  # done while the chat still runs
        assert stack.mock["embed_peak"] == 2  # both at once: two embedding slots

        live = (await c.get("/v1/queue", headers=stack.headers)).json()
        assert live["max_concurrency"] == 1  # chat slots unchanged
        page = (await c.get("/admin/upstreams")).text
        assert 'name="embedding_concurrency"' in page and 'value="2"' in page

        await set_pool(c, stack, embedding_concurrency="")  # back to shared
        assert stack.app.state.store.list_pools()[0].embedding_concurrency is None


async def test_arliai_plan_fills_embedding_limit(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        form = {"csrf": csrf, "plan": "Pro", "max_concurrency": "", "context_cap": "", "min_spacing_ms": "0"}
        await c.post(f"/admin/pools/{pool.id}", data=form)
        assert stack.app.state.store.list_pools()[0].embedding_concurrency == 2
    assert {p.name: p.embedding_concurrency for p in ARLIAI_PLANS}["Max"] == 6
