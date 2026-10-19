"""Usage tracking: request log, daily rollup, CSV, and rate windows surviving a restart."""

import asyncio

import httpx

from tests.conftest import eventually
from tests.test_admin import OWNER, login
from tests.test_policies import set_policy


def chat(tag, **extra):
    return {"model": "mock-model", "messages": [{"role": "user", "content": tag}], "mock_tag": tag, **extra}


async def flushed(stack, count):
    """Usage is written by a background task about once a second."""
    store = stack.app.state.store

    async def rows():
        return len(await asyncio.to_thread(store.list_requests, 1000))

    for _ in range(100):
        if await rows() >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("usage not flushed")


async def test_request_log_and_usage_pages(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a", mock_chunks=3))
        async with c.stream(
            "POST",
            "/v1/chat/completions",
            headers=stack.headers,
            json=chat("b", stream=True, mock_chunks=4, mock_finish="length"),
        ) as r:
            _ = [x async for x in r.aiter_bytes()]
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("c", mock_status=400))
        await flushed(stack, 3)

        page = await c.get("/admin/requests")
        assert page.status_code == 200 and "length" in page.text and "mock-model" in page.text
        errors_only = await c.get("/admin/requests?outcome=error")
        assert errors_only.text.count("<time data-ts") == 1

        usage = await c.get("/admin/usage")
        assert usage.status_code == 200 and ">3<" in usage.text  # 3 requests
        csv_text = (await c.get("/admin/usage.csv?by=model")).text
        header, row = csv_text.strip().splitlines()[:2]
        assert header == "day,client,model,requests,errors,prompt_tokens,completion_tokens"
        assert row.split(",")[1:] == ["client1", "mock-model", "3", "1", "20", "7"]


async def test_no_content_is_stored(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        body = chat("secret prompt text 4242")
        await c.post("/v1/chat/completions", headers=stack.headers, json=body)
        await flushed(stack, 1)
    rows = stack.app.state.store.query("SELECT * FROM requests")
    assert all("4242" not in str(tuple(r)) for r in rows)


async def test_rate_windows_survive_restart(make_stack, tmp_path):
    data_dir = str(tmp_path / "shared")
    first = make_stack(bootstrap={**OWNER, "data_dir": data_dir})
    async with httpx.AsyncClient(base_url=first.url, timeout=30) as c:
        await login(c, first.url)
        await set_policy(c, first, rpm=2, on_limit="reject")
        for tag in ("a", "b"):
            assert (await c.post("/v1/chat/completions", headers=first.headers, json=chat(tag))).status_code == 200
        await flushed(first, 2)

    # a second app on the same data directory = a restart
    second = make_stack(bootstrap={**OWNER, "data_dir": data_dir})
    async with httpx.AsyncClient(base_url=second.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=second.headers, json=chat("c"))
        assert r.status_code == 429  # still within the minute: the 2 earlier requests count
        await eventually(lambda: True)
