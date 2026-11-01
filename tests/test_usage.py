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

        assert (await c.get("/admin/requests")).status_code == 200
        rows = (await c.get("/admin/api/requests")).json()["rows"]
        assert [r["finish_reason"] for r in rows if r["finish_reason"] == "length"] == ["length"]
        assert {r["model"] for r in rows} == {"mock-model"}
        assert len((await c.get("/admin/api/requests?outcome=error")).json()["rows"]) == 1

        usage = await c.get("/admin/usage")
        assert usage.status_code == 200 and ">3<" in usage.text  # 3 requests
        csv_text = (await c.get("/admin/usage.csv?by=model")).text
        header, row = csv_text.strip().splitlines()[:2]
        columns = (
            "day,client,model,requests,errors,prompt_tokens,completion_tokens,cached_tokens,cache_write_tokens,cost_usd"
        )
        assert header == f"{columns},upstream,label"
        assert row.split(",")[1:9] == ["client1", "mock-model", "3", "1", "20", "7", "0", "0"]
        assert row.endswith(",upstream,upstream|mock-model")


async def test_requests_page_fetches_only_new_rows(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("old"))
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("x", mock_status=400))
        await flushed(stack, 2)
        error_row, old = await asyncio.to_thread(store.list_requests, 10)
        page = (await c.get("/admin/requests")).text
        assert old["id"] not in page and 'data-rows="/admin/api/requests"' in page  # opens without its rows

        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("new"))
        await flushed(stack, 3)
        new = (await asyncio.to_thread(store.list_requests, 1))[0]

        async def ids(query):
            return [r["id"] for r in (await c.get(f"/admin/api/requests?{query}")).json()["rows"]]

        assert await ids(f"after={error_row['ts']}") == [new["id"], error_row["id"]]  # the newest shown counts too
        assert await ids(f"after={new['ts'] + 1}") == []
        assert await ids("after=0&outcome=error") == [error_row["id"]]
        assert await ids(f"before={error_row['ts']}&before_id={error_row['id']}") == [old["id"]]
        assert (await c.get("/admin/api/requests?after=soon")).status_code == 400
        assert (await c.get("/admin/api/requests?before=soon")).status_code == 400
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as anonymous:
        for path in ("/admin/api/requests", "/admin/api/requests/filters", f"/admin/api/requests/{new['id']}"):
            assert (await anonymous.get(path)).status_code in (303, 401)


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
