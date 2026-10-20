"""Live dashboard stream (SSE), the in-flight list, time to first byte, live Usage page."""

import asyncio
import json

import httpx

from tests.test_admin import OWNER, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


async def next_state(lines) -> dict:
    async for line in lines:
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise AssertionError("stream ended")


async def test_live_stream_shows_requests_as_they_happen(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        assert (await c.get("/admin/api/live")).status_code in (303, 307, 401)  # signed out
        await login(c, stack.url)
        dash = (await c.get("/admin")).text
        assert 'data-stream="/admin/api/live"' in dash and 'id="now-body"' in dash

        async with c.stream("GET", "/admin/api/live") as live:
            assert live.headers["content-type"].startswith("text/event-stream")
            lines = live.aiter_lines()
            first = await next_state(lines)
            assert first["inflight"] == [] and "usage_version" in first and isinstance(first["history"], list)
            version = first["usage_version"]

            slow = asyncio.ensure_future(
                c.post("/v1/chat/completions", headers=stack.headers, json={**CHAT, "stream": True, "mock_delay": 0.4})
            )
            running = None
            for _ in range(40):  # the request shows up while it runs, with its first-byte time
                state = await asyncio.wait_for(next_state(lines), 10)
                rows = [r for r in state["inflight"] if r["stage"] == "running" and r["ttft_ms"] is not None]
                if rows:
                    running = rows[0]
                    break
            assert running and running["model"] == "mock-model" and running["stream"] is True
            await slow
            for _ in range(40):  # ...and leaves the list when done; usage is written
                state = await asyncio.wait_for(next_state(lines), 10)
                if not state["inflight"] and state["usage_version"] > version:
                    break
            assert not state["inflight"] and state["usage_version"] > version
            assert len(state["history"]) <= 5  # only the newest chart points after the first message


async def test_time_to_first_byte_recorded(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json={**CHAT, "stream": True}) as s:
            _ = [chunk async for chunk in s.aiter_bytes()]
        await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)
        for _ in range(50):
            rows = await asyncio.to_thread(store.list_requests, 10)
            if len(rows) == 2:
                break
            await asyncio.sleep(0.1)
        assert all(r["ttft_ms"] is not None and r["ttft_ms"] >= 0 for r in rows)
        streamed = next(r for r in rows if r["stream"])
        assert streamed["ttft_ms"] < streamed["total_ms"]  # first chunk well before the end

        await login(c, stack.url)
        assert "First byte ms" in (await c.get("/admin/requests")).text
        usage = (await c.get("/admin/usage")).text
        assert 'id="usage-live" data-stream="/admin/api/live"' in usage
    assert stack.app.state.stats.ttft_hist.n == 2


async def test_live_stream_ends_when_stopping(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        async with c.stream("GET", "/admin/api/live") as live:
            lines = live.aiter_lines()
            assert await anext(lines) == "retry: 1000"  # quick reconnects after a restart
            await next_state(lines)
            stack.app.state.stopping = True  # what a SIGTERM sets
            await asyncio.wait_for(_drain(lines), 3)  # the stream ends instead of waiting forever
        stack.app.state.stopping = False


async def _drain(lines) -> None:
    async for _ in lines:
        pass
