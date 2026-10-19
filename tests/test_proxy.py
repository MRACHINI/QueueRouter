import asyncio
import json

import httpx
import pytest

from tests.conftest import ACCESS_KEY, eventually


def chat(tag="x", stream=False, **extra):
    return {
        "model": "mock-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": stream,
        "mock_tag": tag,
        **extra,
    }


async def read_stream(client, url, headers, body):
    async with client.stream("POST", f"{url}/v1/chat/completions", headers=headers, json=body) as r:
        assert r.status_code == 200
        return r, b"".join([c async for c in r.aiter_bytes()]).decode()


async def stats(client, stack):
    return (await client.get(f"{stack.url}/stats", headers=stack.headers)).json()


# -- auth and limits -----------------------------------------------------------------------


async def test_requires_key(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", json=chat())
        assert r.status_code == 401
        r = await c.post(f"{stack.url}/v1/chat/completions", json=chat(), headers={"x-api-key": ACCESS_KEY})
        assert r.status_code == 200


async def test_blocks_ip_after_repeated_failures(make_stack):
    stack = make_stack(auth_fail_limit=3)
    async with httpx.AsyncClient() as c:
        for _ in range(3):
            await c.get(f"{stack.url}/v1/models", headers={"Authorization": "Bearer wrong"})
        r = await c.get(f"{stack.url}/v1/models", headers=stack.headers)
        assert r.status_code == 429 and "Retry-After" in r.headers


async def test_body_too_large(make_stack):
    stack = make_stack(max_body_mb=0.001)
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(padding="x" * 5000))
        assert r.status_code == 413


async def test_upstream_key_replaces_client_key(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{stack.url}/v1/models", headers=stack.headers)
        assert r.status_code == 200
        assert r.json()["data"][0]["max_model_len"] == 262144


# -- queueing ------------------------------------------------------------------------------


async def test_never_exceeds_limit_with_mixed_burst(make_stack):
    stack = make_stack(limit=2)
    async with httpx.AsyncClient(timeout=30) as c:

        async def nonstream(i):
            r = await c.post(
                f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(f"n{i}", mock_delay=0.02)
            )
            assert r.status_code == 200, r.text
            assert r.json()["choices"][0]["message"]["content"] == f"hi n{i}"

        async def streamed(i):
            _, text = await read_stream(c, stack.url, stack.headers, chat(f"s{i}", stream=True, mock_delay=0.02))
            assert text.rstrip().endswith("data: [DONE]")

        await asyncio.gather(*[nonstream(i) for i in range(6)], *[streamed(i) for i in range(6)])

    assert stack.mock["peak"] <= 2
    assert stack.mock["rejected"] == 0
    assert stack.mock["served"] == 12


async def test_fifo_order(make_stack):
    stack = make_stack(limit=1)
    async with httpx.AsyncClient(timeout=30) as c:
        tasks = []
        for i in range(5):
            tasks.append(
                asyncio.create_task(
                    c.post(
                        f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(str(i), mock_delay=0.02)
                    )
                )
            )
            await asyncio.sleep(0.05)  # make arrival order deterministic
        await asyncio.gather(*tasks)
    starts = [tag for event, tag in stack.mock["log"] if event == "start"]
    assert starts == ["0", "1", "2", "3", "4"]


async def test_keepalive_while_queued(make_stack):
    stack = make_stack(limit=1, keepalive_interval_s=0.1)
    async with httpx.AsyncClient(timeout=30) as c:
        blocker = asyncio.create_task(
            c.post(
                f"{stack.url}/v1/chat/completions",
                headers=stack.headers,
                json=chat("blocker", mock_delay=0.1, mock_chunks=5),
            )
        )
        await eventually(lambda: stack.mock["active"] == 1)
        _, text = await read_stream(c, stack.url, stack.headers, chat("queued", stream=True))
        await blocker
    assert ": queued id=" in text
    assert ": queued position=1" in text
    assert "data: [DONE]" in text


async def test_queue_wait_timeout(make_stack):
    stack = make_stack(limit=1, max_queue_wait_s=0.2)
    async with httpx.AsyncClient(timeout=30) as c:
        blocker = asyncio.create_task(
            c.post(
                f"{stack.url}/v1/chat/completions",
                headers=stack.headers,
                json=chat("blocker", mock_delay=0.2, mock_chunks=5),
            )
        )
        await eventually(lambda: stack.mock["active"] == 1)
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat("late"))
        assert r.status_code == 504
        _, text = await read_stream(c, stack.url, stack.headers, chat("late-stream", stream=True))
        assert '"code": 504' in text
        await blocker


# -- retries and errors --------------------------------------------------------------------


async def test_retries_transient_failures(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(
            f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(mock_key="k1", mock_fail=2)
        )
        assert r.status_code == 200
        _, text = await read_stream(c, stack.url, stack.headers, chat(stream=True, mock_key="k2", mock_fail=2))
        assert "HTTP 503, retry 1" in text and "data: [DONE]" in text
        assert (await stats(c, stack))["retries"] == 4


async def test_gives_up_after_max_retries(make_stack):
    stack = make_stack(max_retries=1)
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(mock_status=503))
        assert r.status_code == 503
        _, text = await read_stream(c, stack.url, stack.headers, chat(stream=True, mock_status=503))
        assert '"code": 503' in text and text.rstrip().endswith("data: [DONE]")


async def test_client_errors_are_not_retried(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(mock_status=400))
        assert r.status_code == 400
        _, text = await read_stream(c, stack.url, stack.headers, chat(stream=True, mock_status=400))
        assert '"code": 400' in text and '"message": "forced"' in text
        assert (await stats(c, stack))["retries"] == 0


async def test_upstream_down(make_stack):
    stack = make_stack(upstream_url="http://127.0.0.1:9/v1", max_retries=1)
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.status_code == 502
        assert (await stats(c, stack))["active"] == 0


# -- disconnects ---------------------------------------------------------------------------


async def test_disconnect_mid_stream_frees_slot(make_stack):
    stack = make_stack(limit=1)
    async with httpx.AsyncClient(timeout=30) as c:
        async with c.stream(
            "POST",
            f"{stack.url}/v1/chat/completions",
            headers=stack.headers,
            json=chat("long", stream=True, mock_delay=0.2, mock_chunks=50),
        ) as r:
            async for chunk in r.aiter_bytes():
                if b"t0" in chunk:
                    break
        await eventually(lambda: stack.mock["aborted"] == 1)
        await eventually(lambda: stack.mock["active"] == 0)
        s = await stats(c, stack)
        assert s["active"] == 0 and s["outcomes"].get("cancelled") == 1
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat("next"))
        assert r.status_code == 200


@pytest.mark.parametrize("stream", [False, True])
async def test_disconnect_while_queued_never_reaches_upstream(make_stack, stream):
    stack = make_stack(limit=1, keepalive_interval_s=0.1)
    async with httpx.AsyncClient(timeout=30) as c:
        blocker = asyncio.create_task(
            c.post(
                f"{stack.url}/v1/chat/completions",
                headers=stack.headers,
                json=chat("blocker", mock_delay=0.3, mock_chunks=5),
            )
        )
        await eventually(lambda: stack.mock["active"] == 1)

        async def impatient():
            async with httpx.AsyncClient(timeout=30) as other:
                await other.post(
                    f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat("impatient", stream=stream)
                )

        task = asyncio.create_task(impatient())
        await asyncio.sleep(0.3)
        assert (await stats(c, stack))["queued"] == 1
        task.cancel()  # closes the connection, like opencode aborting a request
        await asyncio.gather(task, return_exceptions=True)
        for _ in range(100):
            if (await stats(c, stack))["queued"] == 0:
                break
            await asyncio.sleep(0.05)
        assert (await stats(c, stack))["queued"] == 0
        await blocker
    tags = [tag for _, tag in stack.mock["log"]]
    assert "impatient" not in tags


async def test_usage_and_stats(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(timeout=30) as c:
        await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat(mock_chunks=3))
        await read_stream(c, stack.url, stack.headers, chat(stream=True, mock_chunks=4))
        s = await stats(c, stack)
    assert s["outcomes"]["done"] == 2
    assert s["tokens"] == {"prompt": 20, "completion": 7}
    assert s["max_concurrency"] == 2


async def test_tool_call_payload_passes_through_untouched(make_stack):
    """Whatever the client sends (tools, response_format, ...) reaches the upstream unchanged."""
    stack = make_stack()
    body = chat(
        tools=[{"type": "function", "function": {"name": "f", "parameters": {"type": "object"}}}],
        tool_choice="auto",
        response_format={"type": "json_object"},
        seed=7,
    )
    raw = json.dumps(body).encode()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(
            f"{stack.url}/v1/chat/completions",
            content=raw,
            headers={**stack.headers, "Content-Type": "application/json"},
        )
        assert r.status_code == 200
    assert stack.mock["last_body"] == raw
    assert stack.mock["last_auth"] == "Bearer upstream-secret"


# -- scheduling and hints --------------------------------------------------------------------

KEYS = ("chat:k-chat:high", "batch:k-batch:low")


def auth(key):
    return {"Authorization": f"Bearer {key}"}


async def test_high_priority_client_goes_first(make_stack):
    stack = make_stack(limit=1, access_keys=KEYS)
    async with httpx.AsyncClient(timeout=30) as c:
        url = f"{stack.url}/v1/chat/completions"
        blocker = asyncio.create_task(c.post(url, headers=auth("k-batch"), json=chat("blocker", mock_delay=0.1)))
        await eventually(lambda: stack.mock["active"] == 1)
        batch = asyncio.create_task(c.post(url, headers=auth("k-batch"), json=chat("batch")))
        await asyncio.sleep(0.1)
        chat_req = asyncio.create_task(c.post(url, headers=auth("k-chat"), json=chat("chat")))
        await asyncio.gather(blocker, batch, chat_req)
    starts = [tag for event, tag in stack.mock["log"] if event == "start"]
    assert starts == ["blocker", "chat", "batch"]


async def test_priority_header_can_only_lower(make_stack):
    stack = make_stack(limit=1, access_keys=KEYS)
    async with httpx.AsyncClient(timeout=30) as c:
        url = f"{stack.url}/v1/chat/completions"
        blocker = asyncio.create_task(c.post(url, headers=auth("k-chat"), json=chat("blocker", mock_delay=0.1)))
        await eventually(lambda: stack.mock["active"] == 1)
        # batch asks for "high" (ignored: it's a low client); chat asks for "low" (honoured)
        batch = asyncio.create_task(
            c.post(url, headers={**auth("k-batch"), "X-QueueRouter-Priority": "high"}, json=chat("batch"))
        )
        await asyncio.sleep(0.1)
        lowered_headers = {**auth("k-chat"), "X-QueueRouter-Priority": "low", "X-Session-Id": "separate-task"}
        lowered = asyncio.create_task(c.post(url, headers=lowered_headers, json=chat("lowered")))
        await asyncio.gather(blocker, batch, lowered)
    starts = [tag for event, tag in stack.mock["log"] if event == "start"]
    assert starts == ["blocker", "batch", "lowered"]


async def test_queue_hints(make_stack):
    stack = make_stack(limit=1)
    async with httpx.AsyncClient(timeout=30) as c:
        url = f"{stack.url}/v1/chat/completions"
        first = await c.post(url, headers=stack.headers, json=chat("warmup"))
        assert first.headers["X-QueueRouter-Position"] == "0"
        assert first.headers["X-QueueRouter-Slots"] == "0/1"

        blocker = asyncio.create_task(c.post(url, headers=stack.headers, json=chat("blocker", mock_delay=0.1)))
        await eventually(lambda: stack.mock["active"] == 1)
        q = (await c.get(f"{stack.url}/v1/queue", headers=stack.headers)).json()
        assert q["active"] == 1 and q["max_concurrency"] == 1
        assert q["client"]["name"] == "client1" and q["estimated_wait_ms"] > 0

        queued = await c.post(url, headers=stack.headers, json=chat("queued"))
        assert queued.headers["X-QueueRouter-Position"] == "1"
        assert queued.headers["X-QueueRouter-Slots"] == "1/1"
        assert int(queued.headers["X-QueueRouter-Est-Wait-Ms"]) > 0
        assert int(queued.headers["X-Queue-Wait-Ms"]) > 0
        await blocker


async def test_stats_per_client(make_stack):
    stack = make_stack(access_keys=KEYS)
    async with httpx.AsyncClient(timeout=30) as c:
        url = f"{stack.url}/v1/chat/completions"
        await c.post(url, headers=auth("k-chat"), json=chat())
        await c.post(url, headers=auth("k-batch"), json=chat())
        await c.post(url, headers=auth("k-batch"), json=chat())
        s = (await c.get(f"{stack.url}/stats", headers=auth("k-chat"))).json()
    assert s["per_client"]["chat"]["outcomes"] == {"done": 1}
    assert s["per_client"]["batch"]["outcomes"] == {"done": 2}
    assert s["queued_by_lane"] == {"high": 0, "normal": 0, "low": 0}


async def test_not_configured_without_upstream(make_stack):
    stack = make_stack(upstream_api_key="")
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.status_code == 503 and r.json()["error"]["type"] == "not_configured"
        assert (await c.get(f"{stack.url}/health")).status_code == 200


async def test_finish_reason_logged(make_stack):
    """finish_reason=length shows a reply was cut off by the token limit (e.g. truncated tool calls)."""
    stack = make_stack()
    async with httpx.AsyncClient(timeout=30) as c:
        await read_stream(c, stack.url, stack.headers, chat("cut", stream=True, mock_finish="length"))
        await c.post(f"{stack.url}/v1/chat/completions", headers=stack.headers, json=chat("whole"))
    recent = list(stack.app.state.stats.recent)
    assert [r["finish_reason"] for r in recent] == ["length", "stop"]
