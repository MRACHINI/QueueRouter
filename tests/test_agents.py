"""Tags, run budgets and loop detection."""

import asyncio
import time

import httpx

from app.agents import Runs, parse_tags, request_fingerprint, run_key
from tests.test_admin import OWNER, csrf_of, login
from tests.test_policies import set_policy


def test_parse_tags():
    assert parse_tags("Coding, opencode", "bad tag!,opencode,x") == ["coding", "opencode", "x"]
    assert parse_tags(",".join(f"t{i}" for i in range(20))) == [f"t{i}" for i in range(10)]
    assert parse_tags(None, "") == []


def test_run_key_and_fingerprint():
    assert run_key({"x-run-id": "abc"}, "c", "s1") == "c:run:abc"
    assert run_key({}, "c", "s1") == "c:session:s1"
    assert run_key({}, "c", None) is None
    a = {"model": "m", "messages": [{"role": "user", "content": "x"}], "temperature": 0.1}
    b = {**a, "temperature": 0.9}  # sampling settings don't make it a different request
    assert request_fingerprint(a) == request_fingerprint(b)
    assert request_fingerprint({**a, "messages": []}) != request_fingerprint(a)


def test_budgets_and_loops():
    runs = Runs()
    run = runs.get("c:run:1")
    assert runs.over_budget(run, 100, 2, None) is None
    assert runs.note_request(run, "fp") == 1
    assert runs.note_request(run, "fp") == 2
    assert "2 requests" in runs.over_budget(run, None, 2, None)
    runs.note_tokens(run, 150)
    assert "150 tokens" in runs.over_budget(run, 100, None, None)
    run.started = time.time() - 120
    assert "more than 1 minutes" in runs.over_budget(run, None, None, 1)


def chat(content, **extra):
    return {"model": "mock-model", "messages": [{"role": "user", "content": content}], **extra}


async def test_run_budget_stops_a_runaway_agent(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, run_max_requests=3)
        headers = {**stack.headers, "X-Run-Id": "task-42"}
        for i in range(3):
            r = await c.post("/v1/chat/completions", headers=headers, json=chat(f"step {i}"))
            assert r.status_code == 200
        r = await c.post("/v1/chat/completions", headers=headers, json=chat("step 3"))
        assert r.status_code == 429 and r.json()["error"]["type"] == "run_budget_exceeded"
        # another run is unaffected
        other = {**stack.headers, "X-Run-Id": "task-43"}
        assert (await c.post("/v1/chat/completions", headers=other, json=chat("hi"))).status_code == 200


async def test_token_budget_and_header_lowering(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        headers = {**stack.headers, "X-Run-Id": "t", "X-QueueRouter-Run-Budget-Tokens": "20"}
        assert (await c.post("/v1/chat/completions", headers=headers, json=chat("a"))).status_code == 200  # 15 tokens
        assert (await c.post("/v1/chat/completions", headers=headers, json=chat("b"))).status_code == 200  # 30 used
        r = await c.post("/v1/chat/completions", headers=headers, json=chat("c"))
        assert r.status_code == 429 and "tokens" in r.json()["error"]["message"]


async def test_loop_detection(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, loop_threshold=3, loop_action="reject")
        for _ in range(2):
            assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("same"))).status_code == 200
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("same"))
        assert r.status_code == 429 and r.json()["error"]["type"] == "loop_detected"
        # different content is fine
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat("new"))).status_code == 200


async def test_tags_recorded_and_filterable(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await set_policy(c, stack, tags="team-a")
        headers = {**stack.headers, "X-QueueRouter-Tags": "Project-X, nightly"}
        await c.post("/v1/chat/completions", headers=headers, json=chat("tagged"))
        await c.post("/v1/chat/completions", headers=stack.headers, json=chat("plain"))
        store = stack.app.state.store
        for _ in range(100):
            if len(await asyncio.to_thread(store.list_requests, 10)) >= 2:
                break
            await asyncio.sleep(0.05)
        rows = await asyncio.to_thread(store.list_requests, 10, tag="project-x")
        assert [r["tags"] for r in rows] == ["team-a,project-x,nightly"]
        page = (await c.get("/admin/requests?tag=nightly")).text
        assert "#project-x" in page and page.count("<time data-ts") == 1
        csrf = csrf_of(page)
        assert csrf
    assert "x-queuerouter-tags" not in stack.mock["last_headers"]
