"""Model council: members answer, a judge picks one or writes the answer; planning steps only."""

import asyncio
import json

import httpx

from app.council import as_stream, parse_pick
from tests.test_admin import OWNER, csrf_of, login

ASK = [{"role": "system", "content": "You are a coding agent."}, {"role": "user", "content": "plan the refactor"}]


def test_helpers():
    assert parse_pick("2", 3) == 1 and parse_pick("Answer 3 is best", 3) == 2
    assert parse_pick("7", 3) == 0 and parse_pick(None, 3) == 0  # unclear: the first one
    body = json.dumps(
        {
            "id": "x",
            "model": "m",
            "choices": [{"message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
        }
    ).encode()
    text = as_stream(body, "m").decode()
    assert '"content": "hi"' in text and '"finish_reason": "stop"' in text and text.endswith("data: [DONE]\n\n")


async def setup(c, stack, mode="pick", members="mock-model, small-model", when="planning", judge=""):
    stack.mock["extra_models"] = ["small-model"]
    stack.mock["echo_model"] = True
    await c.get("/v1/models", headers=stack.headers)
    await login(c, stack.url)
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {
        "csrf": csrf,
        "name": "team",
        "target_model": "mock-model",
        "council": mode,
        "council_members": members,
        "council_judge": judge,
        "council_when": when,
    }
    r = await c.post("/admin/aliases", data=form)
    assert "m=created" in r.headers["location"], r.headers["location"]


async def rows_with(store, predicate):
    for _ in range(50):
        rows = await asyncio.to_thread(store.list_requests, 30)
        if any(predicate(r) for r in rows):
            return rows
        await asyncio.sleep(0.1)
    return rows


async def test_pick(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200, r.text
        assert r.json()["choices"][0]["message"]["content"] == "answer from small-model"  # the judge said 2
        assert r.headers["x-queuerouter-council"] == "pick 2/2: small-model"
        assert sorted(stack.mock["models"][:2]) == ["mock-model", "small-model"]  # both members
        assert "### Answer 2\nanswer from small-model" in stack.mock["judge_inputs"][0]

        # streaming: headers at once, keep-alives, then the chosen answer as a stream
        payload = {"model": "team", "messages": ASK, "stream": True}
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=payload) as s:
            assert s.status_code == 200
            text = "".join([chunk async for chunk in s.aiter_text()])
        assert text.startswith(": council") and "answer from small-model" in text and text.endswith("data: [DONE]\n\n")

        page = (await c.get("/admin/models")).text
        assert "council pick (2)" in page
    rows = await rows_with(stack.app.state.store, lambda r: r["council"])
    council_rows = [r for r in rows if r["council"]]
    assert council_rows[0]["council"] == "pick 2/2: small-model" and council_rows[0]["model"] == "team"
    assert sum(1 for r in rows if "council" in (r["tags"] or "") and not r["council"]) >= 3  # members + judge
    usage = await asyncio.to_thread(stack.app.state.store.usage, 1)
    assert sum(u["requests"] for u in usage) == len(rows) - len(council_rows)  # its own row isn't usage


async def test_only_planning_steps(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        step = [
            *ASK,
            {"role": "assistant", "content": "reading"},
            {"role": "tool", "tool_call_id": "a", "content": "x"},
        ]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": step})
        assert r.status_code == 200 and "x-queuerouter-council" not in r.headers
        assert stack.mock["models"] == ["mock-model"]  # the target alone
        low = {**stack.headers, "X-QueueRouter-Priority": "low"}  # titles and other background requests
        await c.post("/v1/chat/completions", headers=low, json={"model": "team", "messages": ASK})
        assert stack.mock["models"] == ["mock-model", "mock-model"]
        tagged = {**stack.headers, "X-QueueRouter-Tags": "agent:plan"}  # opencode's plan agent: every step
        r = await c.post("/v1/chat/completions", headers=tagged, json={"model": "team", "messages": step})
        assert r.headers["x-queuerouter-council"].startswith("pick")


async def test_combine_streams_the_judges_answer(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack, mode="combine", judge="small-model")
        payload = {"model": "team", "messages": ASK, "stream": True}
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=payload) as s:
            text = "".join([chunk async for chunk in s.aiter_text()])
        assert "Write the best possible reply" in stack.mock["judge_inputs"][0]
        assert "answer from mock-model" in stack.mock["judge_inputs"][0]
        assert stack.mock["models"][-1] == "small-model"  # the judge
        assert "t0 " in text and text.rstrip().endswith("data: [DONE]")  # the judge's own stream


async def test_members_fail_falls_back_and_validation(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack, members="no-such-a, no-such-b")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "answer from mock-model"
        csrf = csrf_of((await c.get("/admin/models")).text)
        form = {"csrf": csrf, "name": "solo", "target_model": "mock-model", "council": "pick", "council_members": "x"}
        assert "m=created" not in (await c.post("/admin/aliases", data=form)).headers["location"]  # needs two


async def test_council_context_and_self_target(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)  # members mock-model (262,144) and small-model (size not reported)
        csrf = csrf_of((await c.get("/admin/models")).text)
        await c.post("/admin/model-limits", data={"csrf": csrf, "model": "small-model", "context": "100000"})
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["team"]["limit"]["context"] == 100_000  # the smallest
        form = {"csrf": csrf, "name": "loop", "target_model": "loop"}
        assert "m=created" not in (await c.post("/admin/aliases", data=form)).headers["location"]
        team = next(a for a in stack.app.state.store.list_aliases() if a.name == "team")
        r = await c.post(f"/admin/aliases/{team.id}", data={"csrf": csrf, "target_model": "team", "enabled": "on"})
        assert "m=saved" not in r.headers["location"]


async def test_leaving_mid_council_leaves_nothing_behind(make_stack):
    """The app gives up while members answer, or while the judge streams: no row stays in the Now list
    and no slot stays taken."""
    stack = make_stack(limit=1, bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack, mode="combine", judge="small-model")
        slow = {"model": "team", "messages": ASK, "stream": True, "mock_delay": 0.3, "mock_chunks": 20}
        for wait_for in (b": council", b"data:"):  # leave during the members, then during the judge's stream
            async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=slow) as s:
                async for chunk in s.aiter_bytes():
                    if wait_for in chunk:
                        break
        for _ in range(100):
            live = (await c.get("/admin/api/stats")).json()
            if not live["inflight"] and live["active"] == 0:
                break
            await asyncio.sleep(0.1)
        assert live["inflight"] == [] and live["active"] == 0
        quick = {"model": "mock-model", "messages": ASK}  # the slot is free again
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=quick)).status_code == 200
