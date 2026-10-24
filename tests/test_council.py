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
        form = {"csrf": csrf, "name": "solo", "target_model": "mock-model", "council": "pick", "council_members": ""}
        assert "A council needs 1 to 64 members" in (await c.post("/admin/aliases", data=form)).text


async def test_council_context_and_self_target(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)  # members mock-model (262,144) and small-model (size not reported)
        csrf = csrf_of((await c.get("/admin/models")).text)
        await c.post("/admin/model-limits", data={"csrf": csrf, "model": "small-model", "context": "100000"})
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["team"]["limit"]["context"] == 100_000  # the smallest
        form = {"csrf": csrf, "name": "loop", "target_model": "loop"}
        assert "Choose the model this alias sends to" in (await c.post("/admin/aliases", data=form)).text
        team = next(a for a in stack.app.state.store.list_aliases() if a.name == "team")
        r = await c.post(f"/admin/aliases/{team.id}", data={"csrf": csrf, "target_model": "team", "enabled": "on"})
        assert r.status_code == 200 and "Choose the model this alias sends to" in r.text


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


async def test_members_carry_the_apps_identity_and_their_role(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    store = stack.app.state.store
    app_headers = {**stack.headers, "User-Agent": "opencode/2.1.4", "x-opencode-session-id": "ses_9"}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        r = await c.post("/v1/chat/completions", headers=app_headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200
        assert stack.mock["last_headers"]["user-agent"] == "opencode/2.1.4"  # what the provider sees
        assert stack.mock["last_headers"]["authorization"] == "Bearer upstream-secret"  # the upstream key, not ours
        assert not any(k.startswith("x-queuerouter-") for k in stack.mock["last_headers"])
        rows = await rows_with(store, lambda row: row["role"] == "council judge")
        council = next(row for row in rows if row["council"])
        helpers = [row for row in rows if row["role"]]
        assert sorted(row["role"] for row in helpers) == ["council judge", "council member 1/2", "council member 2/2"]
        assert all(row["parent"] == council["id"] and row["user_agent"] == "opencode/2.1.4" for row in helpers)
        assert all("x-opencode-session-id" in (row["client_headers"] or "") for row in helpers)
        page = (await c.get("/admin/requests")).text
        assert f"for {council['id']}" in page and "council member 1/2" in page


async def test_one_member_and_many(make_stack):
    from app.council import judge_payload

    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack, mode="combine", members="small-model", judge="mock-model")  # a second opinion
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "combined answer"
        assert stack.mock["models"] == ["small-model", "mock-model"]  # the member, then the judge
    many = [(f"m{i}", {"content": "x" * 20_000}) for i in range(64)]
    judged = judge_payload({"messages": ASK}, "judge", "pick", many)["messages"][-1]["content"]
    assert judged.count("### Answer") == 64 and len(judged) < 140_000  # each answer gets its share


async def test_judge_instructions(make_stack):
    from app.council import judge_payload

    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        team = next(a for a in stack.app.state.store.list_aliases() if a.name == "team")
        page = (await c.get(f"/admin/aliases/{team.id}")).text
        form = {
            "csrf": csrf_of(page),
            "target_model": "mock-model",
            "council": "pick",
            "enabled": "on",
            "council_members": "mock-model, small-model",
            "council_instructions": "Prefer the shortest answer.",
        }
        assert "m=saved" in (await c.post(f"/admin/aliases/{team.id}", data=form)).headers["location"]
        assert "Prefer the shortest answer." in (await c.get(f"/admin/aliases/{team.id}")).text
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200
        judged = stack.mock["judge_inputs"][-1]
        assert "instructions from the administrator:\nPrefer the shortest answer." in judged
        assert judged.rstrip().endswith("Reply with only the number of the best answer.")  # the format stays last
    combine = judge_payload({"messages": ASK}, "j", "combine", [("a", {"content": "x"})], "Be brief.")
    text = combine["messages"][-1]["content"]
    assert "Be brief." in text and not text.rstrip().endswith("number of the best answer.")
    assert (
        "administrator"
        not in judge_payload({"messages": ASK}, "j", "pick", [("a", {"content": "x"})])["messages"][-1]["content"]
    )


def test_verdict_reading():
    from app.council import verdict

    assert verdict("2", 3) == 1 and verdict(" 3.", 3) == 2
    assert verdict("<think>Answer 1 misses tests, so not 1.</think>2", 3) == 1  # thinking left inline
    assert verdict("Answer 2 is the best.", 3) == 1 and verdict("The best one is 3", 3) == 2
    assert verdict("", 3) is None and verdict("I cannot decide", 3) is None and verdict("7", 3) is None
    assert verdict("<think>still thinking when the limit hit", 3) is None


def test_search_queries_are_checked():
    import pytest

    from app.websearch import SearchError, check_query

    assert check_query("  python   3.14 release notes ") == "python 3.14 release notes"
    for secret in ("sk-abcdefghijklmnopqrstuvwxyz123456", "api_key=abc", "-----BEGIN RSA PRIVATE KEY", "a" * 60):
        with pytest.raises(SearchError):
            check_query(f"docs {secret}")


async def search_setup(c, stack, members, **settings):
    store = stack.app.state.store
    store.set_tunables({"search_provider": "searxng", "search_url": stack.mock["url"], **settings}, "t")
    stack.app.state.reload()
    await setup(c, stack, mode="combine", members=members, judge="mock-model")
    team = next(a for a in store.list_aliases() if a.name == "team")
    page = (await c.get(f"/admin/aliases/{team.id}")).text
    assert "not set up yet" not in page
    form = {
        "csrf": csrf_of(page),
        "target_model": "mock-model",
        "council": "combine",
        "council_members": members,
        "council_judge": "mock-model",
        "council_search": "on",
        "council_search_max": "1",
        "enabled": "on",
    }
    assert "m=saved" in (await c.post(f"/admin/aliases/{team.id}", data=form)).headers["location"]


async def test_members_search_the_web(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["searching_models"] = ["mock-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await search_setup(c, stack, "mock-model, small-model")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "combined answer"
        assert stack.mock["searches"] == ["latest benchmarks"]  # run by QueueRouter, once
        judged = stack.mock["judge_inputs"][-1]
        assert "answer from mock-model citing Results for" in judged  # the member answered with the results
        assert "[Web search results this member used]" in judged and "https://example.com/1" in judged
        assert "[This member ran no web search" in judged  # small-model didn't search
        judge_tools = [t["function"]["name"] for t in json.loads(stack.mock["last_body"]).get("tools") or []]
        assert "queuerouter_web_search" not in judge_tools  # the app's tools only
        assert "web search" in (await c.get("/admin/models")).text
    rows = await rows_with(stack.app.state.store, lambda row: row["council"])
    council = next(row for row in rows if row["council"])
    assert council["council"].endswith("1 searches")
    assert any(row["role"] == "council member 1/2 · after search" for row in rows)


async def test_search_limit_and_failures(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock.update(searching_models=["mock-model"], always_search=True, search_status=403)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await search_setup(c, stack, "mock-model, small-model")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200  # the member that never stops searching is dropped; the other one counts
        assert len(stack.mock["searches"]) == 1  # the limit (1) held
        assert "Search failed: searxng returned HTTP 403" not in stack.mock["judge_inputs"][-1]  # dropped member
    rows = await rows_with(stack.app.state.store, lambda row: row["council"])
    council = next(row for row in rows if row["council"])
    assert council["error"].startswith("web search: searxng returned HTTP 403")


async def test_pick_judge_may_think(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        stack.mock["judge_answer"] = "<think>Answer 1 is weaker.</think>2"
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.headers["x-queuerouter-council"] == "pick 2/2: small-model"
        assert json.loads(stack.mock["last_body"])["max_tokens"] == 4000  # room to think
        stack.mock["judge_answer"] = "hmm, both are fine"
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.headers["x-queuerouter-council"] == "pick 1/2: mock-model (unclear verdict: took answer 1)"
