"""Compaction (trim mode): conversations too big for the model are shrunk before sending."""

import asyncio
import json

import httpx

from app.compaction import SUMMARY_MARK, Plans, compact, needed
from app.params import estimate_prompt_tokens
from tests.test_admin import OWNER, csrf_of, login


def conversation(turns: int, output_chars: int = 6_000) -> list[dict]:
    """An agent session: system, task, then tool-call rounds, then the current question."""
    messages = [{"role": "system", "content": "You are a coding agent."}, {"role": "user", "content": "TASK: fix bug"}]
    for i in range(turns):
        call = {"id": f"c{i}", "type": "function", "function": {"name": "read", "arguments": json.dumps({"n": i})}}
        messages.append({"role": "assistant", "content": f"reading {i}", "tool_calls": [call]})
        messages.append({"role": "tool", "tool_call_id": f"c{i}", "content": f"FILE{i} " + "x" * output_chars})
    messages.append({"role": "assistant", "content": "done reading"})
    messages.append({"role": "user", "content": "CURRENT: now what?"})
    return messages


def test_trim_keeps_what_matters():
    payload = {"model": "m", "messages": conversation(30)}  # ~45k tokens
    before = estimate_prompt_tokens(payload)
    assert needed(payload, 40_000, 1_000) and not needed(payload, 200_000, 1_000)
    result = compact(payload, 40_000, 1_000, Plans(), "s")
    assert result and result.mode == "trim" and not result.reused
    assert result.before == before and result.after <= 40_000 * 0.5
    out = result.payload["messages"]
    assert out[0]["role"] == "system" and out[1]["content"].startswith("TASK: fix bug")
    assert out[-1]["content"] == "CURRENT: now what?" and out[-6:] == payload["messages"][-6:]  # recent turns intact
    assert out[2]["role"] == "assistant"  # never a tool result without its call
    ids = {c["id"] for m in out if m.get("tool_calls") for c in m["tool_calls"]}
    assert all(m["tool_call_id"] in ids for m in out if m["role"] == "tool")
    assert "trimmed by QueueRouter" in json.dumps(out)  # old tool outputs shortened first
    assert payload["messages"][1]["content"] == "TASK: fix bug"  # the original isn't modified


def test_only_shortening_when_enough():
    payload = {"model": "m", "messages": conversation(8)}  # ~12k tokens, mostly tool output
    result = compact(payload, 14_000, 1_000, Plans(), None)
    assert result and len(result.payload["messages"]) == len(payload["messages"])  # nothing dropped
    assert "earlier messages were removed" not in json.dumps(result.payload)


def test_plan_is_reused_while_it_fits():
    plans = Plans()
    messages = conversation(30)
    first = compact({"messages": messages}, 40_000, 1_000, plans, "s")
    grown = [*messages, {"role": "assistant", "content": "ok"}, {"role": "user", "content": "more"}]
    second = compact({"messages": grown}, 40_000, 1_000, plans, "s")
    assert second and second.reused
    assert second.payload["messages"][: len(first.payload["messages"]) - 6] == first.payload["messages"][:-6]
    # another conversation, or an edited start, doesn't reuse it
    edited = [messages[0], {"role": "user", "content": "TASK: other"}, *messages[2:]]
    assert not compact({"messages": edited}, 40_000, 1_000, plans, "s").reused
    assert SUMMARY_MARK  # (summaries arrive with summarise mode)


def test_nothing_to_cut():
    short = [{"role": "system", "content": "s"}, {"role": "user", "content": "x" * 200_000}]
    assert compact({"messages": short}, 10_000, 1_000, Plans(), None) is None


async def test_alias_compacts_before_sending(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model"]
    stack.mock["context_limits"] = {"small-model": (12_000, "openai")}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        await c.post("/admin/model-limits", data={"csrf": csrf, "model": "small-model", "context": "12000"})
        form = {"csrf": csrf, "name": "lean", "target_model": "small-model", "compact": "trim"}
        assert "m=created" in (await c.post("/admin/aliases", data=form)).headers["location"]
        assert 'value="trim" selected' in (await c.get("/admin/models")).text

        payload = {"model": "lean", "messages": conversation(14, 4_000)}  # ~14k tokens: over the 12k limit
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=payload)
        assert r.status_code == 200, r.text
        assert r.headers["x-queuerouter-compacted"].startswith("trim ")
        sent = json.loads(stack.mock["last_body"])
        assert len(json.dumps(sent["messages"])) < len(json.dumps(payload["messages"])) / 2
        assert sent["messages"][-1]["content"] == "CURRENT: now what?"

        plain = {**payload, "model": "small-model"}  # no alias: no compaction, refused before sending
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=plain)
        assert r.status_code == 400 and r.json()["error"]["code"] == "context_length_exceeded"

    for _ in range(50):
        rows = await asyncio.to_thread(stack.app.state.store.list_requests, 10)
        if any(r["compaction"] for r in rows):
            break
        await asyncio.sleep(0.1)
    assert any((r["compaction"] or "").startswith("trim ") for r in rows)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        assert "compacted: trim" in (await c.get("/admin/requests")).text


async def test_request_lists_split_tokens(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        chat = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}], "mock_cached": 4}
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=chat)).status_code == 200
        await login(c, stack.url)
        for _ in range(50):
            page = (await c.get("/admin/requests")).text
            if 'data-label="In tokens"' in page:
                break
            await asyncio.sleep(0.1)
    assert "<th>In tokens</th><th>Out tokens</th>" in page
    assert (
        '<td data-label="In tokens" title="4 of them cached">10 <span class="muted small">(4 cached)</span></td>'
        in page
    )
    assert '<td data-label="Out tokens">5</td>' in page


def agent_turn(rounds: int, chars: int = 4_000) -> list[dict]:
    """One long opencode turn: the user's latest message early, then many tool rounds (the request ends
    with a tool result, not a user message)."""
    messages = conversation(2, 500)[:-1]  # system, task, two rounds, "done reading"
    messages.append({"role": "user", "content": "LATEST: refactor the parser"})
    for i in range(rounds):
        call = {"id": f"t{i}", "type": "function", "function": {"name": "edit", "arguments": json.dumps({"n": i})}}
        messages.append({"role": "assistant", "content": f"step {i}", "tool_calls": [call]})
        messages.append({"role": "tool", "tool_call_id": f"t{i}", "content": f"OUT{i} " + "z" * chars})
    return messages


def test_cut_reaches_into_the_current_turn():
    payload = {"messages": agent_turn(100, 1_500)}  # outputs too short to shorten: rounds must go
    result = compact(payload, 40_000, 1_000, Plans(), None)
    out = result.payload["messages"]
    assert result.after <= 20_000  # before: nothing after the latest user message could be cut
    assert out[-6:] == payload["messages"][-6:]
    assert not any(m.get("content") == "LATEST: refactor the parser" for m in out[2:])  # moved...
    assert out[1]["content"].endswith("[The user's latest message]\nLATEST: refactor the parser")  # ...next to the task
    assert out[2]["role"] == "assistant"


def test_estimate_calibration():
    from app.stats import Stats

    stats = Stats()
    assert stats.ratio("m") == 1.0
    stats.learn_ratio("m", 42_000, 56_700)
    assert 1.34 < stats.ratio("m") < 1.36
    stats.learn_ratio("m", 500, 5_000)  # too small to judge
    stats.learn_ratio("other", 10_000, 8_000)
    assert stats.ratio("other") == 1.0  # never trusted to be pessimistic


async def test_requests_page_lists_queued_and_running(make_stack):
    stack = make_stack(limit=1, bootstrap=OWNER)
    slow = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}], "mock_delay": 0.4}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        tasks = [
            asyncio.ensure_future(c.post("/v1/chat/completions", headers=stack.headers, json=slow)) for _ in range(2)
        ]
        await asyncio.sleep(0.5)
        await login(c, stack.url)
        page = (await c.get("/admin/requests")).text
        assert 'id="requests-live" data-stream="/admin/api/live"' in page and 'id="finished-requests"' in page
        live = (await c.get("/admin/api/stats")).json()["inflight"]
        assert sorted(r["stage"] for r in live) == ["queued", "running"]
        row = live[0]
        assert row["requested_model"] == "mock-model" and row["priority"] and row["ts"] > 0
        assert row["path"] == "/v1/chat/completions" and row["estimate"] is not None
        await asyncio.gather(*tasks)
