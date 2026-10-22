"""Compaction, summarise mode: rolling summaries (cached, extended), chunked for small summarisers,
idle updates, saved summaries, and the context size told to apps."""

import asyncio
import json

import httpx

import app.compaction as compaction_mod
import app.main as main_mod
from app.compaction import SUMMARY_MARK, chunks, conversation_key, plan_from_dict, plan_to_dict, summary_plan
from tests.test_admin import OWNER, csrf_of, login
from tests.test_compaction import conversation

SESSION = {"X-Session-Id": "sess-1"}


def test_helpers():
    messages = conversation(6, 3_000)
    parts = chunks(messages[2:14], 8_000)
    assert len(parts) > 1 and all(len(p) <= 8_000 for p in parts)
    assert "### tool result" in parts[0] and "[calls read(" in parts[0]
    assert len(chunks([{"role": "tool", "content": "y" * 50_000}], 8_000)[0]) <= 8_000  # one huge message
    other = [messages[0], {"role": "user", "content": "TASK: other"}, *messages[2:]]
    assert conversation_key("c", "s", messages) != conversation_key("c", "s", other)  # subagents apart
    assert conversation_key("c", None, messages) is None
    plan = summary_plan(messages, 2, 8, "notes")
    assert plan_from_dict(json.loads(json.dumps(plan_to_dict(plan)))) == plan


async def setup_alias(c, stack, **fields):
    await c.get("/v1/models", headers=stack.headers)
    await login(c, stack.url)
    csrf = csrf_of((await c.get("/admin/models")).text)
    await c.post("/admin/model-limits", data={"csrf": csrf, "model": "small-model", "context": "12000"})
    form = {"csrf": csrf, "name": "sum", "target_model": "small-model", "compact": "summarise", **fields}
    assert "m=created" in (await c.post("/admin/aliases", data=form)).headers["location"]


async def send(c, stack, turns: int, chars: int = 4_000):
    payload = {"model": "sum", "messages": conversation(turns, chars)}
    r = await c.post("/v1/chat/completions", headers={**stack.headers, **SESSION}, json=payload)
    assert r.status_code == 200, r.text
    return r.headers.get("x-queuerouter-compacted", ""), json.loads(stack.mock["last_body"])


async def test_summary_written_reused_and_extended(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, compaction_idle_update=False)
    stack.mock["extra_models"] = ["small-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await setup_alias(c, stack, compact_model="mock-model", virtual_context="1000000")

        label, sent = await send(c, stack, 14)
        assert label.startswith("summarise ") and label.endswith("(new summary)")
        assert len(stack.mock["summary_inputs"]) == 1  # the big summariser reads it in one go
        assert "Summary so far:\n(none yet)" in stack.mock["summary_inputs"][0]
        task = sent["messages"][1]["content"]
        assert task.startswith("TASK: fix bug") and SUMMARY_MARK in task and task.endswith("summary 1")
        assert sent["messages"][-1]["content"] == "CURRENT: now what?"

        label, _ = await send(c, stack, 15)  # a bit more: the summary still covers enough
        assert label.endswith("(reused)") and len(stack.mock["summary_inputs"]) == 1

        label, sent = await send(c, stack, 24)  # much more: the summary is extended, not rewritten
        assert label.endswith("(summary updated)") and len(stack.mock["summary_inputs"]) == 2
        assert stack.mock["summary_inputs"][1].startswith("Summary so far:\nsummary 1")
        assert "FILE0 " not in stack.mock["summary_inputs"][1]  # only the newly old part is read
        assert sent["messages"][1]["content"].endswith("summary 2")

        # opencode is told the bigger context
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["sum"]["limit"]["context"] == 1_000_000
        models = (await c.get("/v1/models", headers=stack.headers)).json()["data"]
        assert next(m for m in models if m["id"] == "sum")["context_length"] == 1_000_000
        page = (await c.get("/admin/models")).text
        assert "summarise with mock-model" in page and "1,000,000 context" in page


async def test_small_summariser_reads_in_parts(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, compaction_idle_update=False)
    stack.mock["extra_models"] = ["small-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await setup_alias(c, stack)  # no summary model: the 12k target writes its own summaries
        label, sent = await send(c, stack, 14)
        inputs = stack.mock["summary_inputs"]
        assert label.endswith("(new summary)") and len(inputs) > 1
        assert inputs[1].startswith("Summary so far:\nsummary 1")  # rolling, one part after another
        assert sent["messages"][1]["content"].endswith(f"summary {len(inputs)}")


async def test_summary_failure_falls_back_to_trim(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, compaction_idle_update=False)
    stack.mock["extra_models"] = ["small-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await setup_alias(c, stack, compact_model="no-such-model")
        label, sent = await send(c, stack, 14)
        assert label.startswith("trim ")
        assert SUMMARY_MARK not in json.dumps(sent)


async def test_idle_update(make_stack, monkeypatch):
    monkeypatch.setattr(main_mod, "IDLE_AFTER_S", 0)
    monkeypatch.setattr(compaction_mod, "IDLE_START_AT", 0.01)  # every conversation counts as near the limit
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await setup_alias(c, stack, compact_model="mock-model")
        await send(c, stack, 14)
        label, _ = await send(c, stack, 16)
        assert label.endswith("(reused)") or label.endswith("(summary updated)")
        for _ in range(150):  # the idle loop runs every 5 s
            if len(stack.mock["summary_inputs"]) >= 2:
                break
            await asyncio.sleep(0.1)
        assert len(stack.mock["summary_inputs"]) >= 2
        assert stack.mock["summary_inputs"][-1].startswith("Summary so far:\nsummary")
        for _ in range(50):
            rows = await asyncio.to_thread(stack.app.state.store.list_requests, 20)
            if any(r["priority"] == "low" and "compaction" in (r["tags"] or "") for r in rows):
                break
            await asyncio.sleep(0.1)
        assert any(r["priority"] == "low" and "compaction" in (r["tags"] or "") for r in rows)  # in the background
        label, sent = await send(c, stack, 16)  # the next message finds the updated summary ready
        assert label.endswith("(reused)")
        assert sent["messages"][1]["content"].endswith(f"summary {len(stack.mock['summary_inputs'])}")


async def test_saved_summaries(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, compaction_idle_update=False, compaction_saved=True)
    stack.mock["extra_models"] = ["small-model"]
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await setup_alias(c, stack, compact_model="mock-model")
        await send(c, stack, 14)
    rows = store.query("SELECT key, client, data FROM compaction_summaries")
    assert len(rows) == 1 and "summary 1" not in rows[0]["data"]  # encrypted
    assert store.load_summary(rows[0]["key"])["summary"] == "summary 1"
    client = store.list_clients()[0]
    store.delete_client(client.id, "t")
    assert store.query("SELECT COUNT(*) AS n FROM compaction_summaries")[0]["n"] == 0
