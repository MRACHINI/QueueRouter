"""Orchestrator (council mode "orchestrate"): plans, delegates briefs to members, uses web search, web
fetch and a calculator through tools QueueRouter runs, then answers; progress streams as reasoning."""

import json

import httpx
import pytest

import app.websearch as websearch
from app.tools import calculate
from tests.test_admin import OWNER, csrf_of, login
from tests.test_council import ASK, rows_with, setup

ASK_NAME = "queuerouter_ask_member"


def test_calculator():
    assert calculate("262144*0.8") == "262144*0.8 = 209715.2"
    assert calculate("2**10") == "2**10 = 1024" and calculate("sqrt(16)+pi*0") == "sqrt(16)+pi*0 = 4"
    for bad in ("__import__('os')", "open('x')", "9**9**9", "1/0", "[1,2]"):
        assert calculate(bad).startswith("Can't calculate")


def test_readable_page():
    html = "<html><head><title>T</title><style>p{}</style></head>"
    html += "<body><nav>menu</nav><p>Hello</p><p>World</p></body></html>"
    assert websearch.readable(html) == "T\n\nHello\nWorld"


async def test_fetch_refuses_private_and_secret_addresses():
    async with httpx.AsyncClient() as client:
        for url in ("http://localhost:8000/admin", "http://127.0.0.1/", "http://169.254.169.254/latest/meta-data"):
            with pytest.raises(websearch.SearchError, match="public internet"):
                await websearch.fetch(client, url)
        with pytest.raises(websearch.SearchError, match="secret"):
            await websearch.fetch(client, "https://example.com/?k=sk-abcdefghijklmnopqrstuvwxyz1234")
        with pytest.raises(websearch.SearchError, match="http"):
            await websearch.fetch(client, "file:///etc/passwd")


async def orchestrate_setup(c, stack, **fields):
    store = stack.app.state.store
    store.set_tunables({"search_provider": "searxng", "search_url": stack.mock["url"]}, "t")
    stack.app.state.reload()
    await setup(c, stack, mode="pick")
    team = next(a for a in store.list_aliases() if a.name == "team")
    page = (await c.get(f"/admin/aliases/{team.id}")).text
    form = {
        "csrf": csrf_of(page),
        "target_model": "mock-model",
        "council": "orchestrate",
        "council_members": "mock-model, small-model",
        "council_judge": "mock-model",
        "council_when": "planning",
        "council_search": "on",
        "council_fetch": "on",
        "enabled": "on",
        **fields,
    }
    assert "m=saved" in (await c.post(f"/admin/aliases/{team.id}", data=form)).headers["location"]


async def test_orchestrator_delegates_uses_tools_and_answers(make_stack, monkeypatch):
    async def anywhere(host):  # the test page is on 127.0.0.1, which the real check refuses
        return None

    monkeypatch.setattr(websearch, "public_host", anywhere)
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["orch_script"] = [
        {
            "note": "Plan: ask both members.",
            "calls": [
                (ASK_NAME, {"member": "mock-model", "task": "find where config is loaded"}),
                (ASK_NAME, {"member": "small-model", "task": "check the call sites"}),
            ],
        },
        {
            "calls": [
                ("queuerouter_web_search", {"query": "python 3.14 release"}),
                ("queuerouter_web_fetch", {"url": f"{stack.mock['url']}/page"}),
                ("queuerouter_calculate", {"expression": "262144*0.8"}),
                (ASK_NAME, {"member": "nobody", "task": "x"}),
            ]
        },
        {"answer": "final from orchestrator"},
    ]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack)
        payload = {"model": "team", "messages": ASK, "stream": True}
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=payload) as s:
            text = "".join([chunk async for chunk in s.aiter_text()])
    notes = "".join(
        json.loads(line[5:])["choices"][0]["delta"].get("reasoning_content", "")
        for line in text.splitlines()
        if line.startswith("data: {") and "reasoning_content" in line
    )
    assert "Orchestrator (mock-model) is planning · team: mock-model, small-model" in notes
    assert "Orchestrator: Plan: ask both members." in notes
    assert "→ mock-model: find where config is loaded" in notes and "✓ small-model answered" in notes
    assert 'searches the web: "python 3.14 release"' in notes and "reads http" in notes
    assert "262144*0.8 = 209715.2" in notes and "Orchestrator is answering." in notes
    assert "final from orchestrator" in text and text.rstrip().endswith("data: [DONE]")

    first, last = stack.mock["orch_requests"][0], stack.mock["orch_requests"][-1]
    assert "You lead a team" in first["messages"][0]["content"] and "Today is" in first["messages"][0]["content"]
    ask = next(t for t in first["tools"] if t["function"]["name"] == ASK_NAME)
    assert ask["function"]["parameters"]["properties"]["member"]["enum"] == ["mock-model", "small-model"]
    results = [m["content"] for m in last["messages"] if m.get("role") == "tool"]
    joined = "\n".join(results)
    assert "answer from mock-model: find where config is loaded" in joined  # the member's brief, answered
    assert "Results for" in joined and "Install docs" in joined and "Use Python 3.14 or newer." in joined
    assert "var x" not in joined and "Home | Blog" not in joined  # scripts and navigation left out
    assert "262144*0.8 = 209715.2" in joined and "No member called 'nobody'" in joined
    assert sorted(stack.mock["member_tasks"]) == [
        ("mock-model", "find where config is loaded"),
        ("small-model", "check the call sites"),
    ]  # briefs only: members don't get the conversation

    rows = await rows_with(stack.app.state.store, lambda row: row["council"])
    council = next(row for row in rows if row["council"])
    assert council["council"].startswith("orchestrate: 2 rounds, 2 member tasks")
    assert {row["role"] for row in rows if row["role"]} >= {"orchestrator", "orchestrator member mock-model"}


async def test_orchestrator_budget(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["orch_script"] = [{"calls": [(ASK_NAME, {"member": "small-model", "task": "again"})]}]  # never stops
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack, orch_rounds="1")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200  # it never answered by itself: the target model answers
    assert stack.mock["orch_turn"] == 3  # round 1, then "budget used up", then it is stopped
    assert len(stack.mock["member_tasks"]) == 1
    rows = await rows_with(stack.app.state.store, lambda row: row["council"])
    assert any("(budget)" in (row["council"] or "") or "alone" in (row["council"] or "") for row in rows)


async def test_editor_offers_orchestrate(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/aliases/new")).text
    assert '<option value="orchestrate"' in page and 'name="orch_rounds"' in page and 'name="council_fetch"' in page
