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
    assert 'searches the web: "python 3.14 release"' in notes and "reads a page: http" in notes
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


async def test_notebook_carries_the_work_across_steps(make_stack):
    """Step 1: it plans, writes its notebook and asks the app to read a file. Step 2 (the file's content comes
    back as a tool result, which a planning-steps council would leave to the target model): it continues from
    its notebook and answers."""
    stack = make_stack(bootstrap=OWNER, models_cache_s=0, compaction_saved=True)
    stack.mock["orch_script"] = [
        {"calls": [("queuerouter_notebook", {"text": "PLAN: 1) read config.py 2) fix the loader"})]},
        {"app_call": ("read_file", {"path": "config.py"})},
        {"answer": "fixed: the loader now reads the env file"},
    ]
    tools = [{"type": "function", "function": {"name": "read_file", "parameters": {"type": "object"}}}]
    headers = {**stack.headers, "X-Session-Id": "ses-nb"}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack)
        r = await c.post(
            "/v1/chat/completions", headers=headers, json={"model": "team", "messages": ASK, "tools": tools}
        )
        call = r.json()["choices"][0]["message"]["tool_calls"][0]
        assert call["function"]["name"] == "read_file"  # the app's own tool goes back to the app
        step2 = [
            *ASK,
            {"role": "assistant", "content": None, "tool_calls": [call]},
            {"role": "tool", "tool_call_id": call["id"], "content": "LOAD_ENV = False"},
        ]
        r = await c.post(
            "/v1/chat/completions", headers=headers, json={"model": "team", "messages": step2, "tools": tools}
        )
        assert r.json()["choices"][0]["message"]["content"] == "fixed: the loader now reads the env file"
    third = stack.mock["orch_requests"][2]  # the orchestrator again, on the tool step
    assert "Your notebook for this conversation" in third["messages"][0]["content"]
    assert "PLAN: 1) read config.py 2) fix the loader" in third["messages"][0]["content"]
    assert third["messages"][-1]["content"] == "LOAD_ENV = False"
    store = stack.app.state.store
    rows = store.query("SELECT key, data FROM compaction_summaries WHERE key LIKE 'notebook:%'")
    assert len(rows) == 1 and "PLAN" not in rows[0]["data"]  # saved, encrypted
    assert store.load_summary(rows[0]["key"])["notebook"].startswith("PLAN:")


async def test_without_a_notebook_tool_steps_go_to_the_target(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack)
        step = [*ASK, {"role": "assistant", "content": "x"}, {"role": "tool", "tool_call_id": "t", "content": "y"}]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": step})
        assert r.status_code == 200 and "orch_requests" not in stack.mock


def test_roles_parsing():
    from app.council import parse_roles, role_note, roster_text

    text = "a = researcher: search and cite\nb=reviewer\nnot-a-member = x: y\nbroken line\n= nothing"
    roles = parse_roles(text, ["a", "b"])
    assert roles == {"a": ("researcher", "search and cite"), "b": ("reviewer", "")}
    assert roster_text(["a", "b", "c"], roles) == "a (researcher: search and cite); b (reviewer); c"
    assert "Your role: reviewer." in role_note("reviewer", "")


async def test_roles_and_trace_page(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["orch_script"] = [
        {"note": "Plan.", "calls": [(ASK_NAME, {"member": "small-model", "task": "secret project details"})]},
        {"answer": "done"},
    ]
    roles = "small-model = reviewer: look for bugs\nmock-model = researcher"
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack, council_roles=roles)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200
        system = stack.mock["orch_requests"][0]["messages"][0]["content"]
        assert "Team: mock-model (researcher); small-model (reviewer: look for bugs)" in system
        task_brief = [b for b in stack.mock.get("member_tasks", [])]
        assert task_brief == [("small-model", "secret project details")]
        rows = await rows_with(stack.app.state.store, lambda row: row["council"])
        council = next(row for row in rows if row["council"])
        trace = (await c.get(f"/admin/requests/{council['id']}/trace")).text
        assert "Timeline" in trace and "→ small-model" in trace and "orchestrator member small-model" in trace
        assert "secret project details" not in trace  # content logging is off: no excerpts
        listing = (await c.get("/admin/requests")).text
        assert f'href="/admin/requests/{council["id"]}/trace"' in listing
        assert "member = role: instructions" in (await c.get("/admin/aliases/new")).text


async def test_council_members_answer_in_their_roles(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await setup(c, stack)
        team = next(a for a in stack.app.state.store.list_aliases() if a.name == "team")
        page = (await c.get(f"/admin/aliases/{team.id}")).text
        form = {
            "csrf": csrf_of(page),
            "target_model": "mock-model",
            "council": "pick",
            "council_members": "mock-model, small-model",
            "council_roles": "small-model = skeptic: question every assumption",
            "enabled": "on",
        }
        assert "m=saved" in (await c.post(f"/admin/aliases/{team.id}", data=form)).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "team", "messages": ASK})
        assert r.status_code == 200
        judged = stack.mock["judge_inputs"][-1]
        assert "### Answer 2 (skeptic)" in judged and "### Answer 1\n" in judged


def test_conflicting_proposals():
    from app.council import conflicts, read_only

    assert read_only("read_file") and read_only("grep") and read_only("list") and not read_only("edit")
    edit = {"function": {"name": "edit", "arguments": json.dumps({"filePath": "a.py", "new": "x"})}}
    write = {"function": {"name": "write", "arguments": json.dumps({"path": "a.py"})}}
    other = {"function": {"name": "edit", "arguments": json.dumps({"filePath": "b.py"})}}
    read = {"function": {"name": "read_file", "arguments": json.dumps({"path": "a.py"})}}
    assert conflicts({"P1": edit, "P2": write}) == "P1 and P2 both change a.py: adopt one of them"
    assert conflicts({"P1": edit, "P2": other, "P3": read}) is None  # different files; reading never conflicts


async def test_members_propose_and_the_orchestrator_adopts(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["orch_script"] = [
        {"calls": [(ASK_NAME, {"member": "small-model", "task": "look at the config loader", "propose": True})]},
        {"calls": [("queuerouter_adopt_proposals", {"proposals": ["P9"]})]},  # a mistake: told what exists
        {"calls": [("queuerouter_adopt_proposals", {"proposals": ["p1"], "text": "Reading the config first."})]},
    ]
    tools = [{"type": "function", "function": {"name": "read_file", "parameters": {"type": "object"}}}]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await orchestrate_setup(c, stack)
        payload = {"model": "team", "messages": ASK, "tools": tools}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=payload)
    message = r.json()["choices"][0]["message"]
    assert message["content"] == "Reading the config first."
    assert [(t["function"]["name"], t["function"]["arguments"]) for t in message["tool_calls"]] == [
        ("read_file", '{"q": "x"}')
    ]  # exactly as the member proposed it
    second = stack.mock["orch_requests"][1]
    results = "\n".join(m["content"] for m in second["messages"] if m.get("role") == "tool")
    assert "P1: read_file(" in results
    third = stack.mock["orch_requests"][2]
    assert "Unknown proposals ['P9']: the proposals so far are P1." in third["messages"][-1]["content"]
    assert len(stack.mock["orch_requests"]) == 3  # adopting ended the step
    first_tools = {t["function"]["name"] for t in stack.mock["orch_requests"][0]["tools"]}
    assert {"queuerouter_adopt_proposals", "read_file"} <= first_tools
