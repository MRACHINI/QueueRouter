"""An alias's answer check with taggers (app/taggers.py): a tagger reads an answer, and the alias's decisions on its
tags fail it (the next model answers) or pass it. ``checker`` makes a tagger like the checker model of 0.39-0.76:
exactly one of ok / bad, and the alias fails an answer tagged bad."""

import asyncio
import json
import time

import httpx
import pytest

from app import taggers
from app.answer_check import clip, last_user_text
from app.answer_rules import ANSWER_KINDS
from app.quoting import free_tag, unwrapped, without_thinking
from tests.test_admin import OWNER, csrf_of, login

INSTRUCTIONS = "Tag bad if the answer says bad, else ok."
TOOLS = [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}]


def checker(store, instructions=INSTRUCTIONS, model="mock-model", name="checker", **more) -> int:
    """A tagger giving exactly one of ok / bad (the tags are made once)."""
    tags = {t.name: t.id for t in store.list_tags()}
    for tag in ("ok", "bad"):
        if tag not in tags:
            tags[tag] = store.create_tag("t", tag, f"the answer is {tag}")
    if found := next((t for t in store.list_taggers() if t.name == name), None):
        return found.id
    raw = {"name": name, "model": model, "instructions": instructions, "tags": [tags["ok"], tags["bad"]]}
    raw |= {"pick": "one", "reads": list(ANSWER_KINDS), "message": "on", "reason": "on", **more}
    return store.create_tagger("t", **taggers.clean_tagger(raw, tags.values()))


def chain_fields(tagger_id: int, fail: str = "bad", slot: int = 0, **more) -> dict:
    """The alias editor's fields for a chain of one tagger whose answers tagged ``fail`` fail."""
    p = f"s{slot}_"
    fields = {"chain_slots": str(slot + 1), f"{p}tagger": str(tagger_id), f"{p}order": str(slot + 1)}
    fields |= {f"{p}lines": "1", f"{p}l0_when": "any", f"{p}l0_tags": fail, f"{p}l0_do": "fail"}
    return fields | {f"{p}otherwise": "next", f"{p}no_reply": "next", **more}


def checked(store, **more) -> dict:
    """The alias editor's fields for the checker (``checker``) failing answers tagged bad."""
    return chain_fields(checker(store, **more))


def reply(content):
    return json.dumps({"choices": [{"message": {"role": "assistant", "content": content}}]}).encode()


def test_what_a_check_reads_of_a_request():
    body = json.dumps(
        {
            "messages": [
                {"role": "system", "content": "You are an agent."},
                {"role": "user", "content": "first question"},
                {"role": "user", "content": [{"type": "text", "text": "look at this"}, {"type": "image_url"}]},
                {"role": "tool", "content": "tool output"},
            ]
        }
    ).encode()
    assert last_user_text(body) == "look at this\n[an image]"
    long = "a" * 50 + "b" * 50
    assert clip(long, 20) == f"{'a' * 10}\n[… 80 characters left out …]\n{'b' * 10}"
    assert clip("short", 20) == "short"


def test_quoting_for_a_model():
    assert free_tag("answer", "a </ANSWER > b", "<answer_2>") == "answer_3"
    assert free_tag("answer", "<answers> and answer_x") == "answer"
    assert unwrapped("<summary_2>\nnotes\n</summary_2>", "summary") == "notes"
    assert unwrapped("<summary>a</summary> and more", "summary") == "<summary>a</summary> and more"  # not all of it


def test_picking_a_tag_takes_time_in_proportion_to_the_text():
    # the text is the client's and this runs on the event loop: a "<" before a long run of spaces, or thousands of
    # numbered tags, must not make it slow (they made the first version backtrack, and rescan per number)
    started = time.monotonic()
    assert free_tag("answer", "<" + " " * 300_000 + "x", "< /" + "\t" * 300_000) == "answer"
    numbered = "<answer>" + "".join(f"<answer_{n}>" for n in range(2, 30_001))
    assert free_tag("answer", "x" * 200_000 + numbered) == "answer_30001"
    assert time.monotonic() - started < 3
    assert free_tag("answer", "<answer>", "<answer_1234567890>", "< / answer_3 >") == "answer_2"  # 10 digits: none
    assert free_tag("answer", "<ANSWER>", "</answer_3>") == "answer_2"


def test_thinking_left_inline_is_left_out():
    assert without_thinking("<think>hmm</think>PASS") == "PASS"
    assert without_thinking("  <THINKING>x\ny</thinking>\n<think>z</think>answer") == "answer"  # one after another
    assert without_thinking("<think>cut off while thinking") == ""  # never closed: all of it
    # later in the text, a tag is the answer's own (a summary or a prompt that mentions it): nothing is cut
    for text in ("- Goal: strip <think> blocks\n- Next: tests", "a <thinking>x</thinking> b", "no thinking"):
        assert without_thinking(text) == text


async def add_checked(c, store, fallbacks=("mock-model",), triggers=("errors",), expect="created", **more):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {
        "csrf": csrf,
        "name": "smart",
        "target_model": "strict-model",
        "fallbacks": ", ".join(fallbacks),
        **checked(store),
        **more,
    }
    form.update({f"on_{t}": "on" for t in triggers})
    r = await c.post("/admin/aliases", data=form)
    if expect == "created":
        assert "m=created" in r.headers["location"], r.headers["location"]
    return r


def chat(**extra):
    return {"model": "smart", "messages": [{"role": "user", "content": "hi"}], **extra}


def words(stack, strict, fallback=("good ", "answer")):
    stack.mock["model_words"] = {"strict-model": list(strict), "mock-model": list(fallback)}
    stack.mock["check_fail"] = ["bad"]


async def entries(stack, count=1):
    """The newest request (once ``count`` have finished: a stream's ends just after the app got all of it) and the
    rows made for it."""
    for _ in range(100):
        recent = list(stack.app.state.stats.recent)
        mains = [e for e in recent if not e.get("parent")]
        if len(mains) >= count:
            break
        await asyncio.sleep(0.05)
    main = mains[-1]
    return main, [e for e in recent if e.get("parent") == main["id"]]


async def stream(c, stack, **extra):
    async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=chat(stream=True, **extra)) as r:
        assert r.status_code == 200
        return "".join([chunk async for chunk in r.aiter_text()])


async def test_a_failed_answer_goes_to_the_next_model(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["bad ", "answer"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, store)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "good answer"
        assert r.headers["x-queuerouter-model"] == "mock-model"
        # the strict model, its tagger, then the next model (the last: not checked)
        assert stack.mock["models"] == ["strict-model", "mock-model", "mock-model"]
        assert len(stack.mock["check_inputs"]) == 1
        system, user = stack.mock["check_inputs"][0]["messages"]
        assert INSTRUCTIONS in system["content"] and "- bad: the answer is bad" in system["content"]
        assert "hi" in user["content"] and "bad answer" in user["content"]
        assert user["content"].endswith("The tags you can give: ok, bad. Reply with nothing else.")
        main, calls = await entries(stack)
        assert main["fallback"] == "strict-model -> mock-model (check: checker tagged bad: it says bad)"
        check = main["answer_check"]
        assert check.startswith("strict-model: checker tagged bad (") and check.endswith("): it says bad → failed")
        assert main["answer_tags"] == "bad"
        roles = {e["role"]: e for e in calls}
        assert set(roles) == {"tagger: checker", "failed answer check"}
        discarded = roles["failed answer check"]
        assert discarded["target_model"] == "strict-model" and discarded["completion_tokens"] == 2
        assert discarded["outcome"] == "done" and discarded["answer_check"] == "failed: checker tagged bad: it says bad"
        assert discarded["answer_tags"] == "bad"
        # stored, and shown under the row on the Requests page, which can show only the requests tagged bad
        for _ in range(100):
            rows = (await c.get("/admin/api/requests")).json()["rows"]
            if any(row["id"] == main["id"] for row in rows):
                break
            await asyncio.sleep(0.05)
        details = (await c.get(f"/admin/api/requests/{main['id']}")).json()
        assert details["fallback"] == main["fallback"] and details["answer_check"] == main["answer_check"]
        assert details["answer_tags"] == "bad"
        tagged = (await c.get("/admin/api/requests?answer_tag=bad")).json()["rows"]
        assert main["id"] in {row["id"] for row in tagged}
        assert (await c.get("/admin/api/requests?answer_tag=ok")).json()["rows"] == []
        assert "bad" in (await c.get("/admin/api/requests/filters")).json()["answer_tag"]
        trace = (await c.get(f"/admin/requests/{main['id']}/trace")).text
        assert "failed answer check" in trace and "answer check: strict-model: checker tagged bad" in trace
        for _ in range(60):  # the Taggers page counts it
            if next(t for t in store.list_tags() if t.name == "bad").given:
                break
            await asyncio.sleep(0.1)
        assert next(t for t in store.list_tags() if t.name == "bad").given == 1


async def test_a_passed_answer_streams_whole_once_checked(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["fine ", "and ", "complete"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store)
        text = await stream(c, stack)
        assert ": checking the answer of strict-model: checker" in text
        assert all(w in text for w in ("fine ", "and ", "complete")) and "trying" not in text
        assert text.rstrip().endswith("data: [DONE]")
        assert stack.mock["models"] == ["strict-model", "mock-model"]
        main, calls = await entries(stack)
        assert main["target_model"] == "strict-model" and main["fallback"] is None
        assert main["answer_check"].startswith("strict-model: checker tagged ok (")
        assert main["answer_check"].endswith(") → next; strict-model: passed (no tagger failed it)")
        assert main["answer_tags"] == "ok"
        assert main["completion_tokens"] == 3 and main["first_token_ms"] is not None  # timed as it came
        assert [e["role"] for e in calls] == ["tagger: checker"]


async def test_a_failed_stream_never_reaches_the_app(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["bad ", "answer"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store)
        text = await stream(c, stack)
        assert "strict-model check: checker tagged bad: it says bad; trying mock-model" in text
        assert "bad " not in text.split("trying mock-model")[1] and "good " in text and "answer" in text
        main, calls = await entries(stack)
        discarded = next(e for e in calls if e["role"] == "failed answer check")
        assert discarded["completion_tokens"] == 2 and discarded["first_token_ms"] is not None
        assert main["target_model"] == "mock-model"


async def test_answers_that_call_tools_are_not_checked(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["bad ", "answer"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store)
        text = await stream(c, stack, tools=TOOLS)
        assert "Let me check." in text and '"tool_calls"' in text and "checking the answer" not in text
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat(tools=TOOLS))
        assert r.json()["choices"][0]["message"]["tool_calls"]
    assert "check_inputs" not in stack.mock and stack.mock["models"] == ["strict-model", "strict-model"]
    main, _ = await entries(stack, 2)
    assert main["answer_check"] is None


@pytest.mark.parametrize(
    "setup, note",
    [
        ({"check_status": 400}, "checker gave no tags (its request failed)"),
        ({"check_reply": "Hmm, hard to say."}, "checker gave no tags (unreadable reply: 'Hmm, hard to say.')"),
        ({"check_delay": 10}, "checker gave no tags (it took over 1 s)"),  # far over: a busy runner stalls
    ],
)
async def test_a_tagger_that_fails_lets_the_answer_through(make_stack, monkeypatch, setup, note):
    monkeypatch.setattr(taggers, "TIMEOUT_S", 1.0)
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["bad ", "answer"])
    stack.mock.update(setup)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "bad answer"
    main, _ = await entries(stack)
    assert main["answer_check"].startswith(f"strict-model: {note} (") and main["fallback"] is None
    assert main["answer_check"].endswith(" → next; strict-model: passed (no tagger failed it)")


async def test_a_tagger_that_gives_no_tags_can_fail_the_answer(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["fine ", "answer"])
    stack.mock["check_status"] = 400
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store, s0_no_reply="fail")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"
    main, _ = await entries(stack)
    assert main["fallback"] == "strict-model -> mock-model (check: checker gave no tags (its request failed))"


async def test_a_refusal_found_in_the_whole_answer_moves_on_unchecked(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_checked(c, stack.app.state.store, triggers=("errors", "refused"))
        text = await stream(c, stack, mock_refuse_models=["strict-model"])
        assert "strict-model refused; trying mock-model" in text and "can't" not in text
    assert "check_inputs" not in stack.mock
    main, calls = await entries(stack)
    assert main["fallback"] == "strict-model -> mock-model (refused)" and [e["role"] for e in calls] == ["refused"]


async def test_taggers_need_a_fallback(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        r = await add_checked(c, store, (), expect="error")
        assert r.status_code == 200 and "Taggers need at least one fallback model" in r.text
        assert f'<option value="{checker(store)}" selected>checker</option>' in r.text  # the chain is shown again
        r = await add_checked(c, store, expect="error", s0_l0_tags="worse")
        assert "There&#39;s no tag named worse" in r.text
        await add_checked(c, store)
        page = (await c.get("/admin/models")).text
        assert "tags answers" in page
        alias = next(a for a in store.list_aliases() if a.name == "smart")
        assert alias.checks and alias.tag_chain[0].tagger == checker(store)
        assert alias.tag_chain[0].lines[0].do == "fail"
        edit = (await c.get(f"/admin/aliases/{alias.id}")).text
        assert "Taggers, in order" in edit and 'name="s0_l0_tags" value="bad"' in edit
