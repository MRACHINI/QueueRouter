"""Answer rules: an alias's own checks of its answers by text or pattern (first match fails the answer), what its
check reads (text, tool calls, empty answers), and the refusal field some providers use."""

import asyncio
import json

import httpx
import pytest

from app.answer_rules import START_CHARS, Rule, RuleError, clean_rule, first_match, kinds_of, matches
from app.fallback import AnswerReader, answer_of, is_refusal, message_text
from tests.test_admin import OWNER, csrf_of, login
from tests.test_answer_check import TOOLS, chat, entries, words


def rule(kind="contains", find="I can't help", scope="start", case=False, enabled=True, name="r", rule_id=1, **more):
    return Rule(rule_id, name, kind, find, case, scope, more.pop("state", "on" if enabled else "off"), **more)


def test_what_a_rule_matches():
    assert matches(rule(), "Sorry. I can’t HELP with that")  # any case, and ’ as '
    assert not matches(rule(case=True), "Sorry. I can’t HELP with that")
    assert matches(rule("starts", "i'm sorry"), "  I'm sorry, but no.") and not matches(rule("starts", "no"), "I'm no")
    assert matches(rule("pattern", r"^(sorry|i'm sorry),? (but )?i (can't|cannot)"), "Sure.\nSorry, but I cannot")
    late = "x" * START_CHARS + " I can't help"
    assert not matches(rule(), late) and matches(rule(scope="anywhere"), late)  # at the start: its first part only
    assert not matches(rule("starts", "x", scope="anywhere"), "y" + late)  # starts with: always at the start
    assert not matches(rule("pattern", r"(a|aa)+$"), "a" * 40 + "b")  # a pattern that takes too long: no match
    rules = [rule(find="nope", rule_id=1), rule(find="can't", enabled=False, rule_id=2), rule(find="help", rule_id=3)]
    assert first_match(rules, "I can't help").id == 3  # in order, those on
    assert first_match(rules, "fine") is None


def test_a_rules_settings_are_checked():
    fields = clean_rule({"name": " Sorry,  I can't ", "kind": "pattern", "find": "^sorry", "scope": "anywhere"})
    assert fields == {"name": "Sorry, I can't", "kind": "pattern", "find": "^sorry", "case": False, "scope": "anywhere",
                      "action": "fail", "negate": False, "state": "on", "notes": ""}  # fmt: skip
    raw = {"name": "w", "kind": "any", "find": " sorry \r\n\r\nunable\n", "action": "ask", "negate": "on"}
    words_ = clean_rule(raw)
    assert words_["find"] == "sorry\nunable" and words_["action"] == "ask" and words_["negate"] is True
    with pytest.raises(RuleError):
        clean_rule({"name": "n", "kind": "contains", "find": "x", "action": "maybe"})
    for wrong in ({"kind": "pattern", "find": "(unclosed"}, {"kind": "glob"}, {"find": " "}, {"name": ""}):
        with pytest.raises(RuleError):
            clean_rule({"name": "n", "kind": "contains", "find": "x", "scope": "start", **wrong})
    assert kinds_of(["tools", "text", "bogus"]) == ("text", "tools") and kinds_of([]) == ("text", "empty")


def test_what_an_answer_is_read_as():
    body = {
        "choices": [
            {
                "message": {
                    "content": "Let me look.",
                    "tool_calls": [{"function": {"name": "lookup", "arguments": '{"q": "x"}'}}],
                },
                "finish_reason": "tool_calls",
            }
        ]
    }
    answer = answer_of(json.dumps(body).encode())
    assert answer.kind == "tools" and answer.shown() == 'Let me look.\n[tool call] lookup({"q": "x"})'
    assert message_text(json.dumps(body).encode()) == ("", "tool_calls")  # acting, not refusing
    refused = {"choices": [{"message": {"content": None, "refusal": "I can't assist with that."}}]}
    answer = answer_of(json.dumps(refused).encode())
    assert answer.kind == "text" and answer.shown() == "I can't assist with that." and is_refusal(answer)
    assert answer_of(b'{"choices": [{"message": {"content": ""}}]}').kind == "empty"

    def line(delta, finish=None):
        return f"data: {json.dumps({'choices': [{'delta': delta, 'finish_reason': finish}]})}\n\n".encode()

    reader = AnswerReader()
    stream = [
        line({"content": "Ok. "}),
        line({"tool_calls": [{"index": 0, "function": {"name": "look", "arguments": '{"q"'}}]}),
        line({"tool_calls": [{"index": 1, "function": {"name": "read", "arguments": "{}"}}]}),
        line({"tool_calls": [{"index": 0, "function": {"name": "up", "arguments": ': "x"}'}}]}, "tool_calls"),
        b"data: [DONE]\n\n",
    ]
    for piece in stream:
        reader.feed(piece[:7])  # split anywhere: lines are put together first
        reader.feed(piece[7:])
    answer = reader.answer
    assert answer.calls == [["lookup", '{"q": "x"}'], ["read", "{}"]] and answer.finish == "tool_calls"
    assert reader.size == len("Ok. ") + len("lookup") + len('{"q": "x"}') + len("read{}")
    reader = AnswerReader()
    reader.feed(line({"refusal": "I can't "}) + line({"refusal": "do that."}, "stop"))
    assert reader.answer.refusal == "I can't do that." and is_refusal(reader.answer)


async def new_rule(c, store, name, find, kind="contains", scope="start", **extra):
    csrf = csrf_of((await c.get("/admin/rules")).text)
    form = {"csrf": csrf, "name": name, "kind": kind, "find": find, "scope": scope, **extra}
    r = await c.post("/admin/rules", data=form)
    assert "m=created" in r.headers["location"], r.text[:300]
    return next(x.id for x in store.list_rules() if x.name == name)


async def save_alias(c, rules=(), kinds=("text", "empty"), fallbacks="mock-model", alias_id=None, **extra):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "name": "smart", "target_model": "strict-model", "fallbacks": fallbacks, "on_errors": "on"}
    form |= {"answer_rules": ",".join(str(i) for i in rules), "kinds": "shown", **{f"kind_{k}": "on" for k in kinds}}
    form |= extra
    if alias_id is None:
        return await c.post("/admin/aliases", data=form)
    return await c.post(f"/admin/aliases/{alias_id}", data={**form, "enabled": "on"})


async def test_a_rule_fails_an_answer_and_the_next_model_answers(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["I can't help ", "with that."])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        sorry = await new_rule(c, store, "Sorry", "I can't help")
        assert "m=created" in (await save_alias(c, [sorry])).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "good answer"
        assert stack.mock["models"] == ["strict-model", "mock-model"]  # no checker model was asked
        main, calls = await entries(stack)
        assert main["answer_check"] == "strict-model: rule 'Sorry' failed it"
        assert main["fallback"] == "strict-model -> mock-model (rule: Sorry)"
        assert {e["role"] for e in calls} == {"failed answer rule"}
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=chat(stream=True)) as r:
            text = "".join([chunk async for chunk in r.aiter_text()])
        assert '"good "' in text and "I can't" not in text
        for _ in range(60):  # the Rules page counts it
            if next(x for x in store.list_rules() if x.id == sorry).hits >= 2:
                break
            await asyncio.sleep(0.1)
        assert next(x for x in store.list_rules() if x.id == sorry).hits == 2
        page = (await c.get("/admin/rules")).text
        assert "Sorry" in page and "smart" in page


async def test_a_rule_at_the_start_reads_only_the_start(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    long = "x" * (START_CHARS + 50) + " "
    words(stack, [long, "As an AI, ", "no."])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        ai = await new_rule(c, store, "As an AI", "As an AI")
        await save_alias(c, [ai])
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=chat(stream=True)) as r:
            text = "".join([chunk async for chunk in r.aiter_text()])
        assert "As an AI" in text and stack.mock["models"] == ["strict-model"]  # past its start: passed, streamed on
        csrf = csrf_of((await c.get(f"/admin/rules/{ai}")).text)
        form = {"csrf": csrf, "name": "As an AI", "kind": "contains", "find": "As an AI", "scope": "anywhere"}
        assert "m=saved" in (await c.post(f"/admin/rules/{ai}", data={**form, "enabled": "on"})).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"  # anywhere: the whole answer is read


async def test_answers_that_call_tools_are_read_only_when_ticked(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        lookup = await new_rule(c, store, "No lookups", "[tool call] lookup(")
        await save_alias(c, [lookup])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat(tools=TOOLS))
        assert r.json()["choices"][0]["finish_reason"] == "tool_calls" and stack.mock["models"] == ["strict-model"]
        alias_id = next(a.id for a in store.list_aliases() if a.name == "smart")
        assert "m=saved" in (await save_alias(c, [lookup], ("text", "tools"), alias_id=alias_id)).headers["location"]
        stack.mock["models"].clear()
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat(tools=TOOLS, stream=True))
        assert r.status_code == 200 and stack.mock["models"] == ["strict-model", "mock-model"]  # read, failed
        main, _ = await entries(stack, 2)
        assert main["fallback"] == "strict-model -> mock-model (rule: No lookups)"


async def test_the_editor_checks_rules_and_kinds(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        sorry = await new_rule(c, store, "Sorry", "sorry")
        r = await save_alias(c, [sorry], fallbacks="")
        assert "Answer rules need at least one fallback model" in r.text
        r = await save_alias(c, [sorry], kinds=())
        assert "Tick at least one kind of answer" in r.text
        csrf = csrf_of((await c.get("/admin/rules")).text)
        bad = {"csrf": csrf, "name": "Bad", "kind": "pattern", "find": "(unclosed", "scope": "start"}
        assert "The pattern isn&#39;t valid" in (await c.post("/admin/rules", data=bad)).text
        r = await c.post("/admin/rules", data={**bad, "name": "Sorry", "kind": "contains"})
        assert "That name is already taken." in r.text
        assert "m=created" in (await save_alias(c, [sorry])).headers["location"]
        csrf = csrf_of((await c.get("/admin/rules")).text)
        tried = await c.post("/admin/rules/try", data={"csrf": csrf, "answer": "Sorry, no.", "alias": "smart"})
        assert "Decided by <strong>Sorry</strong>: it fails." in tried.text
        r = await c.post(f"/admin/rules/{sorry}/delete", data={"csrf": csrf})
        assert "m=deleted" in r.headers["location"]
        assert next(a for a in store.list_aliases() if a.name == "smart").answer_rules == ()  # taken off the alias
