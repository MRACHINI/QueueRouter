"""Taggers (app/taggers.py): reading a tagger's reply, what it's sent, an alias's decisions on the tags (pass, fail, go
on, jump), the answer types, the editors, Try it, and each alias's checker model becoming a tagger."""

import json

import httpx
import pytest

from app import taggers
from app.answer_rules import ANSWER_KINDS, DEFAULT_ANSWER_KINDS, clean_rule
from app.fallback import AnswerReader, answer_of
from app.store import CHECKERS_CONVERTED, Store
from app.taggers import Line, Step, Tag, Tagger, TaggerError
from tests.test_admin import OWNER, csrf_of, login
from tests.test_answer_check import TOOLS, chain_fields, chat, checker, entries, words
from tests.test_answer_rules import new_rule, save_alias

TAGS = {1: Tag(1, "tech", "about technology"), 2: Tag(2, "medical", "about health"), 3: Tag(3, "other")}
TOPIC = Tagger(10, "Topic", "m", "Tag the topic.", tags=(1, 2, 3))


def reply(content):
    return json.dumps({"choices": [{"message": {"role": "assistant", "content": content}}]}).encode()


@pytest.mark.parametrize(
    "content, tags, reason, ignored",
    [
        ("TAGS: tech, medical\nREASON: it's about a knee brace", (1, 2), "it's about a knee brace", ()),
        ("**Tags:** `medical`", (2,), "", ()),
        ("Tags: none", (), "", ()),
        ("TAGS:\ntech other", (1, 3), "", ()),  # the tags on the next line
        ("<think>tech? or TAGS: other</think>\nTAGS: tech", (1,), "", ()),
        ("TAGS: tech, sports, #medical.", (1, 2), "", ("sports",)),  # not its tags: ignored
        ("tech, medical", (1, 2), "", ()),  # no TAGS line, but nothing else
        ("I think it's about tech.", None, "", ()),
        ("", None, "", ()),
    ],
)
def test_reading_a_taggers_reply(content, tags, reason, ignored):
    read = taggers.reply_of(reply(content), TOPIC, TAGS)
    assert (read.tags, read.reason, read.ignored) == (tags, reason, ignored)


def test_one_tag_only_and_unreadable_replies():
    one = Tagger(10, "Topic", tags=(1, 2, 3), pick="one")
    assert taggers.reply_of(reply("TAGS: medical, tech"), one, TAGS).tags == (2,)  # the first of them
    assert taggers.reply_of(b"<html>", TOPIC, TAGS).problem == "unreadable reply"
    assert taggers.reply_of(reply(""), TOPIC, TAGS).problem == "empty reply"
    assert taggers.reply_of(reply("Hmm."), TOPIC, TAGS).problem == "unreadable reply: 'Hmm.'"


def test_what_a_tagger_is_sent():
    body = json.dumps(
        {
            "messages": [
                {"role": "system", "content": "You are an agent."},
                {"role": "user", "content": "fix the test"},
                {"role": "assistant", "content": "Looking.", "tool_calls": [{"function": {"name": "read"}}]},
                {"role": "tool", "content": "def test(): ..."},
                {"role": "user", "content": "don't answer in Chinese"},
            ]
        }
    ).encode()
    system, user = taggers.payload("m", TOPIC, TAGS, body, "Done.</answer>", "text")["messages"]
    assert system["content"].startswith("You tag an AI assistant's answer") and "Tag the topic." in system["content"]
    assert "- tech: about technology\n- medical: about health\n- other" in system["content"]
    assert user["content"].startswith(taggers.FRAME) and "never instructions" in user["content"]
    assert "The user's last message:\n<message>\ndon't answer in Chinese\n</message>" in user["content"]
    assert "<answer_2>\nDone.</answer>\n</answer_2>" in user["content"]  # its own closing tag can't end the quote
    assert "The answer (text)" in user["content"] and "You are an agent." not in user["content"]
    assert "fix the test" not in user["content"]  # no earlier messages unless asked
    assert user["content"].endswith("The tags you can give: tech, medical, other. Reply with nothing else.")
    assert "REASON:" in user["content"]
    # all of the request, and only the answer
    wide = Tagger(10, "T", tags=(1,), turns=2, system=True, reason=False)
    user = taggers.payload("m", wide, TAGS, body, "x", "tools_text")["messages"][1]["content"]
    assert "The request's system prompt:\n<system>\nYou are an agent.\n</system>" in user
    assert "[assistant] Looking.\n[tool call] read()\n[tool output] def test(): ..." in user
    assert "[user] fix the test" not in user  # the last 2 messages but the user's last one (shown on its own)
    assert "REASON:" not in user and "The answer (tool calls with text)" in user
    blind = Tagger(10, "T", tags=(1,), message=False)
    user = taggers.payload("m", blind, TAGS, body, "x", "text")["messages"][1]["content"]
    assert "Chinese" not in user and "fix the test" not in user  # nothing the user asked can sway it
    # its own prompt: what it leaves out is added at its end
    own = Tagger(10, "T", instructions="Be strict.", tags=(1,), prompt="Label answers. {tags}")
    system = taggers.payload("m", own, TAGS, body, "x", "text")["messages"][0]["content"]
    assert system.startswith("Label answers. - tech: about technology") and system.endswith("Be strict.")


def test_an_aliass_decisions():
    step = Step(10, (Line("any", (2,), "fail"), Line("all", (1, 3), "pass"), Line("none", (1,), "goto", 11)))
    assert taggers.move(step, [2, 1])[0] == "fail"  # the first that holds
    assert taggers.move(step, [1, 3])[:2] == ("pass", None)
    assert taggers.move(step, [3])[:2] == ("goto", 11)  # not tagged tech
    assert taggers.move(step, [1]) == ("next", None, None)  # otherwise
    assert taggers.line_words(step.lines[0], TAGS) == "tagged medical"
    assert taggers.line_words(step.lines[1], TAGS) == "tagged all of tech, other"
    assert taggers.line_words(step.lines[2], TAGS) == "not tagged tech"
    chain = (step, Step(11, (Line("any", (1, 2), "goto", 10),)))
    assert taggers.chain_of(taggers.chain_json(chain)) == chain
    assert taggers.without(chain, tag=2) == (
        Step(10, (Line("all", (1, 3), "pass"), Line("none", (1,), "goto", 11))),
        Step(11, (Line("any", (1,), "goto", 10),)),
    )
    assert taggers.without(chain, tagger=11) == (
        Step(10, (Line("any", (2,), "fail"), Line("all", (1, 3), "pass"), Line("none", (1,), "next"))),
    )
    assert "otherwise: go on" in taggers.describe(chain, {10: TOPIC}, TAGS)


def test_the_chain_editor_is_checked():
    tagger_list = {10: TOPIC, 11: Tagger(11, "Work", tags=(1,))}
    rows = [
        {"slot": 0, "tagger": "11", "order": "2", "lines": [{"when": "any", "tags": "tech", "do": "pass"}]},
        {"slot": 1, "tagger": "10", "order": "1", "lines": [{"when": "all", "tags": " Medical,  ", "do": "goto:11"}]},
        {"slot": 2, "tagger": "", "order": "3", "lines": []},
    ]
    chain = taggers.clean_chain(rows, tagger_list, TAGS)
    assert [s.tagger for s in chain] == [10, 11]  # by their order; an empty step is left out
    assert chain[0].lines == (Line("all", (2,), "goto", 11),)
    assert taggers.rows_of(chain, TAGS)[0]["lines"][0] == {"when": "all", "tags": "medical", "do": "goto:11"}
    for wrong, said in (
        ({"tags": "sports"}, "no tag named sports"),
        ({"do": "goto:10"}, "jumps to Topic, which doesn't come after it"),
    ):
        broken = [{**rows[0], "lines": [{**rows[0]["lines"][0], **wrong}]}, rows[1]]
        with pytest.raises(TaggerError, match=said):
            taggers.clean_chain(broken, tagger_list, TAGS)
    with pytest.raises(TaggerError, match="twice"):
        taggers.clean_chain([rows[0], {**rows[1], "tagger": "11"}], tagger_list, TAGS)
    form = {"chain_slots": "1", "s0_tagger": "10", "s0_lines": "1", "s0_l0_tags": "tech", "s0_l0_do": "fail"}
    assert taggers.chain_rows(form)[0]["lines"] == [{"when": "any", "tags": "tech", "do": "fail"}]
    with pytest.raises(TaggerError, match="Tick the tags"):
        taggers.clean_tagger({"name": "x", "tags": ["99"], "reads": ["text"]}, [1])
    with pytest.raises(TaggerError, match="answer type"):
        taggers.clean_tagger({"name": "x", "tags": ["1"]}, [1])
    with pytest.raises(TaggerError, match="lower-case"):
        taggers.clean_tag({"name": "Not OK"})


def test_answer_types():
    def of(message, finish="stop"):
        return answer_of(json.dumps({"choices": [{"message": message, "finish_reason": finish}]}).encode()).kind

    call = [{"function": {"name": "f", "arguments": "{}"}}]
    assert of({"content": "hi"}) == "text"
    assert of({"content": "hi"}, "length") == "cut"
    assert of({"content": ""}, "content_filter") == "filtered"
    assert of({"content": None, "refusal": "No."}) == "refusal"
    assert of({"content": "Ok.", "tool_calls": call}) == "tools_text"
    assert of({"content": None, "tool_calls": call}) == "tools_only"
    assert of({"content": "<think>hmm</think>", "tool_calls": call}) == "tools_only"  # its thinking isn't text
    assert of({"content": "", "reasoning_content": "hmm"}) == "thinking"
    assert of({"content": "<think>hmm</think>"}) == "thinking"
    assert of({"content": ""}) == "nothing"
    assert of({"content": None, "audio": {"id": "a", "transcript": "hi"}}) == "media"
    reader = AnswerReader()
    reader.feed(b'data: {"choices": [{"delta": {"reasoning": "let me see"}}]}\n\n')
    reader.feed(b'data: {"choices": [{"delta": {"content": ""}, "finish_reason": "stop"}]}\n\n')
    assert reader.answer.kind == "thinking"
    assert set(DEFAULT_ANSWER_KINDS) == set(ANSWER_KINDS) - {"tools_text", "tools_only"}


def test_tags_and_taggers_in_the_store(tmp_path):
    store = Store.open(str(tmp_path), None)
    tech, medical = store.create_tag("t", "tech", "about technology"), store.create_tag("t", "medical")
    raw = {"name": "Topic", "model": "plain", "tags": [str(tech), str(medical)], "reads": ["text"], "turns": "2"}
    topic = store.create_tagger("t", **taggers.clean_tagger(raw, [tech, medical]))
    saved = next(t for t in store.list_taggers() if t.id == topic)
    assert saved.tags == (tech, medical) and saved.reads == ("text",) and saved.turns == 2 and not saved.message
    chain = (Step(topic, (Line("any", (medical,), "fail"), Line("all", (tech, medical), "pass"))),)
    alias = store.create_alias("smart", "m", (), "t", fallbacks=("x",), tag_chain=chain)
    assert next(a for a in store.list_aliases() if a.id == alias).tag_chain == chain
    assert store.tagger_users() == {topic: ["smart"]}
    rule = {"name": "Knee", "kind": "contains", "find": "knee", "action": "tag", "tag": str(medical)}
    store.create_rule("t", **clean_rule(rule, [tech, medical]))
    with pytest.raises(ValueError, match="tag_in_use"):
        store.delete_tag(medical, "t")  # a rule gives it
    assert next(r for r in store.list_rules() if r.name == "Knee").tag == medical
    store.delete_rule(next(r.id for r in store.list_rules() if r.name == "Knee"), "t")
    store.delete_tag(medical, "t")  # from the tagger, and from the alias's decisions
    assert next(t for t in store.list_taggers() if t.id == topic).tags == (tech,)
    assert next(a for a in store.list_aliases() if a.id == alias).tag_chain == (
        Step(topic, (Line("all", (tech,), "pass"),)),
    )
    store.add_tag_hits({tech: (3, 100.0)})
    assert next(t for t in store.list_tags() if t.id == tech).given == 3
    store.delete_tagger(topic, "t")
    assert next(a for a in store.list_aliases() if a.id == alias).tag_chain == ()


def test_each_checker_becomes_a_tagger(tmp_path):
    store = Store.open(str(tmp_path), None)
    smart = store.create_alias("smart", "m", (), "t", fallbacks=("x",))
    flipped = store.create_alias("flipped", "m", (), "t", fallbacks=("x",))
    blank = store.create_alias("blank", "m", (), "t")
    with store.tx() as db:  # as 0.76 left them
        db.execute(
            "UPDATE aliases SET check_answers = 1, check_model = 'judge', check_instructions = 'Fail refusals.', "
            'check_kinds = \'["text", "tools"]\' WHERE id = ?',
            (smart,),
        )
        db.execute(
            "UPDATE aliases SET check_answers = 1, check_instructions = 'Pass refusals.', check_flip = 1, "
            'check_kinds = \'["text", "empty"]\' WHERE id = ?',
            (flipped,),
        )
        db.execute("UPDATE aliases SET check_answers = 1, check_instructions = ' ' WHERE id = ?", (blank,))
        db.execute("DELETE FROM meta WHERE key = ?", (CHECKERS_CONVERTED,))
    store._convert_checkers()
    tags = {t.name: t.id for t in store.list_tags()}
    made = {t.name: t for t in store.list_taggers()}
    assert set(made) == {"smart checker", "flipped checker"}  # an alias with no instructions had no checker
    judge = made["smart checker"]
    assert (judge.model, judge.instructions, judge.pick) == ("judge", "Fail refusals.", "one")
    assert judge.tags == (tags["ok"], tags["bad"]) and judge.reads == ANSWER_KINDS
    aliases = {a.name: a for a in store.list_aliases()}
    assert aliases["smart"].tag_chain == (Step(judge.id, (Line("any", (tags["bad"],), "fail"),)),)
    assert aliases["flipped"].tag_chain[0].lines == (Line("any", (tags["ok"],), "fail"),)  # its verdict flipped
    assert aliases["smart"].check_kinds == ("text", "tools_text", "tools_only", "refusal", "cut", "filtered")
    assert aliases["flipped"].check_kinds == DEFAULT_ANSWER_KINDS
    assert aliases["blank"].tag_chain == ()
    store._convert_checkers()  # once
    assert len(store.list_taggers()) == 2


async def test_two_taggers_decide_in_turn(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["My knee ", "hurts? Rest it."])
    stack.mock["tag_words"] = {"knee": "medical", "Let me check": "discussion", "lookup": "other"}
    ids = {name: store.create_tag("t", name) for name in ("medical", "tech", "discussion", "doing-work", "other")}

    def tagger(name, tags, reads):
        raw = {"name": name, "model": "mock-model", "tags": [ids[t] for t in tags], "reads": reads, "message": "on"}
        return store.create_tagger("t", **taggers.clean_tagger(raw, ids.values()))

    work = tagger("Work", ("doing-work", "discussion"), ["tools_text", "tools_only"])
    strict = tagger("Strict", ("other",), list(ANSWER_KINDS))
    topic = tagger("Topic", ("medical", "tech"), ["text", "tools_text"])
    form = {"chain_slots": "3"}
    form |= {"s0_tagger": str(work), "s0_order": "1", "s0_lines": "1", "s0_l0_tags": "discussion"}
    form |= {"s0_l0_do": f"goto:{topic}", "s0_otherwise": "next", "s0_no_reply": "next"}
    form |= {"s1_tagger": str(strict), "s1_order": "2", "s1_lines": "1", "s1_l0_tags": "other", "s1_l0_do": "fail"}
    form |= {"s2_tagger": str(topic), "s2_order": "3", "s2_lines": "1", "s2_l0_tags": "medical", "s2_l0_do": "fail"}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        kinds = ("text", "tools_text", "tools_only")
        assert "m=created" in (await save_alias(c, [], kinds, **form)).headers["location"]
        # a text answer: Work doesn't read it, Strict tags nothing (no "lookup"), Topic tags it medical: it fails
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"
        main, _ = await entries(stack)
        assert main["fallback"] == "strict-model -> mock-model (check: Topic tagged medical: mock)"
        assert main["answer_tags"] == "medical" and len(stack.mock["check_inputs"]) == 2
        # a tool call with text: Work tags it discussion and jumps past Strict to Topic, which tags nothing
        stack.mock["check_inputs"].clear()
        text = await _stream(c, stack, tools=TOOLS)
        assert '"tool_calls"' in text and "trying" not in text
        inputs = stack.mock["check_inputs"]
        offered = [b["messages"][1]["content"].rsplit("The tags you can give: ", 1)[1] for b in inputs]
        assert [o.split(".")[0] for o in offered] == ["doing-work, discussion", "medical, tech"]  # not Strict's
        main, calls = await entries(stack, 2)
        assert "Work tagged discussion (" in main["answer_check"] and "→ on to Topic" in main["answer_check"]
        assert main["answer_check"].endswith("passed (no tagger failed it)") and main["answer_tags"] == "discussion"
        assert {e["role"] for e in calls} == {"tagger: Work", "tagger: Topic"}


async def _stream(c, stack, **extra):
    async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=chat(stream=True, **extra)) as r:
        assert r.status_code == 200
        return "".join([chunk async for chunk in r.aiter_text()])


async def test_a_rules_tag_and_the_decisions_on_it(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["My knee ", "hurts."])
    stack.mock["check_fail"] = []  # the tagger says ok
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        tagger = checker(store)
        medical = store.create_tag("t", "medical")
        stack.app.state.reload()
        knee = await new_rule(c, store, "Knee", "knee", action="tag", tag=str(medical))
        r = await save_alias(c, [knee], **chain_fields(tagger, fail="medical"))
        assert "m=created" in r.headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"  # failed on the rule's tag
        main, _ = await entries(stack)
        assert main["fallback"] == "strict-model -> mock-model (check: checker tagged medical)"
        assert main["answer_tags"] == "medical,ok"
        csrf = csrf_of((await c.get("/admin/rules")).text)
        tried = await c.post("/admin/rules/try", data={"csrf": csrf, "answer": "My knee", "alias": "smart"})
        assert "Tags it gets: <code>medical</code> (from Knee)" in tried.text
        r = await c.post("/admin/rules", data={"csrf": csrf, "name": "W", "kind": "contains", "find": "x",
                                               "action": "tag", "tag": str(medical), "state": "watch"})  # fmt: skip
        assert "A rule that tags is on or off" in r.text
        r = await c.post(f"/admin/rules/{knee}/state", data={"csrf": csrf, "state": "watch"})
        assert "e=invalid" in r.headers["location"]  # a rule that tags doesn't watch
        assert next(x for x in store.list_rules() if x.id == knee).state == "on"


async def test_the_taggers_pages(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/taggers")).text
        assert "No tags yet" in page and "Add tagger" not in page  # a tag first
        csrf = csrf_of(page)
        assert "m=created" in (await c.post("/admin/tags", data={"csrf": csrf, "name": "tech"})).headers["location"]
        r = await c.post("/admin/tags", data={"csrf": csrf, "name": "tech"})
        assert "That name is already taken." in r.text
        assert "lower-case letters" in (await c.post("/admin/tags", data={"csrf": csrf, "name": "Tech!"})).text
        tech = next(t.id for t in store.list_tags() if t.name == "tech")
        form = {"csrf": csrf, "name": "Topic", "model": "mock-model", "tags": str(tech), "reads": "text"}
        form |= {"pick": "one", "message": "on", "reason": "on", "instructions": "Tag tech."}
        assert "m=created" in (await c.post("/admin/taggers", data=form)).headers["location"]
        topic = next(t for t in store.list_taggers() if t.name == "Topic")
        assert topic.tags == (tech,) and topic.reads == ("text",) and topic.prompt == ""
        page = (await c.get("/admin/taggers")).text
        assert "Topic" in page and "no alias yet" in page and "taggers: Topic" in page
        edit = (await c.get(f"/admin/taggers/{topic.id}")).text
        assert "Try it" in edit and "you say which of the tags below apply to it" in edit
        client = store.list_clients()[0].name
        tried = {"csrf": csrf, "try_client": client, "try_message": "Is Python fast?", "try_answer": "It's fine."}
        r = await c.post(f"/admin/taggers/{topic.id}/try", data={**tried, "try_kind": "text"})
        assert "Tagged <strong>ok</strong>" not in r.text  # not its tag: ignored
        assert "Not its tags, ignored: ok." in r.text and "Is Python fast?" in r.text  # what it was sent
        r = await c.post(f"/admin/tags/{tech}/delete", data={"csrf": csrf})
        assert "m=deleted" in r.headers["location"]
        assert next(t for t in store.list_taggers() if t.id == topic.id).tags == ()
