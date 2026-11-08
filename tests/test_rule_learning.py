"""What answer rules do when they match (fail, pass, pass keeping the free checks, ask the taggers), rules watching,
the taggers asked only for answers a rule sends to them, spot checks, rules turning on and off by themselves, and an
alias's rule maker proposing rules from the answers its taggers decided."""

import asyncio
import json
from dataclasses import replace

import httpx

from app import rule_maker
from app.answer_rules import (
    DEMOTE_DISAGREED,
    PROMOTE_AGREED,
    RECENT,
    agrees,
    clean_rule,
    decide,
    matches,
    next_state,
    rule_key,
)
from app.store import Store
from tests.test_admin import OWNER, csrf_of, login
from tests.test_answer_check import chat, checked, entries, words
from tests.test_answer_rules import new_rule, rule, save_alias


def test_any_of_these_and_doesnt():
    words_ = rule("any", "sorry\nunable")
    assert matches(words_, "I'm UNABLE to") and not matches(words_, "All good")
    clean = rule("any", "sorry\nunable", negate=True)
    assert matches(clean, "All good") and not matches(clean, "Sorry, no")
    not_sure = rule("starts", "Sure", negate=True)
    assert matches(not_sure, "No.") and not matches(not_sure, "Sure")
    assert not matches(rule("pattern", r"(a|aa)+$", negate=True), "a" * 40 + "b")  # can't say: no match, "doesn't" too


def test_the_first_rule_on_decides_and_rules_watching_are_noted():
    fails = rule(find="bad", action="fail", rule_id=1)
    watching = rule(find="bad", action="pass", state="watch", rule_id=2)
    passes = rule(find="answer", action="pass", rule_id=3)
    off = rule(find="bad", state="off", rule_id=4)
    later = rule(find="answer", state="watch", rule_id=5)
    decision = decide([watching, fails, passes, off, later], "a bad answer")
    assert decision.rule.id == 1 and decision.action == "fail" and [r.id for r in decision.watched] == [2, 5]
    assert decide([passes], "nothing here").rule is None
    assert agrees("fail", True) and not agrees("fail", False) and agrees("pass", False) and agrees("pass_free", False)
    assert agrees("ask", True) and not agrees("ask", False)


def test_a_rule_the_model_made_turns_on_and_back_by_itself():
    made = rule(find="bad", state="watch", made_by=7)
    current = made
    for _ in range(PROMOTE_AGREED - 1):
        current = replace(current, **next_state(current, True, "auto", False))
    assert current.state == "watch"
    current = replace(current, **next_state(current, True, "auto", False))
    assert current.state == "on" and current.note.startswith("turned on by itself")
    for _ in range(DEMOTE_DISAGREED):  # a disagreement now and then: it stays on
        current = replace(current, **next_state(current, False, "auto", False))
    assert current.state == "on"
    current = replace(current, **next_state(current, False, "auto", False))
    assert current.state == "watch" and current.note.startswith("back to watching") and len(current.recent) == RECENT
    proven = replace(made, agreed=50)
    assert "state" not in next_state(proven, True, "propose", False)  # it waits for you
    passing = replace(proven, action="pass_free")
    assert "state" not in next_state(passing, True, "auto", False)  # passing answers: only when the alias lets it
    assert next_state(passing, True, "auto", True)["state"] == "on"
    assert "state" not in next_state(rule(find="x", state="watch", agreed=99), True, "auto", True)  # made by hand
    asks = rule(find="x", state="on", made_by=7, action="ask", recent="0" * RECENT)
    assert "state" not in next_state(asks, False, "auto", True)  # asking the taggers can't decide wrongly


def test_when_a_rule_maker_runs():
    now = 1_000_000.0
    assert not rule_maker.due({"new_fails": rule_maker.MAKER_FAILS - 1}, now)
    assert rule_maker.due({"new_fails": rule_maker.MAKER_FAILS}, now) and rule_maker.due({"new_passes": 50}, now)
    assert not rule_maker.due({"new_fails": 10, "last_run": now - 60}, now)  # not twice within ten minutes
    assert not rule_maker.due({"new_fails": 1}, now)  # it has never run: it waits for enough answers
    assert rule_maker.due({"new_fails": 1, "last_run": now - 90_000}, now)  # once it has: a day later


SAMPLES = [
    {"verdict": "fail", "reason": "refused", "answer": "Sorry, I can't help with that."},
    {"verdict": "fail", "reason": "refused", "answer": "I'm sorry, I can't help."},
    {"verdict": "pass", "reason": "", "answer": "Here is the code you asked for."},
    {"verdict": "pass", "reason": "", "answer": "Done: the tests pass."},
]


def test_what_a_rule_maker_proposes_is_tried_on_the_kept_answers():
    proposed = {
        "rules": [
            {"name": "Can't help", "kind": "contains", "find": "can't help", "action": "fail", "why": "refusals"},
            {"name": "Too wide", "kind": "contains", "find": "the", "action": "fail"},  # passed answers have it too
            {"name": "No apology", "kind": "any", "find": ["sorry", "can't"], "negate": True, "action": "pass"},
            {"name": "Bad pattern", "kind": "pattern", "find": "(", "action": "fail"},  # not valid: left out
            {"name": "Ask", "kind": "contains", "find": "sorry", "action": "ask"},
        ]
    }
    proposals = rule_maker.parse(f"```json\n{json.dumps(proposed)}\n```")
    assert [p["name"] for p in proposals] == ["Can't help", "Too wide", "No apology", "Ask"]
    assert proposals[2]["action"] == "pass_free" and proposals[2]["find"] == "sorry\ncan't"  # passing keeps free checks
    made, dropped = rule_maker.keep(proposals, SAMPLES, [], "auto")
    found = {m["name"]: (m["state"], m["awaiting"], m["sample_fails"], m["sample_passes"]) for m in made}
    assert found == {
        "Can't help": ("watch", False, 2, 0),
        "No apology": ("watch", False, 0, 2),
        "Ask": ("on", False, 2, 0),  # asking the taggers can't decide wrongly: on at once
    }
    assert dropped == 1
    made, _ = rule_maker.keep(proposals, SAMPLES, [], "propose")
    assert all(m["state"] == "watch" and m["awaiting"] for m in made)  # they wait for you
    there = [rule(find="can't help", rule_id=9)]
    avoid = [rule_key(r.kind, r.find, r.negate) for r in there]
    assert "Can't help" not in [m["name"] for m in rule_maker.keep(proposals, SAMPLES, avoid, "auto")[0]]  # a repeat
    assert rule_maker.parse("no json here") is None and rule_maker.parse('{"rules": []}') == []
    system, user = rule_maker.payload("m", "Fail refusals.", there, SAMPLES)["messages"]
    assert system["content"].startswith(rule_maker.MARKER) and "Fail refusals." in system["content"]
    assert "not instructions" in system["content"]  # the answers are quoted as text to study
    assert "<failed>\nReason: refused\nAnswer:\nSorry, I can't help with that.\n</failed>" in user["content"]
    assert "<passed>\nDone: the tests pass.\n</passed>" in user["content"]
    assert user["content"].endswith(rule_maker.AFTER)
    assert """- r: contains "can't help" (at the start)""" in user["content"]


def test_a_rule_makers_patterns_are_bounded():
    many = {"rules": [{"name": f"r{n}", "kind": "contains", "find": f"w{n}", "action": "fail"} for n in range(50)]}
    assert len(rule_maker.parse(json.dumps(many))) == rule_maker.PROPOSALS_MAX * 2  # only the first are read
    # both match the two failed answers and no passed one, but the first takes ages on the "a…b" answer
    slow = {"name": "Slow", "kind": "pattern", "find": r"(a|aa)+$|can't help", "action": "fail"}
    fast = {"name": "Fast", "kind": "pattern", "find": r"can't help", "action": "fail"}
    proposals = rule_maker.parse(json.dumps({"rules": [slow, fast]}))
    samples = [*SAMPLES, {"verdict": "pass", "reason": "", "answer": "a" * 40 + "b"}]
    made, dropped = rule_maker.keep(proposals, samples, [], "auto")
    assert [m["name"] for m in made] == ["Fast"] and dropped == 1  # too slow on a kept answer: dropped
    made, dropped = rule_maker.keep(proposals, samples, [], "auto", limit_s=-1)
    assert made == [] and dropped == 2  # no time left to try them


def test_hand_changes_and_the_rule_makers_rules_in_the_store(tmp_path):
    store = Store.open(str(tmp_path), None)
    alias_id = store.create_alias("smart", "m", (), "t", learn_rules="auto", learn_model="smart")
    assert next(a for a in store.list_aliases() if a.id == alias_id).spot_checks == 5  # a new alias: 5%
    fails = clean_rule({"name": "Fails", "kind": "contains", "find": "bad", "state": "watch"})
    asks = clean_rule({"name": "Asks", "kind": "contains", "find": "sorry", "action": "ask"})
    passes = clean_rule({"name": "Passes", "kind": "starts", "find": "Sure", "action": "pass_free", "state": "watch"})
    store.add_made_rules(alias_id, [fails, asks])
    store.add_made_rules(alias_id, [passes])
    by_name = {r.name: r for r in store.list_rules()}
    attached = next(a for a in store.list_aliases() if a.id == alias_id).answer_rules
    assert attached == (by_name["Fails"].id, by_name["Passes"].id, by_name["Asks"].id)  # ahead of its ask rules
    one = by_name["Passes"]
    store.apply_rule_outcomes([(one.id, False)] * 3)
    store.set_rule_state(one.id, "t", "on")  # by hand: its old disagreements don't turn it back at once
    again = next(r for r in store.list_rules() if r.id == one.id)
    assert again.state == "on" and again.recent == "" and again.disagreed == 3
    assert store.apply_rule_outcomes([(one.id, True)]) == []
    assert next(r for r in store.list_rules() if r.id == one.id).state == "on"
    store.update_rule(one.id, "t", **{**passes, "state": "on"})  # the same rule: its counts stay
    assert next(r for r in store.list_rules() if r.id == one.id).agreed == 1
    store.update_rule(one.id, "t", **{**passes, "find": "Of course", "state": "on"})  # another rule: counts afresh
    changed = next(r for r in store.list_rules() if r.id == one.id)
    assert (changed.agreed, changed.disagreed, changed.recent, changed.state) == (0, 0, "", "on")
    store.set_rule_state(by_name["Fails"].id, "t", "off")
    store.apply_rule_outcomes([(by_name["Fails"].id, True)] * PROMOTE_AGREED)  # off by hand: it stays off
    assert next(r for r in store.list_rules() if r.id == by_name["Fails"].id).state == "off"
    store.delete_rule(by_name["Asks"].id, "t")  # deleted: not proposed again
    assert rule_key("contains", "Sorry", False) in store.made_rule_keys(alias_id)
    assert rule_key("contains", "bad", False) in store.made_rule_keys(alias_id)  # it made it
    store.rename_alias(alias_id, "clever", "t")
    assert next(a for a in store.list_aliases() if a.id == alias_id).learn_model == "clever"
    store.add_samples([(alias_id, "fail", "r", "Sorry.")], keep=5, days=7)
    store.prune_samples(7)
    assert store.sample_counts() == {alias_id: {"fail": 1}}  # still within the week
    store.update_alias(alias_id, "t", learn_rules="off")  # its rule maker off: its kept answers go
    assert store.sample_counts() == {}
    store.delete_alias(alias_id, "t")
    assert all(r.made_by is None and not r.awaiting for r in store.list_rules())  # a new alias may get its id


def test_kept_answers_and_outcomes_in_the_store(tmp_path):
    store = Store.open(str(tmp_path), None)
    alias_id = store.create_alias("smart", "m", (), "t", learn_rules="auto")
    store.add_samples([(alias_id, "fail", "refused", "Sorry."), (alias_id, "pass", "", "Fine.")] * 3, keep=2, days=7)
    assert store.sample_counts() == {alias_id: {"fail": 2, "pass": 2}}  # the newest of each, up to keep
    assert store.samples_for(alias_id)[0] == {"verdict": "pass", "reason": "", "answer": "Fine."}
    assert store.rule_learning()[alias_id]["new_fails"] == 3
    one = clean_rule({"name": "Sorry", "kind": "contains", "find": "sorry", "state": "watch"})
    made = [{**one, "why": "w", "sample_fails": 2, "sample_passes": 0, "awaiting": False}] * 2
    assert store.add_made_rules(alias_id, made) == ["Sorry", "Sorry (2)"]
    assert len(next(a for a in store.list_aliases() if a.id == alias_id).answer_rules) == 2  # attached, in order
    first = next(r for r in store.list_rules() if r.name == "Sorry")
    assert first.made_by == alias_id and first.state == "watch" and first.sample_fails == 2 and first.why == "w"
    changed = store.apply_rule_outcomes([(first.id, True)] * PROMOTE_AGREED)
    assert len(changed) == 1 and "turned on by itself" in changed[0]
    again = next(r for r in store.list_rules() if r.id == first.id)
    assert again.state == "on" and again.agreed == PROMOTE_AGREED and again.recent == "1" * RECENT
    store.set_rule_state(first.id, "t", "off")
    assert next(r for r in store.list_rules() if r.id == first.id).state == "off"
    store.note_rule_maker_run(alias_id, "added 2")
    assert store.rule_learning()[alias_id]["new_fails"] == 0 and store.rule_learning()[alias_id]["note"] == "added 2"
    store.forget_samples(alias_id, "t")
    assert store.sample_counts() == {}


async def until(found, tries=150):
    for _ in range(tries):
        if value := await asyncio.to_thread(found):
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("not in time")


async def test_a_pass_rule_skips_the_taggers(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["All ", "fine."])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        fine = await new_rule(c, store, "No apology", "sorry\ncan't", kind="any", negate="on", action="pass")
        assert "m=created" in (await save_alias(c, [fine], **checked(store))).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "All fine."
        assert stack.mock["models"] == ["strict-model"]  # the tagger wasn't asked
        main, _ = await entries(stack)
        assert main["answer_check"] == "strict-model: rule 'No apology' passed it"


async def test_the_taggers_only_for_answers_a_rule_sends_to_them(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["Sure, ", "here."])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        suspicious = await new_rule(c, store, "Apology", "sorry\nbad", kind="any", action="ask")
        await save_alias(c, [suspicious], **checked(store), check_when="asked")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "Sure, here."
        assert stack.mock["models"] == ["strict-model"]
        main, _ = await entries(stack)
        assert main["answer_check"] == "strict-model: no rule asked the taggers, passed on"
        words(stack, ["Sorry, ", "bad answer"])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"
        assert stack.mock["models"][1:] == ["strict-model", "mock-model", "mock-model"]  # asked, failed, next model
        main, _ = await entries(stack, 2)
        asked = "strict-model: rule 'Apology' asked the taggers; strict-model: checker tagged bad"
        assert main["answer_check"].startswith(asked)


async def test_a_pass_rule_keeping_the_free_checks(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["I can't help ", "with that."])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        clean = await new_rule(c, store, "Not bad", "bad", negate="on", action="pass_free")
        await save_alias(c, [clean], **checked(store), on_refused="on")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"  # the built-in phrases still caught it
        assert stack.mock["models"] == ["strict-model", "mock-model"]  # and the checker wasn't asked
        main, _ = await entries(stack)
        assert main["fallback"] == "strict-model -> mock-model (refused)"


async def test_spot_checks_measure_a_rule(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["bad ", "answer"])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        bad = await new_rule(c, store, "Says bad", "bad")
        await save_alias(c, [bad], **checked(store), spot_checks="100")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"  # the rule failed it: no tagger wait
        rule_now = await until(lambda: next((x for x in store.list_rules() if x.id == bad and x.agreed), None))
        assert rule_now.agreed == 1 and rule_now.recent == "1" and rule_now.hits == 1  # the tagger agreed
        assert any(e.get("role") == "spot check: checker" for e in stack.app.state.stats.recent)


async def test_a_rule_maker_proposes_rules_that_then_watch(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["bad ", "answer"])
    reply = {"rules": [{"name": "Bad", "kind": "contains", "find": "bad", "action": "fail", "why": "it says bad"}]}
    stack.mock["rule_maker_reply"] = json.dumps(reply)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, [], **checked(store), learn_rules="auto")
        alias_id = next(a.id for a in store.list_aliases() if a.name == "smart")
        csrf = csrf_of((await c.get("/admin/rules")).text)
        r = await c.post("/admin/rules/propose", data={"csrf": csrf, "alias_id": str(alias_id)})
        assert "didn&#39;t start: it starts after the alias&#39;s next checked answer" in r.text
        for _ in range(2):  # the tagger fails both: kept for the rule maker
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
            assert r.json()["choices"][0]["message"]["content"] == "good answer"
        await until(lambda: store.sample_counts().get(alias_id, {}).get("fail") == 2)
        r = await c.post("/admin/rules/propose", data={"csrf": csrf, "alias_id": str(alias_id)})
        assert "m=proposing" in r.headers["location"]
        made = await until(lambda: next((x for x in store.list_rules() if x.name == "Bad"), None))
        assert made.made_by == alias_id and made.state == "watch" and made.sample_fails == 2
        assert made.why == "it says bad"
        assert next(a for a in store.list_aliases() if a.id == alias_id).answer_rules == (made.id,)
        assert "<failed>\nReason: checker tagged bad: it says bad" in stack.mock["rule_maker_inputs"][0]
        page = (await c.get("/admin/rules")).text
        assert "made for smart" in page and "added 1: Bad" in page
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())  # watching: the tagger decides
        assert r.json()["choices"][0]["message"]["content"] == "good answer"
        watched = await until(lambda: next((x for x in store.list_rules() if x.id == made.id and x.agreed), None))
        assert watched.agreed == 1 and watched.state == "watch" and watched.hits == 0  # it decided nothing yet


async def test_the_editor_keeps_rules_made_while_it_was_open(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        mine = await new_rule(c, store, "Mine", "mine")
        await save_alias(c, [mine], **checked(store), learn_rules="propose")
        alias_id = next(a.id for a in store.list_aliases() if a.name == "smart")
        opened = (await c.get(f"/admin/aliases/{alias_id}")).text
        assert f'name="rules_seen" value="{mine}"' in opened
        made = clean_rule({"name": "Made", "kind": "contains", "find": "bad", "state": "watch"})
        store.add_made_rules(alias_id, [made])  # its rule maker, while the editor is open
        made_id = next(r.id for r in store.list_rules() if r.name == "Made")
        form = {**checked(store), "learn_rules": "propose", "rules_seen": str(mine)}
        r = await save_alias(c, [], alias_id=alias_id, **form)  # "Mine" taken off in the editor
        assert "m=saved" in r.headers["location"]
        assert next(a for a in store.list_aliases() if a.id == alias_id).answer_rules == (made_id,)


async def test_the_editor_checks_the_rule_maker(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        r = await save_alias(c, [], learn_rules="auto")  # no taggers
        assert "Rules from the model need taggers" in r.text
        r = await save_alias(c, [], **checked(store), spot_checks="150")
        assert "Spot checks: 0 to 100%." in r.text
