"""An alias that flips its checker model's verdict: its PASS fails the answer (the next model answers), its FAIL
passes it; rules watching and the rule maker go by the flipped verdict."""

import httpx

from app import rule_maker
from app.answer_check import flipped
from tests.test_admin import OWNER, login
from tests.test_answer_check import chat, entries, words
from tests.test_answer_rules import new_rule, save_alias
from tests.test_rule_learning import CHECKED, SAMPLES, until


def test_a_flipped_verdict():
    assert flipped(True, "passed") == (False, "flipped: the checker passed it")
    assert flipped(True, "it answers") == (False, "flipped: the checker passed it (it answers)")
    assert flipped(False, "it says bad") == (True, "flipped: the checker failed it: it says bad")
    assert flipped(None, "the checker's request failed") == (None, "the checker's request failed")  # still none
    system = rule_maker.payload("m", "Pass answers that say bad.", [], SAMPLES, flip=True)["messages"][0]["content"]
    assert rule_maker.FLIPPED in system  # the rule maker is told, so its rules match the flipped verdicts
    assert rule_maker.FLIPPED not in rule_maker.payload("m", "x", [], SAMPLES)["messages"][0]["content"]


async def test_the_checkers_pass_fails_the_answer(make_stack):
    stack = make_stack(bootstrap=OWNER)
    words(stack, ["All ", "fine."])  # the checker passes it (no "bad")
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        assert "m=created" in (await save_alias(c, [], **CHECKED, check_flip="on")).headers["location"]
        alias = next(a for a in stack.app.state.store.list_aliases() if a.name == "smart")
        assert alias.check_flip
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "good answer"  # flipped: failed, the next model's
        main, _ = await entries(stack)
        assert "strict-model: failed (" in main["answer_check"]
        assert main["answer_check"].endswith("flipped: the checker passed it")
        assert main["fallback"] == "strict-model -> mock-model (check: flipped: the checker passed it)"
        page = (await c.get(f"/admin/aliases/{alias.id}")).text
        assert 'name="check_flip" checked' in page


async def test_the_checkers_fail_passes_the_answer(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    words(stack, ["bad ", "answer"])  # the checker fails it
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        bad = await new_rule(c, store, "Says bad", "bad", state="watch")  # watching: measured by the flipped verdict
        await save_alias(c, [bad], **CHECKED, check_flip="on")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat())
        assert r.json()["choices"][0]["message"]["content"] == "bad answer"  # flipped: passed
        assert stack.mock["models"] == ["strict-model", "mock-model"]  # the answer, then the checker: no fallback
        main, _ = await entries(stack)
        assert main["answer_check"].startswith("strict-model: passed (")
        assert main["answer_check"].endswith("flipped: the checker failed it: it says bad")
        watched = await until(lambda: next((x for x in store.list_rules() if x.id == bad and x.disagreed), None))
        assert watched.disagreed == 1 and watched.agreed == 0  # it would have failed an answer that passed
