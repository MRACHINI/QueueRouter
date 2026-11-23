"""The live test of a model's thinking levels (app/levelcheck.py): the requests it plans for each way of asking,
what it reads from the answers, what it concludes, and the admin page that runs it through the pipeline as it is."""

import json

import httpx

from app import levelcheck, thinking
from tests.test_admin import OWNER, csrf_of, login
from tests.test_usage import flushed


def answered(probe, thinking_chars=0, tokens=None):
    message = {"content": "23", "reasoning_content": "x" * thinking_chars}
    usage = {"completion_tokens": 5 + thinking_chars}
    if tokens is not None:
        usage["completion_tokens_details"] = {"reasoning_tokens": tokens}
    return levelcheck.read(probe, 200, json.dumps({"choices": [{"message": message}], "usage": usage}).encode())


def refused(probe, message="reasoning_effort must be one of none, low, high"):
    return levelcheck.read(probe, 400, json.dumps({"error": {"message": message}}).encode())


def test_the_requests_of_a_test():
    known = levelcheck.plan("m", "openai")
    assert [p.level for p in known] == ["", *thinking.EFFORTS] and known[0].sent == "nothing"
    assert known[1].sent == '{"reasoning_effort": "none"}' and known[1].body["max_tokens"] == levelcheck.MAX_TOKENS
    router = levelcheck.plan("m", "openrouter")
    assert router[2].body["reasoning"] == {"effort": "minimal"} and "reasoning_effort" not in router[2].body
    switch = levelcheck.plan("m", "vllm")
    assert [p.level for p in switch] == ["", "none", "low", "medium", "high"]
    assert switch[1].body["chat_template_kwargs"] == {"enable_thinking": False, "thinking": False}
    assert [p.level for p in levelcheck.plan("m", "synthetic")] == ["", "none", "low", "medium", "high"]
    # an API QueueRouter doesn't know: every level as reasoning_effort, off and high the two other ways
    guessed = levelcheck.plan("m", "unknown")
    assert [(p.way, p.level) for p in guessed][-4:] == [
        ("openrouter", "none"),
        ("openrouter", "high"),
        ("vllm", "none"),
        ("vllm", "high"),
    ]
    assert len(guessed) == 1 + 7 + 4
    assert levelcheck.plan("m", "claude") == [] and levelcheck.plan("m", "none") == []


def test_what_a_test_finds():
    plain, *levels = levelcheck.plan("m", "openai")
    takes = {"none": 0, "low": 40, "high": 400}
    results = [answered(plain, 400)] + [answered(p, takes[p.level]) if p.level in takes else refused(p) for p in levels]
    assert results[2].refused and results[2].error == "reasoning_effort must be one of none, low, high"
    found = levelcheck.verdict(results, "openai")
    assert found["levels"] == "none, low, high" and found["way"] == ""
    assert found["ways"][0]["refused"] == ["minimal", "medium", "xhigh", "max"]
    assert any('"none" turns its thinking off' in note for note in found["notes"])

    # off accepted but ignored: left out; thinking counted in tokens only
    ignored = [answered(plain, 0, 300)] + [answered(p, 0, 300) for p in levels]
    found = levelcheck.verdict(ignored, "openai")
    assert found["levels"] == "minimal, low, medium, high, xhigh, max"
    assert any("still thought" in note for note in found["notes"]) and any("Every level" in n for n in found["notes"])

    # nothing thinks: nothing to save for a known API either way, and it says why
    quiet = levelcheck.verdict([answered(plain)] + [answered(p) for p in levels], "openai")
    assert any("No thinking came back" in note for note in quiet["notes"])

    # an unknown API that honours OpenRouter's form only
    plain, *tries = levelcheck.plan("m", "unknown")
    seen = [answered(plain, 200)]
    for p in tries:
        seen.append(answered(p, 0 if (p.way, p.level) == ("openrouter", "none") else 200))
    found = levelcheck.verdict(seen, "unknown")
    assert found["way"] == "openrouter" and found["levels"] == "none, high"
    assert any("test again for every level" in note for note in found["notes"])
    # one where no way changes anything
    same = levelcheck.verdict([answered(plain, 200)] + [answered(p, 200) for p in tries], "unknown")
    assert same["way"] == "" and same["levels"] == "" and any("looks ignored" in note for note in same["notes"])
    # one that doesn't answer at all
    down = levelcheck.verdict([levelcheck.read(plain, 503, b"busy")] + [answered(p, 9) for p in tries], "unknown")
    assert down["levels"] == "" and "didn't answer a plain request (503: busy)" in down["notes"][0]


async def test_the_test_through_the_admin_page(make_stack):
    stack = make_stack(models_cache_s=0, bootstrap=OWNER)
    stack.mock["extra_model_entries"] = [{"id": "ponder", "object": "model"}]
    stack.mock["thinkers"] = {"ponder": {"way": "openrouter", "levels": ["none", "low", "high"]}}
    stack.mock["bodies"] = []
    store = stack.app.state.store
    upstream = store.list_upstreams()[0]
    async with httpx.AsyncClient(base_url=stack.url, timeout=60) as c:
        await login(c, stack.url)
        check = (await c.get(f"/admin/upstreams/{upstream.id}/check")).text
        assert f'action="/admin/upstreams/{upstream.id}/test-thinking"' in check
        csrf = csrf_of(check)

        # an API QueueRouter doesn't know: it finds the way the provider honours
        r = await c.post(f"/admin/upstreams/{upstream.id}/test-thinking", data={"csrf": csrf, "model": "ponder"})
        assert r.status_code == 200, r.text
        assert "honours levels sent as reasoning.effort" in r.text and 'value="none, high"' in r.text
        sent = [json.loads(b) for b in stack.mock["bodies"] if json.loads(b).get("model") == "ponder"]
        assert len(sent) == 12 and all(b["max_tokens"] == levelcheck.MAX_TOKENS for b in sent)
        assert any(b.get("reasoning_effort") == "max" for b in sent)  # sent as it is, not fitted

        # chosen that way: every level is tried as it is (not fitted to known levels), the refused ones named
        form = {"csrf": csrf, "weight": "1", "enabled": "on", "ua_mode": "keep", "thinking_style": "openrouter"}
        await c.post(f"/admin/upstreams/{upstream.id}", data=form)
        stack.mock["bodies"].clear()
        r = await c.post(f"/admin/upstreams/{upstream.id}/test-thinking", data={"csrf": csrf, "model": "ponder"})
        assert 'value="none, low, high"' in r.text and "Refused: minimal, medium, xhigh, max" in r.text
        tried = [json.loads(b).get("reasoning", {}).get("effort") for b in stack.mock["bodies"]]
        assert tried.count(None) == 1 and sorted(e for e in tried if e) == sorted(thinking.EFFORTS)

        # on the Requests page, as QueueRouter's own; saving the levels uses the Models page's form
        await flushed(stack, 20)
        mine = [row for row in store.list_requests(100) if row["role"] == "thinking test"]
        assert len(mine) == 20 and {row["client"] for row in mine} == {"model check"}
        r = await c.post(
            "/admin/model-thinking",
            data={"csrf": csrf, "upstream_id": str(upstream.id), "model": "ponder", "levels": "none, low, high"},
        )
        assert "m=saved" in r.headers["location"]
        assert store.list_model_thinking()[(upstream.id, "ponder")] == "none,low,high"

        # nothing to test, said plainly; a model it doesn't have
        r = await c.post(f"/admin/upstreams/{upstream.id}/test-thinking", data={"csrf": csrf, "model": "no-such"})
        assert r.status_code in (302, 303)
        form["thinking_style"] = "none"
        await c.post(f"/admin/upstreams/{upstream.id}", data=form)
        r = await c.post(f"/admin/upstreams/{upstream.id}/test-thinking", data={"csrf": csrf, "model": "ponder"})
        assert "Thinking levels aren&#39;t sent to this upstream" in r.text or "aren't sent to this upstream" in r.text
