"""Parameter rules and the context-length pre-check."""

import json

import httpx
import pytest

from app.params import apply_rules, estimate_prompt_tokens, parse_rules, too_long
from tests.test_admin import OWNER, csrf_of, login
from tests.test_policies import set_policy


@pytest.mark.parametrize(
    "text, message",
    [
        ("{not json", "not valid JSON"),
        ("[]", "JSON object"),
        ('{"default": {}}', "unknown keys"),
        ('{"clamps": {"max_tokens": 5}}', "clamps"),
        ('{"strip": "x"}', "strip"),
        ('{"overrides": {"model": "x"}}', "can't be changed"),
    ],
)
def test_parse_rules_rejects(text, message):
    with pytest.raises(ValueError, match=message):
        parse_rules(text)


def test_apply_rules_order():
    payload = {"model": "m", "temperature": 1.5, "max_tokens": 50_000, "logit_bias": {"1": 2}}
    alias = {"defaults": {"top_p": 0.8, "seed": 1}, "overrides": {"temperature": 0.7}}
    client = {
        "defaults": {"top_p": 0.5, "presence_penalty": 0.1},  # alias default already set top_p
        "overrides": {"temperature": 0.2},  # client override applies after the alias's
        "clamps": {"max_tokens": {"max": 8192}},
        "strip": ["logit_bias"],
    }
    changes = apply_rules(payload, alias, client)
    assert payload == {
        "model": "m",
        "temperature": 0.2,
        "max_tokens": 8192,
        "top_p": 0.8,
        "seed": 1,
        "presence_penalty": 0.1,
    }
    assert "clamp max_tokens" in changes and "strip logit_bias" in changes


def test_context_check():
    payload = {"messages": [{"role": "user", "content": "x" * 400_000}], "max_tokens": 1000}
    assert estimate_prompt_tokens(payload) == 100_000
    assert too_long(payload, 262_144) is None
    assert too_long(payload, 32_768) == (100_000, 32_768)
    assert too_long(payload, None) is None  # unknown context: never refuse
    assert too_long({"messages": [{"role": "user", "content": "x" * 140_000}]}, 32_768) is None  # within margin


async def test_rules_reach_the_upstream(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        rules = json.dumps({"overrides": {"temperature": 0.3}, "strip": ["logit_bias"]})
        r = await c.post(
            "/admin/aliases", data={"csrf": csrf, "name": "tuned", "target_model": "mock-model", "params": rules}
        )
        assert "m=created" in r.headers["location"]
        bad = await c.post(
            "/admin/aliases",
            data={"csrf": csrf, "name": "bad", "target_model": "x", "params": '{"overrides": {"model": "y"}}'},
        )
        assert "e=rules_invalid" in bad.headers["location"]
        await set_policy(c, stack, params=json.dumps({"clamps": {"max_tokens": {"max": 100}}}))

        body = {
            "model": "tuned",
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 1.9,
            "logit_bias": {"5": 1},
            "max_tokens": 5000,
        }
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
    sent = json.loads(stack.mock["last_body"])
    assert sent["model"] == "mock-model" and sent["temperature"] == 0.3 and sent["max_tokens"] == 100
    assert "logit_bias" not in sent


async def test_context_check_refuses_before_queueing(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        # the model list says 262,144; capping the pool at ArliAI Core makes it 32,768
        await c.get("/v1/models", headers=stack.headers)  # loads the model list
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        await c.post(
            f"/admin/pools/{pool.id}",
            data={"csrf": csrf, "plan": "Core", "max_concurrency": "", "context_cap": "", "min_spacing_ms": "0"},
        )
        huge = {"model": "mock-model", "messages": [{"role": "user", "content": "x" * 400_000}]}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=huge)
        assert r.status_code == 400 and r.json()["error"]["code"] == "context_length_exceeded"
        assert stack.mock["served"] == 0
        ok = {"model": "mock-model", "messages": [{"role": "user", "content": "x" * 40_000}]}
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=ok)).status_code == 200
