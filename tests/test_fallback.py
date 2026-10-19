"""Alias fallback chains and refusal detection."""

import json

import httpx
import pytest

from app.fallback import is_policy_error, is_refusal_text, message_text
from tests.test_admin import OWNER, csrf_of, login


@pytest.mark.parametrize(
    "text, finish, expected",
    [
        ("I can't help with that request.", "stop", True),
        ("I’m sorry, but I can’t assist with this.", "stop", True),
        ("  As an AI, I cannot do that.", None, True),
        ("Sure! Here's the code you asked for.", "stop", False),
        ("I can help with that. First, ...", "stop", False),
        ("Here is the answer.", "content_filter", True),
        ("", None, False),
    ],
)
def test_is_refusal_text(text, finish, expected):
    assert is_refusal_text(text, finish) is expected


def test_policy_error_and_message_text():
    assert is_policy_error(400, b'{"error": {"message": "Request blocked by content policy"}}')
    assert not is_policy_error(400, b'{"error": {"message": "max_tokens must be positive"}}')
    assert not is_policy_error(500, b"content policy")
    tool_call = json.dumps({"choices": [{"message": {"tool_calls": [{"id": "1"}]}, "finish_reason": "tool_calls"}]})
    assert message_text(tool_call.encode()) == ("", "tool_calls")


async def add_chain(c, name, target, fallbacks, triggers):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "name": name, "target_model": target, "fallbacks": ", ".join(fallbacks)}
    form.update({f"on_{t}": "on" for t in triggers})
    r = await c.post("/admin/aliases", data=form)
    assert "m=created" in r.headers["location"], r.headers["location"]


def chat(model, **extra):
    return {"model": model, "messages": [{"role": "user", "content": "hi"}], "mock_tag": "x", **extra}


async def test_fallback_on_errors(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "big-model", ["mock-model"], ["errors"])
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("smart", mock_fail_models=["big-model"])
        )
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
        assert r.headers["x-queuerouter-model"] == "mock-model"
    assert stack.mock["models"] == ["big-model", "big-model", "mock-model"]  # 1 retry, then fallback
    entry = list(stack.app.state.stats.recent)[-1]
    assert entry["fallback"].startswith("big-model -> mock-model") and entry["target_model"] == "mock-model"


async def test_fallback_on_refusal_non_stream_and_policy_error(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "strict-model", ["mock-model"], ["refused"])
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("smart", mock_refuse_models=["strict-model"])
        )
        assert r.json()["choices"][0]["message"]["content"] == "hi x"
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("smart", mock_policy_models=["strict-model"])
        )
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
        # a normal answer from the primary passes through untouched
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("smart"))
        assert r.headers["x-queuerouter-model"] == "strict-model"


async def test_fallback_on_refusal_stream(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "strict-model", ["mock-model"], ["refused"])

        async def stream(**extra):
            async with c.stream(
                "POST", "/v1/chat/completions", headers=stack.headers, json=chat("smart", stream=True, **extra)
            ) as r:
                return "".join([chunk async for chunk in r.aiter_text()])

        refused = await stream(mock_refuse_models=["strict-model"])
        assert "strict-model refused; trying mock-model" in refused
        assert "can't" not in refused and "t0 " in refused and refused.rstrip().endswith("data: [DONE]")

        normal = await stream(mock_chunks=8)  # held back for the check, then replayed in full
        assert [f"t{i} " in normal for i in range(8)] == [True] * 8
        assert normal.rstrip().endswith("data: [DONE]") and "trying" not in normal


async def test_refusal_passes_through_when_not_enabled(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "strict-model", ["mock-model"], ["errors"])  # no "refused"
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json=chat("smart", mock_refuse_models=["strict-model"])
        )
        assert r.json()["choices"][0]["message"]["content"].startswith("I can't")


async def test_last_model_error_is_returned(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=0)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "a", ["b"], ["errors"])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("smart", mock_fail_models=["a", "b"]))
        assert r.status_code == 503
    assert stack.mock["models"] == ["a", "b"]
