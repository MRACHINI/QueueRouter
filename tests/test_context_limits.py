"""Context sizes beyond what providers report: learned from overflow errors, set by hand; overflow
errors rewritten into the standard form agents compact on."""

import asyncio
import json
import re

import httpx

from app.limits import overflow_body, overflow_of
from tests.test_admin import OWNER, csrf_of, login
from tests.test_aliases import add_alias


def body(text: str, model: str = "small-model") -> dict:
    return {"model": model, "messages": [{"role": "user", "content": text}]}


def test_overflow_parsing():
    openai = b'{"error": {"message": "This model\'s maximum context length is 32768 tokens. However..."}}'
    assert overflow_of(400, openai)[:2] == (True, 32768)
    google = (
        b'[{"error": {"message": "The input token count (2000000) exceeds the maximum number of tokens '
        b'allowed (1048576)."}}]'
    )
    assert overflow_of(400, google)[:2] == (True, 1048576)
    anthropic = b'{"error": {"message": "prompt is too long: 250000 tokens > 200000 maximum"}}'
    assert overflow_of(400, anthropic)[:2] == (True, 200000)
    assert overflow_of(413, b"Input is too long for this model.")[:2] == (True, None)
    assert overflow_of(400, b'{"error": {"message": "context_length_exceeded"}}')[:2] == (True, None)
    # not the prompt: the requested answer size, a policy block, other statuses
    output = (
        b'{"error": {"message": "max_tokens is too large: 50000. This model supports at most 16384 completion tokens"}}'
    )
    assert overflow_of(400, output)[0] is False
    assert overflow_of(400, b'{"error": {"message": "Request blocked by content policy"}}')[0] is False
    assert overflow_of(500, openai)[0] is False
    error = json.loads(overflow_body("m", 32768, 40000, "detail"))["error"]
    assert error["code"] == "context_length_exceeded" and "maximum context length is 32768 tokens" in error["message"]
    # opencode 2.x compacts on these (its own patterns, from its source)
    assert re.search(r"maximum context length is \d+ tokens", error["message"])
    assert re.search(r"reduce the length of the messages", error["message"], re.I)


async def test_limit_learned_from_provider_error(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model"]
    stack.mock["context_limits"] = {"small-model": (2000, "google")}
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        assert (await c.get("/v1/models", headers=stack.headers)).status_code == 200
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert "limit" not in config["provider"]["queuerouter"]["models"]["small-model"]  # not reported

        big = body("x" * 12_000)  # ~3,000 tokens
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=big)
        assert r.status_code == 400
        error = r.json()["error"]
        assert error["code"] == "context_length_exceeded"
        assert "maximum context length is 2000 tokens" in error["message"]
        rows = await asyncio.to_thread(store.list_model_limits)
        assert [(r["model"], r["source"], r["context"]) for r in rows] == [("small-model", "learned", 2000)]

        sent = len(stack.mock["overflowed"])
        again = await c.post("/v1/chat/completions", headers=stack.headers, json={**big, "stream": True})
        assert again.status_code == 400 and again.json()["error"]["code"] == "context_length_exceeded"
        assert len(stack.mock["overflowed"]) == sent  # refused before queuing: the provider never saw it
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body("short"))).status_code == 200

        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["small-model"]["limit"]["context"] == 2000
        models = (await c.get("/v1/models", headers=stack.headers)).json()["data"]
        assert next(m for m in models if m["id"] == "small-model")["context_length"] == 2000

        await login(c, stack.url)
        page = (await c.get("/admin/models")).text
        assert "learned 2,000" in page
        upstream = store.list_upstreams()[0]
        csrf = csrf_of(page)
        data = {"csrf": csrf, "upstream_id": str(upstream.id), "model": "small-model"}
        r = await c.post("/admin/model-limits/forget", data=data)
        assert "m=saved" in r.headers["location"]
        assert await asyncio.to_thread(store.list_model_limits) == []


async def test_manual_context_size(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        bad = {"csrf": csrf, "model": "small-model", "context": "4096", "output": "9000"}
        assert "m=saved" not in (await c.post("/admin/model-limits", data=bad)).headers["location"]
        data = {"csrf": csrf, "model": "small-model", "context": "4096", "output": "1024"}
        assert "m=saved" in (await c.post("/admin/model-limits", data=data)).headers["location"]

        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["small-model"]["limit"] == {"context": 4096, "output": 1024}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=body("x" * 20_000))
        assert r.status_code == 400 and "4096 tokens" in r.json()["error"]["message"]

        # a manual size also overrides what the provider reports
        data = {"csrf": csrf, "model": "mock-model", "context": "8192", "output": ""}
        await c.post("/admin/model-limits", data=data)
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert config["provider"]["queuerouter"]["models"]["mock-model"]["limit"]["context"] == 8192

        await c.post("/admin/model-limits", data={"csrf": csrf, "model": "small-model", "context": "", "output": ""})
        rows = await asyncio.to_thread(stack.app.state.store.list_model_limits)
        assert [r["model"] for r in rows] == ["mock-model"]


async def test_overflow_falls_back_to_bigger_model(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model"]
    stack.mock["context_limits"] = {"small-model": (2000, "unstated")}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/models")).text)
        form = {
            "csrf": csrf,
            "name": "auto",
            "target_model": "small-model",
            "fallbacks": "mock-model",
            "on_context": "on",
        }
        assert "m=created" in (await c.post("/admin/aliases", data=form)).headers["location"]
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=body("x" * 12_000, "auto"))
        assert r.status_code == 200, r.text
        assert stack.mock["overflowed"] == ["small-model"] and stack.mock["models"][-1] == "mock-model"
        assert await asyncio.to_thread(stack.app.state.store.list_model_limits) == []  # no number stated
        # without a fallback the provider's wording is still rewritten
        await add_alias(c, "solo", "small-model")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=body("x" * 12_000, "solo"))
        assert r.status_code == 400 and r.json()["error"]["code"] == "context_length_exceeded"
