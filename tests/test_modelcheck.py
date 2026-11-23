"""Check models (app/modelcheck.py): what a provider's model list says about a model, field by field with what
QueueRouter reads it as, on the Upstreams page; and how a thinking level is sent to an upstream, chosen by hand."""

import json

import httpx

from app import modelcheck, thinking
from app.opencode import CONTEXT_KEYS, OUTPUT_KEYS
from tests.test_admin import OWNER, csrf_of, login

RICH = {
    "id": "rich-model",
    "object": "model",
    "owned_by": "someone",
    "context_window": 400_000,
    "top_provider": {"context_length": 400_000, "max_completion_tokens": 32_000},
    "reasoning": {"supported_efforts": ["low", "high"], "default_effort": "high"},
    "architecture": {"input_modalities": ["text", "image"], "tokenizer": "Other"},
    "pricing": {"prompt": "0.000001", "completion": "0.000002"},
    "ctx_len": 123_456,
}


def test_what_each_field_is_read_as():
    rows = {row["path"]: row for row in modelcheck.fields(RICH)}
    assert rows["context_window"]["use"] == "context" and rows["context_window"]["value"] == "400000"
    assert rows["top_provider.max_completion_tokens"]["use"] == "output size"
    assert rows["reasoning.supported_efforts"]["use"] == "thinking levels"
    assert rows["reasoning.supported_efforts"]["value"] == '["low", "high"]'
    assert rows["architecture.input_modalities"]["use"] == "reads images"
    assert rows["pricing.prompt"]["use"] == "input price"
    assert rows["id"]["use"] == modelcheck.PLAIN and rows["owned_by"]["use"] == modelcheck.PLAIN
    assert modelcheck.unread(list(rows.values())) == [
        "top_provider.context_length",
        "architecture.tokenizer",
        "ctx_len",
    ]
    # read as itself, or as a whole
    assert modelcheck.fields({"reasoning": True}) == [{"path": "reasoning", "value": "true", "use": "thinking levels"}]
    claude = modelcheck.fields({"capabilities": {"effort": {"supported": True, "low": {"supported": True}}}})
    assert [(r["path"], r["use"]) for r in claude] == [("capabilities.effort", "thinking levels")]
    assert len(modelcheck.fields({"x": "y" * 500})[0]["value"]) == modelcheck.MAX_VALUE
    # in step with the readers: every key they take is in the table
    for key in (*CONTEXT_KEYS, *OUTPUT_KEYS, "reasoning_efforts", "vlm", "vision", "output_modalities", "engine"):
        assert modelcheck.use_of(key) not in (modelcheck.UNREAD, modelcheck.PLAIN), key
    assert set(thinking.CHOICES) <= set(modelcheck.STYLE_NOTES)


async def test_the_check_page_and_how_levels_are_sent(make_stack):
    stack = make_stack(models_cache_s=0, bootstrap=OWNER)
    stack.mock["extra_model_entries"] = [RICH, {"id": "bare-thinker", "object": "model"}]
    app = stack.app
    items = [{"id": "maker/bare-thinker", "reasoning": {"supported_efforts": ["low", "medium", "high"]}}]
    app.state.reference.use(thinking.Reference.slim(items), 1)
    store = app.state.store
    upstream = store.list_upstreams()[0]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get(f"/admin/upstreams/{upstream.id}/check")).text
        assert "rich-model" in page and "ctx_len" in page and "not read</span>" in page
        assert "400,000" in page and "its list" in page and "low · high (default high)" in page
        assert "$1 in" in page and "$2 out" in page and "upstream" in page  # its list's own prices
        assert "only levels its own list states are offered" in page  # an API QueueRouter doesn't know
        assert "no thinking levels</span>" in page  # bare-thinker: the reference isn't used for an unknown API
        assert f'href="/admin/upstreams/{upstream.id}/check"' in (await c.get("/admin/upstreams")).text
        assert (await c.get("/admin/upstreams/999/check")).status_code in (302, 303)

        async def asked(model: str, effort: str) -> dict:
            body = {"model": model, "messages": [{"role": "user", "content": "hi"}], "reasoning_effort": effort}
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=body)
            assert r.status_code == 200, r.text
            return json.loads(stack.mock["last_body"])

        assert (await asked("bare-thinker", "max"))["reasoning_effort"] == "max"  # unknown: as the app sent it

        async def choose(style: str) -> None:
            csrf = csrf_of((await c.get("/admin/upstreams")).text)
            form = {"csrf": csrf, "weight": "1", "enabled": "on", "ua_mode": "keep", "thinking_style": style}
            r = await c.post(f"/admin/upstreams/{upstream.id}", data=form)
            assert r.status_code in (302, 303) and "e=" not in r.headers["location"], r.headers["location"]

        # chosen by hand: the reference's levels are offered, and a level is fitted and sent that way
        await choose("openai")
        assert store.list_upstreams()[0].thinking_style == "openai"
        page = (await c.get(f"/admin/upstreams/{upstream.id}/check")).text
        assert "low · medium · high" in page and "OpenRouter (reference)" in page
        assert "chosen on the Upstreams page" in page
        assert (await asked("bare-thinker", "max"))["reasoning_effort"] == "high"
        await choose("openrouter")
        sent = await asked("bare-thinker", "low")
        assert sent["reasoning"] == {"effort": "low"} and "reasoning_effort" not in sent
        await choose("none")
        assert "reasoning_effort" not in await asked("bare-thinker", "low")
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert "supports_reasoning" not in listed["bare-thinker"]  # no levels offered there now
        await choose("")
        assert store.list_upstreams()[0].thinking_style == ""
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        bad = {"csrf": csrf, "weight": "1", "enabled": "on", "ua_mode": "keep", "thinking_style": "telepathy"}
        r = await c.post(f"/admin/upstreams/{upstream.id}", data=bad)
        assert "e=invalid" in r.headers["location"] and store.list_upstreams()[0].thinking_style == ""
