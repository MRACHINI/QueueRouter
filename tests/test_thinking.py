"""Thinking levels (app/thinking.py): detection, the opencode variants, and the per-upstream translation."""

import json

import httpx

from app import thinking

DEEPSEEK_BLOCK = {
    "mandatory": False,
    "default_enabled": True,
    "supported_efforts": ["max", "high", "low", "none"],
    "default_effort": "high",
}


def test_levels_from_model_lists():
    assert thinking.from_block(DEEPSEEK_BLOCK) == (("none", "low", "high", "max"), "high")
    assert thinking.from_block(True) == (("none", "high"), None)  # ArliAI: thinks, on / off
    assert thinking.from_block({"mandatory": False, "default_enabled": True}) == (("none", "high"), None)
    assert thinking.from_block({"mandatory": True}) is None  # always thinks, nothing to choose
    assert thinking.from_block({"mandatory": False}) is None  # says nothing about thinking
    found = thinking.from_block(DEEPSEEK_BLOCK)
    assert thinking.for_style("openrouter", found) == (
        ("none", "none"),
        ("low", "low"),
        ("high", "high"),
        ("max", "max"),
    )
    assert thinking.for_style("vllm", found) == (("none", "none"), ("low", "low"), ("high", "high"))
    assert thinking.for_style("vllm", (("none", "high"), None)) == thinking.TOGGLE
    assert thinking.style_of("https://api.arliai.com/v1", None) == "vllm"
    assert thinking.style_of("https://openrouter.ai/api/v1", None) == "openrouter"
    assert thinking.style_of("https://api.anthropic.com/v1", None) == "none"
    assert thinking.style_of("http://10.0.0.5:8000/v1", {"engine": "vllm"}) == "vllm"


def test_each_upstream_is_asked_its_own_way():
    payload = {"reasoning_effort": "max"}
    assert thinking.translate(payload, "openrouter") and payload == {"reasoning": {"effort": "max"}}
    payload = {"reasoning_effort": "max"}
    assert thinking.translate(payload, "vllm")
    assert payload == {"reasoning_effort": "high", "chat_template_kwargs": {"enable_thinking": True, "thinking": True}}
    payload = {"reasoning_effort": "none", "chat_template_kwargs": {"x": 1}}
    assert thinking.translate(payload, "vllm")
    assert payload == {"chat_template_kwargs": {"x": 1, "enable_thinking": False, "thinking": False}}
    payload = {"reasoning_effort": "high"}
    assert thinking.translate(payload, "none") and payload == {}
    assert not thinking.translate({"reasoning_effort": "high"}, "openai")


def test_reference_list_by_name():
    reference = thinking.Reference()
    items = [
        {"id": "deepseek/deepseek-v4.1-flash-1017", "name": "DeepSeek: DeepSeek V4.1 Flash 1017",
         "hugging_face_id": "deepseek-ai/DeepSeek-V4.1-Flash-1017", "reasoning": DEEPSEEK_BLOCK,
         "benchmarks": {"artificial_analysis": {"intelligence_index": 24.7, "coding_index": None}}},
        {"id": "qwen/qwen3.8-max", "name": "Qwen: Qwen3.8 Max", "reasoning": {"mandatory": True}},
    ]  # fmt: skip
    reference.use(thinking.Reference.slim(items), 1)
    assert reference.thinking("DeepSeek-V4.1-Flash-1017") == (("none", "low", "high", "max"), "high")
    assert reference.thinking("hf:deepseek-ai/DeepSeek-V4.1-Flash-1017") is not None
    assert reference.thinking("qwen3.8-max") is None
    assert reference.index_entries() == [
        {"name": "DeepSeek V4.1 Flash 1017", "slug": "deepseek-v4.1-flash-1017", "creator": "",
         "scores": {"intelligence": 24.7}}
    ]  # fmt: skip


async def test_opencode_variants_and_translation(make_stack):
    stack = make_stack(models_cache_s=0)
    stack.mock["extra_model_entries"] = [{"id": "think-model", "object": "model", "reasoning": True, "engine": "vllm"}]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        plain = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        assert "thinking" not in plain["provider"]["queuerouter"]["models"]["think-model"]  # unchanged for opencode
        config = (await c.get("/v1/opencode-config?plugin=2", headers=stack.headers)).json()
        models = config["provider"]["queuerouter"]["models"]
        assert models["think-model"]["thinking"]["variants"] == [["off", "none"], ["thinking", "high"]]
        assert "thinking" not in models["mock-model"]

        body = {"model": "think-model", "messages": [{"role": "user", "content": "hi"}], "reasoning_effort": "none"}
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
        sent = json.loads(stack.mock["last_body"])
        assert "reasoning_effort" not in sent
        assert sent["chat_template_kwargs"] == {"enable_thinking": False, "thinking": False}

        store = stack.app.state.store
        store.set_model_thinking("think-model", "t", "-")
        config = (await c.get("/v1/opencode-config?plugin=2", headers=stack.headers)).json()
        assert "thinking" not in config["provider"]["queuerouter"]["models"]["think-model"]
