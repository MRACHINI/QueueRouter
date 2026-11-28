"""What a request cost and what a model costs, in the forms apps read (app/costs.py): ``usage.cost`` in a chat
answer, ``pricing`` in /v1/models, and LiteLLM's /model/info."""

import json

import httpx
import pytest

from app import costs
from app.pricing import Price
from tests.test_admin import OWNER, csrf_of, login

PRICE = Price(1.0, 2.0, 0.1, source="manual")  # USD per million: in, out, cached in


def cost_of(prompt, completion, cached, written):
    return costs.request_cost(PRICE, prompt, completion, cached, written)


def test_prices_as_apps_read_them():
    assert costs.per_token(1.5) == "0.0000015" and costs.per_token(0.075) == "0.000000075"
    assert costs.per_token(0) == "0" and costs.per_token(1_000_000) == "1"
    assert costs.pricing(PRICE) == {
        "prompt": "0.000001",
        "completion": "0.000002",
        "input_cache_read": "0.0000001",
        "input_cache_write": "0.00000125",
    }
    # 6 normal + 4 cached prompt tokens, 5 out; a write to the cache costs a quarter more
    assert costs.request_cost(PRICE, 10, 5, 4, 0) == pytest.approx(1.64e-5)
    assert costs.request_cost(PRICE, 10, 5, 4, 2) == pytest.approx(1.69e-5)
    assert costs.request_cost(None, 10, 5, 0, 0) is None  # a model without a price
    assert costs.request_cost(Price(0.0, 0.0, 0.0, source="plan"), 10, 5, 0, 0) == 0.0


def test_a_whole_answer_gets_its_cost():
    usage = {"prompt_tokens": 10, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 4}}
    answer = {"id": "x", "choices": [{"message": {"content": "héllo"}}], "usage": usage}
    out = json.loads(costs.with_cost(json.dumps(answer).encode(), cost_of))
    assert out["usage"]["cost"] == pytest.approx(1.64e-5) and out["choices"] == answer["choices"]
    assert "héllo".encode() in costs.with_cost(json.dumps(answer, ensure_ascii=False).encode(), cost_of)
    # left as they are: the provider's own figure, no token counts, no price, not JSON
    for kept in (
        {"usage": {**usage, "cost": 0.5}},
        {"usage": {**usage, "cost": {"total_cost": 0.5}}},
        {"usage": {"seconds": 3}},
        {"choices": []},
    ):
        body = json.dumps(kept).encode()
        assert costs.with_cost(body, cost_of) is body
    body = json.dumps(answer).encode()
    assert costs.with_cost(body, lambda *_: None) is body
    assert costs.with_cost(b'not json "usage"', cost_of) == b'not json "usage"'
    # half a surrogate pair, escaped in the answer, is escaped again rather than failing
    odd = b'{"choices":[{"message":{"content":"\\ud83d"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}'
    assert json.loads(costs.with_cost(odd, cost_of))["usage"]["cost"] == pytest.approx(3e-6)


def test_a_stream_gets_its_cost_in_the_usage_chunk():
    text = {"id": "x", "choices": [{"index": 0, "delta": {"content": "hi"}, "finish_reason": "stop"}]}
    usage = {"id": "x", "choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5}}
    stream = f"data: {json.dumps(text)}\r\n\r\ndata: {json.dumps(usage)}\r\n\r\ndata: [DONE]\r\n\r\n".encode()
    for size in (1, 7, 64, len(stream)):  # however the provider's chunks fall
        writer = costs.CostWriter(cost_of)
        out = b"".join(writer.feed(stream[i : i + size]) for i in range(0, len(stream), size)) + writer.flush()
        lines = out.split(b"\r\n\r\n")
        assert lines[0] == f"data: {json.dumps(text)}".encode()  # untouched, byte for byte
        assert json.loads(lines[1][5:])["usage"]["cost"] == pytest.approx(2e-5)
        assert lines[2:] == [b"data: [DONE]", b""]
    # a stream cut off in the middle of its last line: what came goes on at the end
    writer = costs.CostWriter(cost_of)
    assert writer.feed(b"data: {}\n\ndata: {par") == b"data: {}\n\n" and writer.flush() == b"data: {par"
    assert writer.flush() == b""
    # a usage chunk that ends the stream without a newline
    writer = costs.CostWriter(cost_of)
    assert writer.feed(f"data: {json.dumps(usage)}".encode()) == b""
    assert json.loads(writer.flush()[5:])["usage"]["cost"] == pytest.approx(2e-5)


def test_a_model_as_litellm_lists_it():
    entry = {
        "id": "QWEN|Qwen4 Max",
        "context_length": 1_000_000,
        "supports_reasoning": True,
        "architecture": {"input_modalities": ["text", "image"]},
    }
    row = costs.model_info(entry, PRICE, 1_000_000, 128_000, None)
    assert row["model_name"] == "QWEN|Qwen4 Max" and row["litellm_params"]["model"] == "queuerouter/QWEN|Qwen4 Max"
    info = row["model_info"]
    assert info["mode"] == "chat" and info["max_input_tokens"] == 1_000_000 and info["max_output_tokens"] == 128_000
    assert info["supports_vision"] is True and info["supports_reasoning"] is True
    assert "reasoning_effort" in info["supported_openai_params"] and "tools" in info["supported_openai_params"]
    assert info["input_cost_per_token"] == pytest.approx(1e-6) and info["output_cost_per_token"] == pytest.approx(2e-6)
    assert info["cache_read_input_token_cost"] == pytest.approx(1e-7)
    assert info["cache_creation_input_token_cost"] == pytest.approx(1.25e-6) and info["supports_prompt_caching"]
    # one that doesn't think, with nothing known about it, on a plan: no prices (0 would read as unknown anyway)
    plain = costs.model_info({"id": "m"}, Price(0.0, 0.0, 0.0, source="plan"), None, None, None)["model_info"]
    assert plain["supports_reasoning"] is False and "reasoning_effort" not in plain["supported_openai_params"]
    assert not {"input_cost_per_token", "max_input_tokens", "max_output_tokens", "supports_vision"} & set(plain)
    # what isn't a chat model says so, and claims nothing about chat
    image = costs.model_info({"id": "img", "output_modalities": ["image"]}, None, None, None, "image")["model_info"]
    assert image["mode"] == "image_generation" and "supported_openai_params" not in image
    assert costs.model_info({"id": "v"}, None, None, None, "video")["model_info"]["mode"] == "video_generation"
    assert costs.model_info({"id": "s"}, None, None, None, "speech")["model_info"]["mode"] == "audio_speech"


async def test_apps_are_told_what_a_request_cost(make_stack):
    stack = make_stack(models_cache_s=0, bootstrap=OWNER)
    stack.mock["extra_models"] = ["unpriced-model"]
    chat = {"messages": [{"role": "user", "content": "x"}], "mock_cached": 4}

    async def streamed(c: httpx.AsyncClient, body: dict) -> list[dict]:
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            assert r.status_code == 200
            text = "".join([piece async for piece in r.aiter_text()])
        lines = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
        assert lines[-1] == "[DONE]"
        return [json.loads(line) for line in lines[:-1]]

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        # no price known for a model: nothing is said about cost, anywhere
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "model": "mock-model"})
        assert r.status_code == 200 and "cost" not in r.json()["usage"]
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert "pricing" not in listed["mock-model"]

        # yours: $1 in, $0.10 cached, $2 out per million
        csrf = csrf_of((await c.get("/admin/models")).text)
        form = {
            "csrf": csrf,
            "model": "mock-model",
            "reference": "",
            "input": "1",
            "cached_input": "0.1",
            "output": "2",
        }
        assert "m=saved" in (await c.post("/admin/prices", data=form)).headers["location"]

        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "model": "mock-model"})
        usage = r.json()["usage"]
        assert usage["cost"] == pytest.approx(1.64e-5) and usage["prompt_tokens"] == 10  # 6 + 4 cached in, 5 out
        assert int(r.headers["content-length"]) == len(r.content)

        # streamed: in the usage chunk an app asked for; an app that didn't ask still gets no usage chunk
        asked = {**chat, "model": "mock-model", "stream": True, "stream_options": {"include_usage": True}}
        chunks = await streamed(c, asked)
        assert chunks[-1]["choices"] == [] and chunks[-1]["usage"]["cost"] == pytest.approx(1.64e-5)
        assert "".join(ch["choices"][0]["delta"].get("content", "") for ch in chunks[:-1]) == "t0 t1 t2 t3 t4 "
        assert all("usage" not in ch for ch in await streamed(c, {**chat, "model": "mock-model", "stream": True}))

        # the Responses API says it too
        r = await c.post("/v1/responses", headers=stack.headers, json={"model": "mock-model", "input": "x"})
        assert r.status_code == 200 and r.json()["usage"]["cost"] == pytest.approx(2e-5), r.text

        # the model list, OpenRouter's way
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert listed["mock-model"]["pricing"] == {
            "prompt": "0.000001",
            "completion": "0.000002",
            "input_cache_read": "0.0000001",
            "input_cache_write": "0.00000125",
        }
        assert "pricing" not in listed["unpriced-model"]

        # and LiteLLM's way, at both of its addresses, for the same key
        for path in ("/model/info", "/v1/model/info"):
            rows = {row["model_name"]: row for row in (await c.get(path, headers=stack.headers)).json()["data"]}
            assert set(rows) == set(listed)
            info = rows["mock-model"]["model_info"]
            assert info["input_cost_per_token"] == pytest.approx(1e-6) and info["mode"] == "chat"
            assert info["output_cost_per_token"] == pytest.approx(2e-6) and info["max_input_tokens"] == 262144
            assert rows["mock-model"]["litellm_params"]["model"] == "queuerouter/mock-model"
            assert "input_cost_per_token" not in rows["unpriced-model"]["model_info"]
            assert (await c.get(path)).status_code == 401

        # on a monthly plan a request costs nothing extra, unless you set a price for the model yourself
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        plan = {"csrf": csrf, "plan": "Pro", "max_concurrency": "", "context_cap": "", "min_spacing_ms": "0"}
        await c.post(f"/admin/pools/{pool.id}", data={**plan, "monthly_price": ""})
        assert stack.app.state.store.list_pools()[0].monthly_price == 30
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "model": "unpriced-model"})
        assert r.json()["usage"]["cost"] == 0
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "model": "mock-model"})
        assert r.json()["usage"]["cost"] == pytest.approx(1.64e-5)
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert listed["unpriced-model"]["pricing"]["prompt"] == "0"
        rows = {row["model_name"]: row for row in (await c.get("/model/info", headers=stack.headers)).json()["data"]}
        assert "input_cost_per_token" not in rows["unpriced-model"]["model_info"]
