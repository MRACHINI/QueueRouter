"""Claude's own API: chat completions translated to and from Messages, prompt caching and what it costs."""

import asyncio
import json
import time

import anthropic
import httpx
import openai
import pytest

from app import anthropic_api, claude
from app.pricing import DEFAULT_MIX, Price, PriceCatalog, blended_price, price_text, token_costs
from app.sse import encode
from app.upstream import ANTHROPIC_VERSION, UsageSniffer, claude_headers, usage_from_body
from tests.conftest import ACCESS_KEY, eventually
from tests.test_admin import OWNER, login
from tests.test_fallback import add_chain
from tests.test_model_names import opencode
from tests.test_usage import flushed

MODEL = "claude-opus-5-5"
SYSTEM = "You are a coding agent. " * 40
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search",
            "description": "Search the code",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]},
            "strict": True,
        },
    }
]
PRICES = {"anthropic": {"models": {MODEL: {"cost": {"input": 4, "output": 20, "cache_read": 0.2}}}}}


def claude_stack(make_stack, **options):
    """A stack whose only upstream is Claude's API (the preset, as the mock has another address)."""
    stack = make_stack(**{"models_cache_s": 0, **options})
    store = stack.app.state.store
    store.update_upstream(store.list_upstreams()[0].id, "test", preset="anthropic")
    stack.app.state.reload()
    stack.mock["extra_model_entries"] = [{"id": MODEL, "max_input_tokens": 1_000_000, "max_tokens": 128_000}]
    httpx.get(f"{stack.url}/v1/models", headers=stack.headers)  # read the model list
    return stack


def user(text="hi", **extra):
    return {"model": MODEL, "messages": [{"role": "user", "content": text}], **extra}


async def ask(c, stack, **body):
    r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": MODEL, **body})
    assert r.status_code == 200, r.text
    return r.json()


def written(chat_usage):
    return chat_usage["prompt_tokens_details"]["cache_write_tokens"]


def cached(chat_usage):
    return chat_usage["prompt_tokens_details"]["cached_tokens"]


# -- requests --------------------------------------------------------------------------------


def test_a_conversation_in_claudes_format():
    payload = {
        "model": MODEL,
        "max_tokens": 500,
        "seed": 3,
        "user": "someone",
        "logprobs": True,
        "store": False,
        "metadata": {"a": 1},
        "reasoning_effort": "high",
        "stream_options": {"include_usage": True},
        "stream": True,
        "stop": ["END", "  "],
        "temperature": 1.5,
        "tools": TOOLS,
        "messages": [
            {"role": "system", "content": "first"},
            {"role": "user", "content": [{"type": "text", "text": "look"}, {"type": "text", "text": "  "}]},
            {"role": "developer", "content": [{"type": "text", "text": "second"}]},
            {
                "role": "assistant",
                "content": "",
                "reasoning_content": "hmm",
                "tool_calls": [
                    {
                        "id": "functions.search:0",
                        "type": "function",
                        "function": {"name": "search", "arguments": '{"q": 1}'},
                    },
                    {"id": "call_2", "type": "function", "function": {"name": "search", "arguments": ""}},
                ],
            },
            {"role": "tool", "tool_call_id": "functions.search:0", "content": "found"},
            {"role": "tool", "tool_call_id": "call_2", "content": ""},
            {"role": "user", "content": "thanks"},
        ],
    }
    body = claude.request(payload)
    assert set(body) == {
        "model",
        "max_tokens",
        "messages",
        "system",
        "tools",
        "cache_control",
        "stream",
        "stop_sequences",
        "temperature",
    }
    assert body["system"] == "first\nsecond" and body["cache_control"] == {"type": "ephemeral"}
    assert body["max_tokens"] == 500 and body["stop_sequences"] == ["END"] and body["temperature"] == 1
    assert body["tools"] == [
        {
            "name": "search",
            "description": "Search the code",
            "input_schema": TOOLS[0]["function"]["parameters"],
        }
    ]
    assert body["messages"] == [
        {"role": "user", "content": [{"type": "text", "text": "look"}]},
        {
            "role": "assistant",
            "content": [
                {"type": "tool_use", "id": "functions_search_0", "name": "search", "input": {"q": 1}},
                {"type": "tool_use", "id": "call_2", "name": "search", "input": {}},
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "functions_search_0",
                    "content": [{"type": "text", "text": "found"}],
                },
                {"type": "tool_result", "tool_use_id": "call_2"},
                {"type": "text", "text": "thanks"},
            ],
        },
    ]


def test_tool_results_open_their_turn():
    """An app can put a user message between the results of two calls; Claude wants the results first."""
    calls = [
        {"id": "a", "function": {"name": "t", "arguments": "{}"}},
        {"id": "b", "function": {"name": "t", "arguments": "{}"}},
    ]
    messages = [
        {"role": "user", "content": "go"},
        {"role": "assistant", "content": None, "tool_calls": calls},
        {"role": "tool", "tool_call_id": "a", "content": "1"},
        {"role": "user", "content": "a note"},
        {"role": "tool", "tool_call_id": "b", "content": "2"},
    ]
    last = claude.request({"model": MODEL, "messages": messages})["messages"][-1]
    assert [block["type"] for block in last["content"]] == ["tool_result", "tool_result", "text"]


def test_images_and_parts_it_cant_take():
    parts = [
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,QUJD", "detail": "high"}},
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
        {"type": "input_audio", "input_audio": {"data": "x", "format": "wav"}},
    ]
    content = claude.request({"model": MODEL, "messages": [{"role": "user", "content": parts}]})["messages"][0][
        "content"
    ]
    assert content[0] == {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "QUJD"}}
    assert content[1] == {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}}
    assert content[2]["type"] == "text" and "input_audio part was left out" in content[2]["text"]
    tool_image = [{"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,QUJD"}}]
    calls = [{"id": "a", "function": {"name": "t", "arguments": "{}"}}]
    messages = [
        {"role": "user", "content": "go"},
        {"role": "assistant", "content": None, "tool_calls": calls},
        {"role": "tool", "tool_call_id": "a", "content": tool_image},
    ]
    result = claude.request({"model": MODEL, "messages": messages})["messages"][-1]["content"][0]
    assert result["content"][0]["source"]["media_type"] == "image/jpeg"


@pytest.mark.parametrize(
    "fields, entry, expected",
    [
        ({}, None, claude.MAX_TOKENS),
        ({"stream": True}, None, claude.MAX_TOKENS_STREAM),
        ({}, {"max_tokens": 8192}, 8192),  # the model's own limit
        ({"stream": True}, {"max_tokens": 8192}, 8192),
        ({"max_tokens": 200_000}, {"max_tokens": 128_000}, 128_000),
        ({"max_completion_tokens": 300, "max_tokens": 200}, None, 300),
        ({"max_tokens": 0}, None, claude.MAX_TOKENS),
        ({"max_tokens": "many"}, None, claude.MAX_TOKENS),
    ],
)
def test_max_tokens_is_always_there(fields, entry, expected):
    assert claude.request(user(**fields), entry)["max_tokens"] == expected


@pytest.mark.parametrize(
    "fields, expected",
    [
        ({}, None),
        ({"tool_choice": "auto"}, None),
        ({"tool_choice": "none"}, {"type": "none"}),
        ({"tool_choice": "required"}, {"type": "any"}),
        ({"tool_choice": {"type": "function", "function": {"name": "search"}}}, {"type": "tool", "name": "search"}),
        ({"parallel_tool_calls": False}, {"type": "auto", "disable_parallel_tool_use": True}),
        ({"tool_choice": "none", "parallel_tool_calls": False}, {"type": "none"}),
        (
            {"tool_choice": "required", "parallel_tool_calls": False},
            {"type": "any", "disable_parallel_tool_use": True},
        ),
    ],
)
def test_tool_choice(fields, expected):
    assert claude.request(user(tools=TOOLS, **fields)).get("tool_choice") == expected
    assert "tool_choice" not in claude.request(user(**fields))  # the API refuses it without tools


def test_only_what_comes_again_is_cached():
    """A write costs more than plain input, so a one-off request only marks what can come again."""
    one_off = claude.request(user())
    assert "cache_control" not in one_off and "system" not in one_off
    system = {"role": "system", "content": SYSTEM}
    one_off = claude.request({"model": MODEL, "messages": [system, {"role": "user", "content": "hi"}]})
    assert "cache_control" not in one_off
    assert one_off["system"] == [{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}]
    assert "cache_control" in claude.request(user(tools=TOOLS))
    history = [
        {"role": "user", "content": "one"},
        {"role": "assistant", "content": "two"},
        {"role": "user", "content": "three"},
    ]
    assert "cache_control" in claude.request({"model": MODEL, "messages": history})


def test_answer_formats():
    schema = {"type": "object", "properties": {}, "additionalProperties": False}
    strict = {"type": "json_schema", "json_schema": {"name": "a", "schema": schema, "strict": True}}
    wanted = {"format": {"type": "json_schema", "schema": schema}}
    assert claude.request(user(response_format=strict))["output_config"] == wanted
    loose = {"type": "json_schema", "json_schema": {"name": "a", "schema": schema}}
    for other in (loose, {"type": "json_object"}, {"type": "text"}):  # QueueRouter's repair still sees to these
        assert "output_config" not in claude.request(user(response_format=other))


def test_the_older_form_of_tool_calls():
    messages = [
        {"role": "user", "content": "go"},
        {"role": "assistant", "content": None, "function_call": {"name": "search", "arguments": '{"q": "a"}'}},
        {"role": "function", "name": "search", "content": "found"},
    ]
    functions = [TOOLS[0]["function"]]
    body = claude.request({"model": MODEL, "messages": messages, "functions": functions, "function_call": "none"})
    assert body["tools"][0]["name"] == "search" and body["tool_choice"] == {"type": "none"}
    call, result = body["messages"][1]["content"][0], body["messages"][2]["content"][0]
    assert call["id"] == result["tool_use_id"] == "call_1" and call["input"] == {"q": "a"}


def test_tool_without_parameters_still_has_a_schema():
    tools = [{"type": "function", "function": {"name": "now"}}, {"type": "web_search"}]
    assert claude.request(user(tools=tools))["tools"] == [{"name": "now", "input_schema": {"type": "object"}}]


def test_headers():
    sent = {"Content-Type": "text/plain", "anthropic-beta": "x", "anthropic-version": "2026-10-21", "x-other": "1"}
    headers = claude_headers({**sent, "authorization": "Bearer k"}, "k")
    assert headers == {
        "x-other": "1",
        "content-type": "application/json",
        "anthropic-version": ANTHROPIC_VERSION,
        "x-api-key": "k",
    }


# -- answers ---------------------------------------------------------------------------------


USAGE = {
    "input_tokens": 3,
    "cache_read_input_tokens": 1000,
    "cache_creation_input_tokens": 200,
    "output_tokens": 5,
}


def test_a_message_as_a_chat_completion():
    message = {
        "id": "msg_1",
        "model": MODEL,
        "content": [
            {"type": "thinking", "thinking": "", "signature": "x"},
            {"type": "text", "text": "Let me "},
            {"type": "text", "text": "check."},
            {"type": "tool_use", "id": "toolu_1", "name": "search", "input": {"q": "é"}},
        ],
        "stop_reason": "tool_use",
        "usage": USAGE,
    }
    body = claude.completion(message)
    choice = body["choices"][0]
    assert body["object"] == "chat.completion" and body["id"] == "msg_1" and body["model"] == MODEL
    assert choice["finish_reason"] == "tool_calls" and choice["message"]["content"] == "Let me check."
    assert choice["message"]["tool_calls"] == [
        {"id": "toolu_1", "type": "function", "function": {"name": "search", "arguments": '{"q": "é"}'}}
    ]
    assert body["usage"] == {
        "prompt_tokens": 1203,
        "completion_tokens": 5,
        "total_tokens": 1208,
        "prompt_tokens_details": {"cached_tokens": 1000, "cache_write_tokens": 200},
    }
    assert usage_from_body(json.dumps(body).encode()) == (1203, 5, 1000, 200)


@pytest.mark.parametrize(
    "stop, finish",
    [
        ("end_turn", "stop"),
        ("stop_sequence", "stop"),
        ("max_tokens", "length"),
        ("model_context_window_exceeded", "length"),
        ("tool_use", "tool_calls"),
        ("refusal", "content_filter"),
        ("pause_turn", "stop"),
        (None, "stop"),
    ],
)
def test_finish_reasons(stop, finish):
    body = claude.completion({"content": [{"type": "text", "text": "x"}], "stop_reason": stop})
    assert body["choices"][0]["finish_reason"] == finish


def sse(*events):
    return "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events).encode()


def delta(index, **fields):
    return {"type": "content_block_delta", "index": index, "delta": fields}


STREAM = sse(
    {"type": "message_start", "message": {"id": "msg_1", "model": MODEL, "usage": {**USAGE, "output_tokens": 1}}},
    {"type": "ping"},
    {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}},
    delta(0, type="thinking_delta", thinking=""),
    delta(0, type="signature_delta", signature="x"),
    {"type": "content_block_stop", "index": 0},
    {"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}},
    delta(1, type="text_delta", text="héllo "),
    delta(1, type="text_delta", text="there"),
    {"type": "content_block_stop", "index": 1},
    {"type": "content_block_start", "index": 2, "content_block": {"type": "tool_use", "id": "t1", "name": "a"}},
    delta(2, type="input_json_delta", partial_json=""),
    delta(2, type="input_json_delta", partial_json='{"q"'),
    delta(2, type="input_json_delta", partial_json=': "x"}'),
    {"type": "content_block_stop", "index": 2},
    {"type": "content_block_start", "index": 3, "content_block": {"type": "tool_use", "id": "t2", "name": "b"}},
    {"type": "content_block_stop", "index": 3},
    {"type": "something_new", "index": 9},
    {
        "type": "message_delta",
        "delta": {"stop_reason": "tool_use"},
        "usage": {"output_tokens": 42, "input_tokens": None},
    },
    {"type": "message_stop"},
)


async def translated(raw: bytes, size: int) -> list:
    async def pieces():
        for start in range(0, len(raw), size):
            yield raw[start : start + size]

    return [chunk async for chunk in claude._chunks(pieces())]


def data_of(chunks) -> list[dict]:
    lines = [line for chunk in chunks for line in chunk.decode().splitlines() if line.startswith("data: {")]
    return [json.loads(line[6:]) for line in lines]


@pytest.mark.parametrize("size", [1, 7, 64, 100_000])  # bytes can be split anywhere
async def test_a_stream_as_chat_chunks(size):
    chunks = await translated(STREAM, size)
    joined = b"".join(chunks)
    assert b": ping\n\n" in joined and joined.endswith(b"data: [DONE]\n\n") and b"signature" not in joined
    data = data_of(chunks)
    assert {d["id"] for d in data} == {"msg_1"} and {d["object"] for d in data} == {"chat.completion.chunk"}
    assert data[0]["choices"][0]["delta"] == {"role": "assistant", "content": ""}
    deltas = [d["choices"][0]["delta"] for d in data if d["choices"]]
    assert "".join(x.get("content") or "" for x in deltas) == "héllo there"
    calls = [call for x in deltas for call in x.get("tool_calls", [])]
    assert [(c["index"], c.get("id"), c["function"].get("name")) for c in calls if c.get("id")] == [
        (0, "t1", "a"),
        (1, "t2", "b"),
    ]
    arguments = {0: "", 1: ""}
    for call in calls:
        arguments[call["index"]] += call["function"]["arguments"]
    assert arguments == {0: '{"q": "x"}', 1: "{}"}  # a call without arguments is still JSON
    assert data[-2]["choices"][0]["finish_reason"] == "tool_calls"
    assert data[-1]["choices"] == []
    assert data[-1]["usage"]["prompt_tokens"] == 1203 and data[-1]["usage"]["completion_tokens"] == 42

    sniffer = UsageSniffer()
    sniffer.feed(joined)
    assert (sniffer.prompt_tokens, sniffer.completion_tokens) == (1203, 42)
    assert (sniffer.cached_tokens, sniffer.cache_write_tokens) == (1000, 200)
    assert sniffer.finish_reason == "tool_calls"


def test_half_a_surrogate_pair_cant_break_a_stream():
    assert encode(None, {"text": "\ud83d"}) == b'data: {"text": "?"}\n\n'


async def test_an_error_in_a_stream():
    raw = sse(
        {"type": "message_start", "message": {"id": "msg_1", "model": MODEL, "usage": {}}},
        {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}},
    )
    chunks = await translated(raw, 16)
    assert data_of(chunks)[-1] == {"error": {"message": "Overloaded", "type": "overloaded_error", "code": 529}}
    assert b"".join(chunks).endswith(b"data: [DONE]\n\n")


async def test_a_response_that_isnt_json_passes_through():
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"not json"

    def handler(request):
        return httpx.Response(200, stream=Body(), headers={"content-type": "text/plain"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        request = client.build_request("POST", "http://claude/v1/messages")
        response = claude.answer(await client.send(request, stream=True))
        assert await response.aread() == b"not json"
        await response.aclose()


async def test_errors_are_left_as_they_are():
    def handler(request):
        return httpx.Response(529, json={"type": "error", "error": {"type": "overloaded_error", "message": "Busy"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        request = client.build_request("POST", "http://claude/v1/messages")
        response = await client.send(request, stream=True)
        assert claude.answer(response) is response
        assert (await response.aread()).startswith(b'{"type"')
        await response.aclose()


def test_native_upstreams():
    class Upstream:
        def __init__(self, preset, base_url):
            self.preset, self.base_url = preset, base_url

    assert claude.native(Upstream("anthropic", "https://api.anthropic.com/v1"))
    assert not claude.native(Upstream("custom", "https://api.anthropic.com/v1"))  # the way back to the OpenAI endpoint
    assert not claude.native(Upstream("openrouter", "https://openrouter.ai/api/v1"))


# -- costs -----------------------------------------------------------------------------------


def test_a_write_costs_more_than_plain_input():
    price = Price(4.0, 20.0, 0.2, source="x")
    normal, read, out = token_costs(price, 1_000_000, 100_000, 600_000, 300_000)
    # 100k fresh at $4, 300k written at $5, 600k read at $0.20, 100k out at $20
    assert normal == pytest.approx(0.4 + 1.5) and read == pytest.approx(0.12) and out == pytest.approx(2.0)
    assert token_costs(price, 100, 0, 0, 900)[0] == pytest.approx(token_costs(price, 100, 0, 0, 100)[0])  # at most all
    assert token_costs(price, 1_000_000, 0, 0)[0] == pytest.approx(4.0)  # no writes: as before


def test_the_price_in_opencode_counts_writes():
    price = Price(4.0, 20.0, 0.2, source="x")
    assert price_text(price, DEFAULT_MIX) == "$1.158"
    written = Price(4.0, 20.0, 0.2, source="x", writes_cache=True)
    assert price_text(written, DEFAULT_MIX) == "$1.358"
    assert blended_price(written, (1.0, 0.0, 0.0)) == pytest.approx(5.0)
    assert price_text(Price(0.0, 0.0, 0.0, source="plan", writes_cache=True), DEFAULT_MIX) == "plan"


def test_the_messages_adapter_reports_writes():
    usage = {"prompt_tokens": 1203, "completion_tokens": 5}
    usage["prompt_tokens_details"] = {"cached_tokens": 1000, "cache_write_tokens": 200}
    assert anthropic_api._usage(usage) == {
        "input_tokens": 3,
        "output_tokens": 5,
        "cache_read_input_tokens": 1000,
        "cache_creation_input_tokens": 200,
    }


# -- through QueueRouter ---------------------------------------------------------------------


async def test_a_request_goes_to_messages_with_claudes_headers(make_stack):
    stack = claude_stack(make_stack)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        body = await ask(c, stack, messages=[{"role": "user", "content": "hi"}], mock_tag="dropped", temperature=0.5)
    assert body["choices"][0]["message"]["content"] == "hi from claude"
    assert body["choices"][0]["finish_reason"] == "stop" and body["object"] == "chat.completion"
    sent = stack.mock["claude_requests"][-1]
    headers = stack.mock["last_headers"]
    assert headers["x-api-key"] == "upstream-secret" and "authorization" not in headers
    assert headers["anthropic-version"] == ANTHROPIC_VERSION and headers["content-type"] == "application/json"
    assert sent["max_tokens"] == claude.MAX_TOKENS and sent["temperature"] == 0.5
    assert "last_body" not in stack.mock  # nothing went to chat/completions


async def test_a_malformed_request_is_refused(make_stack):
    stack = claude_stack(make_stack)
    messages = [{"role": "assistant", "content": None, "tool_calls": ["x"]}, {"role": "user", "content": "hi"}]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": MODEL, "messages": messages})
        assert r.status_code == 400 and "expected format" in r.json()["error"]["message"]
        assert (await ask(c, stack, messages=[{"role": "user", "content": "hi"}]))["choices"]  # its slot is free
    assert len(stack.mock["claude_requests"]) == 1


async def test_other_upstreams_stay_openai_compatible(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=user(model="mock-model"))
    assert r.status_code == 200 and "claude_requests" not in stack.mock and "last_body" in stack.mock


async def test_only_chat_completions_are_translated(make_stack):
    stack = claude_stack(make_stack)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        body = {"model": MODEL, "input": "text"}
        r = await c.post("/v1/embeddings", headers=stack.headers, json=body)
    assert r.status_code == 200 and "claude_requests" not in stack.mock


async def test_the_models_output_limit_is_respected(make_stack):
    stack = claude_stack(make_stack)
    stack.mock["extra_model_entries"][0]["max_tokens"] = 4096
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)
        await ask(c, stack, messages=[{"role": "user", "content": "hi"}])
    assert stack.mock["claude_requests"][-1]["max_tokens"] == 4096


async def test_an_agent_loop_reads_what_it_wrote(make_stack):
    """The cached start only helps if it stays exactly the same from request to request."""
    stack = claude_stack(make_stack)
    messages = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": "find it"}]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        first = await ask(c, stack, messages=messages, tools=TOOLS)
        reply = first["choices"][0]
        assert reply["finish_reason"] == "tool_calls" and reply["message"]["content"] == "Let me check."
        call = reply["message"]["tool_calls"][0]
        assert call["function"] == {"name": "search", "arguments": '{"q": "x"}'}
        assert cached(first["usage"]) == 0 and written(first["usage"]) > 100

        messages += [reply["message"], {"role": "tool", "tool_call_id": call["id"], "content": "in main.py"}]
        second = await ask(c, stack, messages=messages, tools=TOOLS)
        assert second["choices"][0]["finish_reason"] == "stop"
        assert cached(second["usage"]) >= written(first["usage"])  # all of the first request

        messages += [second["choices"][0]["message"], {"role": "user", "content": "and now?"}]
        third = await ask(c, stack, messages=messages, tools=TOOLS)
        assert cached(third["usage"]) > cached(second["usage"])
        total = third["usage"]["prompt_tokens"]
        assert total == 3 + cached(third["usage"]) + written(third["usage"])
        await flushed(stack, 3)
    rows = sorted(await asyncio.to_thread(stack.app.state.store.list_requests, 10), key=lambda r: r["ts"])
    assert rows[0]["cached_tokens"] == 0 and rows[0]["cache_write_tokens"] > 100
    assert rows[2]["cached_tokens"] == cached(third["usage"])
    assert rows[2]["cache_write_tokens"] == written(third["usage"])


async def test_streaming_with_the_openai_sdk(make_stack):
    stack = claude_stack(make_stack)
    client = openai.AsyncOpenAI(base_url=f"{stack.url}/v1", api_key=ACCESS_KEY, max_retries=0)
    messages = [{"role": "user", "content": "hi"}]

    stream = await client.chat.completions.create(model=MODEL, messages=messages, stream=True)
    chunks = [chunk async for chunk in stream]
    text = "".join(chunk.choices[0].delta.content or "" for chunk in chunks)  # every chunk has a choice: not asked
    assert text == "hi from claude" and chunks[-1].choices[0].finish_reason == "stop"

    options = {"include_usage": True}
    stream = await client.chat.completions.create(model=MODEL, messages=messages, stream=True, stream_options=options)
    chunks = [chunk async for chunk in stream]
    assert chunks[-1].choices == [] and chunks[-1].usage.completion_tokens == 7

    stream = await client.chat.completions.create(model=MODEL, messages=messages, tools=TOOLS, stream=True)
    calls = [call async for chunk in stream if chunk.choices for call in chunk.choices[0].delta.tool_calls or []]
    assert calls[0].id == "toolu_1" and calls[0].function.name == "search"
    assert "".join(call.function.arguments or "" for call in calls) == '{"q": "x"}'
    await flushed(stack, 3)
    rows = await asyncio.to_thread(stack.app.state.store.list_requests, 10)
    assert {r["finish_reason"] for r in rows} == {"stop", "tool_calls"} and all(
        r["completion_tokens"] == 7 for r in rows
    )
    assert all(r["outcome"] == "done" and r["first_token_ms"] is not None for r in rows)


async def test_usage_is_counted_even_if_the_app_turned_it_off(make_stack):
    stack = claude_stack(make_stack)
    body = user(stream=True, stream_options={"include_usage": False})
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            text = (await r.aread()).decode()
        await flushed(stack, 1)
    assert '"usage"' not in text and "[DONE]" in text
    rows = await asyncio.to_thread(stack.app.state.store.list_requests, 10)
    assert rows[0]["completion_tokens"] == 7


async def test_the_messages_api_on_top_of_claude(make_stack):
    """Clients of QueueRouter's /v1/messages see Claude's cache counters; input_tokens means what it does there."""
    stack = claude_stack(make_stack)
    client = anthropic.AsyncAnthropic(base_url=stack.url, api_key=ACCESS_KEY, max_retries=0)
    history = [
        {"role": "user", "content": "one"},
        {"role": "assistant", "content": "two"},
        {"role": "user", "content": "three"},
    ]
    first = await client.messages.create(model=MODEL, max_tokens=100, system=SYSTEM, messages=history)
    assert first.usage.input_tokens == 3 and first.usage.cache_creation_input_tokens > 100
    assert first.content[0].text == "hi from claude"
    history += [{"role": "assistant", "content": "four"}, {"role": "user", "content": "five"}]
    async with client.messages.stream(model=MODEL, max_tokens=100, system=SYSTEM, messages=history) as stream:
        text = "".join([t async for t in stream.text_stream])
        final = await stream.get_final_message()
    assert text == "hi from claude" and final.usage.cache_read_input_tokens >= first.usage.cache_creation_input_tokens
    assert final.usage.output_tokens == 7
    assert stack.mock["claude_requests"][-1]["system"] == SYSTEM


async def test_overloaded_claude_is_retried_then_reported(make_stack):
    stack = claude_stack(make_stack, max_retries=1, circuit_threshold=0)
    stack.mock["claude_status"] = 529
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=user())
        assert r.status_code == 529 and r.json()["error"]["message"] == "forced"
    assert len(stack.mock["claude_requests"]) == 2  # the first try and one retry


async def test_a_busy_claude_is_waited_for(make_stack):
    stack = claude_stack(make_stack, max_retries=3)
    stack.mock["claude_busy"] = 2
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=user())
    assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi from claude"
    assert len(stack.mock["claude_requests"]) == 3


async def test_its_refusals_reach_the_app(make_stack):
    stack = claude_stack(make_stack)
    stack.mock["claude_status"] = 400
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=user())
    assert r.status_code == 400 and r.json()["error"]["message"] == "forced"


async def test_a_prompt_too_long_for_claude_is_seen_as_such(make_stack):
    """Its wording is how QueueRouter learns a context size, shown on the Models page."""
    stack = claude_stack(make_stack, bootstrap=OWNER)
    stack.mock["context_limits"] = {MODEL: (2000, "anthropic")}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=user("word " * 2000))
        assert r.status_code == 400 and "prompt is too long" in r.text
        assert stack.mock["overflowed"][0] == MODEL
        assert "learned 2,000" in (await c.get("/admin/models")).text


async def test_an_error_in_the_middle_of_a_stream_reaches_the_app(make_stack):
    stack = claude_stack(make_stack)
    stack.mock["claude_stream_error"] = True
    client = openai.AsyncOpenAI(base_url=f"{stack.url}/v1", api_key=ACCESS_KEY, max_retries=0)
    stream = await client.chat.completions.create(
        model=MODEL, messages=[{"role": "user", "content": "hi"}], stream=True
    )
    with pytest.raises(openai.APIError) as caught:
        _ = [chunk async for chunk in stream]
    assert "Overloaded" in str(caught.value)


async def test_a_client_that_leaves_ends_the_stream_at_claude(make_stack):
    stack = claude_stack(make_stack, max_concurrency=1)
    stack.mock["claude_delay"] = 0.2
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        body = user(stream=True)
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
            async for line in r.aiter_lines():
                if '"hi "' in line:
                    break
        await eventually(lambda: stack.mock.get("claude_active") == 0)
        stack.mock["claude_delay"] = 0
        assert (await ask(c, stack, messages=[{"role": "user", "content": "again"}]))["choices"]  # its slot is free
        await flushed(stack, 2)
    rows = await asyncio.to_thread(stack.app.state.store.list_requests, 10)
    assert {r["outcome"] for r in rows} == {"cancelled", "done"}


async def test_a_refusal_in_a_stream_falls_back(make_stack):
    """The fallback check reads the start of a stream in the background, and closes it to go to the next model."""
    stack = claude_stack(make_stack, bootstrap=OWNER)
    stack.mock["claude_refuses"] = True
    stack.mock["extra_model_entries"].append({"id": "claude-other"})
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await c.get("/v1/models", headers=stack.headers)
        await add_chain(c, "careful", MODEL, ["claude-other"], ["refused"])

        async def ask_stream(model):
            payload = user(model=model, stream=True)
            async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=payload) as r:
                return (await r.aread()).decode()

        stack.app.state.reload()
        text = await ask_stream("careful")
    assert "help with" in text or "from" in text
    assert stack.mock["models"] == [MODEL, "claude-other"]


async def test_opencode_is_told_a_price_with_cache_writes(make_stack):
    stack = claude_stack(make_stack)
    stack.app.state.prices.use(PriceCatalog.slim(PRICES), time.time())
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        models = await opencode(c, stack)
    name = models[f"{MODEL}@upstream"]["name"] if f"{MODEL}@upstream" in models else models[MODEL]["name"]
    assert name.startswith("$1.358|")  # with DEFAULT_MIX: 20% at $5 (a write), 79% at $0.20, 1% at $20


async def test_usage_and_requests_pages_price_writes(make_stack):
    stack = claude_stack(make_stack, bootstrap=OWNER)
    stack.app.state.prices.use(PriceCatalog.slim(PRICES), time.time())
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        one = await ask(
            c, stack, messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": "hi"}], tools=TOOLS
        )
        await flushed(stack, 1)
        row = (await c.get("/admin/api/requests")).json()["rows"][0]
        assert row["tokens_written"] == written(one["usage"]) and row["tokens_in"] == written(one["usage"]) + 3
        assert row["cost_in"] == pytest.approx((3 + written(one["usage"]) * 1.25) * 4 / 1e6)
        page = (await c.get("/admin/usage/body")).text
        assert "written to Claude&#39;s prompt cache" in page or "written to Claude's prompt cache" in page
        assert "of them written to the cache" in page
        report = (await c.get("/admin/usage.csv")).text
        assert "cache_write_tokens" in report.splitlines()[0]
        stack.app.state.reload()
