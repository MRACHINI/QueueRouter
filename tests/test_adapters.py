"""Anthropic Messages and OpenAI Responses adapters, exercised with the official SDKs."""

import asyncio
import json

import anthropic
import httpx
import openai
import pytest

from app import anthropic_api, responses_api
from tests.conftest import ACCESS_KEY
from tests.test_usage import flushed

TOOL_SCHEMA = {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]}


def sent(stack) -> dict:
    return json.loads(stack.mock["last_body"])


# -- Anthropic -------------------------------------------------------------------------------


async def test_anthropic_messages(make_stack):
    stack = make_stack()
    client = anthropic.AsyncAnthropic(base_url=stack.url, api_key=ACCESS_KEY, max_retries=0)

    message = await client.messages.create(
        model="mock-model",
        max_tokens=100,
        system="be brief",
        messages=[
            {"role": "user", "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]}
        ],
    )
    assert message.content[0].type == "text" and message.content[0].text.startswith("hi")
    assert message.stop_reason == "end_turn" and message.usage.input_tokens == 10
    upstream = sent(stack)
    assert upstream["messages"][0] == {"role": "system", "content": "be brief"}
    assert upstream["max_tokens"] == 100 and "x-api-key" not in stack.mock["last_headers"]

    async with client.messages.stream(
        model="mock-model", max_tokens=100, messages=[{"role": "user", "content": "hi"}]
    ) as stream:
        text = "".join([t async for t in stream.text_stream])
        final = await stream.get_final_message()
    assert text == "t0 t1 t2 t3 t4 " and final.usage.output_tokens == 5
    assert sent(stack)["stream_options"] == {"include_usage": True}

    count = await client.messages.count_tokens(model="mock-model", messages=[{"role": "user", "content": "hello"}])
    assert count.input_tokens > 0


async def test_anthropic_tool_round_trip(make_stack):
    stack = make_stack()
    client = anthropic.AsyncAnthropic(base_url=stack.url, api_key=ACCESS_KEY, max_retries=0)
    tools = [{"name": "search", "description": "Search", "input_schema": TOOL_SCHEMA}]
    history = [{"role": "user", "content": "find x"}]

    async with client.messages.stream(model="mock-model", max_tokens=100, tools=tools, messages=history) as stream:
        final = await stream.get_final_message()
    assert final.stop_reason == "tool_use"
    assert [b.type for b in final.content] == ["text", "tool_use"]
    use = final.content[1]
    assert use.name == "search" and use.input == {"q": "x"}
    assert sent(stack)["tools"][0]["function"]["parameters"] == TOOL_SCHEMA

    history += [
        {"role": "assistant", "content": [b.model_dump(exclude_none=True) for b in final.content]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": use.id, "content": "42"}]},
    ]
    answer = await client.messages.create(model="mock-model", max_tokens=100, tools=tools, messages=history)
    assert answer.content[0].text == "hi tool said 42"
    roles = [m["role"] for m in sent(stack)["messages"]]
    assert roles == ["user", "assistant", "tool"]
    assert sent(stack)["messages"][1]["tool_calls"][0]["function"]["arguments"] == '{"q": "x"}'


async def test_anthropic_errors(make_stack):
    stack = make_stack()
    bad = anthropic.AsyncAnthropic(base_url=stack.url, api_key="wrong", max_retries=0)
    with pytest.raises(anthropic.AuthenticationError) as caught:
        await bad.messages.create(model="mock-model", max_tokens=10, messages=[{"role": "user", "content": "x"}])
    assert caught.value.body["error"]["type"] == "authentication_error"

    client = anthropic.AsyncAnthropic(base_url=stack.url, api_key=ACCESS_KEY, max_retries=0)
    with pytest.raises(anthropic.BadRequestError) as caught:
        await client.messages.create(
            model="mock-model",
            max_tokens=10,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": "x"}],
        )
    assert "not supported" in caught.value.body["error"]["message"]


async def test_what_claude_code_sends(make_stack):
    """System messages inside messages (from the first request on), effort, adaptive thinking, context management and
    deferred tools: taken; the system message stays where it is, as a reminder in the user's turn."""
    stack = make_stack()
    body = {
        "model": "mock-model",
        "max_tokens": 32_000,
        "system": [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1"},
            {"type": "text", "text": "You are Claude Code.", "cache_control": {"type": "ephemeral"}},
        ],
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "Say hi"}]},
            {
                "role": "system",
                "content": [{"type": "text", "text": "Today is Monday."}],
                "output_config": {"effort": "low"},
            },
        ],
        "tools": [{"name": "Bash", "description": "Run", "input_schema": TOOL_SCHEMA, "defer_loading": True}],
        "thinking": {"type": "adaptive", "display": "omitted"},
        "output_config": {"effort": "high"},
        "context_management": {"edits": [{"type": "clear_tool_uses_20250919"}]},
        "metadata": {"user_id": "u1"},
        "stream": True,
    }
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        async with c.stream("POST", "/v1/messages", headers=stack.headers, json=body) as r:
            text = (await r.aread()).decode()
    assert r.status_code == 200 and "message_stop" in text and '"tool_use"' in text
    upstream = sent(stack)
    assert upstream["messages"] == [
        {"role": "system", "content": "x-anthropic-billing-header: cc_version=2.1\nYou are Claude Code."},
        {"role": "user", "content": "Say hi\n\n<system-reminder>\nToday is Monday.\n</system-reminder>"},
    ]
    assert upstream["max_tokens"] == 32_000  # it fits this model: left as it is
    assert "reasoning_effort" not in upstream  # mock-model's thinking levels aren't known: nothing is guessed
    assert not any(k.startswith("x-queuerouter") for k in stack.mock["last_headers"])


async def test_the_effort_asked_for_goes_only_to_models_with_known_levels(make_stack):
    stack = make_stack(models_cache_s=0)
    reasoning = {"supported_efforts": ["low", "medium", "high"]}
    stack.mock["extra_model_entries"] = [{"id": "thinker", "object": "model", "reasoning": reasoning}]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)
        for model, effort, expected in (
            ("thinker", "max", "high"),
            ("thinker", "low", "low"),
            ("mock-model", "high", None),
        ):
            messages = [{"role": "user", "content": "hi"}]
            body = {"model": model, "max_tokens": 10, "messages": messages, "output_config": {"effort": effort}}
            r = await c.post("/v1/messages", headers=stack.headers, json=body)
            assert r.status_code == 200, r.text
            assert sent(stack).get("reasoning_effort") == expected, model


async def test_messages_in_chat_completions_form(make_stack):
    """Some apps mix chat completions' own forms into a Messages request: taken as they are, never lost."""
    stack = make_stack()
    call = {"id": "c1", "type": "function", "function": {"name": "search", "arguments": '{"q": "x"}'}}
    body = {
        "model": "mock-model",
        "max_tokens": 50,
        "messages": [
            {"role": "user", "content": [{"type": "input_text", "text": "find x"}]},
            {"role": "assistant", "content": None, "tool_calls": [call]},
            {"role": "tool", "tool_call_id": "c1", "content": "42"},
        ],
    }
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/messages", headers=stack.headers, json=body)
    assert r.status_code == 200, r.text
    assert r.json()["content"][0]["text"] == "hi tool said 42"
    messages = sent(stack)["messages"]
    assert messages == [
        {"role": "user", "content": "find x"},
        {"role": "assistant", "content": None, "tool_calls": [call]},
        {"role": "tool", "tool_call_id": "c1", "content": "42"},
    ]


def test_system_messages_and_what_they_carry():
    found = [
        {"type": "search_result", "title": "Doc", "source": "https://x", "content": [{"type": "text", "text": "body"}]},
        {"type": "tool_reference", "tool_name": "Bash"},
        {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": "x"}},
    ]
    removal = {"type": "tool_removal", "tool": {"type": "tool_reference", "name": "Bash"}}
    request = {
        "model": "m",
        "system": "top",
        "messages": [
            {"role": "system", "content": "also top"},  # before the conversation: the system prompt
            {"role": "user", "content": "one"},
            {"role": "system", "content": "old reminder", "clear_at": "next_user_message"},
            {"role": "assistant", "content": "two"},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t", "content": found}]},
            {"role": "system", "content": [removal]},
            {"role": "system", "content": [], "output_config": {"effort": "low"}},  # only sets the effort
            {"role": "system", "content": [{"type": "text", "text": "new reminder"}], "clear_at": "next_user_message"},
        ],
        "tools": [{"name": name, "input_schema": {"type": "object"}} for name in ("Bash", "Read")],
        "output_config": {"effort": "high", "format": {"type": "json_schema", "schema": {"type": "object"}}},
    }
    chat = anthropic_api.to_chat(request)
    messages = chat["messages"]
    assert [m["role"] for m in messages] == ["system", "user", "assistant", "tool", "user"]
    assert messages[0] == {"role": "system", "content": "top\nalso top"}
    assert "old reminder" not in json.dumps(messages)  # a reminder for one turn, cleared since
    assert "Doc - https://x\nbody" in messages[3]["content"] and "[Tool available: Bash]" in messages[3]["content"]
    assert "document in this tool result was left out" in messages[3]["content"]  # said, not lost silently
    assert messages[4] == {"role": "user", "content": "<system-reminder>\nnew reminder\n</system-reminder>"}
    assert [t["function"]["name"] for t in chat["tools"]] == ["Read"]  # taken away
    schema = {"name": "output", "schema": {"type": "object"}, "strict": True}
    assert chat["response_format"] == {"type": "json_schema", "json_schema": schema}
    assert anthropic_api.effort_of(request) == "low" and "reasoning_effort" not in chat
    with pytest.raises(anthropic_api.Unsupported, match="server_tool_use"):
        anthropic_api.to_chat({"messages": [{"role": "user", "content": [{"type": "server_tool_use"}]}]})


def test_a_prompt_too_long_is_said_the_way_claude_does():
    """Claude Code compacts the conversation and tries again only on Claude's own wording."""

    def said(status, message):
        body = json.dumps({"error": {"message": message}}).encode()
        return anthropic_api.from_openai_error(status, body)["error"]["message"]

    overflow = "This model's maximum context length is 8192 tokens. However, your request needs about 9500 tokens."
    assert said(400, overflow) == "prompt is too long: 9500 tokens > 8192 maximum"
    assert said(413, "Input is too long for this model.") == "prompt is too long: Input is too long for this model."
    assert said(400, "max_tokens is too large") == "max_tokens is too large"  # not the prompt


async def anthropic_events(chunks: list[dict], thinking: bool) -> list[dict]:
    async def body():
        for chunk in chunks:
            yield f"data: {json.dumps(chunk)}\n\n".encode()
        yield b"data: [DONE]\n\n"

    out = b"".join([e async for e in anthropic_api.stream(body(), "m", thinking)]).decode()
    return [json.loads(line[6:]) for line in out.splitlines() if line.startswith("data: ")]


async def test_the_models_reasoning_reaches_the_app():
    """Silent while the model reasoned, a stream looked stalled: the Claude app gave up after five minutes."""
    chunks = [
        {"choices": [{"delta": {"reasoning_content": "Let me "}}]},
        {"choices": [{"delta": {"reasoning": "think."}}]},
        {"choices": [{"delta": {"content": "Done."}, "finish_reason": "stop"}]},
    ]
    shown = await anthropic_events(chunks, thinking=True)
    starts = [(e["index"], e["content_block"]["type"]) for e in shown if e["type"] == "content_block_start"]
    assert starts == [(0, "thinking"), (1, "text")]
    deltas = [e["delta"] for e in shown if e["type"] == "content_block_delta"]
    assert "".join(d.get("thinking", "") for d in deltas) == "Let me think." and deltas[-1]["text"] == "Done."
    assert [e["index"] for e in shown if e["type"] == "content_block_stop"] == [0, 1]
    hidden = await anthropic_events(chunks, thinking=False)  # an app that didn't ask: pings, not blocks
    assert [e["content_block"]["type"] for e in hidden if e["type"] == "content_block_start"] == ["text"]
    assert [e["type"] for e in hidden].count("ping") == 1  # one every few seconds at most
    body = {"choices": [{"message": {"content": "Done.", "reasoning_content": "hmm"}, "finish_reason": "stop"}]}
    assert [b["type"] for b in anthropic_api.from_chat(body, "m", thinking=True)["content"]] == ["thinking", "text"]
    assert [b["type"] for b in anthropic_api.from_chat(body, "m")["content"]] == ["text"]
    assert anthropic_api.wants_thinking({"thinking": {"type": "adaptive"}}) and not anthropic_api.wants_thinking({})


async def test_reasoning_through_the_sdk(make_stack):
    stack = make_stack()
    stack.mock["think"] = 3
    client = anthropic.AsyncAnthropic(base_url=stack.url, api_key=ACCESS_KEY, max_retries=0)
    messages = [{"role": "user", "content": "hi"}]
    thinking = {"type": "enabled", "budget_tokens": 1024}
    async with client.messages.stream(model="mock-model", max_tokens=2000, thinking=thinking, messages=messages) as s:
        final = await s.get_final_message()
    assert [b.type for b in final.content] == ["thinking", "text"] and final.content[0].thinking == "r0 r1 r2 "
    async with client.messages.stream(model="mock-model", max_tokens=100, messages=messages) as s:
        plain = await s.get_final_message()
    assert [b.type for b in plain.content] == ["text"]


async def test_a_request_the_adapters_cant_translate_is_on_the_requests_page(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        server_tool = [{"type": "server_tool_use", "id": "s", "name": "web_search", "input": {}}]
        body = {"model": "mock-model", "max_tokens": 5, "messages": [{"role": "user", "content": server_tool}]}
        r = await c.post("/v1/messages", headers=stack.headers, json=body)
        assert r.status_code == 400 and "server_tool_use" in r.json()["error"]["message"]
        r = await c.post("/v1/responses", headers=stack.headers, json={"model": "mock-model", "input": [{"type": "x"}]})
        assert r.status_code == 400
        await flushed(stack, 2)
    rows = await asyncio.to_thread(stack.app.state.store.list_requests, 10)
    assert {(r["path"], r["outcome"], r["model"]) for r in rows} == {
        ("/v1/messages", "refused", "mock-model"),
        ("/v1/responses", "refused", "mock-model"),
    }


# -- OpenAI Responses ------------------------------------------------------------------------


async def test_responses_text_and_previous_response(make_stack):
    stack = make_stack()
    client = openai.AsyncOpenAI(base_url=f"{stack.url}/v1", api_key=ACCESS_KEY, max_retries=0)

    first = await client.responses.create(model="mock-model", input="hi", instructions="be brief", max_output_tokens=50)
    assert first.status == "completed" and first.output_text.startswith("hi")
    assert first.usage.input_tokens == 10 and first.usage.total_tokens == 10 + first.usage.output_tokens
    assert sent(stack)["messages"][0] == {"role": "system", "content": "be brief"}
    assert sent(stack)["max_tokens"] == 50

    second = await client.responses.create(model="mock-model", input="again", previous_response_id=first.id)
    assert second.output_text.startswith("hi")
    messages = sent(stack)["messages"]
    assert [m["role"] for m in messages] == ["user", "assistant", "user"]  # instructions aren't carried over
    assert messages[1]["content"] == first.output_text

    async with client.responses.stream(model="mock-model", input="stream it") as stream:
        deltas = [e.delta async for e in stream if e.type == "response.output_text.delta"]
        final = await stream.get_final_response()
    assert "".join(deltas) == "t0 t1 t2 t3 t4 " and final.output_text == "t0 t1 t2 t3 t4 "
    assert final.usage.output_tokens == 5

    with pytest.raises(openai.BadRequestError):
        await client.responses.create(model="mock-model", input="x", previous_response_id="resp_unknown")
    with pytest.raises(openai.BadRequestError):
        await client.responses.create(model="mock-model", input="x", tools=[{"type": "web_search"}])

    other = await client.responses.create(model="mock-model", input="x", store=False)
    with pytest.raises(openai.BadRequestError):
        await client.responses.create(model="mock-model", input="y", previous_response_id=other.id)


async def test_responses_function_calls(make_stack):
    stack = make_stack()
    client = openai.AsyncOpenAI(base_url=f"{stack.url}/v1", api_key=ACCESS_KEY, max_retries=0)
    tools = [{"type": "function", "name": "search", "description": "Search", "parameters": TOOL_SCHEMA}]

    async with client.responses.stream(model="mock-model", input="find x", tools=tools) as stream:
        final = await stream.get_final_response()
    calls = [item for item in final.output if item.type == "function_call"]
    assert len(calls) == 1 and calls[0].name == "search" and json.loads(calls[0].arguments) == {"q": "x"}

    answer = await client.responses.create(
        model="mock-model",
        previous_response_id=final.id,
        tools=tools,
        input=[{"type": "function_call_output", "call_id": calls[0].call_id, "output": "42"}],
    )
    assert answer.output_text == "hi tool said 42"
    messages = sent(stack)["messages"]
    assert [m["role"] for m in messages] == ["user", "assistant", "tool"]
    assert messages[1]["tool_calls"][0]["id"] == calls[0].call_id

    # non-streaming function call
    plain = await client.responses.create(model="mock-model", input="find y", tools=tools)
    assert plain.output[0].type == "function_call" and plain.output[0].arguments == '{"q": "x"}'

    rows = stack.app.state.stats.recent
    assert any("api:responses" in (r.get("tags") or "") for r in rows)


def test_responses_take_chat_completions_forms():
    call = {"id": "c1", "type": "function", "function": {"name": "s", "arguments": "{}"}}
    messages = responses_api.input_messages(
        [
            {"role": "user", "content": "find x"},
            {"role": "assistant", "content": None, "tool_calls": [call]},
            {"role": "tool", "tool_call_id": "c1", "content": [{"type": "input_text", "text": "42"}]},
        ]
    )
    assert messages == [
        {"role": "user", "content": "find x"},
        {"role": "assistant", "content": "", "tool_calls": [call]},
        {"role": "tool", "tool_call_id": "c1", "content": "42"},
    ]


def test_review_fixes_adapters():
    """Fable review: tool-calling replies are "tool_use" even when the server says "stop"; image tool results
    are noted, not lost; offset-less job times are UTC; adapter endpoints can't run as jobs; passing refusals
    are retried with back-off."""
    import pytest

    from app import anthropic_api, jobs

    body = {
        "id": "x",
        "choices": [
            {
                "finish_reason": "stop",
                "message": {
                    "content": None,
                    "tool_calls": [{"id": "c1", "function": {"name": "f", "arguments": "{}"}}],
                },
            }
        ],
    }
    assert anthropic_api.from_chat(body, "m")["stop_reason"] == "tool_use"
    request = {
        "model": "m",
        "max_tokens": 10,
        "messages": [
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": [
                {"type": "text", "text": "shot:"}, {"type": "image", "source": {"type": "base64", "data": "x"}}]}]},
        ],
    }  # fmt: skip
    chat = anthropic_api.to_chat(request)
    assert "image in this tool result was left out" in chat["messages"][0]["content"]
    assert jobs.parse_time("2026-11-01T09:00:00") == jobs.parse_time("2026-11-01T09:00:00Z")
    for endpoint in ("/v1/messages", "/v1/responses", "/v1/../admin"):
        with pytest.raises(jobs.JobError):
            jobs.check_endpoint(endpoint)
    assert jobs.retry_after(429, 1) == 15 and jobs.retry_after(503, 3) == 60 and jobs.retry_after(400, 1) is None
    assert jobs.retry_after(429, jobs.RETRY_ATTEMPTS) is None
