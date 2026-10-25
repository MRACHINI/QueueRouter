"""Anthropic Messages and OpenAI Responses adapters, exercised with the official SDKs."""

import json

import anthropic
import openai
import pytest

from tests.conftest import ACCESS_KEY

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
