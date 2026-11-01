"""Claude's own API (Messages), for upstreams with the Anthropic preset.

Its OpenAI-compatible endpoint has no prompt caching, so a ``chat/completions`` request is translated to
``POST /messages`` right before it's sent, and the answer back as it's read: the queue, retries, failover, usage and
the API adapters all keep seeing chat completions. It stays plain HTTP like every other upstream rather than
Anthropic's SDK, whose own retries would work against QueueRouter's.

Caching is automatic: a top-level ``cache_control`` puts the breakpoint on the last block and moves it along as a
conversation grows, and what's written lasts 5 minutes from its last use. A one-off request only marks its system
prompt: a write costs 1.25 times the input price (app/pricing.py), which only pays off when something reads it back.

Thinking is neither shown nor passed back. opencode keeps no thinking blocks, blocks in a history that opencode or
QueueRouter shortened would fail their signature check on Claude 5.1 and later (a 400 on newer accounts), and with
adaptive thinking a turn without them is valid: the model just can't see its earlier reasoning, as on the
OpenAI-compatible endpoint.
"""

from __future__ import annotations

import contextlib
import json
import re
import time
from collections.abc import AsyncIterator, Callable

import httpx

from .anthropic_api import Unsupported, tool_input
from .sse import encode, events

PRESET = "anthropic"
MAX_TOKENS = 16_000  # for a request that sets none (the API insists on a number)
MAX_TOKENS_STREAM = 64_000  # streamed: longer answers can't time out
FINISH_REASONS = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "model_context_window_exceeded": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
}
ERROR_STATUS = {  # an error in the middle of a stream: its HTTP status, had it come before the stream
    "invalid_request_error": 400,
    "authentication_error": 401,
    "permission_error": 403,
    "not_found_error": 404,
    "request_too_large": 413,
    "rate_limit_error": 429,
    "timeout_error": 504,
    "overloaded_error": 529,
}
_BAD_ID = re.compile(r"[^a-zA-Z0-9_-]")  # Claude's rule for tool call ids; other providers' ids can break it
_DATA_URL = re.compile(r"data:([^;,]+);base64,(.*)", re.S)


def native(upstream) -> bool:
    """Whether this upstream speaks Messages. By the preset, not the address: a Custom upstream at api.anthropic.com
    stays on its OpenAI-compatible endpoint, the way back if something here doesn't suit."""
    return upstream.preset == PRESET


# -- request ---------------------------------------------------------------------------------


def _image(image) -> dict:
    url = image.get("url", "") if isinstance(image, dict) else str(image or "")
    if found := _DATA_URL.fullmatch(url):
        return {"type": "image", "source": {"type": "base64", "media_type": found[1], "data": found[2]}}
    return {"type": "image", "source": {"type": "url", "url": url}}


def _blocks(content) -> list[dict]:
    """A message's content as Claude's blocks. Blank text is left out: the API refuses it."""
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content.strip() else []
    blocks = []
    for part in content if isinstance(content, list) else []:
        kind = part.get("type") if isinstance(part, dict) else None
        if kind == "text":
            if str(part.get("text") or "").strip():
                blocks.append({"type": "text", "text": part["text"]})
        elif kind == "image_url":
            blocks.append(_image(part.get("image_url")))
        elif kind not in (None, "refusal"):  # say so, rather than lose it silently
            blocks.append({"type": "text", "text": f"[A {kind} part was left out: this model API can't receive it.]"})
    return blocks


def _tool_id(value) -> str:
    return _BAD_ID.sub("_", str(value or "")) or "call"


def _tool_use(call_id, function: dict) -> dict:
    return {
        "type": "tool_use",
        "id": _tool_id(call_id),
        "name": function.get("name", ""),
        "input": tool_input(function.get("arguments")),
    }


def _tool_result(call_id, content) -> dict:
    result = {"type": "tool_result", "tool_use_id": _tool_id(call_id)}
    if blocks := _blocks(content):
        result["content"] = blocks
    return result


def _messages(chat: list) -> tuple[str, list[dict]]:
    """(system prompt, Claude's messages). System and developer messages go to the top, wherever they were."""
    system: list[str] = []
    messages: list[dict] = []
    legacy_calls = 0  # function_call / role "function": the older form of tool calls, which has no ids

    def add(role: str, blocks: list[dict]) -> None:  # consecutive messages of a role are one turn
        if blocks and messages and messages[-1]["role"] == role:
            messages[-1]["content"].extend(blocks)
        elif blocks:
            messages.append({"role": role, "content": blocks})

    for message in chat:
        role = message.get("role") if isinstance(message, dict) else None
        if role is None:
            continue
        content = message.get("content")
        if role in ("system", "developer"):
            system.extend(block["text"] for block in _blocks(content) if block["type"] == "text")
        elif role == "assistant":
            blocks = _blocks(content)
            for call in message.get("tool_calls") or []:
                blocks.append(_tool_use(call.get("id"), call.get("function") or {}))
            if isinstance(message.get("function_call"), dict):
                legacy_calls += 1
                blocks.append(_tool_use(f"call_{legacy_calls}", message["function_call"]))
            add("assistant", blocks)
        elif role == "tool":
            add("user", [_tool_result(message.get("tool_call_id"), content)])
        elif role == "function":
            add("user", [_tool_result(f"call_{legacy_calls}", content)])
        else:
            add("user", _blocks(content))
    for message in messages:  # tool results must open their turn, even with a user message sent between them
        if message["role"] == "user":
            message["content"].sort(key=lambda block: block["type"] != "tool_result")
    return "\n".join(system), messages


def _tools(payload: dict) -> list[dict]:
    functions = [t.get("function") for t in payload.get("tools") or [] if isinstance(t, dict)]
    tools = []
    for function in (*functions, *(payload.get("functions") or [])):
        if not isinstance(function, dict):
            continue
        schema = function.get("parameters") if isinstance(function.get("parameters"), dict) else {}
        tool = {"name": function.get("name", ""), "input_schema": {"type": "object", **schema}}
        if function.get("description"):
            tool["description"] = function["description"]
        tools.append(tool)
    return tools


def _tool_choice(payload: dict) -> dict | None:
    """None: Claude's default, any number of calls if it wants."""
    wanted = payload.get("tool_choice", payload.get("function_call"))
    if isinstance(wanted, dict):
        name = (wanted.get("function") or {}).get("name") or wanted.get("name")
        choice = {"type": "tool", "name": name} if name else {"type": "auto"}
    else:
        choice = {"type": {"none": "none", "required": "any"}.get(wanted, "auto")}
    if payload.get("parallel_tool_calls") is False and choice["type"] != "none":
        choice["disable_parallel_tool_use"] = True
    return None if choice == {"type": "auto"} else choice


def request(payload: dict, entry: dict | None = None) -> dict:
    """A chat completion request as the body of ``POST /messages``; ``entry`` is the model's line in the upstream's
    model list. Only what the API knows goes on: it refuses unknown fields, and the fields left out (seed, logprobs,
    user...) are the ones its OpenAI-compatible endpoint ignores."""
    try:
        return _body(payload, entry)
    except (AttributeError, TypeError) as exc:
        raise Unsupported("the request's messages or tools aren't in the expected format") from exc


def _body(payload: dict, entry: dict | None) -> dict:
    system, messages = _messages(payload.get("messages") or [])
    stream = payload.get("stream") is True
    limits = (payload.get("max_completion_tokens"), payload.get("max_tokens"))
    wanted = next((n for n in limits if isinstance(n, int) and n > 0), MAX_TOKENS_STREAM if stream else MAX_TOKENS)
    most = (entry or {}).get("max_tokens")
    body: dict = {
        "model": payload.get("model"),
        "max_tokens": min(wanted, most) if isinstance(most, int) and most > 0 else wanted,
        "messages": messages,
    }
    if system:
        body["system"] = system
    tools = _tools(payload)
    if tools:
        body["tools"] = tools
        if choice := _tool_choice(payload):
            body["tool_choice"] = choice
    if tools or len(messages) > 1:
        body["cache_control"] = {"type": "ephemeral"}
    elif system:  # a one-off request: only its system prompt can come again, a write of the rest would be wasted
        body["system"] = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
    if stream:
        body["stream"] = True
    stop = payload.get("stop")
    sequences = [s for s in ([stop] if isinstance(stop, str) else stop or []) if isinstance(s, str) and s.strip()]
    if sequences:
        body["stop_sequences"] = sequences
    if isinstance(payload.get("temperature"), int | float):
        body["temperature"] = min(payload["temperature"], 1)
    for name in ("top_p", "top_k"):
        if payload.get(name) is not None:
            body[name] = payload[name]
    answer_format = payload.get("response_format")
    schema = answer_format.get("json_schema") if isinstance(answer_format, dict) else None
    if isinstance(schema, dict) and schema.get("strict") is True and isinstance(schema.get("schema"), dict):
        body["output_config"] = {"format": {"type": "json_schema", "schema": schema["schema"]}}
    return body


# -- response --------------------------------------------------------------------------------


def _usage(usage: dict) -> dict:
    """Claude counts the input in three parts; chat completions count all of it as prompt tokens, with the cached
    ones among them."""
    fresh = usage.get("input_tokens") or 0
    read = usage.get("cache_read_input_tokens") or 0
    written = usage.get("cache_creation_input_tokens") or 0
    output = usage.get("output_tokens") or 0
    return {
        "prompt_tokens": fresh + read + written,
        "completion_tokens": output,
        "total_tokens": fresh + read + written + output,
        "prompt_tokens_details": {"cached_tokens": read, "cache_write_tokens": written},
    }


def completion(message: dict) -> dict:
    """A Claude message as a chat completion."""
    text, calls = [], []
    for block in message.get("content") or []:
        if block.get("type") == "text":
            text.append(block.get("text") or "")
        elif block.get("type") == "tool_use":
            arguments = json.dumps(block.get("input") or {}, ensure_ascii=False)
            function = {"name": block.get("name", ""), "arguments": arguments}
            calls.append({"id": block.get("id"), "type": "function", "function": function})
    reply: dict = {"role": "assistant", "content": "".join(text) or None}
    if calls:
        reply["tool_calls"] = calls
    finish = FINISH_REASONS.get(message.get("stop_reason"), "stop")
    return {
        "id": message.get("id"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": message.get("model"),
        "choices": [{"index": 0, "message": reply, "finish_reason": finish}],
        "usage": _usage(message.get("usage") or {}),
    }


async def _whole(raw: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    body = b"".join([chunk async for chunk in raw])
    try:
        yield json.dumps(completion(json.loads(body)), ensure_ascii=False).encode("utf-8", "replace")
    except (ValueError, AttributeError):  # not a message: the pipeline's checks say what's wrong with it
        yield body


async def _chunks(raw: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Claude's stream events as chat completion chunks. Thinking blocks and events it may add one day are left out."""
    identity = {"id": "", "object": "chat.completion.chunk", "created": int(time.time()), "model": ""}
    usage: dict = {}
    stop = None
    calls: dict[int, int] = {}  # content block index -> tool call index
    with_arguments: set[int] = set()

    def chunk(delta: dict | None, finish: str | None = None, **extra) -> bytes:
        choices = [] if delta is None else [{"index": 0, "delta": delta, "finish_reason": finish}]
        return encode(None, {**identity, "choices": choices, **extra})

    def tool(index: int, **fields) -> bytes:
        return chunk({"tool_calls": [{"index": calls[index], **fields}]})

    async with contextlib.aclosing(events(raw)) as stream:
        async for kind, data in stream:
            if kind != "data" or not isinstance(data, dict):
                continue
            event, index = data.get("type"), data.get("index")
            if event == "message_start":
                message = data.get("message") or {}
                identity.update(id=message.get("id", ""), model=message.get("model", ""))
                usage = message.get("usage") or {}
                yield chunk({"role": "assistant", "content": ""})
            elif event == "content_block_start":
                block = data.get("content_block") or {}
                if block.get("type") == "tool_use":
                    calls[index] = len(calls)
                    function = {"name": block.get("name", ""), "arguments": ""}
                    yield tool(index, id=block.get("id"), type="function", function=function)
                elif block.get("type") == "text" and block.get("text"):
                    yield chunk({"content": block["text"]})
            elif event == "content_block_delta":
                delta = data.get("delta") or {}
                if delta.get("type") == "text_delta" and delta.get("text"):
                    yield chunk({"content": delta["text"]})
                elif delta.get("type") == "input_json_delta" and delta.get("partial_json") and index in calls:
                    with_arguments.add(index)
                    yield tool(index, function={"arguments": delta["partial_json"]})
            elif event == "content_block_stop":
                if index in calls and index not in with_arguments:  # a call without arguments: still valid JSON
                    yield tool(index, function={"arguments": "{}"})
            elif event == "message_delta":
                stop = (data.get("delta") or {}).get("stop_reason") or stop
                usage = {**usage, **{k: v for k, v in (data.get("usage") or {}).items() if v is not None}}  # totals
            elif event == "message_stop":
                yield chunk({}, FINISH_REASONS.get(stop, "stop"))
                yield chunk(None, usage=_usage(usage))
                yield b"data: [DONE]\n\n"
                return
            elif event == "ping":
                yield b": ping\n\n"
            elif event == "error":
                error = data.get("error") or {}
                status = ERROR_STATUS.get(error.get("type"), 500)
                message = str(error.get("message") or "upstream error")
                yield encode(None, {"error": {"message": message, "type": error.get("type"), "code": status}})
                yield b"data: [DONE]\n\n"
                return


class _Answer(httpx.AsyncByteStream):
    """The body of Claude's response, translated as it's read."""

    def __init__(self, response: httpx.Response, translate: Callable) -> None:
        self.response = response
        self.translate = translate

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.translate(self.response.aiter_raw()):
            yield chunk

    async def aclose(self) -> None:
        await self.response.aclose()


def answer(response: httpx.Response) -> httpx.Response:
    """Claude's response as a chat completion response. An error stays as it is: QueueRouter and the apps read
    ``error.message``, which it has too."""
    if response.status_code != 200:
        return response
    streamed = response.headers.get("content-type", "").startswith("text/event-stream")
    dropped = ("content-length", "content-encoding", "transfer-encoding")  # about the bytes before translation
    headers = [(k, v) for k, v in response.headers.multi_items() if k.lower() not in dropped]
    body = _Answer(response, _chunks if streamed else _whole)
    return httpx.Response(200, headers=headers, stream=body, request=response.request)
