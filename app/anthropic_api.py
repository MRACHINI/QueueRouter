"""Anthropic Messages API (``POST /v1/messages``) on top of OpenAI chat completions.

Requests are translated to a chat completion and go through the normal pipeline (queue,
policies, aliases, fallbacks, usage); responses and streams are translated back.
Not supported: server tools (web search...), PDF/file documents. Thinking settings and
cache_control markers are ignored.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from collections.abc import AsyncIterator

from .sse import encode, events

STOP_REASONS = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "content_filter": "refusal",
}
ERROR_TYPES = {
    400: "invalid_request_error",
    401: "authentication_error",
    403: "permission_error",
    404: "not_found_error",
    413: "request_too_large",
    429: "rate_limit_error",
    503: "overloaded_error",
    529: "overloaded_error",
}


class Unsupported(ValueError):
    """A request feature the adapter can't translate (answered with a 400)."""


def error(status: int, message: str) -> dict:
    return {"type": "error", "error": {"type": ERROR_TYPES.get(status, "api_error"), "message": message}}


def from_openai_error(status: int, body: bytes) -> dict:
    try:
        message = json.loads(body)["error"]["message"]
    except (ValueError, KeyError, TypeError):
        message = body.decode("utf-8", "replace")[:500] or f"HTTP {status}"
    return error(status, message)


# -- request ---------------------------------------------------------------------------------


def _text_of(content) -> str:
    if isinstance(content, str):
        return content
    parts = []
    for block in content or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts)


def _image(block: dict) -> dict:
    source = block.get("source") or {}
    if source.get("type") == "base64":
        url = f"data:{source.get('media_type', 'image/png')};base64,{source.get('data', '')}"
    elif source.get("type") == "url":
        url = source.get("url", "")
    else:
        raise Unsupported("image source type not supported")
    return {"type": "image_url", "image_url": {"url": url}}


def _document(block: dict) -> dict:
    source = block.get("source") or {}
    if source.get("type") == "text":
        return {"type": "text", "text": source.get("data", "")}
    if source.get("type") == "content":
        return {"type": "text", "text": _text_of(source.get("content"))}
    raise Unsupported("only plain-text documents are supported (PDFs and files are not)")


def _convert_message(message: dict) -> list[dict]:
    role = message.get("role")
    content = message.get("content")
    if role not in ("user", "assistant"):
        raise Unsupported(f"unknown role {role!r}")
    if isinstance(content, str):
        return [{"role": role, "content": content}]
    out: list[dict] = []
    parts: list[dict] = []
    tool_calls: list[dict] = []
    for block in content or []:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind == "text":
            parts.append({"type": "text", "text": block.get("text", "")})
        elif kind == "image":
            parts.append(_image(block))
        elif kind == "document":
            parts.append(_document(block))
        elif kind == "tool_use":
            tool_calls.append(
                {
                    "id": block.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                    "type": "function",
                    "function": {"name": block.get("name", ""), "arguments": json.dumps(block.get("input") or {})},
                }
            )
        elif kind == "tool_result":
            text = _text_of(block.get("content")) if not isinstance(block.get("content"), str) else block["content"]
            parts = block.get("content") if isinstance(block.get("content"), list) else []
            if any(isinstance(p, dict) and p.get("type") == "image" for p in parts):
                # chat tool results carry text only: say so, rather than losing the image silently
                text += "\n[An image in this tool result was left out: this model API can't receive it.]"
            if block.get("is_error"):
                text = f"Error: {text}"
            out.append({"role": "tool", "tool_call_id": block.get("tool_use_id", ""), "content": text})
        elif kind in ("thinking", "redacted_thinking"):
            continue  # can't be replayed to another model
        else:
            raise Unsupported(f"content block type {kind!r} is not supported")
    if role == "assistant":
        text = "".join(p.get("text", "") for p in parts if p["type"] == "text")
        msg: dict = {"role": "assistant", "content": text or None}
        if tool_calls:
            msg["tool_calls"] = tool_calls
        if text or tool_calls:
            out.append(msg)
    elif parts:
        only_text = all(p["type"] == "text" for p in parts)
        out.append({"role": "user", "content": "\n".join(p["text"] for p in parts) if only_text else parts})
    return out


def _tool(tool: dict) -> dict:
    if tool.get("type") not in (None, "custom") or "input_schema" not in tool:
        raise Unsupported(f"tool type {tool.get('type')!r} (server tools) is not supported")
    function = {"name": tool["name"], "parameters": tool.get("input_schema") or {"type": "object"}}
    if tool.get("description"):
        function["description"] = tool["description"]
    return {"type": "function", "function": function}


def to_chat(payload: dict) -> dict:
    """Anthropic Messages request -> OpenAI chat completions request."""
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise Unsupported("'messages' is required")
    messages: list[dict] = []
    system = payload.get("system")
    if system:
        messages.append({"role": "system", "content": _text_of(system)})
    for message in payload["messages"]:
        if not isinstance(message, dict):
            raise Unsupported("each message must be an object")
        messages.extend(_convert_message(message))
    chat: dict = {"model": payload.get("model"), "messages": messages}
    if payload.get("max_tokens") is not None:
        chat["max_tokens"] = payload["max_tokens"]
    for name in ("temperature", "top_p", "top_k"):
        if payload.get(name) is not None:
            chat[name] = payload[name]
    if payload.get("stop_sequences"):
        chat["stop"] = payload["stop_sequences"]
    if payload.get("tools"):
        chat["tools"] = [_tool(t) for t in payload["tools"]]
    choice = payload.get("tool_choice")
    if isinstance(choice, dict):
        kind = choice.get("type")
        if kind == "auto":
            chat["tool_choice"] = "auto"
        elif kind == "any":
            chat["tool_choice"] = "required"
        elif kind == "none":
            chat["tool_choice"] = "none"
        elif kind == "tool":
            chat["tool_choice"] = {"type": "function", "function": {"name": choice.get("name", "")}}
        if choice.get("disable_parallel_tool_use"):
            chat["parallel_tool_calls"] = False
    user = (payload.get("metadata") or {}).get("user_id")
    if user:
        chat["user"] = str(user)
    if payload.get("stream"):
        chat["stream"] = True
        chat["stream_options"] = {"include_usage": True}
    return chat


# -- response --------------------------------------------------------------------------------


def _usage(usage: dict | None) -> dict:
    usage = usage or {}
    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
    result = {
        "input_tokens": max(0, (usage.get("prompt_tokens") or 0) - cached),
        "output_tokens": usage.get("completion_tokens") or 0,
    }
    if cached:
        result["cache_read_input_tokens"] = cached
    return result


def _arguments(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(raw or "{}")
    except ValueError:
        return {"_raw": raw}
    return value if isinstance(value, dict) else {"value": value}


def from_chat(body: dict, model: str) -> dict:
    """OpenAI chat completion -> Anthropic Message."""
    choice = (body.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content: list[dict] = []
    text = message.get("content")
    if isinstance(text, list):
        text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
    if text:
        content.append({"type": "text", "text": text})
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        content.append(
            {
                "type": "tool_use",
                "id": call.get("id") or f"toolu_{uuid.uuid4().hex[:24]}",
                "name": function.get("name", ""),
                "input": _arguments(function.get("arguments")),
            }
        )
    return {
        "id": f"msg_{(body.get('id') or uuid.uuid4().hex)}".replace("chatcmpl-", ""),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        # some servers say "stop" even when the reply calls tools: Anthropic clients run tools only on "tool_use"
        "stop_reason": "tool_use"
        if any(block["type"] == "tool_use" for block in content)
        else STOP_REASONS.get(choice.get("finish_reason") or "stop", "end_turn"),
        "stop_sequence": None,
        "usage": _usage(body.get("usage")),
    }


async def stream(body: AsyncIterator[bytes], model: str) -> AsyncIterator[bytes]:
    """OpenAI chat completion chunks -> Anthropic stream events."""
    message_id = f"msg_{uuid.uuid4().hex[:24]}"
    started = False
    block_index = -1
    open_block: str | None = None  # "text" or "tool:<openai index>"
    tool_blocks: dict[int, int] = {}  # openai tool index -> our block index
    finish = None
    usage: dict = {}

    def start() -> bytes:
        return encode(
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                },
            },
        )

    def close() -> bytes:
        nonlocal open_block
        if open_block is None:
            return b""
        open_block = None
        return encode("content_block_stop", {"type": "content_block_stop", "index": block_index})

    async with contextlib.aclosing(events(body)) as stream_events:
        async for kind, data in stream_events:
            if kind == "comment":
                yield encode("ping", {"type": "ping"})  # waiting in the queue
                continue
            if kind == "done":
                break
            if not isinstance(data, dict):
                continue
            if "error" in data:
                err = data["error"] if isinstance(data["error"], dict) else {"message": str(data["error"])}
                code = err.get("code")
                status = code if isinstance(code, int) else int(code) if str(code).isdigit() else 500
                yield encode("error", error(status, err.get("message", "upstream error")))
                return
            if not started:
                started = True
                yield start()
            if data.get("usage"):
                usage = data["usage"]
            for choice in data.get("choices") or []:
                delta = choice.get("delta") or {}
                text = delta.get("content")
                if text:
                    if open_block != "text":
                        yield close()
                        block_index += 1
                        open_block = "text"
                        yield encode(
                            "content_block_start",
                            {
                                "type": "content_block_start",
                                "index": block_index,
                                "content_block": {"type": "text", "text": ""},
                            },
                        )
                    yield encode(
                        "content_block_delta",
                        {
                            "type": "content_block_delta",
                            "index": block_index,
                            "delta": {"type": "text_delta", "text": text},
                        },
                    )
                for call in delta.get("tool_calls") or []:
                    index = call.get("index", 0)
                    function = call.get("function") or {}
                    if index not in tool_blocks:
                        yield close()
                        block_index += 1
                        tool_blocks[index] = block_index
                        open_block = f"tool:{index}"
                        yield encode(
                            "content_block_start",
                            {
                                "type": "content_block_start",
                                "index": block_index,
                                "content_block": {
                                    "type": "tool_use",
                                    "id": call.get("id") or f"toolu_{uuid.uuid4().hex[:24]}",
                                    "name": function.get("name", ""),
                                    "input": {},
                                },
                            },
                        )
                    if function.get("arguments"):
                        yield encode(
                            "content_block_delta",
                            {
                                "type": "content_block_delta",
                                "index": tool_blocks[index],
                                "delta": {"type": "input_json_delta", "partial_json": function["arguments"]},
                            },
                        )
                if choice.get("finish_reason"):
                    finish = choice["finish_reason"]
    if not started:
        yield start()
    yield close()
    yield encode(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {
                "stop_reason": "tool_use" if tool_blocks else STOP_REASONS.get(finish or "stop", "end_turn"),
                "stop_sequence": None,
            },
            "usage": _usage(usage),
        },
    )
    yield encode("message_stop", {"type": "message_stop"})
