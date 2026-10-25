"""OpenAI Responses API (``POST /v1/responses``) on top of chat completions.

``input`` / ``instructions`` / function ``tools`` / ``text.format`` are translated to a chat
completion; output items and stream events are translated back. ``previous_response_id`` works
for responses stored by QueueRouter (``store``, the default): the conversation is kept encrypted
for a few days and resent, since upstreams are stateless. Hosted tools (web/file search, code
interpreter, computer use...) are not supported.
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Callable

from .sse import encode, events

STORE_DAYS = 7


class Unsupported(ValueError):
    """A request feature the adapter can't translate (answered with a 400)."""


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


# -- request ---------------------------------------------------------------------------------


def _parts(content) -> str | list[dict]:
    if isinstance(content, str):
        return content
    parts: list[dict] = []
    for part in content or []:
        kind = part.get("type") if isinstance(part, dict) else None
        if kind in ("input_text", "output_text", "text"):
            parts.append({"type": "text", "text": part.get("text", "")})
        elif kind == "input_image":
            url = part.get("image_url")
            if not url:
                raise Unsupported("input_image needs an image_url (file ids are not supported)")
            parts.append({"type": "image_url", "image_url": {"url": url, "detail": part.get("detail", "auto")}})
        elif kind == "refusal":
            parts.append({"type": "text", "text": part.get("refusal", "")})
        else:
            raise Unsupported(f"content type {kind!r} is not supported")
    if all(p["type"] == "text" for p in parts):
        return "".join(p["text"] for p in parts)
    return parts


def input_messages(value) -> list[dict]:
    """Responses ``input`` -> chat messages."""
    if isinstance(value, str):
        return [{"role": "user", "content": value}]
    if not isinstance(value, list):
        raise Unsupported("'input' must be a string or a list of items")
    messages: list[dict] = []
    for item in value:
        if not isinstance(item, dict):
            raise Unsupported("each input item must be an object")
        kind = item.get("type", "message")
        if kind == "message":
            role = item.get("role")
            if role == "developer":
                role = "system"
            if role not in ("user", "assistant", "system"):
                raise Unsupported(f"unknown role {item.get('role')!r}")
            messages.append({"role": role, "content": _parts(item.get("content"))})
        elif kind == "function_call":
            call = {
                "id": item.get("call_id") or item.get("id") or new_id("call"),
                "type": "function",
                "function": {"name": item.get("name", ""), "arguments": item.get("arguments") or "{}"},
            }
            last = messages[-1] if messages else None
            if last and last["role"] == "assistant" and not isinstance(last.get("content"), list):
                last.setdefault("tool_calls", []).append(call)
            else:
                messages.append({"role": "assistant", "content": None, "tool_calls": [call]})
        elif kind == "function_call_output":
            output = item.get("output")
            if not isinstance(output, str):
                output = _parts(output) if isinstance(output, list) else json.dumps(output)
                output = output if isinstance(output, str) else json.dumps(output)
            messages.append({"role": "tool", "tool_call_id": item.get("call_id", ""), "content": output})
        elif kind == "reasoning":
            continue
        else:
            raise Unsupported(f"input item type {kind!r} is not supported")
    return messages


def _tool(tool: dict) -> dict:
    if tool.get("type") != "function":
        raise Unsupported(f"tool type {tool.get('type')!r} (hosted tools) is not supported")
    function = {"name": tool.get("name", ""), "parameters": tool.get("parameters") or {"type": "object"}}
    if tool.get("description"):
        function["description"] = tool["description"]
    if tool.get("strict") is not None:
        function["strict"] = tool["strict"]
    return {"type": "function", "function": function}


def to_chat(payload: dict, history: list[dict]) -> tuple[dict, list[dict]]:
    """Responses request -> (chat completion request, this turn's input messages)."""
    if not isinstance(payload, dict):
        raise Unsupported("the body must be a JSON object")
    turn = input_messages(payload.get("input", []))
    messages = []
    if payload.get("instructions"):
        messages.append({"role": "system", "content": payload["instructions"]})
    messages += history + turn
    chat: dict = {"model": payload.get("model"), "messages": messages}
    if payload.get("max_output_tokens") is not None:
        chat["max_tokens"] = payload["max_output_tokens"]
    for name in ("temperature", "top_p", "parallel_tool_calls", "user"):
        if payload.get(name) is not None:
            chat[name] = payload[name]
    if payload.get("tools"):
        chat["tools"] = [_tool(t) for t in payload["tools"]]
    choice = payload.get("tool_choice")
    if isinstance(choice, str):
        chat["tool_choice"] = choice
    elif isinstance(choice, dict) and choice.get("type") == "function":
        chat["tool_choice"] = {"type": "function", "function": {"name": choice.get("name", "")}}
    fmt = (payload.get("text") or {}).get("format") or {}
    if fmt.get("type") == "json_schema":
        chat["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                k: v
                for k, v in {
                    "name": fmt.get("name", "output"),
                    "schema": fmt.get("schema"),
                    "strict": fmt.get("strict"),
                }.items()
                if v is not None
            },
        }
    elif fmt.get("type") == "json_object":
        chat["response_format"] = {"type": "json_object"}
    if payload.get("stream"):
        chat["stream"] = True
        chat["stream_options"] = {"include_usage": True}
    return chat, turn


# -- response --------------------------------------------------------------------------------


def usage_of(usage: dict | None) -> dict:
    usage = usage or {}
    prompt, completion = usage.get("prompt_tokens") or 0, usage.get("completion_tokens") or 0
    return {
        "input_tokens": prompt,
        "input_tokens_details": {"cached_tokens": (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0},
        "output_tokens": completion,
        "output_tokens_details": {
            "reasoning_tokens": (usage.get("completion_tokens_details") or {}).get("reasoning_tokens") or 0
        },
        "total_tokens": prompt + completion,
    }


def envelope(payload: dict, response_id: str, model: str, status: str, output: list, usage: dict | None) -> dict:
    fmt = (payload.get("text") or {}).get("format") or {"type": "text"}
    return {
        "id": response_id,
        "object": "response",
        "created_at": int(time.time()),
        "status": status,
        "error": None,
        "incomplete_details": {"reason": "max_output_tokens"} if status == "incomplete" else None,
        "instructions": payload.get("instructions"),
        "max_output_tokens": payload.get("max_output_tokens"),
        "model": model,
        "output": output,
        "parallel_tool_calls": payload.get("parallel_tool_calls", True),
        "previous_response_id": payload.get("previous_response_id"),
        "reasoning": {"effort": None, "summary": None},
        "store": payload.get("store", True),
        "temperature": payload.get("temperature"),
        "text": {"format": fmt},
        "tool_choice": payload.get("tool_choice", "auto"),
        "tools": payload.get("tools", []),
        "top_p": payload.get("top_p"),
        "truncation": "disabled",
        "usage": usage,
        "user": payload.get("user"),
        "metadata": payload.get("metadata") or {},
    }


def _message_item(item_id: str, text: str, status: str = "completed") -> dict:
    return {
        "type": "message",
        "id": item_id,
        "status": status,
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}] if status == "completed" else [],
    }


def _call_item(item_id: str, call_id: str, name: str, arguments: str, status: str = "completed") -> dict:
    return {
        "type": "function_call",
        "id": item_id,
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
        "status": status,
    }


def assistant_message(output: list[dict]) -> dict:
    """The chat message equivalent of a response's output (stored for previous_response_id)."""
    text = "".join(c["text"] for item in output if item["type"] == "message" for c in item["content"])
    calls = [
        {"id": i["call_id"], "type": "function", "function": {"name": i["name"], "arguments": i["arguments"]}}
        for i in output
        if i["type"] == "function_call"
    ]
    message: dict = {"role": "assistant", "content": text or None}
    if calls:
        message["tool_calls"] = calls
    return message


def from_chat(body: dict, payload: dict, response_id: str, model: str) -> dict:
    choice = (body.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    output: list[dict] = []
    text = message.get("content")
    if isinstance(text, list):
        text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
    if text:
        output.append(_message_item(new_id("msg"), text))
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        output.append(
            _call_item(
                new_id("fc"),
                call.get("id") or new_id("call"),
                function.get("name", ""),
                function.get("arguments") or "{}",
            )
        )
    status = "incomplete" if choice.get("finish_reason") == "length" else "completed"
    return envelope(payload, response_id, model, status, output, usage_of(body.get("usage")))


async def stream(
    body: AsyncIterator[bytes],
    payload: dict,
    response_id: str,
    model: str,
    on_done: Callable[[dict], None],
) -> AsyncIterator[bytes]:
    """Chat completion chunks -> Responses stream events. ``on_done(response)`` at the end."""
    seq = 0

    def event(kind: str, data: dict) -> bytes:
        nonlocal seq
        seq += 1
        return encode(kind, {"type": kind, "sequence_number": seq, **data})

    output: list[dict] = []
    text_item: dict | None = None
    text = ""
    calls: dict[int, dict] = {}  # openai tool index -> item (arguments accumulated)
    finish = None
    usage = None
    started = False

    def text_done() -> list[bytes]:
        nonlocal text_item, text
        if text_item is None:
            return []
        index = output.index(text_item)
        text_item.update(_message_item(text_item["id"], text))
        item_id, done = text_item["id"], text_item
        text_item = None
        finished, text = text, ""  # a later message item (after a tool call) starts its own text
        part = {"type": "output_text", "text": finished, "annotations": []}
        return [
            event(
                "response.output_text.done",
                {"item_id": item_id, "output_index": index, "content_index": 0, "text": finished},
            ),
            event(
                "response.content_part.done",
                {"item_id": item_id, "output_index": index, "content_index": 0, "part": part},
            ),
            event("response.output_item.done", {"output_index": index, "item": done}),
        ]

    async with contextlib.aclosing(events(body)) as stream_events:
        async for kind, data in stream_events:
            if kind == "comment":
                yield f": {data}\n\n".encode()  # keep-alive while queued
                continue
            if kind == "done":
                break
            if not isinstance(data, dict):
                continue
            if not started:
                started = True
                shell = envelope(payload, response_id, model, "in_progress", [], None)
                yield event("response.created", {"response": shell})
                yield event("response.in_progress", {"response": shell})
            if "error" in data:
                err = data["error"] if isinstance(data["error"], dict) else {"message": str(data["error"])}
                failed = envelope(payload, response_id, model, "failed", output, None)
                failed["error"] = {"code": str(err.get("type") or "server_error"), "message": err.get("message", "")}
                yield event("response.failed", {"response": failed})
                return
            if data.get("usage"):
                usage = data["usage"]
            for choice in data.get("choices") or []:
                delta = choice.get("delta") or {}
                if delta.get("content"):
                    if text_item is None:
                        text_item = _message_item(new_id("msg"), "", "in_progress")
                        output.append(text_item)
                        index = len(output) - 1
                        yield event("response.output_item.added", {"output_index": index, "item": text_item})
                        yield event(
                            "response.content_part.added",
                            {
                                "item_id": text_item["id"],
                                "output_index": index,
                                "content_index": 0,
                                "part": {"type": "output_text", "text": "", "annotations": []},
                            },
                        )
                    text += delta["content"]
                    yield event(
                        "response.output_text.delta",
                        {
                            "item_id": text_item["id"],
                            "output_index": output.index(text_item),
                            "content_index": 0,
                            "delta": delta["content"],
                        },
                    )
                for call in delta.get("tool_calls") or []:
                    index = call.get("index", 0)
                    function = call.get("function") or {}
                    if index not in calls:
                        for chunk in text_done():
                            yield chunk
                        item = _call_item(
                            new_id("fc"), call.get("id") or new_id("call"), function.get("name", ""), "", "in_progress"
                        )
                        calls[index] = item
                        output.append(item)
                        yield event("response.output_item.added", {"output_index": len(output) - 1, "item": item})
                    if function.get("arguments"):
                        item = calls[index]
                        item["arguments"] += function["arguments"]
                        yield event(
                            "response.function_call_arguments.delta",
                            {"item_id": item["id"], "output_index": output.index(item), "delta": function["arguments"]},
                        )
                if choice.get("finish_reason"):
                    finish = choice["finish_reason"]
    if not started:
        shell = envelope(payload, response_id, model, "in_progress", [], None)
        yield event("response.created", {"response": shell})
    for chunk in text_done():
        yield chunk
    for item in calls.values():
        item["status"] = "completed"
        index = output.index(item)
        yield event(
            "response.function_call_arguments.done",
            {"item_id": item["id"], "output_index": index, "arguments": item["arguments"]},
        )
        yield event("response.output_item.done", {"output_index": index, "item": item})
    status = "incomplete" if finish == "length" else "completed"
    final = envelope(payload, response_id, model, status, output, usage_of(usage))
    on_done(final)
    yield event(f"response.{'incomplete' if status == 'incomplete' else 'completed'}", {"response": final})
