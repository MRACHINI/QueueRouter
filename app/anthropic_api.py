"""Anthropic Messages API (``POST /v1/messages``) on top of OpenAI chat completions.

Requests are translated to a chat completion and go through the normal pipeline (queue,
policies, aliases, fallbacks, usage); responses and streams are translated back.
System messages inside ``messages`` (Claude Code sends them) stay where they are, as a reminder in the user's turn
(``_remind``), and the chat completions forms some apps mix in (tool and function messages, tool_calls,
input_text and image_url parts) are taken as they are.
The effort asked for travels beside the request (``effort_of``): QueueRouter passes it on only to a model whose
thinking levels it knows. The model's reasoning comes back as thinking blocks when the app asks for thinking.
Claude's web search tool is run by QueueRouter with its own search (app/main.py). Not supported: other server tools,
PDF/file documents (in a tool result, a note says so). cache_control markers are ignored.
"""

from __future__ import annotations

import base64
import contextlib
import json
import re
import time
import uuid
from collections.abc import AsyncIterator
from urllib.parse import urlsplit

from . import websearch
from .limits import overflow_of
from .sse import arguments_text, encode, events, tool_call_delta

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
TEXT_KINDS = ("text", "input_text", "output_text")  # the last two: OpenAI Responses' names, which some apps send
PING_EVERY_S = 5.0  # while a model reasons and the app didn't ask to see it
IMAGE_NOTE = "[An image in this tool result was left out: this model API can't receive it.]"


class Unsupported(ValueError):
    """A request feature the adapter can't translate (answered with a 400)."""


def error(status: int, message: str) -> dict:
    return {"type": "error", "error": {"type": ERROR_TYPES.get(status, "api_error"), "message": message}}


def from_openai_error(status: int, body: bytes) -> dict:
    try:
        message = str(json.loads(body)["error"]["message"])
    except (ValueError, KeyError, TypeError):
        message = body.decode("utf-8", "replace")[:500] or f"HTTP {status}"
    overflow, limit, _ = overflow_of(status, body)
    return error(status, too_long(message, limit) if overflow else message)


def too_long(message: str, limit: int | None) -> str:
    """An overflow error in Claude's words ("prompt is too long: N tokens > M maximum"): Claude Code answers those by
    compacting the conversation and trying again, which it doesn't for other wordings."""
    if message.lower().startswith("prompt is too long"):
        return message
    sizes = [int(n.replace(",", "")) for n in re.findall(r"(\d[\d,]*) tokens", message)]
    needed = max((n for n in sizes if limit and n > limit), default=None)
    return f"prompt is too long: {needed} tokens > {limit} maximum" if needed else f"prompt is too long: {message}"


# -- request ---------------------------------------------------------------------------------


def _text_of(content) -> str:
    if isinstance(content, str):
        return content
    parts = []
    for block in content or []:
        if isinstance(block, dict) and block.get("type") in TEXT_KINDS:
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


def _image_url(block: dict) -> dict:
    """An image part in chat completions' form (``image_url``) or OpenAI Responses' (``input_image``)."""
    image = block.get("image_url")
    if isinstance(image, dict) and image.get("url"):
        return {"type": "image_url", "image_url": image}
    if isinstance(image, str) and image:
        return {"type": "image_url", "image_url": {"url": image}}
    raise Unsupported(f"{block.get('type')} needs an image_url (file ids are not supported)")


def _search_result(block: dict) -> str:
    """A search result block (title, source, its text) as plain text."""
    head = " - ".join(str(block[key]) for key in ("title", "source") if block.get(key))
    return "\n".join(text for text in (head, _text_of(block.get("content"))) if text)


def _tool_result_text(content) -> str:
    """A tool result as the text of a chat tool message. What text can't carry (images, PDFs) is replaced by a note
    rather than lost silently; the tools a tool search found are named (every tool is offered anyway)."""
    if isinstance(content, str):
        return content
    pieces: list[str] = []
    for part in content or []:
        kind = part.get("type") if isinstance(part, dict) else None
        if kind in TEXT_KINDS:
            pieces.append(part.get("text", ""))
        elif kind == "search_result":
            pieces.append(_search_result(part))
        elif kind == "tool_reference":
            pieces.append(f"[Tool available: {part.get('tool_name') or part.get('name') or '?'}]")
        elif kind == "document" and (part.get("source") or {}).get("type") in ("text", "content"):
            pieces.append(_document(part)["text"])
        elif kind == "image":
            pieces.append(IMAGE_NOTE)
        else:
            pieces.append(f"[A {kind or 'part'} in this tool result was left out: this model API can't receive it.]")
    return "\n".join(pieces)


def _convert_message(message: dict) -> list[dict]:
    role = message.get("role")
    content = message.get("content")
    if role in ("tool", "function"):  # chat completions' own forms, which some apps mix in: taken as they are
        kept = {"role": role, "content": _tool_result_text(content)}
        kept.update({key: message[key] for key in ("tool_call_id", "name") if isinstance(message.get(key), str)})
        return [kept]
    if role not in ("user", "assistant"):
        raise Unsupported(f"unknown role {role!r}")
    # an assistant's tool calls in chat completions' form are kept as they are too
    calls = [c for c in message.get("tool_calls") or [] if isinstance(c, dict)] if role == "assistant" else []
    legacy = message.get("function_call") if role == "assistant" else None
    if isinstance(content, str) and not calls and not isinstance(legacy, dict):
        return [{"role": role, "content": content}]
    out: list[dict] = []
    parts: list[dict] = []
    tool_calls: list[dict] = list(calls)
    for block in [{"type": "text", "text": content}] if isinstance(content, str) else content or []:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind in TEXT_KINDS:
            parts.append({"type": "text", "text": block.get("text", "")})
        elif kind == "image":
            parts.append(_image(block))
        elif kind in ("image_url", "input_image"):
            parts.append(_image_url(block))
        elif kind == "document":
            parts.append(_document(block))
        elif kind == "search_result":
            parts.append({"type": "text", "text": _search_result(block)})
        elif kind == "tool_use" or (kind == "server_tool_use" and role == "assistant"):  # a search, then its result
            tool_calls.append(
                {
                    "id": block.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                    "type": "function",
                    "function": {"name": block.get("name", ""), "arguments": json.dumps(block.get("input") or {})},
                }
            )
        elif kind == "web_search_tool_result" and role == "assistant":  # the turn so far, then what the search found
            out += _assistant_turn(parts, tool_calls, legacy)
            parts, tool_calls, legacy = [], [], None
            out.append({"role": "tool", "tool_call_id": block.get("tool_use_id", ""), "content": _search_text(block)})
        elif kind == "tool_result":
            text = _tool_result_text(block.get("content"))  # chat tool results carry text only
            if block.get("is_error"):
                text = f"Error: {text}"
            out.append({"role": "tool", "tool_call_id": block.get("tool_use_id", ""), "content": text})
        elif kind in ("thinking", "redacted_thinking"):
            continue  # can't be replayed to another model
        else:
            raise Unsupported(f"content block type {kind!r} is not supported")
    if role == "assistant":
        out += _assistant_turn(parts, tool_calls, legacy)
    elif parts:
        only_text = all(p["type"] == "text" for p in parts)
        out.append({"role": "user", "content": "\n".join(p["text"] for p in parts) if only_text else parts})
    return out


def _assistant_turn(parts: list[dict], tool_calls: list[dict], legacy) -> list[dict]:
    """An assistant's text and tool calls as one chat message (none if it has neither)."""
    text = "".join(p.get("text", "") for p in parts if p["type"] == "text")
    msg: dict = {"role": "assistant", "content": text or None}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    if isinstance(legacy, dict):
        msg["function_call"] = legacy
    return [msg] if text or tool_calls or isinstance(legacy, dict) else []


def search_function(tool: dict) -> dict:
    """Claude's web search tool as a function the model calls, under the tool's own name: QueueRouter runs it."""
    function = websearch.TOOL["function"]
    return {"type": "function", "function": {**function, "name": str(tool.get("name") or "web_search")}}


def _tool(tool: dict) -> dict:
    if tool.get("type") not in (None, "custom") or "input_schema" not in tool:
        raise Unsupported(f"tool type {tool.get('type')!r} (server tools) is not supported")
    function = {"name": tool["name"], "parameters": tool.get("input_schema") or {"type": "object"}}
    if tool.get("description"):
        function["description"] = tool["description"]
    return {"type": "function", "function": function}


def _system_content(content) -> tuple[str, list[tuple[str, str]]]:
    """A system message's text, and the tool changes it carries: (tool_addition or tool_removal, the tool's name)."""
    if isinstance(content, str):
        return content, []
    texts, changes = [], []
    for block in content or []:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind in TEXT_KINDS:
            texts.append(block.get("text", ""))
        elif kind in ("tool_addition", "tool_removal") and isinstance(block.get("tool"), dict):
            changes.append((kind, str(block["tool"].get("name") or "")))
        else:
            raise Unsupported(f"content block type {kind!r} is not supported in a system message")
    return "\n".join(text for text in texts if text), changes


def _remind(messages: list[dict], text: str) -> None:
    """A system message from during the conversation, where it is, as Claude Code itself sends one where they can't
    go: a reminder in the user's turn. Chat templates of many open models refuse a system message after the start,
    and moving it to the top would change the start of every request (no cache)."""
    note = f"<system-reminder>\n{text}\n</system-reminder>"
    last = messages[-1] if messages else None
    if last is None or last["role"] != "user":  # e.g. after tool results
        messages.append({"role": "user", "content": note})
    elif isinstance(last["content"], str):
        last["content"] = f"{last['content']}\n\n{note}"
    else:
        last["content"] = [*last["content"], {"type": "text", "text": note}]


def to_chat(payload: dict) -> dict:
    """Anthropic Messages request -> OpenAI chat completions request."""
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise Unsupported("'messages' is required")
    messages: list[dict] = []
    system = payload.get("system")
    if system:
        messages.append({"role": "system", "content": _text_of(system)})
    removed: set[str] = set()  # tools a system message took away (and none brought back)
    users = [i for i, m in enumerate(payload["messages"]) if isinstance(m, dict) and m.get("role") == "user"]
    for i, message in enumerate(payload["messages"]):
        if not isinstance(message, dict):
            raise Unsupported("each message must be an object")
        if message.get("role") in ("system", "developer"):
            if message.get("clear_at") == "next_user_message" and users and i < users[-1]:
                continue  # a reminder for one turn, cleared since
            text, changes = _system_content(message.get("content"))
            for kind, name in changes:
                (removed.add if kind == "tool_removal" else removed.discard)(name)
            if text and all(m["role"] == "system" for m in messages):  # before the conversation: the system prompt
                if messages:
                    messages[0]["content"] += f"\n{text}"
                else:
                    messages.append({"role": "system", "content": text})
            elif text:  # (none: e.g. a message that only sets the effort, see effort_of)
                _remind(messages, text)
            continue
        messages.extend(_convert_message(message))
    chat: dict = {"model": payload.get("model"), "messages": messages}
    if payload.get("max_tokens") is not None:
        chat["max_tokens"] = payload["max_tokens"]
    for name in ("temperature", "top_p", "top_k"):
        if payload.get(name) is not None:
            chat[name] = payload[name]
    if payload.get("stop_sequences"):
        chat["stop"] = payload["stop_sequences"]
    search = search_tool(payload)
    tools = [
        search_function(t) if t is search else _tool(t)
        for t in payload.get("tools") or []
        if not (isinstance(t, dict) and t.get("name") in removed)
    ]
    if tools:
        chat["tools"] = tools
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
    config = payload.get("output_config") if isinstance(payload.get("output_config"), dict) else {}
    answer_format = config.get("format") or payload.get("output_format")  # the second: the older beta's place
    if isinstance(answer_format, dict) and isinstance(answer_format.get("schema"), dict):  # structured outputs
        schema = {"name": "output", "schema": answer_format["schema"], "strict": True}
        chat["response_format"] = {"type": "json_schema", "json_schema": schema}
    user = (payload.get("metadata") or {}).get("user_id")
    if user:
        chat["user"] = str(user)
    if payload.get("stream"):
        chat["stream"] = True
        chat["stream_options"] = {"include_usage": True}
    return chat


def effort_of(payload: dict) -> str | None:
    """How hard the app asked the model to think: ``output_config.effort``, or the last system message that changes
    it (Claude Code's per-turn setting). It isn't put in the chat request: QueueRouter passes it on as the nearest
    thinking level of a model whose levels it knows, and to no other (app/main.py)."""
    systems = [m for m in payload.get("messages") or [] if isinstance(m, dict) and m.get("role") == "system"]
    effort = None
    for holder in (payload, *systems):
        config = holder.get("output_config")
        if isinstance(config, dict) and isinstance(config.get("effort"), str):
            effort = config["effort"].lower()
    return effort


# -- response --------------------------------------------------------------------------------


def _usage(usage: dict | None) -> dict:
    usage = usage or {}
    details = usage.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens") or 0
    written = details.get("cache_write_tokens") or 0
    result = {
        "input_tokens": max(0, (usage.get("prompt_tokens") or 0) - cached - written),
        "output_tokens": usage.get("completion_tokens") or 0,
    }
    if cached:
        result["cache_read_input_tokens"] = cached
    if written:
        result["cache_creation_input_tokens"] = written
    return result


def tool_input(raw) -> dict:
    """A tool call's arguments (JSON text in chat completions) as the object the Messages format has."""
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {"_raw": raw}
    return value if isinstance(value, dict) else {"value": value}


def search_blocks(name: str, query: str, results: list[dict] | None, failure: str = "") -> tuple[dict, dict]:
    """A web search QueueRouter ran, as Claude's server tool blocks: (server_tool_use, web_search_tool_result).
    A result's snippet travels in ``encrypted_content`` (opaque to apps, which send it back with the history), so
    a later turn still has what the model read. ``failure``: Claude's error code instead of results."""
    call_id = f"srvtoolu_{uuid.uuid4().hex[:24]}"
    call = {"type": "server_tool_use", "id": call_id, "name": name, "input": {"query": query}}
    if failure:
        content: list[dict] | dict = {"type": "web_search_tool_result_error", "error_code": failure}
    else:
        content = [
            {
                "type": "web_search_result",
                "url": str(r.get("url") or ""),
                "title": str(r.get("title") or ""),
                "encrypted_content": base64.b64encode(str(r.get("snippet") or "").encode()).decode(),
                "page_age": None,
            }
            for r in results or []
        ]
    return call, {"type": "web_search_tool_result", "tool_use_id": call_id, "content": content}


def _search_text(block: dict) -> str:
    """A web_search_tool_result block from the history, as the text the model read then."""
    content = block.get("content")
    if isinstance(content, dict):
        return f"Web search failed: {content.get('error_code') or 'error'}"
    lines = []
    for n, item in enumerate((i for i in content or [] if isinstance(i, dict)), 1):
        try:
            snippet = base64.b64decode(str(item.get("encrypted_content") or ""), validate=True).decode()
        except ValueError:
            snippet = ""  # Claude's own (encrypted): not readable here
        snippet = " ".join(snippet.split())[:500]
        lines.append(f"{n}. {item.get('title') or '(no title)'} - {item.get('url')}\n   {snippet}")
    return "\n".join(lines) or "No results."


def within_domains(results: list[dict], tool: dict) -> list[dict]:
    """Search results limited as Claude's web search tool says (``allowed_domains``, ``blocked_domains``)."""

    def domains(key: str) -> list[str]:
        return [str(d).lower().lstrip(".") for d in tool.get(key) or [] if isinstance(d, str) and d.strip()]

    def under(host: str, names: list[str]) -> bool:
        return any(host == name or host.endswith(f".{name}") for name in names)

    allowed, blocked = domains("allowed_domains"), domains("blocked_domains")
    kept = []
    for result in results:
        host = (urlsplit(str(result.get("url") or "")).hostname or "").lower()
        if (not allowed or under(host, allowed)) and not under(host, blocked):
            kept.append(result)
    return kept


def search_tool(payload: dict) -> dict | None:
    """Claude's web search server tool in the request, if any (QueueRouter runs the searches: app/main.py)."""
    for tool in payload.get("tools") or []:
        if isinstance(tool, dict) and str(tool.get("type") or "").startswith("web_search_"):
            return tool
    return None


def used_ids(payload: dict) -> set[str]:
    """The tool call ids already in the conversation (a new call must not reuse one)."""
    ids: set[str] = set()
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        ids.update(str(c.get("id")) for c in message.get("tool_calls") or [] if isinstance(c, dict) and c.get("id"))
        content = message.get("content")
        for block in content if isinstance(content, list) else []:
            if isinstance(block, dict) and block.get("type") in ("tool_use", "server_tool_use") and block.get("id"):
                ids.add(str(block["id"]))
    return ids


def wants_thinking(payload: dict) -> bool:
    """Whether the app asked for thinking (``thinking`` enabled or adaptive). Only then does the model's reasoning
    come back as thinking blocks: an app that didn't ask may expect the text first."""
    setting = payload.get("thinking")
    return isinstance(setting, dict) and setting.get("type") in ("enabled", "adaptive")


def _reasoning(holder: dict) -> str:
    """A message's or a delta's reasoning text, as providers name it (vLLM, DeepSeek: reasoning_content)."""
    found = holder.get("reasoning_content") or holder.get("reasoning")
    return found if isinstance(found, str) else ""


def chat_calls(body: dict) -> list[dict]:
    """A whole chat completion's tool calls as {id, name, arguments} (arguments as JSON text)."""
    message = (body.get("choices") or [{}])[0].get("message") or {}
    calls = []
    for call in message.get("tool_calls") or []:
        if not isinstance(call, dict):
            continue
        function = call.get("function") if isinstance(call.get("function"), dict) else {}
        name, arguments = str(function.get("name") or ""), arguments_text(function.get("arguments"))
        calls.append({"id": str(call.get("id") or ""), "name": name, "arguments": arguments})
    return calls


def from_chat(body: dict, model: str, thinking: bool = False, seen: set[str] | None = None) -> dict:
    """OpenAI chat completion -> Anthropic Message (its reasoning as a thinking block, if ``thinking``). A tool call
    whose id is in ``seen`` (earlier in the conversation) gets a new one, as in Translation."""
    choice = (body.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content: list[dict] = []
    if thinking and (reasoning := _reasoning(message)):
        content.append({"type": "thinking", "thinking": reasoning, "signature": ""})
    text = message.get("content")
    if isinstance(text, list):
        text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
    if text:
        content.append({"type": "text", "text": text})
    seen = set(seen or ())
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        call_id = str(call.get("id") or "")
        if not call_id or call_id in seen:
            call_id = f"toolu_{uuid.uuid4().hex[:24]}"
        seen.add(call_id)
        content.append(
            {
                "type": "tool_use",
                "id": call_id,
                "name": function.get("name", ""),
                "input": tool_input(function.get("arguments")),
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
        if any(block["type"] == "tool_use" for block in content) and choice.get("finish_reason") != "length"
        else STOP_REASONS.get(choice.get("finish_reason") or "stop", "end_turn"),
        "stop_sequence": None,
        "usage": _usage(body.get("usage")),
    }


class Translation:
    """One Claude message's stream events, from one chat completion stream or several (QueueRouter runs a web search
    between them). The model's reasoning comes as thinking blocks if ``thinking`` (the app asked for it), else as a
    ping now and then: silent, a stream looks stalled, and Claude Code gives up on it after about five minutes.

    Tool calls are held until the stream ends, then sent whole: some providers send the arguments of parallel calls
    interleaved, while Claude's blocks come one after another and an app runs a tool as its block ends. A call whose
    id came earlier in the conversation (``seen``: some providers count from 0 in every answer) gets a new one, so
    the app can't match a result to the wrong call."""

    def __init__(self, model: str, thinking: bool = False, seen: set[str] | None = None) -> None:
        self.model, self.thinking, self.seen = model, thinking, set(seen or ())
        self.message_id = f"msg_{uuid.uuid4().hex[:24]}"
        self.started = False
        self.failed = False  # an error event went out: the message ends there
        self.pinged = 0.0  # when the last ping for work that isn't shown went
        self.index = -1
        self.open: str | None = None  # the kind of the open block
        self.calls: dict[int, dict] = {}  # this stream's tool calls by the provider's index: id, name, arguments
        self.text = ""  # this stream's answer text
        self.finish: str | None = None
        self.usage: dict = {}
        self.tools_sent = False
        self.searches = 0  # web searches QueueRouter ran for the message

    def start(self) -> bytes:
        if self.started:
            return b""
        self.started = True
        message = {"id": self.message_id, "type": "message", "role": "assistant", "model": self.model, "content": []}
        message |= {"stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 0}}
        return encode("message_start", {"type": "message_start", "message": message})

    def close(self) -> bytes:
        if self.open is None:
            return b""
        self.open = None
        return encode("content_block_stop", {"type": "content_block_stop", "index": self.index})

    def block(self, kind: str, block: dict) -> bytes:
        """Opens a new block (and closes the open one)."""
        out = self.close()
        self.index += 1
        self.open = kind
        return out + encode(
            "content_block_start", {"type": "content_block_start", "index": self.index, "content_block": block}
        )

    def delta(self, delta: dict) -> bytes:
        return encode("content_block_delta", {"type": "content_block_delta", "index": self.index, "delta": delta})

    def ping(self) -> bytes:
        """A ping at most every PING_EVERY_S, for work the app doesn't see yet (reasoning, tool call arguments)."""
        if time.monotonic() - self.pinged < PING_EVERY_S:
            return b""
        self.pinged = time.monotonic()
        return encode("ping", {"type": "ping"})

    async def feed(self, body: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        """The events of one chat completion stream. Its tool calls stay in ``calls`` (``tool_uses`` sends them)."""
        self.calls, self.text, self.finish = {}, "", None
        call_ids: dict[str, int] = {}  # for providers that leave the index out
        async with contextlib.aclosing(events(body)) as stream_events:
            async for kind, data in stream_events:
                if kind == "comment":
                    yield encode("ping", {"type": "ping"})  # waiting in the queue, or a provider that's silent
                    continue
                if kind == "done":
                    break
                if not isinstance(data, dict):
                    continue
                if "error" in data:
                    err = data["error"] if isinstance(data["error"], dict) else {"message": str(data["error"])}
                    code = err.get("code")
                    status = code if isinstance(code, int) else int(code) if str(code).isdecimal() else 500
                    self.failed = True
                    yield encode("error", error(status, err.get("message", "upstream error")))
                    return
                yield self.start()
                if data.get("usage"):
                    self.usage = data["usage"]
                for choice in data.get("choices") or []:
                    delta = choice.get("delta") or {}
                    reasoning = _reasoning(delta)
                    if reasoning and self.thinking:
                        if self.open != "thinking":
                            yield self.block("thinking", {"type": "thinking", "thinking": "", "signature": ""})
                        yield self.delta({"type": "thinking_delta", "thinking": reasoning})
                    elif reasoning or delta.get("tool_calls"):
                        yield self.ping()
                    if text := delta.get("content"):
                        if self.open != "text":
                            yield self.block("text", {"type": "text", "text": ""})
                        self.text += text
                        yield self.delta({"type": "text_delta", "text": text})
                    for call in delta.get("tool_calls") or []:
                        index, function, arguments = tool_call_delta(call, call_ids)
                        held = self.calls.setdefault(index, {"id": "", "name": "", "arguments": ""})
                        held["id"] = held["id"] or str(call.get("id") or "")
                        held["name"] = held["name"] or str(function.get("name") or "")
                        held["arguments"] += arguments
                    if choice.get("finish_reason"):
                        self.finish = choice["finish_reason"]

    def tool_uses(self, calls: list[dict] | None = None) -> bytes:
        """The held tool calls (or ``calls``), each a whole block."""
        out = b""
        for call in self.calls.values() if calls is None else calls:
            call_id = call["id"]
            if not call_id or call_id in self.seen:
                call_id = f"toolu_{uuid.uuid4().hex[:24]}"
            self.seen.add(call_id)
            out += self.block("tool", {"type": "tool_use", "id": call_id, "name": call["name"], "input": {}})
            out += self.delta({"type": "input_json_delta", "partial_json": call["arguments"] or "{}"})
            self.tools_sent = True
        return out + self.close()

    def web_search(self, name: str, query: str, results: list[dict] | None, failure: str = "") -> bytes:
        """A web search QueueRouter ran, as Claude's own server tool blocks: the call, then its results."""
        self.searches += 1
        call, result = search_blocks(name, query, results, failure)
        out = self.block("tool", {**call, "input": {}})
        out += self.delta({"type": "input_json_delta", "partial_json": json.dumps(call["input"])})
        return out + self.block("tool", result) + self.close()

    def error(self, status: int, body: bytes) -> bytes:
        """An error answer (a later round refused or failed) as the stream's error event."""
        self.failed = True
        return encode("error", from_openai_error(status, body))

    def end(self) -> bytes:
        """The end of the message: why it stopped (cut off by the length limit, a tool call's input is incomplete:
        so it says, rather than tool_use), and the last stream's usage, which is the conversation's size."""
        if self.tools_sent and self.finish != "length":
            stop = "tool_use"
        else:
            stop = STOP_REASONS.get(self.finish or "stop", "end_turn")
        usage = _usage(self.usage)
        if self.searches:
            usage["server_tool_use"] = {"web_search_requests": self.searches}
        delta = {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None}, "usage": usage}
        return (
            self.start()
            + self.close()
            + encode("message_delta", delta)
            + encode("message_stop", {"type": "message_stop"})
        )


async def stream(
    body: AsyncIterator[bytes], model: str, thinking: bool = False, seen: set[str] | None = None
) -> AsyncIterator[bytes]:
    """OpenAI chat completion chunks -> Anthropic stream events (see Translation)."""
    translation = Translation(model, thinking, seen)
    async for event in translation.feed(body):
        yield event
    if not translation.failed:
        yield translation.tool_uses()
        yield translation.end()
