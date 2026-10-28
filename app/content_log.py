"""Opt-in content logging helpers: readable replies (streams reassembled) and size caps."""

from __future__ import annotations

import json


def clip(text: str, limit_bytes: int) -> tuple[str, bool]:
    data = text.encode("utf-8", "replace")  # a lone surrogate (from a JSON "\ud800") becomes "?"
    if len(data) <= limit_bytes:
        return data.decode("utf-8"), False
    return data[:limit_bytes].decode("utf-8", "ignore") + "\n…[cut]", True


def _text(value: object) -> str:
    """Text from a delta field: a string, or the text parts of a list (some providers send parts)."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(p.get("text") or "" for p in value if isinstance(p, dict) and isinstance(p.get("text"), str))
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False)  # e.g. tool arguments sent as an object


def readable_reply(raw: bytes) -> str:
    """A non-streaming reply as-is; a stream reassembled into one JSON object."""
    text = raw.decode("utf-8", "replace")
    if not text.lstrip().startswith(("data:", ":")):
        return text
    content, reasoning, finish, usage = [], [], None, None
    calls: dict[int, dict] = {}
    for line in text.splitlines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            continue
        try:
            chunk = json.loads(data)
        except ValueError:
            continue
        if not isinstance(chunk, dict):
            continue
        if chunk.get("error"):
            return json.dumps({"reassembled_from_stream": True, "error": chunk["error"]}, ensure_ascii=False)
        usage = chunk.get("usage") or usage
        choices = chunk.get("choices")
        for choice in choices if isinstance(choices, list) else []:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta")
            delta = delta if isinstance(delta, dict) else {}
            content.append(_text(delta.get("content")))
            reasoning.append(_text(delta.get("reasoning_content")))
            tool_calls = delta.get("tool_calls")
            for call in tool_calls if isinstance(tool_calls, list) else []:
                if not isinstance(call, dict):
                    continue
                index = call.get("index") if isinstance(call.get("index"), int) else 0
                entry = calls.setdefault(index, {"id": None, "name": "", "arguments": ""})
                function = call.get("function") if isinstance(call.get("function"), dict) else {}
                entry["id"] = call.get("id") or entry["id"]
                entry["name"] += _text(function.get("name"))
                entry["arguments"] += _text(function.get("arguments"))
            finish = choice.get("finish_reason") or finish
    summary: dict = {"reassembled_from_stream": True, "content": "".join(content), "finish_reason": finish}
    if any(reasoning):
        summary["reasoning"] = "".join(reasoning)
    if calls:
        summary["tool_calls"] = [calls[i] for i in sorted(calls)]
    if usage:
        summary["usage"] = usage
    return json.dumps(summary, ensure_ascii=False)
