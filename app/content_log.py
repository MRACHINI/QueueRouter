"""Opt-in content logging helpers: readable replies (streams reassembled) and size caps."""

from __future__ import annotations

import json


def clip(text: str, limit_bytes: int) -> tuple[str, bool]:
    data = text.encode("utf-8")
    if len(data) <= limit_bytes:
        return text, False
    return data[:limit_bytes].decode("utf-8", "ignore") + "\n…[cut]", True


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
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            if delta.get("content"):
                content.append(delta["content"])
            if delta.get("reasoning_content"):
                reasoning.append(delta["reasoning_content"])
            for call in delta.get("tool_calls") or []:
                entry = calls.setdefault(call.get("index", 0), {"id": None, "name": "", "arguments": ""})
                function = call.get("function") or {}
                entry["id"] = call.get("id") or entry["id"]
                entry["name"] += function.get("name") or ""
                entry["arguments"] += function.get("arguments") or ""
            finish = choice.get("finish_reason") or finish
    summary: dict = {"reassembled_from_stream": True, "content": "".join(content), "finish_reason": finish}
    if reasoning:
        summary["reasoning"] = "".join(reasoning)
    if calls:
        summary["tool_calls"] = [calls[i] for i in sorted(calls)]
    if usage:
        summary["usage"] = usage
    return json.dumps(summary, ensure_ascii=False)
