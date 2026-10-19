"""Structured-output repair: make broken JSON replies valid when the caller asked for JSON.

Applies to non-streaming chat completions that use ``response_format`` (``json_object`` /
``json_schema``) — the message content — and to tool-call ``arguments``. Common breakage from
smaller models is fixed locally: code fences, prose around the JSON, trailing commas, and
brackets/strings left open. The reply is marked with ``X-QueueRouter-Structured``:
``repaired`` (changed) or ``invalid`` (couldn't fix; returned unchanged).
"""

from __future__ import annotations

import json
import re

_FENCE = re.compile(r"```(?:json|JSON)?\s*\n?(.*?)\n?```", re.S)
_TRAILING_COMMA = re.compile(r",(\s*[}\]])")


def _valid(text: str) -> bool:
    try:
        json.loads(text)
    except ValueError:
        return False
    return True


def _close_open(text: str) -> str:
    """Close strings and brackets left open (e.g. a reply cut off mid-object)."""
    stack, in_string, escaped = [], False, False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "{[":
            stack.append("}" if ch == "{" else "]")
        elif ch in "}]" and stack:
            stack.pop()
    fixed = text + ('"' if in_string else "")
    fixed = re.sub(r",\s*$", "", fixed.rstrip())
    fixed = re.sub(r'(:\s*|,\s*"[^"]*"\s*:\s*)$', lambda m: m.group(0) + "null", fixed)
    return fixed + "".join(reversed(stack))


def repair(text: str) -> str | None:
    """Valid JSON text made from ``text``, or None if it can't be fixed."""
    candidate = text.strip()
    if _valid(candidate):
        return candidate
    if fence := _FENCE.search(candidate):
        candidate = fence.group(1).strip()
    starts = [i for i in (candidate.find("{"), candidate.find("[")) if i >= 0]
    if not starts:
        return None
    candidate = candidate[min(starts) :]  # drop prose before the JSON
    end = max(candidate.rfind("}"), candidate.rfind("]"))
    cut = candidate[: end + 1] if end >= 0 else candidate  # ... and after it
    uncomma = _TRAILING_COMMA.sub
    for attempt in (
        cut,
        uncomma(r"\1", cut),
        uncomma(r"\1", candidate),
        _close_open(uncomma(r"\1", candidate)),  # cut off mid-way: close what's open
        _close_open(uncomma(r"\1", cut)),
    ):
        if _valid(attempt):
            return attempt
    return None


def wants_json(request: dict) -> bool:
    fmt = request.get("response_format")
    return isinstance(fmt, dict) and fmt.get("type") in ("json_object", "json_schema")


def repair_reply(request: dict, reply: dict) -> str | None:
    """Fix the reply in place. Returns "repaired", "invalid" or None (nothing to check)."""
    checked, changed, broken = False, False, False
    for choice in reply.get("choices") or []:
        message = choice.get("message") or {}
        if wants_json(request) and isinstance(message.get("content"), str):
            checked = True
            fixed = repair(message["content"])
            if fixed is None:
                broken = True
            elif fixed != message["content"]:
                message["content"], changed = fixed, True
        for call in message.get("tool_calls") or []:
            function = call.get("function") or {}
            arguments = function.get("arguments")
            if not isinstance(arguments, str):
                continue
            checked = True
            fixed = repair(arguments or "{}")
            if fixed is None:
                broken = True
            elif fixed != arguments:
                function["arguments"], changed = fixed, True
    if not checked:
        return None
    return "invalid" if broken else "repaired" if changed else None
