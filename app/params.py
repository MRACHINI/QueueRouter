"""Request parameter rules (per alias and per client) and the context-length pre-check.

Rules, applied to the JSON body before forwarding:
  defaults  - set only if the request didn't           {"temperature": 0.7}
  overrides - always set                                {"top_p": 0.9}
  clamps    - numeric min/max                           {"max_tokens": {"max": 4096}}
  strip     - remove parameters the upstream rejects    ["logit_bias"]
Order: alias defaults, client defaults, alias overrides, client overrides, clamps, strip.
"""

from __future__ import annotations

import json
from typing import Any

RULE_KEYS = ("defaults", "overrides", "clamps", "strip")
# Never touched by rules: they'd change what the request *is*, not how it's generated.
PROTECTED = {"model", "messages", "prompt", "stream", "tools", "tool_choice"}


def parse_rules(text: str | None) -> dict[str, Any]:
    """Validate rule JSON from the admin UI; raises ValueError with a readable message."""
    if not text or not text.strip():
        return {}
    try:
        rules = json.loads(text)
    except ValueError as exc:
        raise ValueError(f"not valid JSON: {exc}") from exc
    if not isinstance(rules, dict):
        raise ValueError("rules must be a JSON object")
    unknown = set(rules) - set(RULE_KEYS)
    if unknown:
        raise ValueError(f"unknown keys: {', '.join(sorted(unknown))} (use {', '.join(RULE_KEYS)})")
    for key in ("defaults", "overrides"):
        if not isinstance(rules.get(key, {}), dict):
            raise ValueError(f"'{key}' must be an object")
    clamps = rules.get("clamps", {})
    if not isinstance(clamps, dict) or not all(
        isinstance(v, dict) and set(v) <= {"min", "max"} and all(isinstance(x, int | float) for x in v.values())
        for v in clamps.values()
    ):
        raise ValueError('\'clamps\' must look like {"max_tokens": {"min": 1, "max": 4096}}')
    if not isinstance(rules.get("strip", []), list) or not all(isinstance(s, str) for s in rules.get("strip", [])):
        raise ValueError("'strip' must be a list of parameter names")
    touched = (
        set(rules.get("defaults", {})) | set(rules.get("overrides", {})) | set(clamps) | set(rules.get("strip", []))
    )
    if touched & PROTECTED:
        raise ValueError(f"these can't be changed by rules: {', '.join(sorted(touched & PROTECTED))}")
    return rules


def apply_rules(payload: dict, alias_rules: dict, client_rules: dict) -> list[str]:
    """Apply rules in place; returns a short list of what changed (for logs)."""
    changes: list[str] = []
    for rules in (alias_rules, client_rules):
        for name, value in rules.get("defaults", {}).items():
            if name not in payload:
                payload[name] = value
                changes.append(f"default {name}")
    for rules in (alias_rules, client_rules):
        for name, value in rules.get("overrides", {}).items():
            if payload.get(name) != value:
                payload[name] = value
                changes.append(f"override {name}")
    for rules in (alias_rules, client_rules):
        for name, bounds in rules.get("clamps", {}).items():
            value = payload.get(name)
            if isinstance(value, int | float) and not isinstance(value, bool):
                clamped = min(max(value, bounds.get("min", value)), bounds.get("max", value))
                if clamped != value:
                    payload[name] = int(clamped) if isinstance(value, int) and float(clamped).is_integer() else clamped
                    changes.append(f"clamp {name}")
    for rules in (alias_rules, client_rules):
        for name in rules.get("strip", []):
            if name in payload:
                del payload[name]
                changes.append(f"strip {name}")
    return changes


# -- context pre-check --------------------------------------------------------------------

CHARS_PER_TOKEN = 4.0  # typical for English and code; deliberately not too pessimistic
MARGIN = 1.15  # only refuse when the estimate is clearly over: a false refusal is worse than a 400
FIT_SPARE = 1.1  # lowering the answer size: the prompt may count this much more than estimated
FIT_RESERVE = 512  # and a chat template adds tokens of its own (roles, separators), even to a tiny prompt
MIN_FIT = 1_024  # an answer size not lowered below this: the context check decides instead


def _text_of(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(_text_of(v) for v in value)
    if isinstance(value, dict):
        # content parts {"type": "text", "text": ...}; images etc. are ignored (can't be sized)
        return _text_of(value.get("text") or value.get("content") or "")
    return ""


def message_chars(message: dict) -> int:
    """Characters a message adds to the prompt: its text plus any tool calls (whose arguments can hold
    whole files, e.g. an agent's write/edit calls)."""
    calls = message.get("tool_calls")
    return len(_text_of(message.get("content"))) + (len(json.dumps(calls)) if calls else 0) + 1


def estimate_prompt_tokens(payload: dict) -> int:
    chars = 0
    if isinstance(payload.get("messages"), list):
        chars = sum(message_chars(m) for m in payload["messages"] if isinstance(m, dict))
    elif isinstance(payload.get("prompt"), str | list):
        chars = len(_text_of(payload["prompt"]))
    tools = payload.get("tools")
    extra = len(json.dumps(tools)) if tools else 0
    return int((chars + extra) / CHARS_PER_TOKEN)


def requested_output(payload: dict) -> int:
    for key in ("max_completion_tokens", "max_tokens"):
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return 0


def fit_output(payload: dict, context: int | None, ratio: float = 1.0, most: int | None = None) -> bool:
    """Lower the answer size asked for (``max_tokens``, ``max_completion_tokens``) to what the model can give: its own
    output limit (``most``), and what its context leaves after the prompt (estimated; ``ratio``: how much bigger the
    provider counts). Apps made for one model ask for its size everywhere (Claude Code: 128,000 for Opus): lowered,
    the request still goes. A prompt that leaves less than MIN_FIT is left to the context check. True if lowered."""
    asked = requested_output(payload)
    if not asked:
        return False
    room = min(asked, most) if most else asked
    if context:
        left = context - round(estimate_prompt_tokens(payload) * ratio * FIT_SPARE) - FIT_RESERVE
        if left >= MIN_FIT:
            room = min(room, left)
    if room >= asked:
        return False
    for key in ("max_completion_tokens", "max_tokens"):
        if isinstance(payload.get(key), int) and payload[key] > room:
            payload[key] = room
    return True


def too_long(payload: dict, context: int | None) -> tuple[int, int] | None:
    """(estimated prompt tokens, context) when the prompt clearly can't fit, else None."""
    if not context:
        return None
    estimate = estimate_prompt_tokens(payload)
    if estimate + requested_output(payload) > context * MARGIN:
        return estimate, context
    return None
