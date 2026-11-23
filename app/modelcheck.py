"""What a provider's model list says about a model, field by field, and what QueueRouter reads each field as: the
Upstreams page's "Check models" view. A provider says a model's context, output size, thinking levels and prices
under its own names (or not at all: Qwen Cloud's list is only id / object / owner), and a field QueueRouter doesn't
know stays unread, with nothing to show it. Here every field of an entry is listed with its use, the unread ones
marked, next to what QueueRouter uses for the model and where that comes from, so a gap is seen and filled by hand
(Models page: sizes, thinking levels, prices; Upstreams page: how a thinking level is sent).

The table below names the fields the readers take (app/opencode.py CONTEXT_KEYS / OUTPUT_KEYS / listed_output,
app/thinking.py from_block / from_refuseless / from_claude / takes_images, app/media.py kind, main.listed_price); a
test keeps it in step with them.
"""

from __future__ import annotations

import json

from .opencode import CONTEXT_KEYS, OUTPUT_KEYS

UNREAD = ""  # a field QueueRouter doesn't use
PLAIN = "-"  # one that says nothing to use (its id, who owns it, when it was made)

_PLAIN_FIELDS = ("id", "object", "created", "owned_by", "type", "created_at", "display_name")
_USES: dict[str, str] = {
    **dict.fromkeys(CONTEXT_KEYS, "context"),
    **dict.fromkeys(OUTPUT_KEYS, "output size"),
    "top_provider.max_completion_tokens": "output size",
    "reasoning": "thinking levels",
    "reasoning.supported_efforts": "thinking levels",
    "reasoning.default_effort": "default thinking level",
    "reasoning.default_enabled": "thinking levels (on / off)",
    "reasoning.mandatory": "thinking levels (on / off)",
    "reasoning_efforts": "thinking levels",
    "reasoning_default": "default thinking level",
    "capabilities.effort": "thinking levels",
    "capabilities.thinking": "thinking (Claude: adaptive or not)",
    "supported_parameters": "thinking levels (lists reasoning)",
    "engine": "how a thinking level is sent (vllm)",
    "vlm": "reads images",
    "vision": "reads images",
    "input_modalities": "reads images",
    "architecture.input_modalities": "reads images",
    "output_modalities": "what it makes (image, video)",
    "architecture.output_modalities": "what it makes (image, video)",
    "pricing.prompt": "input price",
    "pricing.completion": "output price",
    "pricing.input_cache_read": "cached input price",
    "pricing.input_per_m": "input price",
    "pricing.output_per_m": "output price",
    "pricing.cached_input_per_m": "cached input price",
}
MAX_VALUE = 160  # characters of a value shown


def use_of(path: str) -> str:
    """What QueueRouter reads the field at ``path`` ("pricing.prompt") as; UNREAD when it doesn't."""
    if path in _USES:
        return _USES[path]
    return PLAIN if path in _PLAIN_FIELDS else UNREAD


def _shown(value) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(", ", ": "))
    return text if len(text) <= MAX_VALUE else text[: MAX_VALUE - 1] + "…"


def fields(entry: dict | None) -> list[dict]:
    """An entry's fields in its own order, each {"path", "value", "use"}. A field that holds others ("pricing",
    "architecture") is opened one level (what's under "capabilities.effort" is read as a whole)."""
    rows: list[dict] = []
    for key, value in (entry or {}).items():
        key = str(key)
        holds = isinstance(value, dict) and bool(value)
        # one read as itself, with nothing under it read apart, stays whole
        if holds and (key not in _USES or any(f"{key}.{inner}" in _USES for inner in value)):
            for inner, inner_value in value.items():
                path = f"{key}.{inner}"
                rows.append({"path": path, "value": _shown(inner_value), "use": use_of(path)})
            continue
        rows.append({"path": key, "value": _shown(value), "use": use_of(key)})
    return rows


def unread(rows: list[dict]) -> list[str]:
    """The fields of an entry that QueueRouter doesn't use."""
    return [row["path"] for row in rows if row["use"] == UNREAD]


STYLE_NOTES = {  # how a thinking level reaches an upstream (thinking.style_of), in words
    "openai": "as reasoning_effort, as the app sent it (fitted to the model's levels)",
    "openrouter": "as reasoning.effort (OpenRouter's form)",
    "vllm": "as the chat template's on / off switch, with low / medium / high",
    "refuseless": "as reasoning_effort in Refuseless's own levels",
    "synthetic": "as reasoning_effort in Synthetic's levels (none: off)",
    "claude": "as Claude's output_config.effort, with adaptive thinking turned on",
    "none": "not at all: the field is removed, and no levels are offered",
    "fixed": "as the app sent it; no levels are offered (the provider ignores them)",
    "unknown": "as the app sent it; only levels its own list states are offered (an API QueueRouter doesn't know)",
}
