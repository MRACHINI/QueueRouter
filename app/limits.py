"""Model context sizes QueueRouter knows beyond what providers report, and overflow errors.

Many providers (NVIDIA, Gemini's OpenAI endpoint...) list models without a context size. Two
sources fill the gap:
  learned - read from a provider's own "prompt too long" error ("maximum context length is N"),
            per upstream + model, the first time a request is too big;
  manual  - set per model on the Models page (also corrects a wrong reported value).
Manual wins over learned, learned over what the model list says; plan caps still apply on top.

Every provider words its overflow error differently; they are rewritten into OpenAI's form
(400, ``context_length_exceeded``, "This model's maximum context length is N tokens"), which
coding agents such as opencode recognise and answer by compacting the conversation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

# The phrases providers use for "the prompt doesn't fit" (not "max_tokens is too big").
_OVERFLOW = re.compile(
    r"context[_ ]length[_ ]exceeded|maximum context length|context (?:window|length|size|limit)"
    r"|prompt is too long|input is too long|too many (?:input )?tokens|exceeds? the (?:maximum|max) "
    r"(?:number of )?(?:input )?tokens|input token count|reduce the length of the (?:messages|prompt|input)"
    r"|maximum (?:input|prompt) length|maximum allowed (?:token|input)",
    re.I,
)
_INPUT_AND_OUTPUT = re.compile(
    r"input length and .{0,3}max_tokens.{0,3} exceed context limit: [\d,]+ \+ [\d,]+ > (\d[\d,]*)", re.I
)
# Numbers that state the limit itself, most specific first.
_LIMIT_PATTERNS = (
    re.compile(r"maximum context length (?:is|of) (\d[\d,]*)", re.I),
    re.compile(r"maximum number of tokens allowed \(?(\d[\d,]*)", re.I),
    re.compile(r"context (?:window|length|size|limit) (?:is|of) (\d[\d,]*)", re.I),
    re.compile(r"(?:maximum|max) (?:input|prompt) length (?:is|of) (\d[\d,]*)", re.I),
    re.compile(r"maximum allowed (?:token|input) (?:size|length|count) (?:is |of )?(\d[\d,]*)", re.I),
    re.compile(r"tokens? > (\d[\d,]*) maximum", re.I),  # Anthropic: "250000 tokens > 200000 maximum"
    re.compile(r"(?:limit|maximum)(?: is)?:? (\d[\d,]*) tokens", re.I),
)
MIN_CONTEXT, MAX_CONTEXT = 1_024, 100_000_000
OVERFLOW_STATUS = (400, 413, 422)


def error_message(content: bytes) -> str:
    """The human message of an error body (OpenAI, Google list-wrapped, or plain text)."""
    try:
        data = json.loads(content or b"{}")
    except ValueError:
        return content[:2000].decode("utf-8", "replace").strip()
    if isinstance(data, list) and data:
        data = data[0]
    if isinstance(data, dict):
        error = data.get("error", data)
        if isinstance(error, dict):
            parts = [str(error.get(k) or "") for k in ("message", "code", "type")]
            return " ".join(p for p in parts if p)[:2000]
        if isinstance(error, str):
            return error[:2000]
        if isinstance(data.get("detail"), str):
            return data["detail"][:2000]
    return ""


def overflow_of(status: int, content: bytes) -> tuple[bool, int | None, str]:
    """(is an overflow error, the limit it states or None, the provider's message)."""
    if status not in OVERFLOW_STATUS:
        return False, None, ""
    message = error_message(content)
    if not _OVERFLOW.search(message):
        return False, None, message
    # Anthropic: "input length and `max_tokens` exceed context limit: 190000 + 20000 > 200000": the prompt
    # plus the answer don't fit, so a shorter prompt does help
    if match := _INPUT_AND_OUTPUT.search(message):
        value = int(match.group(1).replace(",", ""))
        return True, value if MIN_CONTEXT <= value <= MAX_CONTEXT else None, message
    lowered = message.lower()
    if ("max_tokens" in lowered or "completion tokens" in lowered) and "messages" not in lowered:
        return False, None, message  # the requested answer size is too big, not the prompt
    for pattern in _LIMIT_PATTERNS:
        if match := pattern.search(message):
            value = int(match.group(1).replace(",", ""))
            if MIN_CONTEXT <= value <= MAX_CONTEXT:
                return True, value, message
    return True, None, message


def overflow_body(model: str, context: int | None, needed: int | None = None, detail: str = "") -> bytes:
    """The standard OpenAI-style overflow error."""
    if context:
        message = f"This model's maximum context length is {context} tokens."
        if needed:
            message += f" However, your request needs about {needed} tokens."
    else:
        message = "The request is longer than this model's maximum context length."
    message += " Please reduce the length of the messages."
    if detail:
        message += f" ({model}: {detail[:300]})"
    error = {
        "message": message,
        "type": "invalid_request_error",
        "param": "messages",
        "code": "context_length_exceeded",
    }
    return json.dumps({"error": error}).encode()


@dataclass(frozen=True)
class Limit:
    context: int | None = None
    output: int | None = None


class ModelLimits:
    """Manual (per model) and learned (per upstream + model) limits, kept in memory."""

    def __init__(self) -> None:
        self.manual: dict[str, Limit] = {}
        self.learned: dict[tuple[int, str], int] = {}

    def load(self, rows: list[dict]) -> None:
        manual, learned = {}, {}
        for row in rows:
            if row["source"] == "manual":
                manual[row["model"]] = Limit(row["context"], row["output"])
            elif row["context"]:
                learned[(row["upstream_id"], row["model"])] = row["context"]
        self.manual, self.learned = manual, learned

    def apply(self, model: str, upstream_id: int, entry: dict | None) -> dict | None:
        """``entry`` (a /v1/models item) with known limits filled in; unchanged if none."""
        manual = self.manual.get(model)
        learned = self.learned.get((upstream_id, model))
        if not manual and not learned:
            return entry
        merged = dict(entry or {"id": model})
        if learned:
            merged["context_length"] = learned
        if manual and manual.context:
            merged["context_length"] = manual.context
        if manual and manual.output:
            merged["max_output_tokens"] = manual.output
        return merged

    def known(self, model: str, upstream_id: int) -> int | None:
        manual = self.manual.get(model)
        return (manual.context if manual else None) or self.learned.get((upstream_id, model))
