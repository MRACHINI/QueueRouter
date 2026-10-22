"""Model council (per alias): several models answer, a judge decides.

The request goes to every member model at once (non-streaming, through QueueRouter's own queue),
then a judge model looks at the conversation and the candidates:
  pick    - the judge only names the best candidate, which is returned as it is (fast; works for tool
            calls, which can't be merged meaningfully);
  combine - the judge writes the final answer itself, using the candidates (plans, explanations); it
            may call tools like any model.
The app gets one ordinary answer. By default only planning steps use the council: the first step after
a user message, or a request tagged agent:plan; ordinary tool steps (and low-priority requests such as
titles) go to the alias's model as usual.
"""

from __future__ import annotations

import json
import re
import time
import uuid

MODES = ("off", "pick", "combine")
WHEN = ("planning", "always")
MAX_MEMBERS = 5
CANDIDATE_CHARS = 12_000  # per candidate shown to the judge

JUDGE_MARK = "[QueueRouter council]"
PICK_PROMPT = (
    f"{JUDGE_MARK} Several AI models answered the conversation above. Their answers are below. Judge which "
    "answer is the best next step for the user: correct, complete, safe and most useful (for an agent: the "
    "right tool calls with the right arguments). Reply with only the number of the best answer."
)
COMBINE_PROMPT = (
    f"{JUDGE_MARK} Several AI models answered the conversation above. Their answers are below. Write the best "
    "possible reply to the user yourself: take what is right in them, fix what is wrong, leave out what is "
    "not needed. If the next step is a tool call, make it. Reply as the assistant, without mentioning the "
    "other answers."
)


def is_planning(messages: list, tags: list[str]) -> bool:
    """The first step after a user message, or opencode's plan agent."""
    if "agent:plan" in tags:
        return True
    last = messages[-1] if messages else None
    return isinstance(last, dict) and last.get("role") == "user"


def applies(alias, payload: dict, tags: list[str], low_priority: bool) -> bool:
    if alias is None or getattr(alias, "council", "off") == "off" or not alias.council_members or low_priority:
        return False
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        return False
    return alias.council_when == "always" or is_planning(messages, tags)


def message_of(body: bytes) -> dict | None:
    """The assistant message of a chat completion, or None."""
    try:
        choice = (json.loads(body).get("choices") or [{}])[0]
    except (ValueError, AttributeError, IndexError):
        return None
    message = choice.get("message")
    if not isinstance(message, dict) or not (message.get("content") or message.get("tool_calls")):
        return None
    return message


def render(message: dict) -> str:
    parts = []
    if isinstance(message.get("content"), str) and message["content"].strip():
        parts.append(message["content"].strip())
    for call in message.get("tool_calls") or []:
        function = (call.get("function") or {}) if isinstance(call, dict) else {}
        parts.append(f"[tool call] {function.get('name')}({function.get('arguments', '')})")
    text = "\n".join(parts)
    if len(text) > CANDIDATE_CHARS:
        text = text[:CANDIDATE_CHARS] + "\n[... cut ...]"
    return text


def judge_payload(payload: dict, model: str, mode: str, candidates: list[tuple[str, dict]]) -> dict:
    listing = "\n\n".join(f"### Answer {i}\n{render(message)}" for i, (_, message) in enumerate(candidates, 1))
    prompt = PICK_PROMPT if mode == "pick" else COMBINE_PROMPT
    judged = {
        **payload,
        "model": model,
        "messages": [*payload["messages"], {"role": "user", "content": f"{prompt}\n\n{listing}"}],
    }
    if mode == "pick":
        judged.update(stream=False, max_tokens=20, temperature=0)
        judged.pop("max_completion_tokens", None)
        judged.pop("stream_options", None)
        if judged.get("tools"):
            judged["tool_choice"] = "none"  # it answers with a number, not a tool call
    return judged


def parse_pick(text: str | None, count: int) -> int:
    """The 0-based candidate the judge named (the first one when its answer is unclear)."""
    match = re.search(r"\d+", text or "")
    number = int(match.group()) if match else 1
    return number - 1 if 1 <= number <= count else 0


def as_stream(body: bytes, model: str) -> bytes:
    """A finished chat completion as an SSE stream (for apps that asked to stream)."""
    data = json.loads(body)
    choice = (data.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    base = {
        "id": data.get("id") or f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion.chunk",
        "created": data.get("created") or int(time.time()),
        "model": data.get("model") or model,
    }
    delta: dict = {"role": "assistant"}
    if message.get("content"):
        delta["content"] = message["content"]
    if message.get("reasoning_content"):
        delta["reasoning_content"] = message["reasoning_content"]
    if message.get("tool_calls"):
        delta["tool_calls"] = [{**call, "index": i} for i, call in enumerate(message["tool_calls"])]
    chunks = [
        {**base, "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
        {**base, "choices": [{"index": 0, "delta": {}, "finish_reason": choice.get("finish_reason") or "stop"}]},
    ]
    if data.get("usage"):
        chunks.append({**base, "choices": [], "usage": data["usage"]})
    return b"".join(f"data: {json.dumps(c)}\n\n".encode() for c in chunks) + b"data: [DONE]\n\n"
