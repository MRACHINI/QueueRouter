"""Compaction: fit a conversation that is too big for the model it goes to.

Opt-in per alias (``compact``). Decided per request, on the whole request (history, current
message, system prompt, tools and room for the answer) against the limit of the model it is about
to go to: from ~80% of that limit the request is shrunk to ~50%, so the start of the request stays
the same for many turns (provider prompt caching keeps working) and the work happens in steps.

Always kept in full: the system prompt(s), the first user message (the task), the current message
and the last few turns. Trim mode, oldest first:
  1. long old tool outputs (file reads, command output) keep their start and end;
  2. old middle messages are dropped (a note on the task says so).
Summarise mode (``summary`` given) replaces the dropped part with a summary instead.

A plan (what to cut) is remembered per conversation and reused while the request still fits, so
the same messages are cut on every turn; it is rebuilt only when the request grows past ~80% again.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from .params import CHARS_PER_TOKEN, estimate_prompt_tokens, message_chars

MODES = ("off", "trim", "summarise")
START_AT = 0.80  # of the model's context: compact from here...
BLOCK_AT = 0.90  # summarise mode: below this the request goes out at once and the summary is written alongside
GOAL = 0.50  # ...down to here
KEEP_RECENT = 6  # messages at the end that are never touched (besides the current user message)
LONG_OUTPUT = 2_000  # characters: old tool outputs longer than this are shortened
KEEP_HEAD, KEEP_TAIL = 600, 300
SUMMARY_MARK = "[QueueRouter summary of the earlier conversation]"


def _text(content: Any) -> str | None:
    """The text of a message's content, or None when it isn't plain text (images...)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list) and all(isinstance(p, dict) and p.get("type") == "text" for p in content):
        return "\n".join(str(p.get("text", "")) for p in content)
    return None


def shorten(text: str) -> str:
    cut = len(text) - KEEP_HEAD - KEEP_TAIL
    note = f"[... {cut} characters trimmed by QueueRouter to fit the model's context ...]"
    return f"{text[:KEEP_HEAD]}\n{note}\n{text[-KEEP_TAIL:]}"


def _hash(messages: list) -> str:
    """Identifies the start of a conversation. Tool results count by their call id only: agents clear old
    tool output from their history ("[Old tool result content cleared]" in opencode) once it has been
    used, and that must not make a summary of that part look like it belongs to another conversation."""
    shape = [
        {"role": m.get("role"), "id": m.get("tool_call_id")}
        if isinstance(m, dict) and m.get("role") == "tool"
        else {k: v for k, v in m.items() if k != "cache_control"}
        if isinstance(m, dict)
        else m
        for m in messages
    ]
    return hashlib.sha256(json.dumps(shape, sort_keys=True, default=str).encode()).hexdigest()[:24]


@dataclass
class Plan:
    """What to cut from a conversation: reusable while its start (up to ``upto``) is unchanged."""

    cut_from: int  # messages[cut_from:cut_to] are dropped (or summarised)
    cut_to: int
    shortened: tuple[int, ...]  # old tool outputs kept as start + end
    upto: int  # the plan depends on messages[:upto]
    prefix: str  # hash of messages[:upto]
    summary: str = ""  # summarise mode: replaces messages[cut_from:cut_to]

    def matches(self, messages: list) -> bool:
        return len(messages) > self.upto and _hash(messages[: self.upto]) == self.prefix


@dataclass
class Result:
    payload: dict
    before: int  # estimated tokens
    after: int
    mode: str
    reused: bool
    note: str = ""  # e.g. "new summary", "summary updated"

    @property
    def label(self) -> str:
        note = self.note or ("reused" if self.reused else "")
        return f"{self.mode} {self.before // 1000}k -> {self.after // 1000}k" + (f" ({note})" if note else "")


def _protected(messages: list) -> tuple[int, int]:
    """(first index that may be cut, first index of the protected tail)."""
    head = 0
    while (
        head < len(messages)
        and isinstance(messages[head], dict)
        and messages[head].get("role") in ("system", "developer")
    ):
        head += 1
    first_user = next((i for i in range(head, len(messages)) if messages[i].get("role") == "user"), None)
    start = (first_user + 1) if first_user is not None else head
    # An agent's turn can be dozens of tool rounds after the user's latest message: those may be cut too
    # (all but the last few). The latest message itself is kept: it moves next to the task (see _apply),
    # which needs it to be plain text.
    tail = len(messages) - KEEP_RECENT
    latest = _latest_user(messages)
    if latest is not None and _text(messages[latest].get("content")) is None:
        tail = min(tail, latest)
    return start, max(start, tail)


def _latest_user(messages: list) -> int | None:
    return max((i for i, m in enumerate(messages) if isinstance(m, dict) and m.get("role") == "user"), default=None)


def _apply(payload: dict, plan: Plan, note_first_user: bool = True) -> dict:
    messages = payload["messages"]
    latest = _latest_user(messages)
    kept_latest = latest if latest is not None and plan.cut_from <= latest < plan.cut_to else None
    out = []
    for i, message in enumerate(messages):
        if plan.cut_from <= i < plan.cut_to:
            continue
        if i in plan.shortened and (text := _text(message.get("content"))) is not None and len(text) > LONG_OUTPUT:
            message = {**message, "content": shorten(text)}
        out.append(message)
    dropped = plan.cut_to - plan.cut_from
    if dropped and note_first_user:
        index = plan.cut_from - 1  # the first user message, just before the cut
        target = out[index] if 0 <= index < len(out) and out[index].get("role") == "user" else None
        if plan.summary:
            note = f"\n\n{SUMMARY_MARK}\n{plan.summary}"
        else:
            note = f"\n\n[QueueRouter: {dropped} earlier messages were removed here to fit the model's context.]"
        if kept_latest is not None:  # the user's latest message was inside the cut: keep it, after the summary
            note += f"\n\n[The user's latest message]\n{_text(messages[kept_latest].get('content'))}"
        if target is not None:
            content = target.get("content")
            if isinstance(content, str):
                out[index] = {**target, "content": content + note}
            elif isinstance(content, list):
                out[index] = {**target, "content": [*content, {"type": "text", "text": note.strip()}]}
    return {**payload, "messages": out}


def make_plan(payload: dict, goal_tokens: int, shorten_old: bool = True) -> Plan | None:
    """The smallest cut that brings the request under ``goal_tokens``; None if nothing can be cut.
    ``shorten_old``: first shorten long old tool outputs (trim mode; summarise mode summarises instead)."""
    messages = payload.get("messages")
    if not isinstance(messages, list) or not all(isinstance(m, dict) for m in messages):
        return None
    start, tail = _protected(messages)
    if tail <= start:
        return None
    total_chars = estimate_prompt_tokens(payload) * CHARS_PER_TOKEN
    goal_chars = goal_tokens * CHARS_PER_TOKEN
    shortened: list[int] = []
    for i in range(start, tail if shorten_old else start):  # 1. long old tool outputs, oldest first
        if total_chars <= goal_chars:
            break
        message = messages[i]
        text = _text(message.get("content"))
        if message.get("role") == "tool" and text is not None and len(text) > LONG_OUTPUT:
            shortened.append(i)
            total_chars -= len(text) - len(shorten(text))
    cut_to = start
    if total_chars > goal_chars:  # 2. drop old messages: the kept part starts at an assistant message
        removed = 0.0
        for i in range(start, tail):
            message = messages[i]
            if i in shortened:
                message = {**message, "content": shorten(_text(message.get("content")) or "")}
            removed += message_chars(message)  # the same measure as estimate_prompt_tokens
            nxt = i + 1
            if total_chars - removed <= goal_chars and nxt < len(messages) and messages[nxt].get("role") == "assistant":
                cut_to = nxt
                break
        else:  # can't reach the goal: cut as much as allowed (the next assistant message at most)
            cut_to = next((j for j in range(tail, start, -1) if messages[j].get("role") == "assistant"), start)
    if cut_to == start and not shortened:
        return None
    upto = max(cut_to, max(shortened, default=0) + 1)
    return Plan(start, cut_to, tuple(shortened), upto, _hash(messages[:upto]))


@dataclass
class _Entry:
    plan: Plan
    used: float = field(default_factory=time.monotonic)


class Plans:
    """Remembered plans per conversation (in memory; forgotten after ``idle_s`` unused)."""

    def __init__(self, max_entries: int = 2_000, idle_s: float = 3_600) -> None:
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self.max_entries, self.idle_s = max_entries, idle_s

    def get(self, key: str) -> Plan | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if time.monotonic() - entry.used > self.idle_s:
            del self._entries[key]
            return None
        entry.used = time.monotonic()
        self._entries.move_to_end(key)
        return entry.plan

    def put(self, key: str, plan: Plan) -> None:
        self._entries[key] = _Entry(plan)
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)


def needed(payload: dict, context: int | None, output: int, start_at: float = START_AT) -> bool:
    return bool(context) and estimate_prompt_tokens(payload) + output > context * start_at


def compact(payload: dict, context: int, output: int, plans: Plans, key: str | None) -> Result | None:
    """Shrink ``payload`` to fit ``context`` (trim mode), reusing the conversation's plan while it fits.
    None when it already fits or nothing can be cut."""
    if not needed(payload, context, output):
        return None
    before = estimate_prompt_tokens(payload)
    messages = payload["messages"]
    plan = plans.get(key) if key else None
    if plan is not None and plan.matches(messages):
        shrunk = _apply(payload, plan)
        if not needed(shrunk, context, output):
            return Result(shrunk, before, estimate_prompt_tokens(shrunk), "summarise" if plan.summary else "trim", True)
    goal = max(int(context * GOAL) - output, int(context * GOAL / 2))
    plan = make_plan(payload, goal)
    if plan is None:
        return None
    if key:
        plans.put(key, plan)
    shrunk = _apply(payload, plan)
    return Result(shrunk, before, estimate_prompt_tokens(shrunk), "trim", False)


# -- summarise mode ---------------------------------------------------------------------------
# The summary is written by a model through QueueRouter's own queue (main.py). It is rolling: each
# update reads the previous summary plus the part that has become old since, in chunks that fit the
# summarising model, one after another, so that model never needs the whole history.

SUMMARY_TOKENS = 8_000  # the summariser's answer at most, and the room kept for the summary in the request
SUMMARY_PROMPT = (
    "You compact the history of a conversation between a user and an AI coding agent so the work can "
    "continue with less context. Merge the summary so far with the next part of the conversation into one "
    "updated summary. Keep: the user's goals, requirements and preferences; decisions made and why; files "
    "read, created or changed (exact paths) and what changed; commands run and important results or errors; "
    "the current state; open problems and next steps. Leave out greetings, repetition and raw tool output "
    "that no longer matters. Keep it under about 2,500 words: when the summary so far is long, condense its "
    "older details rather than dropping recent ones. "
    "Write in the user's language, as compact notes. Output only the summary."
)
IDLE_START_AT = 0.70  # idle updates only for conversations above this share of the model's context


def summarise_goal(context: int, output: int) -> int:
    return max(int(context * GOAL) - output - SUMMARY_TOKENS, int(context * GOAL / 2))


def conversation_key(client: str, session: str | None, messages: list) -> str | None:
    """Summaries belong to a conversation: the client, its session and its start (system prompt and task,
    so a subagent sharing its parent's session id gets its own)."""
    if not session or not isinstance(messages, list):
        return None
    start, _ = _protected(messages)
    return f"{client}:{session}:{_hash(messages[:start])}"


def _render(message: dict) -> str:
    role = str(message.get("role", "?"))
    parts = []
    if (text := _text(message.get("content"))) is None:
        text = json.dumps(message.get("content"), ensure_ascii=False, default=str)
    if text:
        parts.append(text)
    for call in message.get("tool_calls") or []:
        function = (call.get("function") or {}) if isinstance(call, dict) else {}
        parts.append(f"[calls {function.get('name')}({function.get('arguments', '')})]")
    label = "tool result" if role == "tool" else role
    return f"### {label}\n" + "\n".join(parts)


def chunks(messages: list, max_chars: int) -> list[str]:
    """The messages as text for the summariser, in pieces of at most ``max_chars`` (a single longer
    message keeps its start and end)."""
    out: list[str] = []
    current = ""
    for message in messages:
        block = _render(message)
        if len(block) > max_chars:
            keep = max_chars // 2 - 100
            block = f"{block[:keep]}\n[... {len(block) - 2 * keep} characters left out ...]\n{block[-keep:]}"
        if current and len(current) + len(block) + 2 > max_chars:
            out.append(current)
            current = ""
        current = f"{current}\n\n{block}" if current else block
    if current:
        out.append(current)
    return out


def summary_payload(model: str, previous: str, part: str) -> dict:
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": SUMMARY_PROMPT},
            {
                "role": "user",
                "content": f"Summary so far:\n{previous or '(none yet)'}\n\nNext part of the conversation:\n{part}",
            },
        ],
        "max_tokens": SUMMARY_TOKENS,
        "temperature": 0.2,
        "stream": False,
    }


def summary_text(body: bytes) -> str | None:
    try:
        choice = (json.loads(body).get("choices") or [{}])[0]
        text = (choice.get("message") or {}).get("content")
    except (ValueError, AttributeError, IndexError):
        return None
    return text.strip() if isinstance(text, str) and text.strip() else None


def with_summary(payload: dict, plan: Plan) -> dict:
    return _apply(payload, plan)


def plan_to_dict(plan: Plan) -> dict:
    return {
        "cut_from": plan.cut_from,
        "cut_to": plan.cut_to,
        "upto": plan.upto,
        "prefix": plan.prefix,
        "summary": plan.summary,
    }


def plan_from_dict(data: dict) -> Plan | None:
    try:
        return Plan(
            int(data["cut_from"]), int(data["cut_to"]), (), int(data["upto"]), str(data["prefix"]), str(data["summary"])
        )
    except (KeyError, TypeError, ValueError):
        return None


def summary_plan(messages: list, cut_from: int, cut_to: int, summary: str) -> Plan:
    return Plan(cut_from, cut_to, (), cut_to, _hash(messages[:cut_to]), summary)
