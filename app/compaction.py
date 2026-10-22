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
    return hashlib.sha256(json.dumps(messages, sort_keys=True, default=str).encode()).hexdigest()[:24]


@dataclass
class Plan:
    """What to cut from a conversation: reusable while its start (up to ``upto``) is unchanged."""

    cut_from: int  # messages[cut_from:cut_to] are dropped (or summarised)
    cut_to: int
    shortened: tuple[int, ...]  # old tool outputs kept as start + end
    upto: int  # the plan depends on messages[:upto]
    prefix: str  # hash of messages[:upto]
    summary: str = ""
    summary_upto: int = 0  # the summary covers messages[cut_from:summary_upto]

    def matches(self, messages: list) -> bool:
        return len(messages) > self.upto and _hash(messages[: self.upto]) == self.prefix


@dataclass
class Result:
    payload: dict
    before: int  # estimated tokens
    after: int
    mode: str
    reused: bool

    @property
    def label(self) -> str:
        return f"{self.mode} {self.before // 1000}k -> {self.after // 1000}k" + (" (reused)" if self.reused else "")


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
    last_user = max((i for i, m in enumerate(messages) if m.get("role") == "user"), default=len(messages) - 1)
    tail = min(len(messages) - KEEP_RECENT, last_user)
    return start, max(start, tail)


def _apply(payload: dict, plan: Plan, note_first_user: bool = True) -> dict:
    messages = payload["messages"]
    out = []
    for i, message in enumerate(messages):
        if plan.cut_from <= i < plan.cut_to:
            continue
        if i in plan.shortened and (text := _text(message.get("content"))) is not None:
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
        if target is not None:
            content = target.get("content")
            if isinstance(content, str):
                out[index] = {**target, "content": content + note}
            elif isinstance(content, list):
                out[index] = {**target, "content": [*content, {"type": "text", "text": note.strip()}]}
    return {**payload, "messages": out}


def make_plan(payload: dict, goal_tokens: int) -> Plan | None:
    """The smallest cut that brings the request under ``goal_tokens``; None if nothing can be cut."""
    messages = payload.get("messages")
    if not isinstance(messages, list) or not all(isinstance(m, dict) for m in messages):
        return None
    start, tail = _protected(messages)
    if tail <= start:
        return None
    total_chars = estimate_prompt_tokens(payload) * CHARS_PER_TOKEN
    goal_chars = goal_tokens * CHARS_PER_TOKEN
    shortened: list[int] = []
    for i in range(start, tail):  # 1. long old tool outputs, oldest first
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


def needed(payload: dict, context: int | None, output: int) -> bool:
    return bool(context) and estimate_prompt_tokens(payload) + output > context * START_AT


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
