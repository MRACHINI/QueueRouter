"""Fallback chains: refusal detection and a response wrapper that replays inspected bytes.

An alias can list backup models. The request moves to the next one when (per alias):
  unavailable - none of its upstreams can take it right now (down, paused, disabled)
  errors      - retries ran out on 5xx / connection errors
  context     - the prompt is too long for that model
  refused     - the model declined: a content-policy error, finish_reason "content_filter", or a
                reply that opens with a refusal ("I can't help with ..."). For streams the first
                ~200 characters are held back to check, then released (or the model is switched).
A refusal some providers put in a field of its own (``refusal``) counts too. The request itself is resent unchanged;
nothing is rephrased. An alias that checks answers, with rules (app/answer_rules.py) or a checker model
(app/answer_check.py), holds back the answer instead (``hold_answer``: whole, or its start when that's all its rules
read), and moves on when a rule or its checker fails it.
An alias's model and fallbacks may be other aliases: the request is forwarded to that alias (app/main.py, ``Hop``)
and served as one for it, its whole way; the editor refuses loops (``loop_of``).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

TRIGGERS = ("unavailable", "errors", "context", "refused")
DEFAULT_TRIGGERS = ("unavailable", "errors", "context")  # "refused" adds a short delay: opt-in


def loop_of(alias: Any, aliases: Mapping[str, Any]) -> list[str] | None:
    """The aliases around a loop in what an alias uses (``["a", "b", "a"]``), or None."""

    def walk(current: Any, path: list[str]) -> list[str] | None:
        for named in (current.target_model, *current.fallbacks):
            inner = aliases.get(named)
            if inner is None:
                continue
            if inner.name in path:
                return [*path[path.index(inner.name) :], inner.name]
            if found := walk(inner, [*path, inner.name]):
                return found
        return None

    return walk(alias, [alias.name])


SNIFF_CHARS = 200
SNIFF_SECONDS = 2.5

REFUSAL_OPENERS = (
    "i can't help with",
    "i cannot help with",
    "i can't assist",
    "i cannot assist",
    "i can't provide",
    "i cannot provide",
    "i can't comply",
    "i cannot comply",
    "i won't be able to",
    "i'm not able to help",
    "i am not able to help",
    "i'm unable to",
    "i am unable to",
    "i must decline",
    "i can't do that",
    "i cannot do that",
    "i'm sorry, but i can't",
    "i'm sorry, but i cannot",
    "sorry, but i can't",
    "sorry, but i cannot",
    "i apologize, but i can't",
    "i apologize, but i cannot",
    "as an ai",
)
POLICY_ERROR = re.compile(r"content[ _-]?(policy|filter|management)|safety|moderation|flagged|prohibited", re.I)


def is_refusal_text(text: str, finish_reason: str | None = None) -> bool:
    if finish_reason == "content_filter":
        return True
    opening = re.sub(r"\s+", " ", text.strip().lower().replace("’", "'"))[:160]
    return any(opening.startswith(p) or f". {p}" in opening[:80] for p in REFUSAL_OPENERS)


def is_policy_error(status: int, body: bytes) -> bool:
    """A 4xx that means "this content isn't allowed" (vs. a malformed request)."""
    return status in (400, 403, 422, 451) and bool(POLICY_ERROR.search(body[:4000].decode(errors="replace")))


@dataclass
class Answer:
    """What a chat completion answered: its text, the separate ``refusal`` some providers use (OpenAI's structured
    outputs), its tool calls as [name, arguments], and its finish_reason."""

    text: str = ""
    refusal: str = ""
    calls: list = field(default_factory=list)
    finish: str | None = None

    @property
    def acting(self) -> bool:
        return bool(self.calls)

    @property
    def kind(self) -> str:
        """ "tools" (it calls tools), "text" (it says something, a refusal too) or "empty"."""
        if self.calls:
            return "tools"
        return "text" if self.text.strip() or self.refusal.strip() else "empty"

    def shown(self) -> str:
        """The answer as a check reads it: its text, its refusal, and each call as ``[tool call] name(arguments)``."""
        parts = [p.strip() for p in (self.text, self.refusal) if p.strip()]
        parts += [f"[tool call] {name}({arguments})" for name, arguments in self.calls]
        return "\n".join(parts)


def is_refusal(answer: Answer) -> bool:
    """A refusal by the built-in phrases (``is_refusal_text``), or one the provider put in its ``refusal`` field. An
    answer that calls tools is acting, not refusing."""
    if answer.refusal.strip():
        return True
    return not answer.calls and is_refusal_text(answer.text, answer.finish)


def answer_of(body: bytes) -> Answer:
    """The answer of a non-streamed chat completion (or legacy completion)."""
    try:
        choice = (json.loads(body).get("choices") or [{}])[0]
        message = choice.get("message") or {}
    except (ValueError, AttributeError, IndexError):
        return Answer()
    if not isinstance(message, dict):
        return Answer()
    text = message.get("content") if isinstance(message.get("content"), str) else choice.get("text")
    refusal = message.get("refusal")
    calls = [
        [str(fn.get("name") or ""), str(fn.get("arguments") or "")]
        for call in (message.get("tool_calls") or [])
        if isinstance(call, dict) and isinstance(fn := call.get("function"), dict)
    ]
    if isinstance(old := message.get("function_call"), dict):
        calls.append([str(old.get("name") or ""), str(old.get("arguments") or "")])
    return Answer(
        text if isinstance(text, str) else "",
        refusal if isinstance(refusal, str) else "",
        calls,
        choice.get("finish_reason"),
    )


def message_text(body: bytes) -> tuple[str, str | None]:
    """(assistant text, finish_reason) of a non-streaming chat/completions response; no text when it calls tools."""
    answer = answer_of(body)
    return ("" if answer.calls else answer.text), answer.finish


class AnswerReader:
    """Builds an ``Answer`` from a chat completion stream as it comes: text, refusal and tool calls (their names and
    arguments come in pieces, by index)."""

    def __init__(self) -> None:
        self._pending = b""
        self._text: list[str] = []
        self._refusal: list[str] = []
        self._calls: dict = {}  # index -> [name, arguments]
        self.finish: str | None = None
        self.size = 0  # characters of text, refusal and calls so far

    def feed(self, chunk: bytes) -> None:
        self._pending += chunk
        *lines, self._pending = self._pending.split(b"\n")
        for line in lines:
            if not line.startswith(b"data:") or line.strip() == b"data: [DONE]":
                continue
            try:
                choice = (json.loads(line[5:]).get("choices") or [{}])[0]
                delta = choice.get("delta") or {}
            except (ValueError, AttributeError, IndexError):
                continue
            if not isinstance(delta, dict):
                continue
            if isinstance(delta.get("content"), str):
                self._text.append(delta["content"])
                self.size += len(delta["content"])
            elif isinstance(choice.get("text"), str):
                self._text.append(choice["text"])
                self.size += len(choice["text"])
            if isinstance(delta.get("refusal"), str):
                self._refusal.append(delta["refusal"])
                self.size += len(delta["refusal"])
            calls = [(c.get("index", i), c.get("function")) for i, c in enumerate(delta.get("tool_calls") or [])]
            if isinstance(delta.get("function_call"), dict):
                calls.append(("function_call", delta["function_call"]))
            for index, fn in calls:
                entry = self._calls.setdefault(index, ["", ""])
                if isinstance(fn, dict):
                    for i, key in enumerate(("name", "arguments")):
                        if isinstance(fn.get(key), str):
                            entry[i] += fn[key]
                            self.size += len(fn[key])
            self.finish = choice.get("finish_reason") or self.finish

    @property
    def acting(self) -> bool:
        return bool(self._calls)

    @property
    def answer(self) -> Answer:
        calls = [list(c) for c in self._calls.values()]
        return Answer("".join(self._text), "".join(self._refusal), calls, self.finish)


class Pump:
    """Reads a response body in a background task into a queue, so a reader can wait with a
    timeout without cancelling (and so breaking) the upstream stream itself."""

    _END = object()

    def __init__(self, response: httpx.Response) -> None:
        self.queue: asyncio.Queue = asyncio.Queue()
        self.task = asyncio.ensure_future(self._run(response))

    async def _run(self, response: httpx.Response) -> None:
        try:
            async for chunk in response.aiter_bytes():
                await self.queue.put(chunk)
        except Exception as exc:  # handed to the reader, which re-raises it
            await self.queue.put(exc)
        await self.queue.put(self._END)

    async def get(self, timeout: float | None = None):
        """Next chunk; None at the end; raises TimeoutError when nothing arrives in time."""
        item = await asyncio.wait_for(self.queue.get(), timeout)
        if item is self._END:
            return None
        if isinstance(item, Exception):
            raise item
        return item

    async def chunks(self) -> AsyncIterator[bytes]:
        while (chunk := await self.get()) is not None:
            yield chunk

    def stop(self) -> None:
        self.task.cancel()


class Replay:
    """Looks like an httpx streaming response, but first returns bytes we already read."""

    def __init__(
        self,
        response: httpx.Response,
        prefix: bytes = b"",
        rest: AsyncIterator[bytes] | None = None,
        complete: bool = False,
        pump: Pump | None = None,
    ) -> None:
        self._response = response
        self._prefix = prefix
        self._rest = rest
        self._complete = complete
        self._pump = pump
        self.status_code = response.status_code
        self.headers = response.headers
        self.prefix = prefix
        self.sniffer = None  # a UsageSniffer that already saw ``prefix`` as it came (its times are the real ones)

    async def aread(self) -> bytes:
        if self._complete:
            return self._prefix
        chunks = [self._prefix]
        async for chunk in self._remaining():
            chunks.append(chunk)
        return b"".join(chunks)

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        if self._prefix:
            yield self._prefix
        if not self._complete:
            async for chunk in self._remaining():
                yield chunk

    def _remaining(self) -> AsyncIterator[bytes]:
        return self._rest if self._rest is not None else self._response.aiter_bytes()

    async def aclose(self) -> None:
        if self._pump is not None:
            self._pump.stop()
        await self._response.aclose()


async def sniff_stream(response: httpx.Response) -> tuple[bool, Replay]:
    """Read the start of an SSE stream; (refused?, a Replay that re-emits what was read)."""
    pump = Pump(response)
    buffered: list[bytes] = []
    reader = AnswerReader()
    deadline = time.monotonic() + SNIFF_SECONDS
    ended = False
    try:
        while reader.size < SNIFF_CHARS and reader.finish is None and not reader.acting:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                chunk = await pump.get(remaining)
            except TimeoutError:
                break
            if chunk is None:
                ended = True
                break
            buffered.append(chunk)
            reader.feed(chunk)
    except BaseException:  # cancelled while reading: don't leave the reader task behind
        pump.stop()
        raise
    refused = is_refusal(reader.answer)
    return refused, Replay(response, b"".join(buffered), None if ended else pump.chunks(), complete=ended, pump=pump)


@dataclass
class Held:
    """An answer read before any of it was passed on (``hold_answer``)."""

    replay: Replay
    answer: Answer = field(default_factory=Answer)
    whole: bool = True  # all of it was read (False: handed on early: it calls tools, its start was enough, or time)

    @property
    def text(self) -> str:
        return self.answer.text

    @property
    def finish(self) -> str | None:
        return self.answer.finish

    @property
    def acting(self) -> bool:
        return self.answer.acting


async def hold_answer(
    response: httpx.Response,
    stream: bool,
    idle: float,
    deadline: float | None,
    feed: Callable[[bytes], None] | None = None,
    tools: bool = False,
    start: int | None = None,
) -> AsyncIterator[Held | None]:
    """Read an answer before any of it is passed on: ``None`` whenever nothing came for ``idle`` seconds (the
    caller's cue for a keep-alive), then one ``Held``. A stream is handed on as soon as it turns out to call tools
    (unless ``tools``: those are read whole too), once ``start`` characters of it came (when only its start is
    needed), or when it still runs at the monotonic ``deadline``; the unread rest follows as it comes. ``feed`` sees
    each chunk of a stream as it arrives."""
    if not stream:
        content = await response.aread()
        yield Held(Replay(response, content, complete=True), answer_of(content))
        return
    pump = Pump(response)
    buffered: list[bytes] = []
    reader = AnswerReader()
    ended = False
    try:  # the result is handed over below, outside: closing this generator then mustn't stop the pump it uses
        while not ended and not (reader.acting and not tools) and not (start is not None and reader.size >= start):
            wait = idle
            if deadline is not None and (wait := min(idle, deadline - time.monotonic())) <= 0:
                break
            try:
                chunk = await pump.get(wait)
            except TimeoutError:
                yield None
                continue
            if chunk is None:
                ended = True
                break
            if feed is not None:
                feed(chunk)
            buffered.append(chunk)
            reader.feed(chunk)
    except BaseException:  # stopped while reading: don't leave the reader task behind
        pump.stop()
        raise
    rest = None if ended else pump.chunks()
    replay = Replay(response, b"".join(buffered), rest, complete=ended, pump=pump)
    yield Held(replay, reader.answer, whole=ended)
