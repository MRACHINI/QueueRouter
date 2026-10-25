"""Fallback chains: refusal detection and a response wrapper that replays inspected bytes.

An alias can list backup models. The request moves to the next one when (per alias):
  unavailable - none of its upstreams can take it right now (down, paused, disabled)
  errors      - retries ran out on 5xx / connection errors
  context     - the prompt is too long for that model
  refused     - the model declined: a content-policy error, finish_reason "content_filter", or a
                reply that opens with a refusal ("I can't help with ..."). For streams the first
                ~200 characters are held back to check, then released (or the model is switched).
The request itself is resent unchanged; nothing is rephrased.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator

import httpx

TRIGGERS = ("unavailable", "errors", "context", "refused")
DEFAULT_TRIGGERS = ("unavailable", "errors", "context")  # "refused" adds a short delay: opt-in

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


def message_text(body: bytes) -> tuple[str, str | None]:
    """(assistant text, finish_reason) of a non-streaming chat/completions response."""
    try:
        choice = (json.loads(body).get("choices") or [{}])[0]
    except (ValueError, AttributeError, IndexError):
        return "", None
    message = choice.get("message") or {}
    if message.get("tool_calls"):
        return "", choice.get("finish_reason")  # acting, not refusing
    text = message.get("content") if isinstance(message.get("content"), str) else choice.get("text") or ""
    return text or "", choice.get("finish_reason")


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
    pending = b""
    text = ""
    finish: str | None = None
    acting = False
    deadline = time.monotonic() + SNIFF_SECONDS
    ended = False
    try:
        while len(text) < SNIFF_CHARS and finish is None and not acting:
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
            pending += chunk
            *lines, pending = pending.split(b"\n")
            for line in lines:
                if not line.startswith(b"data:") or line.strip() == b"data: [DONE]":
                    continue
                try:
                    choice = (json.loads(line[5:]).get("choices") or [{}])[0]
                except (ValueError, AttributeError, IndexError):
                    continue
                delta = choice.get("delta") or {}
                if delta.get("tool_calls"):
                    acting = True
                if isinstance(delta.get("content"), str):
                    text += delta["content"]
                elif isinstance(choice.get("text"), str):
                    text += choice["text"]
                finish = choice.get("finish_reason") or finish
    except BaseException:  # cancelled while reading: don't leave the reader task behind
        pump.stop()
        raise
    refused = not acting and is_refusal_text(text, finish)
    return refused, Replay(response, b"".join(buffered), None if ended else pump.chunks(), complete=ended, pump=pump)
