"""Parsing the OpenAI-style SSE stream QueueRouter produces (for the API adapters)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator


async def events(body: AsyncIterator[bytes]) -> AsyncIterator[tuple[str, object]]:
    """Yield ("comment", text) for keep-alives, ("data", parsed JSON), ("done", None) for [DONE].

    Upstream bytes can be split anywhere, so lines are buffered until an event ends."""
    buffer = b""
    try:
        async for chunk in body:
            buffer += chunk if isinstance(chunk, bytes) else chunk.encode()
            buffer = buffer.replace(b"\r\n", b"\n")
            while b"\n\n" in buffer:
                raw, buffer = buffer.split(b"\n\n", 1)
                for item in _parse(raw.decode("utf-8", "replace")):
                    yield item
        if buffer.strip():
            for item in _parse(buffer.decode("utf-8", "replace")):
                yield item
    finally:  # stopping early must still end the upstream stream (and free its slot)
        close = getattr(body, "aclose", None)
        if close is not None:
            await close()


def _parse(block: str) -> list[tuple[str, object]]:
    data_lines, out = [], []
    for line in block.split("\n"):
        if line.startswith(":"):
            out.append(("comment", line[1:].strip()))
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        data = "\n".join(data_lines)
        if data.strip() == "[DONE]":
            out.append(("done", None))
        else:
            try:
                out.append(("data", json.loads(data)))
            except ValueError:
                pass  # not JSON: nothing an adapter can translate
    return out


def encode(event: str | None, data: dict) -> bytes:
    head = f"event: {event}\n" if event else ""
    return f"{head}data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()
