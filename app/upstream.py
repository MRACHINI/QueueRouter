"""Talking to the upstream provider: headers, retries, usage extraction."""

from __future__ import annotations

import email.utils
import json
import random
import re
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass

import httpx

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
RETRYABLE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.PoolTimeout,
)

# Never forwarded upstream: hop-by-hop, client credentials, proxy metadata.
_DROP_REQUEST_HEADERS = {
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "authorization",
    "x-api-key",
    "cookie",
    "accept-encoding",
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
}
# Only these upstream response headers are passed back on non-streaming responses.
_KEEP_RESPONSE_HEADERS = {"content-type", "retry-after", "x-ratelimit-limit", "x-ratelimit-remaining"}


def upstream_headers(incoming: Mapping[str, str], api_key: str) -> dict[str, str]:
    headers = {k: v for k, v in incoming.items() if k.lower() not in _DROP_REQUEST_HEADERS}
    headers["authorization"] = f"Bearer {api_key}"
    headers["accept-encoding"] = "identity"  # we pipe bytes through untouched
    return headers


def response_headers(upstream: Mapping[str, str]) -> dict[str, str]:
    return {k: v for k, v in upstream.items() if k.lower() in _KEEP_RESPONSE_HEADERS}


def retry_delay(attempt: int, headers: Mapping[str, str] | None, base: float, cap: float) -> float:
    """Retry-After if the upstream sent one, else capped exponential backoff with jitter."""
    if headers and (value := headers.get("retry-after")):
        try:
            return min(cap, max(0.0, float(value)))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None:
                return min(cap, max(0.0, parsed.timestamp() - time.time()))
    delay = min(cap, base * (2**attempt))
    return delay * random.uniform(0.8, 1.2)


@dataclass
class Wait:
    """The caller should sleep ``delay`` seconds before the next attempt."""

    delay: float
    attempt: int
    reason: str


class UpstreamFailed(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


async def send_with_retries(
    client: httpx.AsyncClient,
    request: httpx.Request,
    max_retries: int,
    base_delay: float,
    max_delay: float,
) -> AsyncIterator[Wait | httpx.Response]:
    """Yield ``Wait`` events between attempts, then the final (streaming) response.

    The caller does the sleeping, so a streaming caller can emit keep-alives meanwhile.
    The final response may still be an error status once the retry budget is exhausted.
    """
    attempt = 0
    while True:
        can_retry = max_retries < 0 or attempt < max_retries
        try:
            response = await client.send(request, stream=True)
        except RETRYABLE_ERRORS as exc:
            if not can_retry:
                raise UpstreamFailed(502, f"upstream unreachable: {type(exc).__name__}") from exc
            yield Wait(retry_delay(attempt, None, base_delay, max_delay), attempt + 1, type(exc).__name__)
            attempt += 1
            continue

        if response.status_code in RETRYABLE_STATUS and can_retry:
            delay = retry_delay(attempt, response.headers, base_delay, max_delay)
            await response.aclose()
            yield Wait(delay, attempt + 1, f"HTTP {response.status_code}")
            attempt += 1
            continue

        yield response
        return


_FINISH_REASON = re.compile(rb'"finish_reason"\s*:\s*"([A-Za-z_]+)"')


def usage_from_body(body: bytes) -> tuple[int | None, int | None]:
    try:
        usage = json.loads(body).get("usage") or {}
    except (ValueError, AttributeError):
        return None, None
    return usage.get("prompt_tokens"), usage.get("completion_tokens")


def finish_reason_from_body(body: bytes) -> str | None:
    """``stop``, ``length`` (cut off by the token limit), ``tool_calls``, ``content_filter``..."""
    match = _FINISH_REASON.search(body)
    return match.group(1).decode() if match else None


class UsageSniffer:
    """Watches an SSE byte stream for usage (usually the last chunk) and the finish reason."""

    def __init__(self) -> None:
        self._buffer = b""
        self.prompt_tokens: int | None = None
        self.completion_tokens: int | None = None
        self.finish_reason: str | None = None

    def feed(self, chunk: bytes) -> None:
        self._buffer += chunk
        *lines, self._buffer = self._buffer.split(b"\n")
        if len(self._buffer) > 1_000_000:  # never hold a runaway partial line
            self._buffer = b""
        for line in lines:
            if not line.startswith(b"data:"):
                continue
            if b'"finish_reason"' in line and (reason := finish_reason_from_body(line)):
                self.finish_reason = reason
            if b'"usage"' in line:
                payload = line[5:].strip()
                prompt, completion = usage_from_body(payload)
                if prompt is not None or completion is not None:
                    self.prompt_tokens, self.completion_tokens = prompt, completion
