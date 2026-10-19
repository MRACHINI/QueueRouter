"""Talking to the upstream provider: headers, retries, usage extraction."""

from __future__ import annotations

import email.utils
import json
import random
import re
import time
from collections.abc import Mapping

import httpx

BUSY_STATUS = 429  # upstream full: keep the slot, back off, retry the same upstream
FAILURE_STATUS = {500, 502, 503, 504}  # upstream broken: counts toward its circuit breaker
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
# QueueRouter's own request headers (X-QueueRouter-Priority, X-Session-Id...) stay here.
_OWN_HEADER_PREFIXES = ("x-queuerouter-", "x-session-id", "x-run-id")
# Only these upstream response headers are passed back on non-streaming responses.
_KEEP_RESPONSE_HEADERS = {"content-type", "retry-after", "x-ratelimit-limit", "x-ratelimit-remaining"}


def upstream_headers(incoming: Mapping[str, str], api_key: str) -> dict[str, str]:
    """Client headers minus credentials, hop-by-hop and our own control headers."""
    headers = {
        k: v
        for k, v in incoming.items()
        if k.lower() not in _DROP_REQUEST_HEADERS and not k.lower().startswith(_OWN_HEADER_PREFIXES)
    }
    if api_key:  # local servers (Ollama, llama.cpp...) may need none
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


_FINISH_REASON = re.compile(rb'"finish_reason"\s*:\s*"([A-Za-z_]+)"')


def usage_from_body(body: bytes) -> tuple[int | None, int | None, int | None]:
    """(prompt, completion, cached prompt) tokens from an OpenAI-style ``usage`` object."""
    try:
        usage = json.loads(body).get("usage") or {}
    except (ValueError, AttributeError):
        return None, None, None
    if not isinstance(usage, dict):
        return None, None, None
    details = usage.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens") if isinstance(details, dict) else None
    if cached is None:
        cached = usage.get("prompt_cache_hit_tokens")  # DeepSeek's naming
    return usage.get("prompt_tokens"), usage.get("completion_tokens"), cached if isinstance(cached, int) else None


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
        self.cached_tokens: int | None = None
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
                prompt, completion, cached = usage_from_body(payload)
                if prompt is not None or completion is not None:
                    self.prompt_tokens, self.completion_tokens, self.cached_tokens = prompt, completion, cached
