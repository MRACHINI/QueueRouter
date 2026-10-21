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


# Only these mean "waiting won't help". Google's per-minute 429s also say "quota exceeded" and
# mention billing, so those words alone don't count.
_HARD_QUOTA_HINTS = ("perday", "per_day", "per day", "limit: 0", "insufficient_quota")


def describe_429(content: bytes) -> tuple[str, bool, float | None]:
    """(message, hard, suggested wait in seconds) from a provider's 429 body.

    *Hard* = waiting a few seconds won't help: a daily quota is used up (Google
    ``...PerDay...`` quota ids), the plan has no quota for the model (``limit: 0``), or the
    account is out of credit (OpenAI ``insufficient_quota``).
    """
    try:
        data = json.loads(content or b"{}")
    except ValueError:
        text = content.decode("utf-8", "replace").strip()[:300]
        return text or "rate limited", any(h in text.lower() for h in _HARD_QUOTA_HINTS), None
    if isinstance(data, list) and data:  # Google wraps errors in a list
        data = data[0]
    error = data.get("error") if isinstance(data, dict) else None
    if not isinstance(error, dict):
        return "rate limited", False, None
    message = str(error.get("message") or error.get("status") or "rate limited")
    hints = " ".join(str(error.get(k, "")) for k in ("type", "code", "status")) + " " + message
    wait = None
    for detail in error.get("details") or []:
        if not isinstance(detail, dict):
            continue
        kind = str(detail.get("@type", ""))
        if kind.endswith("QuotaFailure"):
            for violation in detail.get("violations") or []:
                hints += " " + " ".join(str(v) for v in violation.values())
        elif kind.endswith("RetryInfo"):
            raw = str(detail.get("retryDelay", ""))
            try:
                wait = float(raw.rstrip("s")) if raw else None
            except ValueError:
                wait = None
    hard = any(h in hints.lower() for h in _HARD_QUOTA_HINTS)
    return " ".join(message.split())[:300], hard, wait


UA_MODES = ("keep", "remove", "template")
UA_PLACEHOLDERS = ("original", "client", "priority", "model", "upstream", "version")


def outgoing_user_agent(
    client_rule: tuple[str, str], upstream_rule: tuple[str, str], values: dict[str, str]
) -> str | None:
    """The User-Agent to send to the provider, or None to pass the app's own through.

    Rules are (mode, template); the client's wins unless it is "keep". "remove" sends a neutral
    ``queuerouter/<version>`` (without any, the HTTP library would add its own). Placeholders such as
    ``{client}`` are replaced literally, so a template can never fail; the result is one ASCII line.
    """
    mode, template = client_rule if client_rule[0] != "keep" else upstream_rule
    if mode == "remove":
        return f"queuerouter/{values.get('version', '')}"
    if mode != "template" or not template.strip():
        return None
    result = template
    for name in UA_PLACEHOLDERS:
        result = result.replace("{" + name + "}", values.get(name) or "")
    result = " ".join(result.split()).encode("ascii", "replace").decode()[:500]
    return result or None
