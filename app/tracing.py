"""OpenTelemetry traces over OTLP/HTTP (JSON), without the OpenTelemetry SDK.

Each finished request becomes a server span with two children: ``queue.wait`` (waiting for a
slot) and ``upstream`` (the call to the provider, retries included). An incoming W3C
``traceparent`` is honoured, so QueueRouter shows up inside the caller's trace, and the upstream
span's context is passed on to the provider. Spans are sent in batches to ``<endpoint>/v1/traces``.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
from collections import deque

import httpx

log = logging.getLogger("queuerouter")

TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
SERVER, CLIENT, INTERNAL = 2, 3, 1
MAX_PENDING = 10_000
BATCH = 512


def start(traceparent: str | None) -> dict:
    """IDs for a new request: continue the caller's trace if it sent a valid ``traceparent``."""
    match = TRACEPARENT.match((traceparent or "").strip().lower())
    trace_id = match.group(1) if match and match.group(1) != "0" * 32 else secrets.token_hex(16)
    parent = match.group(2) if match and match.group(2) != "0" * 16 else None
    context = {
        "trace_id": trace_id,
        "span_id": secrets.token_hex(8),
        "parent": parent,
        "upstream": secrets.token_hex(8),
    }
    context["upstream_traceparent"] = f"00-{trace_id}-{context['upstream']}-01"
    return context


def _value(value) -> dict:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, list | tuple):
        return {"arrayValue": {"values": [_value(v) for v in value]}}
    return {"stringValue": str(value)}


def _attributes(values: dict) -> list[dict]:
    return [{"key": k, "value": _value(v)} for k, v in values.items() if v is not None and v != ""]


def _span(trace_id, span_id, parent, name, kind, start_ns, end_ns, attributes, error=None) -> dict:
    span = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(max(end_ns, start_ns)),
        "attributes": _attributes(attributes),
        "status": {"code": 2, "message": error} if error else {"code": 1},
    }
    if parent:
        span["parentSpanId"] = parent
    return span


def spans_for(entry: dict) -> list[dict]:
    """Spans for one finished request log entry (needs ``entry["trace"]`` from :func:`start`)."""
    ctx = entry.get("trace")
    if not ctx:
        return []
    end_ns = int(entry["ts"] * 1e9)
    start_ns = end_ns - int((entry.get("total_ms") or 0) * 1e6)
    queue_ns = int((entry.get("queue_ms") or 0) * 1e6)
    upstream_ns = int((entry.get("upstream_ms") or 0) * 1e6)
    outcome = entry.get("outcome")
    error = None if outcome == "done" else f"{outcome} (HTTP {entry.get('status')})"
    model = entry.get("target_model") or entry.get("model")
    root = _span(
        ctx["trace_id"],
        ctx["span_id"],
        ctx["parent"],
        f"{entry.get('method', 'POST')} {entry.get('path', '')}",
        SERVER,
        start_ns,
        end_ns,
        {
            "http.request.method": entry.get("method"),
            "url.path": entry.get("path"),
            "http.response.status_code": entry.get("status"),
            "client.address": entry.get("ip"),
            "queuerouter.request_id": entry.get("id"),
            "queuerouter.client": entry.get("client"),
            "queuerouter.priority": entry.get("priority"),
            "queuerouter.outcome": outcome,
            "queuerouter.queue_position": entry.get("position"),
            "queuerouter.retries": entry.get("retries"),
            "queuerouter.fallback": entry.get("fallback"),
            "queuerouter.tags": entry.get("tags"),
            "gen_ai.request.model": entry.get("model"),
            "gen_ai.response.model": model,
            "gen_ai.usage.input_tokens": entry.get("prompt_tokens"),
            "gen_ai.usage.output_tokens": entry.get("completion_tokens"),
            "gen_ai.response.finish_reasons": [entry["finish_reason"]] if entry.get("finish_reason") else None,
        },
        error,
    )
    spans = [root]
    if queue_ns:
        spans.append(
            _span(
                ctx["trace_id"],
                secrets.token_hex(8),
                ctx["span_id"],
                "queue.wait",
                INTERNAL,
                start_ns,
                start_ns + queue_ns,
                {"queuerouter.queue_position": entry.get("position")},
            )
        )
    if entry.get("upstream"):
        upstream_start = start_ns + queue_ns
        spans.append(
            _span(
                ctx["trace_id"],
                ctx["upstream"],
                ctx["span_id"],
                "upstream",
                CLIENT,
                upstream_start,
                upstream_start + upstream_ns if upstream_ns else end_ns,
                {"server.address": entry.get("upstream"), "gen_ai.request.model": model},
                error,
            )
        )
    return spans


def parse_headers(text: str) -> dict[str, str]:
    headers = {}
    for part in (text or "").split(","):
        key, sep, value = part.partition("=")
        if sep and key.strip():
            headers[key.strip()] = value.strip()
    return headers


class Exporter:
    def __init__(self) -> None:
        self.pending: deque[dict] = deque(maxlen=MAX_PENDING)  # oldest dropped if the collector is down
        self.failures = 0

    def observe(self, entry: dict) -> None:
        self.pending.extend(spans_for(entry))

    async def flush(self, http: httpx.AsyncClient, endpoint: str, headers: str, service: str, version: str) -> None:
        while self.pending:
            batch = [self.pending.popleft() for _ in range(min(BATCH, len(self.pending)))]
            body = {
                "resourceSpans": [
                    {
                        "resource": {"attributes": _attributes({"service.name": service, "service.version": version})},
                        "scopeSpans": [{"scope": {"name": "queuerouter", "version": version}, "spans": batch}],
                    }
                ]
            }
            try:
                resp = await http.post(
                    endpoint.rstrip("/") + "/v1/traces",
                    content=json.dumps(body).encode(),
                    headers={"content-type": "application/json", **parse_headers(headers)},
                    timeout=10,
                )
                if resp.status_code >= 300:
                    raise httpx.HTTPStatusError(f"HTTP {resp.status_code}", request=resp.request, response=resp)
                self.failures = 0
            except httpx.HTTPError as exc:
                self.failures += 1
                if self.failures in (1, 10) or self.failures % 100 == 0:  # don't flood the log
                    log.warning(
                        json.dumps({"event": "otel_export_failed", "error": str(exc)[:200], "dropped": len(batch)})
                    )
                return
