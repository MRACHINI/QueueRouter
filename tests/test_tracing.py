"""OpenTelemetry trace export (OTLP/HTTP JSON)."""

import asyncio
import json

import httpx

from app.tracing import parse_headers, spans_for, start

CALLER_TRACE, CALLER_SPAN = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"


def test_trace_context():
    ctx = start(f"00-{CALLER_TRACE}-{CALLER_SPAN}-01")
    assert ctx["trace_id"] == CALLER_TRACE and ctx["parent"] == CALLER_SPAN
    assert ctx["upstream_traceparent"] == f"00-{CALLER_TRACE}-{ctx['upstream']}-01"
    fresh = start("garbage")
    assert len(fresh["trace_id"]) == 32 and fresh["parent"] is None
    assert parse_headers("authorization=Bearer a=b, x-tenant = me,bad") == {
        "authorization": "Bearer a=b",
        "x-tenant": "me",
    }
    entry = {
        "trace": ctx,
        "ts": 100.0,
        "total_ms": 3000,
        "queue_ms": 1000,
        "upstream_ms": 2000,
        "outcome": "error",
        "status": 502,
        "upstream": "arli",
        "method": "POST",
        "path": "/v1/chat/completions",
    }
    root, queue_span, upstream = spans_for(entry)
    assert root["startTimeUnixNano"] == str(97 * 10**9) and root["status"]["code"] == 2
    assert queue_span["endTimeUnixNano"] == upstream["startTimeUnixNano"] == str(98 * 10**9)
    assert upstream["spanId"] == ctx["upstream"] and upstream["parentSpanId"] == ctx["span_id"]
    assert spans_for({"ts": 1.0}) == []


async def test_traces_exported(make_stack):
    stack = make_stack()
    store = stack.app.state.store
    store.set_tunables(
        {"otel_endpoint": stack.mock["url"], "otel_headers": "x-tenant=team1", "otel_service_name": "qr-test"}, "test"
    )
    stack.app.state.reload()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post(
            "/v1/chat/completions",
            headers={**stack.headers, "traceparent": f"00-{CALLER_TRACE}-{CALLER_SPAN}-01"},
            json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert r.status_code == 200
    forwarded = stack.mock["last_headers"]["traceparent"]
    assert forwarded.startswith(f"00-{CALLER_TRACE}-") and CALLER_SPAN not in forwarded

    for _ in range(80):
        if stack.mock.get("traces"):
            break
        await asyncio.sleep(0.1)
    sent = stack.mock["traces"][0]
    assert sent["headers"]["x-tenant"] == "team1"
    resource = json.loads(sent["body"])["resourceSpans"][0]
    assert {"key": "service.name", "value": {"stringValue": "qr-test"}} in resource["resource"]["attributes"]
    spans = {s["name"]: s for s in resource["scopeSpans"][0]["spans"]}
    root, upstream = spans["POST /v1/chat/completions"], spans["upstream"]
    assert root["traceId"] == CALLER_TRACE and root["parentSpanId"] == CALLER_SPAN
    assert forwarded == f"00-{CALLER_TRACE}-{upstream['spanId']}-01"
    attributes = {a["key"]: a["value"] for a in root["attributes"]}
    assert attributes["gen_ai.usage.input_tokens"] == {"intValue": "10"}
    assert attributes["queuerouter.outcome"] == {"stringValue": "done"}
