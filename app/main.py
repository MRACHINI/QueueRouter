"""QueueRouter Phase 0: an OpenAI-compatible proxy that queues instead of exceeding the
upstream's concurrency limit."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import __version__
from .auth import AuthGuard, client_ip, key_is_valid, presented_key
from .config import Settings
from .slots import SlotQueue
from .stats import Stats
from .upstream import (
    UpstreamFailed,
    UsageSniffer,
    Wait,
    response_headers,
    send_with_retries,
    upstream_headers,
    usage_from_body,
)

log = logging.getLogger("queuerouter")


def error_body(message: str, status: int, kind: str = "queuerouter_error") -> dict:
    return {"error": {"message": message, "type": kind, "code": status}}


def error_response(message: str, status: int, kind: str = "queuerouter_error", **headers: str) -> JSONResponse:
    return JSONResponse(error_body(message, status, kind), status_code=status, headers=headers or None)


def sse_error(message: str, status: int, kind: str = "upstream_error") -> bytes:
    return b"data: " + json.dumps(error_body(message, status, kind)).encode() + b"\n\ndata: [DONE]\n\n"


class RequestLog:
    """Collects one request's metadata; emitted as a single JSON log line at the end."""

    def __init__(self, request_id: str, method: str, path: str, ip: str) -> None:
        self.started = time.monotonic()
        self.fields: dict = {
            "id": request_id,
            "method": method,
            "path": path,
            "ip": ip,
            "model": None,
            "stream": False,
            "status": None,
            "outcome": None,
            "queue_ms": None,
            "upstream_ms": None,
            "retries": 0,
            "prompt_tokens": None,
            "completion_tokens": None,
        }

    def finish(self, stats: Stats, **fields) -> None:
        self.fields.update(fields)
        self.fields["total_ms"] = round((time.monotonic() - self.started) * 1000)
        f = self.fields
        stats.record(
            f["outcome"], f["queue_ms"], f["upstream_ms"], f["retries"], f["prompt_tokens"], f["completion_tokens"]
        )
        log.info(json.dumps(f))


def create_app(settings: Settings, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    settings.validate()
    logging.basicConfig(level=settings.log_level, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # our own request log covers upstream calls

    queue = SlotQueue(settings.max_concurrency, settings.min_spacing_ms)
    stats = Stats()
    guard = AuthGuard(settings.auth_fail_limit, settings.auth_block_s)
    models_cache: dict = {"at": 0.0, "status": 0, "body": b"", "headers": {}}
    max_body = int(settings.max_body_mb * 1024 * 1024)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(
                connect=settings.upstream_connect_timeout_s,
                read=settings.upstream_read_timeout_s,
                write=30,
                pool=None,
            ),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
        log.info(
            json.dumps(
                {
                    "event": "startup",
                    "version": __version__,
                    "upstream": settings.upstream_url,
                    "max_concurrency": settings.max_concurrency,
                    "auth": "keys" if settings.access_keys else "none",
                }
            )
        )
        yield
        await app.state.client.aclose()

    app = FastAPI(
        title="QueueRouter", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )

    # -- helpers ---------------------------------------------------------------------------

    def authenticate(request: Request) -> JSONResponse | None:
        ip = client_ip(request, settings)
        if remaining := guard.is_blocked(ip):
            return error_response(
                "too many failed authentication attempts",
                429,
                "auth_blocked",
                **{"Retry-After": str(int(remaining) + 1)},
            )
        if not key_is_valid(presented_key(request), settings):
            guard.fail(ip)
            return error_response("invalid or missing API key", 401, "invalid_api_key")
        return None

    def build_upstream_request(request: Request, path: str, body: bytes | None) -> httpx.Request:
        url = f"{settings.upstream_url}/{path}"
        if request.url.query:
            url += f"?{request.url.query}"
        return app.state.client.build_request(
            request.method, url, content=body, headers=upstream_headers(request.headers, settings.upstream_api_key)
        )

    # -- routes ----------------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/stats")
    async def get_stats(request: Request):
        if denied := authenticate(request):
            return denied
        return {
            "version": __version__,
            "active": queue.active,
            "queued": queue.queued,
            "max_concurrency": queue.max_concurrency,
            "oldest_wait_s": round(queue.oldest_wait_s(), 1),
            **stats.snapshot(),
        }

    @app.get("/v1/models")
    async def models(request: Request):
        if denied := authenticate(request):
            return denied
        now = time.monotonic()
        if models_cache["status"] != 200 or now - models_cache["at"] > settings.models_cache_s:
            try:
                resp = await app.state.client.send(build_upstream_request(request, "models", None))
            except httpx.HTTPError as exc:
                return error_response(f"upstream unreachable: {type(exc).__name__}", 502, "upstream_error")
            models_cache.update(
                at=now, status=resp.status_code, body=resp.content, headers=response_headers(resp.headers)
            )
        return Response(models_cache["body"], models_cache["status"], headers=models_cache["headers"])

    @app.get("/v1/{path:path}")
    async def passthrough_get(path: str, request: Request):
        """Non-generation GETs don't use a slot."""
        if denied := authenticate(request):
            return denied
        try:
            resp = await app.state.client.send(build_upstream_request(request, path, None))
        except httpx.HTTPError as exc:
            return error_response(f"upstream unreachable: {type(exc).__name__}", 502, "upstream_error")
        return Response(resp.content, resp.status_code, headers=response_headers(resp.headers))

    @app.post("/v1/{path:path}")
    async def queued_post(path: str, request: Request):
        if denied := authenticate(request):
            return denied

        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > max_body:
            return error_response(f"request body exceeds {settings.max_body_mb} MB", 413, "request_too_large")
        chunks, size = [], 0
        async for chunk in request.stream():  # bounded read, even without Content-Length
            size += len(chunk)
            if size > max_body:
                return error_response(f"request body exceeds {settings.max_body_mb} MB", 413, "request_too_large")
            chunks.append(chunk)
        body = b"".join(chunks)

        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        rlog = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, settings))
        try:
            payload = json.loads(body) if body else {}
        except ValueError:
            payload = {}
        if isinstance(payload, dict):
            rlog.fields["model"] = payload.get("model")
            rlog.fields["stream"] = payload.get("stream") is True

        upstream_request = build_upstream_request(request, path, body)
        if rlog.fields["stream"]:
            return StreamingResponse(
                stream_flow(upstream_request, request_id, rlog),
                media_type="text/event-stream",
                headers={"X-Request-Id": request_id, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await run_until_disconnect(request, nonstream_flow(upstream_request, request_id, rlog), rlog)

    # -- non-streaming -------------------------------------------------------------------

    async def run_until_disconnect(request: Request, work, rlog: RequestLog) -> Response:
        """Run ``work`` but cancel it (freeing its slot) if the client goes away."""
        task = asyncio.ensure_future(work)
        while True:
            done, _ = await asyncio.wait({task}, timeout=1.0)
            if done:
                return task.result()
            if await request.is_disconnected():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                return Response(status_code=499)

    async def nonstream_flow(upstream_request: httpx.Request, request_id: str, rlog: RequestLog) -> Response:
        ticket = queue.enqueue(request_id)
        outcome, status, waits = "error", 500, []
        upstream_started = None
        try:
            timeout = settings.max_queue_wait_s or None
            if not await queue.wait(ticket, timeout):
                outcome, status = "timeout", 504
                return error_response("timed out waiting in queue", 504, "queue_timeout")
            await queue.apply_spacing()
            rlog.fields["queue_ms"] = round(ticket.wait_s * 1000)
            upstream_started = time.monotonic()

            response = None
            async for event in send_with_retries(
                app.state.client,
                upstream_request,
                settings.max_retries,
                settings.retry_base_delay_s,
                settings.retry_max_delay_s,
            ):
                if isinstance(event, Wait):
                    waits.append(event)
                    await asyncio.sleep(event.delay)
                else:
                    response = event
            try:
                content = await response.aread()
            finally:
                await response.aclose()

            status = response.status_code
            outcome = "done" if status < 400 else "error"
            prompt, completion = usage_from_body(content) if status < 400 else (None, None)
            rlog.fields.update(prompt_tokens=prompt, completion_tokens=completion)
            headers = response_headers(response.headers)
            headers.update({"X-Request-Id": request_id, "X-Queue-Wait-Ms": str(rlog.fields["queue_ms"])})
            return Response(content, status, headers=headers)
        except UpstreamFailed as exc:
            status = exc.status
            return error_response(exc.message, exc.status, "upstream_error")
        except httpx.HTTPError as exc:
            status = 502
            return error_response(f"upstream error: {type(exc).__name__}", 502, "upstream_error")
        except asyncio.CancelledError:
            outcome, status = "cancelled", 499
            raise
        finally:
            queue.cancel(ticket)
            upstream_ms = round((time.monotonic() - upstream_started) * 1000) if upstream_started else None
            rlog.finish(stats, outcome=outcome, status=status, retries=len(waits), upstream_ms=upstream_ms)

    # -- streaming -----------------------------------------------------------------------

    async def stream_flow(upstream_request: httpx.Request, request_id: str, rlog: RequestLog) -> AsyncIterator[bytes]:
        ticket = queue.enqueue(request_id)
        outcome, status, waits = "error", 200, []
        upstream_started = None
        response = None
        sniffer = UsageSniffer()
        keepalive = settings.keepalive_interval_s
        try:
            if not ticket.granted:
                yield f": queued id={request_id} position={queue.position(ticket)}\n\n".encode()
            while not await queue.wait(ticket, keepalive):
                if settings.max_queue_wait_s and ticket.wait_s >= settings.max_queue_wait_s:
                    outcome, status = "timeout", 504
                    yield sse_error("timed out waiting in queue", 504, "queue_timeout")
                    return
                yield f": queued position={queue.position(ticket)} waited={int(ticket.wait_s)}s\n\n".encode()
            await queue.apply_spacing()
            rlog.fields["queue_ms"] = round(ticket.wait_s * 1000)
            upstream_started = time.monotonic()

            try:
                async for event in send_with_retries(
                    app.state.client,
                    upstream_request,
                    settings.max_retries,
                    settings.retry_base_delay_s,
                    settings.retry_max_delay_s,
                ):
                    if isinstance(event, Wait):
                        waits.append(event)
                        deadline = time.monotonic() + event.delay
                        note = f": upstream busy ({event.reason}), retry {event.attempt} in {event.delay:.0f}s\n\n"
                        yield note.encode()
                        while (remaining := deadline - time.monotonic()) > 0:
                            await asyncio.sleep(min(remaining, keepalive))
                            if deadline - time.monotonic() > 0:
                                yield b": waiting to retry\n\n"
                    else:
                        response = event
            except UpstreamFailed as exc:
                status = exc.status
                yield sse_error(exc.message, exc.status)
                return

            status = response.status_code
            if status != 200:
                content = await response.aread()
                try:
                    message = json.loads(content)["error"]["message"]
                except (ValueError, KeyError, TypeError):
                    message = content.decode(errors="replace")[:500] or f"upstream returned HTTP {status}"
                yield sse_error(message, status)
                return

            try:
                async for chunk in response.aiter_bytes():
                    sniffer.feed(chunk)
                    yield chunk
            except httpx.HTTPError as exc:
                status = 502
                yield sse_error(f"upstream stream interrupted: {type(exc).__name__}", 502)
                return
            outcome = "done"
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected: Starlette cancels us, or the generator is closed.
            outcome = "cancelled"
            raise
        finally:
            if response is not None:
                await response.aclose()
            queue.cancel(ticket)
            upstream_ms = round((time.monotonic() - upstream_started) * 1000) if upstream_started else None
            rlog.finish(
                stats,
                outcome=outcome,
                status=status,
                retries=len(waits),
                upstream_ms=upstream_ms,
                prompt_tokens=sniffer.prompt_tokens,
                completion_tokens=sniffer.completion_tokens,
            )

    return app


def build_app() -> FastAPI:
    """Entry point for uvicorn (``--factory``)."""
    return create_app(Settings.from_env())
