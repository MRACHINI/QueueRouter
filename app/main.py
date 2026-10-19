"""QueueRouter: an OpenAI-compatible proxy that queues instead of exceeding the upstream's
concurrency limit."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import __version__
from .auth import AuthGuard, client_ip, find_client, presented_key
from .config import PRIORITIES, ClientKey, Settings
from .sessions import session_key
from .slots import LANE_NAMES, LANES, SlotQueue
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
            "client": None,
            "priority": None,
            "session": None,
            "model": None,
            "stream": False,
            "status": None,
            "outcome": None,
            "position": 0,
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
            f["outcome"],
            f["queue_ms"],
            f["upstream_ms"],
            f["retries"],
            f["prompt_tokens"],
            f["completion_tokens"],
            client=f["client"] or "default",
        )
        log.info(json.dumps(f))


@dataclass
class Admission:
    """Everything the queue needs to know about one request."""

    request_id: str
    client: str
    lane: int
    session: str | None
    rlog: RequestLog
    hints: dict[str, str]


def create_app(settings: Settings, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    settings.validate()
    logging.basicConfig(level=settings.log_level, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # our own request log covers upstream calls

    clients = settings.clients
    queue = SlotQueue(
        settings.max_concurrency,
        settings.min_spacing_ms,
        aging_s=settings.aging_s,
        session_window_s=settings.session_window_s,
        session_aware=settings.session_aware,
        fair_share=settings.fair_share,
    )
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
        startup = {
            "event": "startup",
            "version": __version__,
            "upstream": settings.upstream_url,
            "max_concurrency": settings.max_concurrency,
            "clients": {c.name: c.priority for c in clients} or "no auth",
            "scheduling": {
                "aging_s": settings.aging_s,
                "session_aware": settings.session_aware,
                "session_window_s": settings.session_window_s,
                "fair_share": settings.fair_share,
            },
        }
        log.info(json.dumps(startup))
        yield
        await app.state.client.aclose()

    app = FastAPI(
        title="QueueRouter", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )

    # -- helpers ---------------------------------------------------------------------------

    def authenticate(request: Request) -> tuple[ClientKey | None, JSONResponse | None]:
        """The calling client, or an error response to return instead."""
        ip = client_ip(request, settings)
        if remaining := guard.is_blocked(ip):
            retry_after = {"Retry-After": str(int(remaining) + 1)}
            return None, error_response("too many failed authentication attempts", 429, "auth_blocked", **retry_after)
        client = find_client(presented_key(request), clients, settings.allow_no_auth)
        if client is None:
            guard.fail(ip)
            return None, error_response("invalid or missing API key", 401, "invalid_api_key")
        return client, None

    def request_lane(request: Request, client: ClientKey) -> int:
        """Client's priority, optionally lowered (never raised) by X-QueueRouter-Priority."""
        lane = LANES[client.priority]
        wanted = request.headers.get("x-queuerouter-priority", "").strip().lower()
        if wanted in PRIORITIES:
            lane = max(lane, LANES[wanted])
        return lane

    def estimate_wait_ms(position: int) -> int:
        """Rough wait before a request at ``position`` starts, from recent upstream durations."""
        if position <= 0:
            return 0
        return round(math.ceil(position / queue.max_concurrency) * stats.avg_upstream_ms())

    def queue_state() -> dict:
        return {
            "active": queue.active,
            "queued": queue.queued,
            "max_concurrency": queue.max_concurrency,
            "oldest_wait_s": round(queue.oldest_wait_s(), 1),
            **queue.snapshot(),
        }

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
        _, denied = authenticate(request)
        if denied:
            return denied
        return {"version": __version__, **queue_state(), **stats.snapshot()}

    @app.get("/v1/queue")
    async def get_queue(request: Request):
        """Queue hints for orchestrators deciding how many requests to send at once."""
        client, denied = authenticate(request)
        if denied:
            return denied
        state = queue_state()
        mine = state["clients"].get(client.name, {"queued": 0, "active": 0})
        position = queue.predict_position(client.name, LANES[client.priority], None)
        return {
            "active": state["active"],
            "queued": state["queued"],
            "max_concurrency": state["max_concurrency"],
            "queued_by_lane": state["queued_by_lane"],
            "client": {"name": client.name, "priority": client.priority, **mine},
            "estimated_wait_ms": estimate_wait_ms(position),
        }

    @app.get("/v1/models")
    async def models(request: Request):
        _, denied = authenticate(request)
        if denied:
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
        _, denied = authenticate(request)
        if denied:
            return denied
        try:
            resp = await app.state.client.send(build_upstream_request(request, path, None))
        except httpx.HTTPError as exc:
            return error_response(f"upstream unreachable: {type(exc).__name__}", 502, "upstream_error")
        return Response(resp.content, resp.status_code, headers=response_headers(resp.headers))

    @app.post("/v1/{path:path}")
    async def queued_post(path: str, request: Request):
        client, denied = authenticate(request)
        if denied:
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

        try:
            payload = json.loads(body) if body else {}
        except ValueError:
            payload = {}

        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        lane = request_lane(request, client)
        session = session_key(request.headers, client.name, payload)
        position = queue.predict_position(client.name, lane, session)
        rlog = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, settings))
        rlog.fields.update(
            client=client.name, priority=LANE_NAMES[lane], session=session and session[:8], position=position
        )
        if isinstance(payload, dict):
            rlog.fields["model"] = payload.get("model")
            rlog.fields["stream"] = payload.get("stream") is True

        hints = {
            "X-Request-Id": request_id,
            "X-QueueRouter-Queue-Depth": str(queue.queued),
            "X-QueueRouter-Position": str(position),
            "X-QueueRouter-Est-Wait-Ms": str(estimate_wait_ms(position)),
            "X-QueueRouter-Slots": f"{queue.active}/{queue.max_concurrency}",
        }
        admission = Admission(request_id, client.name, lane, session, rlog, hints)
        upstream_request = build_upstream_request(request, path, body)
        if rlog.fields["stream"]:
            return StreamingResponse(
                stream_flow(upstream_request, admission),
                media_type="text/event-stream",
                headers={**hints, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await run_until_disconnect(request, nonstream_flow(upstream_request, admission))

    # -- non-streaming -------------------------------------------------------------------

    async def run_until_disconnect(request: Request, work) -> Response:
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

    async def nonstream_flow(upstream_request: httpx.Request, adm: Admission) -> Response:
        rlog = adm.rlog
        ticket = queue.enqueue(adm.request_id, adm.client, adm.lane, adm.session)
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
            headers.update(adm.hints)
            headers["X-Queue-Wait-Ms"] = str(rlog.fields["queue_ms"])
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

    async def stream_flow(upstream_request: httpx.Request, adm: Admission) -> AsyncIterator[bytes]:
        rlog = adm.rlog
        ticket = queue.enqueue(adm.request_id, adm.client, adm.lane, adm.session)
        outcome, status, waits = "error", 200, []
        upstream_started = None
        response = None
        sniffer = UsageSniffer()
        keepalive = settings.keepalive_interval_s
        try:
            if not ticket.granted:
                position = queue.position(ticket)
                note = f": queued id={adm.request_id} position={position} est_wait_ms={estimate_wait_ms(position)}\n\n"
                yield note.encode()
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
