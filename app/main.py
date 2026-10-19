"""QueueRouter: an OpenAI-compatible proxy that queues instead of exceeding the upstreams'
concurrency limits."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import sys
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from . import __version__
from .admin import AdminContext, mount_admin
from .auth import AuthGuard, client_ip, find_client, presented_key
from .catalog import Catalogs, fetch_models
from .circuit import Circuits
from .config import PRIORITIES, Bootstrap, ClientKey, ClientPolicy, Settings
from .ratelimit import RateLimiter
from .sessions import session_key
from .slots import LANE_NAMES, LANES, Candidate, SlotQueue
from .state import MODES, Target, load_snapshot, open_store
from .stats import Stats
from .store import DataDirError
from .upstream import (
    BUSY_STATUS,
    FAILURE_STATUS,
    RETRYABLE_ERRORS,
    UsageSniffer,
    finish_reason_from_body,
    response_headers,
    retry_delay,
    upstream_headers,
    usage_from_body,
)

log = logging.getLogger("queuerouter")

HEALTH_INTERVAL_S = 30
CATALOG_MAX_AGE_S = 3600


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
            "upstream": None,
            "stream": False,
            "status": None,
            "outcome": None,
            "position": 0,
            "queue_ms": None,
            "upstream_ms": None,
            "retries": 0,
            "prompt_tokens": None,
            "completion_tokens": None,
            "finish_reason": None,
            "target_model": None,
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
        entry = {**f, "ts": time.time()}
        stats.recent.append(entry)
        if stats.sink:
            stats.sink(entry)
        log.info(json.dumps(f))


@dataclass
class Admission:
    """Everything needed to queue and send one request."""

    request_id: str
    client: str
    lane: int
    session: str | None
    candidates: tuple[Candidate, ...]
    method: str
    path: str
    query: str
    headers: dict[str, str]
    body: bytes
    rlog: RequestLog
    hints: dict[str, str]
    policy: ClientPolicy = ClientPolicy()
    deadline: float | None = None  # monotonic time by which the whole request must finish

    def expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline


# Events from the attempt loop to the response writers.
@dataclass
class Queued:
    position: int


@dataclass
class Waiting:
    position: int
    waited_s: float


@dataclass
class Backoff:
    delay: float
    attempt: int
    reason: str


@dataclass
class Got:
    response: httpx.Response
    target: Target


@dataclass
class Failed:
    status: int
    message: str
    kind: str = "upstream_error"


def create_app(
    bootstrap: Bootstrap, seed: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None
) -> FastAPI:
    """Build the app. ``seed`` is imported into the database only on the very first start."""
    logging.basicConfig(level=bootstrap.log_level, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # our own request log covers upstream calls

    store = open_store(bootstrap, seed)
    snapshot = load_snapshot(store)
    stats = Stats()
    catalogs = Catalogs()
    circuits = Circuits()
    guard = AuthGuard(snapshot.settings.auth_fail_limit, snapshot.settings.auth_block_s)

    def available(upstream_id: int) -> bool:
        target = snapshot.upstreams.get(upstream_id)
        return target is not None and target.usable and circuits.allows(upstream_id)

    limiter = RateLimiter()

    def policy_of(client: str) -> ClientPolicy:
        for key in snapshot.clients.values():
            if key.name == client:
                return key.policy
        return ClientPolicy()

    def eligible(ticket) -> bool:
        """Client-level gates checked whenever a slot could be handed out."""
        policy = policy_of(ticket.client)
        if policy.max_active is not None and queue.client_active(ticket.client) >= policy.max_active:
            return False
        return limiter.wait_s(ticket.client, policy) <= 0  # on_limit=delay: wait in the queue

    def on_grant(ticket) -> None:
        circuits.begin(ticket.target.upstream_id)
        limiter.record_request(ticket.client)

    queue = SlotQueue(available=available, on_grant=on_grant, eligible=eligible)

    def cfg() -> Settings:
        return snapshot.settings

    def apply_snapshot() -> None:
        s = snapshot.settings
        queue.set_scheduling(s.aging_s, s.session_window_s, s.session_aware, s.fair_share, s.lb_strategy)
        queue.paused = snapshot.mode == "paused"
        if snapshot.mode == "maintenance":
            queue.evict_waiters()
        queue.set_pools({p.id: (p.max_concurrency, p.min_spacing_ms) for p in snapshot.pools.values()})
        circuits.threshold, circuits.cooldown_s = s.circuit_threshold, s.circuit_cooldown_s
        guard.limit, guard.block_s = s.auth_fail_limit, s.auth_block_s
        catalogs.forget(set(snapshot.upstreams))

    def reload() -> None:
        """Re-read configuration from the database and apply it live."""
        nonlocal snapshot
        snapshot = load_snapshot(store)
        apply_snapshot()
        log.info(json.dumps({"event": "config_reloaded", **summary()}))

    def summary() -> dict:
        s = snapshot.settings
        return {
            "upstreams": {t.upstream.name: t.pool.name for t in snapshot.upstreams.values() if t.usable},
            "pools": {p.name: p.max_concurrency for p in snapshot.pools.values()},
            "clients": {c.name: c.priority for c in snapshot.clients.values()} or "none",
            "scheduling": {
                "aging_s": s.aging_s,
                "session_aware": s.session_aware,
                "fair_share": s.fair_share,
                "lb_strategy": s.lb_strategy,
            },
        }

    def on_circuit_change(upstream_id: int, state: str, reason: str) -> None:
        target = snapshot.upstreams.get(upstream_id)
        name = target.upstream.name if target else upstream_id
        log.warning(json.dumps({"event": "circuit", "upstream": name, "state": state, "reason": reason}))
        if state == "closed":
            queue.poke()

    circuits.on_change = on_circuit_change
    apply_snapshot()

    async def refresh_catalog(target: Target) -> bool:
        return await catalogs.refresh(app.state.client, target.id, target.upstream.base_url, target.api_key)

    pending_usage: list[dict] = []
    stats.sink = pending_usage.append

    async def flush_usage() -> None:
        """Write finished requests to the database off the event loop."""
        if not pending_usage:
            return
        rows = pending_usage[:]
        pending_usage.clear()
        try:
            await asyncio.to_thread(store.record_requests, rows, cfg().request_log_days > 0)
        except Exception as exc:  # never let logging break serving
            log.error(json.dumps({"event": "usage_write_failed", "error": repr(exc)[:200], "rows": len(rows)}))

    def restore_rate_windows() -> None:
        """After a restart, rebuild the last 24 h of rate-limit windows from the request log."""
        requests: dict[str, list[float]] = {}
        tokens: dict[str, list[tuple[float, int]]] = {}
        for row in store.recent_activity(time.time() - 86_400):
            started = row["ts"]  # close enough: rows are written when requests finish
            requests.setdefault(row["client"], []).append(started)
            used = (row["prompt_tokens"] or 0) + (row["completion_tokens"] or 0)
            if used:
                tokens.setdefault(row["client"], []).append((started, used))
        for client in requests:
            limiter.seed(client, requests[client], tokens.get(client, []))

    async def background() -> None:
        """Wake waiters when circuit cooldowns end; write usage; probe broken upstreams; refresh
        model lists; prune old request rows."""
        last_health = 0.0
        last_prune = 0.0
        while True:
            await asyncio.sleep(1)
            if queue.queued:
                queue.poke()
            await flush_usage()
            if time.monotonic() - last_prune > 3600:
                last_prune = time.monotonic()
                if cfg().request_log_days > 0:
                    await asyncio.to_thread(store.prune_requests, cfg().request_log_days)
            if time.monotonic() - last_health < HEALTH_INTERVAL_S:
                continue
            last_health = time.monotonic()
            for target in list(snapshot.upstreams.values()):
                if not target.usable:
                    continue
                state = circuits.state(target.id)
                if state == "half-open" and circuits.allows(target.id):
                    circuits.begin(target.id)
                    if await refresh_catalog(target):
                        circuits.success(target.id)
                    else:
                        circuits.failure(target.id, "health probe failed")
                elif state == "closed" and catalogs.stale(target.id, CATALOG_MAX_AGE_S):
                    await refresh_catalog(target)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = httpx.AsyncClient(
            transport=transport, limits=httpx.Limits(max_connections=200, max_keepalive_connections=40)
        )
        log.info(json.dumps({"event": "startup", "version": __version__, **summary()}))
        if not snapshot.configured:
            log.warning(json.dumps({"event": "no_upstream", "message": "add an upstream in the admin UI"}))
        for target in snapshot.upstreams.values():
            if target.usable:
                asyncio.ensure_future(refresh_catalog(target))
        restore_rate_windows()
        task = asyncio.ensure_future(background())
        yield
        task.cancel()
        await flush_usage()
        await app.state.client.aclose()
        store.close()

    app = FastAPI(
        title="QueueRouter", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.state.store = store
    app.state.reload = reload
    app.state.bootstrap = bootstrap
    app.state.stats = stats
    app.state.circuits = circuits

    # -- helpers ---------------------------------------------------------------------------

    def authenticate(request: Request) -> tuple[ClientKey | None, JSONResponse | None]:
        """The calling client, or an error response to return instead."""
        ip = client_ip(request, bootstrap.trusted_proxies)
        if remaining := guard.is_blocked(ip):
            retry_after = {"Retry-After": str(int(remaining) + 1)}
            return None, error_response("too many failed authentication attempts", 429, "auth_blocked", **retry_after)
        client = find_client(presented_key(request), snapshot.clients, cfg().allow_no_auth)
        if client is None:
            guard.fail(ip)
            return None, error_response("invalid or missing API key", 401, "invalid_api_key")
        return client, None

    def not_configured() -> JSONResponse | None:
        if not snapshot.configured:
            return error_response("no upstream configured yet: add one in the admin UI", 503, "not_configured")
        return None

    def mode_refusal() -> JSONResponse | None:
        """Draining and maintenance refuse new requests; paused still accepts (and queues)."""
        if snapshot.mode in ("draining", "maintenance"):
            message = snapshot.maintenance_message or f"QueueRouter is {snapshot.mode}; try again later"
            return error_response(message, 503, snapshot.mode, **{"Retry-After": "300"})
        return None

    def policy_refusal(client: ClientKey, model: str | None, target: str | None = None) -> JSONResponse | None:
        """Admission checks from the client's policy (the only place QueueRouter itself says no).
        For an alias, allowing either the alias or its target model is enough."""
        policy = client.policy
        if policy.allowed_models and model not in policy.allowed_models and target not in policy.allowed_models:
            return error_response(f"model '{model}' is not allowed for this key", 403, "model_not_allowed")
        if policy.max_queued is not None and queue.client_queued(client.name) >= policy.max_queued:
            return error_response(
                "too many queued requests for this key", 429, "client_queue_full", **{"Retry-After": "5"}
            )
        if policy.on_limit == "reject" and (wait := limiter.wait_s(client.name, policy)) > 0:
            return error_response(
                "rate limit reached for this key", 429, "rate_limited", **{"Retry-After": str(math.ceil(wait))}
            )
        return None

    def candidates_for(model: str | None, only: tuple[int, ...] = ()) -> tuple[Candidate, ...]:
        """Usable upstreams for ``model``: those that list it, or all of them if none does
        (the upstream then decides; lists may be stale or incomplete). ``only`` restricts the
        choice (an alias pinned to some upstreams)."""
        usable = [t for t in snapshot.upstreams.values() if t.usable and (not only or t.id in only)]
        if model:
            listing = [t for t in usable if (ids := catalogs.ids(t.id)) is not None and model in ids]
            unknown = [t for t in usable if catalogs.ids(t.id) is None]
            if listing:
                usable = listing + unknown
        order = {uid: i for i, uid in enumerate(snapshot.upstreams)}
        return tuple(Candidate(t.id, t.pool.id, t.upstream.weight, order[t.id]) for t in usable)

    def request_lane(request: Request, client: ClientKey) -> int:
        """Client's priority, optionally lowered (never raised) by X-QueueRouter-Priority."""
        lane = LANES[client.priority]
        wanted = request.headers.get("x-queuerouter-priority", "").strip().lower()
        if wanted in PRIORITIES:
            lane = max(lane, LANES[wanted])
        return lane

    def estimate_wait_ms(position: int) -> int:
        """Rough wait before a request at ``position`` starts, from recent upstream durations."""
        if position <= 0 or queue.max_concurrency <= 0:
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

    def upstream_info() -> list[dict]:
        active = queue.snapshot()["upstreams_active"]
        return [
            {
                "id": t.id,
                "name": t.upstream.name,
                "pool": t.pool.name,
                "usable": t.usable,
                "active": active.get(t.id, 0),
                **circuits.describe(t.id),
                **{f"catalog_{k}": v for k, v in catalogs.describe(t.id).items()},
            }
            for t in snapshot.upstreams.values()
        ]

    def live_state() -> dict:
        state = queue_state()
        pool_names = {p.id: p.name for p in snapshot.pools.values()}
        state["pools"] = {pool_names.get(pid, str(pid)): v for pid, v in state["pools"].items()}
        state.pop("upstreams_active", None)
        return {
            "version": __version__,
            "configured": snapshot.configured,
            "upstream": ", ".join(t.upstream.name for t in snapshot.upstreams.values() if t.usable) or None,
            "upstreams": upstream_info(),
            "mode": snapshot.mode,
            "maintenance_message": snapshot.maintenance_message,
            "modes": MODES,
            "rate_limits": {
                key.name: limiter.usage(key.name) for key in snapshot.clients.values() if key.policy.rate_limited
            },
            **state,
            **stats.snapshot(),
            "recent": list(stats.recent)[-25:],
        }

    async def probe(base_url: str, api_key: str) -> tuple[bool, str]:
        """Admin 'Test' button: can we list models with this URL and key?"""
        try:
            models = await fetch_models(app.state.client, base_url, api_key)
        except (httpx.HTTPError, ValueError) as exc:
            return False, f"{type(exc).__name__}: {exc}"[:300]
        sample = ", ".join(m["id"] for m in models[:5])
        return True, f"OK — {len(models)} models" + (f" (e.g. {sample})" if sample else "")

    def models_by_upstream() -> dict[str, list[str]]:
        return {t.upstream.name: sorted(catalogs.ids(t.id) or []) for t in snapshot.upstreams.values()}

    mount_admin(app, AdminContext(store, bootstrap, reload, live_state, probe, upstream_info, models_by_upstream))

    # -- the attempt loop ----------------------------------------------------------------

    def step(adm: Admission, keepalive: float) -> float:
        """How long to wait for a slot before the next keep-alive / deadline check."""
        if adm.deadline is None:
            return keepalive
        return max(0.0, min(keepalive, adm.deadline - time.monotonic()))

    def can_retry(adm: Admission, retries: int) -> bool:
        if adm.expired():
            return False
        budget = adm.policy.max_retries if adm.policy.max_retries is not None else cfg().max_retries
        return budget < 0 or retries < budget

    def build_request(adm: Admission, target: Target) -> httpx.Request:
        s = cfg()
        url = f"{target.upstream.base_url}/{adm.path}" + (f"?{adm.query}" if adm.query else "")
        return app.state.client.build_request(
            adm.method,
            url,
            content=adm.body or None,
            headers=upstream_headers(adm.headers, target.api_key),
            timeout=httpx.Timeout(
                connect=s.upstream_connect_timeout_s, read=s.upstream_read_timeout_s, write=30, pool=None
            ),
        )

    async def attempts(adm: Admission) -> AsyncIterator[Queued | Waiting | Backoff | Got | Failed | None]:
        """Queue for a slot, send, retry and fail over. Yields progress events, then exactly one
        ``Got`` / ``Failed`` (or ``None`` on a queue timeout). The granted slot is held until the
        consumer closes this generator, i.e. after the response has been fully forwarded."""
        ticket = None
        first_enqueued = first_seq = None
        excluded: set[int] = set()
        retries = 0
        verdict = False
        try:
            while True:
                pool = tuple(c for c in adm.candidates if c.upstream_id not in excluded) or adm.candidates
                ticket = queue.enqueue(
                    adm.request_id, adm.client, adm.lane, adm.session, pool, first_enqueued, first_seq
                )
                verdict = False
                if first_enqueued is None:
                    first_enqueued, first_seq = ticket.enqueued_at, ticket.seq
                    if not ticket.granted:
                        yield Queued(queue.position(ticket))
                keepalive = cfg().keepalive_interval_s
                while not await queue.wait(ticket, step(adm, keepalive)):
                    if ticket.future.cancelled():  # evicted: maintenance mode
                        message = snapshot.maintenance_message or "QueueRouter is in maintenance mode"
                        yield Failed(503, message, "maintenance")
                        return
                    max_wait = adm.policy.queue_timeout_s or cfg().max_queue_wait_s
                    if (max_wait and ticket.wait_s >= max_wait) or adm.expired():
                        yield None
                        return
                    yield Waiting(queue.position(ticket), ticket.wait_s)
                await queue.apply_spacing(ticket)
                adm.rlog.fields["queue_ms"] = round((time.monotonic() - first_enqueued) * 1000)

                target = snapshot.upstreams.get(ticket.target.upstream_id)
                if target is None:  # removed by an admin while we waited
                    excluded.add(ticket.target.upstream_id)
                    queue.cancel(ticket)
                    if not candidates_for(None):
                        yield Failed(503, "no upstream configured", "not_configured")
                        return
                    continue
                adm.rlog.fields["upstream"] = target.upstream.name

                while True:  # attempts on this slot
                    headers, response = None, None
                    try:
                        response = await app.state.client.send(build_request(adm, target), stream=True)
                    except RETRYABLE_ERRORS as exc:
                        reason = type(exc).__name__
                    else:
                        status = response.status_code
                        if status == BUSY_STATUS and can_retry(adm, retries):
                            delay = retry_delay(
                                retries, response.headers, cfg().retry_base_delay_s, cfg().retry_max_delay_s
                            )
                            await response.aclose()
                            retries += 1
                            yield Backoff(delay, retries, "HTTP 429")
                            continue
                        if status not in FAILURE_STATUS:
                            circuits.success(target.id)
                            verdict = True
                            yield Got(response, target)
                            return
                        reason, headers = f"HTTP {status}", response.headers

                    circuits.failure(target.id, reason)
                    verdict = True
                    if not can_retry(adm, retries):
                        if response is not None:
                            yield Got(response, target)  # pass the upstream's error through
                        else:
                            yield Failed(502, f"upstream unreachable: {reason}")
                        return
                    if response is not None:
                        await response.aclose()
                    retries += 1
                    others = [
                        c
                        for c in adm.candidates
                        if c.upstream_id != target.id and c.upstream_id not in excluded and available(c.upstream_id)
                    ]
                    if others:  # fail over, keeping our place in the queue
                        excluded.add(target.id)
                        queue.cancel(ticket)
                        ticket = None
                        yield Backoff(0, retries, f"{reason} on {target.upstream.name}, failing over")
                        break
                    delay = retry_delay(retries - 1, headers, cfg().retry_base_delay_s, cfg().retry_max_delay_s)
                    yield Backoff(delay, retries, reason)
        finally:
            adm.rlog.fields["retries"] = retries
            if ticket is not None:
                if ticket.target is not None and not verdict:
                    circuits.abandon(ticket.target.upstream_id)
                queue.cancel(ticket)

    # -- routes ----------------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/stats")
    async def get_stats(request: Request):
        _, denied = authenticate(request)
        if denied:
            return denied
        return {"version": __version__, **live_state()}

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
        """Union of all usable upstreams' model lists (cached; doesn't use a slot), limited to the
        client's allowed models."""
        client, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured():
            return unavailable
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        stale = [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s)]
        if stale:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))
        merged: dict[str, dict] = {}
        for target in targets:
            for model in catalogs.models(target.id):
                merged.setdefault(model["id"], model)
        if not merged and all(catalogs.ids(t.id) is None for t in targets):
            return error_response("no upstream returned a model list", 502, "upstream_error")
        for alias in snapshot.aliases.values():
            merged[alias.name] = alias_entry(alias, merged.get(alias.target_model))
        allowed = client.policy.allowed_models
        data = [
            m
            for m in merged.values()
            if not allowed
            or m["id"] in allowed
            or (m["id"] in snapshot.aliases and snapshot.aliases[m["id"]].target_model in allowed)
        ]
        return {"object": "list", "data": data}

    @app.get("/v1/{path:path}")
    async def passthrough_get(path: str, request: Request):
        """Non-generation GETs go to the first usable upstream and don't use a slot."""
        _, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured():
            return unavailable
        target = next(t for t in snapshot.upstreams.values() if t.usable)
        url = f"{target.upstream.base_url}/{path}" + (f"?{request.url.query}" if request.url.query else "")
        try:
            resp = await app.state.client.get(
                url,
                headers=upstream_headers(request.headers, target.api_key),
                timeout=httpx.Timeout(cfg().upstream_read_timeout_s, connect=cfg().upstream_connect_timeout_s),
            )
        except httpx.HTTPError as exc:
            return error_response(f"upstream unreachable: {type(exc).__name__}", 502, "upstream_error")
        return Response(resp.content, resp.status_code, headers=response_headers(resp.headers))

    @app.post("/v1/{path:path}")
    async def queued_post(path: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured() or mode_refusal():
            return unavailable

        max_body = int(cfg().max_body_mb * 1024 * 1024)
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > max_body:
            return error_response(f"request body exceeds {cfg().max_body_mb} MB", 413, "request_too_large")
        chunks, size = [], 0
        async for chunk in request.stream():  # bounded read, even without Content-Length
            size += len(chunk)
            if size > max_body:
                return error_response(f"request body exceeds {cfg().max_body_mb} MB", 413, "request_too_large")
            chunks.append(chunk)
        body = b"".join(chunks)

        try:
            payload = json.loads(body) if body else {}
        except ValueError:
            payload = {}
        model = payload.get("model") if isinstance(payload, dict) else None
        model = model if isinstance(model, str) else None
        alias = snapshot.aliases.get(model) if model else None
        target_model = alias.target_model if alias else model
        if refusal := policy_refusal(client, model, target_model):
            return refusal
        candidates = candidates_for(target_model, alias.upstream_ids if alias else ())
        if not candidates:
            return error_response(f"no usable upstream for '{model}' right now", 503, "no_upstream")
        if alias:  # the upstream gets the real model name
            payload["model"] = alias.target_model
            body = json.dumps(payload).encode()

        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        lane = request_lane(request, client)
        session = session_key(request.headers, client.name, payload)
        position = queue.predict_position(client.name, lane, session)
        rlog = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, bootstrap.trusted_proxies))
        rlog.fields.update(
            client=client.name,
            priority=LANE_NAMES[lane],
            session=session and session[:8],
            position=position,
            model=model,
            target_model=target_model if alias else None,
            stream=isinstance(payload, dict) and payload.get("stream") is True,
        )
        hints = {
            "X-Request-Id": request_id,
            "X-QueueRouter-Queue-Depth": str(queue.queued),
            "X-QueueRouter-Position": str(position),
            "X-QueueRouter-Est-Wait-Ms": str(estimate_wait_ms(position)),
            "X-QueueRouter-Slots": f"{queue.active}/{queue.max_concurrency}",
        }
        adm = Admission(
            request_id,
            client.name,
            lane,
            session,
            candidates,
            "POST",
            path,
            request.url.query,
            dict(request.headers),
            body,
            rlog,
            hints,
            client.policy,
            time.monotonic() + client.policy.total_timeout_s if client.policy.total_timeout_s else None,
        )
        if rlog.fields["stream"]:
            return StreamingResponse(
                stream_flow(adm),
                media_type="text/event-stream",
                headers={**hints, "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await run_until_disconnect(request, nonstream_flow(adm))

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
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                return Response(status_code=499)

    async def nonstream_flow(adm: Admission) -> Response:
        rlog = adm.rlog
        outcome, status = "error", 500
        remaining = adm.deadline - time.monotonic() if adm.deadline else None
        try:
            async with asyncio.timeout(remaining), contextlib.aclosing(attempts(adm)) as events:
                async for event in events:
                    if isinstance(event, Backoff):
                        await asyncio.sleep(event.delay)
                    elif event is None:
                        outcome, status = "timeout", 504
                        return error_response("timed out waiting in queue", 504, "queue_timeout")
                    elif isinstance(event, Failed):
                        status = event.status
                        return error_response(event.message, event.status, event.kind)
                    elif isinstance(event, Got):
                        response = event.response
                        try:
                            content = await response.aread()
                        finally:
                            await response.aclose()
                        status = response.status_code
                        outcome = "done" if status < 400 else "error"
                        if status < 400:
                            prompt, completion = usage_from_body(content)
                            rlog.fields.update(
                                prompt_tokens=prompt,
                                completion_tokens=completion,
                                finish_reason=finish_reason_from_body(content),
                            )
                        headers = response_headers(response.headers)
                        headers.update(adm.hints)
                        headers["X-Queue-Wait-Ms"] = str(rlog.fields["queue_ms"])
                        return Response(content, status, headers=headers)
            return error_response("no response", 502, "upstream_error")
        except TimeoutError:
            outcome, status = "timeout", 504
            return error_response("request exceeded this key's total time limit", 504, "request_timeout")
        except httpx.HTTPError as exc:
            status = 502
            return error_response(f"upstream error: {type(exc).__name__}", 502, "upstream_error")
        except asyncio.CancelledError:
            outcome, status = "cancelled", 499
            raise
        finally:
            started = _granted_at(rlog)
            upstream_ms = round((time.monotonic() - started) * 1000) if started else None
            rlog.finish(stats, outcome=outcome, status=status, upstream_ms=upstream_ms)
            limiter.record_tokens(
                adm.client, (rlog.fields["prompt_tokens"] or 0) + (rlog.fields["completion_tokens"] or 0)
            )

    # -- streaming -----------------------------------------------------------------------

    async def stream_flow(adm: Admission) -> AsyncIterator[bytes]:
        rlog = adm.rlog
        outcome, status = "error", 200
        response = None
        sniffer = UsageSniffer()
        try:
            async with contextlib.aclosing(attempts(adm)) as events:
                async for event in events:
                    if isinstance(event, Queued):
                        note = (
                            f": queued id={adm.request_id} position={event.position} "
                            f"est_wait_ms={estimate_wait_ms(event.position)}\n\n"
                        )
                        yield note.encode()
                    elif isinstance(event, Waiting):
                        yield f": queued position={event.position} waited={int(event.waited_s)}s\n\n".encode()
                    elif isinstance(event, Backoff):
                        yield f": {event.reason}, retry {event.attempt} in {event.delay:.0f}s\n\n".encode()
                        deadline = time.monotonic() + event.delay
                        while (remaining := deadline - time.monotonic()) > 0:
                            await asyncio.sleep(min(remaining, cfg().keepalive_interval_s))
                            if deadline - time.monotonic() > 0:
                                yield b": waiting to retry\n\n"
                    elif event is None:
                        outcome, status = "timeout", 504
                        yield sse_error("timed out waiting in queue", 504, "queue_timeout")
                        return
                    elif isinstance(event, Failed):
                        status = event.status
                        yield sse_error(event.message, event.status, event.kind)
                        return
                    elif isinstance(event, Got):
                        response = event.response
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
                                if adm.expired():
                                    outcome, status = "timeout", 504
                                    yield sse_error(
                                        "request exceeded this key's total time limit", 504, "request_timeout"
                                    )
                                    return
                        except httpx.HTTPError as exc:
                            status = 502
                            yield sse_error(f"upstream stream interrupted: {type(exc).__name__}", 502)
                            return
                        outcome = "done"
                        return
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected: Starlette cancels us, or the generator is closed.
            outcome = "cancelled"
            raise
        finally:
            if response is not None:
                await response.aclose()
            started = _granted_at(rlog)
            upstream_ms = round((time.monotonic() - started) * 1000) if started else None
            rlog.finish(
                stats,
                outcome=outcome,
                status=status,
                upstream_ms=upstream_ms,
                prompt_tokens=sniffer.prompt_tokens,
                completion_tokens=sniffer.completion_tokens,
                finish_reason=sniffer.finish_reason,
            )
            limiter.record_tokens(adm.client, (sniffer.prompt_tokens or 0) + (sniffer.completion_tokens or 0))

    return app


CONTEXT_FIELDS = ("context_length", "max_model_len", "max_context_length", "max_output_tokens", "max_tokens")


def alias_entry(alias, target: dict | None) -> dict:
    """A /v1/models entry for an alias, with the target's context-size fields so apps size prompts right."""
    entry = {"id": alias.name, "object": "model", "created": 0, "owned_by": "queuerouter"}
    for key in CONTEXT_FIELDS:
        if target and key in target:
            entry[key] = target[key]
    entry["queuerouter"] = {"type": "alias", "target": alias.target_model}
    return entry


def _granted_at(rlog: RequestLog) -> float | None:
    """When the request first got a slot (start time + queue wait), if it did."""
    if rlog.fields["queue_ms"] is None:
        return None
    return rlog.started + rlog.fields["queue_ms"] / 1000


def build_app() -> FastAPI:
    """Entry point for uvicorn (``--factory``)."""
    try:
        return create_app(Bootstrap.from_env(), seed=Settings.from_env())
    except DataDirError as exc:  # a configuration problem, not a bug: no traceback
        print(f"QueueRouter cannot start: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(78) from None  # EX_CONFIG
