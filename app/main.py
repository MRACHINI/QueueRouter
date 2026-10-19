"""QueueRouter: an OpenAI-compatible proxy that queues instead of exceeding the upstreams'
concurrency limits."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import functools
import hmac
import json
import logging
import math
import os
import signal
import sys
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from . import __version__, anthropic_api, content_log, metrics, responses_api, structured
from . import jobs as jobs_mod
from .admin import AdminContext, mount_admin
from .agents import Run, Runs, parse_tags, request_fingerprint, run_key
from .alerts import Alerts
from .auth import AuthGuard, client_ip, find_client, ip_in, presented_key
from .backup import auto_backup
from .catalog import Catalogs, fetch_models
from .circuit import Circuits
from .config import PRIORITIES, Bootstrap, ClientKey, ClientPolicy, Settings
from .fallback import Replay, is_policy_error, is_refusal_text, message_text, sniff_stream
from .opencode import PLUGIN_PATH, model_limits, plugin_source, provider_config, wellknown_manifest
from .params import apply_rules, parse_rules, requested_output, too_long
from .pricing import PriceCatalog
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
HISTORY_POINTS, HISTORY_EVERY_S = 360, 5  # dashboard chart: 30 minutes
CONTENT_CAPTURE_MAX = 16 * 1024 * 1024  # streamed reply bytes kept for content logging (cut later)
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
        self.content: tuple[bytes, bytes] | None = None  # (request, reply) when content logging is on
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
            "cached_tokens": None,
            "finish_reason": None,
            "fallback": None,
            "tags": None,
            "run": None,
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
        if self.content is not None and stats.content_sink:
            stats.content_sink(f["id"], f["client"] or "default", *self.content)
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
    steps: tuple[Step, ...] = ()  # alias fallback chain (first = primary); empty = just this request
    triggers: frozenset[str] = frozenset()
    run: Run | None = None  # agent run this request belongs to (budgets, loop detection)
    log_request: bytes | None = None  # set when the client has content logging on

    def expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline


@dataclass(frozen=True)
class Step:
    """One model in an alias's fallback chain."""

    model: str
    candidates: tuple[Candidate, ...]
    body: bytes


@dataclass
class FellBack:
    """Moving on to the next model in the chain."""

    from_model: str
    to_model: str
    reason: str


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
    prices = PriceCatalog(os.path.join(bootstrap.data_dir, "models-dev-prices.json"))
    prices.load_file()
    circuits = Circuits()
    alerts = Alerts(store, lambda: app.state.client)
    guard = AuthGuard(snapshot.settings.auth_fail_limit, snapshot.settings.auth_block_s)

    def available(upstream_id: int) -> bool:
        target = snapshot.upstreams.get(upstream_id)
        return target is not None and target.usable and circuits.allows(upstream_id)

    limiter = RateLimiter()
    runs = Runs()

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
        if limiter.wait_s(ticket.client, policy) > 0:  # on_limit=delay: wait in the queue
            alerts.client_limited(ticket.client, rejected=False)
            return False
        return True

    affinity: dict[str, tuple[int, float]] = {}  # session -> (upstream id, last used, monotonic)

    def on_grant(ticket) -> None:
        circuits.begin(ticket.target.upstream_id)
        limiter.record_request(ticket.client)
        if ticket.session is not None:
            affinity[ticket.session] = (ticket.target.upstream_id, time.monotonic())
            if len(affinity) > 20_000:  # bound memory: drop the stalest half
                for session, _ in sorted(affinity.items(), key=lambda kv: kv[1][1])[:10_000]:
                    affinity.pop(session, None)

    def sticky_upstream(session: str | None) -> int | None:
        if not session or not cfg().sticky_routing:
            return None
        entry = affinity.get(session)
        if entry is None or time.monotonic() - entry[1] > cfg().affinity_ttl_s:
            return None
        return entry[0]

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
        alerts.circuit(str(name), state, reason)
        if state == "closed":
            queue.poke()

    circuits.on_change = on_circuit_change
    apply_snapshot()

    async def refresh_catalog(target: Target) -> bool:
        return await catalogs.refresh(app.state.client, target.id, target.upstream.base_url, target.api_key)

    pending_usage: list[dict] = []

    def sink(entry: dict) -> None:
        pending_usage.append(entry)
        alerts.observe(entry)

    stats.sink = sink
    pending_content: list[tuple[str, float, str, bytes, bytes]] = []
    stats.content_sink = lambda rid, client, req, resp: pending_content.append((rid, time.time(), client, req, resp))

    async def flush_usage() -> None:
        """Write finished requests (and logged content) to the database off the event loop."""
        if pending_usage:
            rows = pending_usage[:]
            pending_usage.clear()
            try:
                await asyncio.to_thread(store.record_requests, rows, cfg().request_log_days > 0)
            except Exception as exc:  # never let logging break serving
                log.error(json.dumps({"event": "usage_write_failed", "error": repr(exc)[:200], "rows": len(rows)}))
        if pending_content:
            limit = cfg().content_log_max_kb * 1024
            contents = []
            for rid, ts, client, req, resp in pending_content:
                request_text, cut_req = content_log.clip(req.decode("utf-8", "replace"), limit)
                reply_text, cut_resp = content_log.clip(content_log.readable_reply(resp), limit)
                contents.append((rid, ts, client, request_text, reply_text, cut_req or cut_resp))
            pending_content.clear()
            try:
                await asyncio.to_thread(store.save_contents, contents)
            except Exception as exc:
                log.error(json.dumps({"event": "content_write_failed", "error": repr(exc)[:200]}))

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

    price_attempt = [0.0]  # last catalog download attempt (at most hourly when failing)
    history: collections.deque[tuple[float, int, int]] = collections.deque(maxlen=HISTORY_POINTS)

    async def background() -> None:
        """Wake waiters when circuit cooldowns end; write usage; probe broken upstreams; refresh
        model lists; prune old request rows."""
        last_health = 0.0
        last_prune = 0.0
        last_sample = 0.0
        peak = [0, 0]  # active, queued since the last sample
        while True:
            await asyncio.sleep(1)
            peak[0], peak[1] = max(peak[0], queue.active), max(peak[1], queue.queued)
            if time.monotonic() - last_sample >= HISTORY_EVERY_S:  # chart: the peak of each interval
                last_sample = time.monotonic()
                history.append((round(time.time()), peak[0], peak[1]))
                peak[0] = peak[1] = 0
            if queue.queued:
                queue.poke()
            await flush_usage()
            if snapshot.mode == "running":
                try:
                    await job_runner.tick(cfg().max_jobs_in_flight, max(0, queue.queued - len(job_runner.running)))
                except Exception as exc:  # jobs must never stop the loop
                    log.error(json.dumps({"event": "job_tick_failed", "error": repr(exc)[:200]}))
            try:
                alerts.check(queue.queued, queue.oldest_wait_s())
                await alerts.check_slow()
            except Exception as exc:  # alerts must never stop the loop
                log.error(json.dumps({"event": "alert_check_failed", "error": repr(exc)[:200]}))
            if cfg().price_catalog_enabled and prices.stale and time.time() - price_attempt[0] > 3600:
                price_attempt[0] = time.time()
                asyncio.ensure_future(prices.refresh(app.state.client))  # don't hold up the loop
            if time.monotonic() - last_prune > 3600:
                last_prune = time.monotonic()
                if cfg().request_log_days > 0:
                    await asyncio.to_thread(store.prune_requests, cfg().request_log_days)
                await asyncio.to_thread(store.prune_notifications)
                await asyncio.to_thread(store.prune_responses, responses_api.STORE_DAYS)
                await asyncio.to_thread(store.prune_jobs, cfg().job_retention_days)
                await asyncio.to_thread(store.prune_content, cfg().content_log_days)
                try:
                    await asyncio.to_thread(auto_backup, store, bootstrap.data_dir, cfg().backup_keep_days)
                except Exception as exc:  # e.g. disk full: say so, keep serving
                    log.error(json.dumps({"event": "auto_backup_failed", "error": repr(exc)[:200]}))
                    alerts.fire("backup_failed", "", "Automatic backup failed", repr(exc)[:300])
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
        alerts.started(__version__)
        if requeued := job_runner.start():
            log.info(json.dumps({"event": "jobs_resumed", "jobs": requeued}))
        task = asyncio.ensure_future(background())
        yield
        task.cancel()
        await job_runner.stop()
        await flush_usage()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(alerts.drain(), 15)
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
    app.state.prices = prices
    app.state.alerts = alerts
    app.state.restart = lambda: os.kill(os.getpid(), signal.SIGTERM)  # Docker's restart policy starts us again

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
            alerts.client_limited(client.name, rejected=True)
            return error_response(
                "rate limit reached for this key", 429, "rate_limited", **{"Retry-After": str(math.ceil(wait))}
            )
        return None

    def model_context(model: str | None, candidates: tuple[Candidate, ...]) -> int | None:
        """Context the request can count on: the smallest across the upstreams it may go to
        (model's own limit, capped by each pool's plan). None when unknown."""
        contexts = []
        for candidate in candidates:
            target = snapshot.upstreams.get(candidate.upstream_id)
            if target is None:
                continue
            entry = next((m for m in catalogs.models(target.id) if m["id"] == model), None)
            limits = model_limits(entry, [target.pool.context_cap])
            if limits is None:
                return None  # one candidate with unknown context: don't guess
            contexts.append(limits["context"])
        return min(contexts) if contexts else None

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

    def request_lane(headers: Mapping[str, str], client: ClientKey) -> int:
        """Client's priority, optionally lowered (never raised) by X-QueueRouter-Priority."""
        lane = LANES[client.priority]
        wanted = headers.get("x-queuerouter-priority", "").strip().lower()
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
            "runs": runs.snapshot(),
            "alerts_unread": store.unread_notifications(),
            "history": list(history),
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

    async def refresh_prices() -> bool:
        return await prices.refresh(app.state.client)

    async def client_opencode_config(request: Request, client_name: str) -> dict | None:
        """The same config /v1/opencode-config gives that client (for the admin download)."""
        client = next((c for c in snapshot.clients.values() if c.name == client_name), None)
        if client is None:
            return None
        return provider_config(public_base(request), await opencode_models(client))

    async def playground(request: Request, client_name: str, body: bytes) -> Response:
        """Admin Playground: a chat request *as* a client, through the normal pipeline."""
        client = next((c for c in snapshot.clients.values() if c.name == client_name), None)
        if client is None:
            return error_response(f"no enabled client named '{client_name}'", 404, "unknown_client")
        headers = {"content-type": "application/json", "x-queuerouter-tags": "playground"}
        return await serve(request, client, "chat/completions", body, headers=headers)

    mount_admin(
        app,
        AdminContext(
            store,
            bootstrap,
            reload,
            live_state,
            probe,
            upstream_info,
            models_by_upstream,
            prices,
            refresh_prices,
            playground,
            client_opencode_config,
            alerts,
            lambda: app.state.restart(),
        ),
    )

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
                preferred = sticky_upstream(adm.session)
                ticket = queue.enqueue(
                    adm.request_id,
                    adm.client,
                    adm.lane,
                    adm.session,
                    pool,
                    first_enqueued,
                    first_seq,
                    preferred=preferred if preferred not in excluded else None,
                    prefer_wait_s=cfg().affinity_wait_s,
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

    async def chained(adm: Admission) -> AsyncIterator:
        """``attempts`` over the alias's fallback chain: on a trigger (unavailable / errors /
        refused) the request moves on to the next model, unchanged otherwise. Same events as
        ``attempts`` plus ``FellBack``; responses may be ``Replay`` wrappers (already inspected)."""
        steps = adm.steps or (Step(adm.rlog.fields["model"] or "", adm.candidates, adm.body),)
        triggers = adm.triggers
        for index, step in enumerate(steps):
            last = index == len(steps) - 1
            nxt = None if last else steps[index + 1].model

            def fall(reason: str, step=step, nxt=nxt):
                adm.rlog.fields["fallback"] = f"{step.model} -> {nxt} ({reason})"
                return FellBack(step.model, nxt, reason)

            if not last and "unavailable" in triggers and not any(available(c.upstream_id) for c in step.candidates):
                yield fall("unavailable")
                continue
            if index:
                adm.rlog.fields["target_model"] = step.model
            step_adm = dataclasses.replace(adm, candidates=step.candidates, body=step.body)
            async with contextlib.aclosing(attempts(step_adm)) as events:
                moved_on = False
                async for event in events:
                    if last or not isinstance(event, Got | Failed):
                        yield event
                        if isinstance(event, Got | Failed) or event is None:
                            return
                        continue
                    if isinstance(event, Failed):
                        if event.status >= 500 and "errors" in triggers:
                            yield fall(event.message)
                            moved_on = True
                            break
                        yield event
                        return
                    response = event.response
                    status = response.status_code
                    if status >= 500 and "errors" in triggers:
                        await response.aclose()
                        yield fall(f"HTTP {status}")
                        moved_on = True
                        break
                    if "refused" in triggers:
                        if 400 <= status < 500:
                            content = await response.aread()
                            if is_policy_error(status, content):
                                await response.aclose()
                                yield fall(f"refused (HTTP {status})")
                                moved_on = True
                                break
                            yield Got(Replay(response, content, complete=True), event.target)
                            return
                        if status == 200:
                            if adm.rlog.fields["stream"]:
                                refused, replay = await sniff_stream(response)
                            else:
                                content = await response.aread()
                                refused = is_refusal_text(*message_text(content))
                                replay = Replay(response, content, complete=True)
                            if refused:
                                await replay.aclose()
                                yield fall("refused")
                                moved_on = True
                                break
                            yield Got(replay, event.target)
                            return
                    yield event
                    return
                if not moved_on:
                    return

    # -- routes ----------------------------------------------------------------------------

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse("/admin", status_code=303)

    def public_base(request: Request) -> str:
        """The externally visible base URL (PUBLIC_URL, else what the reverse proxy says)."""
        if bootstrap.public_url:
            return bootstrap.public_url
        scheme, host = request.url.scheme, request.headers.get("host", request.url.netloc)
        peer = request.client.host if request.client else ""
        if ip_in(peer, bootstrap.trusted_proxies):
            scheme = request.headers.get("x-forwarded-proto", scheme).split(",")[0].strip()
            host = request.headers.get("x-forwarded-host", host).split(",")[0].strip()
        return f"{scheme}://{host}"

    async def opencode_models(client: ClientKey) -> dict[str, dict]:
        """Every model and alias ``client`` may use, with opencode limits (plan caps applied)."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        stale = [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s)]
        if stale:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))
        entries: dict[str, dict] = {}
        caps: dict[str, list[int | None]] = {}
        for target in targets:
            for model in catalogs.models(target.id):
                entries.setdefault(model["id"], model)
                caps.setdefault(model["id"], []).append(target.pool.context_cap)
        allowed = client.policy.allowed_models
        result: dict[str, dict] = {}
        for model_id, entry in sorted(entries.items()):
            if not allowed or model_id in allowed:
                limits = model_limits(entry, caps[model_id])
                result[model_id] = {"name": model_id, **({"limit": limits} if limits else {})}
        for alias in sorted(snapshot.aliases.values(), key=lambda a: a.name):
            if allowed and alias.name not in allowed and alias.target_model not in allowed:
                continue
            pinned = [t for t in targets if not alias.upstream_ids or t.id in alias.upstream_ids]
            limits = model_limits(entries.get(alias.target_model), [t.pool.context_cap for t in pinned])
            result[alias.name] = {
                "name": f"{alias.name} → {alias.target_model}",
                **({"limit": limits} if limits else {}),
            }
        return result

    @app.get("/v1/opencode-config")
    async def opencode_config(request: Request):
        """opencode provider config with this client's models (see app/opencode.py)."""
        client, denied = authenticate(request)
        if denied:
            return denied
        return provider_config(public_base(request), await opencode_models(client))

    @app.get("/.well-known/opencode")
    async def wellknown_posix(request: Request):
        return wellknown_manifest(public_base(request), "posix")

    @app.get("/windows/.well-known/opencode")
    async def wellknown_windows(request: Request):
        return wellknown_manifest(public_base(request), "windows")

    @app.get(PLUGIN_PATH)
    async def opencode_plugin(request: Request):
        """The opencode plugin (session tags, priorities, model list); holds no secrets."""
        return Response(plugin_source(public_base(request)), media_type="text/javascript; charset=utf-8")

    @app.get("/metrics")
    async def prometheus(request: Request):
        """Prometheus metrics; needs the metrics token from Settings (404 while none is set)."""
        token = cfg().metrics_token
        presented = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        if not token:
            return error_response("not found", 404, "not_found")
        ip = client_ip(request, bootstrap.trusted_proxies)
        if guard.is_blocked(ip):
            return error_response("too many failed attempts", 429, "blocked")
        if not hmac.compare_digest(presented.encode(), token.encode()):
            guard.fail(ip)
            return error_response("invalid metrics token", 401, "invalid_api_key")
        jobs_now = await asyncio.to_thread(store.jobs_by_status)
        return Response(
            metrics.render(stats, live_state(), jobs_now, __version__), media_type="text/plain; version=0.0.4"
        )

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

    async def _never_disconnects() -> dict:
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def execute_job(row, request_body: dict) -> tuple[int, bytes]:
        """Run a stored job through the normal pipeline (non-streaming), as its client."""
        client = next((c for c in snapshot.clients.values() if c.name == row["client"]), None)
        if client is None:
            body = error_body("the client of this job no longer exists or is disabled", 403, "client_unavailable")
            return 403, json.dumps(body).encode()
        path = row["endpoint"].removeprefix("/v1/")
        scope = {
            "type": "http",
            "method": "POST",
            "path": row["endpoint"],
            "raw_path": row["endpoint"].encode(),
            "query_string": b"",
            "headers": [],
            "client": ("job", 0),
            "server": None,
            "scheme": "http",
            "root_path": "",
            "http_version": "1.1",
        }
        tags = ",".join(["job", *[t for t in (row["tags"] or "").split(",") if t]])
        headers = {
            "content-type": "application/json",
            "x-queuerouter-priority": row["priority"],
            "x-queuerouter-tags": tags,
        }
        response = await serve(
            Request(scope, _never_disconnects), client, path, json.dumps(request_body).encode(), headers, ""
        )
        if isinstance(response, StreamingResponse):  # shouldn't happen (stream is removed); read it all
            chunks = [c if isinstance(c, bytes) else c.encode() async for c in response.body_iterator]
            return response.status_code, b"".join(chunks)
        return response.status_code, bytes(response.body)

    job_runner = jobs_mod.JobRunner(store, execute_job, lambda: app.state.client, usage_from_body)

    # -- async jobs, files and batches (see app/jobs.py) -------------------------------------

    def job_error(exc: Exception) -> JSONResponse:
        return error_response(str(exc), 400, "invalid_request_error")

    async def json_body(request: Request) -> dict | JSONResponse:
        body = await read_body(request)
        if isinstance(body, Response):
            return body
        try:
            data = json.loads(body or b"{}")
        except ValueError:
            return error_response("the body must be JSON", 400, "invalid_request_error")
        return (
            data
            if isinstance(data, dict)
            else error_response("the body must be an object", 400, "invalid_request_error")
        )

    async def submit_job(client: ClientKey, body: dict) -> JSONResponse:
        try:
            job = jobs_mod.new_job(client.name, body)
        except jobs_mod.JobError as exc:
            return job_error(exc)
        await asyncio.to_thread(store.create_jobs, [job])
        row = await asyncio.to_thread(store.get_job, job["id"])
        return JSONResponse(jobs_mod.job_view(store, row, with_result=False), status_code=202)

    @app.post("/v1/jobs")
    async def jobs_create(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        body = await json_body(request)
        return body if isinstance(body, Response) else await submit_job(client, body)

    @app.get("/v1/jobs")
    async def jobs_list(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        q = request.query_params
        rows = await asyncio.to_thread(
            store.list_jobs,
            client.name,
            q.get("status"),
            q.get("tag"),
            q.get("batch_id"),
            min(1000, max(1, int(q.get("limit", "100")) if q.get("limit", "100").isdigit() else 100)),
        )
        return {"object": "list", "data": [jobs_mod.job_view(store, r, with_result=False) for r in rows]}

    @app.get("/v1/jobs/{job_id}")
    async def jobs_get(job_id: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        row = await asyncio.to_thread(store.get_job, job_id, client.name)
        if row is None:
            return error_response(f"job '{job_id}' not found", 404, "not_found")
        return jobs_mod.job_view(store, row)

    @app.delete("/v1/jobs/{job_id}")
    async def jobs_cancel(job_id: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        row = await asyncio.to_thread(store.get_job, job_id, client.name)
        if row is None:
            return error_response(f"job '{job_id}' not found", 404, "not_found")
        await asyncio.to_thread(store.cancel_jobs, [job_id])
        if row["batch_id"]:
            await asyncio.to_thread(jobs_mod.finish_batch_if_done, store, row["batch_id"])
        return jobs_mod.job_view(store, await asyncio.to_thread(store.get_job, job_id), with_result=False)

    @app.post("/v1/files")
    async def files_upload(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        max_bytes = int(cfg().max_body_mb * 1024 * 1024)
        async with request.form(max_files=1, max_part_size=max_bytes) as form:
            upload, purpose = form.get("file"), str(form.get("purpose", ""))
            if purpose != "batch":
                return error_response("only purpose=batch is supported", 400, "invalid_request_error")
            if upload is None or isinstance(upload, str):
                return error_response("'file' is required", 400, "invalid_request_error")
            data = await upload.read(max_bytes + 1)
            if len(data) > max_bytes:
                return error_response(f"file exceeds {cfg().max_body_mb} MB", 413, "request_too_large")
            try:
                data.decode("utf-8")
            except UnicodeDecodeError:
                return error_response("the file must be UTF-8 JSONL", 400, "invalid_request_error")
            row = await asyncio.to_thread(
                store.create_file, jobs_mod.new_id("file"), client.name, purpose, upload.filename or "input.jsonl", data
            )
        return jobs_mod.file_view(row)

    async def own_file(request: Request, file_id: str):
        client, denied = authenticate(request)
        if denied:
            return None, denied
        row = await asyncio.to_thread(store.get_file, file_id, client.name)
        if row is None:
            return None, error_response(f"file '{file_id}' not found", 404, "not_found")
        return row, None

    @app.get("/v1/files/{file_id}")
    async def files_get(file_id: str, request: Request):
        row, denied = await own_file(request, file_id)
        return denied or jobs_mod.file_view(row)

    @app.get("/v1/files/{file_id}/content")
    async def files_content(file_id: str, request: Request):
        row, denied = await own_file(request, file_id)
        if denied:
            return denied
        return Response(store.file_content(row), media_type="application/jsonl")

    @app.delete("/v1/files/{file_id}")
    async def files_delete(file_id: str, request: Request):
        row, denied = await own_file(request, file_id)
        if denied:
            return denied
        await asyncio.to_thread(store.delete_file, file_id, row["client"])
        return {"id": file_id, "object": "file", "deleted": True}

    @app.post("/v1/batches")
    async def batches_create(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        body = await json_body(request)
        if isinstance(body, Response):
            return body
        endpoint = str(body.get("endpoint", ""))
        window = str(body.get("completion_window", "24h"))
        if endpoint not in jobs_mod.BATCH_ENDPOINTS:
            return error_response(
                f"endpoint must be one of {', '.join(jobs_mod.BATCH_ENDPOINTS)}", 400, "invalid_request_error"
            )
        if window not in jobs_mod.WINDOWS:
            return error_response("completion_window must be 24h", 400, "invalid_request_error")
        source = await asyncio.to_thread(store.get_file, str(body.get("input_file_id", "")), client.name)
        if source is None:
            return error_response("input_file_id not found", 400, "invalid_request_error")
        batch_id, now = jobs_mod.new_id("batch"), time.time()
        try:
            batch_jobs = jobs_mod.batch_jobs(
                client.name, batch_id, endpoint, store.file_content(source), now + jobs_mod.WINDOWS[window]
            )
        except jobs_mod.JobError as exc:
            return job_error(exc)
        await asyncio.to_thread(
            store.create_batch,
            {
                "id": batch_id,
                "client": client.name,
                "endpoint": endpoint,
                "input_file_id": source["id"],
                "completion_window": window,
                "created": now,
                "expires_at": now + jobs_mod.WINDOWS[window],
                "total": len(batch_jobs),
                "metadata": body.get("metadata"),
            },
        )
        await asyncio.to_thread(store.create_jobs, batch_jobs)
        return jobs_mod.batch_view(store, await asyncio.to_thread(store.get_batch, batch_id))

    @app.get("/v1/batches")
    async def batches_list(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        limit = request.query_params.get("limit", "20")
        rows = await asyncio.to_thread(
            store.list_batches,
            client.name,
            min(100, int(limit) if limit.isdigit() else 20),
            request.query_params.get("after"),
        )
        data = [jobs_mod.batch_view(store, r) for r in rows]
        return {
            "object": "list",
            "data": data,
            "first_id": data[0]["id"] if data else None,
            "last_id": data[-1]["id"] if data else None,
            "has_more": len(data) == int(limit if limit.isdigit() else 20),
        }

    @app.get("/v1/batches/{batch_id}")
    async def batches_get(batch_id: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        row = await asyncio.to_thread(store.get_batch, batch_id, client.name)
        if row is None:
            return error_response(f"batch '{batch_id}' not found", 404, "not_found")
        return jobs_mod.batch_view(store, row)

    @app.post("/v1/batches/{batch_id}/cancel")
    async def batches_cancel(batch_id: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        row = await asyncio.to_thread(store.get_batch, batch_id, client.name)
        if row is None:
            return error_response(f"batch '{batch_id}' not found", 404, "not_found")
        if row["status"] == "in_progress":
            await asyncio.to_thread(store.update_batch, batch_id, status="cancelling", cancelled_at=time.time())
            await asyncio.to_thread(store.cancel_jobs, None, batch_id)
            await asyncio.to_thread(jobs_mod.finish_batch_if_done, store, batch_id)
        return jobs_mod.batch_view(store, await asyncio.to_thread(store.get_batch, batch_id))

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

    async def read_body(request: Request) -> bytes | JSONResponse:
        """The request body, or a 413 when it's larger than allowed (also without Content-Length)."""
        max_body = int(cfg().max_body_mb * 1024 * 1024)
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > max_body:
            return error_response(f"request body exceeds {cfg().max_body_mb} MB", 413, "request_too_large")
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > max_body:
                return error_response(f"request body exceeds {cfg().max_body_mb} MB", 413, "request_too_large")
            chunks.append(chunk)
        return b"".join(chunks)

    # -- API adapters: Anthropic Messages and OpenAI Responses over chat completions ---------

    def adapter_headers(request: Request, tag: str) -> dict[str, str]:
        headers = {k: v for k, v in request.headers.items() if not k.lower().startswith("anthropic-")}
        tags = headers.get("x-queuerouter-tags", "")
        headers["x-queuerouter-tags"] = f"{tags},{tag}" if tags else tag
        return headers

    def kept_headers(response: Response) -> dict[str, str]:
        return {
            k: v
            for k, v in response.headers.items()
            if k.lower().startswith("x-queuerouter-") or k.lower() in ("x-request-id", "x-queue-wait-ms", "retry-after")
        }

    def anthropic_denied(response: Response) -> JSONResponse:
        return JSONResponse(
            anthropic_api.from_openai_error(response.status_code, response.body),
            status_code=response.status_code,
            headers=kept_headers(response),
        )

    @app.post("/v1/messages")
    async def anthropic_messages(request: Request):
        client, denied = authenticate(request)
        if denied:
            return anthropic_denied(denied)
        body = await read_body(request)
        if isinstance(body, Response):
            return anthropic_denied(body)
        try:
            payload = json.loads(body)
            chat = anthropic_api.to_chat(payload)
        except ValueError as exc:
            message = str(exc) if isinstance(exc, anthropic_api.Unsupported) else "the body must be JSON"
            return JSONResponse(anthropic_api.error(400, message), status_code=400)
        model = str(payload.get("model") or "")
        response = await serve(
            request,
            client,
            "chat/completions",
            json.dumps(chat).encode(),
            adapter_headers(request, "api:anthropic"),
            "",
        )
        if isinstance(response, StreamingResponse):
            return StreamingResponse(
                anthropic_api.stream(response.body_iterator, model),
                media_type="text/event-stream",
                headers={**kept_headers(response), "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        if response.status_code >= 400:
            return anthropic_denied(response)
        try:
            message = anthropic_api.from_chat(json.loads(response.body), model)
        except (ValueError, AttributeError):
            return JSONResponse(anthropic_api.error(502, "the upstream answer was not valid JSON"), status_code=502)
        return JSONResponse(message, headers=kept_headers(response))

    @app.post("/v1/messages/count_tokens")
    async def anthropic_count_tokens(request: Request):
        """An estimate (about 4 characters per token); good enough for clients deciding to compact."""
        client, denied = authenticate(request)
        if denied:
            return anthropic_denied(denied)
        body = await read_body(request)
        if isinstance(body, Response):
            return anthropic_denied(body)
        try:
            payload = json.loads(body)
            material = json.dumps([payload.get("system"), payload.get("messages"), payload.get("tools")])
        except (ValueError, AttributeError):
            return JSONResponse(anthropic_api.error(400, "the body must be JSON"), status_code=400)
        return {"input_tokens": max(1, len(material) // 4)}

    @app.post("/v1/responses")
    async def openai_responses(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        body = await read_body(request)
        if isinstance(body, Response):
            return body
        try:
            payload = json.loads(body)
            history: list[dict] = []
            previous = payload.get("previous_response_id") if isinstance(payload, dict) else None
            if previous:
                found = await asyncio.to_thread(store.load_response, str(previous), client.name)
                if found is None:
                    return error_response(
                        f"previous response '{previous}' not found (expired, not stored, or from another key)",
                        400,
                        "previous_response_not_found",
                    )
                history = found
            chat, turn = responses_api.to_chat(payload, history)
        except ValueError as exc:
            message = str(exc) if isinstance(exc, responses_api.Unsupported) else "the body must be JSON"
            return error_response(message, 400, "invalid_request_error")
        model = str(payload.get("model") or "")
        response_id = responses_api.new_id("resp")

        def remember(final: dict) -> None:
            if payload.get("store", True) is False or final.get("status") == "failed":
                return
            conversation = [*history, *turn, responses_api.assistant_message(final["output"])]
            try:
                store.save_response(response_id, client.name, conversation)
            except Exception as exc:  # the answer itself still goes out
                log.error(json.dumps({"event": "response_store_failed", "error": repr(exc)[:200]}))

        response = await serve(
            request,
            client,
            "chat/completions",
            json.dumps(chat).encode(),
            adapter_headers(request, "api:responses"),
            "",
        )
        if isinstance(response, StreamingResponse):
            return StreamingResponse(
                responses_api.stream(response.body_iterator, payload, response_id, model, remember),
                media_type="text/event-stream",
                headers={**kept_headers(response), "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        if response.status_code >= 400:
            return response
        try:
            final = responses_api.from_chat(json.loads(response.body), payload, response_id, model)
        except (ValueError, AttributeError):
            return error_response("the upstream answer was not valid JSON", 502, "upstream_error")
        remember(final)
        return JSONResponse(final, headers=kept_headers(response))

    @app.post("/v1/{path:path}")
    async def queued_post(path: str, request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured() or mode_refusal():
            return unavailable

        body = await read_body(request)
        if isinstance(body, Response):
            return body
        if request.headers.get("x-queuerouter-async", "").strip().lower() in ("1", "true", "yes"):
            try:
                payload = json.loads(body)
            except ValueError:
                return error_response("the body must be JSON", 400, "invalid_request_error")
            wanted = request.headers.get("x-queuerouter-priority", "low").strip().lower()
            tags = request.headers.get("x-queuerouter-tags", "")
            return await submit_job(
                client, {"request": payload, "endpoint": f"/v1/{path}", "priority": wanted, "tags": tags}
            )
        return await serve(request, client, path, body)

    async def serve(
        request: Request,
        client: ClientKey,
        path: str,
        body: bytes,
        headers: dict[str, str] | None = None,
        query: str | None = None,
    ) -> Response:
        """Everything after authentication: policies, aliases/fallbacks, queueing, forwarding.
        Used by the API and by the admin Playground (which passes its own clean ``headers``)."""
        if unavailable := not_configured() or mode_refusal():
            return unavailable
        request_headers = headers if headers is not None else dict(request.headers)
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
        # The chain: the requested model (or the alias's target), then the alias's fallbacks.
        chain = [target_model, *(alias.fallbacks if alias else ())]
        triggers = frozenset(alias.fallback_on) if alias and alias.fallbacks else frozenset()
        param_changes = []
        if isinstance(payload, dict):
            alias_rules = rules_of(alias.params if alias else "")
            client_rules = rules_of(client.policy.params)
            if alias_rules or client_rules:
                param_changes = apply_rules(payload, alias_rules, client_rules)
        steps: list[Step] = []
        too_long_for: tuple | None = None
        for i, step_model in enumerate(chain):
            step_candidates = candidates_for(step_model, alias.upstream_ids if alias and i == 0 else ())
            if not step_candidates:
                continue
            if not isinstance(payload, dict):  # not JSON: forward as-is, no chain
                steps.append(Step(step_model, step_candidates, body))
                break
            step_payload = dict(payload)
            if alias or i > 0:
                step_payload["model"] = step_model  # the upstream gets the real model name
            context = model_context(step_model, step_candidates)
            if cfg().context_check and (over := too_long(step_payload, context)):
                too_long_for = too_long_for or (*over, step_model, requested_output(step_payload))
                if "context" in triggers:
                    continue  # try a model with a bigger context
                break
            unchanged = i == 0 and not alias and not param_changes
            steps.append(Step(step_model, step_candidates, body if unchanged else json.dumps(step_payload).encode()))
        if not steps:
            if too_long_for:
                estimate, context, _, output = too_long_for
                return JSONResponse(
                    {
                        "error": {
                            "message": f"This request needs about {estimate:,} prompt tokens plus "
                            f"{output:,} output tokens, but '{model}' accepts {context:,}.",
                            "type": "invalid_request_error",
                            "code": "context_length_exceeded",
                        }
                    },
                    status_code=400,
                )
            return error_response(f"no usable upstream for '{model}' right now", 503, "no_upstream")
        candidates, body = steps[0].candidates, steps[0].body
        if steps[0].model != target_model:
            target_model, alias_used = steps[0].model, True
        else:
            alias_used = bool(alias)

        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        lane = request_lane(request_headers, client)
        session = session_key(request_headers, client.name, payload)
        tags = parse_tags(client.policy.tags, request_headers.get("x-queuerouter-tags"))
        rkey = run_key(request_headers, client.name, session)
        run = runs.get(rkey) if rkey else None
        if run is not None:
            policy = client.policy
            max_tokens = policy.run_max_tokens
            header_budget = request_headers.get("x-queuerouter-run-budget-tokens", "")
            if header_budget.isdigit():  # a client may lower its own budget, never raise it
                max_tokens = min(int(header_budget), max_tokens) if max_tokens else int(header_budget)
            if reason := runs.over_budget(run, max_tokens, policy.run_max_requests, policy.run_max_minutes):
                log.warning(json.dumps({"event": "run_budget_exceeded", "client": client.name, "reason": reason}))
                return error_response(f"Run budget exceeded: {reason}.", 429, "run_budget_exceeded")
            repeats = runs.note_request(run, request_fingerprint(payload))
            if policy.loop_threshold and repeats >= policy.loop_threshold:
                log.warning(
                    json.dumps({"event": "loop_detected", "client": client.name, "repeats": repeats, "run": rkey})
                )
                if policy.loop_action == "reject":
                    return error_response(
                        f"The same request was sent {repeats} times in 5 minutes; this looks like a loop.",
                        429,
                        "loop_detected",
                    )
        position = queue.predict_position(client.name, lane, session)
        rlog = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, bootstrap.trusted_proxies))
        rlog.fields.update(
            client=client.name,
            priority=LANE_NAMES[lane],
            session=session and session[:8],
            position=position,
            model=model,
            target_model=target_model if alias_used else None,
            tags=",".join(tags) or None,
            run=rkey[:80] if rkey else None,
            stream=isinstance(payload, dict) and payload.get("stream") is True,
        )
        keep_content = client.policy.logs_content(time.time())
        hints = {
            "X-Request-Id": request_id,
            "X-QueueRouter-Queue-Depth": str(queue.queued),
            "X-QueueRouter-Position": str(position),
            "X-QueueRouter-Est-Wait-Ms": str(estimate_wait_ms(position)),
            "X-QueueRouter-Slots": f"{queue.active}/{queue.max_concurrency}",
        }
        if keep_content:
            hints["X-QueueRouter-Logging"] = "on"  # never secret: the client is told
        adm = Admission(
            request_id,
            client.name,
            lane,
            session,
            candidates,
            "POST",
            path,
            request.url.query if query is None else query,
            request_headers,
            body,
            rlog,
            hints,
            client.policy,
            time.monotonic() + client.policy.total_timeout_s if client.policy.total_timeout_s else None,
            tuple(steps),
            triggers,
            run,
            log_request=body if keep_content else None,
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
            async with asyncio.timeout(remaining), contextlib.aclosing(chained(adm)) as events:
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
                        if adm.log_request is not None:
                            rlog.content = (adm.log_request, content)
                        verdict = None
                        if status < 400 and cfg().structured_repair:
                            content, verdict = repair_structured(adm, content)
                        if status < 400:
                            prompt, completion, cached = usage_from_body(content)
                            rlog.fields.update(
                                prompt_tokens=prompt,
                                completion_tokens=completion,
                                cached_tokens=cached,
                                finish_reason=finish_reason_from_body(content),
                            )
                        headers = response_headers(response.headers)
                        headers.update(adm.hints)
                        headers["X-Queue-Wait-Ms"] = str(rlog.fields["queue_ms"])
                        headers["X-QueueRouter-Model"] = rlog.fields["target_model"] or rlog.fields["model"] or ""
                        if verdict:
                            headers["X-QueueRouter-Structured"] = verdict
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
            if adm.run is not None:
                runs.note_tokens(adm.run, (rlog.fields["prompt_tokens"] or 0) + (rlog.fields["completion_tokens"] or 0))

    # -- streaming -----------------------------------------------------------------------

    async def stream_flow(adm: Admission) -> AsyncIterator[bytes]:
        rlog = adm.rlog
        outcome, status = "error", 200
        response = None
        sniffer = UsageSniffer()
        captured = bytearray() if adm.log_request is not None else None
        try:
            async with contextlib.aclosing(chained(adm)) as events:
                async for event in events:
                    if isinstance(event, FellBack):
                        yield f": {event.from_model} {event.reason}; trying {event.to_model}\n\n".encode()
                    elif isinstance(event, Queued):
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
                                if captured is not None and len(captured) < CONTENT_CAPTURE_MAX:
                                    captured += chunk
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
            if captured is not None:
                rlog.content = (adm.log_request or b"", bytes(captured))
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
                cached_tokens=sniffer.cached_tokens,
                finish_reason=sniffer.finish_reason,
            )
            limiter.record_tokens(adm.client, (sniffer.prompt_tokens or 0) + (sniffer.completion_tokens or 0))
            if adm.run is not None:
                runs.note_tokens(adm.run, (sniffer.prompt_tokens or 0) + (sniffer.completion_tokens or 0))

    return app


CONTEXT_FIELDS = ("context_length", "max_model_len", "max_context_length", "max_output_tokens", "max_tokens")


@functools.lru_cache(maxsize=256)
def rules_of(text: str) -> dict:
    """Parsed parameter rules (validated when saved; anything unreadable counts as none)."""
    try:
        return parse_rules(text)
    except ValueError:
        return {}


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


def repair_structured(adm: Admission, content: bytes) -> tuple[bytes, str | None]:
    """Fix broken JSON in a JSON-mode reply or tool-call arguments (see app/structured.py)."""
    if b"response_format" not in adm.body and b"tool_calls" not in content:
        return content, None
    try:
        request, reply = json.loads(adm.body), json.loads(content)
    except ValueError:
        return content, None
    if not isinstance(request, dict) or not isinstance(reply, dict):
        return content, None
    verdict = structured.repair_reply(request, reply)
    if verdict:
        adm.rlog.fields["structured"] = verdict
    if verdict == "repaired":
        return json.dumps(reply, ensure_ascii=False).encode(), verdict
    return content, verdict
