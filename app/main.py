"""QueueRouter: an OpenAI-compatible proxy that queues instead of exceeding the upstreams'
concurrency limits."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import difflib
import functools
import hashlib
import hmac
import json
import logging
import math
import os
import signal
import sys
import time
import urllib.parse
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from . import __version__, anthropic_api, content_log, metrics, ranking, responses_api, structured, tracing, websearch
from . import compaction as compaction_mod
from . import council as council_mod
from . import cron as cron_mod
from . import jobs as jobs_mod
from . import thinking as thinking_mod
from . import tools as tools_mod
from .admin import AdminContext, mount_admin
from .agents import Run, Runs, parse_tags, request_fingerprint, run_key
from .alerts import Alerts
from .auth import AuthGuard, client_ip, find_client, ip_in, presented_key
from .backup import MAX_UPLOAD_BYTES, auto_backup
from .bodylimit import BodyLimit
from .catalog import Catalogs, fetch_models, parse_filter
from .circuit import Circuits
from .compaction import Plans
from .compaction import compact as compact_request
from .config import PRIORITIES, Bootstrap, ClientKey, ClientPolicy, Settings
from .fallback import Replay, is_policy_error, is_refusal_text, message_text, sniff_stream
from .limits import OVERFLOW_STATUS, ModelLimits, error_message, overflow_body, overflow_of
from .opencode import (
    DEFAULT_OUTPUT,
    PLUGIN_HEADER,
    PLUGIN_PATH,
    model_limits,
    plugin_fingerprint,
    plugin_source,
    provider_config,
    wellknown_manifest,
)
from .params import CHARS_PER_TOKEN, apply_rules, estimate_prompt_tokens, parse_rules, requested_output, too_long
from .pricing import Price, PriceCatalog, resolve, token_cost
from .quotas import MonthlyUsage, month_of, over_quota, quota_share, seconds_to_next_month
from .ratelimit import RateLimiter
from .sessions import session_key
from .slots import LANE_NAMES, LANES, Candidate, SlotQueue
from .state import MODES, Target, load_snapshot, open_store
from .stats import Stats
from .store import DataDirError
from .upstream import (
    BUSY_STATUS,
    FAILURE_STATUS,
    UsageSniffer,
    client_identity,
    describe_429,
    finish_reason_from_body,
    outgoing_user_agent,
    response_headers,
    retry_delay,
    upstream_headers,
    usage_from_body,
)

log = logging.getLogger("queuerouter")

HEALTH_INTERVAL_S = 30
IDLE_AFTER_S = 45  # a conversation this quiet gets its summary updated in the background (summarise mode)
# Headers of the original request not passed on to the requests QueueRouter makes for it.
_NOT_INHERITED = {"authorization", "x-api-key", "cookie", "host", "content-length", "content-type", "accept-encoding"}
HISTORY_POINTS, HISTORY_EVERY_S = 1800, 1  # dashboard chart: 30 minutes, one point per second
QUOTA_BLOCK_S = 3600  # how long an upstream whose quota ran out is skipped for that model (unless it says)
ADMIN_MAX_BODY = 4 * 1024 * 1024  # admin forms and the Playground (the restore upload has its own limit)
CONTENT_CAPTURE_MAX = 16 * 1024 * 1024  # streamed reply bytes kept for content logging (cut later)
CATALOG_MAX_AGE_S = 3600


def error_body(message: str, status: int, kind: str = "queuerouter_error") -> dict:
    return {"error": {"message": message, "type": kind, "code": status}}


def error_response(message: str, status: int, kind: str = "queuerouter_error", **headers: str) -> JSONResponse:
    return JSONResponse(error_body(message, status, kind), status_code=status, headers=headers or None)


def odd_path(path: str) -> bool:
    """Dot segments (or backslashes) would let a /v1/ path reach other paths on the provider."""
    path = urllib.parse.unquote(path)  # once more: "%252e%252e" arrives as "%2e%2e", which servers may decode
    return "\\" in path or any(part in (".", "..") for part in path.split("/"))


def one_line(text: str) -> str:
    """Provider text for an SSE comment line: a line break would end the comment."""
    return " ".join(str(text).split())


def header_value(text: str) -> str:
    """Client-supplied text made safe for a response header (no line breaks or other control characters)."""
    return "".join(ch for ch in text if ch.isprintable() and ord(ch) < 256)[:200]


# ends whatever event the upstream left half-sent, so an error added mid-stream parses as its own event
MID_STREAM = b"\n\n"


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
            "error": None,  # why it failed or was retried (e.g. the provider's 429 message)
            "ttft_ms": None,  # from arrival to the first byte of the answer (what the user waits for)
            "user_agent": None,  # the calling app, as it identifies itself
            "sent_user_agent": None,  # what the provider saw instead, when changed by a User-Agent rule
            "compaction": None,  # e.g. "trim 812k -> 402k": the conversation was shrunk to fit (app/compaction.py)
            "council": None,  # e.g. "pick 2/3: GLM-4.7": what a model council did (app/council.py)
            "client_headers": None,  # what the app sent about itself (app/upstream.py client_identity)
            "role": None,  # e.g. "council member 1/2", "council judge", "summary": made by QueueRouter for...
            "parent": None,  # ...this request (its id)
            "timeline": None,  # council / orchestrator: [[seconds, note], ...] (Requests > trace page)
            # The provider's own timings, streamed requests only, in ms from when the (last) attempt was sent:
            "headers_ms": None,  # its response started (time spent in the provider's own queue)
            "first_token_ms": None,  # first generated token, thinking included (prefill)
            "answer_ms": None,  # first token of the answer itself, after any thinking
            "tokens_per_s": None,  # output speed from the first token to the last
            "stall_ms": None,  # longest pause between tokens
            "backoff_ms": None,  # waited before retries (429s, errors), any request
        }
        self.sent_at: float | None = None  # monotonic: the current attempt went to the provider

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
        stats.learn_ratio(f["target_model"] or f["model"], f.get("estimate"), f["prompt_tokens"])
        if f.get("ttft_ms") is not None and f["outcome"] == "done":
            stats.ttft_hist.observe(f["ttft_ms"] / 1000)
        entry = {**f, "ts": time.time()}
        stats.recent.append(entry)
        if stats.sink:
            stats.sink(entry)
        if self.content is not None and stats.content_sink:
            stats.content_sink(f["id"], f["client"] or "default", *self.content)
        log.info(json.dumps(f))


def note_backoff(rlog: RequestLog, event: Backoff) -> None:
    """Time spent waiting to retry (a 429, an error), added up per request."""
    rlog.fields["backoff_ms"] = (rlog.fields["backoff_ms"] or 0) + round(event.delay * 1000)


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
    compaction: str | None = None  # what compaction did to this step's request (shown on the Requests page)
    estimate: int | None = None  # estimated prompt tokens of ``body`` (compared with the provider's count)


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
    context_sizes = ModelLimits()  # context sizes set by hand or learned from overflow errors
    context_sizes.load(store.list_model_limits())
    plans = Plans()  # trim plans per conversation and model (app/compaction.py)
    summaries = Plans()  # summarise mode: the summary per conversation
    notebooks = Plans()  # orchestrator: its notebook (plan, findings) per conversation
    summary_locks: collections.defaultdict[str, asyncio.Lock] = collections.defaultdict(asyncio.Lock)
    summary_users: collections.Counter[str] = collections.Counter()  # so finished conversations' locks go

    @contextlib.asynccontextmanager
    async def summary_lock(key: str):
        """One summariser per conversation; the lock is forgotten once nobody holds or waits for it."""
        summary_users[key] += 1
        try:
            async with summary_locks[key]:
                yield
        finally:
            summary_users[key] -= 1
            if not summary_users[key]:
                del summary_users[key]
                summary_locks.pop(key, None)

    prices = PriceCatalog(os.path.join(bootstrap.data_dir, "models-dev-prices.json"))
    prices.load_file()
    scores = ranking.Scores(os.path.join(bootstrap.data_dir, "model-scores.json"))  # strength, for opencode
    scores.load_file()
    # OpenRouter's public model list (no key): thinking levels by model name, and index scores without a key
    reference = thinking_mod.Reference(os.path.join(bootstrap.data_dir, "openrouter-models.json"))
    reference.load_file()
    reference_scores = ranking.Scores()  # the index from the reference list, used while there's no key
    circuits = Circuits()
    alerts = Alerts(store, lambda: app.state.client)
    guard = AuthGuard(snapshot.settings.auth_fail_limit, snapshot.settings.auth_block_s)

    def available(upstream_id: int) -> bool:
        target = snapshot.upstreams.get(upstream_id)
        return target is not None and target.usable and circuits.allows(upstream_id)

    quota_until: dict[tuple[int, str], float] = {}  # (upstream, model) -> monotonic time its quota may be back

    def block_quota(upstream_id: int, model: str, wait: float | None) -> None:
        """A provider said a quota is used up: skip that upstream for this model for a while
        (its suggested wait, else an hour; daily quotas come back the next day)."""
        quota_until[(upstream_id, model)] = time.monotonic() + min(max(wait or QUOTA_BLOCK_S, 30), 6 * 3600)

    def quota_blocked(upstream_id: int, model: str) -> bool:
        until = quota_until.get((upstream_id, model))
        if until is None:
            return False
        if until <= time.monotonic():
            quota_until.pop((upstream_id, model), None)
            return False
        return True

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

    def keepalive_s() -> float:
        """The keep-alive interval, never so small that waiting turns into a busy loop."""
        return max(0.1, cfg().keepalive_interval_s)

    def apply_snapshot() -> None:
        s = snapshot.settings
        queue.set_scheduling(s.aging_s, s.session_window_s, s.session_aware, s.fair_share, s.lb_strategy)
        queue.paused = snapshot.mode == "paused"
        if snapshot.mode == "maintenance":
            queue.evict_waiters()
        pools = {p.id: (p.max_concurrency, p.min_spacing_ms) for p in snapshot.pools.values()}
        # /v1/embeddings on a pool with its own limit use a second set of slots (pool id negated)
        pools.update({-p.id: (p.embedding_concurrency, 0) for p in snapshot.pools.values() if p.embedding_concurrency})
        queue.set_pools(pools)
        circuits.threshold, circuits.cooldown_s = s.circuit_threshold, s.circuit_cooldown_s
        guard.limit, guard.block_s = s.auth_fail_limit, s.auth_block_s
        catalogs.forget(set(snapshot.upstreams))
        catalogs.filters = {
            t.id: rule for t in snapshot.upstreams.values() if (rule := parse_filter(t.upstream.model_filter))
        }

    def reload() -> None:
        """Re-read configuration from the database and apply it live."""
        nonlocal snapshot
        snapshot = load_snapshot(store)
        context_sizes.load(store.list_model_limits())
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
    inflight: dict[str, RequestLog] = {}  # request id -> its log, while queued or running
    usage_version = [0]  # bumped whenever usage rows are written (the Usage page refreshes)

    price_overrides: dict = {}

    def price_on(model: str, target: Target, entry: dict | None, overrides: dict) -> Price | None:
        """What one request to ``model`` costs on ``target``: nothing extra on a monthly plan, the upstream's own
        price where its model list states one (OpenRouter), else QueueRouter's price for the name."""
        if target.pool.monthly_price:
            return Price(0.0, 0.0, 0.0, source="plan")
        listed = (entry or {}).get("pricing")
        if isinstance(listed, dict):
            try:  # USD per token, as strings
                rates = [
                    float(listed.get(k) or listed.get("prompt") or 0)
                    for k in ("prompt", "completion", "input_cache_read")
                ]
            except (TypeError, ValueError):
                rates = []
            if len(rates) == 3 and min(rates) >= 0:  # -1: priced per request (routers)
                return Price(*(r * 1e6 for r in rates), source="upstream")
        return resolve(model, overrides, prices)

    def model_price(model: str, pairs: list, overrides: dict) -> Price | None:
        """The price opencode is told: yours when set, else the highest across the upstreams serving it (so it
        never shows less than a request may cost)."""
        if model in overrides:
            return resolve(model, overrides, prices)
        known = [p for p in (price_on(model, t, e, overrides) for t, e in pairs) if p is not None]
        return max(known, key=lambda p: (p.input + p.output, p.output)) if known else None

    def cost_of(model: str, prompt: int, completion: int, cached: int) -> float:
        costs = token_cost(resolve(model, price_overrides, prices), prompt, completion, cached)
        return sum(costs) if costs else 0.0

    monthly = MonthlyUsage(cost_of)

    async def refresh_quotas() -> None:
        """Re-read this month's usage (and prices); alert clients close to a monthly quota."""
        price_overrides.clear()
        price_overrides.update(await asyncio.to_thread(store.list_model_prices))
        month = month_of(time.time())
        monthly.refresh(await asyncio.to_thread(store.usage_month, month))
        for key in snapshot.clients.values():
            share = quota_share(key.policy, monthly.used(key.name))
            if share is not None:
                alerts.quota(key.name, share, month)

    exporter = tracing.Exporter()

    def sink(entry: dict) -> None:
        pending_usage.append(entry)
        inflight.pop(entry.get("id"), None)
        alerts.observe(entry)
        if entry.get("trace"):
            exporter.observe(entry)
        monthly.observe(entry)

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
                usage_version[0] += 1
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
    score_attempt = [0.0]  # the same for the strength index
    reference_attempt = [0.0]  # and for the reference model list
    history: collections.deque[tuple[float, int, int]] = collections.deque(maxlen=HISTORY_POINTS)

    async def background() -> None:
        """Wake waiters when circuit cooldowns end; write usage; probe broken upstreams; refresh
        model lists; prune old request rows."""
        last_health = 0.0
        last_prune = 0.0
        last_sample = 0.0
        last_quota = 0.0
        last_trace = 0.0
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
            try:
                await flush_usage()
            except Exception as exc:  # e.g. the disk is full: keep the loop (and the queue) going
                log.error(json.dumps({"event": "usage_flush_failed", "error": repr(exc)[:200]}))
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
            if exporter.pending and (time.monotonic() - last_trace > 5 or len(exporter.pending) >= tracing.BATCH):
                last_trace = time.monotonic()
                s = cfg()
                if s.otel_endpoint:
                    asyncio.ensure_future(
                        exporter.flush(
                            app.state.client, s.otel_endpoint, s.otel_headers, s.otel_service_name, __version__
                        )
                    )
                else:
                    exporter.pending.clear()
            if time.monotonic() - last_quota > 60:
                last_quota = time.monotonic()
                try:
                    await refresh_quotas()
                except Exception as exc:
                    log.error(json.dumps({"event": "quota_refresh_failed", "error": repr(exc)[:200]}))
            if cfg().price_catalog_enabled and prices.stale and time.time() - price_attempt[0] > 3600:
                price_attempt[0] = time.time()
                asyncio.ensure_future(prices.refresh(app.state.client))  # don't hold up the loop
            if reference.stale and time.time() - reference_attempt[0] > 3600:
                reference_attempt[0] = time.time()
                asyncio.ensure_future(reference.refresh(app.state.client))
            if cfg().rank_key and scores.stale and time.time() - score_attempt[0] > 3600:
                score_attempt[0] = time.time()
                asyncio.ensure_future(scores.refresh(app.state.client, cfg().rank_key))
            if time.monotonic() - last_prune > 3600:
                last_prune = time.monotonic()
                try:
                    if cfg().request_log_days > 0:
                        await asyncio.to_thread(store.prune_requests, cfg().request_log_days)
                    await asyncio.to_thread(store.prune_notifications)
                    await asyncio.to_thread(store.prune_responses, responses_api.STORE_DAYS)
                    await asyncio.to_thread(store.prune_jobs, cfg().job_retention_days)
                    await asyncio.to_thread(store.prune_content, cfg().content_log_days)
                    await asyncio.to_thread(store.prune_summaries, cfg().compaction_keep_days)
                except Exception as exc:  # e.g. database locked or disk full: try again next hour
                    log.error(json.dumps({"event": "prune_failed", "error": repr(exc)[:200]}))
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
                try:
                    if state == "half-open" and circuits.allows(target.id):
                        circuits.begin(target.id)
                        if await refresh_catalog(target):
                            circuits.success(target.id)
                        else:
                            circuits.failure(target.id, "health probe failed")
                    elif state == "closed" and catalogs.stale(target.id, CATALOG_MAX_AGE_S):
                        await refresh_catalog(target)
                except Exception as exc:  # e.g. an unusable address: a failed probe, not a stopped loop
                    if state == "half-open":
                        circuits.failure(target.id, f"health probe failed: {type(exc).__name__}")
                    log.error(
                        json.dumps({"event": "health_probe_failed", "upstream": target.id, "error": repr(exc)[:200]})
                    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        watch_stop_signals(app)
        app.state.client = httpx.AsyncClient(
            transport=transport, limits=httpx.Limits(max_connections=200, max_keepalive_connections=40)
        )
        log.info(json.dumps({"event": "startup", "version": __version__, **summary()}))
        if not snapshot.configured:
            log.warning(json.dumps({"event": "no_upstream", "message": "add an upstream in the admin UI"}))
        if not bootstrap.public_url:
            log.warning(
                json.dumps(
                    {
                        "event": "public_url_unset",
                        "message": "set PUBLIC_URL: without it, links in the opencode config and plugin come from "
                        "the request's Host header",
                    }
                )
            )
        for target in snapshot.upstreams.values():
            if target.usable:
                asyncio.ensure_future(refresh_catalog(target))
        restore_rate_windows()
        with contextlib.suppress(Exception):  # quotas are read again every minute anyway
            await refresh_quotas()
        alerts.started(__version__)
        if requeued := job_runner.start():
            log.info(json.dumps({"event": "jobs_resumed", "jobs": requeued}))
        task = asyncio.ensure_future(background())
        idle_task = asyncio.ensure_future(idle_updates())
        yield
        task.cancel()
        idle_task.cancel()
        await job_runner.stop()
        await flush_usage()
        if exporter.pending and cfg().otel_endpoint:
            with contextlib.suppress(Exception):
                s = cfg()
                await asyncio.wait_for(
                    exporter.flush(app.state.client, s.otel_endpoint, s.otel_headers, s.otel_service_name, __version__),
                    10,
                )
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
        user_agent = request.headers.get("user-agent", "")
        if not client.policy.allows_app(user_agent):
            # Recorded with the User-Agent that was used; the caller isn't told which one is expected.
            rlog = RequestLog(f"qr-{uuid.uuid4().hex[:12]}", request.method, request.url.path, ip)
            rlog.fields.update(
                client=client.name,
                user_agent=user_agent[:300] or None,
                client_headers=client_identity(request.headers, None),
            )
            rlog.finish(stats, outcome="refused", status=403, error="app not allowed for this key (see User-Agent)")
            return None, error_response("this key can't be used from this app", 403, "app_not_allowed")
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
        if reason := over_quota(policy, monthly.used(client.name)):
            return error_response(
                f"{reason}; it resets at the start of next month (UTC)",
                429,
                "monthly_quota_exceeded",
                **{"Retry-After": str(seconds_to_next_month(time.time()))},
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
            entry = next((m for m in catalogs.models(target.id, raw=True) if m["id"] == model), None)
            known = model_limits(context_sizes.apply(model or "", target.id, entry), [target.pool.context_cap])
            if known is None:
                return None  # one candidate with unknown context: don't guess
            contexts.append(known["context"])
        return min(contexts) if contexts else None

    def candidates_for(model: str | None, only: tuple[int, ...] = (), raw: bool = False) -> tuple[Candidate, ...]:
        """Usable upstreams for ``model``: those that list it, or all of them if none does
        (the upstream then decides; lists may be stale or incomplete), plus those whose list isn't fetched
        yet (just added). One whose list can't be fetched only gets models no list names: a Claude account
        must not get "hf:..." models another upstream lists, nor ones hidden there. ``only`` restricts the
        choice (an alias pinned to some upstreams). A model hidden on an upstream (Upstreams page) isn't
        sent there, except with ``raw``: an alias, summary model or council member names it."""
        usable = [t for t in snapshot.upstreams.values() if t.usable and (not only or t.id in only)]
        if model:
            listing = [t for t in usable if (ids := catalogs.ids(t.id, raw)) is not None and model in ids]
            pending = [t for t in usable if catalogs.ids(t.id) is None and not catalogs.failed(t.id)]
            if listing:
                usable = listing + pending
            else:
                if any(model in (catalogs.ids(t.id, raw=True) or ()) for t in usable):  # listed, but hidden
                    usable = [t for t in usable if not catalogs.failed(t.id)]
                if not raw:
                    usable = [t for t in usable if catalogs.offered(t.id, model)]
        order = {uid: i for i, uid in enumerate(snapshot.upstreams)}
        return tuple(Candidate(t.id, t.pool.id, t.upstream.weight, order[t.id]) for t in usable)

    def split_pin(name: str | None) -> tuple[str | None, tuple[int, ...]]:
        """ "model@upstream" -> ("model", (that upstream's id,)): only that upstream serves it. Split only when
        the part after the last "@" is exactly an upstream's name, so model ids containing "@" still work."""
        if not name or "@" not in name:
            return name, ()
        bare, _, upstream = name.rpartition("@")
        target = next((t for t in snapshot.upstreams.values() if t.upstream.name == upstream), None)
        return (bare, (target.id,)) if target is not None and bare else (name, ())

    def embedding_slots(candidates: tuple[Candidate, ...]) -> tuple[Candidate, ...]:
        """Candidates using their pool's embedding slots (pool id negated) where it has its own limit."""
        return tuple(
            dataclasses.replace(c, pool_id=-c.pool_id)
            if (pool := snapshot.pools.get(c.pool_id)) is not None and pool.embedding_concurrency
            else c
            for c in candidates
        )

    def unknown_model(name: str | None, alias=None, raw: bool = False) -> JSONResponse | None:
        """A 404 with suggestions when no upstream offers ``name`` (only when every upstream's model
        list is known: a list we couldn't fetch proves nothing). ``raw``: hidden models count (an alias
        or QueueRouter's own request names it)."""
        only = alias.upstream_ids if alias else ()
        usable = [t for t in snapshot.upstreams.values() if t.usable and (not only or t.id in only)]
        if not name or not usable:
            return None
        lists = [catalogs.ids(t.id, raw=raw or alias is not None) for t in usable]
        if any(ids is None for ids in lists) or any(name in ids for ids in lists):
            return None
        known = sorted({m for ids in lists for m in ids} | set(snapshot.aliases))
        close = difflib.get_close_matches(name, known, n=3, cutoff=0.6)
        hint = f"; did you mean {', '.join(close)}?" if close else "; GET /v1/models lists the models you can use"
        what = f"alias '{alias.name}' points to '{name}', which" if alias else f"model '{name}'"
        return error_response(f"{what} is not offered by any upstream{hint}", 404, "model_not_found")

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
                "catalog_offered": len(catalogs.ids(t.id) or ()),
            }
            for t in snapshot.upstreams.values()
        ]

    def inflight_view() -> list[dict]:
        """Requests queued or running now, longest first (for the dashboard's Now panel)."""
        now = time.monotonic()
        rows = []
        for request_id, rlog in list(inflight.items()):
            elapsed = now - rlog.started
            if elapsed > 6 * 3600:  # never finished (shouldn't happen): don't show it forever
                inflight.pop(request_id, None)
                continue
            f = rlog.fields
            rows.append(
                {
                    "id": request_id,
                    "client": f["client"],
                    "model": f["target_model"] or f["model"],
                    "upstream": f["upstream"],
                    "stage": f.get("stage") or ("queued" if f["queue_ms"] is None else "running"),
                    "elapsed_ms": round(elapsed * 1000),
                    "queue_ms": f["queue_ms"],
                    "ttft_ms": f["ttft_ms"],
                    "stream": f["stream"],
                    "retries": f["retries"],
                    "error": f["error"],
                    "user_agent": f["user_agent"],
                    "sent_user_agent": f["sent_user_agent"],
                    # the rest is for the Requests page's live "Now" table (same columns as its list)
                    "ts": time.time() - elapsed,
                    "priority": f["priority"],
                    "requested_model": f["model"],
                    "target_model": f["target_model"],
                    "tags": f["tags"],
                    "compaction": f["compaction"],
                    "council": f.get("council"),
                    "client_headers": f.get("client_headers"),
                    "role": f.get("role"),
                    "parent": f.get("parent"),
                    "position": f["position"],
                    "estimate": f.get("estimate"),
                    "path": f["path"],
                }
            )
        rows.sort(key=lambda r: -r["elapsed_ms"])
        return rows[:50]

    def live_signature() -> tuple:
        """Changes whenever the dashboard would look different (cheap: checked 4x a second)."""
        stages = tuple(
            (rid, r.fields["queue_ms"] is None, r.fields["ttft_ms"] is None, r.fields["retries"])
            for rid, r in inflight.items()
        )
        return (queue.active, queue.queued, sum(stats.outcomes.values()), stages, usage_version[0], snapshot.mode)

    def live_state(full_history: bool = True) -> dict:
        state = queue_state()
        pool_names = {p.id: p.name for p in snapshot.pools.values()}
        pool_names.update({-p.id: f"{p.name} (embeddings)" for p in snapshot.pools.values()})
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
            "history": list(history) if full_history else list(history)[-5:],  # live updates: the latest only
            "inflight": inflight_view(),
            "usage_version": usage_version[0],
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

    def model_choices() -> list[dict]:
        """Every model the upstreams list (hidden ones too: aliases may use them) and every alias, for the
        alias editor's pickers: id, the upstreams offering it, its context, hidden, alias."""
        rows: dict[str, dict] = {}
        per_upstream: dict[str, list[dict]] = {}  # model -> one pinned choice per upstream ("model@upstream")
        for target in snapshot.upstreams.values():
            for model in catalogs.models(target.id, raw=True):
                row = rows.setdefault(
                    model["id"], {"id": model["id"], "upstreams": [], "context": None, "hidden": True, "alias": False}
                )
                row["upstreams"].append(target.upstream.name)
                offered = catalogs.offered(target.id, model["id"])
                row["hidden"] = row["hidden"] and not offered
                known = model_limits(context_sizes.apply(model["id"], target.id, model), [target.pool.context_cap])
                if known:
                    row["context"] = max(row["context"] or 0, known["context"])
                per_upstream.setdefault(model["id"], []).append(
                    {
                        "id": f"{model['id']}@{target.upstream.name}",
                        "upstreams": [f"only {target.upstream.name}"],
                        "context": known["context"] if known else None,
                        "hidden": not offered,
                        "alias": False,
                    }
                )
        for pinned in per_upstream.values():
            if len(pinned) > 1:  # offered by several upstreams: let a choice name one of them
                rows.update({p["id"]: p for p in pinned})
        for alias in snapshot.aliases.values():
            rows[alias.name] = {"id": alias.name, "upstreams": [], "context": None, "hidden": False, "alias": True}
        return sorted(rows.values(), key=lambda r: (r["alias"], r["id"].lower()))

    async def upstream_models(upstream_id: int) -> list[str] | None:
        """Every model the upstream lists, hidden ones too (Upstreams page > choose models); None if its
        list can't be fetched."""
        target = snapshot.upstreams.get(upstream_id)
        if target is None:
            return None
        if catalogs.stale(target.id, cfg().models_cache_s):
            await refresh_catalog(target)
        ids = catalogs.ids(target.id, raw=True)
        return sorted(ids, key=str.lower) if ids is not None else None

    def context_table() -> list[dict]:
        """Per model: the context each upstream reports, what was learned from its errors, the plan cap,
        the manual override and the size in use (the smallest; None = unknown). For the Models page."""
        rows: dict[str, dict] = {}
        entries: dict[tuple[int, str], dict] = {}
        for target in snapshot.upstreams.values():
            for model in catalogs.models(target.id):
                entries[(target.id, model["id"])] = model
        learned_only = {key for key in context_sizes.learned if key not in entries and key[0] in snapshot.upstreams}
        for upstream_id, model_id in [*entries, *learned_only]:
            target = snapshot.upstreams[upstream_id]
            entry = entries.get((upstream_id, model_id))
            reported = model_limits(entry, []) if entry else None
            used = model_limits(context_sizes.apply(model_id, upstream_id, entry), [target.pool.context_cap])
            row = rows.setdefault(model_id, {"model": model_id, "upstreams": [], "manual": None, "context": None})
            row["upstreams"].append(
                {
                    "id": upstream_id,
                    "name": target.upstream.name,
                    "reported": reported["context"] if reported else None,
                    "learned": context_sizes.learned.get((upstream_id, model_id)),
                    "cap": target.pool.context_cap,
                    "used": used["context"] if used else None,
                }
            )
        for model_id, manual in context_sizes.manual.items():
            rows.setdefault(model_id, {"model": model_id, "upstreams": [], "manual": None, "context": None})
            rows[model_id]["manual"] = manual
        for row in rows.values():
            used = [u["used"] for u in row["upstreams"]]
            row["context"] = min(used) if used and all(used) else None
        return sorted(rows.values(), key=lambda r: r["model"].lower())

    async def refresh_prices() -> bool:
        return await prices.refresh(app.state.client)

    async def refresh_scores() -> bool:
        score_attempt[0] = time.time()
        return bool(cfg().rank_key) and await scores.refresh(app.state.client, cfg().rank_key)

    def strength_order(names: list[str], manual: dict[str, float]) -> tuple[list[tuple[str, str]], dict[str, dict]]:
        """opencode order of models and aliases ([(name, number prefix)]) and why ({name: score, source}): your
        own score first, else the index (fine-tunes get their base model's); aliases without one come first."""
        index, info, ranked, first, rest = cfg().rank_index, {}, [], [], []
        for name in names:
            if name in manual:
                info[name] = {"score": manual[name], "source": "yours", "inherited": False}
                ranked.append((name, manual[name]))
            elif name in snapshot.aliases:
                info[name] = {"score": None, "source": "alias", "inherited": False}
                first.append(name)
            elif (hit := index_book()[0].score(name, index)) is not None:
                info[name] = {"score": hit[0], "source": hit[1], "inherited": hit[2]}
                ranked.append((name, hit[0]))
            else:
                info[name] = {"score": None, "source": "", "inherited": False}
                rest.append(name)
        return ranking.numbered(ranked, first, rest), info

    def index_book() -> tuple[ranking.Scores, str]:
        """The strength index in use: Artificial Analysis's own (with a key), else its scores as OpenRouter's
        public list carries them."""
        if scores.models:
            return scores, ""
        if reference_scores.fetched_at != reference.fetched_at:
            reference_scores.use(reference.index_entries(), reference.fetched_at)
        return reference_scores, "OpenRouter"

    def thinking_for(model_id: str, pairs: list, manual: dict[str, str]) -> thinking_mod.Thinking | None:
        """A model's thinking levels: yours, else what the upstream's own list says, else the reference list
        by name; shaped for how that upstream is asked (app/thinking.py)."""
        if model_id in manual:
            efforts = thinking_mod.ordered(manual[model_id].split(","))
            return thinking_mod.Thinking(thinking_mod.levels(efforts), None, "yours") if efforts else None
        for target, entry in pairs:
            style = thinking_mod.style_of(target.upstream.base_url, entry)
            if style in ("none", "fixed"):  # no levels to offer there
                continue
            found = thinking_mod.from_block(entry.get("reasoning"), entry.get("supported_parameters") or ())
            source = target.upstream.name
            if found is None or found == ("none", "high"):  # nothing, or only on / off: the reference may know more
                ref = reference.thinking(model_id)
                if ref is not None and (found is not None or style in ("openai", "openrouter")):
                    found, source = ref, "OpenRouter (reference)"
            if found is None or (style == "unknown" and source != target.upstream.name):
                continue  # an unknown API: only levels its own list states
            return thinking_mod.Thinking(thinking_mod.for_style(style, found), found[1], source)
        return None

    def ranking_table() -> dict:
        """The Models page's strength section: every offered model and alias, its score and place."""
        names = sorted({m for ids in models_by_upstream().values() for m in ids} | set(snapshot.aliases))
        order, info = strength_order(names, store.list_model_scores())
        rows = [{"model": name, "prefix": prefix, **info[name]} for name, prefix in order]
        book, via = index_book()
        return {
            "rows": rows,
            "index": cfg().rank_index,
            "enabled": cfg().opencode_rank,
            "has_key": bool(cfg().rank_key),
            "fetched_at": book.fetched_at,
            "count": len(book.models),
            "error": scores.error if book is scores or cfg().rank_key else reference.error,
            "via": via,
        }

    def thinking_table() -> dict:
        """The Models page's thinking section: per offered model, the levels found (and where), and yours."""
        pairs: dict[str, list] = {}
        for target in snapshot.upstreams.values():
            if target.usable:
                for entry in catalogs.models(target.id):
                    pairs.setdefault(entry["id"], []).append((target, entry))
        manual = store.list_model_thinking()
        rows = []
        for model in sorted(pairs):
            found = thinking_for(model, pairs[model], {})
            rows.append({"model": model, "found": found, "yours": manual.get(model)})
        return {
            "rows": rows,
            "fetched_at": reference.fetched_at,
            "count": len(reference.models),
            "error": reference.error,
        }

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
            lambda name: monthly.used(name),
            live_signature,
            context_table,
            upstream_models,
            model_choices,
            scores,
            refresh_scores,
            ranking_table,
            thinking_table,
        ),
    )

    def body_limit(path: str) -> int | None:
        """Max request body per path (see app/bodylimit.py). /v1 JSON bodies are checked by
        read_body instead, which answers in the OpenAI error format."""
        if path == "/admin/backup/restore":  # owner-only; checked before the body is read
            return MAX_UPLOAD_BYTES
        if path.startswith("/admin"):
            return ADMIN_MAX_BODY
        if path == "/v1/files":
            return int(cfg().max_body_mb * 1024 * 1024)
        return None

    app.add_middleware(BodyLimit, limit_for=body_limit)

    # -- the attempt loop ----------------------------------------------------------------

    def step(adm: Admission, keepalive: float) -> float:
        """How long to wait for a slot before the next keep-alive / deadline check."""
        if adm.deadline is None:
            return keepalive
        return max(0.0, min(keepalive, adm.deadline - time.monotonic()))

    def can_retry(adm: Admission, retries: int, target: Target | None = None) -> bool:
        if adm.expired():
            return False
        # The client's and the upstream's own limits win over the global one; the lower counts.
        upstream_limit = target.upstream.max_retries if target else None
        budgets = [b for b in (adm.policy.max_retries, upstream_limit) if b is not None] or [cfg().max_retries]
        finite = [b for b in budgets if b >= 0]  # -1 = keep retrying
        return not finite or retries < min(finite)

    def build_request(adm: Admission, target: Target) -> httpx.Request:
        s = cfg()
        url = f"{target.upstream.base_url}/{adm.path}" + (f"?{adm.query}" if adm.query else "")
        body = adm.body
        thinking = bool(body) and b'"reasoning_effort"' in body  # asked the way this provider understands
        if body and (target.upstream.params or thinking):  # this provider's own rules (e.g. fields it rejects)
            try:
                payload = json.loads(body)
            except ValueError:
                payload = None
            if isinstance(payload, dict):
                apply_rules(payload, rules_of(target.upstream.params), {})
                if thinking:
                    entry = next(
                        (m for m in catalogs.models(target.id, raw=True) if m.get("id") == payload.get("model")), None
                    )
                    thinking_mod.translate(payload, thinking_mod.style_of(target.upstream.base_url, entry))
                body = json.dumps(payload).encode()
        headers = upstream_headers(adm.headers, target.api_key)
        original_ua = next((v for k, v in headers.items() if k.lower() == "user-agent"), "")
        user_agent = outgoing_user_agent(
            (adm.policy.ua_mode, adm.policy.ua_template),
            (target.upstream.ua_mode, target.upstream.ua_template),
            {
                "original": original_ua,
                "client": adm.client,
                "priority": LANE_NAMES[adm.lane],
                "model": adm.rlog.fields["target_model"] or adm.rlog.fields["model"] or "",
                "upstream": target.upstream.name,
                "version": __version__,
            },
        )
        # set on every attempt, so a fallback to an upstream without a rule clears it
        adm.rlog.fields["sent_user_agent"] = user_agent if user_agent not in (None, original_ua) else None
        if user_agent is not None:
            headers = {k: v for k, v in headers.items() if k.lower() != "user-agent"}
            headers["user-agent"] = user_agent
        return app.state.client.build_request(
            adm.method,
            url,
            content=body or None,
            headers=headers,
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
        model = body_model(adm.body)
        try:
            blocked = {c.upstream_id for c in adm.candidates if quota_blocked(c.upstream_id, model)}
            if blocked and blocked >= {c.upstream_id for c in adm.candidates}:  # fail fast, no slot needed
                names = ", ".join(
                    sorted(snapshot.upstreams[u].upstream.name for u in blocked if u in snapshot.upstreams)
                )
                yield Failed(
                    429, f"the quota for {model} is used up on {names}; try again later", "upstream_quota_exhausted"
                )
                return
            excluded |= blocked
            while True:
                if snapshot.mode == "maintenance":  # e.g. failing over after maintenance began: don't queue again
                    yield Failed(
                        503, snapshot.maintenance_message or "QueueRouter is in maintenance mode", "maintenance"
                    )
                    return
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
                keepalive = keepalive_s()
                while not await queue.wait(ticket, step(adm, keepalive)):
                    if ticket.future.cancelled():  # evicted: maintenance mode
                        message = snapshot.maintenance_message or "QueueRouter is in maintenance mode"
                        yield Failed(503, message, "maintenance")
                        return
                    max_wait = adm.policy.queue_timeout_s or cfg().max_queue_wait_s
                    if (max_wait and ticket.wait_s >= max_wait) or adm.expired():
                        yield None
                        return
                    # every upstream it could use was disabled or removed meanwhile: it would wait forever
                    if not any(
                        (t := snapshot.upstreams.get(c.upstream_id)) is not None and t.upstream.enabled for c in pool
                    ):
                        yield Failed(503, "no enabled upstream can serve this request now", "not_configured")
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
                        outgoing = build_request(adm, target)
                        adm.rlog.sent_at = time.monotonic()
                        response = await app.state.client.send(outgoing, stream=True)
                        if adm.rlog.fields["stream"]:  # a non-streamed answer's headers come with all of it
                            adm.rlog.fields["headers_ms"] = round((time.monotonic() - adm.rlog.sent_at) * 1000)
                    except (
                        httpx.TransportError
                    ) as exc:  # any network failure (write errors too), not only the usual ones
                        reason = type(exc).__name__
                        adm.rlog.fields["error"] = f"{reason} talking to {target.upstream.name}"[:500]
                    else:
                        status = response.status_code
                        if status == BUSY_STATUS:
                            try:
                                content = await response.aread()
                            finally:
                                await response.aclose()
                            message, hard, wait = describe_429(content)
                            adm.rlog.fields["error"] = f"429 from {target.upstream.name}: {message}"[:500]
                            if hard:  # quota used up: waiting won't help
                                block_quota(target.id, model, wait)
                                others = [
                                    c
                                    for c in adm.candidates
                                    if c.upstream_id != target.id
                                    and c.upstream_id not in excluded
                                    and available(c.upstream_id)
                                    and not quota_blocked(c.upstream_id, model)
                                ]
                                if others:
                                    excluded.add(target.id)
                                    queue.cancel(ticket)
                                    circuits.abandon(target.id)  # a half-open trial must not stay reserved
                                    ticket = None
                                    yield Backoff(0, retries, f"quota used up on {target.upstream.name}, failing over")
                                    break
                                yield Failed(429, f"{target.upstream.name}: {message}", "upstream_quota_exhausted")
                                return
                            if can_retry(adm, retries, target):
                                delay = retry_delay(
                                    retries, response.headers, cfg().retry_base_delay_s, cfg().retry_max_delay_s
                                )
                                if wait and "retry-after" not in response.headers:
                                    delay = min(wait, cfg().retry_max_delay_s)
                                retries += 1
                                yield Backoff(delay, retries, f"HTTP 429 ({message[:150]})")
                                continue
                            yield Got(Replay(response, content, complete=True), target)  # pass the 429 through
                            return
                        if status == 404:  # this upstream hasn't got the model (its list unknown): try another
                            others = [
                                c
                                for c in adm.candidates
                                if c.upstream_id not in excluded | {target.id} and available(c.upstream_id)
                            ]
                            if others and not adm.expired():
                                try:
                                    content = await response.aread()
                                except httpx.HTTPError:
                                    content = b""
                                finally:
                                    await response.aclose()
                                note_error(adm.rlog, status, content, target)
                                circuits.success(target.id)  # it answered: not broken
                                verdict = True
                                excluded.add(target.id)
                                queue.cancel(ticket)
                                ticket = None
                                retries += 1
                                yield Backoff(0, retries, f"HTTP 404 on {target.upstream.name}, failing over")
                                break
                        if status not in FAILURE_STATUS:
                            circuits.success(target.id)
                            verdict = True
                            yield Got(response, target)
                            return
                        reason, headers = f"HTTP {status}", response.headers
                        try:  # keep what the provider said (small), for the Requests page
                            content = await response.aread()
                        except httpx.HTTPError:
                            content = b""
                        note_error(adm.rlog, status, content, target)
                        response = Replay(response, content, complete=True)

                    circuits.failure(target.id, reason)
                    verdict = True
                    if not can_retry(adm, retries, target):
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

    def note_error(rlog: RequestLog, status: int, content: bytes, target: Target) -> None:
        """The provider's own error text on the request's log (Requests page), e.g. "HTTP 503 from arliai:
        model overloaded". More specific notes already there (a 429's quota message, the original wording
        of a "too long" error) are kept."""
        current = rlog.fields["error"] or ""
        if current.startswith(("too long for", "429 from")):
            return
        message = " ".join((error_message(content) or "").split()) or "(no error text)"
        rlog.fields["error"] = f"HTTP {status} from {target.upstream.name}: {message}"[:500]

    async def check_overflow(adm: Admission, event: Got, model: str) -> tuple[Got, bool]:
        """A provider's "prompt too long" error: remember the limit it states (so the next request is
        checked before sending and opencode is told), and answer in the standard form agents act on."""
        response = event.response
        if response.status_code not in OVERFLOW_STATUS:
            return event, False
        content = await response.aread()
        overflow, limit, message = overflow_of(response.status_code, content)
        if not overflow:
            return Got(Replay(response, content, complete=True), event.target), False
        upstream = event.target.upstream
        adm.rlog.fields["error"] = f"too long for {model} on {upstream.name}: {message}"[:500]
        if limit and model and context_sizes.learned.get((upstream.id, model)) != limit:
            context_sizes.learned[(upstream.id, model)] = limit
            await asyncio.to_thread(store.learn_model_limit, upstream.id, model, limit, message)
            log.info(
                json.dumps({"event": "context_learned", "upstream": upstream.name, "model": model, "context": limit})
            )
        known = limit or context_sizes.known(model, upstream.id)
        replay = Replay(response, overflow_body(model, known, detail=message), complete=True)
        replay.status_code = 400
        replay.headers = httpx.Headers({"content-type": "application/json"})
        return Got(replay, event.target), True

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

            if (
                not last
                and "unavailable" in triggers
                and not any(
                    available(c.upstream_id) and not quota_blocked(c.upstream_id, step.model) for c in step.candidates
                )
            ):
                yield fall("unavailable")
                continue
            if index:
                adm.rlog.fields["target_model"] = step.model
                adm.rlog.fields["compaction"] = step.compaction
                adm.rlog.fields["estimate"] = step.estimate
            step_adm = dataclasses.replace(adm, candidates=step.candidates, body=step.body)
            held = None  # a response read here before it's handed on: closed if we're cancelled meanwhile
            try:
                async with contextlib.aclosing(attempts(step_adm)) as events:
                    moved_on = False
                    async for event in events:
                        held = event.response if isinstance(event, Got) else None
                        if isinstance(event, Got):
                            event, overflow = await check_overflow(adm, event, step.model)
                            held = event.response
                            if overflow and not last and "context" in triggers:
                                held = None
                                await event.response.aclose()
                                yield fall("context (provider)")
                                moved_on = True
                                break
                        if last or not isinstance(event, Got | Failed):
                            held = None
                            yield event
                            if isinstance(event, Got | Failed) or event is None:
                                return
                            continue
                        if isinstance(event, Failed):
                            quota_out = event.kind == "upstream_quota_exhausted" and triggers & {
                                "unavailable",
                                "errors",
                            }
                            if quota_out or (event.status >= 500 and "errors" in triggers):
                                yield fall(event.message)
                                moved_on = True
                                break
                            yield event
                            return
                        response = event.response
                        status = response.status_code
                        if status >= 500 and "errors" in triggers:
                            held = None
                            await response.aclose()
                            yield fall(f"HTTP {status}")
                            moved_on = True
                            break
                        if "refused" in triggers:
                            if 400 <= status < 500:
                                content = await response.aread()
                                if is_policy_error(status, content):
                                    held = None
                                    await response.aclose()
                                    yield fall(f"refused (HTTP {status})")
                                    moved_on = True
                                    break
                                held = None
                                yield Got(Replay(response, content, complete=True), event.target)
                                return
                            if status == 200:
                                if adm.rlog.fields["stream"]:
                                    refused, replay = await sniff_stream(response)
                                else:
                                    content = await response.aread()
                                    refused = is_refusal_text(*message_text(content))
                                    replay = Replay(response, content, complete=True)
                                held = None
                                if refused:
                                    await replay.aclose()
                                    yield fall("refused")
                                    moved_on = True
                                    break
                                yield Got(replay, event.target)
                                return
                        held = None
                        yield event
                        return
            except BaseException:
                if held is not None:
                    await held.aclose()
                raise
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

    async def opencode_models(client: ClientKey, with_thinking: bool = False) -> dict[str, dict]:
        """Every model and alias ``client`` may use, with opencode limits (plan caps applied). ``with_thinking``:
        each model's thinking levels and price too (for the plugin: opencode's level selector and its session cost)."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        stale = [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s)]
        if stale:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))
        served: dict[str, list[tuple[Target, dict]]] = {}  # model -> the upstreams offering it, with its entry
        listed: dict[str, list[tuple[Target, dict]]] = {}  # the same with hidden models (aliases may use them)
        for target in targets:
            for model in catalogs.models(target.id, raw=True):
                listed.setdefault(model["id"], []).append((target, model))
                if catalogs.offered(target.id, model["id"]):
                    served.setdefault(model["id"], []).append((target, model))

        def limits_on(model_id: str, on: list[Target]) -> dict[str, int] | None:
            """The smallest known context / output across the upstreams the request may go to."""
            entries = {t.id: e for t, e in listed.get(model_id, [])}
            known = [
                lim
                for t in on
                if t.id in entries
                and (lim := model_limits(context_sizes.apply(model_id, t.id, entries[t.id]), [t.pool.context_cap]))
            ]
            if not known:
                return None
            context = min(lim["context"] for lim in known)
            return {"context": context, "output": min(min(lim["output"] for lim in known), context)}

        def size_of(name: str, on: list[Target]) -> dict[str, int] | None:
            """A model's or an alias's limits (an alias: its target, on its pinned upstreams)."""
            other = snapshot.aliases.get(name)
            if other is None:
                bare, pin = split_pin(name)
                return limits_on(bare, [t for t in on if not pin or t.id in pin])
            bare, pin = split_pin(other.target_model)
            ids = pin or other.upstream_ids
            return limits_on(bare, [t for t in on if not ids or t.id in ids])

        offered_by = {m: [t.upstream.name for t, _ in pairs] for m, pairs in listed.items()}  # shown in opencode
        manual_thinking = await asyncio.to_thread(store.list_model_thinking) if with_thinking else {}
        overrides = await asyncio.to_thread(store.list_model_prices) if with_thinking else {}  # yours, current
        allowed = client.policy.allowed_models
        result: dict[str, dict] = {}
        for model_id in sorted(served):
            if not allowed or model_id in allowed:
                serving = [t for t, _ in served[model_id]]
                limits = limits_on(model_id, serving)
                name = with_providers(model_id, [t.upstream.name for t in serving])
                result[model_id] = {"name": name, **({"limit": limits} if limits else {})}
                if with_thinking and (levels := thinking_for(model_id, served[model_id], manual_thinking)):
                    result[model_id]["thinking"] = {"variants": levels.variants, "default": levels.default}
                if with_thinking and (price := model_price(model_id, served[model_id], overrides)):
                    result[model_id]["cost"] = price_json(price)
                if with_thinking and reads_images(model_id, served[model_id]):
                    result[model_id]["input"] = ["text", "image"]
        for alias in sorted(snapshot.aliases.values(), key=lambda a: a.name):
            if allowed and alias.name not in allowed and alias.target_model not in allowed:
                continue
            target, pin = split_pin(alias.target_model)  # "model@upstream" pins it like the upstream ticks
            ids = pin or alias.upstream_ids
            pinned = [t for t in targets if not ids or t.id in ids]
            limits = limits_on(target, pinned)
            if alias.council != "off":  # every member and the judge must take the conversation: the smallest counts
                names = [alias.target_model, alias.council_judge or alias.target_model, *alias.council_members]
                sizes = [s for s in (size_of(n, targets) for n in names) if s]
                if sizes:
                    context = min(s["context"] for s in sizes)
                    limits = {"context": context, "output": min(min(s["output"] for s in sizes), context)}
            if alias.virtual_context:  # told a bigger context than the model has; compaction makes it fit
                output = limits["output"] if limits else DEFAULT_OUTPUT
                limits = {"context": alias.virtual_context, "output": min(output, alias.virtual_context)}
            result[alias.name] = {
                "name": with_providers(
                    f"{alias.name} → {alias.target_model}",
                    [t.upstream.name for t in pinned if t.upstream.name in offered_by.get(target, [])]
                    or [t.upstream.name for t in pinned],
                ),
                **({"limit": limits} if limits else {}),
            }
            plain = alias.council == "off"  # a council's members and judge are asked as they are
            pairs = [(t, e) for t, e in listed.get(target, []) if t in pinned]
            if with_thinking and plain and (levels := thinking_for(target, pairs, manual_thinking)):
                result[alias.name]["thinking"] = {"variants": levels.variants, "default": levels.default}
            # a council makes several calls opencode never sees: no price rather than a low one
            if with_thinking and plain and (price := model_price(target, pairs, overrides)):
                result[alias.name]["cost"] = price_json(price)
            if with_thinking and reads_images(target, pairs):
                result[alias.name]["input"] = ["text", "image"]
        if not cfg().opencode_rank:
            return result
        # opencode's desktop app sorts by name, so the order travels in the names; its terminal keeps this order
        order, _ = strength_order(list(result), await asyncio.to_thread(store.list_model_scores))
        return {
            name: {**result[name], "name": f"{prefix} · {result[name]['name']}" if prefix else result[name]["name"]}
            for name, prefix in order
        }

    def reads_images(model: str, pairs: list) -> bool:
        """Whether opencode may send a model images: an upstream's own list says so, else the reference list by
        name. Unknown: no (opencode then refuses images for it, rather than the provider failing on them)."""
        said = [thinking_mod.takes_images(e) for _, e in pairs]
        if True in said:
            return True
        if False in said:
            return False
        return bool(reference.images(model))

    def price_json(price: Price) -> dict:
        """USD per million tokens, for the plugin (opencode works out the session cost from it)."""
        return {
            "input": round(price.input, 6),
            "output": round(price.output, 6),
            "cache_read": round(price.cached_input, 6),
        }

    def with_providers(label: str, upstreams: list[str]) -> str:
        """``model · nvidia, arliai``: which upstreams serve it (the model ID itself stays as it is)."""
        names = list(dict.fromkeys(upstreams))
        if not names:
            return label
        shown = ", ".join(names[:3]) + (f" +{len(names) - 3}" if len(names) > 3 else "")
        return f"{label} · {shown}"

    @app.get("/v1/opencode-config")
    async def opencode_config(request: Request):
        """opencode provider config with this client's models (see app/opencode.py)."""
        client, denied = authenticate(request)
        if denied:
            return denied
        # keys in the order built (strongest first with ranking on): the terminal UI shows them in that order
        plugin = request.query_params.get("plugin") == "2"  # the opencode 2.x plugin also reads thinking levels
        body = json.dumps(provider_config(public_base(request), await opencode_models(client, plugin))).encode()
        # An ETag lets the opencode plugin check for new models and aliases cheaply (304 when unchanged).
        etag = '"' + hashlib.sha256(body).hexdigest()[:20] + '"'
        headers = {"ETag": etag}
        if cfg().opencode_plugin_updates:  # the plugin replaces itself when this differs from its own
            headers[PLUGIN_HEADER] = plugin_fingerprint()
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)
        return Response(body, media_type="application/json", headers={**headers, "Cache-Control": "no-cache"})

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

    def month_view(client: ClientKey) -> dict:
        used, p = monthly.used(client.name), client.policy
        return {
            "month": monthly.month,
            "requests": used.requests,
            "tokens": used.tokens,
            "cost_usd": round(used.cost, 4),
            "quota": {"requests": p.month_requests, "tokens": p.month_tokens, "cost_usd": p.month_cost_usd},
        }

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
            "month": month_view(client),
        }

    async def model_entries(client: ClientKey) -> list[dict] | JSONResponse:
        """Union of all usable upstreams' model lists (cached; doesn't use a slot) plus the aliases, limited
        to the client's allowed models."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        stale = [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s)]
        if stale:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))
        merged: dict[str, dict] = {}
        everything: dict[str, dict] = {}  # hidden models too: an alias may point at one
        for target in targets:
            for model in catalogs.models(target.id, raw=True):
                entry = context_sizes.apply(model["id"], target.id, model)
                everything.setdefault(model["id"], entry)
                if catalogs.offered(target.id, model["id"]):
                    merged.setdefault(model["id"], entry)
        if not merged and all(catalogs.ids(t.id) is None for t in targets):
            return error_response("no upstream returned a model list", 502, "upstream_error")
        for alias in snapshot.aliases.values():
            merged[alias.name] = alias_entry(alias, everything.get(alias.target_model))
        allowed = client.policy.allowed_models
        return [
            m
            for m in merged.values()
            if not allowed
            or m["id"] in allowed
            or (m["id"] in snapshot.aliases and snapshot.aliases[m["id"]].target_model in allowed)
        ]

    @app.get("/v1/models")
    async def models(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured():
            return unavailable
        data = await model_entries(client)
        return data if isinstance(data, Response) else {"object": "list", "data": data}

    @app.get("/v1/models/{model_id:path}")
    async def model_info(model_id: str, request: Request):
        """One model ("retrieve model" in the SDKs), answered from the same list; "model@upstream" names
        the model on that upstream."""
        client, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured():
            return unavailable
        data = await model_entries(client)
        if isinstance(data, Response):
            return data
        bare, pin = split_pin(model_id)
        found = next((m for m in data if m["id"] == model_id), None)
        if found is None and pin and (not client.policy.allowed_models or bare in client.policy.allowed_models):
            found = next(({**m, "id": model_id} for m in catalogs.models(pin[0]) if m["id"] == bare), None)
        if found is None:
            return error_response(f"The model '{model_id}' does not exist", 404, "model_not_found")
        return found

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

    # -- compaction: summarise mode (see app/compaction.py) ---------------------------------

    async def internal_completion(
        client: ClientKey,
        payload: dict,
        priority: str,
        tag: str = "compaction",
        origin: Mapping[str, str] | None = None,
        role: str | None = None,
        parent: str | None = None,
    ) -> bytes | None:
        """A chat completion through the normal pipeline (queue, limits, Requests page) as ``client``;
        the answer's body, or None if it failed. ``origin``: the headers of the request it is made for,
        so it identifies as the same app (User-Agent, session ids; never credentials), on the Requests
        page and at the provider; ``role`` and ``parent`` label it there."""
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "raw_path": b"/v1/chat/completions",
            "query_string": b"",
            "headers": [],
            "client": ("compaction", 0),
            "server": None,
            "scheme": "http",
            "root_path": "",
            "http_version": "1.1",
        }
        headers = {k.lower(): v for k, v in (origin or {}).items() if k.lower() not in _NOT_INHERITED}
        headers = {k: v for k, v in headers.items() if not k.startswith("x-queuerouter-")}
        headers.update(
            {"content-type": "application/json", "x-queuerouter-priority": priority, "x-queuerouter-tags": tag}
        )
        if role:
            headers["x-queuerouter-role"] = role
        if parent:
            headers["x-queuerouter-parent"] = parent
        try:
            response = await serve(
                Request(scope, _never_disconnects),
                client,
                "chat/completions",
                json.dumps(payload).encode(),
                headers,
                "",
                allow_defer=False,
                allow_council=False,
                internal=True,
            )
        except Exception as exc:  # summaries and council members are extras: never fail the user's request
            log.warning(json.dumps({"event": f"{tag}_failed", "client": client.name, "error": repr(exc)[:200]}))
            return None
        if isinstance(response, StreamingResponse) or response.status_code != 200:
            return None
        return bytes(response.body)

    def summariser(alias, step_model: str) -> tuple[str, int | None]:
        """The model that writes summaries (the alias's compaction model, else the step's) and its context."""
        name = alias.compact_model or step_model
        target_alias = snapshot.aliases.get(name)
        real, pin = split_pin(target_alias.target_model if target_alias else name)
        return name, model_context(
            real, candidates_for(real, pin or (target_alias.upstream_ids if target_alias else ()), raw=True)
        )

    def load_summary(key: str | None) -> compaction_mod.Plan | None:
        if not key:
            return None
        summaries.idle_s = cfg().compaction_idle_minutes * 60
        plan = summaries.get(key)
        if plan is None and cfg().compaction_saved and (data := store.load_summary(key)):
            plan = compaction_mod.plan_from_dict(data)
            if plan is not None:
                summaries.put(key, plan)
        return plan

    def notebook_key(client: ClientKey, headers: Mapping[str, str], payload: dict) -> str | None:
        """The orchestrator notebook of a conversation (client, session and start), like summaries."""
        session = session_key(headers, client.name, payload)
        return compaction_mod.conversation_key(client.name, session, payload.get("messages"))

    def load_notebook(key: str | None) -> str:
        if not key:
            return ""
        notebooks.idle_s = cfg().compaction_idle_minutes * 60
        text = notebooks.get(key)
        if text is None and cfg().compaction_saved and (data := store.load_summary(f"notebook:{key}")):
            text = str(data.get("notebook") or "")
            notebooks.put(key, text)
        return text or ""

    def keep_notebook(key: str | None, client: str, text: str) -> None:
        """Saved like summaries (memory, or encrypted in the database with "Save summaries" on)."""
        if not key:
            return
        notebooks.put(key, text)
        if cfg().compaction_saved:
            store.save_summary(f"notebook:{key}", client, {"notebook": text})

    def keep_summary(key: str | None, client: str, plan: compaction_mod.Plan) -> None:
        if not key:
            return
        summaries.put(key, plan)
        if cfg().compaction_saved:
            store.save_summary(key, client, compaction_mod.plan_to_dict(plan))

    async def write_summary(
        client: ClientKey,
        messages: list,
        cut_from: int,
        cut_to: int,
        previous,
        model: str,
        context: int | None,
        priority: str,
        origin: Mapping[str, str] | None = None,
    ) -> tuple[str | None, bool]:
        """(summary of messages[cut_from:cut_to], whether an earlier summary was extended). Rolling: an
        earlier summary of a shorter stretch is extended with just the newly old part, in chunks that fit
        the summarising model, one after another."""
        text, begin, extended = "", cut_from, False
        if previous is not None and previous.summary and previous.cut_from == cut_from and previous.cut_to <= cut_to:
            text, begin, extended = previous.summary, previous.cut_to, True
        budget = (context or 131_072) * 0.6 - compaction_mod.SUMMARY_TOKENS - 1_000
        size = max(int(budget * CHARS_PER_TOKEN), 8_000)
        for part in compaction_mod.chunks(messages[begin:cut_to], size):
            summary_request = compaction_mod.summary_payload(model, text, part)
            body = await internal_completion(client, summary_request, priority, "compaction", origin, "summary")
            if body is None or (text := compaction_mod.summary_text(body) or "") == "":
                return None, extended
        return text or None, extended

    async def summarise_request(
        client: ClientKey,
        session: str | None,
        payload: dict,
        context: int,
        output: int,
        alias,
        step_model: str,
        priority: str,
        start_at: float = compaction_mod.START_AT,
        background: bool = False,
        write: bool = True,
        origin: Mapping[str, str] | None = None,
    ) -> compaction_mod.Result | None:
        """Summarise mode: reuse the conversation's summary while the request stays under ``start_at`` of
        the context, else extend it (down to about half). With ``background``, a request that still fits
        under BLOCK_AT goes out at once (with the current summary, or none yet) while the summary is
        extended alongside; only a request that wouldn't fit waits for it."""
        if not compaction_mod.needed(payload, context, output, start_at):
            return None
        messages = payload["messages"]
        key = compaction_mod.conversation_key(client.name, session, messages)
        before = estimate_prompt_tokens(payload)
        if background and write and key and (lock := summary_locks.get(key)) is not None and lock.locked():
            # another request of this conversation is writing the summary: don't wait for it while this one
            # still fits under BLOCK_AT with the summary there is (or none yet)
            previous = await asyncio.to_thread(load_summary, key)
            if previous is not None and not previous.matches(messages):
                previous = None
            current = compaction_mod.with_summary(payload, previous) if previous is not None else payload
            if not compaction_mod.needed(current, context, output, compaction_mod.BLOCK_AT):
                after = estimate_prompt_tokens(current)
                return compaction_mod.Result(current, before, after, "summarise", previous is not None, "updating")
        async with summary_lock(key or ""):  # one summariser per conversation; the others wait and reuse
            previous = await asyncio.to_thread(load_summary, key)
            if previous is not None and not previous.matches(messages):
                previous = None
            current = compaction_mod.with_summary(payload, previous) if previous is not None else payload
            if previous is not None and not compaction_mod.needed(current, context, output, start_at):
                return compaction_mod.Result(current, before, estimate_prompt_tokens(current), "summarise", True)
            if not write:  # only use what exists (the deferred retry): trim takes over otherwise
                return None
            if background and key and not compaction_mod.needed(current, context, output, compaction_mod.BLOCK_AT):
                if key not in summarising:  # one background update per conversation
                    summarising.add(key)
                    asyncio.ensure_future(
                        background_summary(
                            key, client, session, payload, context, output, alias, step_model, "low", origin
                        )
                    )
                after = estimate_prompt_tokens(current)
                return compaction_mod.Result(current, before, after, "summarise", previous is not None, "updating")
            cut = compaction_mod.make_plan(payload, compaction_mod.summarise_goal(context, output), shorten_old=False)
            if cut is None or cut.cut_to <= cut.cut_from:
                return None
            model, model_ctx = summariser(alias, step_model)
            text, extended = await write_summary(
                client, messages, cut.cut_from, cut.cut_to, previous, model, model_ctx, priority, origin
            )
            if text is None:
                return None
            plan = compaction_mod.summary_plan(messages, cut.cut_from, cut.cut_to, text)
            await asyncio.to_thread(keep_summary, key, client.name, plan)
        shrunk = compaction_mod.with_summary(payload, plan)
        note = "summary updated" if extended else "new summary"
        return compaction_mod.Result(shrunk, before, estimate_prompt_tokens(shrunk), "summarise", False, note)

    # -- model council (see app/council.py) -------------------------------------------------

    def council_compacted(client: ClientKey, alias, payload: dict, request_headers) -> dict:
        """Council members and the judge get the conversation as it is, under their own model names (so no
        alias compaction of their own): with compaction on, it's trimmed first to fit the smallest context
        among the models taking part."""
        if alias.compact == "off" or not isinstance(payload.get("messages"), list):
            return payload
        contexts = []
        for named in {alias.target_model, alias.council_judge or alias.target_model, *alias.council_members}:
            model, pin = split_pin(named)
            candidates = candidates_for(model, pin, raw=True)
            if candidates and (context := model_context(model, candidates)):
                contexts.append(int(context / stats.ratio(model)))  # in estimate units, as for the alias
        if not contexts:
            return payload
        context = min(contexts)
        output = requested_output(payload) or min(DEFAULT_OUTPUT, context // 8)
        session = session_key(request_headers, client.name, payload)
        plans.idle_s = cfg().compaction_idle_minutes * 60
        # per conversation start too: a subagent sharing its parent's session id gets its own plan
        start = compaction_mod.conversation_key(client.name, session, payload["messages"])
        key = f"{start}:council:{alias.name}" if start else None
        result = compact_request(dict(payload), context, output, plans, key)
        return result.payload if result is not None else payload

    async def run_council(
        request, client, alias, payload, request_headers, lane, tags, path, body, query, original: dict | None = None
    ) -> Response:
        """Members answer at once (internal requests, tagged "council"), the judge picks one or writes the
        answer. A streaming app gets its response headers at once and keep-alives meanwhile."""
        priority = LANE_NAMES[lane]
        stream = payload.get("stream") is True
        rlog = RequestLog(
            f"qr-{uuid.uuid4().hex[:12]}", "POST", f"/v1/{path}", client_ip(request, bootstrap.trusted_proxies)
        )
        rlog.fields.update(
            client=client.name,
            user_agent=(request_headers.get("user-agent") or "")[:300] or None,
            client_headers=client_identity(request_headers, payload),
            role=(request_headers.get("x-queuerouter-role") or "")[:80] or None,
            parent=(request_headers.get("x-queuerouter-parent") or "")[:40] or None,
            priority=priority,
            model=alias.name,
            tags=",".join([*tags, "council"]),
            stream=stream,
            stage="council",
        )
        inflight[rlog.fields["id"]] = rlog  # "council…" in the Now table while members and judge work
        search_count, search_errors = [0], []  # web searches run for the members (app/websearch.py)
        notes: asyncio.Queue[str] = asyncio.Queue()  # progress, streamed to the app as reasoning (opencode: "Thought")

        timeline: list[list] = []  # [seconds, note]: the trace page
        began_at = time.monotonic()
        keep_content = client.policy.logs_content(time.time())

        def progress(note: str, private: str = "") -> None:
            """A step of the work: streamed whole to the app; on the trace page ``private`` (excerpts of the
            conversation: tasks, queries, addresses) only for clients with content logging on."""
            # excerpts of the conversation are stored encrypted, apart from the note, and only with content logging
            # on; the trace page shows them only to admins allowed to read logged content
            excerpt = store.encrypt(private[:400]) if keep_content and private else ""
            timeline.append([round(time.monotonic() - began_at, 1), note[:200], excerpt])
            if stream:
                notes.put_nowait(note + private)

        async def orchestrate(base, members, searching, fetching, member, web_tool) -> tuple[bytes | None, None]:
            """Mode "orchestrate": the orchestrator (the judge model) plans, delegates tasks to members, uses the
            web and a calculator through tools QueueRouter runs, and finally answers (text or the app's tools).
            Budgets: rounds, member tasks, time; past them it is told to finish."""
            orchestrator = alias.council_judge or alias.target_model
            roster = list(members)
            key = notebook_key(client, request_headers, original or base)  # before compaction: stays the same
            notebook = await asyncio.to_thread(load_notebook, key)
            own = [council_mod.ask_tool(roster), tools_mod.CALC_TOOL]
            own += [council_mod.NOTEBOOK_TOOL] if key else []
            own += [council_mod.ADOPT_TOOL] if base.get("tools") else []
            own += [websearch.TOOL] if searching else []
            own += [websearch.FETCH_TOOL] if fetching else []
            own_names = {t["function"]["name"] for t in own}
            today = tools_mod.today()
            roles = council_mod.parse_roles(alias.council_roles, roster)
            prompt = council_mod.orchestration_prompt(
                roster,
                alias.orch_rounds,
                alias.orch_tasks,
                searching,
                fetching,
                today,
                alias.council_instructions,
                council_mod.roster_text(roster, roles),
            )
            if key:
                prompt += council_mod.notebook_section(notebook)
            messages = council_mod.with_orchestration(base["messages"], prompt)
            first_round = len(messages)
            orch_model, orch_pin = split_pin(orchestrator)
            orch_context = model_context(orch_model, candidates_for(orch_model, orch_pin, raw=True))
            orch_context = int(orch_context / stats.ratio(orch_model)) if orch_context else None

            def fitted(messages: list) -> list:
                """Members' answers and pages pile up round after round: past 80% of the orchestrator's context the
                oldest long results are shortened (start and end kept), so a round isn't lost to an overflow."""
                if not orch_context:
                    return messages
                out = list(messages)
                for i in range(first_round, len(out)):
                    if estimate_prompt_tokens({"messages": out}) <= orch_context * 0.8:
                        break
                    content = out[i].get("content")
                    if (
                        out[i].get("role") == "tool"
                        and isinstance(content, str)
                        and len(content) > compaction_mod.LONG_OUTPUT
                    ):
                        out[i] = {**out[i], "content": compaction_mod.shorten(content)}
                return out

            deadline = time.monotonic() + alias.orch_seconds
            rounds, tasks, web_used = 0, [0], [0]
            proposals: dict[str, dict] = {}  # P1, P2…: app tool calls members proposed
            final: list[bytes] = []  # set when it adopts proposals: the answer

            app_names = {(t.get("function") or {}).get("name") for t in base.get("tools") or [] if isinstance(t, dict)}

            def describe_answer(who: str, message: dict, sources: list[str], propose: bool) -> str:
                """A member's answer for the orchestrator; its proposed app tool calls get numbers (only calls of
                the app's own tools, and only when proposals were asked for)."""
                calls = [
                    c
                    for c in message.get("tool_calls") or []
                    if propose and (c.get("function") or {}).get("name") in app_names
                ]
                shown = {k: v for k, v in message.items() if k != "tool_calls"}
                text = council_mod.render({**shown, "_sources": sources} if sources else shown)
                if calls:
                    found = {}
                    for call in calls:
                        pid = f"P{len(proposals) + 1}"
                        proposals[pid] = found[pid] = call
                    progress(f"{who} proposes {len(found)} tool call(s): {', '.join(found)}")
                    text = f"{text}\n\n{council_mod.proposals_text(found)}".strip()
                return text

            started_from = "continues from its notebook" if notebook else "is planning"
            progress(f"Orchestrator ({orchestrator}) {started_from} · team: {', '.join(roster)}")

            async def run(call: dict, closed: bool) -> dict:
                name = (call.get("function") or {}).get("name")
                if closed and name not in (council_mod.ADOPT_NAME, council_mod.NOTEBOOK_NAME):  # wrapping up is allowed
                    text = "Budget used up: write your final answer now with what you have."
                elif name == council_mod.ASK_NAME:
                    who, task = websearch.call_argument(call, "member"), websearch.call_argument(call, "task")
                    wants_proposals = websearch.call_argument(call, "propose").lower() in ("true", "1", "yes")
                    propose = bool(base.get("tools")) and wants_proposals  # app tools exist to propose
                    if who not in roster:
                        text = f"No member called {who!r}. The team: {', '.join(roster)}."
                    elif tasks[0] >= alias.orch_tasks:
                        text = "No member tasks left in this step's budget: finish with what you have."
                    else:
                        tasks[0] += 1
                        progress(f"→ {who}", f": {' '.join(task.split())[:160]}")
                        brief = [
                            {
                                "role": "system",
                                "content": council_mod.member_prompt(
                                    searching, fetching, today, roles.get(who, ("", ""))
                                ),
                            },
                            {"role": "user", "content": task},
                        ]
                        body, sources = await member(
                            roster.index(who) + 1, who, brief, f"orchestrator member {who}", who, app_tools=propose
                        )
                        message = council_mod.message_of(body) if body else None
                        if message is None:
                            text = f"{who} didn't answer."
                        else:
                            text = describe_answer(who, message, sources, propose)
                elif name == council_mod.ADOPT_NAME:
                    arguments = websearch.call_arguments(call)
                    wanted = [str(p).strip().upper() for p in arguments.get("proposals") or [] if str(p).strip()]
                    unknown = [p for p in wanted if p not in proposals]
                    chosen = {p: proposals[p] for p in wanted if p in proposals}
                    if not chosen or unknown:
                        known = ", ".join(proposals) or "none yet"
                        text = f"Unknown proposals {unknown or wanted}: the proposals so far are {known}."
                    elif problem := council_mod.conflicts(chosen):
                        text = f"Not adopted: {problem}."
                    else:
                        progress(f"Adopting {', '.join(chosen)} for the app")
                        final.append(council_mod.adopted_body(chosen, str(arguments.get("text") or ""), orchestrator))
                        text = "Adopted: the calls go to the app."
                elif name == council_mod.NOTEBOOK_NAME:
                    text_in = websearch.call_argument(call, "text")[: council_mod.NOTEBOOK_MAX]
                    await asyncio.to_thread(keep_notebook, key, client.name, text_in)
                    progress(f"Notebook updated ({len(text_in.splitlines())} lines)")
                    text = "Notebook saved."
                elif name == tools_mod.CALC_NAME:
                    text = tools_mod.calculate(websearch.call_argument(call, "expression"))
                    progress("Calculated", f": {text}")
                elif name in (websearch.TOOL_NAME, websearch.FETCH_NAME):
                    if web_used[0] >= alias.council_search_max * max(1, len(roster)):
                        text = "No web searches or page reads left in this step's budget."
                    else:
                        web_used[0] += 1
                        text = await web_tool(call, "Orchestrator")
                else:
                    text = f"Unknown tool {name!r}."
                return {"role": "tool", "tool_call_id": call.get("id", ""), "content": text}

            while True:
                asked = {
                    **base,
                    "model": orchestrator,
                    "messages": (messages := fitted(messages)),
                    "tools": [*(base.get("tools") or []), *own],
                }
                body = await internal_completion(
                    client, asked, priority, "council", request_headers, "orchestrator", rlog.fields["id"]
                )
                message = council_mod.message_of(body) if body else None
                if message is None:
                    rlog.fields["council"] = f"orchestrator ({orchestrator}) didn't answer: {alias.target_model} alone"
                    progress(f"The orchestrator didn't answer: {alias.target_model} answers alone.")
                    return None, None
                mine = [
                    c for c in message.get("tool_calls") or [] if (c.get("function") or {}).get("name") in own_names
                ]
                summary = f"orchestrate: {rounds} rounds, {tasks[0]} member tasks"
                if not mine:
                    rlog.fields["council"] = summary
                    progress("Orchestrator is answering.")
                    return body, None
                if rounds > alias.orch_rounds:  # told to finish, still delegating: keep what it says
                    rlog.fields["council"] = f"{summary} (budget)"
                    progress("Budget used up: taking the orchestrator's answer as it is.")
                    return council_mod.without_calls(body, own_names), None
                if note := " ".join(str(message.get("content") or "").split())[:300]:
                    progress("Orchestrator", f": {note}")
                rounds += 1
                closed = rounds > alias.orch_rounds or time.monotonic() > deadline
                results = await asyncio.gather(*(run(call, closed) for call in mine))
                if final:  # it adopted members' proposals: they are the answer
                    rlog.fields["council"] = (
                        f"orchestrate: {rounds} rounds, {tasks[0]} member tasks · adopted proposals"
                    )
                    return final[0], None
                assistant = {"role": "assistant", "content": message.get("content") or "", "tool_calls": mine}
                messages = [*messages, assistant, *results]

        async def work() -> tuple[bytes | None, Response | None]:
            base = {k: v for k, v in payload.items() if k not in ("stream", "stream_options")}
            members = alias.council_members[: council_mod.MAX_MEMBERS]
            s = cfg()
            searching = alias.council_search and s.search_provider not in ("", "off")

            fetching = alias.council_fetch
            own_web = {websearch.TOOL_NAME, websearch.FETCH_NAME}

            async def web_tool(call: dict, who: str) -> str:
                """Run a web_search / web_fetch call for ``who``; the text for the model (results or why not)."""
                name = (call.get("function") or {}).get("name")
                if name == websearch.FETCH_NAME:
                    url = websearch.call_argument(call, "url")
                    progress(f"{who} reads a page", f": {url[:100]}")
                    try:
                        final, text = await websearch.fetch(app.state.client, url)
                    except websearch.SearchError as exc:
                        search_errors.append("a page couldn't be read")  # the reason can name the host
                        progress(f"{who}: couldn't read it", f" ({exc})")  # the reason can name the host
                        return f"Couldn't read {url}: {exc}. Do not present its content as known."
                    progress(f"{who}: read {len(text):,} characters")
                    return f"Page {final}:\n{text}"
                query = websearch.call_query(call)
                progress(f"{who} searches the web", f': "{query[:80]}"')
                search_count[0] += 1
                try:
                    found = await websearch.search(
                        app.state.client, s.search_provider, s.search_key, s.search_url, query,
                        alias.council_search_results,
                    )  # fmt: skip
                except websearch.SearchError as exc:
                    search_errors.append(str(exc))
                    progress(f"{who}: search failed ({exc})")
                    return f"Search failed: {exc}. Do not present unverified facts as found."
                progress(f"{who}: {len(found)} results")
                return websearch.results_text(query, found)

            async def member(
                i: int,
                model: str,
                messages: list | None = None,
                role: str | None = None,
                label: str | None = None,
                app_tools: bool = True,
            ) -> tuple[bytes | None, list[str]]:
                """One member's answer; with web tools on, its searches / page reads run here and it is asked
                again with the results, until it answers without them (or reaches the limit)."""
                role = role or f"council member {i}/{len(members)}"
                label = label or model
                began = time.monotonic()
                messages, sources, used = list(messages or base["messages"]), [], 0
                web = [*([websearch.TOOL] if searching else []), *([websearch.FETCH_TOOL] if fetching else [])]
                tools = [*((base.get("tools") or []) if app_tools else []), *web]
                while True:
                    asked = {**base, "model": model, "messages": messages}
                    asked.pop("tools", None)
                    asked.pop("tool_choice", None)
                    if tools:
                        asked["tools"] = tools
                    body = await internal_completion(
                        client, asked, priority, "council", request_headers, role, rlog.fields["id"]
                    )
                    message = council_mod.message_of(body) if body else None
                    calls = (message or {}).get("tool_calls") or []
                    wanted = [c for c in calls if (c.get("function") or {}).get("name") in own_web] if web else []
                    if not wanted:
                        seconds = round(time.monotonic() - began)
                        progress(f"✓ {label} answered ({seconds}s)" if message else f"✗ {label} didn't answer")
                        return body, sources
                    results = []
                    for call in wanted:
                        if used >= alias.council_search_max:
                            text = "Limit reached: answer now with what you have."
                        else:
                            used += 1
                            text = await web_tool(call, label)
                            if not text.startswith(("Search failed", "Couldn't read")):
                                sources.append(text)
                        results.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": text})
                    if used >= alias.council_search_max and all(
                        r["content"].startswith("Limit reached") for r in results
                    ):
                        # it keeps searching past the limit: take what it says, without those calls
                        return council_mod.without_calls(body, own_web), sources
                    assistant = {"role": "assistant", "content": message.get("content") or "", "tool_calls": wanted}
                    messages = [*messages, assistant, *results]
                    role = f"{role.split(' · ')[0]} · after search"

            if alias.council == "orchestrate":
                return await orchestrate(base, members, searching, fetching, member, web_tool)

            names = ", ".join(members)
            progress(
                f"Council ({alias.council}): asking {len(members)} member(s): {names}"
                + (" · web search on" if searching else "")
            )
            roles = council_mod.parse_roles(alias.council_roles, members)

            def briefed(m: str) -> list | None:  # a member with a role gets it in its instructions
                if m not in roles:
                    return None
                return council_mod.with_orchestration(base["messages"], council_mod.role_note(*roles[m]))

            replies = await asyncio.gather(*(member(i, m, briefed(m)) for i, m in enumerate(members, 1)))
            answered = []
            for m, (b, sources) in zip(members, replies, strict=True):
                if b and (msg := council_mod.message_of(b)):
                    shown = {**msg, "_sources": sources} if searching else dict(msg)
                    if m in roles:
                        shown["_role"] = roles[m][0]
                    answered.append((m, shown, b))
            if not answered:
                rlog.fields["council"] = f"no member answered: {alias.target_model} alone"
                progress(f"No member answered: {alias.target_model} answers alone.")
                return None, None
            if len(answered) == 1 and alias.council == "pick":  # nothing to choose between
                rlog.fields["council"] = f"only {answered[0][0]} answered"
                progress(f"Only {answered[0][0]} answered: using its answer.")
                return answered[0][2], None
            judge = alias.council_judge or alias.target_model
            candidates = [(m, msg) for m, msg, _ in answered]
            if alias.council == "pick":
                progress(f"The judge ({judge}) is choosing between {len(answered)} answers…")
                verdict = await internal_completion(
                    client,
                    council_mod.judge_payload(payload, judge, "pick", candidates, alias.council_instructions),
                    priority,
                    "council",
                    request_headers,
                    "council judge",
                    rlog.fields["id"],
                )
                said = (council_mod.message_of(verdict) or {}).get("content") if verdict else None
                chosen = council_mod.verdict(said, len(answered))
                index = chosen if chosen is not None else 0
                note = (
                    ""
                    if chosen is not None
                    else " (no verdict)"
                    if not verdict
                    else " (unclear verdict: took answer 1)"
                )
                if chosen is None and said:
                    # the reply itself only in the (encrypted, permission-gated) trace, never the plain error column
                    rlog.fields["error"] = "council judge's reply wasn't a clear number"
                    progress("The judge's reply wasn't a clear number", f": {said[:200]}")
                rlog.fields["council"] = f"pick {index + 1}/{len(answered)}: {answered[index][0]}{note}"
                progress(f"Judge chose answer {index + 1}: {answered[index][0]}{note}")
                return answered[index][2], None
            progress(f"The judge ({judge}) is writing the answer from {len(answered)} answers…")
            judged = council_mod.judge_payload(payload, judge, "combine", candidates, alias.council_instructions)
            judge_headers = {
                **request_headers,
                "x-queuerouter-tags": ",".join([*tags, "council"]),
                "x-queuerouter-role": "council judge",
                "x-queuerouter-parent": rlog.fields["id"],
            }
            response = await serve(
                request,
                client,
                path,
                json.dumps(judged).encode(),
                judge_headers,
                query,
                allow_defer=False,
                allow_council=False,
            )
            rlog.fields["council"] = f"combine {len(answered)}: {judge}"
            return None, response

        def finish(ok: bool) -> None:
            inflight.pop(rlog.fields["id"], None)
            rlog.fields["target_model"] = alias.target_model
            rlog.fields["timeline"] = json.dumps(timeline[-300:], ensure_ascii=False) if timeline else None
            if search_count[0]:
                rlog.fields["council"] = f"{rlog.fields['council'] or ''} · {search_count[0]} searches".lstrip(" ·")
            if search_errors and not rlog.fields["error"]:
                rlog.fields["error"] = f"web search: {search_errors[0]}"[:500]
            rlog.finish(stats, outcome="done" if ok else "error", status=200 if ok else 502)

        async def fallback() -> Response:  # no member answered: the alias's own model, as without a council
            return await serve(request, client, path, body, request_headers, query, allow_council=False)

        if not stream:
            try:  # stopped when the app goes away, as for a plain request: members free their slots
                outcome = await run_until_disconnect(request, work())
            except Exception:
                finish(False)
                raise
            if isinstance(outcome, Response):  # the app went away
                finish(False)
                return outcome
            content, response = outcome
            if content is not None:
                finish(True)
                return Response(
                    content,
                    200,
                    media_type="application/json",
                    headers={"X-QueueRouter-Council": header_value(rlog.fields["council"] or "")},
                )
            finish(response is not None)
            return response if response is not None else await fallback()

        async def events() -> AsyncIterator[bytes]:
            task = asyncio.ensure_future(work())
            ok = False
            started = time.monotonic()
            getter = None
            try:
                while not task.done() or not notes.empty():
                    if notes.empty():
                        yield b": council at work\n\n"  # keep-alive between notes
                        getter = asyncio.ensure_future(notes.get())
                        done, _ = await asyncio.wait(
                            {task, getter},
                            timeout=max(1.0, keepalive_s()),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if getter not in done:
                            getter.cancel()
                            continue
                        note = getter.result()
                    else:
                        note = notes.get_nowait()
                    yield council_mod.reasoning_chunk(note, alias.name, round(time.monotonic() - started))
                content, response = task.result()
                ok = content is not None or response is not None
            except Exception as exc:
                log.warning(json.dumps({"event": "council_failed", "error": repr(exc)[:200]}))
                content, response = None, None
            finally:
                if getter is not None and not getter.done():
                    getter.cancel()
                if not task.done():  # the app went away while members answered: stop the council
                    task.cancel()
                finish(ok)
            if content is not None:
                yield council_mod.as_stream(content, alias.target_model)
                return
            if response is None:
                response = await fallback()
            if isinstance(response, StreamingResponse):
                # closed with us: when the app goes away mid-answer, its slot is released at once
                async with contextlib.aclosing(response.body_iterator) as chunks:
                    async for chunk in chunks:
                        yield chunk if isinstance(chunk, bytes) else chunk.encode()
            elif response.status_code >= 400:
                yield b"data: " + bytes(response.body).replace(b"\n", b" ") + b"\n\n"
            else:
                yield council_mod.as_stream(bytes(response.body), alias.target_model)

        return StreamingResponse(
            events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    summarising: set[str] = set()  # conversations whose summary is being extended in the background

    def summary_would_block(client: ClientKey, session: str | None, payload: dict, context: int, output: int) -> bool:
        """Whether this request would have to wait for a summary to be written (see summarise_request)."""
        if not compaction_mod.needed(payload, context, output):
            return False
        messages = payload["messages"]
        key = compaction_mod.conversation_key(client.name, session, messages)
        previous = load_summary(key)
        current = payload
        if previous is not None and previous.matches(messages):
            current = compaction_mod.with_summary(payload, previous)
            if not compaction_mod.needed(current, context, output):
                return False
        return not key or compaction_mod.needed(current, context, output, compaction_mod.BLOCK_AT)

    def deferred_response(request, client, path, body, headers, query, write, model, priority) -> StreamingResponse:
        """Answer a streaming request at once and keep the connection alive (SSE comments, which apps
        ignore) while its summary is written, then serve it normally and pass its stream on. Without this
        the app sees no response headers for a minute or more, gives up and retries."""

        async def stream() -> AsyncIterator[bytes]:
            request_id = f"qr-{uuid.uuid4().hex[:12]}"
            placeholder = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, bootstrap.trusted_proxies))
            placeholder.fields.update(
                client=client.name, model=model, priority=priority, stream=True, stage="compacting"
            )
            placeholder.fields["user_agent"] = ((headers or dict(request.headers)).get("user-agent") or "")[
                :300
            ] or None
            inflight[request_id] = placeholder  # shown as "compacting" in the Now table, never logged itself
            task = asyncio.ensure_future(write())  # finishes even if the app goes away: the summary is kept
            try:
                while not task.done():
                    yield b": compacting the conversation to fit the model (writing a summary)\n\n"
                    await asyncio.wait({task}, timeout=max(1.0, keepalive_s()))
            finally:
                inflight.pop(request_id, None)
            with contextlib.suppress(Exception):
                task.result()
            response = await serve(request, client, path, body, headers, query, allow_defer=False)
            if isinstance(response, StreamingResponse):
                # closed with us: when the app goes away mid-answer, its slot is released at once
                async with contextlib.aclosing(response.body_iterator) as chunks:
                    async for chunk in chunks:
                        yield chunk if isinstance(chunk, bytes) else chunk.encode()
            elif response.status_code >= 400:  # headers are sent: an error goes out as a stream event
                yield b"data: " + bytes(response.body).replace(b"\n", b" ") + b"\n\n"
            else:
                yield bytes(response.body)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "X-QueueRouter-Compacting": "summary"},
        )

    async def background_summary(
        key, client, session, payload, context, output, alias, step_model, priority, origin=None
    ) -> None:
        try:
            result = await summarise_request(
                client, session, payload, context, output, alias, step_model, priority, origin=origin
            )
            if result is not None and not result.reused:
                log.info(json.dumps({"event": "summary_background", "client": client.name, "result": result.label}))
        except Exception as exc:
            log.warning(json.dumps({"event": "summary_background_failed", "error": repr(exc)[:200]}))
        finally:
            summarising.discard(key)

    @dataclass
    class IdleWatch:
        """A conversation near its model's limit, for an update of its summary while it is quiet."""

        client: ClientKey
        session: str
        payload: dict
        context: int
        output: int
        alias: object
        step_model: str
        origin: Mapping[str, str] | None = None
        seen: float = dataclasses.field(default_factory=time.monotonic)

    idle_watch: dict[str, IdleWatch] = {}

    def watch_idle(client, session, payload, result, context, output, alias, step_model, origin=None) -> None:
        """Remember a conversation whose request (after compaction) is above ~70% of its model's context,
        for an idle update of its summary; others are forgotten."""
        key = compaction_mod.conversation_key(client.name, session, payload.get("messages"))
        if not key or not session:
            return
        sent = result.payload if result is not None else payload
        if not cfg().compaction_idle_update or not compaction_mod.needed(
            sent, context, output, compaction_mod.IDLE_START_AT
        ):
            idle_watch.pop(key, None)
            return
        idle_watch.pop(key, None)
        idle_watch[key] = IdleWatch(client, session, payload, context, output, alias, step_model, origin)
        while len(idle_watch) > 50:  # each holds a whole conversation: keep only the most recent
            idle_watch.pop(next(iter(idle_watch)))

    async def idle_updates() -> None:
        """Every few seconds: conversations quiet for ~45 s (nothing of theirs in flight) get their summary
        extended at low priority, once, so the next message doesn't wait for it."""
        while True:
            await asyncio.sleep(5)
            now = time.monotonic()
            busy = {(r.fields["client"], r.fields["session"]) for r in inflight.values()}
            for key, watch in list(idle_watch.items()):
                if (watch.client.name, watch.session[:8]) in busy:
                    watch.seen = now  # still working: quiet time starts when it's done
                    continue
                if now - watch.seen > 600 or not cfg().compaction_idle_update:
                    idle_watch.pop(key, None)  # gone quiet for good, or switched off
                    continue
                if now - watch.seen < IDLE_AFTER_S:
                    continue
                idle_watch.pop(key, None)  # once per quiet period
                try:
                    result = await summarise_request(
                        watch.client,
                        watch.session,
                        watch.payload,
                        watch.context,
                        watch.output,
                        watch.alias,
                        watch.step_model,
                        "low",
                        compaction_mod.IDLE_START_AT,
                        origin=watch.origin,
                    )
                    if result is not None and not result.reused:
                        log.info(
                            json.dumps(
                                {"event": "summary_idle_update", "client": watch.client.name, "result": result.label}
                            )
                        )
                except Exception as exc:
                    log.warning(json.dumps({"event": "summary_idle_failed", "error": repr(exc)[:200]}))

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

    @app.post("/v1/schedules")
    async def schedules_create(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        body = await json_body(request)
        if isinstance(body, Response):
            return body
        try:
            schedule = jobs_mod.new_schedule(client.name, body)
        except jobs_mod.JobError as exc:
            return job_error(exc)
        await asyncio.to_thread(store.create_schedule, schedule)
        row = await asyncio.to_thread(store.get_schedule, schedule["id"])
        return JSONResponse(jobs_mod.schedule_view(store, row), status_code=201)

    @app.get("/v1/schedules")
    async def schedules_list(request: Request):
        client, denied = authenticate(request)
        if denied:
            return denied
        rows = await asyncio.to_thread(store.list_schedules, client.name)
        return {"object": "list", "data": [jobs_mod.schedule_view(store, r) for r in rows]}

    async def own_schedule(request: Request, schedule_id: str):
        client, denied = authenticate(request)
        if denied:
            return None, denied
        row = await asyncio.to_thread(store.get_schedule, schedule_id, client.name)
        if row is None:
            return None, error_response(f"schedule '{schedule_id}' not found", 404, "not_found")
        return row, None

    @app.get("/v1/schedules/{schedule_id}")
    async def schedules_get(schedule_id: str, request: Request):
        row, denied = await own_schedule(request, schedule_id)
        return denied or jobs_mod.schedule_view(store, row)

    @app.patch("/v1/schedules/{schedule_id}")
    async def schedules_patch(schedule_id: str, request: Request):
        """Pause / resume: ``{"enabled": false}``."""
        row, denied = await own_schedule(request, schedule_id)
        if denied:
            return denied
        body = await json_body(request)
        if isinstance(body, Response):
            return body
        if not isinstance(body.get("enabled"), bool):
            return error_response("only 'enabled' (true/false) can be changed", 400, "invalid_request_error")
        fields: dict = {"enabled": body["enabled"]}
        if body["enabled"]:  # resuming: next run from now, not the missed ones
            fields["next_run"] = cron_mod.parse(row["cron"]).next_after(time.time())
        await asyncio.to_thread(store.set_schedule, schedule_id, **fields)
        return jobs_mod.schedule_view(store, await asyncio.to_thread(store.get_schedule, schedule_id))

    @app.delete("/v1/schedules/{schedule_id}")
    async def schedules_delete(schedule_id: str, request: Request):
        row, denied = await own_schedule(request, schedule_id)
        if denied:
            return denied
        await asyncio.to_thread(store.delete_schedule, schedule_id)
        return {"id": schedule_id, "object": "schedule", "deleted": True}

    @app.post("/v1/schedules/{schedule_id}/run")
    async def schedules_run(schedule_id: str, request: Request):
        """Run now (once), without changing the schedule."""
        row, denied = await own_schedule(request, schedule_id)
        if denied:
            return denied
        job_id = await asyncio.to_thread(jobs_mod.fire_schedule, store, row, time.time(), True)
        return JSONResponse(
            jobs_mod.job_view(store, await asyncio.to_thread(store.get_job, job_id), with_result=False), status_code=202
        )

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

    def passthrough_allowed(path: str) -> bool:
        allowed = (p.strip().strip("/").removeprefix("v1/") for p in cfg().passthrough_get_paths.split(","))
        return any(path == p or path.startswith(f"{p}/") for p in allowed if p)

    @app.get("/v1/{path:path}")
    async def passthrough_get(path: str, request: Request):
        """Other GETs: refused, unless the path is one the admin lets through (Settings): then it goes to the
        first usable upstream, with that upstream's key, without a slot. Open by default it would let any
        client read the provider account behind the key (credits, key details)."""
        _, denied = authenticate(request)
        if denied:
            return denied
        if unavailable := not_configured():
            return unavailable
        if odd_path(path):
            return error_response("unsupported path", 404, "not_found")
        if not passthrough_allowed(path):
            return error_response(
                f"GET /v1/{path} isn't served by QueueRouter (an admin can let it through to the provider in Settings)",
                404,
                "not_found",
            )
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
        if odd_path(path):
            return error_response("unsupported path", 404, "not_found")
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
        allow_defer: bool = True,
        allow_council: bool = True,
        internal: bool = False,
    ) -> Response:
        """Everything after authentication: policies, aliases/fallbacks, queueing, forwarding.
        Used by the API and by the admin Playground (which passes its own clean ``headers``).
        ``allow_defer``: a streaming request that must wait for a summary is answered at once (see
        deferred_response) and served again once the summary is ready."""
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
        bare_target, target_pin = split_pin(target_model)
        # QueueRouter's own requests (council members, judge, summaries) serve a request already admitted: the
        # client's model list and queue limits apply to that request, not to the models working on it
        if not internal and (refusal := policy_refusal(client, model, bare_target)):
            return refusal
        no_fallbacks = not (alias and alias.fallbacks)  # with fallbacks, a missing target just moves on
        checked = cfg().unknown_model_check and no_fallbacks and not target_pin  # a pinned upstream decides
        if checked and (missing := unknown_model(target_model, alias, raw=internal)) is not None:
            return missing
        if allow_council and alias and alias.council != "off" and isinstance(payload, dict):
            lane = request_lane(request_headers, client)
            auto = "opencode" if "x-opencode-session-id" in request_headers else None
            tags = parse_tags(client.policy.tags, request_headers.get("x-queuerouter-tags"), auto)
            low = lane == LANES["low"]
            continues = (  # an orchestrator with a notebook here carries on after the app ran its tools
                alias.council == "orchestrate"
                and not low
                and alias.council_members
                and isinstance(payload.get("messages"), list)
                and payload["messages"]
                and isinstance(payload["messages"][-1], dict)
                and payload["messages"][-1].get("role") == "tool"
                and bool(await asyncio.to_thread(load_notebook, notebook_key(client, request_headers, payload)))
            )
            if continues or council_mod.applies(alias, payload, tags, low):
                compacted = council_compacted(client, alias, payload, request_headers)
                return await run_council(
                    request, client, alias, compacted, request_headers, lane, tags, path, body, query, original=payload
                )
        # The chain: the requested model (or the alias's target), then the alias's fallbacks.
        chain = [target_model, *(alias.fallbacks if alias else ())]
        triggers = frozenset(alias.fallback_on) if alias and alias.fallbacks else frozenset()
        param_changes = []
        if isinstance(payload, dict):
            alias_rules = rules_of(alias.params if alias else "")
            client_rules = rules_of(client.policy.params)
            if alias_rules or client_rules:
                param_changes = apply_rules(payload, alias_rules, client_rules)
        session = session_key(request_headers, client.name, payload)
        steps: list[Step] = []
        too_long_for: tuple | None = None
        for i, named in enumerate(chain):
            step_model, pin = split_pin(named)  # "model@upstream": only that upstream
            only = pin or (alias.upstream_ids if alias and i == 0 else ())
            step_candidates = candidates_for(step_model, only, raw=alias is not None or bool(pin) or internal)
            if not step_candidates:
                continue
            if not isinstance(payload, dict):  # not JSON: forward as-is, no chain
                steps.append(Step(step_model, step_candidates, body))
                break
            step_payload = dict(payload)
            if alias or i > 0 or pin:
                step_payload["model"] = step_model  # the upstream gets the real model name
            context = model_context(step_model, step_candidates)
            real_context = context  # the provider's own figure (compaction below works in estimate units)
            compaction = None
            bigger_next = "context" in triggers and i < len(chain) - 1  # a fallback loses nothing: prefer it
            if alias and alias.compact != "off" and context and not (bigger_next and too_long(step_payload, context)):
                output = requested_output(step_payload) or min(DEFAULT_OUTPUT, context // 8)
                context = int(context / stats.ratio(step_model))  # in estimate units: providers count more
                result = None
                if alias.compact == "summarise" and isinstance(step_payload.get("messages"), list):
                    priority = LANE_NAMES[request_lane(request_headers, client)]
                    full = step_payload
                    if (
                        allow_defer
                        and step_payload.get("stream") is True
                        and summary_would_block(client, session, step_payload, context, output)
                    ):
                        write = functools.partial(
                            summarise_request,
                            client,
                            session,
                            step_payload,
                            context,
                            output,
                            alias,
                            step_model,
                            priority,
                            origin=request_headers,
                        )
                        return deferred_response(request, client, path, body, headers, query, write, model, priority)
                    result = await summarise_request(
                        client,
                        session,
                        step_payload,
                        context,
                        output,
                        alias,
                        step_model,
                        priority,
                        background=True,
                        write=allow_defer or not step_payload.get("stream"),
                        origin=request_headers,
                    )
                    watch_idle(client, session, full, result, context, output, alias, step_model, request_headers)
                if result is None:  # trim mode, or a summary couldn't be written: trim instead
                    plans.idle_s = cfg().compaction_idle_minutes * 60
                    start = compaction_mod.conversation_key(client.name, session, step_payload.get("messages"))
                    key = f"{start}:{step_model}" if start else None  # per conversation start, like summaries
                    result = compact_request(step_payload, context, output, plans, key)
                if result is not None:
                    step_payload, compaction = result.payload, result.label
            if cfg().context_check and (over := too_long(step_payload, context)):
                scale = real_context / context if context and real_context else 1  # reported in the provider's units
                too_long_for = too_long_for or (
                    round(over[0] * scale),
                    real_context,
                    step_model,
                    requested_output(step_payload),
                )
                if "context" in triggers:
                    continue  # try a model with a bigger context
                break
            unchanged = i == 0 and not alias and not param_changes and not pin
            unchanged = unchanged and compaction is None
            step_body = body if unchanged else json.dumps(step_payload).encode()
            estimate = estimate_prompt_tokens(step_payload)
            steps.append(Step(step_model, step_candidates, step_body, compaction, estimate))
        if not steps:
            if too_long_for:
                estimate, context, _, output = too_long_for
                return Response(
                    overflow_body(model or "", context, estimate + output),
                    400,
                    media_type="application/json",
                )
            return error_response(f"no usable upstream for '{model}' right now", 503, "no_upstream")
        if path == "embeddings":  # their own slots where the provider limits them separately
            steps = [dataclasses.replace(s, candidates=embedding_slots(s.candidates)) for s in steps]
        candidates, body = steps[0].candidates, steps[0].body
        if steps[0].model != target_model:
            target_model, alias_used = steps[0].model, True
        else:
            alias_used = bool(alias)

        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        lane = request_lane(request_headers, client)
        auto_tags = "opencode" if "x-opencode-session-id" in request_headers else None  # any opencode version
        tags = parse_tags(client.policy.tags, request_headers.get("x-queuerouter-tags"), auto_tags)
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
        if cfg().otel_endpoint:  # tracing: continue the caller's trace, hand ours to the provider
            trace = tracing.start(request_headers.get("traceparent"))
            rlog.fields["trace"] = trace
            request_headers = {**request_headers, "traceparent": trace["upstream_traceparent"]}
        rlog.fields.update(
            client=client.name,
            user_agent=(request_headers.get("user-agent") or "")[:300] or None,
            client_headers=client_identity(request_headers, payload),
            role=(request_headers.get("x-queuerouter-role") or "")[:80] or None,
            parent=(request_headers.get("x-queuerouter-parent") or "")[:40] or None,
            priority=LANE_NAMES[lane],
            session=session and session[:8],
            position=position,
            model=model,
            target_model=target_model if alias_used else None,
            compaction=steps[0].compaction,
            estimate=steps[0].estimate,
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
        if steps[0].compaction:
            hints["X-QueueRouter-Compacted"] = steps[0].compaction
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
            try:
                done, _ = await asyncio.wait({task}, timeout=1.0)
            except asyncio.CancelledError:  # whoever waits for it gave up (e.g. an abandoned council)
                task.cancel()
                raise
            if done:
                return task.result()
            if await request.is_disconnected():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                return Response(status_code=499)

    async def nonstream_flow(adm: Admission) -> Response:
        rlog = adm.rlog
        inflight[adm.request_id] = rlog  # shown live (dashboard, Requests > Now) until it finishes
        outcome, status = "error", 500
        remaining = adm.deadline - time.monotonic() if adm.deadline else None
        try:
            async with asyncio.timeout(remaining), contextlib.aclosing(chained(adm)) as events:
                async for event in events:
                    if isinstance(event, Backoff):
                        note_backoff(rlog, event)
                        await asyncio.sleep(event.delay)
                    elif event is None:
                        outcome, status = "timeout", 504
                        return error_response("timed out waiting in queue", 504, "queue_timeout")
                    elif isinstance(event, Failed):
                        status = event.status
                        rlog.fields["error"] = event.message[:500]
                        return error_response(event.message, event.status, event.kind)
                    elif isinstance(event, Got):
                        response = event.response
                        try:
                            content = await response.aread()
                        finally:
                            await response.aclose()
                        status = response.status_code
                        outcome = "done" if status < 400 else "error"
                        if status >= 400:
                            note_error(rlog, status, content, event.target)
                        rlog.fields["ttft_ms"] = round((time.monotonic() - rlog.started) * 1000)
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
                        headers["X-QueueRouter-Model"] = header_value(
                            rlog.fields["target_model"] or rlog.fields["model"] or ""
                        )
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
        inflight[adm.request_id] = rlog  # only once the stream is read: an unread one never shows
        outcome, status = "error", 200
        response = None
        sniffer = UsageSniffer()
        captured = bytearray() if adm.log_request is not None else None
        try:
            async with contextlib.aclosing(chained(adm)) as events:
                async for event in events:
                    if isinstance(event, FellBack):
                        yield f": {event.from_model} {one_line(event.reason)}; trying {event.to_model}\n\n".encode()
                    elif isinstance(event, Queued):
                        note = (
                            f": queued id={adm.request_id} position={event.position} "
                            f"est_wait_ms={estimate_wait_ms(event.position)}\n\n"
                        )
                        yield note.encode()
                    elif isinstance(event, Waiting):
                        yield f": queued position={event.position} waited={int(event.waited_s)}s\n\n".encode()
                    elif isinstance(event, Backoff):
                        note_backoff(rlog, event)
                        yield f": {one_line(event.reason)}, retry {event.attempt} in {event.delay:.0f}s\n\n".encode()
                        deadline = time.monotonic() + event.delay
                        while (remaining := deadline - time.monotonic()) > 0:
                            await asyncio.sleep(min(remaining, keepalive_s()))
                            if deadline - time.monotonic() > 0:
                                yield b": waiting to retry\n\n"
                    elif event is None:
                        outcome, status = "timeout", 504
                        yield sse_error("timed out waiting in queue", 504, "queue_timeout")
                        return
                    elif isinstance(event, Failed):
                        status = event.status
                        rlog.fields["error"] = event.message[:500]
                        yield sse_error(event.message, event.status, event.kind)
                        return
                    elif isinstance(event, Got):
                        response = event.response
                        status = response.status_code
                        if status != 200:
                            content = await response.aread()
                            note_error(rlog, status, content, event.target)
                            try:
                                message = json.loads(content)["error"]["message"]
                            except (ValueError, KeyError, TypeError):
                                message = content.decode(errors="replace")[:500] or f"upstream returned HTTP {status}"
                            yield sse_error(message, status)
                            return
                        try:
                            async for chunk in response.aiter_bytes():
                                sniffer.feed(chunk, time.monotonic())
                                if rlog.fields["ttft_ms"] is None:
                                    rlog.fields["ttft_ms"] = round((time.monotonic() - rlog.started) * 1000)
                                if captured is not None and len(captured) < CONTENT_CAPTURE_MAX:
                                    captured += chunk
                                yield chunk
                                if adm.expired():
                                    outcome, status = "timeout", 504
                                    yield MID_STREAM + sse_error(
                                        "request exceeded this key's total time limit", 504, "request_timeout"
                                    )
                                    return
                        except httpx.HTTPError as exc:
                            status = 502
                            yield MID_STREAM + sse_error(f"upstream stream interrupted: {type(exc).__name__}", 502)
                            return
                        outcome = "done"
                        return
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected: Starlette cancels us, or the generator is closed.
            outcome = "cancelled"
            raise
        except httpx.HTTPError as exc:  # reading an error body or the stream's start, before any of it was sent
            status = 502
            rlog.fields["error"] = rlog.fields["error"] or f"{type(exc).__name__} reading the upstream's answer"
            yield sse_error(f"upstream error: {type(exc).__name__}", 502)
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
                **(sniffer.timings(rlog.sent_at) if response is not None and status == 200 else {}),
            )
            limiter.record_tokens(adm.client, (sniffer.prompt_tokens or 0) + (sniffer.completion_tokens or 0))
            if adm.run is not None:
                runs.note_tokens(adm.run, (sniffer.prompt_tokens or 0) + (sniffer.completion_tokens or 0))

    return app


CONTEXT_FIELDS = (
    "context_length",
    "max_model_len",
    "max_context_length",
    "context_window",  # Groq
    "max_input_tokens",
    "max_output_tokens",
    "max_tokens",
)


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
    if alias.virtual_context:  # compaction makes conversations up to this size fit the real model
        for key in [k for k in CONTEXT_FIELDS if k in entry]:
            del entry[key]
        entry["context_length"] = alias.virtual_context
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


def body_model(body: bytes) -> str:
    """The ``model`` of a JSON request body ("" when there is none)."""
    try:
        payload = json.loads(body) if body else None
    except ValueError:
        return ""
    model = payload.get("model") if isinstance(payload, dict) else None
    return model if isinstance(model, str) else ""


def watch_stop_signals(app: FastAPI) -> None:
    """Note when we're asked to stop (docker stop / Ctrl-C), so endless streams such as the
    admin's live dashboard end at once instead of holding up the shutdown until Docker kills us.
    Uvicorn's own handler still runs afterwards."""
    app.state.stopping = False
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous = signal.getsignal(sig)
        if not callable(previous):
            continue

        def handler(signum, frame, previous=previous):
            app.state.stopping = True
            previous(signum, frame)

        try:
            signal.signal(sig, handler)
        except ValueError:  # not the main thread (e.g. tests): nothing to watch
            return
