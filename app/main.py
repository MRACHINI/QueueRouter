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
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from . import (
    __version__,
    answer_check,
    anthropic_api,
    claude,
    content_log,
    metrics,
    ranking,
    responses_api,
    structured,
    tracing,
    websearch,
)
from . import compaction as compaction_mod
from . import council as council_mod
from . import cron as cron_mod
from . import edits as edits_mod
from . import jobs as jobs_mod
from . import thinking as thinking_mod
from . import tools as tools_mod
from .admin import AdminContext, mount_admin
from .agents import Run, Runs, parse_tags, request_fingerprint, run_key
from .alerts import Alerts
from .auth import AuthGuard, client_ip, find_client, how_long, ip_in, presented_key
from .backup import MAX_UPLOAD_BYTES, auto_backup
from .bodylimit import BodyLimit
from .catalog import Catalogs, fetch_models, parse_filter
from .circuit import Circuits
from .compaction import Plans
from .compaction import compact as compact_request
from .config import PRIORITIES, Bootstrap, ClientKey, ClientPolicy, Settings
from .fallback import Replay, hold_answer, is_policy_error, is_refusal_text, message_text, sniff_stream
from .limits import OVERFLOW_STATUS, ModelLimits, error_message, output_limit_of, overflow_body, overflow_of
from .names import EVERY, SEPARATOR, PublicIds, providers, public_ids
from .opencode import (
    DEFAULT_OUTPUT,
    PLUGIN_HEADER,
    PLUGIN_PATH,
    listed_output,
    model_limits,
    plugin_fingerprint,
    plugin_source,
    provider_config,
    wellknown_manifest,
)
from .params import (
    CHARS_PER_TOKEN,
    apply_rules,
    estimate_prompt_tokens,
    fit_output,
    parse_rules,
    requested_output,
    too_long,
)
from .pricing import (
    DEFAULT_MIX,
    MIX_DAYS,
    Price,
    PriceCatalog,
    blended_price,
    price_text,
    resolve,
    token_cost,
    token_mix,
)
from .quotas import MonthlyUsage, month_of, over_quota, quota_share, seconds_to_next_month
from .ratelimit import RateLimiter
from .sessions import session_key
from .slots import LANE_NAMES, LANES, Candidate, SlotQueue
from .state import MODES, Target, load_snapshot, open_store
from .stats import Stats
from .store import DataDirError, hash_key
from .upstream import (
    BUSY_STATUS,
    FAILURE_STATUS,
    UsageChunkFilter,
    UsageSniffer,
    claude_headers,
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
MAX_FORWARDS = 8  # aliases one request may be forwarded through (the editor refuses loops; this ends any other)
EDIT_RESULT_DAYS = 30  # a prompt edit's kept rewrite unused this long is deleted (made again when needed)
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
SEARCH_RESULTS = 8  # results per web search QueueRouter runs for Claude's web search tool
DEFAULT_SEARCHES, MAX_SEARCHES = 5, 10  # searches per message: without the tool's max_uses, and at most
MID_STREAM = b"\n\n"
KEEPALIVE = b": keep-alive\n\n"  # an SSE comment: apps skip it, but it shows the stream is alive


def sse_error(message: str, status: int, kind: str = "upstream_error") -> bytes:
    return b"data: " + json.dumps(error_body(message, status, kind)).encode() + b"\n\ndata: [DONE]\n\n"


MIX_REFRESH_S = 86_400


def stopped_at(fields: dict, answered: bool = False) -> str:
    """How far a request had got when its app went away."""
    if fields["queue_ms"] is None:
        return "in queue"
    if fields["ttft_ms"] is None:
        return "before first byte"
    return "after answer" if answered else "mid-answer"


class RequestLog:
    """Collects one request's metadata; emitted as a single JSON log line at the end."""

    def __init__(self, request_id: str, method: str, path: str, ip: str) -> None:
        self.started = time.monotonic()
        self.content: tuple[bytes, bytes] | None = None  # (request, reply) when content logging is on
        self.edit_detail: dict | None = None  # with content logging on: the system prompt before and after edits
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
            "cache_write_tokens": None,  # of the prompt tokens, written to the provider's cache (Claude: billed extra)
            "finish_reason": None,
            "stopped": None,  # cancelled: how far it had got ("in queue", "mid-answer"...)
            "fallback": None,  # why it moved on to the alias's next model ("a -> b (reason)"; several: "; ")
            "answer_check": None,  # what the alias's answer check said, per model checked (app/answer_check.py)
            "via": None,  # forwarded from alias to alias: the way it came ("a → b"; the app asked for a)
            "edits": None,  # what its prompt edits did, as JSON (app/edits.py Outcome.summary)
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
            client=f["client"],  # None: no client (a refused key)
        )
        stats.learn_ratio(f["target_model"] or f["model"], f.get("estimate"), f["prompt_tokens"])
        if f.get("ttft_ms") is not None and f["outcome"] == "done":
            stats.ttft_hist.observe(f["ttft_ms"] / 1000)
        entry = {**f, "ts": time.time()}
        stats.recent.append(entry)
        if stats.sink:
            stats.sink(entry)
        if (self.content is not None or self.edit_detail) and stats.content_sink:
            request, reply = self.content or (b"", b"")
            stats.content_sink(f["id"], f["client"] or "default", request, reply, self.edit_detail)
        log.info(json.dumps(f))


def note_backoff(rlog: RequestLog, event: Backoff) -> None:
    """Time spent waiting to retry (a 429, an error), added up per request."""
    rlog.fields["backoff_ms"] = (rlog.fields["backoff_ms"] or 0) + round(event.delay * 1000)


def add_note(rlog: RequestLog, field: str, note: str) -> None:
    """``note`` after what ``field`` already says (a request that falls back twice, or is checked twice)."""
    current = rlog.fields.get(field)
    rlog.fields[field] = f"{current}; {note}" if current else note


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
    strip_usage: bool = False  # QueueRouter asked for the usage chunk, the app didn't: not passed on
    # [first enqueued at, its seq, seconds waited in the queue so far]: shared by the fallback steps (replace()
    # copies the list's reference), so a step queues where the request first did and its waits add up
    place: list = dataclasses.field(default_factory=list)
    check: Check | None = None  # the alias checks plain-text answers (app/answer_check.py)
    hop: Hop | None = None  # after its own models, the request goes on to another alias (Forward)
    forward: Callable[[Hop, str, list], Awaitable[Response]] | None = None  # serves it there (see serve)

    def expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline


@dataclass(frozen=True)
class Check:
    """An alias's answer check: who checks, how, and as which client (the check is made as that client)."""

    model: str
    instructions: str
    client: ClientKey


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


@dataclass
class Checking:
    """An answer is held back while it comes or is checked: a keep-alive is due (``note``: say what's happening)."""

    note: str | None = None


@dataclass(frozen=True)
class Hop:
    """Where a request goes on when it's forwarded: ``alias``'s list from ``start`` (0: into it, later: back to an
    alias that forwarded, after the alias it forwarded to), then ``then``, the same for the aliases around it."""

    alias: str
    start: int = 0
    then: tuple[tuple[str, int], ...] = ()


@dataclass
class Forward:
    """The alias's own models are done: the request goes on as one for another alias (``hop``)."""

    hop: Hop


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
    alerts = Alerts(store, lambda: app.state.client, lambda: snapshot.names)
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
        # on_limit=delay: wait in the queue (a retry or failover of a request already counted doesn't wait again)
        if not ticket.requeued and limiter.wait_s(ticket.client, policy) > 0:
            alerts.client_limited(ticket.client, rejected=False)
            return False
        return True

    affinity: dict[str, tuple[int, float]] = {}  # session -> (upstream id, last used, monotonic)

    def on_grant(ticket) -> None:
        ticket.trial = circuits.begin(ticket.target.upstream_id)
        if not ticket.requeued:  # a retry or failover is the same request: counted once for rate limits
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

    upstream_identity: dict[int, tuple[str, str]] = {}  # upstream id -> (base URL, key) its model list came from

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
        # an upstream whose address or key changed may be another provider or plan: its model list is fetched again
        now_is = {t.id: (t.upstream.base_url, t.api_key) for t in snapshot.upstreams.values()}
        moved = {i for i, was in upstream_identity.items() if i in now_is and now_is[i] != was}
        upstream_identity.clear()
        upstream_identity.update(now_is)
        catalogs.forget(set(snapshot.upstreams) - moved)
        catalogs.filters = {
            t.id: rule for t in snapshot.upstreams.values() if (rule := parse_filter(t.upstream.model_filter))
        }

    def reload() -> None:
        """Re-read configuration from the database and apply it live."""
        nonlocal snapshot
        snapshot = load_snapshot(store)
        context_sizes.load(store.list_model_limits())
        apply_snapshot()
        edit_engine.forget()  # what prompt edits read and kept: from the database again
        log.info(json.dumps({"event": "config_reloaded", **summary()}))

    edit_engine = edits_mod.Engine(
        edits_mod.EditStore(
            seen=store.edit_seen,
            add_made=store.add_made_edits,
            result=store.edit_result,
            keep_result=store.keep_edit_result,
            reload=reload,
        )
    )

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
        return await catalogs.refresh(
            app.state.client, target.id, target.upstream.base_url, target.api_key, target.upstream.name
        )

    pending_usage: list[dict] = []
    inflight: dict[str, RequestLog] = {}  # request id -> its log, while queued or running
    usage_version = [0]  # bumped whenever usage rows are written (the Usage page refreshes)

    price_overrides: dict = {}

    def listed_price(model: str, target: Target, entry: dict | None, overrides: dict) -> Price | None:
        """What one request to ``model`` costs on ``target``: yours when set; else nothing extra on a monthly plan,
        the upstream's own price where its model list states one (OpenRouter), else QueueRouter's price for the name."""
        found = functools.partial(resolve, model, overrides, prices, target.id)
        name = snapshot.names.name(model, target.upstream.name)
        if (target.id, model) in overrides or (EVERY, model) in overrides:
            return found(name)
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
        return found(name)

    def price_on(model: str, target: Target, entry: dict | None, overrides: dict) -> Price | None:
        price = listed_price(model, target, entry, overrides)
        return dataclasses.replace(price, writes_cache=True) if price and claude.native(target.upstream) else price

    def model_price(model: str, pairs: list, overrides: dict) -> Price | None:
        """The price opencode is told: the highest across the upstreams that may serve it (so it never shows less
        than a request may cost)."""
        known = [p for p in (price_on(model, t, e, overrides) for t, e in pairs) if p is not None]
        return max(known, key=lambda p: (p.input + p.output, p.output)) if known else None

    def cost_of(
        model: str, prompt: int, completion: int, cached: int, upstream: str | None = None, written: int = 0
    ) -> float:
        upstream_id, name = snapshot.names.id_of(upstream), snapshot.names.name(model, upstream)
        price = resolve(model, price_overrides, prices, upstream_id, name)
        costs = token_cost(price, prompt, completion, cached, written)
        return sum(costs) if costs else 0.0

    mix_read: dict = {"at": None, "since": "", "mix": DEFAULT_MIX}

    def mix_due() -> bool:
        """Read once a day, so names change rarely, and again when the day it counts from changes."""
        at = mix_read["at"]
        return at is None or mix_read["since"] != cfg().price_mix_since or time.monotonic() - at > MIX_REFRESH_S

    def read_mix() -> tuple[float, float, float]:
        since = cfg().price_mix_since
        mix_read.update(at=time.monotonic(), since=since, mix=token_mix(*store.token_totals(MIX_DAYS, since)))
        return mix_read["mix"]

    async def current_mix() -> tuple[float, float, float]:
        """The token mix of the last days (from the day set on the Models page, when later), for the prices in
        opencode names."""
        return await asyncio.to_thread(read_mix) if mix_due() else mix_read["mix"]

    monthly = MonthlyUsage(cost_of)

    async def refresh_quotas() -> None:
        """Re-read this month's usage (and prices); alert clients close to a monthly quota."""
        overrides = await asyncio.to_thread(store.list_model_prices)
        price_overrides.clear()  # swapped at once: requests finishing meanwhile keep the manual prices
        price_overrides.update(overrides)
        month = month_of(time.time())
        monthly.refresh(await asyncio.to_thread(store.usage_month, month))
        for entry in pending_usage:  # finished but not written yet: not in those rows, so counted again here
            monthly.observe(entry)
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
    pending_content: list[tuple[str, float, str, bytes, bytes, dict | None]] = []

    def content_sink(rid: str, client: str, req: bytes, resp: bytes, detail: dict | None = None) -> None:
        pending_content.append((rid, time.time(), client, req, resp, detail))

    stats.content_sink = content_sink

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
            taken = pending_content[:]
            pending_content.clear()  # first: one entry that can't be read must not block the rest for good
            for rid, ts, client, req, resp, detail in taken:
                try:
                    reply = content_log.readable_reply(resp)
                except Exception:  # an answer in a shape we don't know: kept as it came
                    reply = resp.decode("utf-8", "replace")
                request_text, cut_req = content_log.clip(req.decode("utf-8", "replace"), limit)
                reply_text, cut_resp = content_log.clip(reply, limit)
                edited = json.dumps(content_log.clip_edits(detail, limit), ensure_ascii=False) if detail else None
                contents.append((rid, ts, client, request_text, reply_text, cut_req or cut_resp, edited))
            try:
                await asyncio.to_thread(store.save_contents, contents)
            except Exception as exc:
                log.error(json.dumps({"event": "content_write_failed", "error": repr(exc)[:200]}))
        if hits := edit_engine.take_hits():  # how often each prompt edit changed something (Edits page)
            try:
                await asyncio.to_thread(store.add_edit_hits, hits)
            except Exception as exc:
                log.error(json.dumps({"event": "edit_hits_write_failed", "error": repr(exc)[:200]}))

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
                    await asyncio.to_thread(store.prune_edit_results, EDIT_RESULT_DAYS)
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
                if not target.usable or target.id in health_checks:
                    continue
                state = circuits.state(target.id)
                # an upstream whose model list was never fetched (a Claude account) would fail this probe every
                # time and stay out: its trial is the next request instead
                probing = state == "half-open" and circuits.allows(target.id) and not catalogs.failed(target.id)
                if probing:
                    circuits.begin(target.id)
                elif not (state == "closed" and catalogs.stale(target.id, CATALOG_MAX_AGE_S)):
                    continue
                health_checks[target.id] = asyncio.ensure_future(health_check(target, probing))

    health_checks: dict[int, asyncio.Future] = {}  # upstream id -> its probe or model-list refresh, while it runs

    async def health_check(target: Target, probing: bool) -> None:
        """A half-open upstream's probe, or a model-list refresh: beside the background loop, not in it (an
        unreachable upstream can take its whole connect timeout, and the loop wakes the queue every second)."""
        try:
            ok = await refresh_catalog(target)
            if probing and ok:
                circuits.success(target.id)
            elif probing:
                circuits.failure(target.id, "health probe failed")
        except Exception as exc:  # e.g. an unusable address: a failed probe, not a stopped loop
            if probing:
                circuits.failure(target.id, f"health probe failed: {type(exc).__name__}")
            log.error(json.dumps({"event": "health_probe_failed", "upstream": target.id, "error": repr(exc)[:200]}))
        finally:
            health_checks.pop(target.id, None)

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
        for check in list(health_checks.values()):
            check.cancel()
        await job_runner.stop()
        await edit_engine.close()
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
    app.state.edit_engine = edit_engine
    app.state.circuits = circuits
    app.state.prices = prices
    app.state.scores = scores
    app.state.alerts = alerts
    app.state.restart = lambda: os.kill(os.getpid(), signal.SIGTERM)  # Docker's restart policy starts us again

    # -- helpers ---------------------------------------------------------------------------

    def authenticate(request: Request) -> tuple[ClientKey | None, JSONResponse | None]:
        """The calling client, or an error response to return instead."""
        ip = client_ip(request, bootstrap.trusted_proxies)
        user_agent = request.headers.get("user-agent", "")
        if remaining := guard.is_blocked(ip):
            guard.refused(ip, user_agent)
            retry_after = {"Retry-After": str(int(remaining) + 1)}
            return None, error_response("too many failed authentication attempts", 429, "auth_blocked", **retry_after)
        key = presented_key(request)
        client = find_client(key, snapshot.clients, cfg().allow_no_auth, snapshot.any_clients)
        if client is None:
            key_refused(request, ip, key, guard.fail(ip, user_agent))
            return None, error_response("invalid or missing API key", 401, "invalid_api_key")
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

    def key_refused(request: Request, ip: str, key: str | None, blocked: bool) -> None:
        """A key that didn't get in, on the Requests page with the app and why (never the key itself): a key
        replaced by New key that some app still sends shows which app. The caller is told only 401."""
        hashed = hash_key(key) if key else ""
        if not key:
            reason = "no key (Authorization: Bearer, or x-api-key)"
        elif inactive := snapshot.inactive_clients.get(hashed):
            reason = f"the key of {inactive[0]}, which is {inactive[1]}"
        elif known := snapshot.clients.get(hashed):
            reason = f"the key of {known.name}, which is expired"  # since the configuration was loaded
        else:
            reason = "unknown key: mistyped, or replaced by New key"
        if blocked:
            reason += f"; {guard.limit} in a minute from this address: it's blocked for {how_long(guard.block_s)}"
        rlog = RequestLog(f"qr-{uuid.uuid4().hex[:12]}", request.method, request.url.path, ip)
        rlog.fields.update(
            user_agent=request.headers.get("user-agent", "")[:300] or None,
            client_headers=client_identity(request.headers, None),
        )
        rlog.finish(stats, outcome="refused", status=401, error=reason)

    def not_configured() -> JSONResponse | None:
        if not snapshot.configured:
            return error_response("no upstream configured yet: add one in the admin UI", 503, "not_configured")
        return None

    def mode_refusal() -> JSONResponse | None:
        """Draining and maintenance refuse new requests; paused still accepts (and queues)."""
        if snapshot.mode in ("draining", "maintenance"):
            message = snapshot.maintenance_message or f"QueueRouter is {snapshot.mode}; try again later"
            retry = "30" if snapshot.mode == "draining" else "300"  # draining ends with a restart, soon
            return error_response(message, 503, snapshot.mode, **{"Retry-After": retry})
        return None

    def model_allowed(allowed: Collection[str], real: str, upstream: str, public: str) -> bool:
        """Whether a group's list (empty = every model) lets in ``real`` on ``upstream``: by its ID for apps, the real
        ID with its provider, or the real ID alone unless an alias has that name (a bare name is the alias's, as in
        requests: the model then needs its provider). A model doesn't bring the aliases that send to it."""
        if not allowed or public in allowed or f"{real}@{upstream}" in allowed:
            return True
        return real in allowed and real not in snapshot.aliases

    def policy_refusal(client: ClientKey, model: str | None, allowed_by: set[str | None]) -> JSONResponse | None:
        """Admission checks from the client's policy (the only place QueueRouter itself says no).
        ``allowed_by``: the entries of the group's list that let the request in (see serve)."""
        policy = client.policy
        if policy.allowed_models and not allowed_by & set(policy.allowed_models):
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
            known = model_limits(sized(model or "", target, entry), [target.pool.context_cap])
            if known is None:
                return None  # one candidate with unknown context: don't guess
            contexts.append(known["context"])
        return min(contexts) if contexts else None

    def model_output(model: str | None, candidates: tuple[Candidate, ...]) -> int | None:
        """The longest answer the model gives, when it's known for every upstream it may go to (see sized): the
        smallest of them. None when one isn't known."""
        sizes = []
        for candidate in candidates:
            target = snapshot.upstreams.get(candidate.upstream_id)
            if target is None:
                continue
            entry = next((m for m in catalogs.models(target.id, raw=True) if m["id"] == model), None)
            size = listed_output(sized(model or "", target, entry))
            if size is None:
                return None
            sizes.append(size)
        return min(sizes) if sizes else None

    def output_source(model: str, target: Target, entry: dict | None) -> tuple[int | None, str]:
        """The longest answer ``model`` gives on ``target`` and where that comes from, first known first: yours (the
        Models page), learned from the provider refusing a bigger one, its model list, models.dev."""
        key = (target.id, model)
        manual = context_sizes.manual_for(model, target.id)
        if manual and manual.output:
            return manual.output, "yours"
        if key in context_sizes.learned_output:
            return context_sizes.learned_output[key], "learned"
        if (listed := listed_output(entry)) is not None:
            return listed, "its list"
        provider = prices.provider_of(target.upstream.base_url)  # the catalog the prices come from
        if found := prices.output(model, provider, snapshot.names.custom.get(key)):
            return found[0], f"models.dev: {found[1]}"
        return None, ""

    def sized(model: str, target: Target, entry: dict | None) -> dict | None:
        """``entry`` (a model list item, None when not listed) with the sizes QueueRouter knows beyond it: yours and
        the learned ones (context_sizes), and the output size in use as ``max_output_tokens`` (output_source)."""
        merged = context_sizes.apply(model, target.id, entry)
        output, _ = output_source(model, target, entry)
        if output is not None and (merged or {}).get("max_output_tokens") != output:  # the key every reader knows
            merged = {**(merged or {"id": model}), "max_output_tokens": output}
        return merged

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
            pending = [  # its choice of models is known before its list is: a model hidden there stays hidden
                t
                for t in usable
                if catalogs.ids(t.id) is None and not catalogs.failed(t.id) and (raw or catalogs.offered(t.id, model))
            ]
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

    def first_model(name: str | None) -> tuple[str | None, tuple[int, ...]]:
        """The model a name sends to first and the upstreams it may go to (none = any): an alias's model, through
        the aliases it forwards to, else the name itself; ``model@upstream`` only there. None: a loop."""
        only: tuple[int, ...] = ()
        seen: set[str] = set()
        while name and (alias := snapshot.aliases.get(name)) is not None:
            if name in seen:
                return None, ()
            seen.add(name)
            name, only = alias.target_model, alias.upstream_ids
        model, pin = split_pin(name)
        return model, pin or only

    app_ids_memo: list = [None, None, PublicIds()]  # [snapshot, model lists' state, the IDs]

    def app_ids() -> PublicIds:
        """The IDs apps know the offered models by (app/names.py), worked out again when a model list, the choice of
        models offered, a name or an alias changes. A provider whose list isn't in (just started, or failing) counts
        the models named there: the names are stored, so a name still reaches it as the real ID."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        state = tuple((t.id, catalogs.version(t.id), catalogs.filters.get(t.id)) for t in targets)
        if app_ids_memo[0] is not snapshot or app_ids_memo[1] != state:

            def offered_by(target: Target) -> Iterable[str]:
                ids = catalogs.ids(target.id)
                if ids is not None:
                    return ids
                return [m for uid, m in snapshot.names.custom if uid == target.id and catalogs.offered(target.id, m)]

            offered = {t.upstream.name: offered_by(t) for t in targets}
            app_ids_memo[:] = [snapshot, state, public_ids(offered, snapshot.names, snapshot.aliases)]
        return app_ids_memo[2]

    def app_id(model: str, upstream: str) -> str:
        """The ID apps know ``model`` on ``upstream`` by (``model@upstream`` for one not offered there)."""
        return app_ids().of.get((upstream, model), f"{model}@{upstream}")

    def embedding_slots(candidates: tuple[Candidate, ...]) -> tuple[Candidate, ...]:
        """Candidates using their pool's embedding slots (pool id negated) where it has its own limit."""
        return tuple(
            dataclasses.replace(c, pool_id=-c.pool_id)
            if (pool := snapshot.pools.get(c.pool_id)) is not None and pool.embedding_concurrency
            else c
            for c in candidates
        )

    def unknown_model(
        name: str | None, alias=None, raw: bool = False, only: tuple[int, ...] = ()
    ) -> JSONResponse | None:
        """A 404 with suggestions when no upstream offers ``name`` (only when every upstream's model
        list is known: a list we couldn't fetch proves nothing). ``raw``: hidden models count (an alias
        or QueueRouter's own request names it). ``only``: the upstreams it may go to (none = any)."""
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
                    "label": snapshot.names.of_request(f["model"], f["target_model"], f["upstream"]),
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

    def live_brief() -> dict:
        """What the Requests and Usage pages need from the live stream. Unlike live_state, it reads no database."""
        return {"inflight": inflight_view(), "usage_version": usage_version[0]}

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
            "recent": [
                {**r, "label": snapshot.names.of_request(r.get("model"), r.get("target_model"), r.get("upstream"))}
                for r in list(stats.recent)[-25:]
            ],
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
        except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
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
                        "label": snapshot.names.label(model["id"], target.upstream.name),
                        "upstreams": [f"only {target.upstream.name}"],
                        "context": known["context"] if known else None,
                        "hidden": not offered,
                        "alias": False,
                    }
                )
        for row in rows.values():
            row["label"] = snapshot.names.label_on(row["id"], row["upstreams"])
        for model_id, pinned in per_upstream.items():
            # offered by several upstreams, or an alias has its name (the bare choice is the alias): a choice names one
            if len(pinned) > 1 or model_id in snapshot.aliases:
                rows.update({p["id"]: p for p in pinned})
        models = {model_id: row["context"] for model_id, row in rows.items()}
        for alias in snapshot.aliases.values():
            first, _ = first_model(alias.name)  # what it sends to first (an alias may be another's model or fallback)
            rows[alias.name] = {
                "id": alias.name,
                "label": alias.name,
                "upstreams": [],
                "context": models.get(first or ""),
                "hidden": False,
                "alias": True,
            }
        return sorted(rows.values(), key=lambda r: (r["alias"], r["label"].lower()))

    def model_label(name: str | None, upstream: str | None = None) -> str:
        """``Provider|name`` for a model as aliases and jobs name it: on that upstream, the one it's pinned to, or
        every upstream listing it."""
        bare, pin = split_pin(name)
        if upstream or pin or not bare or bare in snapshot.aliases:
            return snapshot.names.label(name, upstream)
        listing = [t.upstream.name for t in snapshot.upstreams.values() if bare in (catalogs.ids(t.id, raw=True) or ())]
        return snapshot.names.label_on(bare, listing)

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
        """One row per model on a provider: what it reports, what was learned from its errors, the plan cap, your
        size and the one in use (None = unknown), and the output size in use with where it comes from; plus one row
        for a size set for every provider. For the Models page."""
        entries: dict[tuple[int, str], dict] = {}
        for target in snapshot.upstreams.values():
            for model in catalogs.models(target.id):
                entries[(target.id, model["id"])] = model
        learned = set(context_sizes.learned) | set(context_sizes.learned_output)
        keys = set(entries) | {k for k in learned if k[0] in snapshot.upstreams}
        keys |= {k for k in context_sizes.manual if k[0] == EVERY or k[0] in snapshot.upstreams}
        rows = []
        for upstream_id, model_id in keys:
            target = snapshot.upstreams.get(upstream_id)
            name = target.upstream.name if target else None
            entry = entries.get((upstream_id, model_id))
            reported = model_limits(entry, []) if entry else None
            caps = [target.pool.context_cap] if target else []
            used = model_limits(sized(model_id, target, entry), caps) if target else None
            output, source = output_source(model_id, target, entry) if target else (None, "")
            rows.append(
                {
                    "model": model_id,
                    "upstream_id": upstream_id,
                    "label": snapshot.names.label(model_id, name),
                    "reported": reported["context"] if reported else None,
                    "learned": context_sizes.learned.get((upstream_id, model_id)),
                    "learned_output": context_sizes.learned_output.get((upstream_id, model_id)),
                    "cap": target.pool.context_cap if target else None,
                    "manual": context_sizes.manual.get((upstream_id, model_id)),
                    "context": used["context"] if used else None,
                    "output": used["output"] if used else output,  # what opencode is told (unknown: the default)
                    "output_source": source if output is not None else "default",
                    "every": target is None,
                }
            )
        return sorted(rows, key=lambda r: r["label"].lower())

    def name_table() -> list[dict]:
        """Every model on a provider, its custom name and the ID apps know it by (None: not offered there), and what
        has that name already when it can't be the ID, for the Models page."""
        keys = {(t.id, m["id"]) for t in snapshot.upstreams.values() for m in catalogs.models(t.id)}
        keys |= {k for k in snapshot.names.custom if k[0] in snapshot.upstreams}
        ids = app_ids()
        rows = []
        for upstream_id, model_id in keys:
            upstream = snapshot.upstreams[upstream_id].upstream.name
            rows.append(
                {
                    "upstream_id": upstream_id,
                    "upstream": upstream,
                    "model": model_id,
                    "name": snapshot.names.custom.get((upstream_id, model_id), ""),
                    "app_id": ids.of.get((upstream, model_id)),
                    "clash": ids.clashes.get((upstream, model_id)),
                }
            )
        return sorted(rows, key=lambda r: (r["upstream"].lower(), r["model"].lower()))

    async def refresh_prices() -> bool:
        return await prices.refresh(app.state.client)

    async def refresh_scores() -> bool:
        score_attempt[0] = time.time()
        return bool(cfg().rank_key) and await scores.refresh(app.state.client, cfg().rank_key)

    def order_costs(names: Iterable[str], overrides: dict, mix: tuple[float, float, float]) -> dict[str, float]:
        """What each model ("model@upstream") or alias costs per million tokens of the usual token mix, the price
        opencode shows, for putting equal scores cheapest first: a plan nothing extra; unknown isn't there."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        entries = {(t.id, e["id"]): e for t in targets for e in catalogs.models(t.id, raw=True)}
        costs = {}
        for name in names:
            alias = snapshot.aliases.get(name)
            if alias and alias.council != "off":  # several calls: no price, as in opencode
                continue
            model, ids = first_model(name)
            on = [t for t in targets if (not ids or t.id in ids) and (t.id, model) in entries]
            if price := model_price(model, [(t, entries[(t.id, model)]) for t in on], overrides):
                costs[name] = 0.0 if price.source == "plan" else blended_price(price, mix)
        return costs

    def strength_order(
        names: list[str], manual: dict[tuple[int, str], float], costs: Mapping[str, float] | None = None
    ) -> tuple[list[tuple[str, str]], dict[str, dict]]:
        """opencode order of models and aliases ([(name, number prefix)]) and why ({name: score, source}): your
        own score first, else the index, looked up by the custom name (fine-tunes get their base model's); equal
        scores, and models without one, cheapest first (``costs``), then by name; aliases without one come first.
        ``names``: "model@upstream" and alias names."""
        index, info, ranked, first, rest = cfg().rank_index, {}, [], [], []
        for name in names:
            bare, pin = split_pin(name)
            yours = manual.get((pin[0], bare)) if pin else None
            if yours is None and not (pin and bare in snapshot.aliases):  # an alias's own is under its name
                yours = manual.get((EVERY, bare or ""))
            if yours is not None:
                info[name] = {"score": yours, "source": "yours", "inherited": False}
                ranked.append((name, yours))
            elif name in snapshot.aliases:
                info[name] = {"score": None, "source": "alias", "inherited": False}
                first.append(name)
            elif (hit := index_book()[0].score(snapshot.names.name(name), index)) is not None:
                info[name] = {"score": hit[0], "source": hit[1], "inherited": hit[2]}
                ranked.append((name, hit[0]))
            else:
                info[name] = {"score": None, "source": "", "inherited": False}
                rest.append(name)

        def tie(name: str) -> tuple:
            return (costs or {}).get(name, math.inf), snapshot.names.name(name).casefold(), name

        return ranking.numbered(ranked, first, rest, tie), info

    def index_book() -> tuple[ranking.Scores, str]:
        """The strength index in use: Artificial Analysis's own (with a key), else its scores as OpenRouter's
        public list carries them."""
        if scores.models:
            return scores, ""
        if reference_scores.fetched_at != reference.fetched_at:
            reference_scores.use(reference.index_entries(), reference.fetched_at)
        return reference_scores, "OpenRouter"

    def thinking_for(model_id: str, pairs: list, manual: dict) -> thinking_mod.Thinking | None:
        """A model's thinking levels: yours, else what the upstream's own list says, else the reference list
        by name; shaped for how that upstream is asked (app/thinking.py)."""
        yours = next((manual[(t.id, model_id)] for t, _ in pairs if (t.id, model_id) in manual), None)
        yours = manual.get((EVERY, model_id)) if yours is None else yours
        if yours is not None:
            efforts = thinking_mod.ordered(yours.split(","))
            return thinking_mod.Thinking(thinking_mod.levels(efforts), None, "yours") if efforts else None
        for target, entry in pairs:
            native = claude.native(target.upstream)  # Claude's own levels, which its model list states
            style = "claude" if native else thinking_mod.style_of(target.upstream.base_url, entry)
            if style in ("none", "fixed"):  # no levels to offer there
                continue
            if native:
                found = thinking_mod.from_claude(entry)
            else:
                found = thinking_mod.from_block(entry.get("reasoning"), entry.get("supported_parameters") or ())
            source = target.upstream.name
            if found is None or found == ("none", "high"):  # nothing, or only on / off: the reference may know more
                ref = reference.thinking(snapshot.names.name(model_id, target.upstream.name))
                if ref is not None and (found is not None or style in ("openai", "openrouter", "claude")):
                    found, source = ref, "OpenRouter (reference)"
            if found is None or (style == "unknown" and source != target.upstream.name):
                continue  # an unknown API: only levels its own list states
            if variants := thinking_mod.for_style(style, found):
                return thinking_mod.Thinking(variants, found[1], source)
        return None

    def ranking_table() -> dict:
        """The Models page's strength section: every model on a provider and every alias, its score and place."""
        keys = {f"{m}@{upstream}" for upstream, ids in models_by_upstream().items() for m in ids}
        names = sorted(keys | set(snapshot.aliases))
        costs = order_costs(names, store.list_model_prices(), read_mix() if mix_due() else mix_read["mix"])
        order, info = strength_order(names, store.list_model_scores(), costs)
        rows = []
        for key, prefix in order:
            model, pin = split_pin(key)
            upstream = snapshot.names.unpin(key)[1]
            rows.append(
                {
                    "model": model,
                    "upstream_id": pin[0] if pin else EVERY,
                    "label": snapshot.names.label(key),
                    "name": snapshot.names.name(key),
                    "upstream": upstream,
                    "prefix": prefix,
                    **info[key],
                }
            )
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
        """The Models page's thinking section: per model on a provider, the levels found (and where), and yours."""
        manual = store.list_model_thinking()
        rows = []
        for target in snapshot.upstreams.values():
            if target.usable:
                for entry in catalogs.models(target.id):
                    model = entry["id"]
                    rows.append(
                        {
                            "model": model,
                            "upstream_id": target.id,
                            "label": snapshot.names.label(model, target.upstream.name),
                            "found": thinking_for(model, [(target, entry)], {}),
                            "yours": manual.get((target.id, model), manual.get((EVERY, model))),
                        }
                    )
        rows.sort(key=lambda r: r["label"].lower())
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
            live_brief,
            context_table,
            upstream_models,
            model_choices,
            scores,
            refresh_scores,
            ranking_table,
            thinking_table,
            name_table,
            lambda: snapshot.names,
            model_label,
            lambda request: public_base(request),
            guard,
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

    def speaks_claude(adm: Admission, target: Target) -> bool:
        return adm.path == "chat/completions" and claude.native(target.upstream)

    def build_request(adm: Admission, target: Target) -> httpx.Request:
        s = cfg()
        native = speaks_claude(adm, target)
        path = "messages" if native else adm.path
        url = f"{target.upstream.base_url}/{path}" + (f"?{adm.query}" if adm.query else "")
        body = adm.body
        # how hard to think, not one of the model's levels (Claude Code's effort, from /v1/messages): the nearest one
        hint = adm.headers.get("x-queuerouter-effort") if adm.path == "chat/completions" else None
        thinking = bool(body) and (b'"reasoning_effort"' in body or bool(hint))  # asked as this provider understands
        images = bool(body) and not native and b'"image_url"' in body  # Claude reads images; others may not
        if body and (
            native or target.upstream.params or thinking or images
        ):  # this provider's own rules (e.g. fields it rejects)
            try:
                payload = json.loads(body)
            except ValueError:
                payload = None
            if isinstance(payload, dict):
                apply_rules(payload, rules_of(target.upstream.params), {})
                if native or thinking or images:
                    entry = next(
                        (m for m in catalogs.models(target.id, raw=True) if m.get("id") == payload.get("model")), None
                    )
                    if images and text_only(str(payload.get("model") or ""), target, entry):
                        payload = council_mod.without_images(payload)  # a note, rather than the provider's 400
                    if hint and "reasoning_effort" not in payload:  # to no model whose levels aren't known
                        found = thinking_for(str(payload.get("model") or ""), [(target, entry or {})], {})
                        level = thinking_mod.nearest([value for _, value in found.variants], hint) if found else None
                        if level:
                            payload["reasoning_effort"] = level
                    if native:
                        payload = claude.request(payload, entry)
                    else:
                        thinking_mod.translate(payload, thinking_mod.style_of(target.upstream.base_url, entry))
                body = json.dumps(payload).encode()
        headers = upstream_headers(adm.headers, target.api_key)
        if native:
            headers = claude_headers(headers, target.api_key)
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
        if not adm.place:
            adm.place.extend([None, None, 0.0])
        place = adm.place  # where the request first queued, and how long it has waited in the queue in all
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
                    place[0],
                    place[1],
                    preferred=preferred if preferred not in excluded else None,
                    prefer_wait_s=cfg().affinity_wait_s,
                )
                queued_at = time.monotonic()
                verdict = False
                if place[0] is None:
                    place[0], place[1] = ticket.enqueued_at, ticket.seq
                    if not ticket.granted:
                        yield Queued(queue.position(ticket))
                keepalive = keepalive_s()
                while not await queue.wait(ticket, step(adm, keepalive)):
                    if ticket.future.cancelled():  # evicted: maintenance mode
                        message = snapshot.maintenance_message or "QueueRouter is in maintenance mode"
                        yield Failed(503, message, "maintenance")
                        return
                    max_wait = adm.policy.queue_timeout_s or cfg().max_queue_wait_s
                    waited = place[2] + time.monotonic() - queued_at  # in the queue only, not at upstreams
                    if (max_wait and waited >= max_wait) or adm.expired():
                        yield None
                        return
                    # every upstream it could use was disabled or removed meanwhile: it would wait forever
                    if not any(
                        (t := snapshot.upstreams.get(c.upstream_id)) is not None and t.upstream.enabled for c in pool
                    ):
                        yield Failed(503, "no enabled upstream can serve this request now", "not_configured")
                        return
                    yield Waiting(queue.position(ticket), waited)
                await queue.apply_spacing(ticket)
                place[2] += time.monotonic() - queued_at
                adm.rlog.fields["queue_ms"] = round(place[2] * 1000)

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
                        sending = app.state.client.send(outgoing, stream=True)
                        if adm.deadline is not None:  # the key's total time limit holds before the first byte too
                            sending = asyncio.wait_for(sending, max(0.0, adm.deadline - time.monotonic()))
                        response = await sending
                        if speaks_claude(adm, target):
                            response = claude.answer(response)
                        if adm.rlog.fields["stream"]:  # a non-streamed answer's headers come with all of it
                            adm.rlog.fields["headers_ms"] = round((time.monotonic() - adm.rlog.sent_at) * 1000)
                    except TimeoutError:
                        yield Failed(504, "request exceeded this key's total time limit", "request_timeout")
                        return
                    except anthropic_api.Unsupported as exc:
                        yield Failed(400, str(exc), "invalid_request")
                        return
                    except (
                        httpx.TransportError,  # any network failure (write errors too), not only the usual ones
                        httpx.InvalidURL,  # a base URL httpx can't use (e.g. "host:8080v1"): this upstream's fault
                    ) as exc:
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
                                    if ticket.trial:  # a half-open trial must not stay reserved
                                        circuits.abandon(target.id)
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
                        if status in OVERFLOW_STATUS and requested_size(adm.body):
                            # an answer size the provider refuses, naming the most it gives: noted, and the request
                            # goes again at once on this slot asking for that much (each time less: it ends)
                            try:
                                content = await response.aread()
                            except httpx.HTTPError:
                                content = b""
                            finally:
                                await response.aclose()
                            most, message = output_limit_of(status, content)
                            if most is not None and (lowered := lowered_output(adm.body, most)) is not None:
                                adm.body, asked = lowered
                                await learn_output(target, model, most, message)
                                adm.rlog.fields["error"] = (
                                    f"{target.upstream.name} gives at most {most} output tokens: asked for {asked}, "
                                    f"sent again with {most}"
                                )
                                continue
                            response = Replay(response, content, complete=True)
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
                        finally:
                            await response.aclose()  # all of it is read (or the read was stopped): Replay has it
                        note_error(adm.rlog, status, content, target)
                        response = Replay(response, content, complete=True)

                    circuits.failure(target.id, reason)
                    verdict = True
                    others = [
                        c
                        for c in adm.candidates
                        if c.upstream_id != target.id and c.upstream_id not in excluded and available(c.upstream_id)
                    ]
                    # Failing over is the client's (or the global) retry budget's to allow; this upstream's own
                    # retry limit is about asking it again.
                    failover = bool(others) and can_retry(adm, retries)
                    if not failover and not can_retry(adm, retries, target):
                        if response is not None:
                            yield Got(response, target)  # pass the upstream's error through
                        else:
                            yield Failed(502, f"upstream unreachable: {reason}")
                        return
                    if response is not None:
                        await response.aclose()
                    retries += 1
                    if failover:  # keeping our place in the queue
                        excluded.add(target.id)
                        queue.cancel(ticket)
                        ticket = None
                        yield Backoff(0, retries, f"{reason} on {target.upstream.name}, failing over")
                        break
                    delay = retry_delay(retries - 1, headers, cfg().retry_base_delay_s, cfg().retry_max_delay_s)
                    # the slot is free while we wait (others on this pool go meanwhile); then we queue again in
                    # our place, and an upstream taken out of rotation meanwhile isn't asked again until it's back
                    queue.cancel(ticket)
                    ticket = None
                    yield Backoff(delay, retries, reason)
                    break
        finally:
            adm.rlog.fields["retries"] = retries
            if ticket is not None:
                if ticket.target is not None and not verdict and ticket.trial:
                    circuits.abandon(ticket.target.upstream_id)  # only the half-open trial this request was
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

    async def learn_output(target: Target, model: str, most: int, message: str) -> None:
        """The most a provider says ``model`` answers (refusing a bigger answer size): kept, so opencode is told it
        and answer sizes asked for later are lowered to it before they're sent."""
        key = (target.id, model)
        if not model or context_sizes.learned_output.get(key) == most:
            return
        context_sizes.learned_output[key] = most
        await asyncio.to_thread(store.learn_model_output, target.id, model, most, message)
        log.info(
            json.dumps({"event": "output_learned", "upstream": target.upstream.name, "model": model, "output": most})
        )

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
        # only a limit this request could have hit is learned: providers repeat parts of a request in their errors,
        # so a short request "saying" a small limit mustn't shrink the model for every client
        estimate = adm.rlog.fields.get("estimate")
        plausible = isinstance(estimate, int | float) and limit and estimate * stats.ratio(model) >= limit / 2
        if plausible and model and context_sizes.learned.get((upstream.id, model)) != limit:
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
            last = index == len(steps) - 1 and adm.hop is None  # a forward after it: not the last
            nxt = steps[index + 1].model if index + 1 < len(steps) else adm.hop.alias if adm.hop else None

            def fall(reason: str, step=step, nxt=nxt):
                add_note(adm.rlog, "fallback", f"{step.model} -> {nxt} ({reason})")
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
                        if status == 200 and adm.check is not None:  # held back whole, then checked
                            result: tuple = (None, "")
                            checks = check_answer(adm, step, event, events, triggers)
                            async with contextlib.aclosing(checks) as items:
                                async for item in items:
                                    if isinstance(item, Checking):
                                        yield item
                                    else:
                                        result = item
                            held = None  # passed on, or closed by check_answer
                            passed, why = result
                            if passed is not None:
                                yield passed
                                return
                            yield fall(why)
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
        if adm.hop is not None:  # every model of its own moved on: the alias after them takes the request
            yield Forward(adm.hop)

    async def check_answer(
        adm: Admission, step: Step, got: Got, events: AsyncIterator, triggers: frozenset[str]
    ) -> AsyncIterator[Checking | tuple[Got | None, str]]:
        """An alias's answer check (app/answer_check.py): the answer is held back whole while it comes, then its
        checker reads it. Yields ``Checking`` while waiting, then (the ``Got`` to pass on, "") or (None, why it
        failed: the next model's turn). An answer that calls tools, or that still runs when the key's time is up,
        is passed on at once, unchecked. Once all of it is read, ``events`` (the attempt) is closed so its slot is
        free meanwhile, for the checker's request too."""
        stream = bool(adm.rlog.fields["stream"])
        timed = UsageSniffer() if stream else None  # the answer's real times, as it comes (the app sees it later)
        held = None
        try:
            reads = hold_answer(
                got.response,
                stream,
                keepalive_s(),
                adm.deadline,
                (lambda chunk: timed.feed(chunk, time.monotonic())) if timed else None,
            )
            async with contextlib.aclosing(reads) as items:
                async for item in items:
                    if item is None:
                        yield Checking()
                    else:
                        held = item
        except httpx.HTTPError as exc:  # broke off before any of it was passed on: the next model can still answer
            await got.response.aclose()
            if "errors" not in triggers:
                raise
            yield None, f"answer broke off: {type(exc).__name__}"
            return
        replay = held.replay
        replay.sniffer = timed
        if held.acting or not held.whole:
            if not held.acting:
                add_note(adm.rlog, "answer_check", f"{step.model}: not checked (no time left)")
            yield Got(replay, got.target), ""
            return
        await events.aclose()  # all of it is read: the slot is free
        answered = time.monotonic()
        try:
            if "refused" in triggers and is_refusal_text(held.text, held.finish):
                note_discarded(adm, step, held, timed, answered, "refused")
                await replay.aclose()
                yield None, "refused"
                return
            check = adm.check
            limit = answer_check.TIMEOUT_S
            if adm.deadline is not None:
                limit = min(limit, adm.deadline - answered)
            if limit < 1:
                add_note(adm.rlog, "answer_check", f"{step.model}: not checked (no time left)")
                yield Got(replay, got.target), ""
                return
            adm.rlog.fields["stage"] = "checking"
            yield Checking(f"checking the answer of {step.model}")
            payload = answer_check.check_payload(check.model, check.instructions, step.body, held.text)
            task = asyncio.ensure_future(
                internal_completion(
                    check.client,
                    payload,
                    LANE_NAMES[adm.lane],
                    tag="check",
                    origin=adm.headers,
                    role="answer check",
                    parent=adm.request_id,
                )
            )
            started = time.monotonic()
            try:
                while not task.done() and (left := started + limit - time.monotonic()) > 0:
                    await asyncio.wait({task}, timeout=min(keepalive_s(), left))
                    if not task.done():
                        yield Checking()
            finally:
                if not task.done():
                    task.cancel()  # its own request ends too (its slot freed), as a cancelled one
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            if task.cancelled():
                passed, reason = None, f"the checker took over {round(limit)} s"
            elif (body := task.result()) is None:
                passed, reason = None, "the checker's request failed"
            else:
                passed, reason = answer_check.verdict_of(body)
            seconds = f"{time.monotonic() - started:.1f} s"
            if passed is None:
                add_note(adm.rlog, "answer_check", f"{step.model}: not checked ({reason}), passed on")
                log.warning(json.dumps({"event": "check_failed", "client": adm.client, "reason": reason}))
            elif passed:
                add_note(adm.rlog, "answer_check", f"{step.model}: passed ({seconds})")
            else:
                add_note(adm.rlog, "answer_check", f"{step.model}: failed ({seconds}): {reason}")
                note_discarded(adm, step, held, timed, answered, "failed answer check", f"failed: {reason}")
                await replay.aclose()
                yield None, f"check: {reason}"
                return
        except BaseException:
            await replay.aclose()
            raise
        finally:
            adm.rlog.fields["stage"] = None
        yield Got(replay, got.target), ""

    def note_discarded(
        adm: Admission,
        step: Step,
        held,
        timed: UsageSniffer | None,
        answered: float,
        role: str,
        check: str | None = None,
    ) -> None:
        """An answer the app never got (it failed the check, or refused): a row of its own under the request on
        the Requests page, so what it cost counts (usage, the client's limits and quotas, its run's budget).
        ``answered``: when all of it had come (monotonic)."""
        main = adm.rlog
        if timed is not None:
            tokens = timed.prompt_tokens, timed.completion_tokens, timed.cached_tokens, timed.cache_write_tokens
            timings = timed.timings(main.sent_at)
        else:
            tokens, timings = usage_from_body(held.replay.prefix), {}
        prompt, completion, cached, written = tokens
        row = RequestLog(f"qr-{uuid.uuid4().hex[:12]}", main.fields["method"], main.fields["path"], main.fields["ip"])
        row.started = main.started + (time.monotonic() - answered)  # its total time ends when it had all come
        kept = ("client", "priority", "session", "model", "upstream", "stream", "position", "queue_ms", "retries")
        kept += ("tags", "run", "user_agent", "sent_user_agent", "client_headers", "headers_ms")
        row.fields.update({key: main.fields[key] for key in kept})
        row.fields.update(
            target_model=step.model,
            compaction=step.compaction,
            estimate=step.estimate,
            role=role,
            parent=main.fields["id"],
            answer_check=check,
        )
        granted = _granted_at(main)
        row.finish(
            stats,
            outcome="done",
            status=200,
            upstream_ms=round((answered - granted) * 1000) if granted else None,
            prompt_tokens=prompt,
            completion_tokens=completion,
            cached_tokens=cached,
            cache_write_tokens=written,
            finish_reason=held.finish,
            **timings,
        )
        used = (prompt or 0) + (completion or 0)
        limiter.record_tokens(adm.client, used)
        if adm.run is not None:
            runs.note_tokens(adm.run, used)

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
        if stale := [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s) or catalogs.loading(t.id)]:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))  # a first list on its way too, or it's missing
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
                if t.id in entries and (lim := model_limits(sized(model_id, t, entries[t.id]), [t.pool.context_cap]))
            ]
            if not known:
                return None
            context = min(lim["context"] for lim in known)
            return {"context": context, "output": min(min(lim["output"] for lim in known), context)}

        def size_of(name: str, on: list[Target]) -> dict[str, int] | None:
            """A model's or an alias's limits (an alias: its target, on its pinned upstreams)."""
            bare, ids = first_model(name)  # an alias: its first model, through the aliases in it
            return limits_on(bare, [t for t in on if not ids or t.id in ids]) if bare else None

        offered_by = {m: [t.upstream.name for t, _ in pairs] for m, pairs in listed.items()}  # shown in opencode
        manual_thinking = await asyncio.to_thread(store.list_model_thinking) if with_thinking else {}
        show_prices, rank = cfg().opencode_price, cfg().opencode_rank
        priced = with_thinking or show_prices or rank  # equal scores go cheapest first
        overrides = await asyncio.to_thread(store.list_model_prices) if priced else {}  # current
        mix = await current_mix() if show_prices or rank else DEFAULT_MIX
        shown: dict[str, str] = {}  # the price in each entry's name
        allowed = client.policy.allowed_models
        result: dict[str, dict] = {}
        public: dict[str, str] = {}  # "model@upstream" -> the ID apps know it by
        for model_id in sorted(served):
            for target, entry in served[model_id]:  # one entry per provider: you choose which serves it
                key = f"{model_id}@{target.upstream.name}"
                public[key] = app_id(model_id, target.upstream.name)
                if not model_allowed(allowed, model_id, target.upstream.name, public[key]):
                    continue
                pair = [(target, entry)]
                limits = limits_on(model_id, [target])
                name = snapshot.names.listed(model_id, [target.upstream.name])
                result[key] = {"name": name, **({"limit": limits} if limits else {})}
                price = model_price(model_id, pair, overrides)
                if show_prices:
                    shown[key] = price_text(price, mix)
                if with_thinking and (levels := thinking_for(model_id, pair, manual_thinking)):
                    result[key]["thinking"] = {"variants": levels.variants, "default": levels.default}
                if with_thinking and price:
                    result[key]["cost"] = price_json(price)
                if with_thinking and reads_images(model_id, pair):
                    result[key]["input"] = ["text", "image"]
        for alias in sorted(snapshot.aliases.values(), key=lambda a: a.name):
            if allowed and alias.name not in allowed:  # by its own name: the model it sends to doesn't bring it
                continue
            target, ids = first_model(alias.name)  # "model@upstream" pins it like the upstream ticks
            pinned = [t for t in targets if not ids or t.id in ids]
            limits = limits_on(target, pinned) if target else None
            if alias.council != "off":  # every member and the judge must take the conversation: the smallest counts
                names = [alias.target_model, alias.council_judge or alias.target_model, *alias.council_members]
                sizes = [s for s in (size_of(n, targets) for n in names) if s]
                if sizes:
                    context = min(s["context"] for s in sizes)
                    limits = {"context": context, "output": min(min(s["output"] for s in sizes), context)}
            if alias.virtual_context:  # told a bigger context than the model has; compaction makes it fit
                output = limits["output"] if limits else DEFAULT_OUTPUT
                limits = {"context": alias.virtual_context, "output": min(output, alias.virtual_context)}
            on = providers(
                [t.upstream.name for t in pinned if t.upstream.name in offered_by.get(target, [])]
                or [t.upstream.name for t in pinned]
            )  # where it's sent, not what to
            result[alias.name] = {
                "name": f"{alias.name}{SEPARATOR}{on}" if on else alias.name,
                **({"limit": limits} if limits else {}),
            }
            plain = alias.council == "off"  # a council's members and judge are asked as they are
            pairs = [(t, e) for t, e in listed.get(target, []) if t in pinned]
            if with_thinking and plain and (levels := thinking_for(target, pairs, manual_thinking)):
                result[alias.name]["thinking"] = {"variants": levels.variants, "default": levels.default}
            # a council makes several calls opencode never sees: no price rather than a low one
            price = model_price(target, pairs, overrides) if plain else None
            if show_prices and plain:
                shown[alias.name] = price_text(price, mix)
            if with_thinking and price:
                result[alias.name]["cost"] = price_json(price)
            if with_thinking and reads_images(target, pairs):
                result[alias.name]["input"] = ["text", "image"]
        order, why = [(name, "") for name in result], {}
        if rank:
            # opencode's desktop app sorts by name, so the order travels in the names; its terminal keeps this order
            scored = await asyncio.to_thread(store.list_model_scores)
            order, why = strength_order(list(result), scored, order_costs(result, overrides, mix))
        return {
            public.get(name, name): {
                **result[name],
                "name": ranking.label(
                    prefix, why.get(name, {}).get("score"), shown.get(name, ""), result[name]["name"]
                ),
            }
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
        return bool(reference.images(snapshot.names.name(model, pairs[0][0].upstream.name) if pairs else model))

    def text_only(model: str, target: Target, entry: dict | None) -> bool:
        """Whether a model is known to read text only on this upstream (its list, else the reference list by name).
        Unknown isn't text only: the provider decides."""
        said = thinking_mod.takes_images(entry)
        if said is None:
            said = reference.images(snapshot.names.name(model, target.upstream.name))
        return said is False

    def text_only_fit(payload: dict) -> dict:
        """``payload`` with its images replaced by a note when its model doesn't read them (judged as for
        opencode): council members and judges get the conversation the app sent for the alias."""
        stripped = council_mod.without_images(payload)
        model = payload.get("model")
        if stripped is payload or not isinstance(model, str):  # no images
            return payload
        name, only = first_model(model)
        pairs = [
            (t, e)
            for t in snapshot.upstreams.values()
            if t.usable and (not only or t.id in only)
            for e in catalogs.models(t.id, raw=True)
            if e["id"] == name
        ]
        return payload if reads_images(name or "", pairs) else stripped

    def price_json(price: Price) -> dict:
        """USD per million tokens, for the plugin (opencode works out the session cost from it)."""
        return {
            "input": round(price.input, 6),
            "output": round(price.output, 6),
            "cache_read": round(price.cached_input, 6),
        }

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
        user_agent = request.headers.get("user-agent", "")
        if guard.is_blocked(ip):
            guard.refused(ip, user_agent)
            return error_response("too many failed attempts", 429, "blocked")
        if not hmac.compare_digest(presented.encode(), token.encode()):
            guard.fail(ip, user_agent)
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
        """Every offered model on every usable upstream (lists cached; doesn't use a slot), under its ID for apps,
        plus the aliases, limited to the client's allowed models. A model with a custom name gets QueueRouter's own
        entry, so nothing of the provider's shows; the others keep the provider's. In opencode's order, for apps that
        keep the list's: strongest first, equal scores cheapest first, then by name; aliases without a score last."""
        targets = [t for t in snapshot.upstreams.values() if t.usable]
        if stale := [t for t in targets if catalogs.stale(t.id, cfg().models_cache_s) or catalogs.loading(t.id)]:
            await asyncio.gather(*(refresh_catalog(t) for t in stale))  # a first list on its way too, or it's missing
        ids = app_ids()
        allowed = set(client.policy.allowed_models or ())
        manual_thinking = await asyncio.to_thread(store.list_model_thinking)
        merged: dict[str, dict] = {}
        inner: dict[str, str] = {}  # "model@upstream" or an alias's name -> the ID in the list
        everything: dict[str, dict] = {}  # hidden models too: an alias may point at one
        for target in targets:
            upstream = target.upstream.name
            for model in catalogs.models(target.id, raw=True):
                real = model["id"]
                entry = sized(real, target, model)
                everything.setdefault(real, entry)
                public = ids.of.get((upstream, real))
                if public is None or not model_allowed(allowed, real, upstream, public):
                    continue  # hidden there, or not this client's
                if (target.id, real) in snapshot.names.custom:
                    pair = [(target, model)]
                    levels = thinking_for(real, pair, manual_thinking)
                    merged[public] = own_entry(public, entry, levels, reads_images(real, pair))
                else:
                    merged[public] = {**entry, "id": public}
                inner[f"{real}@{upstream}"] = public
        if not merged and all(catalogs.ids(t.id) is None for t in targets):
            return error_response("no upstream returned a model list", 502, "upstream_error")
        for alias in snapshot.aliases.values():
            if not allowed or alias.name in allowed:
                merged[alias.name] = alias_entry(alias, everything.get(first_model(alias.name)[0] or ""))
                inner[alias.name] = alias.name
        scored = await asyncio.to_thread(store.list_model_scores)
        overrides = await asyncio.to_thread(store.list_model_prices)
        order, why = strength_order(list(inner), scored, order_costs(inner, overrides, await current_mix()))
        listed = [merged[inner[name]] for name, _ in order if why[name]["source"] != "alias"]
        return listed + [merged[name] for name, _ in order if why[name]["source"] == "alias"]  # no score: last

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
        allowed = client.policy.allowed_models  # a bare name an alias has is the alias's, as in model_allowed
        lets_in = not allowed or model_id in allowed or (bare in allowed and bare not in snapshot.aliases)
        if found is None and pin and lets_in:
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
        page and at the provider; ``role`` and ``parent`` label it there. Images become a note for a
        model that reads text only."""
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
                json.dumps(text_only_fit(payload)).encode(),
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

    def chosen_edits(alias, client: ClientKey, done: frozenset[int]) -> list[edits_mod.Edit]:
        """The prompt edits a request gets, in order: its alias's, then its group's and its client's own (the
        client's policy has both); those already made to it (``done``: forwarded from another alias) aren't again."""
        ids = dict.fromkeys((*(alias.edits if alias else ()), *client.policy.edits))
        found = (snapshot.edits.get(i) for i in ids if i not in done)
        return [edit for edit in found if edit is not None and edit.on]

    def edit_asker(client: ClientKey, headers: Mapping[str, str], request_id: str) -> edits_mod.Ask:
        """How a request's model edits ask their model: as its client, in its lane, as a call made for it (a row
        of its own under it on the Requests page, so its cost counts)."""
        priority = LANE_NAMES[request_lane(headers, client)]

        async def ask(edit: edits_mod.Edit, messages: list[dict]) -> str | None:
            payload = {"model": edit.settings.get("model"), "messages": messages}
            role = f"prompt edit: {edit.label}"[:80]
            body = await internal_completion(
                client, payload, priority, tag="edit", origin=headers, role=role, parent=request_id
            )
            return edits_mod.reply_text(body)

        return ask

    def summariser(alias, step_model: str) -> tuple[str, int | None]:
        """The model that writes summaries (the alias's compaction model, else the step's) and its context."""
        name = alias.compact_model or step_model
        real, only = first_model(name)
        return name, model_context(real, candidates_for(real, only, raw=True)) if real else None

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
            model, pin = first_model(named)
            candidates = candidates_for(model, pin, raw=True) if model else ()
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
        request,
        client,
        alias,
        payload,
        request_headers,
        lane,
        tags,
        path,
        body,
        query,
        original: dict | None = None,
        onward: dict | None = None,
        edit_info: tuple[str, edits_mod.Outcome | None] | None = None,
    ) -> Response:
        """Members answer at once (internal requests, tagged "council"), the judge picks one or writes the
        answer. A streaming app gets its response headers at once and keep-alives meanwhile. ``onward``: how the
        request was forwarded here from another alias (serve's ``via``, ``then``, ``parent``, ``place``);
        ``edit_info``: the request's id and what its prompt edits did (already in ``payload``)."""
        onward = onward or {}
        via = onward.get("via") or ()
        priority = LANE_NAMES[lane]
        stream = payload.get("stream") is True
        request_id, edit_outcome = edit_info or (f"qr-{uuid.uuid4().hex[:12]}", None)
        rlog = RequestLog(request_id, "POST", f"/v1/{path}", client_ip(request, bootstrap.trusted_proxies))
        if edit_outcome is not None:
            rlog.fields["edits"] = edit_outcome.summary()
            rlog.edit_detail = edit_outcome.detail
        rlog.fields.update(
            client=client.name,
            user_agent=(request_headers.get("user-agent") or "")[:300] or None,
            client_headers=client_identity(request_headers, payload),
            role=(request_headers.get("x-queuerouter-role") or "")[:80] or None,
            parent=onward.get("parent") or (request_headers.get("x-queuerouter-parent") or "")[:40] or None,
            priority=priority,
            model=via[0] if via else alias.name,
            via=" → ".join((*via, alias.name))[:300] if via else None,
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
            told_to_finish = False  # a round ran past the rounds or time budget: its next answer is the last
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
                elif (name == websearch.TOOL_NAME and searching) or (name == websearch.FETCH_NAME and fetching):
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
                # told to finish (rounds or time used up), still delegating: keep what it says
                if rounds > alias.orch_rounds or told_to_finish:
                    rlog.fields["council"] = f"{summary} (budget)"
                    progress("Budget used up: taking the orchestrator's answer as it is.")
                    return council_mod.without_calls(body, own_names), None
                if note := " ".join(str(message.get("content") or "").split())[:300]:
                    progress("Orchestrator", f": {note}")
                rounds += 1
                closed = rounds > alias.orch_rounds or time.monotonic() > deadline
                told_to_finish = told_to_finish or closed
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
            # only the tools this alias has on: a call to the other one is the app's (or nobody's) to answer
            own_web = {*([websearch.TOOL_NAME] if searching else []), *([websearch.FETCH_NAME] if fetching else [])}

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
                json.dumps(text_only_fit(judged)).encode(),
                judge_headers,
                query,
                allow_defer=False,
                allow_council=False,
                internal=True,  # QueueRouter's own call, like the members: the key's model list allowed the alias
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
            return await serve(request, client, path, body, request_headers, query, allow_council=False, **onward)

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
            min(1000, max(1, int(q.get("limit", "100")) if q.get("limit", "100").isdecimal() else 100)),
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
        await asyncio.to_thread(store.create_jobs, batch_jobs)  # first: a batch is never seen without its jobs
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
            min(100, int(limit) if limit.isdecimal() else 20),
            request.query_params.get("after"),
        )
        data = [jobs_mod.batch_view(store, r) for r in rows]
        return {
            "object": "list",
            "data": data,
            "first_id": data[0]["id"] if data else None,
            "last_id": data[-1]["id"] if data else None,
            "has_more": len(data) == int(limit if limit.isdecimal() else 20),
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
        if declared and declared.isdecimal() and int(declared) > max_body:
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

    def log_refusal(request: Request, client: ClientKey, path: str, status: int, message: str, model) -> None:
        """A request QueueRouter turned away before queueing it, on the Requests page too: the app sees only the
        answer (a model not allowed for the key, a request an adapter can't translate...)."""
        rlog = RequestLog(f"qr-{uuid.uuid4().hex[:12]}", "POST", path, client_ip(request, bootstrap.trusted_proxies))
        rlog.fields.update(
            client=client.name,
            model=model if isinstance(model, str) else None,
            user_agent=(request.headers.get("user-agent") or "")[:300] or None,
            client_headers=client_identity(request.headers, None),
        )
        rlog.finish(stats, outcome="refused" if status < 500 else "error", status=status, error=message[:500])

    def adapter_refused(request: Request, client: ClientKey, path: str, message: str, payload) -> None:
        """A request an API adapter couldn't translate."""
        model = payload.get("model") if isinstance(payload, dict) else None
        log_refusal(request, client, path, 400, message, model)

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
        payload = None
        try:
            payload = json.loads(body)
            chat = anthropic_api.to_chat(payload)
            effort = anthropic_api.effort_of(payload)
        except (ValueError, AttributeError, KeyError, TypeError) as exc:  # not JSON, or fields of the wrong kind
            message = str(exc) if isinstance(exc, anthropic_api.Unsupported) else "the body isn't a valid request"
            adapter_refused(request, client, "/v1/messages", message, payload)
            return JSONResponse(anthropic_api.error(400, message), status_code=400)
        model = str(payload.get("model") or "")
        headers = adapter_headers(request, "api:anthropic")
        if effort:  # passed on only to a model whose thinking levels are known (build_request)
            headers["x-queuerouter-effort"] = effort
        thinking = anthropic_api.wants_thinking(payload)  # the model's reasoning as thinking blocks
        seen = anthropic_api.used_ids(payload)  # a new tool call doesn't reuse an id of the conversation
        rounds = SearchRounds(request, client, chat, headers, anthropic_api.search_tool(payload))
        response = await rounds.ask()
        if isinstance(response, StreamingResponse):
            translation = anthropic_api.Translation(model, thinking, seen)
            return StreamingResponse(
                rounds.stream(response, translation),
                media_type="text/event-stream",
                headers={**kept_headers(response), "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await rounds.whole(response, model, thinking, seen)

    class SearchRounds:
        """One /v1/messages request, asked of the model once or, with Claude's web search tool, in rounds: the model
        has the tool as a function, and when it calls it, QueueRouter runs the search itself (Settings > Web search)
        and asks again with the results, up to the tool's ``max_uses``. The app gets one message, with the searches
        in it as Claude's own server tool blocks. Each round is a request of its own on the Requests page."""

        def __init__(self, request: Request, client: ClientKey, chat: dict, headers: dict, tool: dict | None) -> None:
            self.request, self.client, self.chat, self.headers, self.tool = request, client, chat, headers, tool
            self.name = str((tool or {}).get("name") or "web_search")
            uses = (tool or {}).get("max_uses")
            self.left = min(uses, MAX_SEARCHES) if isinstance(uses, int) and uses > 0 else DEFAULT_SEARCHES

        async def ask(self) -> Response:
            return await serve(
                self.request, self.client, "chat/completions", json.dumps(self.chat).encode(), self.headers, ""
            )

        def searches(self, calls: list[dict]) -> list[dict]:
            """The calls of QueueRouter's search among a round's tool calls ({id, name, arguments})."""
            return [c for c in calls if self.tool is not None and c["name"] == self.name]

        async def search(self, call: dict) -> tuple[str, list[dict] | None, str, str]:
            """(query, results, Claude's error code or "", what the model is told) for one search call."""
            query = websearch.call_argument({"function": {"arguments": call["arguments"]}}, "query")
            if self.left <= 0:
                return query, None, "max_uses_exceeded", "The search limit is reached: answer with what you have."
            self.left -= 1
            s = cfg()
            try:
                found = await websearch.search(
                    app.state.client, s.search_provider, s.search_key, s.search_url, query, SEARCH_RESULTS
                )
            except websearch.SearchError as exc:
                reason = str(exc)
                code = (
                    "invalid_input"
                    if reason.startswith("refused") or reason == "empty query"
                    else "too_many_requests"
                    if "HTTP 429" in reason
                    else "unavailable"
                )
                return query, None, code, f"Web search failed: {reason}"
            found = anthropic_api.within_domains(found, self.tool or {})
            return query, found, "", websearch.results_text(query, found)

        def next_round(self, text: str, calls: list[dict], told: list[str]) -> None:
            """The model's turn and the search results go into the conversation; past the limit the tool goes."""
            for call in calls:
                call["id"] = call["id"] or f"call_{uuid.uuid4().hex[:24]}"
            asked = [
                {
                    "id": c["id"],
                    "type": "function",
                    "function": {"name": c["name"], "arguments": c["arguments"] or "{}"},
                }
                for c in calls
            ]
            self.chat["messages"].append({"role": "assistant", "content": text or None, "tool_calls": asked})
            for call, content in zip(calls, told, strict=True):
                self.chat["messages"].append({"role": "tool", "tool_call_id": call["id"], "content": content})
            self.chat.pop("tool_choice", None)  # a forced search is done: the model decides now
            if self.left <= 0:
                tools = [t for t in self.chat.get("tools") or [] if (t.get("function") or {}).get("name") != self.name]
                self.chat["tools"] = tools
                if not tools:
                    self.chat.pop("tools")

        async def stream(
            self, response: StreamingResponse, translation: anthropic_api.Translation
        ) -> AsyncIterator[bytes]:
            while True:
                async for event in translation.feed(response.body_iterator):
                    yield event
                if translation.failed:
                    return
                calls = list(translation.calls.values())
                searches = self.searches(calls)
                if not searches:
                    yield translation.tool_uses()
                    yield translation.end()
                    return
                told = []
                for call in searches:
                    yield translation.ping()  # the search can take a few seconds
                    query, found, failure, content = await self.search(call)
                    yield translation.web_search(self.name, query, found, failure)
                    told.append(content)
                self.next_round(translation.text, searches, told)
                response = await self.ask()
                if not isinstance(response, StreamingResponse):  # refused or failed before it started
                    yield translation.error(response.status_code, response.body)
                    return

        async def whole(self, response: Response, model: str, thinking: bool, seen: set[str]) -> Response:
            content: list[dict] = []
            searched = 0
            while True:
                if response.status_code >= 400:
                    return anthropic_denied(response)
                try:
                    body = json.loads(response.body)
                    message = anthropic_api.from_chat(body, model, thinking, seen)
                    calls = anthropic_api.chat_calls(body)
                except (ValueError, AttributeError):
                    return JSONResponse(
                        anthropic_api.error(502, "the upstream answer was not valid JSON"), status_code=502
                    )
                searches = self.searches(calls)
                if not searches:
                    message["content"] = content + message["content"]
                    if searched:
                        message["usage"]["server_tool_use"] = {"web_search_requests": searched}
                    return JSONResponse(message, headers=kept_headers(response))
                content += [b for b in message["content"] if b["type"] in ("thinking", "text")]
                told = []
                for call in searches:
                    query, found, failure, said = await self.search(call)
                    content += anthropic_api.search_blocks(self.name, query, found, failure)
                    told.append(said)
                    searched += 1
                text = "".join(b["text"] for b in message["content"] if b["type"] == "text")
                self.next_round(text, searches, told)
                response = await self.ask()

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
        payload = None
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
        except (ValueError, AttributeError, KeyError, TypeError) as exc:  # not JSON, or fields of the wrong kind
            message = str(exc) if isinstance(exc, responses_api.Unsupported) else "the body isn't a valid request"
            adapter_refused(request, client, "/v1/responses", message, payload)
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
        via: tuple[str, ...] = (),
        then: tuple[tuple[str, int], ...] = (),
        start: int = 0,
        parent: str | None = None,
        place: list | None = None,
        edited: frozenset[int] = frozenset(),
    ) -> Response:
        """Everything after authentication: policies, aliases/fallbacks, queueing, forwarding.
        Used by the API and by the admin Playground (which passes its own clean ``headers``).
        ``allow_defer``: a streaming request that must wait for a summary is answered at once (see
        deferred_response) and served again once the summary is ready. A request forwarded from another alias
        (Hop): ``via`` the aliases it came through (the first is what the app asked for, already let in), ``then``
        where it goes on if this alias runs out, ``start`` where in this alias's list it begins, ``parent`` the
        request it was forwarded from, ``place`` that one's place in the queue (kept, and not counted again),
        ``edited`` the prompt edits already made to it (its body has them)."""
        if unavailable := not_configured() or mode_refusal():
            return unavailable
        original_body = body  # what a forwarded request starts from (``body`` changes below)
        request_headers = headers if headers is not None else dict(request.headers)
        try:
            payload = json.loads(body) if body else {}
        except ValueError:  # e.g. multipart (audio transcriptions): forwarded as it is, no rules written into it
            payload = None
        model = payload.get("model") if isinstance(payload, dict) else None
        model = asked = model if isinstance(model, str) else None
        alias = snapshot.aliases.get(model) if model else None
        ids = app_ids() if model and not alias and not internal else None
        # a model's ID for apps: its real ID, only on that provider. A real ID one provider offers stays as it is
        # (the body unchanged, an upstream whose list isn't in yet still a candidate)
        if ids and model in ids.to and (ids.to[model][0] != model or model in ids.several):
            model = "{}@{}".format(*ids.to[model])
            payload["model"] = model
        target_model = alias.target_model if alias else model
        bare_target, target_pin = split_pin(target_model)

        def refused(answer: Response) -> Response:
            """QueueRouter's own no, on the Requests page too."""
            try:
                message = str(json.loads(answer.body)["error"]["message"])
            except (ValueError, KeyError, TypeError):
                message = f"HTTP {answer.status_code}"
            log_refusal(request, client, f"/v1/{path}", answer.status_code, message, model)
            return answer

        if ids and model in ids.shared:  # several providers offer it: the app picks one, QueueRouter doesn't
            choices = ", ".join(ids.shared[model])
            return refused(
                error_response(
                    f"model '{model}' is offered by several providers: use one of {choices}", 400, "model_ambiguous"
                )
            )
        # The group's list lets in an alias by its name only, not the model it sends to; a model by its ID for apps,
        # the real one with its provider, or the real one alone unless an alias has that name (then it's the alias's)
        if alias:
            allowed_by = {alias.name}
        else:
            allowed_by = {model, asked} | ({bare_target} if bare_target not in snapshot.aliases else set())
        # QueueRouter's own requests (council members, judge, summaries) serve a request already admitted: the
        # client's model list and queue limits apply to that request, not to the models working on it
        if not internal and not via and (refusal := policy_refusal(client, asked or model, allowed_by)):
            return refused(refusal)
        # The chain: the requested model, or the alias's model then its fallbacks (from ``start``). Another alias in
        # it is forwarded to (Hop): the request is served as one for that alias, its whole way (council,
        # orchestrator, answer check, its fallbacks), and comes back here for what follows it if that runs out.
        if alias:
            entries = [(alias.target_model, alias.upstream_ids), *((name, ()) for name in alias.fallbacks)][start:]
        else:
            entries = [(target_model or "", ())]  # "": no model named
        forwards = alias is not None and isinstance(payload, dict)  # a body that isn't JSON goes as it is
        cut = next((i for i, (name, _) in enumerate(entries) if forwards and name in snapshot.aliases), len(entries))
        hop = None
        if cut < len(entries):  # another alias in its list: its own models before it, then that alias
            back = ((alias.name, start + cut + 1),) if cut + 1 < len(entries) else ()
            hop = Hop(entries[cut][0], 0, back + then)
        elif then:  # forwarded here: after its own list, back to the alias it came from
            hop = Hop(then[0][0], then[0][1], then[1:])
        own_after = len(entries) > 1  # models of its own after the first, an alias among them
        entries = entries[:cut]
        edit_outcome: edits_mod.Outcome | None = None  # what this alias's (and the client's) prompt edits did
        done_edits = edited

        async def forward(onward: Hop, from_id: str | None, spot: list | None) -> Response:
            """This request served as one for another alias (``onward``), its whole way, from what the app sent
            (with the prompt edits made here: that alias makes only its own)."""
            came = (*via, alias.name) if alias else via
            if len(came) > MAX_FORWARDS:
                stuck = f"model '{came[0]}' was forwarded through {len(came)} aliases: do they loop?"
                return refused(error_response(stuck, 508, "alias_loop"))
            changed = json.loads(original_body)
            changed["model"] = onward.alias
            if edit_outcome is not None and isinstance(changed.get("messages"), list):
                edit_outcome.replay(changed["messages"])
            return await serve(
                request,
                client,
                path,
                json.dumps(changed).encode(),
                headers,
                query,
                allow_defer=False,  # served again later it would lose its way; a summary is written meanwhile
                allow_council=onward.start == 0 and not internal,  # its council; not again when coming back
                internal=internal,
                via=came,
                then=onward.then,
                start=onward.start,
                parent=from_id,
                place=spot,
                edited=done_edits,
            )

        if not entries:  # its model is another alias, or nothing of its own is left: straight on
            if hop is not None:
                return await forward(hop, parent, place)
            return refused(error_response(f"alias '{asked}' has no model", 404, "model_not_found"))
        first, first_pin = split_pin(entries[0][0] or None)
        checked = cfg().unknown_model_check and len(entries) == 1 and hop is None and not first_pin
        if checked and (missing := unknown_model(first, alias, internal, entries[0][1])) is not None:
            return refused(missing)  # with fallbacks it moves on; a pinned upstream decides
        # its conversation as the app started it: prompt edits (below) don't make it another one
        session = session_key(request_headers, client.name, payload)
        request_id = f"qr-{uuid.uuid4().hex[:12]}"
        # Prompt edits (app/edits.py): the alias's, then the group's and the client's, on the app's requests (not on
        # QueueRouter's own: council members, judges, summaries, checks). Every model of the alias gets the result.
        if not internal and isinstance(payload, dict) and isinstance(payload.get("messages"), list):
            if chosen := chosen_edits(alias, client, edited):
                record = client.policy.logs_content(time.time())  # the texts only with content logging on
                asker = edit_asker(client, request_headers, request_id)
                edit_outcome = await edit_engine.apply(payload["messages"], chosen, asker, record)
                done_edits = edited | {e.id for e in chosen}
                if edit_outcome.refusal:
                    return refused(error_response(edit_outcome.refusal, 502, "prompt_edit_failed"))
        prompt_changed = edit_outcome is not None and bool(edit_outcome.finals or edit_outcome.inserted)
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
                onward = {"via": via, "then": then, "parent": parent, "place": place}  # when forwarded here
                return await run_council(
                    request,
                    client,
                    alias,
                    compacted,
                    request_headers,
                    lane,
                    tags,
                    path,
                    body,
                    query,
                    payload,
                    onward,
                    (request_id, edit_outcome),
                )
        if alias and own_after:  # when it moves on: its own ticks while it has more of its own
            triggers = frozenset(alias.fallback_on)
        elif hop is not None and (outer := snapshot.aliases.get(hop.alias)) is not None:
            triggers = frozenset(outer.fallback_on)  # only its model, then back: the alias it came from decides
        else:
            triggers = frozenset()
        more = len(entries) > 1 or hop is not None  # something to move on to
        check = None  # the alias's answer check: chat completions from apps (not QueueRouter's own requests)
        if alias and alias.check_answers and more and alias.check_instructions.strip():
            if not internal and path == "chat/completions" and isinstance(payload, dict):
                check = Check(alias.check_model or alias.target_model, alias.check_instructions, client)
        param_changes = []
        strip_usage = False  # usage asked for here, not by the app: its chunk is left out of the app's stream
        if isinstance(payload, dict):
            options = payload.get("stream_options")
            wants = isinstance(options, dict) and options.get("include_usage") is True
            if payload.get("stream") is True and path in ("chat/completions", "completions") and not wants:
                payload["stream_options"] = {**(options if isinstance(options, dict) else {}), "include_usage": True}
                strip_usage = True  # without it most providers send no token counts for a stream
            alias_rules = rules_of(alias.params if alias else "")
            client_rules = rules_of(client.policy.params)
            if alias_rules or client_rules:
                param_changes = apply_rules(payload, alias_rules, client_rules)
        steps: list[Step] = []
        too_long_for: tuple | None = None
        for i, (named, ticked) in enumerate(entries):
            step_model, pin = split_pin(named or None)  # "model@upstream": only that upstream ("": none named)
            only = pin or ticked
            # models hidden on an upstream: for aliases and QueueRouter's own calls, not a client's own model@upstream
            step_candidates = candidates_for(step_model, only, raw=alias is not None or internal)
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
            fit = cfg().context_check  # an answer size the model can't give is lowered, not refused (fit_output)
            most = model_output(step_model, step_candidates) if fit else None
            lowered = fit and fit_output(step_payload, real_context, stats.ratio(step_model), most)
            compaction = None
            later = i < len(entries) - 1 or hop is not None  # a model, or an alias, after this one
            bigger_next = "context" in triggers and later  # a fallback loses nothing: prefer it
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
                    lowered = fit and fit_output(step_payload, real_context, stats.ratio(step_model), most) or lowered
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
            unchanged = i == 0 and not alias and not param_changes and not pin and not strip_usage
            unchanged = unchanged and compaction is None and not lowered and not prompt_changed
            step_body = body if unchanged else json.dumps(step_payload).encode()
            estimate = estimate_prompt_tokens(step_payload)
            steps.append(Step(step_model, step_candidates, step_body, compaction, estimate))
        if not steps:
            if hop is not None and ("context" in triggers or not too_long_for):  # none of its own can take it
                return await forward(hop, parent, place)
            if too_long_for:
                estimate, context, _, output = too_long_for
                return refused(
                    Response(overflow_body(asked or "", context, estimate + output), 400, media_type="application/json")
                )
            return refused(error_response(f"no usable upstream for '{asked}' right now", 503, "no_upstream"))
        if path == "embeddings":  # their own slots where the provider limits them separately
            steps = [dataclasses.replace(s, candidates=embedding_slots(s.candidates)) for s in steps]
        candidates, body = steps[0].candidates, steps[0].body
        if steps[0].model != target_model:
            target_model, alias_used = steps[0].model, True
        else:
            alias_used = bool(alias)

        lane = request_lane(request_headers, client)
        auto_tags = "opencode" if "x-opencode-session-id" in request_headers else None  # any opencode version
        tags = parse_tags(client.policy.tags, request_headers.get("x-queuerouter-tags"), auto_tags)
        rkey = run_key(request_headers, client.name, session)
        run = runs.get(rkey) if rkey else None
        if run is not None and not via:  # a forwarded request was counted where the app's arrived
            policy = client.policy
            max_tokens = policy.run_max_tokens
            header_budget = request_headers.get("x-queuerouter-run-budget-tokens", "")
            if header_budget.isdecimal():  # a client may lower its own budget, never raise it
                max_tokens = min(int(header_budget), max_tokens) if max_tokens else int(header_budget)
            if reason := runs.over_budget(run, max_tokens, policy.run_max_requests, policy.run_max_minutes):
                log.warning(json.dumps({"event": "run_budget_exceeded", "client": client.name, "reason": reason}))
                return refused(error_response(f"Run budget exceeded: {reason}.", 429, "run_budget_exceeded"))
            repeats = runs.note_request(run, request_fingerprint(payload))
            if policy.loop_threshold and repeats >= policy.loop_threshold:
                log.warning(
                    json.dumps({"event": "loop_detected", "client": client.name, "repeats": repeats, "run": rkey})
                )
                if policy.loop_action == "reject":
                    return refused(
                        error_response(
                            f"The same request was sent {repeats} times in 5 minutes; this looks like a loop.",
                            429,
                            "loop_detected",
                        )
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
            parent=parent or (request_headers.get("x-queuerouter-parent") or "")[:40] or None,
            priority=LANE_NAMES[lane],
            session=session and session[:8],
            position=position,
            model=via[0] if via else model,  # forwarded: still the name the app asked for
            via=" → ".join((*via, alias.name))[:300] if via and alias else None,
            target_model=target_model if alias_used else None,
            compaction=steps[0].compaction,
            estimate=steps[0].estimate,
            tags=",".join(tags) or None,
            run=rkey[:80] if rkey else None,
            stream=isinstance(payload, dict) and payload.get("stream") is True,
            edits=edit_outcome.summary() if edit_outcome else None,
        )
        rlog.edit_detail = edit_outcome.detail if edit_outcome else None
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
            strip_usage=strip_usage,
            place=place if place is not None else [],  # forwarded: where the app's request queued
            check=check,
            hop=hop,
            forward=forward,
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
                    elif isinstance(event, Forward) and adm.forward is not None:  # the alias after its models answers
                        forwarded = await adm.forward(event.hop, adm.request_id, adm.place)
                        outcome, status = "forwarded", forwarded.status_code
                        return forwarded
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
                            prompt, completion, cached, written = usage_from_body(content)
                            rlog.fields.update(
                                prompt_tokens=prompt,
                                completion_tokens=completion,
                                cached_tokens=cached,
                                cache_write_tokens=written,
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
            rlog.fields["stopped"] = stopped_at(rlog.fields)
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

    async def passed_on(response: Response, model: str) -> AsyncIterator[bytes]:
        """Another request's answer (forwarded to another alias) as this stream's: its chunks as they come (closed
        with us, so an app going away frees its slot at once), or its error or whole answer as events."""
        if isinstance(response, StreamingResponse):
            async with contextlib.aclosing(response.body_iterator) as chunks:
                async for chunk in chunks:
                    yield chunk if isinstance(chunk, bytes) else chunk.encode()
        elif response.status_code >= 400:
            yield b"data: " + bytes(response.body).replace(b"\n", b" ") + b"\n\n"
        else:
            yield council_mod.as_stream(bytes(response.body), model)

    async def stream_flow(adm: Admission) -> AsyncIterator[bytes]:
        rlog = adm.rlog
        inflight[adm.request_id] = rlog  # only once the stream is read: an unread one never shows
        outcome, status = "error", 200
        response = None
        sniffer = UsageSniffer()
        held_back = UsageChunkFilter() if adm.strip_usage else None
        captured = bytearray() if adm.log_request is not None else None
        try:
            async with contextlib.aclosing(chained(adm)) as events:
                async for event in events:
                    if isinstance(event, FellBack):
                        yield f": {event.from_model} {one_line(event.reason)}; trying {event.to_model}\n\n".encode()
                    elif isinstance(event, Checking):  # the answer is held back: show the stream is alive
                        yield f": {one_line(event.note)}\n\n".encode() if event.note else KEEPALIVE
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
                    elif isinstance(event, Forward) and adm.forward is not None:  # the alias after its models answers
                        outcome = "forwarded"
                        forwarded = await adm.forward(event.hop, adm.request_id, adm.place)
                        status = forwarded.status_code
                        async for chunk in passed_on(forwarded, event.hop.alias):
                            yield chunk
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
                        # a provider can be silent for minutes (a long prompt, a model thinking in a way that isn't
                        # passed on): keep-alives between events keep apps and proxies from giving up on the stream
                        sse = response.headers.get("content-type", "").startswith("text/event-stream")
                        between = True  # what the app got so far ends with a whole event
                        idle = keepalive_s() if sse else None
                        unfed = None  # an answer held back for its check was timed as it came: not again now
                        if (timed := getattr(response, "sniffer", None)) is not None:
                            sniffer, unfed = timed, response.prefix
                        try:
                            async with contextlib.aclosing(
                                within(response.aiter_bytes(), adm.deadline, idle)
                            ) as chunks:
                                async for chunk in chunks:
                                    if not chunk:
                                        if between:
                                            yield KEEPALIVE
                                        continue
                                    if chunk is unfed:
                                        unfed = None
                                    else:
                                        sniffer.feed(chunk, time.monotonic())
                                    if rlog.fields["ttft_ms"] is None:
                                        rlog.fields["ttft_ms"] = round((time.monotonic() - rlog.started) * 1000)
                                    if captured is not None and len(captured) < CONTENT_CAPTURE_MAX:
                                        captured += chunk
                                    if held_back is not None:
                                        chunk = held_back.feed(chunk)
                                    if chunk:
                                        between = chunk.endswith((b"\n\n", b"\r\n\r\n"))
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
                        except TimeoutError:  # the stream stalled past the key's total time limit
                            outcome, status = "timeout", 504
                            yield MID_STREAM + sse_error(
                                "request exceeded this key's total time limit", 504, "request_timeout"
                            )
                            return
                        if held_back is not None and (rest := held_back.flush()):
                            yield rest
                        outcome = "done"
                        return
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected: Starlette cancels us, or the generator is closed.
            outcome = "cancelled"
            rlog.fields["stopped"] = stopped_at(rlog.fields, sniffer.finish_reason is not None)
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
                cache_write_tokens=sniffer.cache_write_tokens,
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
    """A /v1/models entry for an alias, with the target's context-size fields so apps size prompts right; not
    the target itself."""
    entry = {"id": alias.name, "object": "model", "created": 0, "owned_by": "queuerouter"}
    for key in CONTEXT_FIELDS:
        if target and key in target:
            entry[key] = target[key]
    if alias.virtual_context:  # compaction makes conversations up to this size fit the real model
        for key in [k for k in CONTEXT_FIELDS if k in entry]:
            del entry[key]
        entry["context_length"] = alias.virtual_context
    entry["queuerouter"] = {"type": "alias"}
    return entry


def own_entry(public: str, entry: dict, thinking: thinking_mod.Thinking | None, images: bool) -> dict:
    """QueueRouter's /v1/models entry for a model with a custom name: its sizes, thinking levels and whether it
    reads images, in OpenRouter's well-known form, and nothing else of the provider's (its owner, its own names)."""
    created = entry.get("created")
    own = {
        "id": public,
        "object": "model",
        "created": created if isinstance(created, int) else 0,
        "owned_by": "queuerouter",
    }
    own.update({key: entry[key] for key in CONTEXT_FIELDS if key in entry})
    if thinking is not None:
        reasoning = {"supported_efforts": list(dict.fromkeys(value for _, value in thinking.variants))}
        if thinking.default:
            reasoning["default_effort"] = thinking.default
        own["reasoning"] = reasoning
    own["architecture"] = {"input_modalities": ["text", "image"] if images else ["text"]}
    own["queuerouter"] = {"type": "model"}
    return own


async def within(
    chunks: AsyncIterator[bytes], deadline: float | None, idle: float | None = None
) -> AsyncIterator[bytes]:
    """``chunks`` until the monotonic ``deadline`` (None: no limit); TimeoutError once it passes, also while a
    stalled provider sends nothing (checking only between chunks would wait for its read timeout). With ``idle``,
    an empty chunk whenever nothing came for that long: the caller's cue for a keep-alive. The read in progress
    goes on meanwhile."""
    iterator = chunks.__aiter__()
    pending: asyncio.Future | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(anext(iterator, None))
            limits = [idle] if idle else []
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                limits.append(remaining)
            done, _ = await asyncio.wait({pending}, timeout=min(limits) if limits else None)
            if not done:
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError
                yield b""
                continue
            chunk, pending = pending.result(), None
            if chunk is None:
                return
            yield chunk
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
        elif pending is not None and not pending.cancelled():
            pending.exception()  # seen, so it isn't reported as never retrieved


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


def requested_size(body: bytes) -> int:
    """The answer size a JSON request body asks for (0: none)."""
    try:
        payload = json.loads(body) if body else None
    except ValueError:
        return 0
    return requested_output(payload) if isinstance(payload, dict) else 0


def lowered_output(body: bytes, most: int) -> tuple[bytes, int] | None:
    """``body`` asking for an answer of at most ``most`` tokens, and what it asked for; None when it asked no more."""
    try:
        payload = json.loads(body) if body else None
    except ValueError:
        return None
    if not isinstance(payload, dict) or (asked := requested_output(payload)) <= most:
        return None
    for key in ("max_completion_tokens", "max_tokens"):
        if isinstance(payload.get(key), int) and payload[key] > most:
            payload[key] = most
    return json.dumps(payload).encode(), asked


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
