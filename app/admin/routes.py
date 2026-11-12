"""Admin web UI: server-rendered pages, no build step, no third-party JS/CSS."""

from __future__ import annotations

import asyncio
import csv
import dataclasses
import io
import json
import logging
import math
import os
import re
import secrets
import sqlite3
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import quote, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import __version__, latency, totp
from .. import cron as cron_mod
from .. import jobs as jobs_mod
from .. import taggers as taggers_mod
from ..agents import parse_tags
from ..alerts import LEVELS, SPECS, WEBHOOK_KINDS, Alerts, Note
from ..answer_rules import (
    ANSWER_KIND_LABELS,
    ANSWER_KINDS,
    CHECK_WHEN,
    CHECK_WHEN_LABELS,
    DEFAULT_ANSWER_KINDS,
    LEARN_LABELS,
    LEARN_MODES,
    RULES_PER_ALIAS,
    SPOT_DEFAULT,
)
from ..auth import AuthGuard, client_ip, how_long, ignored_proxy
from ..backup import MAX_UPLOAD_BYTES, RestoreError, backup_path, list_backups, make_archive, stage_restore
from ..catalog import make_filter, parse_filter
from ..compaction import MODES as COMPACT_MODES
from ..config import (
    GROUP_LIMITS,
    ON_LIMIT,
    PRIORITIES,
    SECRET_TUNABLES,
    TUNABLES,
    Bootstrap,
    ClientPolicy,
    Settings,
)
from ..council import MAX_MEMBERS as COUNCIL_MAX
from ..council import MIN_MEMBERS as COUNCIL_MIN
from ..council import MODES as COUNCIL_MODES
from ..council import WHEN as COUNCIL_WHEN
from ..fallback import DEFAULT_TRIGGERS, TRIGGERS, loop_of
from ..names import EVERY, ModelNames
from ..opencode import plugin_install_commands
from ..params import parse_rules
from ..presets import PRESETS, all_plans, plan_choice
from ..pricing import (
    DEFAULT_MIX,
    MIX_DAYS,
    PriceCatalog,
    alias_costs,
    model_costs,
    request_costs,
    resolve,
    summarize_usage,
    token_mix,
    usage_by,
)
from ..ranking import CREDIT, CREDIT_URL, Scores
from ..ranking import INDEXES as RANK_INDEXES
from ..slots import STRATEGIES
from ..state import MODES
from ..store import ROLES, Store
from ..taggers import TaggerError
from ..thinking import EFFORTS as THINKING_EFFORTS
from ..upstream import UA_MODES
from ..websearch import PROVIDERS as SEARCH_PROVIDERS
from . import edit_routes, periods, rule_routes, tagger_routes
from .charts import lines, usage_charts
from .security import (
    AdminSession,
    Sessions,
    admin_ip_allowed,
    csrf_ok,
    hash_password,
    is_https,
    password_problem,
    verify_password,
)

log = logging.getLogger("queuerouter")
TWO_FA_COOKIE, TWO_FA_TTL_S = "qr_2fa", 300
ENABLED_COMPACT_MODES = ("off", "trim", "summarise")
ALIAS_ERRORS = {  # what was wrong in the alias editor
    "name": "An alias name uses letters, digits and . _ : / - (up to 128 characters, starting with a letter or digit).",
    "label": "A display name is up to 64 characters, without |.",
    "target_model": "Choose the model this alias sends to (not the alias itself).",
    "fallbacks": "Up to 10 fallback models.",
    "compact": "Unknown compaction mode.",
    "compact_model": "The summary model name is too long.",
    "virtual_context": "Context told to apps: from 1,024 to 100,000,000 tokens, or blank.",
    "council": "Unknown council setting.",
    "council_members": "A council needs 1 to 64 members.",
    "check": "Taggers need at least one fallback model (a failed answer goes to the next model).",
    "answer_rules": f"Answer rules need at least one fallback model (a failed answer goes to the next model), and "
    f"an alias takes up to {RULES_PER_ALIAS} of them.",
    "check_kinds": "Tick at least one answer type for the check to read.",
    "check_when": "Choose when the taggers run.",
    "spot_checks": "Spot checks: 0 to 100%.",
    "learn_rules": "Rules from the model need taggers: the rule maker learns from what they decide.",
    "council_search_max": "Searches per member: 1 to 10.",
    "council_search_results": "Results per search: 1 to 10.",
    "orch_rounds": "Orchestrator rounds: 1 to 30.",
    "orch_tasks": "Orchestrator member tasks: 1 to 64.",
    "orch_seconds": "Orchestrator time: 30 to 3,600 seconds.",
}
PRICING_DAYS = 30  # the price table lists models used this recently (plus offered and manually priced ones)
LIVE_CHECK_S, LIVE_HEARTBEAT_S = 0.25, 5  # dashboard stream: look for changes / send anyway
REQUEST_LIST_COLUMNS = (
    "id, ts, client, priority, model, target_model, upstream, status, outcome, retries, queue_ms, ttft_ms, "
    "upstream_ms, prompt_tokens, completion_tokens, cached_tokens, cache_write_tokens, finish_reason, stopped, "
    "tokens_per_s, compaction, council, role, parent, answer_check, answer_tags, via, edits IS NOT NULL AS edited"
)
REQUEST_DETAILS = (  # what opens under a row; the list above stays small
    "id",
    "path",
    "model",
    "target_model",
    "upstream",
    "priority",
    "stream",
    "user_agent",
    "sent_user_agent",
    "tags",
    "error",
    "retries",
    "total_ms",
    "headers_ms",
    "first_token_ms",
    "answer_ms",
    "tokens_per_s",
    "stall_ms",
    "backoff_ms",
    "compaction",
    "council",
    "role",
    "parent",
    "fallback",
    "answer_check",
    "answer_tags",
    "thinking",
    "via",
    "edits",
)
HERE = os.path.dirname(__file__)

# Short, fixed flash messages (passed by code in the redirect URL, never as free text).
MESSAGES = {
    "saved": "Saved.",
    "created": "Created.",
    "deleted": "Deleted.",
    "password": "Password changed. Other sessions were signed out.",
    "welcome": "Setup complete. Welcome!",
    "loggedout": "Signed out.",
    "last_owner": "There must always be at least one enabled owner.",
    "self": "You can't do that to your own account here.",
    "exists": "That name is already taken.",
    "invalid": "Some values were invalid; nothing was changed.",
    "pool_in_use": "That pool is still used by upstreams; move or delete them first.",
    "rules_invalid": "Parameter rules must be a JSON object with defaults / overrides / clamps / strip "
    "(see the example).",
    "client_unavailable": "That client is disabled or expired; enable it first.",
    "refresh_failed": "Couldn't download prices from models.dev right now; the previous list is kept.",
    "scores_failed": "Couldn't read the Artificial Analysis index right now (the reason is on the Models page); "
    "the previous scores are kept.",
    "webhook_ok": "Test notification delivered.",
    "webhook_failed": "The test notification could not be delivered; see the error on the webhook.",
    "read": "All notifications marked as read.",
    "2fa_off": "Two-factor sign-in is off.",
    "backup_missing": "That backup file doesn't exist.",
    "content_purged": "Logged prompts and replies were deleted.",
    "schedule_invalid": "Check the schedule: a name, a valid cron expression (e.g. 0 2 * * *) and a JSON request body.",
    "content_missing": "No logged content for that request (logging was off, or it expired).",
    "2fa_reset": "Two-factor sign-in was reset for that user; they sign in with their password "
    "and can set it up again.",
    "unblocked": "Unblocked: requests from that address are let in again.",
    "not_blocked": "That address isn't blocked any more.",
    "group_in_use": "Clients are still in that group; move them to another group first.",
    "group_last": "Every client is in a group, so the last group can't be deleted.",
    "group_models": "Pick at least one model or alias, or tick Every model.",
    "edits": f"Up to {edit_routes.EDITS_PER_OWNER} prompt edits.",
    **edit_routes.MESSAGES,
    **rule_routes.MESSAGES,
    **tagger_routes.MESSAGES,
}

MAX_USERNAME = 100
SETTING_CHOICES: dict[str, tuple[str, ...]] = {
    "lb_strategy": STRATEGIES,
    "search_provider": SEARCH_PROVIDERS,
    "search_backup": SEARCH_PROVIDERS,
    "rank_index": tuple(RANK_INDEXES),
}

SETTING_HELP: dict[str, tuple[str, str, str]] = {
    # name: (section, label, help)
    "aging_s": ("Scheduling", "Aging (s)", "Waiting this long bumps a request one priority level up."),
    "session_aware": ("Scheduling", "Finish what you started", "Sessions already mid-task go before new ones."),
    "session_window_s": ("Scheduling", "Session window (s)", "How long after a request a session counts as mid-task."),
    "fair_share": ("Scheduling", "Fair share", "Among equals, the client served least recently goes first."),
    "max_queue_wait_s": ("Queue", "Max queue wait (s)", "Give up after waiting this long (504). 0 = wait forever."),
    "keepalive_interval_s": (
        "Queue",
        "Keep-alive interval (s)",
        "SSE comments sent to streaming clients while queued, or while the provider sends nothing.",
    ),
    "max_retries": ("Upstream", "Max retries", "On 429/5xx/connection errors. -1 = forever, 0 = none."),
    "retry_base_delay_s": ("Upstream", "Retry base delay (s)", "Exponential backoff start (Retry-After wins)."),
    "retry_max_delay_s": ("Upstream", "Retry max delay (s)", "Backoff cap."),
    "upstream_connect_timeout_s": ("Upstream", "Connect timeout (s)", ""),
    "upstream_read_timeout_s": ("Upstream", "Read timeout (s)", "Max silence between upstream bytes."),
    "models_cache_s": ("Upstream", "Models cache (s)", "How long /v1/models is cached."),
    "request_log_days": (
        "Usage",
        "Keep request log (days)",
        "Per-request metadata rows; 0 = keep none. Daily totals are kept forever.",
    ),
    "max_jobs_in_flight": (
        "Jobs",
        "Jobs at once",
        "Async jobs and batch requests running at the same time; live requests keep the other slots.",
    ),
    "job_retention_days": (
        "Jobs",
        "Keep finished jobs (days)",
        "Finished jobs, batches and their result files are deleted after this.",
    ),
    "content_log_days": (
        "Usage",
        "Keep logged content (days)",
        "Prompts and replies of clients with content logging on (Clients page).",
    ),
    "content_log_max_kb": (
        "Usage",
        "Logged content size limit (KB)",
        "Per request and per reply; longer ones are cut.",
    ),
    "unknown_model_check": (
        "Upstream",
        "Refuse unknown models",
        "Answer 404 (with the closest names) for a model no upstream lists, instead of sending it on. "
        "Turn off for providers whose model list is incomplete.",
    ),
    "compaction_idle_update": (
        "Compaction",
        "Update summaries while idle",
        "For aliases in summarise mode: when a conversation near its model's limit has been quiet for about a "
        "minute, extend its summary in the background (low priority) so the next message doesn't wait.",
    ),
    "compaction_idle_minutes": (
        "Compaction",
        "Forget summaries after (minutes)",
        "Summaries and trim plans are kept in memory and dropped after this long unused.",
    ),
    "compaction_saved": (
        "Compaction",
        "Save summaries",
        "Also keep summaries and orchestrator notebooks in the database (encrypted), so they survive restarts. "
        "They contain your "
        "conversations; they are deleted with the client and after the days below.",
    ),
    "compaction_keep_days": (
        "Compaction",
        "Keep saved summaries (days)",
        "Saved summaries unused this long are deleted.",
    ),
    "search_provider": (
        "Web search",
        "Provider",
        "Lets council members search the web before the judge decides (per alias: Council > members can search), "
        "and runs Claude Code's web search. SearXNG runs on your own server, so queries stay there. Synthetic is "
        "its search API, included in its subscriptions with an hourly quota of its own.",
    ),
    "search_key": (
        "Web search",
        "API key",
        "Tavily, Brave or Synthetic key (stored encrypted; leave blank to keep the current one). Each search uses "
        "credits or quota. For Synthetic, none set here means the key of your Synthetic upstream.",
    ),
    "search_url": (
        "Web search",
        "SearXNG address",
        "Only for SearXNG (as provider or backup), e.g. http://searxng:8080 (its JSON output must be enabled).",
    ),
    "search_backup": (
        "Web search",
        "Backup provider",
        "Asked when the provider's search fails: its limit is reached, its key is refused, it's down. The provider "
        "is then left out for a while (until its quota renews, where it says when; else 10 minutes after a limit, "
        "1 minute after another failure), so searches don't wait for it each time. A search that finds nothing "
        "isn't a failure.",
    ),
    "search_backup_key": (
        "Web search",
        "Backup's API key",
        "The backup's own key, if it needs one (SearXNG doesn't; Synthetic can use your Synthetic upstream's).",
    ),
    "opencode_plugin_updates": (
        "opencode",
        "Update the plugin automatically",
        "Installed QueueRouter plugins (0.41 and later) replace their file with this server's when it changes; "
        "the desktop app reloads it (else opencode uses it after a restart). Off: re-install by hand. A machine "
        "can opt out with QUEUEROUTER_AUTO_UPDATE=0. Only over https (or on the same machine): plugins that "
        "reach QueueRouter over plain http don't update themselves (0.44 and later).",
    ),
    "opencode_rank": (
        "opencode",
        "Order models by strength",
        "Numbers the opencode model list: aliases first (00), then the strongest models (01, 02…), then the rest. "
        "Scores come from the index below, or from your own scores on the Models page.",
    ),
    "opencode_price": (
        "opencode",
        "Show prices in model names",
        "Adds one price per million tokens to each model's name in opencode, from its prices and your token mix of "
        "the last 30 days (or from the day set on the Models page): 04|32.5|$0.200|model|Provider. A monthly plan "
        "without a price of its own shows plan, a model without a known price $?. Prices are set on the Models page.",
    ),
    "rank_index": (
        "opencode",
        "Strength index",
        "Which Artificial Analysis index ranks the models: intelligence (overall), coding or agentic.",
    ),
    "rank_key": (
        "opencode",
        "Artificial Analysis API key",
        "Free key from artificialanalysis.ai (stored encrypted; blank keeps the current one). The index is "
        "read once a day.",
    ),
    "passthrough_get_paths": (
        "Upstream",
        "GET paths passed to the provider",
        "Other /v1/ GETs are refused. Paths listed here (comma-separated, e.g. credits) go to the first usable "
        "upstream with its key, so every client can read what they return.",
    ),
    "structured_repair": (
        "Upstream",
        "Repair broken JSON",
        "When a JSON-mode reply or tool-call arguments aren't valid JSON, fix common breakage "
        "(code fences, extra text, trailing commas, unclosed brackets).",
    ),
    "metrics_token": (
        "Monitoring",
        "Prometheus token",
        "Enables /metrics; Prometheus sends it as a bearer token (authorization.credentials).",
    ),
    "otel_endpoint": (
        "Monitoring",
        "OpenTelemetry collector URL",
        "OTLP/HTTP base URL, e.g. http://otel-collector:4318 — traces go to /v1/traces. Empty = off.",
    ),
    "otel_headers": (
        "Monitoring",
        "Collector headers",
        "Optional, e.g. authorization=Bearer abc,x-tenant=me (stored encrypted).",
    ),
    "otel_service_name": ("Monitoring", "Service name", "service.name on every trace."),
    "backup_keep_days": (
        "Backup",
        "Daily backups to keep",
        "A copy of the database is made in /data/backups every day; 0 = off. See the Backup page.",
    ),
    "lb_strategy": (
        "Upstreams",
        "Load balancing",
        "least-busy: most free slots · weighted: random by weight · ordered: first healthy upstream (failover).",
    ),
    "circuit_threshold": (
        "Upstreams",
        "Circuit breaker threshold",
        "Consecutive failures (5xx, connection errors) before an upstream is taken out of rotation. "
        "0 = never: requests keep going to it (and retry).",
    ),
    "circuit_cooldown_s": ("Upstreams", "Circuit cooldown (s)", "Then one trial request decides if it's back."),
    "sticky_routing": (
        "Upstreams",
        "Sticky routing",
        "Send a conversation back to the upstream that served it (warm prompt cache).",
    ),
    "affinity_wait_s": (
        "Upstreams",
        "Sticky wait (s)",
        "How long to wait for that upstream when it's busy before using another.",
    ),
    "affinity_ttl_s": ("Upstreams", "Sticky memory (s)", "Forget a conversation's upstream after this long."),
    "context_check": (
        "Queue",
        "Context-length check",
        "Refuse prompts that clearly exceed the model's context before they wait.",
    ),
    "price_catalog_enabled": ("Usage", "Official price catalog", "Download reference prices from models.dev daily."),
    "allow_no_auth": ("Security", "Allow without key", "Only when no clients exist. Private networks only!"),
    "max_body_mb": ("Security", "Max request body (MB)", ""),
    "auth_fail_limit": (
        "Security",
        "Auth failures per minute",
        "Before an IP is temporarily blocked. Blocked addresses are on the Clients page.",
    ),
    "auth_block_s": ("Security", "Block duration (s)", ""),
}


@dataclass
class AdminContext:
    store: Store
    bootstrap: Bootstrap
    reload: Callable[[], None]
    live_state: Callable[..., dict]
    probe: Callable[[str, str], Awaitable[tuple[bool, str]]]
    upstream_info: Callable[[], list[dict]]
    models_by_upstream: Callable[[], dict[str, list[str]]]
    prices: PriceCatalog
    refresh_prices: Callable[[], Awaitable[bool]]
    playground: Callable[[Request, str, bytes], Awaitable[Response]]
    opencode_config: Callable[[Request, str], Awaitable[dict | None]]
    alerts: Alerts
    restart: Callable[[], None]
    month_usage: Callable[[str], object]
    live_signature: Callable[[], tuple]
    live_brief: Callable[[], dict]
    context_table: Callable[[], list[dict]]
    upstream_models: Callable[[int], Awaitable[list[str] | None]]
    model_choices: Callable[[], list[dict]]
    scores: Scores
    refresh_scores: Callable[[], Awaitable[bool]]
    ranking_table: Callable[[], dict]
    thinking_table: Callable[[], dict]
    name_table: Callable[[], list[dict]]
    names: Callable[[], ModelNames]
    model_label: Callable[..., str]
    public_base: Callable[[Request], str]
    guard: AuthGuard  # API keys: the addresses blocked for failed ones


class NeedLogin(Exception):
    pass


def mount_admin(app: FastAPI, ctx: AdminContext) -> None:
    store, bootstrap = ctx.store, ctx.bootstrap
    sessions = Sessions(store, bootstrap)
    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    templates.env.filters["expiry_date"] = _expiry_date
    templates.env.filters["model_label"] = ctx.model_label
    templates.env.filters["model_name"] = lambda model, upstream=None: ctx.names().name(model, upstream)
    templates.env.filters["request_label"] = lambda r: ctx.names().of_request(
        r["model"], r["target_model"], r["upstream"]
    )
    login_guard = AuthGuard(limit_per_minute=5, block_s=300, what="admin sign-in")
    pending_2fa: dict[str, tuple[int, float, str]] = {}  # ticket -> (user id, expires, next)
    pending_setup: dict[str, tuple[str, float]] = {}  # session -> (new TOTP secret, expires)
    setup = {"token": None}

    # -- first admin ---------------------------------------------------------------------

    if store.count_admins() == 0:
        if bootstrap.admin_username and bootstrap.admin_password:
            if problem := password_problem(bootstrap.admin_password):
                raise ValueError(f"ADMIN_PASSWORD: {problem}")
            store.create_admin(bootstrap.admin_username, hash_password(bootstrap.admin_password), "owner", "system")
            log.info(json.dumps({"event": "admin_created_from_env", "username": bootstrap.admin_username}))
        else:
            setup["token"] = secrets.token_urlsafe(18)
            where = f"{bootstrap.public_url}/admin/setup" if bootstrap.public_url else "/admin/setup"
            log.warning(json.dumps({"event": "setup_required", "url": where, "setup_token": setup["token"]}))

    app.state.admin_setup = setup  # exposed for tests and diagnostics
    app.mount("/admin/static", StaticFiles(directory=os.path.join(HERE, "static")), name="admin-static")

    # -- plumbing ------------------------------------------------------------------------

    app.add_middleware(AdminGuard, bootstrap=bootstrap)

    @app.exception_handler(NeedLogin)
    async def need_login(request: Request, exc: NeedLogin):
        return RedirectResponse(f"/admin/login?next={quote(request.url.path)}", status_code=303)

    def page(request: Request, template: str, session: AdminSession | None, **context) -> HTMLResponse:
        flash = MESSAGES.get(request.query_params.get("m", ""))
        error = MESSAGES.get(request.query_params.get("e", ""))
        return templates.TemplateResponse(
            request,
            template,
            {
                "user": session.user if session else None,
                "csrf": session.csrf if session else "",
                "version": __version__,
                "flash": flash,
                "error": context.pop("error", None) or error,
                "path": request.url.path,
                "alerts_unread": store.unread_notifications() if session else 0,
                **context,
            },
        )

    def back(path: str, message: str | None = None, error: str | None = None) -> RedirectResponse:
        path, _, fragment = path.partition("#")
        query = f"?m={message}" if message else f"?e={error}" if error else ""
        return RedirectResponse(path + query + (f"#{fragment}" if fragment else ""), status_code=303)

    def require(request: Request, role: str) -> AdminSession:
        session = sessions.current(request)
        if session is None:
            raise NeedLogin()
        if not session.user.can(role):
            raise NotAllowed(role)
        return session

    async def require_post(request: Request, role: str) -> tuple[AdminSession, dict]:
        session = require(request, role)
        form = dict(await request.form())
        if not csrf_ok(session, form.get("csrf")):
            raise NotAllowed("csrf")
        return session, form

    @app.exception_handler(OverflowError)
    async def out_of_range(request: Request, exc: OverflowError):
        """A number too big to store (a 400-digit limit, the day after 31 Dec 9999): the form's mistake, not a 500."""
        if not request.url.path.startswith("/admin"):
            message = {"error": {"message": "a number is out of range", "type": "invalid_request_error"}}
            return JSONResponse(message, status_code=400)
        reason = "A number or date in the form is out of range; nothing was saved."
        response = page(request, "error.html", sessions.current(request), title="Not saved", reason=reason)
        response.status_code = 400
        return response

    @app.exception_handler(NotAllowed)
    async def forbidden(request: Request, exc: NotAllowed):
        session = sessions.current(request)
        reason = (
            "Your session's form token is invalid; reload the page."
            if str(exc) == "csrf"
            else "This needs the 'can read logged content' permission (Users page)."
            if str(exc) == "content"
            else f"This page needs the '{exc}' role."
        )
        response = page(request, "error.html", session, title="Not allowed", reason=reason)
        response.status_code = 403
        return response

    # -- setup / login / logout ----------------------------------------------------------

    @app.get("/admin/setup")
    async def setup_form(request: Request):
        if setup["token"] is None:
            return RedirectResponse("/admin/login", status_code=303)
        return page(request, "setup.html", None)

    @app.post("/admin/setup")
    async def setup_submit(request: Request):
        if setup["token"] is None:
            return RedirectResponse("/admin/login", status_code=303)
        ip = client_ip(request, bootstrap.trusted_proxies)
        if login_guard.is_blocked(ip):
            return page(request, "setup.html", None, error="Too many attempts. Try again later.")
        form = dict(await request.form())
        if not secrets.compare_digest(str(form.get("token", "")).encode(), setup["token"].encode()):
            login_guard.fail(ip)
            return page(request, "setup.html", None, error="Wrong setup token (see the container log).")
        username = str(form.get("username", "")).strip()
        if not username or len(username) > MAX_USERNAME:  # sign-in reads at most MAX_USERNAME characters
            return page(request, "setup.html", None, error=f"Choose a username (up to {MAX_USERNAME} characters).")
        if problem := password_problem(str(form.get("password", "")), str(form.get("confirm", ""))):
            return page(request, "setup.html", None, error=problem, username=username)
        user_id = store.create_admin(username, hash_password(str(form["password"])), "owner", username)
        setup["token"] = None
        user = store.get_admin(user_id=user_id)
        token, _ = sessions.start(request, user)
        response = back("/admin", "welcome")
        sessions.set_cookie(response, request, token)
        return response

    @app.get("/admin/login")
    async def login_form(request: Request):
        if setup["token"] is not None:
            return RedirectResponse("/admin/setup", status_code=303)
        if sessions.current(request):
            return RedirectResponse("/admin", status_code=303)
        return page(request, "login.html", None, next=_safe_next(request.query_params.get("next")))

    @app.post("/admin/login")
    async def login_submit(request: Request):
        form = dict(await request.form())
        username = str(form.get("username", "")).strip()[:MAX_USERNAME]  # bounded: it is kept for the lockout and audit
        nxt = _safe_next(str(form.get("next", "")))
        ip = client_ip(request, bootstrap.trusted_proxies)
        # wrong passwords block that name from that address only: from anywhere, anyone who can reach this page
        # could keep an admin locked out (the address's own limit still counts every name tried from it)
        if login_guard.is_blocked(ip) or login_guard.is_blocked(f"user:{username}@{ip}"):
            return page(request, "login.html", None, error="Too many attempts. Try again later.", next=nxt)
        user = store.get_admin(username=username)
        hashed = store.password_hash(user.id) if user else None
        if not verify_password(hashed, str(form.get("password", ""))) or not user.enabled:
            login_guard.fail(ip)
            login_guard.fail(f"user:{username}@{ip}")
            store.audit(username or "?", "login.failed", ip)
            return page(request, "login.html", None, error="Wrong username or password.", next=nxt)
        if user.has_2fa:  # password was right; now the code from the authenticator app
            ticket = secrets.token_urlsafe(32)
            now = time.time()
            for stale in [k for k, v in pending_2fa.items() if v[1] < now]:
                pending_2fa.pop(stale, None)
            pending_2fa[ticket] = (user.id, now + TWO_FA_TTL_S, nxt)
            response = page(request, "login_2fa.html", None)
            response.set_cookie(
                TWO_FA_COOKIE,
                ticket,
                max_age=TWO_FA_TTL_S,
                httponly=True,
                secure=is_https(request, bootstrap),
                samesite="strict",
                path="/admin",
            )
            return response
        return finish_login(request, user, ip, nxt)

    def finish_login(request: Request, user, ip: str, nxt: str, how: str = "") -> RedirectResponse:
        token, _ = sessions.start(request, user)
        store.audit(user.username, "login", ip, how)
        response = RedirectResponse(nxt, status_code=303)
        sessions.set_cookie(response, request, token)
        response.delete_cookie(TWO_FA_COOKIE, path="/admin")
        return response

    @app.post("/admin/login/2fa")
    async def login_2fa(request: Request):
        form = dict(await request.form())
        ip = client_ip(request, bootstrap.trusted_proxies)
        ticket = request.cookies.get(TWO_FA_COOKIE, "")
        entry = pending_2fa.get(ticket)
        if entry is None or entry[1] < time.time():
            pending_2fa.pop(ticket, None)
            return page(request, "login.html", None, error="The sign-in expired; please start again.", next="/admin")
        user_id, _, nxt = entry
        user = store.get_admin(user_id=user_id)
        # here the name stays blocked from every address: only someone with the password gets this far, and
        # codes tried from many addresses must not add up
        if (
            user is None
            or not user.enabled
            or login_guard.is_blocked(ip)
            or login_guard.is_blocked(f"user:{user.username}")
        ):
            pending_2fa.pop(ticket, None)
            return page(request, "login.html", None, error="Too many attempts. Try again later.", next=nxt)
        secret, last, _ = store.totp_state(user.id)
        code = str(form.get("code", "")).strip()
        counter = totp.verify(secret, code, after=last) if secret else None
        if counter is not None:
            store.totp_used(user.id, counter)
            how = ""
        elif secret and len(code) > totp.DIGITS and store.use_recovery_code(user.id, totp.hash_recovery(code)):
            how = "recovery code"
        else:
            login_guard.fail(ip)
            login_guard.fail(f"user:{user.username}")
            store.audit(user.username, "login.failed", ip, "2fa")
            return page(request, "login_2fa.html", None, error="That code didn't work. Try the current one.")
        pending_2fa.pop(ticket, None)
        return finish_login(request, user, ip, nxt, how)

    @app.post("/admin/logout")
    async def logout(request: Request):
        session, _ = await require_post(request, "viewer")
        sessions.end(session)
        response = back("/admin/login", "loggedout")
        sessions.clear_cookie(response)
        return response

    # -- dashboard -----------------------------------------------------------------------

    @app.get("/admin")
    async def dashboard(request: Request):
        session = require(request, "viewer")
        return page(request, "dashboard.html", session, state=ctx.live_state())

    @app.get("/admin/api/stats")
    async def api_stats(request: Request):
        require(request, "viewer")
        return JSONResponse(ctx.live_state())

    @app.get("/admin/api/live")
    async def api_live(request: Request):
        """Server-sent events: the dashboard state whenever it changes (checked 4x a second), and
        every few seconds anyway. The first message has the full chart history, later ones the
        latest points only. ``?view=brief`` sends only what the Requests and Usage pages use."""
        require(request, "viewer")
        brief = request.query_params.get("view") == "brief"

        async def events():
            last_signature, last_sent, first = None, 0.0, True
            checked_session = time.monotonic()
            yield "retry: 1000\n\n"  # after a restart, browsers reconnect within a second
            while not getattr(request.app.state, "stopping", False) and not await request.is_disconnected():
                now = time.monotonic()
                if now - checked_session > 60:  # signed out or expired: stop streaming
                    checked_session = now
                    if sessions.current(request) is None:
                        return
                signature = ctx.live_signature()
                if signature != last_signature or now - last_sent >= LIVE_HEARTBEAT_S:
                    state = ctx.live_brief() if brief else ctx.live_state(full_history=first)
                    yield f"data: {json.dumps(state)}\n\n"
                    first, last_signature, last_sent = False, signature, now
                await asyncio.sleep(LIVE_CHECK_S)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # -- settings ------------------------------------------------------------------------

    @app.get("/admin/settings")
    async def settings_form(request: Request):
        session = require(request, "admin")
        values = {**{k: None for k in TUNABLES}, **store.get_tunables()}
        sections: dict[str, list] = {}
        for name, (section, label, help_text) in SETTING_HELP.items():
            sections.setdefault(section, []).append(
                {
                    "name": name,
                    "label": label,
                    "help": help_text,
                    "type": TUNABLES[name].__name__,
                    "secret": name in SECRET_TUNABLES,
                    "value": bool(values.get(name)) if name in SECRET_TUNABLES else values.get(name),
                    "choices": SETTING_CHOICES.get(name, ()),
                }
            )
        status = getattr(request.app.state, "search_status", None)  # searches left this hour, a provider left out
        search_note = await status() if status else ""
        return page(request, "settings.html", session, sections=sections, search_note=search_note)

    @app.post("/admin/settings")
    async def settings_submit(request: Request):
        session, form = await require_post(request, "admin")
        values: dict[str, object] = {}
        try:
            for name in SETTING_HELP:  # only what the page shows: an absent checkbox isn't "off"
                kind = TUNABLES[name]
                if kind is bool:
                    values[name] = form.get(name) == "on"
                elif kind is str and name in SETTING_CHOICES:
                    if name in form:
                        if form[name] not in SETTING_CHOICES[name]:
                            raise ValueError(name)
                        values[name] = form[name]
                elif kind is str:
                    text = str(form.get(name, "")).strip()[:1000]
                    if form.get(f"clear_{name}") == "on":
                        values[name] = ""
                    elif text or name not in SECRET_TUNABLES:  # a blank secret field keeps the secret
                        values[name] = text
                elif name in form and str(form[name]).strip() == "":  # emptied: back to the default
                    values[name] = getattr(Settings, name)
                elif name in form:
                    values[name] = kind(form[name])
                    if not math.isfinite(values[name]):  # "nan" and "inf" parse as floats
                        raise ValueError(name)
                    if values[name] < -1 or (name != "max_retries" and values[name] < 0):
                        raise ValueError(name)
        except ValueError:
            return back("/admin/settings", error="invalid")
        current = store.get_tunables()
        new_endpoint = values.get("otel_endpoint")
        if (
            new_endpoint is not None
            and new_endpoint != current.get("otel_endpoint", "")
            and "otel_headers" not in values
        ):
            values["otel_headers"] = ""  # the stored headers (e.g. an API key) were meant for the old collector
        store.set_tunables(values, session.user.username)
        ctx.reload()
        return back("/admin/settings", "saved")

    # -- users ---------------------------------------------------------------------------

    def enabled_owners() -> int:
        return store.count_admins(role="owner", enabled_only=True)

    @app.get("/admin/users")
    async def users(request: Request):
        session = require(request, "owner")
        return page(request, "users.html", session, users=store.list_admins(), roles=ROLES)

    @app.post("/admin/users")
    async def users_create(request: Request):
        session, form = await require_post(request, "owner")
        username = str(form.get("username", "")).strip()
        role = str(form.get("role", "viewer"))
        bad_name = not username or len(username) > MAX_USERNAME
        if bad_name or role not in ROLES or password_problem(str(form.get("password", ""))):
            return back("/admin/users", error="invalid")
        try:
            store.create_admin(username, hash_password(str(form["password"])), role, session.user.username)
        except sqlite3.IntegrityError:
            return back("/admin/users", error="exists")
        return back("/admin/users", "created")

    @app.post("/admin/users/{user_id}")
    async def users_update(user_id: int, request: Request):
        session, form = await require_post(request, "owner")
        target = store.get_admin(user_id=user_id)
        if target is None:
            return back("/admin/users", error="invalid")
        role = str(form.get("role", target.role))
        enabled = form.get("enabled") == "on"
        if role not in ROLES:
            return back("/admin/users", error="invalid")
        losing_owner = target.role == "owner" and target.enabled and (role != "owner" or not enabled)
        if losing_owner and enabled_owners() <= 1:
            return back("/admin/users", error="last_owner")
        fields: dict[str, object] = {"role": role, "enabled": enabled, "can_view_content": form.get("content") == "on"}
        if new_password := str(form.get("password", "")):
            if password_problem(new_password):
                return back("/admin/users", error="invalid")
            fields["password_hash"] = hash_password(new_password)
        store.update_admin(user_id, session.user.username, **fields)
        return back("/admin/users", "saved")

    @app.post("/admin/users/{user_id}/delete")
    async def users_delete(user_id: int, request: Request):
        session, _ = await require_post(request, "owner")
        target = store.get_admin(user_id=user_id)
        if target is None:
            return back("/admin/users", error="invalid")
        if target.id == session.user.id:
            return back("/admin/users", error="self")
        if target.role == "owner" and target.enabled and enabled_owners() <= 1:
            return back("/admin/users", error="last_owner")
        store.delete_admin(user_id, session.user.username)
        return back("/admin/users", "deleted")

    # -- account -------------------------------------------------------------------------

    def account_page(request: Request, session: AdminSession, **context) -> HTMLResponse:
        _, _, recovery = store.totp_state(session.user.id)
        return page(request, "account.html", session, recovery_left=len(recovery), **context)

    @app.get("/admin/account")
    async def account(request: Request):
        session = require(request, "viewer")
        return account_page(request, session)

    @app.post("/admin/account/2fa/start")
    async def account_2fa_start(request: Request):
        session, _ = await require_post(request, "viewer")
        if session.user.has_2fa:
            return back("/admin/account")
        secret = totp.new_secret()
        pending_setup[session.token_hash] = (secret, time.time() + 600)
        return account_2fa_setup_page(request, session, secret)

    def account_2fa_setup_page(request: Request, session: AdminSession, secret: str, error: str | None = None):
        site = (bootstrap.public_url or f"{request.url.scheme}://{request.url.netloc}").split("://")[-1]
        uri = totp.provisioning_uri(secret, f"{session.user.username}@{site}")
        grouped = " ".join(secret[i : i + 4] for i in range(0, len(secret), 4))
        return page(request, "account_2fa.html", session, qr=totp.qr_svg(uri), secret=grouped, error=error)

    @app.post("/admin/account/2fa/enable")
    async def account_2fa_enable(request: Request):
        session, form = await require_post(request, "viewer")
        secret, expires = pending_setup.get(session.token_hash, (None, 0.0))
        if secret is None or expires < time.time():
            pending_setup.pop(session.token_hash, None)
            return account_page(request, session, error="The setup expired; please start again.")
        # the password too: a stolen session alone mustn't tie the account to someone else's phone
        if not verify_password(store.password_hash(session.user.id), str(form.get("current", ""))):
            return account_2fa_setup_page(request, session, secret, "Current password is wrong.")
        counter = totp.verify(secret, str(form.get("code", "")))
        if counter is None:
            return account_2fa_setup_page(request, session, secret, "That code didn't match. Check the phone's clock.")
        pending_setup.pop(session.token_hash, None)
        codes = totp.new_recovery_codes()
        store.set_totp(session.user.id, secret, [totp.hash_recovery(c) for c in codes], session.user.username)
        store.totp_used(session.user.id, counter)
        return only_this_session(request, session, "account_recovery.html", codes=codes)

    def only_this_session(request: Request, session: AdminSession, template: str, **context) -> Response:
        """After a change to how this account signs in: every other session of it ends (one taken from another
        browser mustn't outlive it); this browser gets a new one, and ``template`` is shown with it."""
        store.delete_sessions(session.user.id)
        token, csrf = sessions.start(request, store.get_admin(user_id=session.user.id))
        if template.startswith("/"):  # a page to go back to
            response = back(template, context.get("message"))
        else:
            response = page(request, template, dataclasses.replace(session, csrf=csrf), **context)
        sessions.set_cookie(response, request, token)
        return response

    @app.post("/admin/account/2fa/recovery")
    async def account_2fa_recovery(request: Request):
        session, form = await require_post(request, "viewer")
        secret, _, _ = store.totp_state(session.user.id)
        if secret is None:
            return back("/admin/account")
        if not verify_password(store.password_hash(session.user.id), str(form.get("current", ""))):
            return account_page(request, session, error="Current password is wrong.")
        codes = totp.new_recovery_codes()
        store.set_totp(session.user.id, secret, [totp.hash_recovery(c) for c in codes], session.user.username)
        return only_this_session(request, session, "account_recovery.html", codes=codes)

    @app.post("/admin/account/2fa/disable")
    async def account_2fa_disable(request: Request):
        session, form = await require_post(request, "viewer")
        if not verify_password(store.password_hash(session.user.id), str(form.get("current", ""))):
            return account_page(request, session, error="Current password is wrong.")
        store.set_totp(session.user.id, None, [], session.user.username)
        return only_this_session(request, session, "/admin/account", message="2fa_off")

    @app.post("/admin/users/{user_id}/reset-2fa")
    async def users_reset_2fa(user_id: int, request: Request):
        """For a lost phone: the user signs in with the password only, then sets 2FA up again."""
        session, _ = await require_post(request, "owner")
        target = store.get_admin(user_id=user_id)
        if target is None or target.id == session.user.id:
            return back("/admin/users", error="invalid" if target is None else "self")
        store.set_totp(target.id, None, [], session.user.username)
        store.delete_sessions(target.id)
        return back("/admin/users", "2fa_reset")

    @app.post("/admin/account")
    async def account_password(request: Request):
        session, form = await require_post(request, "viewer")
        if not verify_password(store.password_hash(session.user.id), str(form.get("current", ""))):
            return page(request, "account.html", session, error="Current password is wrong.")
        if problem := password_problem(str(form.get("password", "")), str(form.get("confirm", ""))):
            return page(request, "account.html", session, error=problem)
        store.update_admin(session.user.id, session.user.username, password_hash=hash_password(str(form["password"])))
        token, _ = sessions.start(request, store.get_admin(user_id=session.user.id))
        response = back("/admin/account", "password")
        sessions.set_cookie(response, request, token)
        return response

    # -- operating mode ------------------------------------------------------------------

    @app.post("/admin/mode")
    async def set_mode(request: Request):
        session, form = await require_post(request, "operator")
        mode = str(form.get("mode", ""))
        if mode not in MODES:
            return back("/admin", error="invalid")
        message = str(form.get("message", "")).strip()[:300]
        store.set_meta("mode", mode)
        store.set_meta("maintenance_message", message)
        store.audit(session.user.username, "mode", mode, {"message": message})
        ctx.reload()
        return back("/admin", "saved")

    # -- clients -------------------------------------------------------------------------

    def show_new_key(request: Request, session: AdminSession, name: str, key: str, regenerated: bool):
        base = ctx.public_base(request)
        return page(
            request,
            "client_key.html",
            session,
            name=name,
            key=key,
            regenerated=regenerated,
            base_url=f"{base}/v1",
            plugin=plugin_install_commands(base, key),
        )

    @app.get("/admin/clients")
    async def clients(request: Request):
        session = require(request, "admin")
        live = ctx.live_state().get("clients", {})
        every_client = store.list_clients()
        groups = store.list_groups()
        blocks = [
            {
                "ip": ip,
                "left": how_long(left),
                "apps": block.apps,
                "refused": block.refused,
                "proxy": ignored_proxy(ip),
            }
            for ip, left, block in ctx.guard.blocks()
        ]
        return page(
            request,
            "clients.html",
            session,
            blocks=blocks,
            guard=ctx.guard,
            blocked_for=how_long(ctx.guard.block_s),
            clients=every_client,
            groups=groups,
            group_of={g.id: g for g in groups},
            members={g.id: [c.name for c in every_client if c.group_id == g.id] for g in groups},
            own_limits=_own_limits,
            priorities=PRIORITIES,
            live=live,
            plugin=plugin_install_commands(ctx.public_base(request)),
            month={c.name: ctx.month_usage(c.name) for c in every_client},
            edit_choices=edit_routes.edit_choices(store),
            edit_names={e.id: e.name for e in store.list_edits()},
            now=time.time(),
            content_days=store.get_tunables().get("content_log_days", Settings().content_log_days),
            content_kb=store.get_tunables().get("content_log_max_kb", Settings().content_log_max_kb),
        )

    @app.post("/admin/clients")
    async def clients_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        priority = str(form.get("priority", "normal"))
        try:
            expires_at = _parse_date(str(form.get("expires", "")))
        except ValueError:
            return back("/admin/clients", error="invalid")
        # "anonymous" is what requests without a key are stored under (allow_no_auth)
        if not CLIENT_NAME.fullmatch(name) or name == "anonymous" or priority not in PRIORITIES:
            return back("/admin/clients", error="invalid")
        try:
            key = store.create_client(
                name,
                session.user.username,
                priority=priority,
                notes=str(form.get("notes", ""))[:500],
                expires_at=expires_at,
                group_id=_group_field(form, store),
            )
        except sqlite3.IntegrityError:
            return back("/admin/clients", error="exists")
        except ValueError:
            return back("/admin/clients", error="invalid")
        ctx.reload()
        return show_new_key(request, session, name, key, regenerated=False)

    @app.post("/admin/clients/{client_id}")
    async def clients_update(client_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields = {
                "priority": str(form.get("priority", "normal")),
                "enabled": form.get("enabled") == "on",
                "notes": str(form.get("notes", ""))[:500],
                "expires_at": _parse_date(str(form.get("expires", ""))),
            }
            name = str(form.get("name", "")).strip()
            if name:  # renamed: its history, jobs, files... move with it (the key stays the same)
                if not CLIENT_NAME.fullmatch(name) or name == "anonymous":
                    raise ValueError("name")
                fields["name"] = name
            if form.get("group_id"):
                fields["group_id"] = _group_field(form, store)
            store.update_client(client_id, session.user.username, **fields)
        except sqlite3.IntegrityError:
            return back("/admin/clients", error="exists")
        except (ValueError, KeyError):
            return back("/admin/clients", error="invalid")
        ctx.reload()
        return back("/admin/clients", "saved")

    @app.post("/admin/clients/{client_id}/key")
    async def clients_show_key(client_id: int, request: Request):
        """A client's key, for the Show button (audited); 404 when only its hash is kept."""
        session = require(request, "admin")
        if not csrf_ok(session, request.headers.get("x-csrf-token")):
            raise NotAllowed("csrf")
        if not session.user.can_view_content:  # the key reads that client's job results and files through /v1
            return JSONResponse({"error": "showing keys needs 'can read logged content' (Users page)"}, status_code=403)
        key = store.client_key(client_id, session.user.username)
        if key is None:
            return JSONResponse({"error": "only this key's hash is kept: issue a new key to see it"}, status_code=404)
        shown = {"key": key, "install": plugin_install_commands(ctx.public_base(request), key)}  # for the opencode card
        return JSONResponse(shown, headers={"Cache-Control": "no-store"})

    @app.post("/admin/clients/{client_id}/policy")
    async def clients_policy(client_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            current = next((c.policy for c in store.list_clients() if c.id == client_id), None)
            if current is None:
                raise KeyError(client_id)
            policy = ClientPolicy(  # its models come from its group
                user_agents=_user_agents(form),
                **_ua_fields(form),
                tags=",".join(parse_tags(str(form.get("tags", "")))),
                **_limits(form),  # blank: the group's
                log_content=current.log_content,  # set with its own form (below)
                log_content_until=current.log_content_until,
                edits=edit_routes.edit_ids(form, store),  # after its group's
            )
            store.update_client(client_id, session.user.username, policy=policy)
        except RulesError:
            return back("/admin/clients", error="rules_invalid")
        except (ValueError, KeyError):
            return back("/admin/clients", error="invalid")
        ctx.reload()
        return back("/admin/clients", "saved")

    @app.post("/admin/clients/{client_id}/logging")
    async def clients_logging(client_id: int, request: Request):
        """Content logging on/off for one client (optionally for N hours only)."""
        session, form = await require_post(request, "admin")
        target = next((c for c in store.list_clients() if c.id == client_id), None)
        if target is None:
            return back("/admin/clients", error="invalid")
        enabled = form.get("enabled") == "on"
        hours = str(form.get("hours", "")).strip()
        try:
            span = float(hours) if enabled and hours else None
        except ValueError:
            return back("/admin/clients", error="invalid")
        if span is not None and not (math.isfinite(span) and 0 < span <= 24 * 366):  # "inf" would break the page
            return back("/admin/clients", error="invalid")
        until = time.time() + span * 3600 if span is not None else None
        policy = dataclasses.replace(target.policy, log_content=enabled, log_content_until=until)
        store.update_client(client_id, session.user.username, policy=policy)
        ctx.reload()
        return back("/admin/clients", "saved")

    @app.post("/admin/clients/{client_id}/purge-content")
    async def clients_purge_content(client_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        target = next((c for c in store.list_clients() if c.id == client_id), None)
        if target is None:
            return back("/admin/clients", error="invalid")
        store.purge_content(target.name, session.user.username)
        return back("/admin/clients", "content_purged")

    @app.post("/admin/requests/purge-content")
    async def requests_purge_content(request: Request):
        session, _ = await require_post(request, "admin")
        store.purge_content(None, session.user.username)
        return back("/admin/requests", "content_purged")

    @app.get("/admin/requests/{request_id}/content")
    async def request_content(request_id: str, request: Request):
        session = require(request, "admin")
        if not session.user.can_view_content:
            raise NotAllowed("content")
        content = await asyncio.to_thread(store.get_content, request_id)
        if content is None:
            return back("/admin/requests", error="content_missing")
        store.audit(session.user.username, "content.view", request_id, {"client": content["client"]})
        return page(
            request,
            "request_content.html",
            session,
            content=content,
            request_text=_pretty(content["request"]),
            response_text=_pretty(content["response"]),
        )

    @app.get("/admin/clients/{client_id}/opencode.json")
    async def clients_opencode(client_id: int, request: Request):
        """Download the opencode config for this client (key left as {env:QUEUEROUTER_KEY})."""
        session = require(request, "admin")
        target = next((c for c in store.list_clients() if c.id == client_id), None)
        config = await ctx.opencode_config(request, target.name) if target else None
        if config is None:
            return back("/admin/clients", error="client_unavailable")
        store.audit(session.user.username, "client.opencode_config", target.name)
        return Response(
            json.dumps(config, indent=2, ensure_ascii=False),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="opencode.json"'},
        )

    @app.post("/admin/unblock")  # not under /admin/clients/: /admin/clients/{client_id} would take it
    async def clients_unblock(request: Request):
        """Lets an address blocked for failed keys in again (its apps get in at once once they have a working key)."""
        session, form = await require_post(request, "admin")
        ip = str(form.get("ip", ""))
        if not ctx.guard.unblock(ip):
            return back("/admin/clients", "not_blocked")
        store.audit(session.user.username, "client.unblock", ip)
        return back("/admin/clients", "unblocked")

    @app.post("/admin/clients/{client_id}/regenerate")
    async def clients_regenerate(client_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        target = next((c for c in store.list_clients() if c.id == client_id), None)
        if target is None:
            return back("/admin/clients", error="invalid")
        key = store.regenerate_client_key(client_id, session.user.username)
        ctx.reload()
        return show_new_key(request, session, target.name, key, regenerated=True)

    @app.post("/admin/clients/{client_id}/delete")
    async def clients_delete(client_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_client(client_id, session.user.username)
        ctx.reload()
        return back("/admin/clients", "deleted")

    # -- client groups -------------------------------------------------------------------

    def group_page(request: Request, session: AdminSession, group, error: str | None = None) -> HTMLResponse:
        members = [c.name for c in store.list_clients() if group.id is not None and c.group_id == group.id]
        return page(
            request,
            "group_edit.html",
            session,
            g=group,
            p=group.policy,
            new=group.id is None,
            every=group.every,
            members=members,
            error=error,
            choices=ctx.model_choices(),
            edit_choices=edit_routes.edit_choices(store),
        )

    def group_fields(form: dict) -> dict:
        """A group's name, notes and policy from its editor (ValueError / RulesError when something is wrong)."""
        name = str(form.get("name", "")).strip()
        if not GROUP_NAME.fullmatch(name):
            raise ValueError("name")
        every = form.get("every_model") == "on"
        models = () if every else tuple(dict.fromkeys(_items(form, "models")))
        if not every and not models:
            raise ValueError("group_models")
        edits = edit_routes.edit_ids(form, store)
        policy = ClientPolicy(allowed_models=models, edits=edits, **_limits(form))
        return {"name": name, "notes": str(form.get("notes", "")).strip()[:500], "policy": policy}

    def group_from_form(form: dict, group_id: int | None = None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""
        raw = {name: str(form.get(name, "")).strip() or None for name in GROUP_LIMITS}
        raw.update(on_limit=str(form.get("on_limit", "")), loop_action=str(form.get("loop_action", "")))
        raw["params"] = str(form.get("params", ""))
        return SimpleNamespace(
            id=group_id,
            name=str(form.get("name", "")).strip(),
            notes=str(form.get("notes", "")),
            policy=SimpleNamespace(allowed_models=_items(form, "models"), edits=_items(form, "edits"), **raw),
            every=form.get("every_model") == "on",
        )

    def group_error(exc: Exception) -> str:
        if isinstance(exc, RulesError):
            return MESSAGES["rules_invalid"]
        if isinstance(exc, sqlite3.IntegrityError):
            return MESSAGES["exists"]
        return MESSAGES.get(str(exc), MESSAGES["invalid"])

    @app.get("/admin/groups/new")
    async def group_new(request: Request):
        session = require(request, "admin")
        empty = SimpleNamespace(id=None, name="", notes="", policy=ClientPolicy(), every=False)
        return group_page(request, session, empty)

    @app.post("/admin/groups")
    async def groups_create(request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields = group_fields(form)
            store.create_group(fields["name"], session.user.username, fields["policy"], fields["notes"])
        except (RulesError, ValueError, sqlite3.IntegrityError) as exc:
            return group_page(request, session, group_from_form(form), group_error(exc))
        return back("/admin/clients#groups", "created")

    @app.get("/admin/groups/{group_id}")
    async def group_edit(group_id: int, request: Request):
        session = require(request, "admin")
        group = next((g for g in store.list_groups() if g.id == group_id), None)
        if group is None:
            return back("/admin/clients", error="invalid")
        shown = SimpleNamespace(**vars(group), every=not group.policy.allowed_models)
        return group_page(request, session, shown)

    @app.post("/admin/groups/{group_id}")
    async def groups_update(group_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.update_group(group_id, session.user.username, **group_fields(form))
        except KeyError:
            return back("/admin/clients", error="invalid")
        except (RulesError, ValueError, sqlite3.IntegrityError) as exc:
            return group_page(request, session, group_from_form(form, group_id), group_error(exc))
        ctx.reload()  # its clients get the change at once
        return back("/admin/clients#groups", "saved")

    @app.post("/admin/groups/{group_id}/delete")
    async def groups_delete(group_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_group(group_id, session.user.username)
        except KeyError:
            return back("/admin/clients", error="invalid")
        except ValueError as exc:
            return back("/admin/clients#groups", error=str(exc))
        return back("/admin/clients#groups", "deleted")

    # -- upstreams and limit pools -------------------------------------------------------

    def upstreams_page(request: Request, session: AdminSession, test: dict | None = None):
        info = {u["id"]: u for u in ctx.upstream_info()}
        pools_live = ctx.live_state().get("pools", {})
        return page(
            request,
            "upstreams.html",
            session,
            pools=store.list_pools(),
            upstreams=store.list_upstreams(),
            info=info,
            pools_live=pools_live,
            presets=PRESETS.values(),
            plans=all_plans(),
            test=test,
            strategy=store.get_tunables().get("lb_strategy", "least-busy"),
        )

    def int_field(form: dict, name: str, low: int, high: int, default: int | None = None) -> int | None:
        raw = str(form.get(name, "")).strip()
        if raw == "":
            if default is None:
                return None
            return default
        value = int(raw)
        if not low <= value <= high:
            raise ValueError(name)
        return value

    def pool_fields(form: dict) -> dict:
        """max_concurrency / context_cap / plan from the form; a provider's plan (ArliAI's, Synthetic's) fills the
        first two and its price."""
        chosen = plan_choice(str(form.get("plan", "")) or None)
        label, plan = chosen if chosen else (None, None)
        concurrency = int_field(form, "max_concurrency", 1, 1000, plan.max_concurrency if plan else 1)
        context_cap = int_field(form, "context_cap", 1, 10_000_000)
        if plan and not str(form.get("max_concurrency", "")).strip():
            concurrency = plan.max_concurrency
        if plan and context_cap is None:
            context_cap = plan.context_cap
        monthly_price = _opt(form, "monthly_price", float, 0)
        if plan and monthly_price is None:
            monthly_price = plan.monthly_price
        embeddings = int_field(form, "embedding_concurrency", 1, 1000)  # blank = shared with chat
        if plan and embeddings is None and "embedding_concurrency" not in form:
            embeddings = plan.embedding_concurrency
        return {
            "plan": label if plan else (str(form.get("plan_label", "")).strip() or None),
            "max_concurrency": concurrency,
            "context_cap": context_cap,
            "min_spacing_ms": int_field(form, "min_spacing_ms", 0, 600_000, 0),
            "monthly_price": monthly_price,
            "embedding_concurrency": embeddings,
        }

    @app.get("/admin/upstreams")
    async def upstreams(request: Request):
        session = require(request, "admin")
        return upstreams_page(request, session)

    @app.post("/admin/pools")
    async def pools_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        try:
            fields = pool_fields(form)
            if not CLIENT_NAME.fullmatch(name):
                raise ValueError("name")
            store.create_pool(name, fields.pop("max_concurrency"), session.user.username, **fields)
        except ValueError:
            return back("/admin/upstreams", error="invalid")
        except sqlite3.IntegrityError:
            return back("/admin/upstreams", error="exists")
        ctx.reload()
        return back("/admin/upstreams", "created")

    @app.post("/admin/pools/{pool_id}")
    async def pools_update(pool_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.update_pool(pool_id, session.user.username, **pool_fields(form))
        except (ValueError, KeyError):
            return back("/admin/upstreams", error="invalid")
        ctx.reload()
        return back("/admin/upstreams", "saved")

    @app.get("/admin/upstreams/{upstream_id}/models")
    async def upstream_models_page(upstream_id: int, request: Request):
        session = require(request, "admin")
        upstream = next((u for u in store.list_upstreams() if u.id == upstream_id), None)
        if upstream is None:
            return back("/admin/upstreams", error="invalid")
        listed = await ctx.upstream_models(upstream_id)
        rule = parse_filter(upstream.model_filter)
        offer_new = rule[0] if rule else True
        rows = [
            {"id": m, "offered": rule is None or ((m not in rule[1]) if rule[0] else (m in rule[1]))}
            for m in listed or []
        ]
        return page(
            request,
            "upstream_models.html",
            session,
            upstream=upstream,
            rows=rows,
            listed=listed,
            offer_new=offer_new,
            offered=sum(1 for r in rows if r["offered"]),
        )

    @app.post("/admin/upstreams/{upstream_id}/models")
    async def upstream_models_save(upstream_id: int, request: Request):
        session, form = await require_post(request, "admin")
        listed = await ctx.upstream_models(upstream_id)
        if listed is None:
            return back(f"/admin/upstreams/{upstream_id}/models", error="invalid")
        chosen = {str(v) for k, v in form.items() if k.startswith("model_")} & set(listed)
        rule = make_filter(set(listed), chosen, form.get("offer_new") == "on")
        try:
            store.update_upstream(upstream_id, session.user.username, model_filter=rule)
        except (ValueError, KeyError):
            return back("/admin/upstreams", error="invalid")
        ctx.reload()
        return back(f"/admin/upstreams/{upstream_id}/models", "saved")

    @app.post("/admin/pools/{pool_id}/delete")
    async def pools_delete(pool_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_pool(pool_id, session.user.username)
        except ValueError:
            return back("/admin/upstreams", error="pool_in_use")
        ctx.reload()
        return back("/admin/upstreams", "deleted")

    @app.post("/admin/upstreams")
    async def upstreams_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        preset = PRESETS.get(str(form.get("preset", "custom")), PRESETS["custom"])
        base_url = str(form.get("base_url", "")).strip().rstrip("/") or preset.base_url
        api_key = str(form.get("api_key", "")).strip()
        try:
            if not CLIENT_NAME.fullmatch(name) or not URL.fullmatch(base_url):
                raise ValueError("name/url")
            if preset.key_required and not api_key:
                raise ValueError("api_key")
            weight = int_field(form, "weight", 1, 100, 1)
            # everything checked before a new pool is made: a mistake mustn't leave one behind (whose name then
            # makes the corrected form fail with "taken")
            params = _rules_text(form) or preset.params
            retries = _opt(form, "max_retries", int, 0)
            ua = _ua_fields(form)
            if name in {u.name for u in store.list_upstreams()}:
                raise sqlite3.IntegrityError("name")
            pool_choice = str(form.get("pool", "new"))
            new_pool = pool_choice == "new"
            if new_pool:
                fields = pool_fields(form)
                planned = plan_choice(str(form.get("plan", "")) or None)  # a plan sets it (ArliAI's, Synthetic's)
                if preset.key != "arliai" and not planned and not str(form.get("max_concurrency", "")).strip():
                    fields["max_concurrency"] = preset.max_concurrency
                pool_id = store.create_pool(name, fields.pop("max_concurrency"), session.user.username, **fields)
            else:
                pool_id = int(pool_choice)
                if pool_id not in {p.id for p in store.list_pools()}:
                    raise ValueError("pool")
            try:
                upstream_id = store.create_upstream(
                    name,
                    base_url,
                    api_key,
                    pool_id,
                    session.user.username,
                    preset=preset.key,
                    weight=weight,
                    params=params,
                    max_retries=retries if retries is not None else preset.max_retries,
                    **ua,
                )
            except Exception:
                if new_pool:
                    store.delete_pool(pool_id, session.user.username)
                raise
        except RulesError:
            return back("/admin/upstreams", error="rules_invalid")
        except ValueError:
            return back("/admin/upstreams", error="invalid")
        except sqlite3.IntegrityError:
            return back("/admin/upstreams", error="exists")
        ctx.reload()
        return back(f"/admin/upstreams/{upstream_id}/models", "created")  # then choose what it offers

    @app.post("/admin/upstreams/{upstream_id}")
    async def upstreams_update(upstream_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields: dict[str, object] = {
                "enabled": form.get("enabled") == "on",
                "paused": form.get("paused") == "on",
                "weight": int_field(form, "weight", 1, 100, 1),
                "params": _rules_text(form),
                "max_retries": _opt(form, "max_retries", int, 0),
                **_ua_fields(form),
            }
            base_url = str(form.get("base_url", "")).strip().rstrip("/")
            if base_url:
                if not URL.fullmatch(base_url):
                    raise ValueError("url")
                fields["base_url"] = base_url
            if api_key := str(form.get("api_key", "")).strip():  # blank = keep the stored key
                fields["api_key"] = api_key
            if pool := str(form.get("pool", "")).strip():
                if int(pool) not in {p.id for p in store.list_pools()}:
                    raise ValueError("pool")
                fields["pool_id"] = int(pool)
            if "preset" in form:  # another provider: what QueueRouter knows it as (a custom one becomes a known one)
                preset = PRESETS.get(str(form["preset"]))
                if preset is None:
                    raise ValueError("preset")
                fields["preset"] = preset.key
                if form.get("preset_defaults") == "on":  # and its defaults, in place of the row's
                    if preset.key != "custom":
                        fields["base_url"] = preset.base_url
                    fields["params"] = preset.params
                    fields["max_retries"] = preset.max_retries
            store.update_upstream(upstream_id, session.user.username, **fields)
        except RulesError:
            return back("/admin/upstreams", error="rules_invalid")
        except (ValueError, KeyError):
            return back("/admin/upstreams", error="invalid")
        ctx.reload()
        return back("/admin/upstreams", "saved")

    @app.post("/admin/upstreams/{upstream_id}/delete")
    async def upstreams_delete(upstream_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_upstream(upstream_id, session.user.username)
        ctx.reload()
        return back("/admin/upstreams", "deleted")

    @app.post("/admin/upstreams/{upstream_id}/test")
    async def upstreams_test(upstream_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        target = next((u for u in store.list_upstreams() if u.id == upstream_id), None)
        if target is None:
            return back("/admin/upstreams", error="invalid")
        ok, message = await ctx.probe(target.base_url, store.upstream_key(upstream_id))
        store.audit(session.user.username, "upstream.test", target.name, {"ok": ok})
        return upstreams_page(request, session, test={"name": target.name, "ok": ok, "message": message})

    # -- requests log and usage ----------------------------------------------------------

    @app.get("/admin/requests/{request_id}/trace")
    async def request_trace(request_id: str, request: Request):
        """A council's or orchestrator's work: its timeline and every call it made."""
        session = require(request, "viewer")
        family = [dict(r) for r in await asyncio.to_thread(store.request_family, request_id[:40])]
        main = next((r for r in family if r["id"] == request_id), None)
        if main is None:
            return back("/admin/requests", error="invalid")
        try:
            timeline = json.loads(main["timeline"] or "[]")
        except ValueError:
            timeline = []
        # [seconds, note, encrypted excerpt]: excerpts of the conversation only for those who may read logged
        # content (as on the content page); anything malformed is skipped rather than breaking the page
        may_read = session.user.can("admin") and session.user.can_view_content
        steps = []
        for item in timeline if isinstance(timeline, list) else []:
            if not (isinstance(item, list | tuple) and len(item) in (2, 3) and isinstance(item[0], int | float)):
                continue
            note = str(item[1])
            if may_read and len(item) == 3 and item[2]:
                note += store.reveal(str(item[2])) or ""
            steps.append((float(item[0]), note))
        timeline = steps
        calls = [r for r in family if r["id"] != request_id]
        totals = {
            "calls": len(calls),
            "prompt": sum(r["prompt_tokens"] or 0 for r in calls),
            "completion": sum(r["completion_tokens"] or 0 for r in calls),
            "errors": sum(1 for r in calls if r["outcome"] != "done"),
        }
        return page(request, "request_trace.html", session, r=main, timeline=timeline, calls=calls, totals=totals)

    REQUEST_PAGE, REQUEST_CATCH_UP = 50, 300  # rows fetched at a time / at most after being away
    REQUEST_FILTERS = ("client", "outcome", "model", "upstream", "tag", "answer_tag")

    @app.get("/admin/requests")
    async def requests_log(request: Request):
        """Only the page: its rows and filter choices come from the /admin/api/requests calls below, so it opens at
        once however long the log is."""
        session = require(request, "viewer")
        filters = {k: request.query_params.get(k, "") for k in REQUEST_FILTERS}
        return page(request, "requests.html", session, filters=filters, catch_up=REQUEST_CATCH_UP)

    @app.get("/admin/api/requests")
    async def api_requests(request: Request):
        """Finished requests, newest first, with what each cost: a page of the newest, the page older than a row
        (``?before=<ts>&before_id=<id>``), or the ones written since a time (``?after=<ts>``, the row at that time
        included). ``more`` says that there are further rows beyond these."""
        require(request, "viewer")
        query = request.query_params
        filters = {k: query.get(k, "") for k in REQUEST_FILTERS}
        try:
            after = float(query["after"]) if "after" in query else None
            before = (float(query["before"]), query.get("before_id", "")) if "before" in query else None
        except ValueError:
            message = {"error": {"message": "after and before must be timestamps", "code": 400}}
            return JSONResponse(message, status_code=400)
        limit = REQUEST_CATCH_UP if after is not None else REQUEST_PAGE

        def load() -> dict:
            rows = store.list_requests(limit + 1, after, before, REQUEST_LIST_COLUMNS, **filters)
            calls = store.calls_of([r for r in rows[:limit] if r["council"]])
            costed = request_costs(rows[:limit], calls, store.list_model_prices(), ctx.prices, ctx.names())
            return {"rows": costed, "more": len(rows) > limit}

        return JSONResponse(await asyncio.to_thread(load))

    @app.get("/admin/api/requests/filters")
    async def api_request_filters(request: Request):
        """What the Requests page's filters can choose from."""
        require(request, "viewer")
        values = await asyncio.to_thread(store.request_filter_values)
        labels = {value: ctx.model_label(value) for value in values["model"]}
        return JSONResponse({**values, "labels": labels})

    @app.get("/admin/api/requests/{request_id}")
    async def api_request(request_id: str, request: Request):
        """What opens under a row on the Requests page."""
        session = require(request, "viewer")
        row = await asyncio.to_thread(store.request_row, request_id)
        if row is None:
            return JSONResponse({"error": {"message": "that request is gone", "code": 404}}, status_code=404)
        details = {key: row[key] for key in REQUEST_DETAILS}
        details["client_headers"] = _from_json(row["client_headers"])
        if session.user.can("admin"):  # where it came from: for admins, like the blocked addresses
            details["ip"] = row["ip"]
        may_read = session.user.can("admin") and session.user.can_view_content  # as the content page requires
        has_content = may_read and request_id in await asyncio.to_thread(store.content_ids, [request_id])
        return JSONResponse({**details, "has_content": has_content})

    async def usage_summary(request: Request) -> dict:
        """The Usage page's numbers for its period (?range=24h|1d|7d|30d|custom, custom with from, from_hour, to and
        to_hour), per day or over the whole period (?group=range)."""
        query = request.query_params
        now = time.time()
        fields = (query.get(k, "") for k in ("range", "from", "from_hour", "to", "to_hour"))
        period = periods.resolve(*fields, now=now)
        merged = query.get("group") == "range"
        kept = {k: v for k in ("range", "from", "from_hour", "to", "to_hour", "group") if (v := query.get(k))}

        def link(path: str, **changed: str) -> str:
            params = {k: v for k, v in {**kept, **changed}.items() if v}
            return f"{path}?{urlencode(params)}" if params else path

        def load() -> dict:
            stored = store.usage_between(period.start, period.end)
            whole = any(r["hour"] is None for r in stored)  # days from before hourly totals began
            overrides = store.list_model_prices()
            plan_per_month = sum(p.monthly_price or 0 for p in store.list_pools())
            names = ctx.names()
            ran = period.elapsed_hours(now)
            rows, totals, _ = summarize_usage(stored, overrides, ctx.prices, merged, plan_per_month, ran / 24, names)
            models, model_total = model_costs(stored, overrides, ctx.prices, names)
            per = "hour" if period.hours <= periods.HOURLY_UP_TO and not whole else "day"
            slots = period.hour_slots() if per == "hour" else period.day_slots()
            return {
                "period": period,
                "period_text": period.text(now),
                "choices": periods.CHOICES,
                "merged": merged,
                "rows": rows,
                "totals": totals,
                "plan_per_month": plan_per_month,
                "plan_span": f"{round(ran)} hours" if ran < 47.5 else f"{round(ran / 24)} days",
                "whole_before": store.hourly_usage_from() if whole else None,
                "links": {  # for a browser without scripts: the page's script keeps them up to date itself
                    "by_day": link("/admin/usage", group=""),
                    "whole": link("/admin/usage", group="range"),
                    "csv": link("/admin/usage.csv"),
                    "csv_models": link("/admin/usage.csv", by="model"),
                },
                "models": models,
                "model_total": model_total,
                "aliases": alias_costs(stored, overrides, ctx.prices, names),
                "catalog_loaded": bool(ctx.prices.prices),
                "charts": usage_charts(usage_by(stored, overrides, ctx.prices, per, names), slots, per),
            }

        return await asyncio.to_thread(load)

    @app.get("/admin/usage")
    async def usage(request: Request):
        session = require(request, "viewer")
        return page(request, "usage.html", session, **await usage_summary(request))

    @app.get("/admin/usage/body")
    async def usage_body(request: Request):
        """The Usage page's numbers alone, for the open page to bring up to date."""
        require(request, "viewer")
        return templates.TemplateResponse(request, "usage_body.html", await usage_summary(request))

    @app.get("/admin/response-times")
    async def response_times(request: Request):
        """The provider's own timings per model and upstream (app/latency.py)."""
        session = require(request, "viewer")
        window = request.query_params.get("window", "24h")
        if window not in latency.WINDOWS:
            window = "24h"
        now = time.time()
        rows = await asyncio.to_thread(store.response_rows, now - latency.WINDOWS[window])
        starts, series = latency.trend(rows, window, now, ctx.names())
        fmt = "%m-%d %H:%M" if window == "7d" else "%H:%M"
        labels = [datetime.fromtimestamp(t, UTC).strftime(fmt) for t in starts]
        chart = (
            lines(
                "Time to first token (median, UTC)",
                labels,
                [(name, f"l{i}", values) for i, (name, values) in enumerate(series, 1)],
                unit="s",
            )
            if series
            else None
        )
        return page(
            request,
            "response_times.html",
            session,
            window=window,
            windows=list(latency.WINDOWS),
            summaries=latency.summarize(rows),
            chart=chart,
            keeps_rows=(store.get_tunables().get("request_log_days", Settings.request_log_days) or 0) > 0,
        )

    @app.get("/admin/usage.csv")
    async def usage_csv(request: Request):
        require(request, "viewer")
        summary = await usage_summary(request)
        period, by_model = summary["period"], request.query_params.get("by") == "model"
        start, end = (periods.stamp(t, "%Y-%m-%dT%H:%MZ") for t in (period.start, period.end))
        out = io.StringIO()
        writer = csv.writer(out)
        columns = [
            "day",
            "client",
            *(["model"] if by_model else []),
            "requests",
            "errors",
            "prompt_tokens",
            "completion_tokens",
            "cached_tokens",
            "cache_write_tokens",
            "cost_usd",
            *(["upstream", "label"] if by_model else []),
        ]
        writer.writerow(columns)
        for client in summary["rows"]:
            day = client["day"] or f"{start}/{end}"  # over the whole period: its start and end
            for r in client["models"] if by_model else [client]:
                cost = f"{r['cost']:.4f}" if r["priced"] else ""
                values = {**r, "day": day, "client": client["client"], "cost_usd": cost}
                writer.writerow([_csv_cell(values[c]) for c in columns])
        name = f"queuerouter-usage-{start[:13].replace('-', '')}-{end[:13].replace('-', '')}.csv"
        return Response(
            out.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )

    # -- models and aliases --------------------------------------------------------------

    @app.get("/admin/models")
    async def models_page(request: Request):
        session = require(request, "admin")
        upstream_names = {u.id: u.name for u in store.list_upstreams()}
        models = ctx.models_by_upstream()
        overrides = store.list_model_prices()
        names = ctx.names()
        offered = {(upstream, m) for upstream, ids in models.items() for m in ids}
        aliases = store.list_aliases()
        alias_names = {a.name for a in aliases if a.enabled}  # a disabled one's name is a model's, as in requests
        listed = {m for _, m in offered}
        # a bare name in an alias's list that is an alias's and a provider's model too: it means the alias, as in
        # requests (model@provider is the model); said under the alias, since before 0.66 it meant the model
        shadowed = {
            a.name: [n for n in (a.target_model, *a.fallbacks) if n in alias_names and n in listed] for a in aliases
        }
        for alias in aliases:
            bare, pinned = names.unpin(alias.target_model)
            if not pinned and bare in alias_names:
                continue  # another alias: its own model is priced
            if pinned or not any(m == bare for _, m in offered):
                offered.add((pinned, bare))
        recent = store.used_models(days=PRICING_DAYS)  # older names only matter for old usage
        pairs = offered | {(None if upstream == "-" else upstream, m) for upstream, m in recent}
        pairs |= {(names.upstreams.get(upstream_id), m) for upstream_id, m in overrides}
        pricing = sorted(
            (
                {
                    "model": m,
                    "upstream_id": names.id_of(upstream),
                    "label": names.label(m, upstream),
                    "price": resolve(m, overrides, ctx.prices, names.id_of(upstream), names.name(m, upstream)),
                    "override": overrides.get((names.id_of(upstream), m)),
                    "offered": (upstream, m) in offered,
                }
                for upstream, m in pairs
            ),
            key=lambda row: row["label"].lower(),
        )
        since = store.get_tunables().get("price_mix_since", "")  # the token mix counts from this day
        return page(
            request,
            "models.html",
            session,
            aliases=aliases,
            alias_names=alias_names,
            shadowed=shadowed,
            triggers=TRIGGERS,
            default_triggers=DEFAULT_TRIGGERS,
            compact_modes=[m for m in COMPACT_MODES if m in ENABLED_COMPACT_MODES],
            council_modes=COUNCIL_MODES,
            council_when=COUNCIL_WHEN,
            upstreams=store.list_upstreams(),
            upstream_names=upstream_names,
            models=models,
            pricing=pricing,
            mix=(mix := token_mix(*store.token_totals(MIX_DAYS, since))),
            typical_mix=mix == DEFAULT_MIX,
            mix_days=MIX_DAYS,
            mix_since=since,
            mix_counts_since=since > time.strftime("%Y-%m-%d", time.gmtime(time.time() - (MIX_DAYS - 1) * 86_400)),
            today=datetime.now(UTC).date().isoformat(),
            references=ctx.prices.references(),
            catalog_size=len(ctx.prices.prices),
            catalog_age_h=round((time.time() - ctx.prices.fetched_at) / 3600, 1) if ctx.prices.fetched_at else None,
            contexts=ctx.context_table(),
            model_names=ctx.name_table(),
            ranking=ctx.ranking_table(),
            thinking=ctx.thinking_table(),
            efforts=THINKING_EFFORTS,
            rank_indexes=RANK_INDEXES,
            credit=(CREDIT, CREDIT_URL),
        )

    def chosen_upstream(form: dict) -> int:
        """The upstream a Models page row belongs to; EVERY for a setting for all of them."""
        upstream_id = int(form.get("upstream_id") or EVERY)
        if upstream_id != EVERY and upstream_id not in ctx.names().upstreams:
            raise ValueError("upstream")
        return upstream_id

    @app.post("/admin/model-names")
    async def model_names_update(request: Request):
        session, form = await require_post(request, "admin")
        model = str(form.get("model", "")).strip()
        name = " ".join(str(form.get("name", "")).split())
        try:
            upstream_id = chosen_upstream(form)
            if not model or len(model) > 200 or len(name) > 120 or upstream_id == EVERY:
                raise ValueError("model")
            store.set_model_name(upstream_id, model, session.user.username, name or None)
        except ValueError:
            return back("/admin/models#names", error="invalid")
        ctx.reload()
        return back("/admin/models#names", "saved")

    @app.post("/admin/model-limits")
    async def model_limits_update(request: Request):
        session, form = await require_post(request, "admin")
        model = str(form.get("model", "")).strip()
        try:
            if not model or len(model) > 200:
                raise ValueError("model")
            context = _opt(form, "context", int, 1024)
            output = _opt(form, "output", int, 1)
            if context and output and output > context:
                raise ValueError("output")
            store.set_model_limit(model, session.user.username, context, output, chosen_upstream(form))
        except ValueError:
            return back("/admin/models#contexts", error="invalid")
        ctx.reload()
        return back("/admin/models#contexts", "saved")

    @app.post("/admin/model-limits/forget")
    async def model_limits_forget(request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.forget_learned_limit(
                int(form.get("upstream_id", "")), str(form.get("model", "")), session.user.username
            )
        except ValueError:
            return back("/admin/models#contexts", error="invalid")
        ctx.reload()
        return back("/admin/models#contexts", "saved")

    @app.post("/admin/model-scores")
    async def model_scores_update(request: Request):
        session, form = await require_post(request, "admin")
        model = str(form.get("model", "")).strip()
        try:
            if not model or len(model) > 200:
                raise ValueError("model")
            score = _opt(form, "score", float, 0)  # blank: the index decides again
            if score is not None and score > 1000:
                raise ValueError("score")
            upstream_id = chosen_upstream(form)
            store.set_model_score(model, session.user.username, score, upstream_id)
            # one set for every provider, from before scores were per provider, goes too; not an alias's of that name
            if score is None and model not in {a.name for a in store.list_aliases()}:
                store.set_model_score(model, session.user.username, None)
        except ValueError:
            return back("/admin/models#ranking", error="invalid")
        return back("/admin/models#ranking", "saved")

    @app.post("/admin/model-thinking")
    async def model_thinking_update(request: Request):
        session, form = await require_post(request, "admin")
        model = str(form.get("model", "")).strip()
        levels = " ".join(str(form.get("levels", "")).lower().replace(",", " ").split())
        if not model or len(model) > 200 or any(e not in (*THINKING_EFFORTS, "-") for e in levels.split()):
            return back("/admin/models#thinking", error="invalid")
        try:
            upstream_id = chosen_upstream(form)
        except ValueError:
            return back("/admin/models#thinking", error="invalid")
        # blank: detection decides again; "-": no level selector for this model
        store.set_model_thinking(
            model, session.user.username, ",".join(levels.split()) if levels else None, upstream_id
        )
        if not levels:  # ones set for every provider, from before levels were per provider, go too
            store.set_model_thinking(model, session.user.username, None)
        return back("/admin/models#thinking", "saved")

    @app.post("/admin/model-scores/refresh")
    async def model_scores_refresh(request: Request):
        session, _ = await require_post(request, "admin")
        ok = await ctx.refresh_scores()
        store.audit(session.user.username, "scores.refresh", "artificialanalysis", {"ok": ok})
        return back("/admin/models#ranking", "saved" if ok else None, None if ok else "scores_failed")

    @app.post("/admin/prices")
    async def prices_update(request: Request):
        session, form = await require_post(request, "admin")
        model = str(form.get("model", "")).strip()
        try:
            if not model or len(model) > 200:
                raise ValueError("model")
            reference = str(form.get("reference", "")).strip() or None
            if reference and reference not in ctx.prices.prices:
                raise ValueError("reference")
            store.set_model_price(
                model,
                session.user.username,
                reference,
                _opt(form, "input", float, 0),
                _opt(form, "output", float, 0),
                _opt(form, "cached_input", float, 0),
                chosen_upstream(form),
            )
        except ValueError:
            return back("/admin/models#pricing", error="invalid")
        return back("/admin/models#pricing", "saved")

    @app.post("/admin/price-mix")
    async def price_mix_update(request: Request):
        session, form = await require_post(request, "admin")
        since = str(form.get("since", "")).strip()  # blank: the last 30 days again
        if since:
            try:
                day = datetime.strptime(since, "%Y-%m-%d").date()
            except ValueError:
                return back("/admin/models#mix", error="invalid")
            if day > datetime.now(UTC).date():
                return back("/admin/models#mix", error="invalid")
            since = day.isoformat()
        store.set_tunables({"price_mix_since": since}, session.user.username)
        ctx.reload()
        return back("/admin/models#mix", "saved")

    @app.post("/admin/prices/refresh")
    async def prices_refresh(request: Request):
        session, _ = await require_post(request, "admin")
        ok = await ctx.refresh_prices()
        store.audit(session.user.username, "prices.refresh", "models.dev", {"ok": ok})
        return back("/admin/models#pricing", "saved" if ok else None, None if ok else "refresh_failed")

    def alias_fields(form: dict, name: str, old: str | None = None) -> dict:
        """The editor's fields, checked (``old``: the alias's name before a rename to ``name``)."""
        target = str(form.get("target_model", "")).strip()
        if not target or len(target) > 200 or target == name:  # an alias pointing at itself reaches no model
            raise ValueError("target_model")
        label = " ".join(str(form.get("label", "")).split())
        if len(label) > LABEL_MAX or "|" in label:  # "|" separates the parts of opencode's names
            raise ValueError("label")
        known = {u.id for u in store.list_upstreams()}
        upstream_ids = tuple(int(v) for k, v in form.items() if k.startswith("upstream_") and int(v) in known)
        params = _rules_text(form)
        fallbacks = tuple(m.strip() for m in str(form.get("fallbacks", "")).replace("\n", ",").split(",") if m.strip())
        if any(len(m) > 200 for m in fallbacks) or len(fallbacks) > 10:
            raise ValueError("fallbacks")
        fallback_on = tuple(t for t in TRIGGERS if form.get(f"on_{t}") == "on")
        compact = str(form.get("compact", "off"))
        if compact not in ENABLED_COMPACT_MODES:
            raise ValueError("compact")
        compact_model = str(form.get("compact_model", "")).strip()
        if len(compact_model) > 200:
            raise ValueError("compact_model")
        virtual_context = _opt(form, "virtual_context", int, 1024)
        if virtual_context is not None and virtual_context > 100_000_000:
            raise ValueError("virtual_context")
        council = str(form.get("council", "off"))
        members = tuple(
            m.strip() for m in str(form.get("council_members", "")).replace("\n", ",").split(",") if m.strip()
        )
        judge = str(form.get("council_judge", "")).strip()
        when = str(form.get("council_when", "planning"))
        if council not in COUNCIL_MODES or when not in COUNCIL_WHEN or len(judge) > 200:
            raise ValueError("council")
        if any(len(m) > 200 for m in members) or len(members) > COUNCIL_MAX:
            raise ValueError("council_members")
        if council != "off" and len(members) < COUNCIL_MIN:
            raise ValueError("council_members")
        # its model and fallbacks may be other aliases (requests are forwarded to them), never leading back around;
        # renamed, the others that used its old name use the new one
        draft = SimpleNamespace(name=name, target_model=target, fallbacks=fallbacks)
        others = [a for a in store.list_aliases() if a.enabled and a.name != (old or name)]
        if old and old != name:
            others = [
                SimpleNamespace(
                    name=a.name,
                    target_model=name if a.target_model == old else a.target_model,
                    fallbacks=tuple(name if f == old else f for f in a.fallbacks),
                )
                for a in others
            ]
        aliases = {a.name: a for a in others} | {name: draft}
        if loop := loop_of(draft, aliases):
            raise AliasLoop(
                f"These aliases use each other in a loop: {' → '.join(loop)}. An alias can't lead back to itself."
            )
        tag_chain = tagger_routes.chain_from(form, store)  # TaggerError says what's wrong
        if tag_chain and not fallbacks:
            raise ValueError("check")
        answer_rules = rule_routes.rule_ids(form, store)
        if answer_rules and not fallbacks:
            raise ValueError("answer_rules")
        # unticked boxes aren't sent: "kinds" says the editor showed them (a form without them keeps the default)
        check_kinds = tuple(k for k in ANSWER_KINDS if form.get(f"kind_{k}") == "on")
        if "kinds" not in form:
            check_kinds = check_kinds or DEFAULT_ANSWER_KINDS
        elif (answer_rules or tag_chain) and not check_kinds:
            raise ValueError("check_kinds")
        check_when = str(form.get("check_when", "undecided"))
        if check_when not in CHECK_WHEN:
            raise ValueError("check_when")
        spot_checks = int_field(form, "spot_checks", 0, 100, SPOT_DEFAULT)
        learn_rules = str(form.get("learn_rules", "off"))
        if learn_rules not in LEARN_MODES or (learn_rules != "off" and not tag_chain):
            raise ValueError("learn_rules")
        learn_model = " ".join(str(form.get("learn_model", "")).split())[:200]
        edits = edit_routes.edit_ids(form, store)
        return {
            "edits": edits,
            "label": label,
            "answer_rules": answer_rules,
            "check_kinds": check_kinds or DEFAULT_ANSWER_KINDS,
            "check_when": check_when,
            "spot_checks": spot_checks,
            "learn_rules": learn_rules,
            "learn_model": learn_model,
            "learn_pass": form.get("learn_pass") == "on",
            "target_model": target,
            "upstream_ids": upstream_ids,
            "notes": str(form.get("notes", ""))[:300],
            "params": params,
            "fallbacks": fallbacks,
            "fallback_on": fallback_on,
            "compact": compact,
            "compact_model": compact_model,
            "virtual_context": virtual_context,
            "council": council,
            "council_members": members,
            "council_judge": judge,
            "council_when": when,
            "council_instructions": str(form.get("council_instructions", "")).strip()[:4000],
            "council_search": form.get("council_search") == "on",
            "council_search_max": int_field(form, "council_search_max", 1, 10, 3),
            "council_search_results": int_field(form, "council_search_results", 1, 10, 5),
            "council_fetch": form.get("council_fetch") == "on",
            "orch_rounds": int_field(form, "orch_rounds", 1, 30, 6),
            "orch_tasks": int_field(form, "orch_tasks", 1, 64, 8),
            "orch_seconds": int_field(form, "orch_seconds", 30, 3600, 300),
            "council_roles": "\n".join(
                line.strip() for line in str(form.get("council_roles", "")).splitlines() if line.strip()
            )[:8000],
            "tag_chain": tag_chain,
        }

    def alias_from_form(form: dict, existing=None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""

        def items(key: str) -> list[str]:
            return [m.strip() for m in str(form.get(key, "")).replace("\n", ",").split(",") if m.strip()]

        return SimpleNamespace(
            id=existing.id if existing else None,
            name=str(form.get("name", existing.name if existing else "")).strip(),
            label=str(form.get("label", "")),
            target_model=str(form.get("target_model", "")).strip(),
            upstream_ids=tuple(int(v) for k, v in form.items() if k.startswith("upstream_") and str(v).isdecimal()),
            notes=str(form.get("notes", "")),
            params=str(form.get("params", "")),
            fallbacks=items("fallbacks"),
            fallback_on=tuple(t for t in TRIGGERS if form.get(f"on_{t}") == "on"),
            compact=str(form.get("compact", "off")),
            compact_model=str(form.get("compact_model", "")).strip(),
            virtual_context=str(form.get("virtual_context", "")).strip() or None,
            council=str(form.get("council", "off")),
            council_members=items("council_members"),
            council_judge=str(form.get("council_judge", "")).strip(),
            council_when=str(form.get("council_when", "planning")),
            council_instructions=str(form.get("council_instructions", "")),
            council_search=form.get("council_search") == "on",
            council_search_max=str(form.get("council_search_max", "3")),
            council_search_results=str(form.get("council_search_results", "5")),
            council_fetch=form.get("council_fetch") == "on",
            orch_rounds=str(form.get("orch_rounds", "6")),
            orch_tasks=str(form.get("orch_tasks", "8")),
            orch_seconds=str(form.get("orch_seconds", "300")),
            council_roles=str(form.get("council_roles", "")),
            tag_chain=(),
            edits=items("edits"),
            answer_rules=items("answer_rules"),
            check_kinds=tuple(k for k in ANSWER_KINDS if form.get(f"kind_{k}") == "on"),
            check_when=str(form.get("check_when", "undecided")),
            spot_checks=str(form.get("spot_checks", SPOT_DEFAULT)),
            learn_rules=str(form.get("learn_rules", "off")),
            learn_model=str(form.get("learn_model", "")).strip(),
            learn_pass=form.get("learn_pass") == "on",
            enabled=form.get("enabled") == "on" if existing else True,
        )

    def alias_page(
        request: Request,
        session: AdminSession,
        alias,
        error: str | None = None,
        rules_seen: str | None = None,
        chain: list[dict] | None = None,
    ) -> HTMLResponse:
        """``rules_seen``: the rules the alias had when its editor was first opened (a form shown again keeps it);
        ``chain``: its taggers as the editor sent them (shown again after an error), else as saved."""
        if rules_seen is None:
            rules_seen = ",".join(str(i) for i in alias.answer_rules)
        if chain is None:
            chain = taggers_mod.rows_of(alias.tag_chain, {t.id: t for t in store.list_tags()})
        return page(
            request,
            "alias_edit.html",
            session,
            a=alias,
            new=alias.id is None,
            error=error,
            rules_seen=rules_seen,
            triggers=TRIGGERS,
            compact_modes=[m for m in COMPACT_MODES if m in ENABLED_COMPACT_MODES],
            council_modes=COUNCIL_MODES,
            council_when=COUNCIL_WHEN,
            council_max=COUNCIL_MAX,
            upstreams=store.list_upstreams(),
            choices=ctx.model_choices(),
            edit_choices=edit_routes.edit_choices(store),
            rule_choices=rule_routes.rule_choices(store),
            answer_kinds=ANSWER_KINDS,
            answer_kind_labels=ANSWER_KIND_LABELS,
            check_when=CHECK_WHEN,
            check_when_labels=CHECK_WHEN_LABELS,
            learn_modes=LEARN_MODES,
            learn_labels=LEARN_LABELS,
            search_on=store.get_tunables().get("search_provider", "off") not in ("", "off"),
            chain=tagger_routes.chain_slots(chain),
            **tagger_routes.chain_context(store),
        )

    def alias_error(exc: Exception) -> str:
        if isinstance(exc, AliasLoop | TaggerError):
            return str(exc)
        if str(exc) == "edits":
            return MESSAGES["edits"]
        if isinstance(exc, RulesError):
            return MESSAGES["rules_invalid"]
        if isinstance(exc, sqlite3.IntegrityError):
            return MESSAGES["exists"]
        return ALIAS_ERRORS.get(str(exc), MESSAGES["invalid"])

    @app.get("/admin/aliases/new")
    async def alias_new(request: Request):
        session = require(request, "admin")
        empty = SimpleNamespace(
            id=None, name="", label="", target_model="", upstream_ids=(), notes="", params="", fallbacks=[],
            fallback_on=DEFAULT_TRIGGERS, compact="off", compact_model="", virtual_context=None, council="off",
            council_members=[], council_judge="", council_when="planning", council_instructions="",
            council_search=False, council_search_max=3, council_search_results=5, council_fetch=False,
            orch_rounds=6, orch_tasks=8, orch_seconds=300, council_roles="", tag_chain=(), edits=(), enabled=True,
            answer_rules=(), check_kinds=DEFAULT_ANSWER_KINDS, check_when="undecided", spot_checks=SPOT_DEFAULT,
            learn_rules="off", learn_model="", learn_pass=False,
        )  # fmt: skip
        return alias_page(request, session, empty)

    @app.get("/admin/aliases/{alias_id}")
    async def alias_edit(alias_id: int, request: Request):
        session = require(request, "admin")
        alias = next((a for a in store.list_aliases() if a.id == alias_id), None)
        if alias is None:
            return back("/admin/models", error="invalid")
        return alias_page(request, session, alias)

    @app.post("/admin/aliases")
    async def aliases_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        try:
            if not ALIAS_NAME.fullmatch(name):
                raise ValueError("name")
            fields = alias_fields(form, name)
            target_model, upstream_ids = fields.pop("target_model"), fields.pop("upstream_ids")
            store.create_alias(name, target_model, upstream_ids, session.user.username, **fields)
        except (RulesError, ValueError, sqlite3.IntegrityError) as exc:
            shown = taggers_mod.chain_rows(form)
            return alias_page(request, session, alias_from_form(form), alias_error(exc), chain=shown)
        ctx.reload()
        return back("/admin/models", "created")

    @app.post("/admin/aliases/{alias_id}")
    async def aliases_update(alias_id: int, request: Request):
        session, form = await require_post(request, "admin")
        existing = next((a for a in store.list_aliases() if a.id == alias_id), None)
        if existing is None:
            return back("/admin/models", error="invalid")
        name = str(form.get("name", existing.name)).strip()
        try:
            if not ALIAS_NAME.fullmatch(name):
                raise ValueError("name")
            fields = alias_fields(form, name, existing.name)
            if "rules_seen" in form:  # rules its rule maker attached while the editor was open stay attached
                seen = {int(v) for v in str(form["rules_seen"]).split(",") if v.strip().isdecimal()}
                picked = fields["answer_rules"]
                since = tuple(i for i in existing.answer_rules if i not in seen and i not in picked)
                fields["answer_rules"] = (*picked, *since)[:RULES_PER_ALIAS]
            if name != existing.name:  # renamed: what QueueRouter keeps that names it follows (store.rename_alias)
                listed = {m for ids in ctx.models_by_upstream().values() for m in ids}
                store.rename_alias(alias_id, name, session.user.username, own_settings=existing.name not in listed)
            store.update_alias(alias_id, session.user.username, enabled=form.get("enabled") == "on", **fields)
        except (RulesError, ValueError, KeyError, sqlite3.IntegrityError) as exc:
            seen = str(form["rules_seen"]) if "rules_seen" in form else None
            shown = taggers_mod.chain_rows(form)
            return alias_page(request, session, alias_from_form(form, existing), alias_error(exc), seen, shown)
        ctx.reload()
        return back("/admin/models", "saved")

    @app.post("/admin/aliases/{alias_id}/delete")
    async def aliases_delete(alias_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_alias(alias_id, session.user.username)
        ctx.reload()
        return back("/admin/models", "deleted")

    # -- prompt edits (app/admin/edit_routes.py) -------------------------------------------------------------------

    helpers = edit_routes.Helpers(page, require, require_post, back, ctx.reload, ctx.model_choices, ctx.model_label)
    edit_routes.mount_edit_pages(app, store, helpers)
    rule_routes.mount_rule_pages(app, store, helpers)
    tagger_routes.mount_tagger_pages(app, store, helpers)

    # -- playground ----------------------------------------------------------------------

    @app.get("/admin/playground")
    async def playground_page(request: Request):
        session = require(request, "operator")
        names = ctx.names()
        offered = [(f"{m}@{u}", names.label(m, u)) for u, ids in ctx.models_by_upstream().items() for m in ids]
        aliases = [a.name for a in store.list_aliases() if a.enabled]
        clients = [c for c in store.list_clients() if c.enabled and not c.expired]
        offered.sort(key=lambda m: m[1].lower())
        return page(request, "playground.html", session, models=offered, aliases=aliases, clients=clients)

    @app.post("/admin/playground/send")
    async def playground_send(request: Request):
        """JSON in (fetch from the page, CSRF token in a header), OpenAI response / SSE out."""
        session = require(request, "operator")
        if not csrf_ok(session, request.headers.get("x-csrf-token")):
            raise NotAllowed("csrf")
        try:
            data = json.loads(await request.body())
            client_name = str(data.pop("client"))
        except (ValueError, KeyError, AttributeError, TypeError):  # TypeError: a JSON array
            return JSONResponse({"error": {"message": "bad playground request", "code": 400}}, status_code=400)
        allowed = {"model", "messages", "stream", "max_tokens", "temperature", "stream_options"}
        body = json.dumps({k: v for k, v in data.items() if k in allowed}).encode()
        store.audit(session.user.username, "playground", client_name, {"model": data.get("model")})
        return await ctx.playground(request, client_name, body)

    # -- alerts ----------------------------------------------------------------------------

    @app.get("/admin/alerts")
    async def alerts_page(request: Request):
        session = require(request, "viewer")
        hooks = store.list_webhooks() if session.user.can("admin") else []
        return page(
            request,
            "alerts.html",
            session,
            notifications=store.list_notifications(200),
            rules=ctx.alerts.rules,
            webhooks=[(h, _mask_url(h.url)) for h in hooks],
            webhook_kinds=WEBHOOK_KINDS,
            levels=LEVELS,
        )

    @app.post("/admin/alerts/read")
    async def alerts_read(request: Request):
        await require_post(request, "operator")  # read state is shared: a viewer mustn't clear it for everyone
        store.mark_notifications_read()
        return back("/admin/alerts", "read")

    @app.post("/admin/alerts/rules/{kind}")
    async def alerts_rule(kind: str, request: Request):
        session, form = await require_post(request, "admin")
        spec = SPECS.get(kind)
        if spec is None:
            return back("/admin/alerts#rules", error="invalid")
        try:
            threshold = float(form["threshold"]) if spec.threshold is not None else None
            window_s = int(form["window_s"]) if spec.window_s is not None else None
            cooldown_s = int(form.get("cooldown_s") or spec.cooldown_s)
        except (KeyError, ValueError):
            return back("/admin/alerts#rules", error="invalid")
        bad_threshold = threshold is not None and (not math.isfinite(threshold) or threshold < 0)
        if bad_threshold or (window_s is not None and window_s < 0) or cooldown_s < 0:
            return back("/admin/alerts#rules", error="invalid")
        store.save_alert_rule(kind, form.get("enabled") == "on", threshold, window_s, cooldown_s, session.user.username)
        ctx.alerts.reload()
        return back("/admin/alerts#rules", "saved")

    def webhook_fields(form: dict, creating: bool) -> dict:
        kind = str(form.get("kind", "")).strip()
        level = str(form.get("min_level", "warning")).strip()
        url = str(form.get("url", "")).strip()
        if kind not in WEBHOOK_KINDS or level not in LEVELS:
            raise ValueError("kind/level")
        fields: dict = {"kind": kind, "min_level": level}
        if url or creating:
            if not WEBHOOK_URL.fullmatch(url):
                raise ValueError("url")
            fields["url"] = url
        secret = str(form.get("secret", "")).strip()
        if secret or form.get("clear_secret") == "on":
            fields["secret"] = secret or None
        return fields

    @app.post("/admin/alerts/webhooks")
    async def webhooks_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        try:
            if not CLIENT_NAME.fullmatch(name):
                raise ValueError("name")
            fields = webhook_fields(form, creating=True)
            store.create_webhook(
                name, fields["kind"], fields["url"], fields.get("secret"), fields["min_level"], session.user.username
            )
        except ValueError:
            return back("/admin/alerts#webhooks", error="invalid")
        except sqlite3.IntegrityError:
            return back("/admin/alerts#webhooks", error="exists")
        return back("/admin/alerts#webhooks", "created")

    @app.post("/admin/alerts/webhooks/{webhook_id}")
    async def webhooks_update(webhook_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields = webhook_fields(form, creating=False)
            store.update_webhook(webhook_id, session.user.username, enabled=form.get("enabled") == "on", **fields)
        except (ValueError, KeyError):
            return back("/admin/alerts#webhooks", error="invalid")
        return back("/admin/alerts#webhooks", "saved")

    @app.post("/admin/alerts/webhooks/{webhook_id}/delete")
    async def webhooks_delete(webhook_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_webhook(webhook_id, session.user.username)
        return back("/admin/alerts#webhooks", "deleted")

    @app.post("/admin/alerts/webhooks/{webhook_id}/test")
    async def webhooks_test(webhook_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        hook = next((h for h in store.list_webhooks() if h.id == webhook_id), None)
        if hook is None:
            return back("/admin/alerts#webhooks", error="invalid")
        note = Note("warning", "test", "Test notification", f"Sent from the admin by {session.user.username}.")
        error = await ctx.alerts.send(hook, note, attempts=1)
        store.audit(session.user.username, "webhook.test", hook.name, {"ok": error is None})
        return back(
            "/admin/alerts#webhooks", "webhook_ok" if error is None else None, "webhook_failed" if error else None
        )

    # -- backup and restore ----------------------------------------------------------------

    def backup_page(request: Request, session: AdminSession, **context) -> HTMLResponse:
        return page(
            request,
            "backup.html",
            session,
            files=list_backups(bootstrap.data_dir),
            keep_days=store.get_tunables().get("backup_keep_days", Settings().backup_keep_days),
            env_key=bool(bootstrap.secret_key),
            **context,
        )

    @app.get("/admin/backup")
    async def backup(request: Request):
        session = require(request, "owner")
        return backup_page(request, session)

    @app.post("/admin/backup/download")
    async def backup_download(request: Request):
        session, form = await require_post(request, "owner")
        include_key = form.get("include_key") == "on"
        data = await asyncio.to_thread(make_archive, store, bootstrap.data_dir, include_key, __version__)
        store.audit(session.user.username, "backup.download", "", {"includes_key": include_key})
        stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        return Response(
            data,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="queuerouter-backup-{stamp}.zip"'},
        )

    @app.get("/admin/backup/files/{name}")
    async def backup_file(name: str, request: Request):
        session = require(request, "owner")
        path = backup_path(bootstrap.data_dir, name)
        if path is None:
            return back("/admin/backup", error="backup_missing")
        store.audit(session.user.username, "backup.file_download", name)
        return FileResponse(path, media_type="application/vnd.sqlite3", filename=name)

    async def staged(request: Request, session: AdminSession, path: str, source: str) -> HTMLResponse:
        try:
            summary = await asyncio.to_thread(
                stage_restore, bootstrap.data_dir, path, store.fernet, bool(bootstrap.secret_key)
            )
        except RestoreError as exc:
            return backup_page(request, session, error=str(exc))
        store.audit(session.user.username, "backup.restore", source, summary)
        asyncio.get_running_loop().call_later(1.5, ctx.restart)  # after this page has been sent
        return page(request, "backup_restarting.html", session, summary=summary)

    @app.post("/admin/backup/files/{name}/restore")
    async def backup_file_restore(name: str, request: Request):
        session, _ = await require_post(request, "owner")
        path = backup_path(bootstrap.data_dir, name)
        if path is None:
            return back("/admin/backup", error="backup_missing")
        return await staged(request, session, path, name)

    @app.post("/admin/backup/restore")
    async def backup_restore(request: Request):
        session = require(request, "owner")
        async with request.form(max_files=1, max_part_size=MAX_UPLOAD_BYTES) as form:
            if not csrf_ok(session, str(form.get("csrf", ""))):
                raise NotAllowed("csrf")
            upload = form.get("file")
            if upload is None or isinstance(upload, str) or not upload.filename:
                return backup_page(request, session, error="Choose a backup file (.zip or .db) to restore.")
            fd, path = tempfile.mkstemp(dir=bootstrap.data_dir, prefix=".upload-")
            try:
                size = 0
                with os.fdopen(fd, "wb") as out:
                    while chunk := await upload.read(1024 * 1024):
                        size += len(chunk)
                        if size > MAX_UPLOAD_BYTES:
                            return backup_page(request, session, error="That file is too large.")
                        out.write(chunk)
                return await staged(request, session, path, upload.filename)
            finally:
                os.remove(path)

    # -- jobs ------------------------------------------------------------------------------

    @app.get("/admin/jobs")
    async def jobs_page(request: Request):
        session = require(request, "viewer")
        q = request.query_params
        status, client, batch = q.get("status") or None, q.get("client") or None, q.get("batch") or None
        rows = await asyncio.to_thread(store.list_jobs, client, status, None, batch, 300)
        batches = await asyncio.to_thread(store.query, "SELECT * FROM batches ORDER BY created DESC LIMIT 30")
        return page(
            request,
            "jobs.html",
            session,
            jobs=[jobs_mod.job_view(store, r, with_result=False) | {"client": r["client"]} for r in rows],
            batches=[jobs_mod.batch_view(store, b) | {"client": b["client"]} for b in batches],
            statuses=("queued", "running", "succeeded", "failed", "cancelled", "expired"),
            schedules=[jobs_mod.schedule_view(store, s) | {"client": s["client"]} for s in store.list_schedules()],
            clients=[c.name for c in store.list_clients()],
            status=status or "",
            client=client or "",
            batch=batch or "",
            now=time.time(),
        )

    @app.post("/admin/jobs/{job_id}/cancel")
    async def jobs_cancel_admin(job_id: str, request: Request):
        session, _ = await require_post(request, "operator")
        row = store.get_job(job_id)
        if row is not None and store.cancel_jobs([job_id]):
            store.audit(session.user.username, "job.cancel", job_id)
            if row["batch_id"]:
                jobs_mod.finish_batch_if_done(store, row["batch_id"])
        return back("/admin/jobs", "saved")

    @app.post("/admin/jobs/{job_id}/retry")
    async def jobs_retry_admin(job_id: str, request: Request):
        session, _ = await require_post(request, "operator")
        row = store.get_job(job_id)
        if row is None or row["batch_id"] or not store.retry_job(job_id):
            return back("/admin/jobs", error="invalid")
        store.audit(session.user.username, "job.retry", job_id)
        return back("/admin/jobs", "saved")

    @app.post("/admin/schedules")
    async def schedules_create_admin(request: Request):
        session, form = await require_post(request, "admin")
        client = str(form.get("client", ""))
        if client not in {c.name for c in store.list_clients()}:
            return back("/admin/jobs#schedules", error="invalid")
        try:
            body = {
                "name": str(form.get("name", "")),
                "cron": str(form.get("cron", "")),
                "endpoint": str(form.get("endpoint") or "/v1/chat/completions"),
                "request": json.loads(str(form.get("request", ""))),
                "priority": str(form.get("priority", "low")),
                "idle_only": form.get("idle_only") == "on",
                "tags": str(form.get("tags", "")),
            }
            schedule = jobs_mod.new_schedule(client, body)
        except (ValueError, jobs_mod.JobError):
            return back("/admin/jobs#schedules", error="schedule_invalid")
        store.create_schedule(schedule)
        store.audit(
            session.user.username, "schedule.create", schedule["name"], {"client": client, "cron": body["cron"]}
        )
        return back("/admin/jobs#schedules", "created")

    @app.post("/admin/schedules/{schedule_id}/toggle")
    async def schedules_toggle_admin(schedule_id: str, request: Request):
        session, _ = await require_post(request, "operator")
        row = store.get_schedule(schedule_id)
        if row is None:
            return back("/admin/jobs#schedules", error="invalid")
        enabled = not row["enabled"]
        fields: dict = {"enabled": enabled}
        if enabled:
            fields["next_run"] = cron_mod.parse(row["cron"]).next_after(time.time())
        store.set_schedule(schedule_id, **fields)
        store.audit(session.user.username, "schedule.toggle", row["name"], {"enabled": enabled})
        return back("/admin/jobs#schedules", "saved")

    @app.post("/admin/schedules/{schedule_id}/run")
    async def schedules_run_admin(schedule_id: str, request: Request):
        session, _ = await require_post(request, "operator")
        row = store.get_schedule(schedule_id)
        if row is None:
            return back("/admin/jobs#schedules", error="invalid")
        jobs_mod.fire_schedule(store, row, time.time(), manual=True)
        store.audit(session.user.username, "schedule.run", row["name"])
        return back("/admin/jobs", "saved")

    @app.post("/admin/schedules/{schedule_id}/delete")
    async def schedules_delete_admin(schedule_id: str, request: Request):
        session, _ = await require_post(request, "admin")
        row = store.get_schedule(schedule_id)
        if row is not None:
            store.delete_schedule(schedule_id)
            store.audit(session.user.username, "schedule.delete", row["name"])
        return back("/admin/jobs#schedules", "deleted")

    @app.post("/admin/batches/{batch_id}/cancel")
    async def batches_cancel_admin(batch_id: str, request: Request):
        session, _ = await require_post(request, "operator")
        batch = store.get_batch(batch_id)
        if batch is None or batch["status"] != "in_progress":
            return back("/admin/jobs", error="invalid")
        store.update_batch(batch_id, status="cancelling", cancelled_at=time.time())
        store.cancel_jobs(None, batch_id)
        jobs_mod.finish_batch_if_done(store, batch_id)
        store.audit(session.user.username, "batch.cancel", batch_id)
        return back("/admin/jobs", "saved")

    # -- audit ---------------------------------------------------------------------------

    @app.get("/admin/audit")
    async def audit(request: Request):
        session = require(request, "admin")
        return page(request, "audit.html", session, rows=store.list_audit(300))


CLIENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
GROUP_NAME = re.compile(r"[^\x00-\x1f\x7f]{1,64}")  # shown on pages only: any text
ALIAS_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")
LABEL_MAX = 64  # characters of an alias's display name
URL = re.compile(r"https?://[^\s/?#]+(/[^\s?#]*)?")
WEBHOOK_URL = re.compile(r"https?://[^\s/?#]+\S*")


def _parse_date(value: str) -> float | None:
    """``YYYY-MM-DD`` (end of that day, UTC) -> timestamp; empty -> None (never expires)."""
    value = value.strip()
    if not value:
        return None
    day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return (day + timedelta(days=1)).timestamp()


class RulesError(ValueError):
    pass


class AliasLoop(ValueError):
    """Aliases that would use each other in a loop: the message names them."""


class NotAllowed(Exception):
    """A missing role (or a bad form token, "csrf"). Not the built-in PermissionError: that is also what a file
    that can't be written raises, which then showed as "needs the '[Errno 13] ...' role" (on /v1 routes too)."""


def _rules_text(form: dict) -> str:
    """Parameter rules from a form, validated and re-serialized compactly ('' = none)."""
    text = str(form.get("params", "")).strip()
    try:
        rules = parse_rules(text)
    except ValueError as exc:
        raise RulesError(str(exc)) from exc
    return json.dumps(rules) if rules else ""


def _opt(form: dict, name: str, kind: type, minimum: float):
    """Optional number from a form: blank -> None; otherwise must be >= minimum."""
    raw = str(form.get(name, "")).strip()
    if raw == "":
        return None
    value = kind(raw)
    if not math.isfinite(value) or value < minimum:  # "nan" and "inf" parse as floats
        raise ValueError(name)
    return value


def _expiry_date(timestamp: float | None) -> str:
    """Inverse of ``_parse_date`` for <input type=date> values."""
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp - 1, UTC).strftime("%Y-%m-%d")


def _items(form: dict, name: str) -> list[str]:
    """A comma- (or line-) separated list from a form."""
    return [item.strip() for item in str(form.get(name, "")).replace("\n", ",").split(",") if item.strip()]


def _limits(form: dict) -> dict:
    """The limits a client or a group sets (config.GROUP_LIMITS). Blank = not set: for a client the group's, for a
    group none (or the global default)."""
    on_limit, loop_action = str(form.get("on_limit", "")), str(form.get("loop_action", ""))
    if on_limit not in ("", *ON_LIMIT) or loop_action not in ("", "warn", "reject"):
        raise ValueError("on_limit")
    return {
        "rpm": _opt(form, "rpm", int, 1),
        "rpd": _opt(form, "rpd", int, 1),
        "tpm": _opt(form, "tpm", int, 1),
        "tpd": _opt(form, "tpd", int, 1),
        "on_limit": on_limit,
        "max_queued": _opt(form, "max_queued", int, 0),
        "max_active": _opt(form, "max_active", int, 1),
        "queue_timeout_s": _opt(form, "queue_timeout_s", float, 1),
        "total_timeout_s": _opt(form, "total_timeout_s", float, 1),
        "max_retries": _opt(form, "max_retries", int, -1),
        "params": _rules_text(form),
        "run_max_tokens": _opt(form, "run_max_tokens", int, 1),
        "run_max_requests": _opt(form, "run_max_requests", int, 1),
        "run_max_minutes": _opt(form, "run_max_minutes", float, 0.1),
        "loop_threshold": _opt(form, "loop_threshold", int, 2),
        "loop_action": loop_action,
        "month_requests": _opt(form, "month_requests", int, 1),
        "month_tokens": _opt(form, "month_tokens", int, 1),
        "month_cost_usd": _opt(form, "month_cost_usd", float, 0.01),
    }


def _own_limits(policy: ClientPolicy) -> bool:
    """Whether a client or group sets any limit itself."""
    return any(getattr(policy, name) not in (None, "") for name in GROUP_LIMITS)


def _group_field(form: dict, store: Store) -> int | None:
    """The group picked in a client form (None: none picked, the first group); ValueError for one that doesn't exist."""
    raw = str(form.get("group_id", "")).strip()
    if not raw:
        return None
    group_id = int(raw)
    if group_id not in {g.id for g in store.list_groups()}:
        raise ValueError("group")
    return group_id


def _user_agents(form: dict) -> tuple[str, ...]:
    """A client's allowed apps: at most 20, refused rather than silently cut."""
    prefixes = tuple(p.strip() for p in str(form.get("user_agents", "")).split(",") if p.strip())
    if len(prefixes) > 20:
        raise ValueError("user_agents")
    return prefixes


def _csv_cell(value):
    """Text a spreadsheet would run as a formula (=, +, -, @ ...) starts with a quote instead; client and model
    names come from requests."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value


def _safe_next(value: str | None) -> str:
    """Only allow redirects to local admin pages."""
    if value and value.startswith("/admin") and not value.startswith("//") and "\\" not in value:
        return value
    return "/admin"


SECURITY_HEADERS = [
    (
        b"content-security-policy",
        b"default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; form-action 'self'; base-uri 'none'",
    ),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"same-origin"),
    (b"cache-control", b"no-store"),
]


class AdminGuard:
    """Pure ASGI middleware for /admin: IP allow-list and security headers.

    (Not BaseHTTPMiddleware, which would wrap the streaming /v1 responses too.)
    """

    def __init__(self, app, bootstrap: Bootstrap) -> None:
        self.app = app
        self.bootstrap = bootstrap

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith("/admin"):
            return await self.app(scope, receive, send)
        if not admin_ip_allowed(Request(scope), self.bootstrap):
            return await Response(status_code=404)(scope, receive, send)

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                names = {name for name, _ in SECURITY_HEADERS}
                message["headers"] = [h for h in message.get("headers", []) if h[0] not in names] + SECURITY_HEADERS
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _mask_url(url: str) -> str:
    """Webhook URLs often embed a token: show only scheme and host."""
    scheme, _, rest = url.partition("://")
    host, _, path = rest.partition("/")
    return f"{scheme}://{host}/" + ("…" if path else "")


def _pretty(text: str) -> str:
    try:
        return json.dumps(json.loads(text), indent=2, ensure_ascii=False)
    except ValueError:
        return text


def _ua_fields(form: dict) -> dict:
    """Outgoing User-Agent settings from a form (client Limits or an upstream)."""
    mode = str(form.get("ua_mode", "keep"))
    if mode not in UA_MODES:
        raise ValueError("ua_mode")
    template = " ".join(str(form.get("ua_template", "")).split())[:500]
    if mode == "template" and not template:
        raise ValueError("ua_template")
    return {"ua_mode": mode, "ua_template": template}


def _from_json(text: str | None) -> dict:
    try:
        value = json.loads(text or "{}")
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}
