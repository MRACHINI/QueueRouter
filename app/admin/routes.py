"""Admin web UI: server-rendered pages, no build step, no third-party JS/CSS."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import secrets
import sqlite3
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import __version__, totp
from ..agents import parse_tags
from ..alerts import LEVELS, SPECS, WEBHOOK_KINDS, Alerts, Note
from ..auth import AuthGuard, client_ip
from ..backup import MAX_UPLOAD_BYTES, RestoreError, backup_path, list_backups, make_archive, stage_restore
from ..config import ON_LIMIT, PRIORITIES, TUNABLES, Bootstrap, ClientPolicy, Settings
from ..fallback import DEFAULT_TRIGGERS, TRIGGERS
from ..opencode import plugin_install_commands
from ..params import parse_rules
from ..presets import ARLIAI_PLANS, PRESETS, plan_for
from ..pricing import PriceCatalog, resolve, summarize_usage
from ..slots import STRATEGIES
from ..state import MODES
from ..store import ROLES, Store
from .charts import usage_charts
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
    "webhook_ok": "Test notification delivered.",
    "webhook_failed": "The test notification could not be delivered; see the error on the webhook.",
    "read": "All notifications marked as read.",
    "2fa_off": "Two-factor sign-in is off.",
    "backup_missing": "That backup file doesn't exist.",
    "2fa_reset": "Two-factor sign-in was reset for that user; they sign in with their password "
    "and can set it up again.",
}

SETTING_CHOICES: dict[str, tuple[str, ...]] = {"lb_strategy": STRATEGIES}

SETTING_HELP: dict[str, tuple[str, str, str]] = {
    # name: (section, label, help)
    "aging_s": ("Scheduling", "Aging (s)", "Waiting this long bumps a request one priority level up."),
    "session_aware": ("Scheduling", "Finish what you started", "Sessions already mid-task go before new ones."),
    "session_window_s": ("Scheduling", "Session window (s)", "How long after a request a session counts as mid-task."),
    "fair_share": ("Scheduling", "Fair share", "Among equals, the client served least recently goes first."),
    "max_queue_wait_s": ("Queue", "Max queue wait (s)", "Give up after waiting this long (504). 0 = wait forever."),
    "keepalive_interval_s": ("Queue", "Keep-alive interval (s)", "SSE comments sent to queued streaming clients."),
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
        "Consecutive failures before an upstream is taken out of rotation.",
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
    "auth_fail_limit": ("Security", "Auth failures per minute", "Before an IP is temporarily blocked."),
    "auth_block_s": ("Security", "Block duration (s)", ""),
}


@dataclass
class AdminContext:
    store: Store
    bootstrap: Bootstrap
    reload: Callable[[], None]
    live_state: Callable[[], dict]
    probe: Callable[[str, str], Awaitable[tuple[bool, str]]]
    upstream_info: Callable[[], list[dict]]
    models_by_upstream: Callable[[], dict[str, list[str]]]
    prices: PriceCatalog
    refresh_prices: Callable[[], Awaitable[bool]]
    playground: Callable[[Request, str, bytes], Awaitable[Response]]
    opencode_config: Callable[[Request, str], Awaitable[dict | None]]
    alerts: Alerts
    restart: Callable[[], None]


class NeedLogin(Exception):
    pass


def mount_admin(app: FastAPI, ctx: AdminContext) -> None:
    store, bootstrap = ctx.store, ctx.bootstrap
    sessions = Sessions(store, bootstrap)
    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    templates.env.filters["expiry_date"] = _expiry_date
    login_guard = AuthGuard(limit_per_minute=5, block_s=300)
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
            raise PermissionError(role)
        return session

    async def require_post(request: Request, role: str) -> tuple[AdminSession, dict]:
        session = require(request, role)
        form = dict(await request.form())
        if not csrf_ok(session, form.get("csrf")):
            raise PermissionError("csrf")
        return session, form

    @app.exception_handler(PermissionError)
    async def forbidden(request: Request, exc: PermissionError):
        session = sessions.current(request)
        reason = (
            "Your session's form token is invalid; reload the page."
            if str(exc) == "csrf"
            else (f"This page needs the '{exc}' role.")
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
        if not secrets.compare_digest(str(form.get("token", "")), setup["token"]):
            login_guard.fail(ip)
            return page(request, "setup.html", None, error="Wrong setup token (see the container log).")
        username = str(form.get("username", "")).strip()
        if not username:
            return page(request, "setup.html", None, error="Choose a username.")
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
        username = str(form.get("username", "")).strip()
        nxt = _safe_next(str(form.get("next", "")))
        ip = client_ip(request, bootstrap.trusted_proxies)
        if login_guard.is_blocked(ip) or login_guard.is_blocked(f"user:{username}"):
            return page(request, "login.html", None, error="Too many attempts. Try again later.", next=nxt)
        user = store.get_admin(username=username)
        hashed = store.password_hash(user.id) if user else None
        if not verify_password(hashed, str(form.get("password", ""))) or not user.enabled:
            login_guard.fail(ip)
            login_guard.fail(f"user:{username}")
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
                    "value": values.get(name),
                    "choices": SETTING_CHOICES.get(name, ()),
                }
            )
        return page(request, "settings.html", session, sections=sections)

    @app.post("/admin/settings")
    async def settings_submit(request: Request):
        session, form = await require_post(request, "admin")
        values: dict[str, object] = {}
        try:
            for name in SETTING_HELP:  # only what the page shows: an absent checkbox isn't "off"
                kind = TUNABLES[name]
                if kind is bool:
                    values[name] = form.get(name) == "on"
                elif kind is str:
                    if name in form:
                        if form[name] not in SETTING_CHOICES.get(name, ()):
                            raise ValueError(name)
                        values[name] = form[name]
                elif name in form and form[name] != "":
                    values[name] = kind(form[name])
                    if values[name] < -1 or (name != "max_retries" and values[name] < 0):
                        raise ValueError(name)
        except ValueError:
            return back("/admin/settings", error="invalid")
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
        if not username or role not in ROLES or password_problem(str(form.get("password", ""))):
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
        counter = totp.verify(secret, str(form.get("code", "")))
        if counter is None:
            return account_2fa_setup_page(request, session, secret, "That code didn't match. Check the phone's clock.")
        pending_setup.pop(session.token_hash, None)
        codes = totp.new_recovery_codes()
        store.set_totp(session.user.id, secret, [totp.hash_recovery(c) for c in codes], session.user.username)
        store.totp_used(session.user.id, counter)
        return page(request, "account_recovery.html", session, codes=codes)

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
        return page(request, "account_recovery.html", session, codes=codes)

    @app.post("/admin/account/2fa/disable")
    async def account_2fa_disable(request: Request):
        session, form = await require_post(request, "viewer")
        if not verify_password(store.password_hash(session.user.id), str(form.get("current", ""))):
            return account_page(request, session, error="Current password is wrong.")
        store.set_totp(session.user.id, None, [], session.user.username)
        return back("/admin/account", "2fa_off")

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
        base = bootstrap.public_url or f"{request.url.scheme}://{request.url.netloc}"
        return page(
            request,
            "client_key.html",
            session,
            name=name,
            key=key,
            regenerated=regenerated,
            base_url=f"{base}/v1",
            site_url=base,
            plugin=plugin_install_commands(base),
        )

    @app.get("/admin/clients")
    async def clients(request: Request):
        session = require(request, "admin")
        live = ctx.live_state().get("clients", {})
        base = bootstrap.public_url or f"{request.url.scheme}://{request.url.netloc}"
        return page(
            request,
            "clients.html",
            session,
            clients=store.list_clients(),
            priorities=PRIORITIES,
            live=live,
            plugin=plugin_install_commands(base),
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
        if not CLIENT_NAME.fullmatch(name) or priority not in PRIORITIES:
            return back("/admin/clients", error="invalid")
        try:
            key = store.create_client(
                name,
                session.user.username,
                priority=priority,
                notes=str(form.get("notes", ""))[:500],
                expires_at=expires_at,
            )
        except sqlite3.IntegrityError:
            return back("/admin/clients", error="exists")
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
            store.update_client(client_id, session.user.username, **fields)
        except (ValueError, KeyError):
            return back("/admin/clients", error="invalid")
        ctx.reload()
        return back("/admin/clients", "saved")

    @app.post("/admin/clients/{client_id}/policy")
    async def clients_policy(client_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            models = tuple(
                m.strip() for m in str(form.get("allowed_models", "")).replace("\n", ",").split(",") if m.strip()
            )
            on_limit = str(form.get("on_limit", "delay"))
            if on_limit not in ON_LIMIT:
                raise ValueError("on_limit")
            policy = ClientPolicy(
                allowed_models=models,
                rpm=_opt(form, "rpm", int, 1),
                rpd=_opt(form, "rpd", int, 1),
                tpm=_opt(form, "tpm", int, 1),
                tpd=_opt(form, "tpd", int, 1),
                on_limit=on_limit,
                max_queued=_opt(form, "max_queued", int, 0),
                max_active=_opt(form, "max_active", int, 1),
                queue_timeout_s=_opt(form, "queue_timeout_s", float, 1),
                total_timeout_s=_opt(form, "total_timeout_s", float, 1),
                max_retries=_opt(form, "max_retries", int, -1),
                params=_rules_text(form),
                tags=",".join(parse_tags(str(form.get("tags", "")))),
                run_max_tokens=_opt(form, "run_max_tokens", int, 1),
                run_max_requests=_opt(form, "run_max_requests", int, 1),
                run_max_minutes=_opt(form, "run_max_minutes", float, 0.1),
                loop_threshold=_opt(form, "loop_threshold", int, 2),
                loop_action="reject" if form.get("loop_action") == "reject" else "warn",
            )
            store.update_client(client_id, session.user.username, policy=policy)
        except RulesError:
            return back("/admin/clients", error="rules_invalid")
        except (ValueError, KeyError):
            return back("/admin/clients", error="invalid")
        ctx.reload()
        return back("/admin/clients", "saved")

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
            arliai_plans=ARLIAI_PLANS,
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
        """max_concurrency / context_cap / plan from the form; an ArliAI plan fills the first two."""
        plan = plan_for("arliai", str(form.get("plan", "")) or None)
        concurrency = int_field(form, "max_concurrency", 1, 1000, plan.max_concurrency if plan else 1)
        context_cap = int_field(form, "context_cap", 1, 10_000_000)
        if plan and not str(form.get("max_concurrency", "")).strip():
            concurrency = plan.max_concurrency
        if plan and context_cap is None:
            context_cap = plan.context_cap
        monthly_price = _opt(form, "monthly_price", float, 0)
        if plan and monthly_price is None:
            monthly_price = plan.monthly_price
        return {
            "plan": f"ArliAI {plan.name}" if plan else (str(form.get("plan_label", "")).strip() or None),
            "max_concurrency": concurrency,
            "context_cap": context_cap,
            "min_spacing_ms": int_field(form, "min_spacing_ms", 0, 600_000, 0),
            "monthly_price": monthly_price,
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
            pool_choice = str(form.get("pool", "new"))
            if pool_choice == "new":
                fields = pool_fields(form)
                if preset.key != "arliai" and not str(form.get("max_concurrency", "")).strip():
                    fields["max_concurrency"] = preset.max_concurrency
                pool_id = store.create_pool(name, fields.pop("max_concurrency"), session.user.username, **fields)
            else:
                pool_id = int(pool_choice)
                if pool_id not in {p.id for p in store.list_pools()}:
                    raise ValueError("pool")
            store.create_upstream(
                name, base_url, api_key, pool_id, session.user.username, preset=preset.key, weight=weight
            )
        except ValueError:
            return back("/admin/upstreams", error="invalid")
        except sqlite3.IntegrityError:
            return back("/admin/upstreams", error="exists")
        ctx.reload()
        return back("/admin/upstreams", "created")

    @app.post("/admin/upstreams/{upstream_id}")
    async def upstreams_update(upstream_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields: dict[str, object] = {
                "enabled": form.get("enabled") == "on",
                "paused": form.get("paused") == "on",
                "weight": int_field(form, "weight", 1, 100, 1),
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
            store.update_upstream(upstream_id, session.user.username, **fields)
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

    @app.get("/admin/requests")
    async def requests_log(request: Request):
        session = require(request, "viewer")
        filters = {k: request.query_params.get(k, "") for k in ("client", "outcome", "model", "upstream", "tag")}
        rows = await asyncio.to_thread(store.list_requests, 300, **filters)
        options = await asyncio.to_thread(store.request_filter_values)
        return page(request, "requests.html", session, rows=rows, filters=filters, options=options)

    async def usage_summary(request: Request):
        days = _clamp_int(request.query_params.get("days"), 30, 1, 366)
        by_model = request.query_params.get("by") == "model"
        rows = await asyncio.to_thread(store.usage, days)
        overrides = await asyncio.to_thread(store.list_model_prices)
        plan_per_month = sum(p.monthly_price or 0 for p in store.list_pools())
        rows, totals, _ = summarize_usage(rows, overrides, ctx.prices, by_model, plan_per_month, days)
        return days, by_model, rows, totals, plan_per_month

    @app.get("/admin/usage")
    async def usage(request: Request):
        session = require(request, "viewer")
        days, by_model, rows, totals, plan_per_month = await usage_summary(request)
        return page(
            request,
            "usage.html",
            session,
            rows=rows,
            days=days,
            by_model=by_model,
            totals=totals,
            plan_per_month=plan_per_month,
            catalog_loaded=bool(ctx.prices.prices),
            charts=usage_charts(rows, days),
        )

    @app.get("/admin/usage.csv")
    async def usage_csv(request: Request):
        require(request, "viewer")
        days, by_model, rows, _, _ = await usage_summary(request)
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
            "cost_usd",
        ]
        writer.writerow(columns)
        for r in rows:
            values = {**r, "cost_usd": f"{r['cost']:.4f}" if r["priced"] else ""}
            writer.writerow([values[c] for c in columns])
        return Response(
            out.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="queuerouter-usage-{days}d.csv"'},
        )

    # -- models and aliases --------------------------------------------------------------

    @app.get("/admin/models")
    async def models_page(request: Request):
        session = require(request, "admin")
        upstream_names = {u.id: u.name for u in store.list_upstreams()}
        models = ctx.models_by_upstream()
        overrides = store.list_model_prices()
        priced_models = sorted({m for ids in models.values() for m in ids} | set(store.used_models()) | set(overrides))
        pricing = [
            {"model": m, "price": resolve(m, overrides, ctx.prices), "override": overrides.get(m)}
            for m in priced_models
        ]
        return page(
            request,
            "models.html",
            session,
            aliases=store.list_aliases(),
            triggers=TRIGGERS,
            default_triggers=DEFAULT_TRIGGERS,
            upstreams=store.list_upstreams(),
            upstream_names=upstream_names,
            models=models,
            pricing=pricing,
            references=ctx.prices.references(),
            catalog_size=len(ctx.prices.prices),
            catalog_age_h=round((time.time() - ctx.prices.fetched_at) / 3600, 1) if ctx.prices.fetched_at else None,
        )

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
            )
        except ValueError:
            return back("/admin/models#pricing", error="invalid")
        return back("/admin/models#pricing", "saved")

    @app.post("/admin/prices/refresh")
    async def prices_refresh(request: Request):
        session, _ = await require_post(request, "admin")
        ok = await ctx.refresh_prices()
        store.audit(session.user.username, "prices.refresh", "models.dev", {"ok": ok})
        return back("/admin/models#pricing", "saved" if ok else None, None if ok else "refresh_failed")

    def alias_fields(form: dict) -> dict:
        target = str(form.get("target_model", "")).strip()
        if not target or len(target) > 200:
            raise ValueError("target_model")
        known = {u.id for u in store.list_upstreams()}
        upstream_ids = tuple(int(v) for k, v in form.items() if k.startswith("upstream_") and int(v) in known)
        params = _rules_text(form)
        fallbacks = tuple(m.strip() for m in str(form.get("fallbacks", "")).replace("\n", ",").split(",") if m.strip())
        if any(len(m) > 200 for m in fallbacks) or len(fallbacks) > 10:
            raise ValueError("fallbacks")
        fallback_on = tuple(t for t in TRIGGERS if form.get(f"on_{t}") == "on")
        return {
            "target_model": target,
            "upstream_ids": upstream_ids,
            "notes": str(form.get("notes", ""))[:300],
            "params": params,
            "fallbacks": fallbacks,
            "fallback_on": fallback_on,
        }

    @app.post("/admin/aliases")
    async def aliases_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        try:
            if not ALIAS_NAME.fullmatch(name):
                raise ValueError("name")
            fields = alias_fields(form)
            store.create_alias(
                name,
                fields["target_model"],
                fields["upstream_ids"],
                session.user.username,
                notes=fields["notes"],
                params=fields["params"],
                fallbacks=fields["fallbacks"],
                fallback_on=fields["fallback_on"],
            )
        except RulesError:
            return back("/admin/models", error="rules_invalid")
        except ValueError:
            return back("/admin/models", error="invalid")
        except sqlite3.IntegrityError:
            return back("/admin/models", error="exists")
        ctx.reload()
        return back("/admin/models", "created")

    @app.post("/admin/aliases/{alias_id}")
    async def aliases_update(alias_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.update_alias(
                alias_id, session.user.username, enabled=form.get("enabled") == "on", **alias_fields(form)
            )
        except RulesError:
            return back("/admin/models", error="rules_invalid")
        except (ValueError, KeyError):
            return back("/admin/models", error="invalid")
        ctx.reload()
        return back("/admin/models", "saved")

    @app.post("/admin/aliases/{alias_id}/delete")
    async def aliases_delete(alias_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_alias(alias_id, session.user.username)
        ctx.reload()
        return back("/admin/models", "deleted")

    # -- playground ----------------------------------------------------------------------

    @app.get("/admin/playground")
    async def playground_page(request: Request):
        session = require(request, "operator")
        model_ids = sorted({m for ids in ctx.models_by_upstream().values() for m in ids})
        aliases = [a.name for a in store.list_aliases() if a.enabled]
        clients = [c for c in store.list_clients() if c.enabled and not c.expired]
        return page(request, "playground.html", session, models=model_ids, aliases=aliases, clients=clients)

    @app.post("/admin/playground/send")
    async def playground_send(request: Request):
        """JSON in (fetch from the page, CSRF token in a header), OpenAI response / SSE out."""
        session = require(request, "operator")
        if not csrf_ok(session, request.headers.get("x-csrf-token")):
            raise PermissionError("csrf")
        try:
            data = json.loads(await request.body())
            client_name = str(data.pop("client"))
        except (ValueError, KeyError, AttributeError):
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
        await require_post(request, "viewer")
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
        if (threshold is not None and threshold < 0) or (window_s is not None and window_s < 0) or cooldown_s < 0:
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
                raise PermissionError("csrf")
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

    # -- audit ---------------------------------------------------------------------------

    @app.get("/admin/audit")
    async def audit(request: Request):
        session = require(request, "admin")
        return page(request, "audit.html", session, rows=store.list_audit(300))


CLIENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
ALIAS_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")
URL = re.compile(r"https?://[^\s/?#]+(/[^\s?#]*)?")
WEBHOOK_URL = re.compile(r"https?://[^\s/?#]+\S*")


def _parse_date(value: str) -> float | None:
    """``YYYY-MM-DD`` (end of that day, UTC) -> timestamp; empty -> None (never expires)."""
    value = value.strip()
    if not value:
        return None
    day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return (day + timedelta(days=1)).timestamp()


def _clamp_int(raw: str | None, default: int, low: int, high: int) -> int:
    try:
        return min(high, max(low, int(raw or default)))
    except ValueError:
        return default


class RulesError(ValueError):
    pass


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
    if value < minimum:
        raise ValueError(name)
    return value


def _expiry_date(timestamp: float | None) -> str:
    """Inverse of ``_parse_date`` for <input type=date> values."""
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp - 1, UTC).strftime("%Y-%m-%d")


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
