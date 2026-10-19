"""Admin web UI: server-rendered pages, no build step, no third-party JS/CSS."""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import __version__
from ..auth import AuthGuard, client_ip
from ..config import PRIORITIES, TUNABLES, Bootstrap
from ..store import ROLES, Store
from .security import (
    AdminSession,
    Sessions,
    admin_ip_allowed,
    csrf_ok,
    hash_password,
    password_problem,
    verify_password,
)

log = logging.getLogger("queuerouter")
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
}

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


class NeedLogin(Exception):
    pass


def mount_admin(app: FastAPI, ctx: AdminContext) -> None:
    store, bootstrap = ctx.store, ctx.bootstrap
    sessions = Sessions(store, bootstrap)
    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    templates.env.filters["expiry_date"] = _expiry_date
    login_guard = AuthGuard(limit_per_minute=5, block_s=300)
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
                **context,
            },
        )

    def back(path: str, message: str | None = None, error: str | None = None) -> RedirectResponse:
        query = f"?m={message}" if message else f"?e={error}" if error else ""
        return RedirectResponse(path + query, status_code=303)

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
        token, _ = sessions.start(request, user)
        store.audit(username, "login", ip)
        response = RedirectResponse(nxt, status_code=303)
        sessions.set_cookie(response, request, token)
        return response

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
                }
            )
        return page(request, "settings.html", session, sections=sections)

    @app.post("/admin/settings")
    async def settings_submit(request: Request):
        session, form = await require_post(request, "admin")
        values: dict[str, object] = {}
        try:
            for name, kind in TUNABLES.items():
                if kind is bool:
                    values[name] = form.get(name) == "on"
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

    @app.get("/admin/account")
    async def account(request: Request):
        session = require(request, "viewer")
        return page(request, "account.html", session)

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

    # -- clients -------------------------------------------------------------------------

    def show_new_key(request: Request, session: AdminSession, name: str, key: str, regenerated: bool):
        base = bootstrap.public_url or f"{request.url.scheme}://{request.url.netloc}"
        return page(
            request, "client_key.html", session, name=name, key=key, regenerated=regenerated, base_url=f"{base}/v1"
        )

    @app.get("/admin/clients")
    async def clients(request: Request):
        session = require(request, "admin")
        live = ctx.live_state().get("clients", {})
        return page(request, "clients.html", session, clients=store.list_clients(), priorities=PRIORITIES, live=live)

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

    # -- audit ---------------------------------------------------------------------------

    @app.get("/admin/audit")
    async def audit(request: Request):
        session = require(request, "admin")
        return page(request, "audit.html", session, rows=store.list_audit(300))


CLIENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def _parse_date(value: str) -> float | None:
    """``YYYY-MM-DD`` (end of that day, UTC) -> timestamp; empty -> None (never expires)."""
    value = value.strip()
    if not value:
        return None
    day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return (day + timedelta(days=1)).timestamp()


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
