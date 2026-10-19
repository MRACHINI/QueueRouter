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
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import __version__
from ..auth import AuthGuard, client_ip
from ..config import ON_LIMIT, PRIORITIES, TUNABLES, Bootstrap, ClientPolicy
from ..presets import ARLIAI_PLANS, PRESETS, plan_for
from ..pricing import PriceCatalog, resolve, summarize_usage
from ..slots import STRATEGIES
from ..state import MODES
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
    "pool_in_use": "That pool is still used by upstreams; move or delete them first.",
    "refresh_failed": "Couldn't download prices from models.dev right now; the previous list is kept.",
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
                    "choices": SETTING_CHOICES.get(name, ()),
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
            )
            store.update_client(client_id, session.user.username, policy=policy)
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
        filters = {k: request.query_params.get(k, "") for k in ("client", "outcome", "model", "upstream")}
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
        return {"target_model": target, "upstream_ids": upstream_ids, "notes": str(form.get("notes", ""))[:300]}

    @app.post("/admin/aliases")
    async def aliases_create(request: Request):
        session, form = await require_post(request, "admin")
        name = str(form.get("name", "")).strip()
        try:
            if not ALIAS_NAME.fullmatch(name):
                raise ValueError("name")
            fields = alias_fields(form)
            store.create_alias(
                name, fields["target_model"], fields["upstream_ids"], session.user.username, notes=fields["notes"]
            )
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

    # -- audit ---------------------------------------------------------------------------

    @app.get("/admin/audit")
    async def audit(request: Request):
        session = require(request, "admin")
        return page(request, "audit.html", session, rows=store.list_audit(300))


CLIENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
ALIAS_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")
URL = re.compile(r"https?://[^\s/?#]+(/[^\s?#]*)?")


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
