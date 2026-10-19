"""Admin authentication: argon2 passwords, server-side sessions, CSRF tokens, roles."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import Request

from ..auth import client_ip, ip_in
from ..config import Bootstrap
from ..store import AdminUser, Store

COOKIE = "qr_session"
SESSION_ABSOLUTE_S = 7 * 24 * 3600
SESSION_IDLE_S = 12 * 3600
MIN_PASSWORD = 12

_hasher = PasswordHasher()
_DUMMY_HASH = _hasher.hash("timing-equaliser-not-a-real-password")


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password_hash: str | None, password: str) -> bool:
    """Constant-ish time whether or not the user exists (always runs one argon2 verify)."""
    try:
        return _hasher.verify(password_hash or _DUMMY_HASH, password) and password_hash is not None
    except (VerificationError, InvalidHashError):
        return False


def password_problem(password: str, confirm: str | None = None) -> str | None:
    if len(password) < MIN_PASSWORD:
        return f"Password must be at least {MIN_PASSWORD} characters."
    if confirm is not None and password != confirm:
        return "Passwords don't match."
    return None


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass
class AdminSession:
    user: AdminUser
    token_hash: str
    csrf: str


def is_https(request: Request, bootstrap: Bootstrap) -> bool:
    if request.url.scheme == "https":
        return True
    peer = request.client.host if request.client else ""
    return ip_in(peer, bootstrap.trusted_proxies) and request.headers.get("x-forwarded-proto", "") == "https"


def admin_ip_allowed(request: Request, bootstrap: Bootstrap) -> bool:
    if not bootstrap.admin_allowed_ips:
        return True
    return ip_in(client_ip(request, bootstrap.trusted_proxies), bootstrap.admin_allowed_ips)


class Sessions:
    def __init__(self, store: Store, bootstrap: Bootstrap) -> None:
        self.store = store
        self.bootstrap = bootstrap

    def start(self, request: Request, user: AdminUser) -> tuple[str, str]:
        """Create a session; returns (cookie token, csrf token)."""
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        ip = client_ip(request, self.bootstrap.trusted_proxies)
        self.store.create_session(user.id, _token_hash(token), csrf, ip, SESSION_ABSOLUTE_S)
        return token, csrf

    def current(self, request: Request) -> AdminSession | None:
        token = request.cookies.get(COOKIE)
        if not token:
            return None
        token_hash = _token_hash(token)
        row = self.store.get_session(token_hash)
        now = time.time()
        if row is None or row["expires_at"] < now or row["last_seen"] + SESSION_IDLE_S < now:
            if row is not None:
                self.store.delete_session(token_hash)
            return None
        user = self.store.get_admin(user_id=row["user_id"])
        if user is None or not user.enabled:
            return None
        if now - row["last_seen"] > 60:  # don't write on every request
            self.store.touch_session(token_hash)
        return AdminSession(user, token_hash, row["csrf"])

    def end(self, session: AdminSession) -> None:
        self.store.delete_session(session.token_hash)

    def set_cookie(self, response, request: Request, token: str) -> None:
        response.set_cookie(
            COOKIE,
            token,
            max_age=SESSION_ABSOLUTE_S,
            httponly=True,
            secure=is_https(request, self.bootstrap),
            samesite="strict",
            path="/admin",
        )

    @staticmethod
    def clear_cookie(response) -> None:
        response.delete_cookie(COOKIE, path="/admin")


def csrf_ok(session: AdminSession, submitted: str | None) -> bool:
    return bool(submitted) and hmac.compare_digest(session.csrf, submitted)
