"""Client access keys, real client IP, and auth-failure throttling."""

from __future__ import annotations

import hmac
import ipaddress
import time
from collections import deque

from fastapi import Request

from .config import ClientKey, Settings


def client_ip(request: Request, settings: Settings) -> str:
    """Peer address, or the right-most untrusted X-Forwarded-For hop when behind our proxy."""
    peer = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded or not _is_trusted(peer, settings):
        return peer
    hops = [h.strip() for h in forwarded.split(",") if h.strip()]
    for hop in reversed(hops):
        if not _is_trusted(hop, settings):
            return hop
    return hops[0] if hops else peer


def _is_trusted(ip: str, settings: Settings) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in settings.trusted_proxies)


def presented_key(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.headers.get("x-api-key")


ANONYMOUS = ClientKey("anonymous", "", "normal")


def find_client(key: str | None, clients: tuple[ClientKey, ...], allow_no_auth: bool) -> ClientKey | None:
    """The client owning ``key``; ANONYMOUS when auth is disabled; None if not authorised."""
    if allow_no_auth and not clients:
        return ANONYMOUS
    if not key:
        return None
    # Compare against every key so timing doesn't reveal which one matched.
    found = None
    for client in clients:
        if hmac.compare_digest(key.encode(), client.key.encode()):
            found = client
    return found


class AuthGuard:
    """Blocks an IP for a while after too many failed authentication attempts."""

    def __init__(self, limit_per_minute: int, block_s: float) -> None:
        self.limit = limit_per_minute
        self.block_s = block_s
        self._failures: dict[str, deque[float]] = {}
        self._blocked_until: dict[str, float] = {}

    def is_blocked(self, ip: str) -> float:
        """Seconds remaining on a block, or 0."""
        until = self._blocked_until.get(ip, 0)
        remaining = until - time.monotonic()
        if remaining <= 0:
            self._blocked_until.pop(ip, None)
            return 0
        return remaining

    def fail(self, ip: str) -> None:
        now = time.monotonic()
        window = self._failures.setdefault(ip, deque())
        window.append(now)
        while window and window[0] < now - 60:
            window.popleft()
        if len(window) >= self.limit:
            self._blocked_until[ip] = now + self.block_s
            window.clear()
        if len(self._failures) > 10_000:  # bound memory under a spray of source IPs
            self._failures = {k: v for k, v in self._failures.items() if v and v[-1] > now - 60}
