"""Client access keys, real client IP, and auth-failure throttling."""

from __future__ import annotations

import ipaddress
import json
import logging
import math
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field

from fastapi import Request

from .config import ClientKey, Network
from .store import hash_key

log = logging.getLogger("queuerouter")


def client_ip(request: Request, trusted_proxies: Iterable[Network]) -> str:
    """Peer address, or the right-most untrusted X-Forwarded-For hop when behind our proxy."""
    trusted = tuple(trusted_proxies)
    peer = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded:
        return peer
    if not ip_in(peer, trusted):
        _warn_untrusted(peer)
        return peer
    hops = [h.strip() for h in forwarded.split(",") if h.strip()]
    for hop in reversed(hops):
        if not ip_in(hop, trusted):
            return hop
    return hops[0] if hops else peer


_warned: set[str] = set()


def _warn_untrusted(peer: str) -> None:
    """Once per address (a few at most): a reverse proxy left out of TRUSTED_PROXIES makes every client look
    like the proxy, which is easy to miss."""
    if peer in _warned or len(_warned) >= 10:
        return
    _warned.add(peer)
    log.warning(
        json.dumps(
            {
                "event": "untrusted_proxy",
                "peer": peer,
                "message": "X-Forwarded-For from an address not in TRUSTED_PROXIES was ignored; if this is your "
                "reverse proxy, add its address or network to TRUSTED_PROXIES",
            }
        )
    )


def ip_in(ip: str, networks: Iterable[Network]) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in networks)


def presented_key(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.headers.get("x-api-key")


ANONYMOUS = ClientKey("anonymous", "", "normal")


def find_client(
    key: str | None, clients: dict[str, ClientKey], allow_no_auth: bool, any_clients: bool | None = None
) -> ClientKey | None:
    """The client owning ``key``; ANONYMOUS when auth is disabled; None if not authorised.

    "Allow without key" holds only while no client exists at all (``any_clients``: disabled and expired
    ones count, so switching every client off doesn't open the door). Keys are looked up by SHA-256 hash,
    so the stored value never equals the secret and the lookup time doesn't depend on how much of a guessed
    key is correct.
    """
    if allow_no_auth and not clients and not any_clients:
        return ANONYMOUS
    if not key:
        return None
    client = clients.get(hash_key(key))
    if client is not None and client.expires_at is not None and client.expires_at <= time.time():
        return None  # expired since the configuration was loaded
    return client


def ignored_proxy(ip: str) -> bool:
    """``ip`` sent X-Forwarded-For that wasn't believed: a reverse proxy left out of TRUSTED_PROXIES."""
    return ip in _warned


def how_long(seconds: float) -> str:
    """A block's length for people: "5 min", "40 s"."""
    return f"{math.ceil(seconds / 60)} min" if seconds >= 60 else f"{math.ceil(seconds)} s"


MAX_APPS = 5  # User-Agents kept per block


@dataclass
class Block:
    """An address turned away after too many failed attempts, and what it tried."""

    until: float  # monotonic
    since: float  # wall clock
    apps: list[str] = field(default_factory=list)  # the User-Agents that failed or were turned away (a few)
    refused: int = 0  # requests turned away since


def _note_app(apps: list[str], app: str) -> None:
    app = app[:120]
    if app and app not in apps and len(apps) < MAX_APPS:
        apps.append(app)


class AuthGuard:
    """Blocks an IP for a while after too many failed authentication attempts."""

    def __init__(self, limit_per_minute: int, block_s: float, what: str = "api") -> None:
        self.limit = limit_per_minute
        self.block_s = block_s
        self.what = what  # in the log line: which door ("api", "admin sign-in")
        self._failures: dict[str, deque[tuple[float, str]]] = {}
        self._blocks: dict[str, Block] = {}

    def is_blocked(self, ip: str) -> float:
        """Seconds remaining on a block, or 0."""
        block = self._blocks.get(ip)
        remaining = block.until - time.monotonic() if block else 0
        if remaining <= 0:
            self._blocks.pop(ip, None)
            return 0
        return remaining

    def refused(self, ip: str, app: str = "") -> None:
        """A request the block turned away."""
        if block := self._blocks.get(ip):
            block.refused += 1
            _note_app(block.apps, app)

    def fail(self, ip: str, app: str = "") -> bool:
        """A failed attempt (from the app with this User-Agent); True when it starts a block."""
        now = time.monotonic()
        window = self._failures.setdefault(ip, deque())
        window.append((now, app))
        while window and window[0][0] < now - 60:
            window.popleft()
        started = len(window) >= self.limit
        if started:
            apps: list[str] = []
            for _, seen in window:
                _note_app(apps, seen)
            self._blocks[ip] = Block(now + self.block_s, time.time(), apps)
            window.clear()
            log.warning(
                json.dumps(
                    {
                        "event": "auth_blocked",
                        "guard": self.what,
                        "ip": ip,
                        "failures": self.limit,
                        "block_s": self.block_s,
                        "apps": apps,
                    }
                )
            )
        if len(self._failures) > 10_000:  # bound memory under a spray of source IPs
            self._failures = {k: v for k, v in self._failures.items() if v and v[-1][0] > now - 60}
            self._blocks = {k: b for k, b in self._blocks.items() if b.until > now}
        return started

    def unblock(self, ip: str) -> bool:
        """Lifts a block, and forgets the failures counted towards the next one."""
        self._failures.pop(ip, None)
        return self._blocks.pop(ip, None) is not None

    def blocks(self) -> list[tuple[str, float, Block]]:
        """(address, seconds left, block) for each block still on, the newest first."""
        now = time.monotonic()
        self._blocks = {ip: b for ip, b in self._blocks.items() if b.until > now}
        return sorted(((ip, b.until - now, b) for ip, b in self._blocks.items()), key=lambda t: -t[2].since)
