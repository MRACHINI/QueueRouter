"""Alerts: rules watch the queue, upstreams and clients; what fires becomes an in-app notification
and is sent to webhooks (Discord, Slack, ntfy or signed JSON).

Rules have defaults here; the admin's changes are stored per kind (``alert_rules``). Each rule has
a cooldown per subject (an upstream, a client...) so a flapping condition can't flood a channel.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import hmac
import json
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from .store import Store, Webhook

log = logging.getLogger("queuerouter")

LEVELS = ("info", "warning", "error")
WEBHOOK_KINDS = ("discord", "slack", "ntfy", "json")
RETRY_DELAYS_S = (2.0, 10.0)  # between the 3 delivery attempts
SEND_TIMEOUT_S = 10.0
SLOW_CHECK_S = 600  # key expiry and daily tokens
ERROR_RATE_MIN_REQUESTS = 10


@dataclass(frozen=True)
class RuleSpec:
    kind: str
    label: str
    help: str
    level: str
    enabled: bool
    cooldown_s: int
    threshold: float | None = None  # None: the rule has no threshold
    unit: str = ""
    window_s: int | None = None  # None: the rule has no window
    window_label: str = ""


SPECS: dict[str, RuleSpec] = {
    s.kind: s
    for s in (
        RuleSpec("started", "QueueRouter started", "After every (re)start, with the version.", "info", True, 0),
        RuleSpec(
            "circuit",
            "Upstream down / recovered",
            "An upstream keeps failing and is taken out of rotation; another notice when it recovers.",
            "error",
            True,
            300,
        ),
        RuleSpec(
            "queue_length",
            "Queue backed up",
            "Many requests waiting for a slot, for a while.",
            "warning",
            True,
            900,
            10,
            "requests waiting",
            60,
            "for at least (s)",
        ),
        RuleSpec(
            "oldest_wait",
            "Long wait",
            "The oldest queued request has waited this long.",
            "warning",
            True,
            900,
            300,
            "seconds",
        ),
        RuleSpec(
            "error_rate",
            "High error rate",
            f"Share of failed requests (errors, timeouts) over the window, once there are at least "
            f"{ERROR_RATE_MIN_REQUESTS}.",
            "warning",
            True,
            1800,
            25,
            "% failed",
            600,
            "over the last (s)",
        ),
        RuleSpec(
            "client_limited",
            "Client hit its rate limit",
            "A client's requests are delayed or rejected by its requests/tokens limits.",
            "info",
            True,
            3600,
        ),
        RuleSpec(
            "key_expiring",
            "Client key expiring",
            "A client key expires within this many days.",
            "warning",
            True,
            86_400,
            7,
            "days",
        ),
        RuleSpec(
            "backup_failed",
            "Automatic backup failed",
            "The daily database copy couldn't be written (disk full, permissions...).",
            "error",
            True,
            86_400,
        ),
        RuleSpec(
            "daily_tokens",
            "Daily tokens",
            "All clients together used more tokens than this today (UTC).",
            "warning",
            False,
            86_400,
            5_000_000,
            "tokens",
        ),
    )
}


@dataclass(frozen=True)
class Rule:
    spec: RuleSpec
    enabled: bool
    threshold: float | None
    window_s: int | None
    cooldown_s: int


@dataclass(frozen=True)
class Note:
    level: str
    kind: str
    title: str
    body: str = ""
    ts: float = dataclasses.field(default_factory=time.time)


def load_rules(store: Store) -> dict[str, Rule]:
    overrides = store.alert_rule_overrides()
    rules = {}
    for kind, spec in SPECS.items():
        row = overrides.get(kind)
        rules[kind] = Rule(
            spec,
            bool(row["enabled"]) if row else spec.enabled,
            (row["threshold"] if row and row["threshold"] is not None else spec.threshold)
            if spec.threshold is not None
            else None,
            (row["window_s"] if row and row["window_s"] is not None else spec.window_s)
            if spec.window_s is not None
            else None,
            row["cooldown_s"] if row and row["cooldown_s"] is not None else spec.cooldown_s,
        )
    return rules


def _ascii(text: str) -> str:
    """HTTP header values must be ASCII-safe."""
    return text.encode("ascii", "replace").decode()[:250]


def webhook_request(hook: Webhook, note: Note) -> tuple[bytes, dict[str, str]]:
    """Body and headers for one webhook kind."""
    icon = {"info": "ℹ️", "warning": "⚠️", "error": "🚨"}[note.level]
    text = f"{icon} **{note.title}**" + (f"\n{note.body}" if note.body else "")
    if hook.kind == "discord":
        return json.dumps({"username": "QueueRouter", "content": text[:2000]}).encode(), {
            "content-type": "application/json"
        }
    if hook.kind == "slack":
        slack = f"{icon} *{note.title}*" + (f"\n{note.body}" if note.body else "")
        return json.dumps({"text": slack}).encode(), {"content-type": "application/json"}
    if hook.kind == "ntfy":
        headers = {
            "title": _ascii(f"QueueRouter: {note.title}"),
            "priority": {"info": "default", "warning": "high", "error": "urgent"}[note.level],
            "tags": {"info": "information_source", "warning": "warning", "error": "rotating_light"}[note.level],
        }
        return (note.body or note.title).encode(), headers
    body = json.dumps(
        {
            "source": "queuerouter",
            "kind": note.kind,
            "level": note.level,
            "title": note.title,
            "body": note.body,
            "ts": note.ts,
        }
    ).encode()
    headers = {"content-type": "application/json"}
    if hook.secret:
        digest = hmac.new(hook.secret.encode(), body, hashlib.sha256).hexdigest()
        headers["x-queuerouter-signature"] = f"sha256={digest}"
    return body, headers


class Alerts:
    def __init__(self, store: Store, http: Callable[[], httpx.AsyncClient]) -> None:
        self.store = store
        self._http = http
        self.rules = load_rules(store)
        self._last: dict[str, float] = {}  # "kind:subject" -> last fired (cooldowns)
        self._since: dict[str, float] = {}  # sustained conditions: when they started
        self._outcomes: deque[tuple[float, bool]] = deque(maxlen=20_000)  # (finished, failed)
        self._down: set[str] = set()  # upstreams we announced as down
        self._tasks: set[asyncio.Task] = set()
        self._last_slow = 0.0

    def reload(self) -> None:
        self.rules = load_rules(self.store)

    # -- firing ----------------------------------------------------------------------------

    def fire(self, kind: str, subject: str, title: str, body: str = "", level: str | None = None) -> bool:
        """Record and send an alert unless its rule is off or it fired for ``subject`` recently."""
        rule = self.rules[kind]
        if not rule.enabled:
            return False
        now = time.time()
        key = f"{kind}:{subject}"
        if now - self._last.get(key, float("-inf")) < rule.cooldown_s:
            return False
        self._last[key] = now
        self.notify(Note(level or rule.spec.level, kind, title, body, now))
        return True

    def notify(self, note: Note) -> None:
        log.warning(json.dumps({"event": "alert", "kind": note.kind, "level": note.level, "title": note.title}))
        try:
            self.store.add_notification(note.level, note.kind, note.title, note.body)
        except Exception as exc:  # alerts must never break serving
            log.error(json.dumps({"event": "notification_write_failed", "error": repr(exc)[:200]}))
        self._spawn(self.deliver(note))

    def _spawn(self, coro) -> None:
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:  # no loop (e.g. called from a worker thread): skip sending
            coro.close()
            return
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def deliver(self, note: Note) -> None:
        hooks = await asyncio.to_thread(self.store.list_webhooks)
        wanted = [h for h in hooks if h.enabled and LEVELS.index(note.level) >= LEVELS.index(h.min_level or "warning")]
        await asyncio.gather(*(self.send(h, note) for h in wanted))

    async def send(self, hook: Webhook, note: Note, attempts: int = 3) -> str | None:
        """Deliver to one webhook with retries; returns the error, or None when delivered."""
        body, headers = webhook_request(hook, note)
        error: str | None = None
        for attempt in range(attempts):
            try:
                resp = await self._http().post(hook.url, content=body, headers=headers, timeout=SEND_TIMEOUT_S)
                if resp.status_code < 300:
                    error = None
                    break
                error = f"HTTP {resp.status_code}: {resp.text[:120]}"
            except httpx.HTTPError as exc:
                error = f"{type(exc).__name__}: {str(exc)[:120]}"
            if attempt < attempts - 1:
                await asyncio.sleep(RETRY_DELAYS_S[min(attempt, len(RETRY_DELAYS_S) - 1)])
        try:
            await asyncio.to_thread(self.store.webhook_result, hook.id, error is None, error)
        except Exception:  # the webhook may have been deleted meanwhile
            pass
        if error:
            log.warning(json.dumps({"event": "webhook_failed", "webhook": hook.name, "error": error}))
        return error

    async def drain(self) -> None:
        """Wait for deliveries in flight (shutdown, tests)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -- inputs ----------------------------------------------------------------------------

    def started(self, version: str) -> None:
        self.fire("started", "", f"QueueRouter {version} started")

    def circuit(self, upstream: str, state: str, reason: str) -> None:
        if state == "open":
            if self.fire("circuit", upstream, f"Upstream {upstream} is down", f"Taken out of rotation: {reason}"):
                self._down.add(upstream)
        elif state == "closed" and upstream in self._down:
            self._down.discard(upstream)
            self.notify(Note("info", "circuit", f"Upstream {upstream} recovered", "Back in rotation."))

    def client_limited(self, client: str, rejected: bool) -> None:
        how = "rejected (429)" if rejected else "delayed in the queue"
        self.fire(
            "client_limited",
            client,
            f"Client {client} hit its rate limit",
            f"Its requests are {how} until the limit window frees up.",
        )

    def observe(self, entry: dict) -> None:
        """Every finished request (for the error rate)."""
        outcome = entry.get("outcome")
        if outcome in ("done", "error", "timeout"):
            self._outcomes.append((entry.get("ts") or time.time(), outcome != "done"))

    def _sustained(self, kind: str, active: bool, window_s: float, now: float) -> bool:
        if not active:
            self._since.pop(kind, None)
            return False
        return now - self._since.setdefault(kind, now) >= window_s

    def check(self, queued: int, oldest_wait_s: float, now: float | None = None) -> None:
        """Queue and error-rate rules (cheap; called about once a second)."""
        now = now or time.time()
        rule = self.rules["queue_length"]
        if rule.enabled and self._sustained("queue_length", queued >= (rule.threshold or 0), rule.window_s or 0, now):
            self.fire(
                "queue_length",
                "",
                f"Queue backed up: {queued} requests waiting",
                f"At least {rule.threshold:g} requests have been waiting for {rule.window_s}s or more.",
            )
        rule = self.rules["oldest_wait"]
        if rule.enabled and oldest_wait_s >= (rule.threshold or 0):
            self.fire(
                "oldest_wait",
                "",
                f"A request has waited {oldest_wait_s:.0f}s in the queue",
                "Requests are queuing faster than the upstreams finish them.",
            )
        rule = self.rules["error_rate"]
        if rule.enabled:
            since = now - (rule.window_s or 600)
            recent = [failed for ts, failed in self._outcomes if ts >= since]
            if len(recent) >= ERROR_RATE_MIN_REQUESTS:
                rate = 100 * sum(recent) / len(recent)
                if rate >= (rule.threshold or 0):
                    self.fire(
                        "error_rate",
                        "",
                        f"{rate:.0f}% of requests failed",
                        f"{sum(recent)} of {len(recent)} requests in the last {rule.window_s}s failed.",
                    )

    async def check_slow(self, now: float | None = None) -> None:
        """Key expiry and daily tokens (database reads; at most every 10 minutes)."""
        now = now or time.time()
        if now - self._last_slow < SLOW_CHECK_S:
            return
        self._last_slow = now
        rule = self.rules["key_expiring"]
        if rule.enabled:
            horizon = now + (rule.threshold or 0) * 86_400
            for client in await asyncio.to_thread(self.store.list_clients):
                if client.enabled and client.expires_at and now < client.expires_at <= horizon:
                    days = (client.expires_at - now) / 86_400
                    self.fire(
                        "key_expiring",
                        client.name,
                        f"Client key {client.name} expires in {days:.1f} days",
                        f"On {time.strftime('%Y-%m-%d', time.gmtime(client.expires_at))} (UTC). "
                        "Extend it on the Clients page.",
                    )
        rule = self.rules["daily_tokens"]
        if rule.enabled:
            day = time.strftime("%Y-%m-%d", time.gmtime(now))
            used = sum((await asyncio.to_thread(self.store.tokens_on, day)).values())
            if used >= (rule.threshold or 0):
                self.fire("daily_tokens", day, f"{used:,} tokens used today", f"Threshold: {rule.threshold:,.0f}.")
