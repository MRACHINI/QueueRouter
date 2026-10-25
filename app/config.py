"""Configuration.

Two layers:
- ``Bootstrap``: the few settings that must exist before the database opens (data dir,
  secret key, proxies, logging, first admin). Always from environment variables.
- ``Settings``: runtime knobs. Stored in the database and edited in the admin UI; on the very
  first start they are imported once from the environment (see ``Settings.from_env``).
"""

from __future__ import annotations

import ipaddress
import json
import os
from dataclasses import asdict, dataclass, field, fields
from typing import NamedTuple

PRIORITIES = ("high", "normal", "low")
# Only the machine itself by default: anyone else could otherwise claim any address in X-Forwarded-For
# (and pass ADMIN_ALLOWED_IPS or dodge per-IP limits). Behind a reverse proxy, set it to the proxy's network.
DEFAULT_TRUSTED_PROXIES = "127.0.0.1/32,::1/128"

Network = ipaddress.IPv4Network | ipaddress.IPv6Network


ON_LIMIT = ("delay", "reject")


@dataclass(frozen=True)
class ClientPolicy:
    """Per-client limits. ``None`` / empty = no limit (or the global default)."""

    allowed_models: tuple[str, ...] = ()  # empty = every model
    user_agents: tuple[str, ...] = ()  # allowed apps: User-Agent prefixes (empty = any app)
    ua_mode: str = "keep"  # outgoing User-Agent to providers: keep | remove | template
    ua_template: str = ""  # with ua_mode=template: e.g. "{original} via queuerouter/{version}"
    rpm: int | None = None  # requests per minute
    rpd: int | None = None  # requests per day (rolling 24 h)
    tpm: int | None = None  # tokens per minute
    tpd: int | None = None  # tokens per day
    on_limit: str = "delay"  # delay = wait in the queue until allowed; reject = 429 + Retry-After
    max_queued: int | None = None  # requests waiting at once (beyond -> 429)
    max_active: int | None = None  # requests in flight at once
    queue_timeout_s: float | None = None  # overrides the global max queue wait
    total_timeout_s: float | None = None  # whole request incl. generation
    max_retries: int | None = None  # overrides the global retry budget
    params: str = ""  # parameter rules JSON (app/params.py)
    tags: str = ""  # default tags for this client's requests (comma-separated)
    run_max_tokens: int | None = None  # per agent run (X-Run-Id, else the conversation)
    run_max_requests: int | None = None
    run_max_minutes: float | None = None
    loop_threshold: int | None = None  # the same request this many times within 5 min = a loop
    loop_action: str = "warn"  # warn (log it) | reject (429)
    month_requests: int | None = None  # per calendar month (UTC); beyond -> 429 until the next month
    month_tokens: int | None = None
    month_cost_usd: float | None = None  # at official prices (Usage page)
    log_content: bool = False  # keep prompts and replies (encrypted; admins with content permission)
    log_content_until: float | None = None  # epoch seconds; None = until turned off

    @classmethod
    def from_json(cls, text: str | None) -> ClientPolicy:
        try:
            data = json.loads(text or "{}")
        except ValueError:
            data = {}
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in data.items() if k in known}
        if "allowed_models" in data:
            data["allowed_models"] = tuple(data["allowed_models"] or ())
        if "user_agents" in data:
            data["user_agents"] = tuple(data["user_agents"] or ())
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps({k: (list(v) if isinstance(v, tuple) else v) for k, v in asdict(self).items()})

    def allows_app(self, user_agent: str) -> bool:
        """Whether a request with this User-Agent may use the key (no rule = any app)."""
        return not self.user_agents or any(user_agent.startswith(prefix) for prefix in self.user_agents)

    def logs_content(self, now: float) -> bool:
        return self.log_content and (self.log_content_until is None or self.log_content_until > now)

    @property
    def rate_limited(self) -> bool:
        return any(v is not None for v in (self.rpm, self.rpd, self.tpm, self.tpd))


@dataclass(frozen=True)
class ClientKey:
    """An authenticated caller. The key itself is only ever stored hashed."""

    name: str
    key_hash: str
    priority: str = "normal"
    policy: ClientPolicy = ClientPolicy()
    expires_at: float | None = None  # checked on every request, not only when the configuration reloads


class ParsedKey(NamedTuple):
    name: str
    key: str
    priority: str


def parse_access_keys(entries: tuple[str, ...]) -> tuple[ParsedKey, ...]:
    """Parse ACCESS_KEYS entries: ``key``, ``name:key`` or ``name:key:priority``."""
    parsed = []
    for i, entry in enumerate(entries, 1):
        parts = [p.strip() for p in entry.split(":")]
        if len(parts) == 1:
            name, key, priority = f"client{i}", parts[0], "normal"
        elif len(parts) == 2:
            name, key, priority = parts[0], parts[1], "normal"
        elif len(parts) == 3:
            name, key, priority = parts
        else:
            raise ValueError(f"ACCESS_KEYS entry #{i} must be 'key', 'name:key' or 'name:key:priority'")
        if not name or not key:
            raise ValueError(f"ACCESS_KEYS entry #{i} has an empty name or key")
        if priority not in PRIORITIES:
            raise ValueError(f"ACCESS_KEYS entry #{i}: priority must be one of {', '.join(PRIORITIES)}")
        parsed.append(ParsedKey(name, key, priority))
    names = [p.name for p in parsed]
    if len(set(names)) != len(names):
        raise ValueError("ACCESS_KEYS client names must be unique")
    return tuple(parsed)


def _env(env: dict[str, str], name: str) -> str | None:
    """The variable's value; None when unset or blank (compose files often leave ``NAME=`` empty)."""
    value = env.get(name)
    return value.strip() if value is not None and value.strip() else None


def _int(env: dict[str, str], name: str, default: int) -> int:
    value = _env(env, name)
    return default if value is None else int(value)


def _float(env: dict[str, str], name: str, default: float) -> float:
    value = _env(env, name)
    return default if value is None else float(value)


def _bool(env: dict[str, str], name: str, default: bool) -> bool:
    value = _env(env, name)
    return default if value is None else value.lower() in {"1", "true", "yes", "on"}


def _list(env: dict[str, str], name: str, default: str = "") -> list[str]:
    return [p.strip() for p in (_env(env, name) or default).split(",") if p.strip()]


def _networks(env: dict[str, str], name: str, default: str) -> tuple[Network, ...]:
    return tuple(ipaddress.ip_network(p, strict=False) for p in _list(env, name, default))


@dataclass(frozen=True)
class Bootstrap:
    data_dir: str = "/data"
    secret_key: str | None = None  # None = generated into <data_dir>/secret.key
    trusted_proxies: tuple[Network, ...] = field(default_factory=tuple)
    log_level: str = "INFO"
    admin_username: str | None = None
    admin_password: str | None = None
    admin_allowed_ips: tuple[Network, ...] = field(default_factory=tuple)  # empty = any
    public_url: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Bootstrap:
        env = dict(os.environ if env is None else env)
        return cls(
            data_dir=_env(env, "DATA_DIR") or cls.data_dir,
            secret_key=env.get("SECRET_KEY") or None,
            trusted_proxies=_networks(env, "TRUSTED_PROXIES", DEFAULT_TRUSTED_PROXIES),
            log_level=(_env(env, "LOG_LEVEL") or cls.log_level).upper(),
            admin_username=env.get("ADMIN_USERNAME") or None,
            admin_password=env.get("ADMIN_PASSWORD") or None,
            admin_allowed_ips=_networks(env, "ADMIN_ALLOWED_IPS", ""),
            public_url=(env.get("PUBLIC_URL") or "").rstrip("/") or None,
        )


# Runtime knobs kept in the database "settings" table (name -> type). Upstream URL/key and
# concurrency live in the upstreams / limit_pools tables instead.
TUNABLES: dict[str, type] = {
    "max_queue_wait_s": float,
    "keepalive_interval_s": float,
    "aging_s": float,
    "session_aware": bool,
    "session_window_s": float,
    "fair_share": bool,
    "max_retries": int,
    "retry_base_delay_s": float,
    "retry_max_delay_s": float,
    "upstream_connect_timeout_s": float,
    "upstream_read_timeout_s": float,
    "allow_no_auth": bool,
    "max_body_mb": float,
    "auth_fail_limit": int,
    "auth_block_s": float,
    "models_cache_s": float,
    "request_log_days": int,
    "backup_keep_days": int,
    "max_jobs_in_flight": int,
    "job_retention_days": int,
    "content_log_days": int,
    "content_log_max_kb": int,
    "structured_repair": bool,
    "unknown_model_check": bool,
    "compaction_idle_update": bool,
    "compaction_idle_minutes": int,
    "compaction_saved": bool,
    "compaction_keep_days": int,
    "search_provider": str,
    "search_key": str,
    "search_url": str,
    "passthrough_get_paths": str,
    "metrics_token": str,
    "otel_endpoint": str,
    "otel_headers": str,
    "otel_service_name": str,
    "price_catalog_enabled": bool,
    "context_check": bool,
    "sticky_routing": bool,
    "affinity_ttl_s": float,
    "affinity_wait_s": float,
    "lb_strategy": str,
    "opencode_rank": bool,
    "rank_index": str,
    "rank_key": str,
    "circuit_threshold": int,
    "circuit_cooldown_s": float,
}
# stored encrypted, never shown back
SECRET_TUNABLES = frozenset({"metrics_token", "otel_headers", "search_key", "rank_key"})


@dataclass(frozen=True)
class Settings:
    upstream_url: str = "https://api.arliai.com/v1"
    upstream_api_key: str = ""

    # Queue
    max_concurrency: int = 2
    min_spacing_ms: int = 0
    max_queue_wait_s: float = 0  # 0 = unlimited
    keepalive_interval_s: float = 15

    # Scheduling
    aging_s: float = 120  # waiting this long bumps a request one priority lane up
    session_aware: bool = True  # sessions already mid-task go before new ones
    session_window_s: float = 60  # how long a session counts as "mid-task" after a request
    fair_share: bool = True  # among equals, the client served least recently goes first

    # Upstream retry safety net (429 / 5xx / connection errors)
    max_retries: int = 8  # -1 = forever, 0 = none
    retry_base_delay_s: float = 2
    retry_max_delay_s: float = 60

    # Upstream timeouts
    upstream_connect_timeout_s: float = 15
    upstream_read_timeout_s: float = 600

    # Client access
    access_keys: tuple[str, ...] = ()  # only used for the first-start import
    allow_no_auth: bool = False
    max_body_mb: float = 20
    auth_fail_limit: int = 10  # failures per minute per IP before blocking
    auth_block_s: float = 300

    models_cache_s: float = 300
    request_log_days: int = 30  # per-request rows kept this long (0 = don't keep rows; daily totals always)
    backup_keep_days: int = 7  # daily automatic database copies in /data/backups (0 = off)
    max_jobs_in_flight: int = 1  # async jobs / batch requests running at once (live requests keep the rest)
    job_retention_days: int = 7  # finished jobs, batches and their files are deleted after this
    content_log_days: int = 7  # logged prompts/replies (clients with content logging on) kept this long
    content_log_max_kb: int = 256  # per request and per reply; longer ones are cut
    structured_repair: bool = True  # fix broken JSON replies (JSON mode, tool-call arguments)
    unknown_model_check: bool = True  # 404 for models no upstream lists (when every list is known)
    compaction_idle_update: bool = True  # update a summary in the background while a conversation is idle
    compaction_idle_minutes: int = 60  # summaries are forgotten after this long unused
    compaction_saved: bool = False  # also keep summaries in the database (encrypted), so they survive restarts
    compaction_keep_days: int = 7  # saved summaries are deleted after this long unused
    search_provider: str = "off"  # web search for council members: off | tavily | brave | searxng
    search_key: str = ""  # its API key (Tavily, Brave)
    search_url: str = ""  # SearXNG address, e.g. http://searxng:8080
    passthrough_get_paths: str = ""  # /v1/ GET paths forwarded to the provider, comma-separated (default: none)
    metrics_token: str = ""  # /metrics needs "Authorization: Bearer <token>"; empty = /metrics is off
    otel_endpoint: str = ""  # OTLP/HTTP collector base URL (traces go to <it>/v1/traces); empty = off
    otel_headers: str = ""  # extra headers for the collector, "key=value,key2=value2"
    otel_service_name: str = "queuerouter"
    price_catalog_enabled: bool = True  # fetch official prices from models.dev daily (for cost estimates)
    context_check: bool = True  # refuse prompts that clearly exceed the model's context before queuing
    sticky_routing: bool = True  # send a conversation back to the upstream that served it (warm cache)
    affinity_ttl_s: float = 1800  # forget a conversation's upstream after this long
    affinity_wait_s: float = 5  # how long to wait for that upstream when it's busy

    # Several upstreams
    lb_strategy: str = "least-busy"  # least-busy | weighted | ordered (failover)
    opencode_rank: bool = False  # order and number the opencode model list by strength (app/ranking.py)
    rank_index: str = "intelligence"  # intelligence | coding | agentic (Artificial Analysis)
    rank_key: str = ""  # Artificial Analysis API key (free tier)
    circuit_threshold: int = 5  # consecutive failures before an upstream is taken out
    circuit_cooldown_s: float = 60  # then one trial request decides whether it's back

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        env = dict(os.environ if env is None else env)
        return cls(
            upstream_url=(_env(env, "UPSTREAM_URL") or cls.upstream_url).rstrip("/"),
            upstream_api_key=env.get("ARLIAI_API_KEY", ""),
            max_concurrency=_int(env, "MAX_CONCURRENCY", cls.max_concurrency),
            min_spacing_ms=_int(env, "MIN_SPACING_MS", cls.min_spacing_ms),
            max_queue_wait_s=_float(env, "MAX_QUEUE_WAIT_S", cls.max_queue_wait_s),
            keepalive_interval_s=_float(env, "KEEPALIVE_INTERVAL_S", cls.keepalive_interval_s),
            aging_s=_float(env, "AGING_S", cls.aging_s),
            session_aware=_bool(env, "SESSION_AWARE", cls.session_aware),
            session_window_s=_float(env, "SESSION_WINDOW_S", cls.session_window_s),
            fair_share=_bool(env, "FAIR_SHARE", cls.fair_share),
            max_retries=_int(env, "MAX_RETRIES", cls.max_retries),
            retry_base_delay_s=_float(env, "RETRY_BASE_DELAY_S", cls.retry_base_delay_s),
            retry_max_delay_s=_float(env, "RETRY_MAX_DELAY_S", cls.retry_max_delay_s),
            upstream_connect_timeout_s=_float(env, "UPSTREAM_CONNECT_TIMEOUT_S", cls.upstream_connect_timeout_s),
            upstream_read_timeout_s=_float(env, "UPSTREAM_READ_TIMEOUT_S", cls.upstream_read_timeout_s),
            access_keys=tuple(_list(env, "ACCESS_KEYS")),
            allow_no_auth=_bool(env, "ALLOW_NO_AUTH", False),
            max_body_mb=_float(env, "MAX_BODY_MB", cls.max_body_mb),
            auth_fail_limit=_int(env, "AUTH_FAIL_LIMIT", cls.auth_fail_limit),
            auth_block_s=_float(env, "AUTH_BLOCK_S", cls.auth_block_s),
            models_cache_s=_float(env, "MODELS_CACHE_S", cls.models_cache_s),
        )

    def tunables(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in TUNABLES}
