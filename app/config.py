"""Phase 0 settings, read from environment variables (see .env.example).

These move into the database / web UI in Phase 1a.
"""

import ipaddress
import os
from dataclasses import dataclass, field


def _int(env: dict[str, str], name: str, default: int) -> int:
    return int(env.get(name, default))


def _float(env: dict[str, str], name: str, default: float) -> float:
    return float(env.get(name, default))


def _bool(env: dict[str, str], name: str, default: bool) -> bool:
    return env.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _list(env: dict[str, str], name: str, default: str = "") -> list[str]:
    return [p.strip() for p in env.get(name, default).split(",") if p.strip()]


@dataclass(frozen=True)
class Settings:
    upstream_url: str = "https://api.arliai.com/v1"
    upstream_api_key: str = ""

    # Queue
    max_concurrency: int = 2
    min_spacing_ms: int = 0
    max_queue_wait_s: float = 0  # 0 = unlimited
    keepalive_interval_s: float = 15

    # Upstream retry safety net (429 / 5xx / connection errors)
    max_retries: int = 8  # -1 = forever, 0 = none
    retry_base_delay_s: float = 2
    retry_max_delay_s: float = 60

    # Upstream timeouts
    upstream_connect_timeout_s: float = 15
    upstream_read_timeout_s: float = 600

    # Client access
    access_keys: tuple[str, ...] = ()
    allow_no_auth: bool = False
    trusted_proxies: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = field(default_factory=tuple)
    max_body_mb: float = 20
    auth_fail_limit: int = 10  # failures per minute per IP before blocking
    auth_block_s: float = 300

    models_cache_s: float = 300
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        env = dict(os.environ if env is None else env)
        return cls(
            upstream_url=env.get("UPSTREAM_URL", cls.upstream_url).rstrip("/"),
            upstream_api_key=env.get("ARLIAI_API_KEY", ""),
            max_concurrency=_int(env, "MAX_CONCURRENCY", cls.max_concurrency),
            min_spacing_ms=_int(env, "MIN_SPACING_MS", cls.min_spacing_ms),
            max_queue_wait_s=_float(env, "MAX_QUEUE_WAIT_S", cls.max_queue_wait_s),
            keepalive_interval_s=_float(env, "KEEPALIVE_INTERVAL_S", cls.keepalive_interval_s),
            max_retries=_int(env, "MAX_RETRIES", cls.max_retries),
            retry_base_delay_s=_float(env, "RETRY_BASE_DELAY_S", cls.retry_base_delay_s),
            retry_max_delay_s=_float(env, "RETRY_MAX_DELAY_S", cls.retry_max_delay_s),
            upstream_connect_timeout_s=_float(env, "UPSTREAM_CONNECT_TIMEOUT_S", cls.upstream_connect_timeout_s),
            upstream_read_timeout_s=_float(env, "UPSTREAM_READ_TIMEOUT_S", cls.upstream_read_timeout_s),
            access_keys=tuple(_list(env, "ACCESS_KEYS")),
            allow_no_auth=_bool(env, "ALLOW_NO_AUTH", False),
            trusted_proxies=tuple(
                ipaddress.ip_network(p, strict=False)
                for p in _list(env, "TRUSTED_PROXIES", "172.16.0.0/12,10.0.0.0/8,192.168.0.0/16,127.0.0.1/32")
            ),
            max_body_mb=_float(env, "MAX_BODY_MB", cls.max_body_mb),
            auth_fail_limit=_int(env, "AUTH_FAIL_LIMIT", cls.auth_fail_limit),
            auth_block_s=_float(env, "AUTH_BLOCK_S", cls.auth_block_s),
            models_cache_s=_float(env, "MODELS_CACHE_S", cls.models_cache_s),
            log_level=env.get("LOG_LEVEL", cls.log_level).upper(),
        )

    def validate(self) -> None:
        """Refuse to start with an unsafe or unusable configuration."""
        if not self.upstream_api_key:
            raise ValueError("ARLIAI_API_KEY is required")
        if self.max_concurrency < 1:
            raise ValueError("MAX_CONCURRENCY must be >= 1")
        if not self.access_keys and not self.allow_no_auth:
            raise ValueError("ACCESS_KEYS is required (or set ALLOW_NO_AUTH=true on a private network)")
