"""Per-upstream model lists (``GET /models``), used to route requests and answer /v1/models."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import httpx

from .upstream import upstream_headers

FAILED_RETRY_S = 10  # retry a model list we never managed to fetch this often


@dataclass
class _Entry:
    fetched_at: float = 0.0
    models: list[dict] = field(default_factory=list)
    ok: bool = False
    error: str = ""


async def fetch_models(client: httpx.AsyncClient, base_url: str, api_key: str, timeout: float = 15) -> list[dict]:
    """GET {base_url}/models; raises on network errors or a non-200 / malformed answer."""
    response = await client.get(
        f"{base_url.rstrip('/')}/models", headers=upstream_headers({}, api_key), timeout=timeout
    )
    if response.status_code != 200:
        raise ValueError(f"HTTP {response.status_code}")
    data = response.json().get("data")
    if not isinstance(data, list):
        raise ValueError("no 'data' list in the response")
    return [m for m in data if isinstance(m, dict) and isinstance(m.get("id"), str)]


class Catalogs:
    def __init__(self) -> None:
        self._entries: dict[int, _Entry] = {}

    def ids(self, upstream_id: int) -> set[str] | None:
        """Model ids the upstream serves, or None if not known (yet)."""
        entry = self._entries.get(upstream_id)
        return {m["id"] for m in entry.models} if entry and entry.ok else None

    def models(self, upstream_id: int) -> list[dict]:
        entry = self._entries.get(upstream_id)
        return entry.models if entry and entry.ok else []

    def stale(self, upstream_id: int, max_age_s: float) -> bool:
        """Due for a refresh. A list we never got is retried soon, not after ``max_age_s``."""
        entry = self._entries.get(upstream_id)
        if entry is None:
            return True
        age = time.monotonic() - entry.fetched_at
        return age > (max_age_s if entry.ok else min(max_age_s, FAILED_RETRY_S))

    def describe(self, upstream_id: int) -> dict:
        entry = self._entries.get(upstream_id)
        if entry is None:
            return {"models": None, "error": ""}
        return {"models": len(entry.models) if entry.ok else None, "error": entry.error}

    async def refresh(self, client: httpx.AsyncClient, upstream_id: int, base_url: str, api_key: str) -> bool:
        entry = self._entries.setdefault(upstream_id, _Entry())
        entry.fetched_at = time.monotonic()
        try:
            entry.models = await fetch_models(client, base_url, api_key)
            entry.ok, entry.error = True, ""
        except (httpx.HTTPError, ValueError) as exc:
            entry.error = f"{type(exc).__name__}: {exc}"[:200]  # keep the last good list, if any
            return False
        return True

    def forget(self, keep: set[int]) -> None:
        for upstream_id in list(self._entries):
            if upstream_id not in keep:
                del self._entries[upstream_id]
