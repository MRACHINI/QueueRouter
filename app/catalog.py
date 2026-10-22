"""Per-upstream model lists (``GET /models``), used to route requests and answer /v1/models."""

from __future__ import annotations

import json
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
        # Which models each upstream offers (Upstreams page): {upstream_id: (offer_new, models)}, where
        # models are the hidden ones when offer_new, else the only ones offered. Absent = all offered.
        self.filters: dict[int, tuple[bool, frozenset[str]]] = {}

    def offered(self, upstream_id: int, model: str) -> bool:
        rule = self.filters.get(upstream_id)
        if rule is None:
            return True
        offer_new, models = rule
        return (model not in models) if offer_new else (model in models)

    def ids(self, upstream_id: int, raw: bool = False) -> set[str] | None:
        """Model ids the upstream offers, or None if not known (yet). ``raw``: all it lists, hidden ones too
        (what an alias may still target, and what the Upstreams page offers to choose from)."""
        entry = self._entries.get(upstream_id)
        if not (entry and entry.ok):
            return None
        return {m["id"] for m in entry.models if raw or self.offered(upstream_id, m["id"])}

    def models(self, upstream_id: int, raw: bool = False) -> list[dict]:
        entry = self._entries.get(upstream_id)
        if not (entry and entry.ok):
            return []
        return [m for m in entry.models if raw or self.offered(upstream_id, m["id"])]

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


def parse_filter(text: str) -> tuple[bool, frozenset[str]] | None:
    """An upstream's stored model choice ("" = everything offered)."""
    if not text:
        return None
    try:
        data = json.loads(text)
        return bool(data.get("offer_new", True)), frozenset(str(m) for m in data.get("models") or [])
    except (ValueError, AttributeError, TypeError):
        return None


def make_filter(listed: set[str], chosen: set[str], offer_new: bool) -> str:
    """Store a choice: the hidden models when new ones are offered automatically, else the chosen ones."""
    if offer_new and chosen >= listed:
        return ""  # everything offered, as by default
    models = sorted(listed - chosen) if offer_new else sorted(chosen)
    return json.dumps({"offer_new": offer_new, "models": models})
