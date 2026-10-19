"""The live configuration snapshot the proxy reads on every request.

Built from the database and swapped atomically whenever an admin changes something, so no
restart is needed.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass, field

from .config import Bootstrap, ClientKey, Settings
from .store import Pool, Store, Upstream

log = logging.getLogger("queuerouter")


@dataclass(frozen=True)
class ActiveUpstream:
    """The upstream requests currently go to (multi-upstream routing arrives with pools)."""

    upstream: Upstream
    pool: Pool
    api_key: str


@dataclass(frozen=True)
class Snapshot:
    settings: Settings
    clients: dict[str, ClientKey] = field(default_factory=dict)  # key_hash -> client
    upstream: ActiveUpstream | None = None


def load_snapshot(store: Store) -> Snapshot:
    tunables = store.get_tunables()
    pools = {p.id: p for p in store.list_pools()}
    active = None
    for upstream in store.list_upstreams():
        if upstream.enabled and upstream.pool_id in pools:
            active = ActiveUpstream(upstream, pools[upstream.pool_id], store.upstream_key(upstream.id))
            break

    settings = Settings(**tunables)
    if active is not None:
        settings = dataclasses.replace(
            settings,
            upstream_url=active.upstream.base_url,
            upstream_api_key=active.api_key,
            max_concurrency=active.pool.max_concurrency,
            min_spacing_ms=active.pool.min_spacing_ms,
        )
    clients = {
        c.key_hash: ClientKey(c.name, c.key_hash, c.priority)
        for c in store.list_clients()
        if c.enabled and not c.expired
    }
    return Snapshot(settings, clients, active)


def open_store(bootstrap: Bootstrap, seed: Settings | None) -> Store:
    """Open (and migrate) the database; on the very first start import ``seed``."""
    store = Store.open(bootstrap.data_dir, bootstrap.secret_key)
    if not store.initialized:
        summary = store.import_settings(seed or Settings(), source="environment" if seed else "defaults")
        log.info(json.dumps({"event": "first_start_import", **summary}))
    elif seed is not None and (seed.upstream_api_key or seed.access_keys):
        log.warning(
            json.dumps(
                {
                    "event": "env_ignored",
                    "message": "ARLIAI_API_KEY / ACCESS_KEYS / tuning variables are only imported on first start; "
                    "the database is now the source of truth",
                }
            )
        )
    return store
