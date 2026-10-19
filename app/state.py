"""The live configuration snapshot the proxy reads on every request.

Built from the database and swapped atomically whenever an admin changes something, so no
restart is needed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from .config import Bootstrap, ClientKey, Settings
from .store import Alias, Pool, Store, Upstream

log = logging.getLogger("queuerouter")

# running: normal · paused: accept and queue, start nothing new · draining: refuse new requests,
# finish the queue · maintenance: refuse new requests and fail queued ones (in-flight finish).
MODES = ("running", "paused", "draining", "maintenance")


@dataclass(frozen=True)
class Target:
    """An upstream with what's needed to call it."""

    upstream: Upstream
    pool: Pool
    api_key: str

    @property
    def id(self) -> int:
        return self.upstream.id

    @property
    def usable(self) -> bool:
        return self.upstream.enabled and not self.upstream.paused


@dataclass(frozen=True)
class Snapshot:
    settings: Settings
    clients: dict[str, ClientKey] = field(default_factory=dict)  # key_hash -> client
    pools: dict[int, Pool] = field(default_factory=dict)
    upstreams: dict[int, Target] = field(default_factory=dict)  # in display / failover order
    mode: str = "running"
    maintenance_message: str = ""
    aliases: dict[str, Alias] = field(default_factory=dict)  # enabled aliases by name

    @property
    def configured(self) -> bool:
        return any(t.usable for t in self.upstreams.values())


def load_snapshot(store: Store) -> Snapshot:
    settings = Settings(**store.get_tunables())
    pools = {p.id: p for p in store.list_pools()}
    upstreams = {
        u.id: Target(u, pools[u.pool_id], store.upstream_key(u.id))
        for u in store.list_upstreams()
        if u.pool_id in pools
    }
    clients = {
        c.key_hash: ClientKey(c.name, c.key_hash, c.priority, c.policy)
        for c in store.list_clients()
        if c.enabled and not c.expired
    }
    mode = store.get_meta("mode") or "running"
    message = store.get_meta("maintenance_message") or ""
    aliases = {a.name: a for a in store.list_aliases() if a.enabled}
    return Snapshot(settings, clients, pools, upstreams, mode if mode in MODES else "running", message, aliases)


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
