"""The live configuration snapshot the proxy reads on every request.

Built from the database and swapped atomically whenever an admin changes something, so no
restart is needed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from .answer_rules import Rule
from .config import Bootstrap, ClientKey, Settings, with_group
from .edits import Edit
from .names import ModelNames
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
    any_clients: bool = False  # any client at all, disabled and expired ones too ("allow without key" is off then)
    names: ModelNames = field(default_factory=ModelNames)
    # key_hash -> (name, "disabled" / "expired"): named on the Requests page when such a key is refused
    inactive_clients: dict[str, tuple[str, str]] = field(default_factory=dict)
    edits: dict[int, Edit] = field(default_factory=dict)  # prompt edits by id (app/edits.py), with what models made
    rules: dict[int, Rule] = field(default_factory=dict)  # answer rules by id (app/answer_rules.py)

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
    every_client = store.list_clients()
    groups = {g.id: g.policy for g in store.list_groups()}
    clients = {  # each with its group's models and limits, its own where set
        c.key_hash: ClientKey(
            c.name, c.key_hash, c.priority, with_group(c.policy, groups.get(c.group_id)), c.expires_at
        )
        for c in every_client
        if c.enabled and not c.expired
    }
    mode = store.get_meta("mode") or "running"
    message = store.get_meta("maintenance_message") or ""
    aliases = {a.name: a for a in store.list_aliases() if a.enabled}
    mode = mode if mode in MODES else "running"
    inactive = {
        c.key_hash: (c.name, "disabled" if not c.enabled else "expired")
        for c in every_client
        if not c.enabled or c.expired
    }
    names = ModelNames({t.id: t.upstream.name for t in upstreams.values()}, store.list_model_names())
    return Snapshot(
        settings,
        clients,
        pools,
        upstreams,
        mode,
        message,
        aliases,
        any_clients=bool(every_client),
        names=names,
        inactive_clients=inactive,
        edits={e.id: e for e in store.list_edits()},
        rules={r.id: r for r in store.list_rules()},
    )


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
    if store.get_meta("mode") == "draining":  # it was for this restart: the new start takes requests again
        store.set_meta("mode", "running")
        store.set_meta("maintenance_message", "")
        store.audit("system", "mode", "running", {"message": "draining ends with a restart"})
    return store
