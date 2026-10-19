"""SQLite persistence: configuration edited in the admin UI, users, audit log.

Small synchronous operations behind a lock; every call is well under a millisecond, so they
run directly on the event loop. Secrets are encrypted with Fernet; client keys are stored only
as SHA-256 hashes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from .config import PRIORITIES, SECRET_TUNABLES, TUNABLES, ClientPolicy, Settings, parse_access_keys

log = logging.getLogger("queuerouter")

MIGRATIONS: list[str] = [
    # 1: initial schema
    """
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE limit_pools (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        plan TEXT,
        max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 1),
        context_cap INTEGER,
        min_spacing_ms INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL
    );
    CREATE TABLE upstreams (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        preset TEXT,
        base_url TEXT NOT NULL,
        api_key_enc TEXT NOT NULL,
        pool_id INTEGER NOT NULL REFERENCES limit_pools(id),
        weight INTEGER NOT NULL DEFAULT 1,
        enabled INTEGER NOT NULL DEFAULT 1,
        paused INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL
    );
    CREATE TABLE clients (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        key_hash TEXT NOT NULL UNIQUE,
        key_prefix TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'normal',
        enabled INTEGER NOT NULL DEFAULT 1,
        expires_at REAL,
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    CREATE TABLE admin_users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        can_view_content INTEGER NOT NULL DEFAULT 0,
        totp_secret_enc TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at REAL NOT NULL
    );
    CREATE TABLE admin_sessions (
        token_hash TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
        csrf TEXT NOT NULL,
        ip TEXT,
        created_at REAL NOT NULL,
        last_seen REAL NOT NULL,
        expires_at REAL NOT NULL
    );
    CREATE TABLE audit_log (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        target TEXT NOT NULL DEFAULT '',
        detail TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX audit_log_ts ON audit_log(ts);
    """,
    # 2: per-client policies (JSON, see config.ClientPolicy)
    """
    ALTER TABLE clients ADD COLUMN policy TEXT NOT NULL DEFAULT '{}';
    """,
    # 3: model aliases
    """
    CREATE TABLE aliases (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        target_model TEXT NOT NULL,
        upstream_ids TEXT NOT NULL DEFAULT '[]',
        enabled INTEGER NOT NULL DEFAULT 1,
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    """,
    # 4: request log (metadata only, never content) and daily usage rollup
    """
    CREATE TABLE requests (
        id TEXT PRIMARY KEY,
        ts REAL NOT NULL,
        client TEXT,
        priority TEXT,
        session TEXT,
        path TEXT,
        model TEXT,
        target_model TEXT,
        upstream TEXT,
        stream INTEGER NOT NULL DEFAULT 0,
        status INTEGER,
        outcome TEXT,
        position INTEGER,
        queue_ms INTEGER,
        upstream_ms INTEGER,
        total_ms INTEGER,
        retries INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER,
        completion_tokens INTEGER,
        finish_reason TEXT,
        ip TEXT
    );
    CREATE INDEX requests_ts ON requests(ts);
    CREATE INDEX requests_client_ts ON requests(client, ts);
    CREATE TABLE usage_daily (
        day TEXT NOT NULL,
        client TEXT NOT NULL,
        model TEXT NOT NULL,
        upstream TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (day, client, model, upstream)
    );
    """,
    # 5: cost tracking - cached prompt tokens, plan prices, per-model price overrides
    """
    ALTER TABLE requests ADD COLUMN cached_tokens INTEGER;
    ALTER TABLE usage_daily ADD COLUMN cached_tokens INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE limit_pools ADD COLUMN monthly_price REAL;
    CREATE TABLE model_prices (
        model TEXT PRIMARY KEY,
        reference TEXT,
        input REAL,
        output REAL,
        cached_input REAL,
        updated_at REAL NOT NULL
    );
    """,
    # 6: per-alias parameter rules (JSON, see app/params.py)
    """
    ALTER TABLE aliases ADD COLUMN params TEXT NOT NULL DEFAULT '';
    """,
    # 7: fallback chains on aliases
    """
    ALTER TABLE aliases ADD COLUMN fallbacks TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE aliases ADD COLUMN fallback_on TEXT NOT NULL DEFAULT '["unavailable", "errors", "context"]';
    """,
    # 8: request tags
    """
    ALTER TABLE requests ADD COLUMN tags TEXT;
    """,
    # 9: alerts: rule overrides, webhooks (URL and secret encrypted), in-app notifications
    """
    CREATE TABLE alert_rules (
        kind TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL,
        threshold REAL,
        window_s INTEGER,
        cooldown_s INTEGER
    );
    CREATE TABLE webhooks (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        kind TEXT NOT NULL,
        url TEXT NOT NULL,
        secret TEXT,
        min_level TEXT NOT NULL DEFAULT 'warning',
        enabled INTEGER NOT NULL DEFAULT 1,
        last_at REAL,
        last_ok INTEGER,
        last_error TEXT,
        created_at REAL NOT NULL
    );
    CREATE TABLE notifications (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        level TEXT NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL DEFAULT '',
        read INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX notifications_ts ON notifications (ts);
    """,
    # 10: two-factor login (TOTP secret encrypted, recovery codes hashed)
    """
    ALTER TABLE admin_users ADD COLUMN totp_secret TEXT;
    ALTER TABLE admin_users ADD COLUMN totp_last INTEGER NOT NULL DEFAULT -1;
    ALTER TABLE admin_users ADD COLUMN recovery_codes TEXT NOT NULL DEFAULT '[]';
    """,
    # 11: Responses API conversations for previous_response_id (encrypted, kept a few days)
    """
    CREATE TABLE stored_responses (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        created REAL NOT NULL,
        data TEXT NOT NULL
    );
    CREATE INDEX stored_responses_created ON stored_responses (created);
    """,
    # 12: async jobs, and OpenAI-style files + batches (request bodies and results encrypted)
    """
    CREATE TABLE jobs (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        batch_id TEXT,
        custom_id TEXT,
        status TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'low',
        endpoint TEXT NOT NULL,
        request TEXT NOT NULL,
        run_at REAL,
        deadline REAL,
        idle_only INTEGER NOT NULL DEFAULT 0,
        callback_url TEXT,
        callback_secret TEXT,
        callback_status TEXT,
        tags TEXT,
        created REAL NOT NULL,
        started REAL,
        finished REAL,
        attempts INTEGER NOT NULL DEFAULT 0,
        status_code INTEGER,
        result TEXT,
        error TEXT,
        model TEXT,
        prompt_tokens INTEGER,
        completion_tokens INTEGER
    );
    CREATE INDEX jobs_due ON jobs (status, run_at);
    CREATE INDEX jobs_batch ON jobs (batch_id);
    CREATE INDEX jobs_client ON jobs (client, created);
    CREATE TABLE files (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        purpose TEXT NOT NULL,
        filename TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        created REAL NOT NULL,
        data TEXT NOT NULL
    );
    CREATE TABLE batches (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        input_file_id TEXT NOT NULL,
        completion_window TEXT NOT NULL,
        status TEXT NOT NULL,
        created REAL NOT NULL,
        expires_at REAL,
        finished_at REAL,
        cancelled_at REAL,
        output_file_id TEXT,
        error_file_id TEXT,
        total INTEGER NOT NULL,
        metadata TEXT
    );
    """,
    # 13: opt-in content logging (prompts and replies, encrypted)
    """
    CREATE TABLE request_content (
        id TEXT PRIMARY KEY,
        ts REAL NOT NULL,
        client TEXT NOT NULL,
        request TEXT NOT NULL,
        response TEXT NOT NULL,
        truncated INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX request_content_ts ON request_content (ts);
    """,
    # 14: recurring jobs (cron schedules that create jobs)
    """
    CREATE TABLE schedules (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        name TEXT NOT NULL,
        cron TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        request TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'low',
        tags TEXT,
        idle_only INTEGER NOT NULL DEFAULT 0,
        deadline_s REAL,
        callback_url TEXT,
        callback_secret TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created REAL NOT NULL,
        next_run REAL,
        last_run REAL,
        last_job_id TEXT
    );
    CREATE INDEX schedules_due ON schedules (enabled, next_run);
    """,
]

JOB_DONE = ("succeeded", "failed", "cancelled", "expired")

REQUEST_COLUMNS = (
    "id",
    "ts",
    "client",
    "priority",
    "session",
    "path",
    "model",
    "target_model",
    "upstream",
    "stream",
    "status",
    "outcome",
    "position",
    "queue_ms",
    "upstream_ms",
    "total_ms",
    "retries",
    "prompt_tokens",
    "completion_tokens",
    "cached_tokens",
    "tags",
    "finish_reason",
    "ip",
)


_UPSERT = "INSERT INTO {table} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def new_client_key() -> str:
    return "qr-" + secrets.token_urlsafe(32)


@dataclass(frozen=True)
class Pool:
    id: int
    name: str
    plan: str | None
    max_concurrency: int
    context_cap: int | None
    min_spacing_ms: int
    monthly_price: float | None = None  # plan fee, for "savings vs official prices"


@dataclass(frozen=True)
class Upstream:
    id: int
    name: str
    preset: str | None
    base_url: str
    pool_id: int
    weight: int
    enabled: bool
    paused: bool


@dataclass(frozen=True)
class Client:
    id: int
    name: str
    key_hash: str
    key_prefix: str
    priority: str
    enabled: bool
    expires_at: float | None
    notes: str
    created_at: float
    policy: ClientPolicy = ClientPolicy()

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and self.expires_at <= time.time()


@dataclass(frozen=True)
class Webhook:
    id: int
    name: str
    kind: str  # discord | slack | ntfy | json
    url: str
    secret: str | None  # HMAC key for "json" webhooks
    min_level: str
    enabled: bool
    last_at: float | None = None
    last_ok: bool | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class Alias:
    id: int
    name: str
    target_model: str
    upstream_ids: tuple[int, ...]  # empty = any upstream
    enabled: bool
    notes: str
    params: str = ""  # parameter rules JSON (app/params.py)
    fallbacks: tuple[str, ...] = ()  # backup models, tried in order
    fallback_on: tuple[str, ...] = ("unavailable", "errors", "context")


ROLES = ("viewer", "operator", "admin", "owner")  # ascending privilege


@dataclass(frozen=True)
class AdminUser:
    id: int
    username: str
    role: str
    can_view_content: bool
    enabled: bool
    created_at: float
    has_2fa: bool = False

    def can(self, role: str) -> bool:
        """Whether this user has at least ``role``."""
        return ROLES.index(self.role) >= ROLES.index(role)


def _admin(row: sqlite3.Row) -> AdminUser:
    return AdminUser(
        row["id"],
        row["username"],
        row["role"],
        bool(row["can_view_content"]),
        bool(row["enabled"]),
        row["created_at"],
        row["totp_secret"] is not None,
    )


class DataDirError(RuntimeError):
    """The data directory can't be used; the message says how to fix it."""


def check_data_dir(data_dir: str) -> None:
    """Fail early, with a fix, when the data directory (or a file in it) isn't writable.

    Typical cause: a bind mount or pre-existing volume owned by root, while the container
    runs as an unprivileged user.
    """
    try:
        os.makedirs(data_dir, exist_ok=True)
    except PermissionError:
        pass  # reported below with the ownership details
    uid, gid = os.getuid(), os.getgid()
    problems = []
    if not os.access(data_dir, os.W_OK | os.X_OK):
        problems.append(data_dir)
    elif os.path.isdir(data_dir):
        for name in os.listdir(data_dir):
            path = os.path.join(data_dir, name)
            if os.path.isfile(path) and not os.access(path, os.R_OK | os.W_OK):
                problems.append(path)
    if not problems:
        return
    try:
        st = os.stat(data_dir)
        owner_uid, owner = st.st_uid, f"{st.st_uid}:{st.st_gid}"
    except OSError:
        owner_uid, owner = None, "unknown"
    run_as_owner = (
        f'  - or run the container as the owner: add  user: "{owner}"  to the service\n'
        if owner_uid not in (None, 0)  # never suggest running as root
        else ""
    )
    raise DataDirError(
        f"{', '.join(problems)} is not writable by this container (running as {uid}:{gid}; "
        f"{data_dir} is owned by {owner}). Fix it with one of:\n"
        "  - let the container fix it: remove `user:` from the service and add\n"
        "    cap_add: [CHOWN, SETUID, SETGID]  (set PUID/PGID to choose the owner)\n"
        f"  - named volume:  docker run --rm -v <volume>:{data_dir} alpine chown -R {uid}:{gid} {data_dir}\n"
        f"  - bind mount:    sudo chown -R {uid}:{gid} /path/on/host\n"
        f"{run_as_owner}"
        "See README > Data volume permissions."
    )


def load_fernet(data_dir: str, secret_key: str | None) -> Fernet:
    """Fernet from SECRET_KEY, or from <data_dir>/secret.key (created on first start)."""
    if secret_key:
        return Fernet(secret_key.encode())
    path = os.path.join(data_dir, "secret.key")
    if not os.path.exists(path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(Fernet.generate_key())
        log.info(json.dumps({"event": "secret_key_created", "path": path}))
    with open(path, "rb") as f:
        return Fernet(f.read().strip())


class Store:
    def __init__(self, path: str, fernet: Fernet) -> None:
        self.path = path
        self.fernet = fernet
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA busy_timeout=5000")

    @classmethod
    def open(cls, data_dir: str, secret_key: str | None) -> Store:
        check_data_dir(data_dir)
        from .backup import apply_staged_restore  # imported here: backup imports this module

        apply_staged_restore(data_dir)
        store = cls(os.path.join(data_dir, "queuerouter.db"), load_fernet(data_dir, secret_key))
        store.migrate()
        return store

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def backup_to(self, dest: str) -> None:
        """A consistent copy of the live database (SQLite online backup)."""
        target = sqlite3.connect(dest)
        try:
            with self._lock:
                self._db.backup(target)
        finally:
            target.close()

    # -- plumbing ------------------------------------------------------------------------

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def query(self, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def encrypt(self, value: str) -> str:
        return self.fernet.encrypt(value.encode()).decode()

    def decrypt(self, token: str) -> str:
        try:
            return self.fernet.decrypt(token.encode()).decode()
        except InvalidToken as exc:
            raise RuntimeError("cannot decrypt a stored secret: SECRET_KEY / secret.key changed?") from exc

    def schema_version(self) -> int:
        with self._lock:
            return self._db.execute("PRAGMA user_version").fetchone()[0]

    def migrate(self) -> None:
        current = self.schema_version()
        if current > len(MIGRATIONS):
            raise RuntimeError(f"database schema v{current} is newer than this QueueRouter (v{len(MIGRATIONS)})")
        if current == len(MIGRATIONS):
            return
        if current > 0 and os.path.exists(self.path):
            backup_dir = os.path.join(os.path.dirname(self.path), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            backup = os.path.join(backup_dir, f"queuerouter-v{current}-{int(time.time())}.db")
            with self._lock:
                self._db.execute("PRAGMA wal_checkpoint(FULL)")
            shutil.copy2(self.path, backup)
            log.info(json.dumps({"event": "db_backup", "path": backup}))
        for version in range(current + 1, len(MIGRATIONS) + 1):
            with self._lock:
                self._db.executescript(f"BEGIN;\n{MIGRATIONS[version - 1]}\nPRAGMA user_version={version};\nCOMMIT;")
            log.info(json.dumps({"event": "db_migrated", "version": version}))

    def audit(self, actor: str, action: str, target: str = "", detail: object = "") -> None:
        text = detail if isinstance(detail, str) else json.dumps(detail, default=str)
        with self.tx() as db:
            db.execute(
                "INSERT INTO audit_log (ts, actor, action, target, detail) VALUES (?, ?, ?, ?, ?)",
                (time.time(), actor, action, target, text),
            )

    # -- meta / first start --------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        rows = self.query("SELECT value FROM meta WHERE key = ?", (key,))
        return rows[0]["value"] if rows else None

    def set_meta(self, key: str, value: str) -> None:
        with self.tx() as db:
            db.execute(_UPSERT.format(table="meta"), (key, value))

    @property
    def initialized(self) -> bool:
        return self.get_meta("initialized") is not None

    def import_settings(self, seed: Settings, source: str = "environment") -> dict:
        """One-time import of env configuration on first start. Returns a summary."""
        summary = {"settings": 0, "upstream": None, "clients": []}
        parsed = parse_access_keys(seed.access_keys)
        with self.tx() as db:
            for name, value in seed.tunables().items():
                if name in SECRET_TUNABLES and value:
                    value = self.encrypt(value)
                db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (name, json.dumps(value)))
                summary["settings"] += 1
            if seed.upstream_api_key:
                now = time.time()
                is_arliai = "arliai.com" in seed.upstream_url
                cur = db.execute(
                    "INSERT INTO limit_pools (name, plan, max_concurrency, context_cap, min_spacing_ms, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "arliai-main" if is_arliai else "main",
                        None,
                        seed.max_concurrency,
                        None,
                        seed.min_spacing_ms,
                        now,
                    ),
                )
                db.execute(
                    "INSERT INTO upstreams (name, preset, base_url, api_key_enc, pool_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "arliai" if is_arliai else "upstream",
                        "arliai" if is_arliai else "custom",
                        seed.upstream_url,
                        self.encrypt(seed.upstream_api_key),
                        cur.lastrowid,
                        now,
                    ),
                )
                summary["upstream"] = seed.upstream_url
            for p in parsed:
                db.execute(
                    "INSERT INTO clients (name, key_hash, key_prefix, priority, created_at) VALUES (?, ?, ?, ?, ?)",
                    (p.name, hash_key(p.key), p.key[:6], p.priority, time.time()),
                )
                summary["clients"].append(p.name)
            db.execute("INSERT INTO meta (key, value) VALUES ('initialized', ?)", (str(time.time()),))
            db.execute("INSERT INTO meta (key, value) VALUES ('imported_from', ?)", (source,))
        self.audit("system", "import", source, summary)
        return summary

    # -- settings ------------------------------------------------------------------------

    def get_tunables(self) -> dict[str, object]:
        values = {}
        for row in self.query("SELECT key, value FROM settings"):
            if row["key"] in TUNABLES:
                value = json.loads(row["value"])
                if row["key"] in SECRET_TUNABLES and value:
                    value = self.decrypt(value)
                values[row["key"]] = TUNABLES[row["key"]](value)
        return values

    def set_tunables(self, values: dict[str, object], actor: str) -> None:
        unknown = set(values) - set(TUNABLES)
        if unknown:
            raise ValueError(f"unknown settings: {', '.join(sorted(unknown))}")
        with self.tx() as db:
            for name, value in values.items():
                value = TUNABLES[name](value)
                if name in SECRET_TUNABLES and value:
                    value = self.encrypt(value)  # stored encrypted like upstream keys
                db.execute(_UPSERT.format(table="settings"), (name, json.dumps(value)))
        self.audit(
            actor,
            "settings.update",
            "",
            {k: ("(secret changed)" if k in SECRET_TUNABLES else v) for k, v in values.items()},
        )

    # -- pools and upstreams -------------------------------------------------------------

    def list_pools(self) -> list[Pool]:
        return [
            Pool(
                r["id"],
                r["name"],
                r["plan"],
                r["max_concurrency"],
                r["context_cap"],
                r["min_spacing_ms"],
                r["monthly_price"],
            )
            for r in self.query("SELECT * FROM limit_pools ORDER BY id")
        ]

    def list_upstreams(self) -> list[Upstream]:
        return [
            Upstream(
                r["id"],
                r["name"],
                r["preset"],
                r["base_url"],
                r["pool_id"],
                r["weight"],
                bool(r["enabled"]),
                bool(r["paused"]),
            )
            for r in self.query("SELECT * FROM upstreams ORDER BY id")
        ]

    def upstream_key(self, upstream_id: int) -> str:
        rows = self.query("SELECT api_key_enc FROM upstreams WHERE id = ?", (upstream_id,))
        if not rows:
            raise KeyError(upstream_id)
        return self.decrypt(rows[0]["api_key_enc"])

    def create_pool(
        self,
        name: str,
        max_concurrency: int,
        actor: str,
        plan: str | None = None,
        context_cap: int | None = None,
        min_spacing_ms: int = 0,
        monthly_price: float | None = None,
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO limit_pools (name, plan, max_concurrency, context_cap, min_spacing_ms, monthly_price, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, plan, max_concurrency, context_cap, min_spacing_ms, monthly_price, time.time()),
            )
        self.audit(actor, "pool.create", name, {"max_concurrency": max_concurrency, "plan": plan})
        return cur.lastrowid

    def update_pool(self, pool_id: int, actor: str, **fields: object) -> None:
        allowed = {"name", "plan", "max_concurrency", "context_cap", "min_spacing_ms", "monthly_price"}
        self._update("limit_pools", pool_id, allowed, fields)
        self.audit(actor, "pool.update", str(pool_id), fields)

    def create_upstream(
        self,
        name: str,
        base_url: str,
        api_key: str,
        pool_id: int,
        actor: str,
        preset: str | None = None,
        weight: int = 1,
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO upstreams (name, preset, base_url, api_key_enc, pool_id, weight, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, preset, base_url.rstrip("/"), self.encrypt(api_key), pool_id, weight, time.time()),
            )
        self.audit(actor, "upstream.create", name, {"base_url": base_url, "pool_id": pool_id})
        return cur.lastrowid

    def update_upstream(self, upstream_id: int, actor: str, **fields: object) -> None:
        if "api_key" in fields:
            fields["api_key_enc"] = self.encrypt(str(fields.pop("api_key")))
        allowed = {"name", "preset", "base_url", "api_key_enc", "pool_id", "weight", "enabled", "paused"}
        self._update("upstreams", upstream_id, allowed, fields)
        self.audit(actor, "upstream.update", str(upstream_id), {k: v for k, v in fields.items() if k != "api_key_enc"})

    def delete_upstream(self, upstream_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM upstreams WHERE id = ?", (upstream_id,))
        self.audit(actor, "upstream.delete", str(upstream_id))

    def delete_pool(self, pool_id: int, actor: str) -> None:
        """Delete a pool; refuses while upstreams still use it."""
        with self.tx() as db:
            used = db.execute("SELECT COUNT(*) FROM upstreams WHERE pool_id = ?", (pool_id,)).fetchone()[0]
            if used:
                raise ValueError("pool is still used by upstreams")
            db.execute("DELETE FROM limit_pools WHERE id = ?", (pool_id,))
        self.audit(actor, "pool.delete", str(pool_id))

    # -- clients -------------------------------------------------------------------------

    def list_clients(self) -> list[Client]:
        return [
            Client(
                r["id"],
                r["name"],
                r["key_hash"],
                r["key_prefix"],
                r["priority"],
                bool(r["enabled"]),
                r["expires_at"],
                r["notes"],
                r["created_at"],
                ClientPolicy.from_json(r["policy"]),
            )
            for r in self.query("SELECT * FROM clients ORDER BY name")
        ]

    def create_client(
        self, name: str, actor: str, priority: str = "normal", notes: str = "", expires_at: float | None = None
    ) -> str:
        """Create a client and return its new key (shown once, stored only hashed)."""
        if priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        key = new_client_key()
        with self.tx() as db:
            db.execute(
                "INSERT INTO clients (name, key_hash, key_prefix, priority, notes, expires_at, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, hash_key(key), key[:6], priority, notes, expires_at, time.time()),
            )
        self.audit(actor, "client.create", name, {"priority": priority})
        return key

    def update_client(self, client_id: int, actor: str, **fields: object) -> None:
        if "priority" in fields and fields["priority"] not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        if isinstance(fields.get("policy"), ClientPolicy):
            fields["policy"] = fields["policy"].to_json()
        self._update("clients", client_id, {"name", "priority", "enabled", "expires_at", "notes", "policy"}, fields)
        self.audit(actor, "client.update", str(client_id), fields)

    def regenerate_client_key(self, client_id: int, actor: str) -> str:
        key = new_client_key()
        fields = {"key_hash": hash_key(key), "key_prefix": key[:6]}
        self._update("clients", client_id, set(fields), fields)
        self.audit(actor, "client.regenerate_key", str(client_id))
        return key

    def delete_client(self, client_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        self.audit(actor, "client.delete", str(client_id))

    # -- request log and usage -----------------------------------------------------------

    def record_requests(self, rows: list[dict], keep_rows: bool = True) -> None:
        """Store finished requests (metadata) and add them to the daily usage rollup."""
        if not rows:
            return
        placeholders = ", ".join("?" for _ in REQUEST_COLUMNS)
        with self.tx() as db:
            for row in rows:
                if keep_rows:
                    db.execute(
                        f"INSERT OR IGNORE INTO requests ({', '.join(REQUEST_COLUMNS)}) VALUES ({placeholders})",
                        tuple(int(row[c]) if c == "stream" else row.get(c) for c in REQUEST_COLUMNS),
                    )
                day = time.strftime("%Y-%m-%d", time.gmtime(row["ts"]))
                db.execute(
                    "INSERT INTO usage_daily (day, client, model, upstream, requests, errors, prompt_tokens, "
                    "completion_tokens, cached_tokens) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?) "
                    "ON CONFLICT(day, client, model, upstream) DO UPDATE SET requests = requests + 1, "
                    "errors = errors + excluded.errors, prompt_tokens = prompt_tokens + excluded.prompt_tokens, "
                    "completion_tokens = completion_tokens + excluded.completion_tokens, "
                    "cached_tokens = cached_tokens + excluded.cached_tokens",
                    (
                        day,
                        row.get("client") or "-",
                        row.get("target_model") or row.get("model") or "-",
                        row.get("upstream") or "-",
                        0 if row.get("outcome") == "done" else 1,
                        row.get("prompt_tokens") or 0,
                        row.get("completion_tokens") or 0,
                        row.get("cached_tokens") or 0,
                    ),
                )

    def list_requests(self, limit: int = 200, **filters: str) -> list[sqlite3.Row]:
        clauses, params = [], []
        for column in ("client", "outcome", "model", "upstream"):
            if value := filters.get(column):
                clauses.append(f"{column} = ?")
                params.append(value)
        if tag := filters.get("tag"):
            clauses.append("(',' || tags || ',') LIKE ?")
            params.append(f"%,{tag},%")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(f"SELECT * FROM requests {where} ORDER BY ts DESC LIMIT ?", (*params, limit))  # noqa: S608

    def request_filter_values(self) -> dict[str, list[str]]:
        values = {
            column: [
                r[0]
                for r in self.query(
                    f"SELECT DISTINCT {column} FROM requests WHERE {column} IS NOT NULL ORDER BY {column} LIMIT 200"
                )
            ]  # noqa: S608
            for column in ("client", "outcome", "model", "upstream")
        }
        tags: set[str] = set()
        for (text,) in self.query("SELECT DISTINCT tags FROM requests WHERE tags IS NOT NULL AND tags != '' LIMIT 500"):
            tags.update(text.split(","))
        values["tag"] = sorted(tags)
        return values

    def usage_month(self, month: str) -> list[sqlite3.Row]:
        """This month's usage per client and model (``month`` = YYYY-MM)."""
        return self.query(
            "SELECT client, model, SUM(requests) AS requests, SUM(prompt_tokens) AS prompt_tokens, "
            "SUM(completion_tokens) AS completion_tokens, SUM(cached_tokens) AS cached_tokens "
            "FROM usage_daily WHERE day >= ? AND day < ? GROUP BY client, model",
            (f"{month}-01", f"{month}-32"),
        )

    def usage(self, days: int = 30) -> list[sqlite3.Row]:
        """Usage per day, client and model (cost is computed per model, then grouped)."""
        since = time.strftime("%Y-%m-%d", time.gmtime(time.time() - days * 86_400))
        return self.query(
            "SELECT day, client, model, SUM(requests) AS requests, SUM(errors) AS errors, "
            "SUM(prompt_tokens) AS prompt_tokens, SUM(completion_tokens) AS completion_tokens, "
            "SUM(cached_tokens) AS cached_tokens FROM usage_daily WHERE day >= ? "
            "GROUP BY day, client, model ORDER BY day DESC, client, model",
            (since,),
        )

    def used_models(self) -> list[str]:
        return [r[0] for r in self.query("SELECT DISTINCT model FROM usage_daily WHERE model != '-' ORDER BY model")]

    # -- model prices (manual overrides) ----------------------------------------------------

    def list_model_prices(self) -> dict[str, sqlite3.Row]:
        return {r["model"]: r for r in self.query("SELECT * FROM model_prices")}

    def set_model_price(
        self,
        model: str,
        actor: str,
        reference: str | None,
        input_price: float | None,
        output_price: float | None,
        cached_price: float | None,
    ) -> None:
        with self.tx() as db:
            if reference is None and input_price is None and output_price is None and cached_price is None:
                db.execute("DELETE FROM model_prices WHERE model = ?", (model,))
            else:
                db.execute(
                    "INSERT INTO model_prices (model, reference, input, output, cached_input, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(model) DO UPDATE SET reference = excluded.reference, "
                    "input = excluded.input, output = excluded.output, cached_input = excluded.cached_input, "
                    "updated_at = excluded.updated_at",
                    (model, reference, input_price, output_price, cached_price, time.time()),
                )
        self.audit(
            actor,
            "price.update",
            model,
            {"reference": reference, "input": input_price, "output": output_price, "cached": cached_price},
        )

    def recent_activity(self, since: float) -> list[sqlite3.Row]:
        """Dispatched requests since ``since``: to restore rate-limit windows after a restart."""
        return self.query(
            "SELECT client, ts, queue_ms, prompt_tokens, completion_tokens FROM requests "
            "WHERE ts >= ? AND queue_ms IS NOT NULL",
            (since,),
        )

    def prune_requests(self, keep_days: int) -> int:
        with self.tx() as db:
            cur = db.execute("DELETE FROM requests WHERE ts < ?", (time.time() - keep_days * 86_400,))
        return cur.rowcount

    # -- aliases -------------------------------------------------------------------------

    def list_aliases(self) -> list[Alias]:
        return [
            Alias(
                r["id"],
                r["name"],
                r["target_model"],
                tuple(json.loads(r["upstream_ids"])),
                bool(r["enabled"]),
                r["notes"],
                r["params"],
                tuple(json.loads(r["fallbacks"] or "[]")),
                tuple(json.loads(r["fallback_on"] or "[]")),
            )
            for r in self.query("SELECT * FROM aliases ORDER BY name")
        ]

    def create_alias(
        self,
        name: str,
        target_model: str,
        upstream_ids: tuple[int, ...],
        actor: str,
        notes: str = "",
        params: str = "",
        fallbacks: tuple[str, ...] = (),
        fallback_on: tuple[str, ...] = ("unavailable", "errors", "context"),
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO aliases (name, target_model, upstream_ids, notes, params, fallbacks, fallback_on, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    target_model,
                    json.dumps(list(upstream_ids)),
                    notes,
                    params,
                    json.dumps(list(fallbacks)),
                    json.dumps(list(fallback_on)),
                    time.time(),
                ),
            )
        self.audit(
            actor,
            "alias.create",
            name,
            {"target_model": target_model, "upstreams": list(upstream_ids), "fallbacks": list(fallbacks)},
        )
        return cur.lastrowid

    def update_alias(self, alias_id: int, actor: str, **fields: object) -> None:
        for name in ("upstream_ids", "fallbacks", "fallback_on"):
            if name in fields:
                fields[name] = json.dumps(list(fields[name]))
        allowed = {"target_model", "upstream_ids", "enabled", "notes", "params", "fallbacks", "fallback_on"}
        self._update("aliases", alias_id, allowed, fields)
        self.audit(actor, "alias.update", str(alias_id), fields)

    def delete_alias(self, alias_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM aliases WHERE id = ?", (alias_id,))
        self.audit(actor, "alias.delete", str(alias_id))

    # -- alerts, webhooks, notifications -------------------------------------------------

    def alert_rule_overrides(self) -> dict[str, sqlite3.Row]:
        return {r["kind"]: r for r in self.query("SELECT * FROM alert_rules")}

    def save_alert_rule(
        self,
        kind: str,
        enabled: bool,
        threshold: float | None,
        window_s: int | None,
        cooldown_s: int | None,
        actor: str,
    ) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO alert_rules (kind, enabled, threshold, window_s, cooldown_s) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(kind) DO UPDATE SET enabled = excluded.enabled, threshold = excluded.threshold, "
                "window_s = excluded.window_s, cooldown_s = excluded.cooldown_s",
                (kind, int(enabled), threshold, window_s, cooldown_s),
            )
        self.audit(
            actor,
            "alert.rule",
            kind,
            {"enabled": enabled, "threshold": threshold, "window_s": window_s, "cooldown_s": cooldown_s},
        )

    def list_webhooks(self) -> list[Webhook]:
        return [
            Webhook(
                r["id"],
                r["name"],
                r["kind"],
                self.decrypt(r["url"]),
                self.decrypt(r["secret"]) if r["secret"] else None,
                r["min_level"],
                bool(r["enabled"]),
                r["last_at"],
                None if r["last_ok"] is None else bool(r["last_ok"]),
                r["last_error"],
            )
            for r in self.query("SELECT * FROM webhooks ORDER BY name")
        ]

    def create_webhook(self, name: str, kind: str, url: str, secret: str | None, min_level: str, actor: str) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO webhooks (name, kind, url, secret, min_level, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (name, kind, self.encrypt(url), self.encrypt(secret) if secret else None, min_level, time.time()),
            )
        self.audit(actor, "webhook.create", name, {"kind": kind, "min_level": min_level})
        return cur.lastrowid

    def update_webhook(self, webhook_id: int, actor: str, **fields: object) -> None:
        if "url" in fields:
            fields["url"] = self.encrypt(str(fields["url"]))
        if "secret" in fields:
            fields["secret"] = self.encrypt(str(fields["secret"])) if fields["secret"] else None
        self._update("webhooks", webhook_id, {"url", "secret", "min_level", "enabled", "kind"}, fields)
        logged = {k: v for k, v in fields.items() if k not in ("url", "secret")}
        logged.update({k: "changed" for k in ("url", "secret") if k in fields})
        self.audit(actor, "webhook.update", str(webhook_id), logged)

    def webhook_result(self, webhook_id: int, ok: bool, error: str | None) -> None:
        with self.tx() as db:
            db.execute(
                "UPDATE webhooks SET last_at = ?, last_ok = ?, last_error = ? WHERE id = ?",
                (time.time(), int(ok), error, webhook_id),
            )

    def delete_webhook(self, webhook_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
        self.audit(actor, "webhook.delete", str(webhook_id))

    def add_notification(self, level: str, kind: str, title: str, body: str = "") -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO notifications (ts, level, kind, title, body) VALUES (?, ?, ?, ?, ?)",
                (time.time(), level, kind, title, body),
            )
        return cur.lastrowid

    def list_notifications(self, limit: int = 100) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (limit,))

    def unread_notifications(self) -> int:
        return self.query("SELECT COUNT(*) AS n FROM notifications WHERE read = 0")[0]["n"]

    def mark_notifications_read(self) -> None:
        with self.tx() as db:
            db.execute("UPDATE notifications SET read = 1 WHERE read = 0")

    def prune_notifications(self, keep_days: int = 30, keep_rows: int = 1000) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM notifications WHERE ts < ?", (time.time() - keep_days * 86_400,))
            db.execute(
                "DELETE FROM notifications WHERE id NOT IN (SELECT id FROM notifications ORDER BY id DESC LIMIT ?)",
                (keep_rows,),
            )

    def tokens_on(self, day: str) -> dict[str, int]:
        """Tokens (prompt + completion) per client on a UTC day (YYYY-MM-DD), from the rollup."""
        rows = self.query(
            "SELECT client, SUM(prompt_tokens + completion_tokens) AS n FROM usage_daily WHERE day = ? GROUP BY client",
            (day,),
        )
        return {r["client"]: int(r["n"] or 0) for r in rows}

    # -- async jobs, files, batches ------------------------------------------------------

    def create_jobs(self, jobs: list[dict]) -> None:
        """Insert jobs (``request`` is a dict, stored encrypted)."""
        with self.tx() as db:
            for job in jobs:
                db.execute(
                    "INSERT INTO jobs (id, client, batch_id, custom_id, status, priority, endpoint, request, run_at, "
                    "deadline, idle_only, callback_url, callback_secret, tags, created) "
                    "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        job["id"],
                        job["client"],
                        job.get("batch_id"),
                        job.get("custom_id"),
                        job.get("priority", "low"),
                        job["endpoint"],
                        self.encrypt(json.dumps(job["request"])),
                        job.get("run_at"),
                        job.get("deadline"),
                        int(bool(job.get("idle_only"))),
                        job.get("callback_url"),
                        self.encrypt(job["callback_secret"]) if job.get("callback_secret") else None,
                        ",".join(job.get("tags") or ()) or None,
                        job.get("created", time.time()),
                    ),
                )

    def get_job(self, job_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM jobs WHERE id = ?", [job_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def job_request(self, row: sqlite3.Row) -> dict:
        return json.loads(self.decrypt(row["request"]))

    def job_result(self, row: sqlite3.Row) -> object:
        return json.loads(self.decrypt(row["result"])) if row["result"] else None

    def list_jobs(
        self,
        client: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        sql, params = "SELECT * FROM jobs WHERE 1=1", []
        for column, value in (("client", client), ("status", status), ("batch_id", batch_id)):
            if value:
                sql += f" AND {column} = ?"
                params.append(value)
        if tag:
            sql += " AND (',' || tags || ',') LIKE ?"
            params.append(f"%,{tag},%")
        sql += " ORDER BY created DESC LIMIT ?"
        params.append(limit)
        return self.query(sql, tuple(params))

    def due_jobs(self, now: float, limit: int) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM jobs WHERE status = 'queued' AND (run_at IS NULL OR run_at <= ?) "
            "ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, created LIMIT ?",
            (now, limit),
        )

    def claim_job(self, job_id: str) -> bool:
        """queued -> running; False if someone else (or a cancel) got there first."""
        with self.tx() as db:
            cur = db.execute(
                "UPDATE jobs SET status = 'running', started = ?, attempts = attempts + 1 "
                "WHERE id = ? AND status = 'queued'",
                (time.time(), job_id),
            )
        return cur.rowcount == 1

    def finish_job(self, job_id: str, status: str, **fields: object) -> None:
        if "result" in fields and fields["result"] is not None:
            fields["result"] = self.encrypt(json.dumps(fields["result"]))
        fields = {"status": status, "finished": time.time(), **fields}
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE jobs SET {columns} WHERE id = ?", (*fields.values(), job_id))  # noqa: S608

    def set_job_fields(self, job_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE jobs SET {columns} WHERE id = ?", (*fields.values(), job_id))  # noqa: S608

    def cancel_jobs(self, job_ids: list[str] | None = None, batch_id: str | None = None) -> int:
        """Cancel jobs that haven't started (running ones finish)."""
        with self.tx() as db:
            if batch_id is not None:
                cur = db.execute(
                    "UPDATE jobs SET status = 'cancelled', finished = ? WHERE batch_id = ? AND status = 'queued'",
                    (time.time(), batch_id),
                )
            else:
                marks = ",".join("?" for _ in job_ids or [])
                cur = db.execute(
                    f"UPDATE jobs SET status = 'cancelled', finished = ? WHERE status = 'queued' AND id IN ({marks})",  # noqa: S608
                    (time.time(), *(job_ids or [])),
                )
        return cur.rowcount

    def retry_job(self, job_id: str) -> bool:
        with self.tx() as db:
            cur = db.execute(
                "UPDATE jobs SET status = 'queued', finished = NULL, started = NULL, error = NULL, result = NULL, "
                "status_code = NULL WHERE id = ? AND status IN ('failed', 'expired', 'cancelled')",
                (job_id,),
            )
        return cur.rowcount == 1

    def requeue_running_jobs(self, max_attempts: int) -> int:
        """After a restart: jobs that were running start again (or fail after too many tries)."""
        with self.tx() as db:
            db.execute(
                "UPDATE jobs SET status = 'failed', finished = ?, error = 'interrupted by restarts too often' "
                "WHERE status = 'running' AND attempts >= ?",
                (time.time(), max_attempts),
            )
            cur = db.execute("UPDATE jobs SET status = 'queued', started = NULL WHERE status = 'running'")
        return cur.rowcount

    def expire_jobs(self, now: float) -> list[str]:
        """Queued jobs past their deadline -> expired; returns their batch ids."""
        rows = self.query(
            "SELECT id, batch_id FROM jobs WHERE status = 'queued' AND deadline IS NOT NULL AND deadline < ?", (now,)
        )
        if rows:
            with self.tx() as db:
                db.executemany(
                    "UPDATE jobs SET status = 'expired', finished = ?, error = 'deadline passed before it could run' "
                    "WHERE id = ? AND status = 'queued'",
                    [(now, r["id"]) for r in rows],
                )
        return sorted({r["batch_id"] for r in rows if r["batch_id"]})

    def jobs_by_status(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self.query("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")}

    def job_counts(self, batch_id: str) -> dict[str, int]:
        rows = self.query("SELECT status, COUNT(*) AS n FROM jobs WHERE batch_id = ? GROUP BY status", (batch_id,))
        return {r["status"]: r["n"] for r in rows}

    def prune_jobs(self, keep_days: float) -> None:
        cutoff = time.time() - keep_days * 86_400
        with self.tx() as db:
            db.execute(f"DELETE FROM jobs WHERE status IN {JOB_DONE!r} AND finished < ?", (cutoff,))  # noqa: S608
            db.execute("DELETE FROM batches WHERE finished_at IS NOT NULL AND finished_at < ?", (cutoff,))
            db.execute(
                "DELETE FROM files WHERE created < ? AND id NOT IN (SELECT input_file_id FROM batches) "
                "AND id NOT IN (SELECT output_file_id FROM batches WHERE output_file_id IS NOT NULL) "
                "AND id NOT IN (SELECT error_file_id FROM batches WHERE error_file_id IS NOT NULL)",
                (cutoff,),
            )

    def create_schedule(self, s: dict) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO schedules (id, client, name, cron, endpoint, request, priority, tags, idle_only, "
                "deadline_s, callback_url, callback_secret, enabled, created, next_run) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    s["id"],
                    s["client"],
                    s["name"],
                    s["cron"],
                    s["endpoint"],
                    self.encrypt(json.dumps(s["request"])),
                    s.get("priority", "low"),
                    ",".join(s.get("tags") or ()) or None,
                    int(bool(s.get("idle_only"))),
                    s.get("deadline_s"),
                    s.get("callback_url"),
                    self.encrypt(s["callback_secret"]) if s.get("callback_secret") else None,
                    int(s.get("enabled", True)),
                    time.time(),
                    s["next_run"],
                ),
            )

    def list_schedules(self, client: str | None = None) -> list[sqlite3.Row]:
        if client is None:
            return self.query("SELECT * FROM schedules ORDER BY client, name")
        return self.query("SELECT * FROM schedules WHERE client = ? ORDER BY name", (client,))

    def get_schedule(self, schedule_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM schedules WHERE id = ?", [schedule_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def due_schedules(self, now: float) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM schedules WHERE enabled = 1 AND next_run <= ? ORDER BY next_run", (now,))

    def set_schedule(self, schedule_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        values = [int(v) if isinstance(v, bool) else v for v in fields.values()]
        with self.tx() as db:
            db.execute(f"UPDATE schedules SET {columns} WHERE id = ?", (*values, schedule_id))  # noqa: S608

    def delete_schedule(self, schedule_id: str, client: str | None = None) -> bool:
        with self.tx() as db:
            if client is None:
                cur = db.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
            else:
                cur = db.execute("DELETE FROM schedules WHERE id = ? AND client = ?", (schedule_id, client))
        return cur.rowcount == 1

    def create_file(self, file_id: str, client: str, purpose: str, filename: str, data: bytes) -> sqlite3.Row:
        with self.tx() as db:
            db.execute(
                "INSERT INTO files (id, client, purpose, filename, bytes, created, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (file_id, client, purpose, filename, len(data), time.time(), self.encrypt(data.decode("utf-8"))),
            )
        return self.get_file(file_id, client)

    def get_file(self, file_id: str, client: str) -> sqlite3.Row | None:
        rows = self.query("SELECT * FROM files WHERE id = ? AND client = ?", (file_id, client))
        return rows[0] if rows else None

    def file_content(self, row: sqlite3.Row) -> str:
        return self.decrypt(row["data"])

    def delete_file(self, file_id: str, client: str) -> bool:
        with self.tx() as db:
            cur = db.execute("DELETE FROM files WHERE id = ? AND client = ?", (file_id, client))
        return cur.rowcount == 1

    def create_batch(self, batch: dict) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO batches (id, client, endpoint, input_file_id, completion_window, status, created, "
                "expires_at, total, metadata) VALUES (?, ?, ?, ?, ?, 'in_progress', ?, ?, ?, ?)",
                (
                    batch["id"],
                    batch["client"],
                    batch["endpoint"],
                    batch["input_file_id"],
                    batch["completion_window"],
                    batch["created"],
                    batch["expires_at"],
                    batch["total"],
                    json.dumps(batch.get("metadata")) if batch.get("metadata") else None,
                ),
            )

    def get_batch(self, batch_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM batches WHERE id = ?", [batch_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def list_batches(self, client: str, limit: int = 20, after: str | None = None) -> list[sqlite3.Row]:
        if after and (anchor := self.get_batch(after, client)):
            return self.query(
                "SELECT * FROM batches WHERE client = ? AND created < ? ORDER BY created DESC LIMIT ?",
                (client, anchor["created"], limit),
            )
        return self.query("SELECT * FROM batches WHERE client = ? ORDER BY created DESC LIMIT ?", (client, limit))

    def update_batch(self, batch_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE batches SET {columns} WHERE id = ?", (*fields.values(), batch_id))  # noqa: S608

    # -- opt-in content logging ----------------------------------------------------------

    def save_contents(self, rows: list[tuple[str, float, str, str, str, bool]]) -> None:
        """(request id, ts, client, request text, reply text, truncated)."""
        with self.tx() as db:
            db.executemany(
                "INSERT OR REPLACE INTO request_content (id, ts, client, request, response, truncated) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(i, ts, c, self.encrypt(req), self.encrypt(resp), int(cut)) for i, ts, c, req, resp, cut in rows],
            )

    def get_content(self, request_id: str) -> dict | None:
        rows = self.query("SELECT * FROM request_content WHERE id = ?", (request_id,))
        if not rows:
            return None
        row = rows[0]
        return {
            "id": row["id"],
            "ts": row["ts"],
            "client": row["client"],
            "request": self.decrypt(row["request"]),
            "response": self.decrypt(row["response"]),
            "truncated": bool(row["truncated"]),
        }

    def content_ids(self, request_ids: list[str]) -> set[str]:
        if not request_ids:
            return set()
        marks = ",".join("?" for _ in request_ids)
        rows = self.query(f"SELECT id FROM request_content WHERE id IN ({marks})", tuple(request_ids))  # noqa: S608
        return {r["id"] for r in rows}

    def purge_content(self, client: str | None, actor: str) -> int:
        with self.tx() as db:
            if client is None:
                cur = db.execute("DELETE FROM request_content")
            else:
                cur = db.execute("DELETE FROM request_content WHERE client = ?", (client,))
        self.audit(actor, "content.purge", client or "*", {"rows": cur.rowcount})
        return cur.rowcount

    def prune_content(self, keep_days: float) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM request_content WHERE ts < ?", (time.time() - keep_days * 86_400,))

    # -- Responses API conversations ----------------------------------------------------

    def save_response(self, response_id: str, client: str, messages: list[dict]) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO stored_responses (id, client, created, data) VALUES (?, ?, ?, ?)",
                (response_id, client, time.time(), self.encrypt(json.dumps(messages))),
            )

    def load_response(self, response_id: str, client: str) -> list[dict] | None:
        """The whole conversation up to that response, or None (unknown, expired or another client's)."""
        rows = self.query("SELECT data FROM stored_responses WHERE id = ? AND client = ?", (response_id, client))
        return json.loads(self.decrypt(rows[0]["data"])) if rows else None

    def prune_responses(self, keep_days: float) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM stored_responses WHERE created < ?", (time.time() - keep_days * 86_400,))

    # -- admin users and sessions --------------------------------------------------------

    def count_admins(self, role: str | None = None, enabled_only: bool = False) -> int:
        sql, params = "SELECT COUNT(*) AS n FROM admin_users WHERE 1=1", []
        if role:
            sql += " AND role = ?"
            params.append(role)
        if enabled_only:
            sql += " AND enabled = 1"
        return self.query(sql, tuple(params))[0]["n"]

    def list_admins(self) -> list[AdminUser]:
        return [_admin(r) for r in self.query("SELECT * FROM admin_users ORDER BY username")]

    def get_admin(self, user_id: int | None = None, username: str | None = None) -> AdminUser | None:
        if user_id is not None:
            rows = self.query("SELECT * FROM admin_users WHERE id = ?", (user_id,))
        else:
            rows = self.query("SELECT * FROM admin_users WHERE username = ?", (username,))
        return _admin(rows[0]) if rows else None

    def password_hash(self, user_id: int) -> str:
        return self.query("SELECT password_hash FROM admin_users WHERE id = ?", (user_id,))[0]["password_hash"]

    def create_admin(self, username: str, password_hash: str, role: str, actor: str) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO admin_users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                (username, password_hash, role, time.time()),
            )
        self.audit(actor, "user.create", username, {"role": role})
        return cur.lastrowid

    def update_admin(self, user_id: int, actor: str, **fields: object) -> None:
        allowed = {"role", "enabled", "password_hash", "can_view_content"}
        self._update("admin_users", user_id, allowed, fields)
        if "password_hash" in fields or fields.get("enabled") is False:
            self.delete_sessions(user_id)
        logged = {k: v for k, v in fields.items() if k != "password_hash"}
        if "password_hash" in fields:
            logged["password"] = "changed"
        self.audit(actor, "user.update", str(user_id), logged)

    def totp_state(self, user_id: int) -> tuple[str | None, int, list[str]]:
        """(secret or None, last used time step, recovery code hashes)."""
        row = self.query("SELECT totp_secret, totp_last, recovery_codes FROM admin_users WHERE id = ?", (user_id,))[0]
        secret = self.decrypt(row["totp_secret"]) if row["totp_secret"] else None
        return secret, row["totp_last"], json.loads(row["recovery_codes"] or "[]")

    def set_totp(self, user_id: int, secret: str | None, recovery_hashes: list[str], actor: str) -> None:
        """Turn two-factor login on (secret) or off (None)."""
        with self.tx() as db:
            db.execute(
                "UPDATE admin_users SET totp_secret = ?, totp_last = -1, recovery_codes = ? WHERE id = ?",
                (self.encrypt(secret) if secret else None, json.dumps(recovery_hashes), user_id),
            )
        self.audit(actor, "user.2fa", str(user_id), {"enabled": secret is not None})

    def totp_used(self, user_id: int, counter: int) -> None:
        with self.tx() as db:
            db.execute("UPDATE admin_users SET totp_last = MAX(totp_last, ?) WHERE id = ?", (counter, user_id))

    def use_recovery_code(self, user_id: int, code_hash: str) -> bool:
        """Consume a recovery code; False if it isn't (or no longer) valid."""
        with self.tx() as db:
            row = db.execute("SELECT recovery_codes FROM admin_users WHERE id = ?", (user_id,)).fetchone()
            codes = json.loads(row["recovery_codes"] or "[]") if row else []
            if code_hash not in codes:
                return False
            codes.remove(code_hash)
            db.execute("UPDATE admin_users SET recovery_codes = ? WHERE id = ?", (json.dumps(codes), user_id))
        return True

    def delete_admin(self, user_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_users WHERE id = ?", (user_id,))
        self.audit(actor, "user.delete", str(user_id))

    def create_session(self, user_id: int, token_hash: str, csrf: str, ip: str, ttl_s: float) -> None:
        now = time.time()
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE expires_at < ?", (now,))
            db.execute(
                "INSERT INTO admin_sessions (token_hash, user_id, csrf, ip, created_at, last_seen, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (token_hash, user_id, csrf, ip, now, now, now + ttl_s),
            )

    def get_session(self, token_hash: str) -> sqlite3.Row | None:
        rows = self.query("SELECT * FROM admin_sessions WHERE token_hash = ?", (token_hash,))
        return rows[0] if rows else None

    def touch_session(self, token_hash: str) -> None:
        with self.tx() as db:
            db.execute("UPDATE admin_sessions SET last_seen = ? WHERE token_hash = ?", (time.time(), token_hash))

    def delete_session(self, token_hash: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE token_hash = ?", (token_hash,))

    def delete_sessions(self, user_id: int) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE user_id = ?", (user_id,))

    def list_audit(self, limit: int = 200) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))

    # -- helpers -------------------------------------------------------------------------

    def _update(self, table: str, row_id: int, allowed: set[str], fields: dict[str, object]) -> None:
        bad = set(fields) - allowed
        if bad:
            raise ValueError(f"cannot update {', '.join(sorted(bad))}")
        if not fields:
            return
        columns = ", ".join(f"{name} = ?" for name in fields)
        values = [int(v) if isinstance(v, bool) else v for v in fields.values()]
        with self.tx() as db:
            cur = db.execute(f"UPDATE {table} SET {columns} WHERE id = ?", (*values, row_id))  # noqa: S608
            if cur.rowcount == 0:
                raise KeyError(row_id)
