import os
import sqlite3

import pytest
from cryptography.fernet import Fernet

from app.config import Bootstrap, Settings
from app.state import load_snapshot, open_store
from app.store import MIGRATIONS, Store, hash_key

SEED = Settings(
    upstream_url="https://api.arliai.com/v1",
    upstream_api_key="arli-secret",
    max_concurrency=2,
    access_keys=("opencode:key-one:high", "key-two"),
    aging_s=30,
)


@pytest.fixture
def data_dir(tmp_path):
    return str(tmp_path)


def test_first_start_imports_env(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    snap = load_snapshot(store)
    assert snap.settings.upstream_api_key == "arli-secret"
    assert snap.settings.max_concurrency == 2
    assert snap.settings.aging_s == 30
    assert snap.upstream.pool.name == "arliai-main"
    assert {c.name: c.priority for c in snap.clients.values()} == {"opencode": "high", "client2": "normal"}
    assert snap.clients[hash_key("key-one")].name == "opencode"


def test_secrets_are_not_stored_in_plaintext(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    store.close()
    raw = open(os.path.join(data_dir, "queuerouter.db"), "rb").read()
    raw += b"".join(open(os.path.join(data_dir, f), "rb").read() for f in os.listdir(data_dir) if "-wal" in f)
    assert b"arli-secret" not in raw
    assert b"key-one" not in raw


def test_secret_key_file_created_private(data_dir):
    open_store(Bootstrap(data_dir=data_dir), SEED)
    path = os.path.join(data_dir, "secret.key")
    assert os.path.exists(path)
    if os.name == "posix":
        assert os.stat(path).st_mode & 0o077 == 0


def test_explicit_secret_key_and_wrong_key(data_dir):
    key = Fernet.generate_key().decode()
    store = open_store(Bootstrap(data_dir=data_dir, secret_key=key), SEED)
    store.close()
    other = Store.open(data_dir, Fernet.generate_key().decode())
    with pytest.raises(RuntimeError, match="SECRET_KEY"):
        load_snapshot(other)


def test_env_only_imported_once(data_dir):
    open_store(Bootstrap(data_dir=data_dir), SEED).close()
    changed = Settings(upstream_api_key="different", max_concurrency=9, access_keys=("x:y",))
    snap = load_snapshot(open_store(Bootstrap(data_dir=data_dir), changed))
    assert snap.settings.upstream_api_key == "arli-secret"
    assert snap.settings.max_concurrency == 2
    assert "x" not in {c.name for c in snap.clients.values()}


def test_first_start_without_upstream(data_dir):
    snap = load_snapshot(open_store(Bootstrap(data_dir=data_dir), Settings()))
    assert snap.upstream is None and snap.clients == {}


def test_migrations_are_idempotent_and_versioned(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    assert store.schema_version() == len(MIGRATIONS)
    store.migrate()
    store.close()
    db = sqlite3.connect(os.path.join(data_dir, "queuerouter.db"))
    db.execute(f"PRAGMA user_version={len(MIGRATIONS) + 1}")
    db.close()
    with pytest.raises(RuntimeError, match="newer"):
        Store.open(data_dir, None)


def test_client_lifecycle(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    key = store.create_client("scripts", "admin", priority="low")
    assert key.startswith("qr-")
    client = next(c for c in store.list_clients() if c.name == "scripts")
    assert client.key_hash == hash_key(key) and client.key_prefix == key[:6]

    store.update_client(client.id, "admin", enabled=False)
    assert "scripts" not in {c.name for c in load_snapshot(store).clients.values()}

    store.update_client(client.id, "admin", enabled=True)
    new_key = store.regenerate_client_key(client.id, "admin")
    clients = load_snapshot(store).clients
    assert hash_key(key) not in clients and clients[hash_key(new_key)].name == "scripts"

    store.delete_client(client.id, "admin")
    assert "scripts" not in {c.name for c in store.list_clients()}
    with pytest.raises(ValueError):
        store.update_client(1, "admin", priority="urgent")
    actions = [r["action"] for r in store.query("SELECT action FROM audit_log ORDER BY id")]
    assert actions[0] == "import" and "client.delete" in actions


def test_expired_client_is_rejected(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    store.create_client("temp", "admin", expires_at=1.0)
    assert "temp" not in {c.name for c in load_snapshot(store).clients.values()}


def test_tunables_and_pool_updates(data_dir):
    store = open_store(Bootstrap(data_dir=data_dir), SEED)
    store.set_tunables({"aging_s": 5, "session_aware": False}, "admin")
    with pytest.raises(ValueError):
        store.set_tunables({"nope": 1}, "admin")
    pool = store.list_pools()[0]
    store.update_pool(pool.id, "admin", max_concurrency=6)
    upstream = store.list_upstreams()[0]
    store.update_upstream(upstream.id, "admin", api_key="rotated")
    snap = load_snapshot(store)
    assert (snap.settings.aging_s, snap.settings.session_aware) == (5, False)
    assert snap.settings.max_concurrency == 6
    assert snap.settings.upstream_api_key == "rotated"
