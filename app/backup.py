"""Backups and restore.

- Download: a zip with a consistent copy of the database, the encryption key (optional) and a
  manifest.
- Automatic: one database copy per day in ``/data/backups`` (``auto-YYYY-MM-DD.db``), the last N kept.
- Restore: an uploaded zip (or a ``.db`` from ``/data/backups``) is checked, then *staged* in
  ``/data/restore``; it replaces the database on the next start (QueueRouter restarts itself), after
  the current database is saved as ``pre-restore-<time>.db``.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
import zipfile
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from .store import MIGRATIONS, Store

log = logging.getLogger("queuerouter")

DB_NAME, KEY_NAME, MANIFEST_NAME = "queuerouter.db", "secret.key", "manifest.json"
RESTORE_DIR = "restore"
BACKUP_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.db")
MAX_UPLOAD_BYTES = 2 * 1024**3
MAX_RESTORE_DB_BYTES = 8 * 1024**3  # unpacked (a small zip could otherwise fill the disk)


class RestoreError(ValueError):
    """The upload can't be restored; the message says why."""


@dataclass(frozen=True)
class BackupFile:
    name: str
    size: int
    mtime: float


def backups_dir(data_dir: str) -> str:
    path = os.path.join(data_dir, "backups")
    os.makedirs(path, exist_ok=True)
    return path


def list_backups(data_dir: str) -> list[BackupFile]:
    folder = backups_dir(data_dir)
    files = []
    for name in os.listdir(folder):
        if BACKUP_FILE.fullmatch(name):
            st = os.stat(os.path.join(folder, name))
            files.append(BackupFile(name, st.st_size, st.st_mtime))
    return sorted(files, key=lambda f: f.mtime, reverse=True)


def backup_path(data_dir: str, name: str) -> str | None:
    """Path of a file in /data/backups, or None for anything that isn't one (no traversal)."""
    if not BACKUP_FILE.fullmatch(name):
        return None
    path = os.path.join(backups_dir(data_dir), name)
    return path if os.path.isfile(path) else None


def make_archive(store: Store, data_dir: str, include_key: bool, version: str) -> bytes:
    key_path = os.path.join(data_dir, KEY_NAME)
    with tempfile.TemporaryDirectory(dir=data_dir, prefix=".backup-") as tmp:
        db_copy = os.path.join(tmp, DB_NAME)
        store.backup_to(db_copy)
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(db_copy, DB_NAME)
            has_key = include_key and os.path.exists(key_path)
            if has_key:
                zf.write(key_path, KEY_NAME)
            manifest = {
                "app": "queuerouter",
                "version": version,
                "schema": store.schema_version(),
                "created": time.time(),
                "includes_key": has_key,
            }
            zf.writestr(MANIFEST_NAME, json.dumps(manifest, indent=2))
    return out.getvalue()


def auto_backup(store: Store, data_dir: str, keep: int, now: float | None = None) -> str | None:
    """Today's automatic copy (once a day), keeping the newest ``keep``. Returns the new file name."""
    if keep <= 0:
        return None
    folder = backups_dir(data_dir)
    name = f"auto-{time.strftime('%Y-%m-%d', time.gmtime(now or time.time()))}.db"
    made = None
    if not os.path.exists(os.path.join(folder, name)):
        partial = os.path.join(folder, f".{name}.partial")
        store.backup_to(partial)
        os.replace(partial, os.path.join(folder, name))
        made = name
        log.info(json.dumps({"event": "auto_backup", "file": name}))
    autos = sorted(n for n in os.listdir(folder) if n.startswith("auto-") and n.endswith(".db"))
    for old in autos[:-keep]:
        os.remove(os.path.join(folder, old))
    return made


def _check_database(path: str, fernet: Fernet) -> dict:
    try:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise RestoreError("That isn't a QueueRouter database.") from exc
    try:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RestoreError("The database in the backup is damaged.")
        schema = db.execute("PRAGMA user_version").fetchone()[0]
        if not 1 <= schema <= len(MIGRATIONS):
            raise RestoreError(
                "That backup is from a newer QueueRouter; upgrade first."
                if schema > len(MIGRATIONS)
                else "That isn't a QueueRouter database."
            )
        owners = db.execute("SELECT COUNT(*) FROM admin_users WHERE role = 'owner' AND enabled = 1").fetchone()[0]
        sample = db.execute("SELECT api_key_enc FROM upstreams WHERE api_key_enc != '' LIMIT 1").fetchone()
        if sample is not None:
            try:
                fernet.decrypt(sample[0].encode())
            except InvalidToken as exc:
                raise RestoreError(
                    "The upstream keys in this backup were encrypted with a different key. Use a backup "
                    "that includes its encryption key (or set the matching SECRET_KEY)."
                ) from exc
        return {
            "schema": schema,
            "owners": owners,
            "clients": db.execute("SELECT COUNT(*) FROM clients").fetchone()[0],
            "upstreams": db.execute("SELECT COUNT(*) FROM upstreams").fetchone()[0],
        }
    except sqlite3.DatabaseError as exc:
        raise RestoreError("That isn't a QueueRouter database.") from exc
    finally:
        db.close()


def stage_restore(data_dir: str, upload_path: str, current: Fernet, env_key: bool) -> dict:
    """Check an uploaded zip or .db and stage it for the next start. Returns a summary."""
    with tempfile.TemporaryDirectory(dir=data_dir, prefix=".restore-") as tmp:
        db_path, key_bytes = os.path.join(tmp, DB_NAME), None
        if zipfile.is_zipfile(upload_path):
            with zipfile.ZipFile(upload_path) as zf:
                names = set(zf.namelist())
                if DB_NAME not in names:
                    raise RestoreError("The zip has no queuerouter.db; is it a QueueRouter backup?")
                if zf.getinfo(DB_NAME).file_size > MAX_RESTORE_DB_BYTES:
                    raise RestoreError("The database in the backup is too large.")
                with zf.open(DB_NAME) as src, open(db_path, "wb") as dst:
                    copied = 0
                    while chunk := src.read(1024 * 1024):  # count as we go: sizes in a zip can lie
                        copied += len(chunk)
                        if copied > MAX_RESTORE_DB_BYTES:
                            raise RestoreError("The database in the backup is too large.")
                        dst.write(chunk)
                if KEY_NAME in names:
                    if zf.getinfo(KEY_NAME).file_size > 1024:
                        raise RestoreError("The encryption key in the backup is invalid.")
                    key_bytes = zf.read(KEY_NAME).strip()
        else:
            shutil.copyfile(upload_path, db_path)
        fernet = current
        if key_bytes and not env_key:  # with SECRET_KEY set, the environment's key always wins
            try:
                fernet = Fernet(key_bytes)
            except ValueError as exc:
                raise RestoreError("The encryption key in the backup is invalid.") from exc
        summary = _check_database(db_path, fernet)
        if summary["owners"] < 1:
            raise RestoreError("That backup has no enabled owner account, so nobody could sign in.")
        staged = os.path.join(data_dir, RESTORE_DIR)
        shutil.rmtree(staged, ignore_errors=True)
        os.makedirs(os.path.join(tmp, RESTORE_DIR))
        os.replace(db_path, os.path.join(tmp, RESTORE_DIR, DB_NAME))
        if key_bytes and not env_key:
            with open(os.path.join(tmp, RESTORE_DIR, KEY_NAME), "wb") as f:
                f.write(key_bytes)
        os.replace(os.path.join(tmp, RESTORE_DIR), staged)
    summary["replaces_key"] = bool(key_bytes and not env_key)
    return summary


def apply_staged_restore(data_dir: str) -> bool:
    """On start, before the database is opened: swap in a staged restore (keeping the old copy)."""
    staged = os.path.join(data_dir, RESTORE_DIR)
    staged_db = os.path.join(staged, DB_NAME)
    if not os.path.isfile(staged_db):
        return False
    folder = backups_dir(data_dir)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    live = os.path.join(data_dir, DB_NAME)
    if os.path.exists(live):
        old = sqlite3.connect(live)  # folds in the WAL, so the saved copy is complete
        try:
            target = sqlite3.connect(os.path.join(folder, f"pre-restore-{stamp}.db"))
            old.backup(target)
            target.close()
        finally:
            old.close()
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(live + suffix):
            os.remove(live + suffix)
    os.replace(staged_db, live)
    staged_key = os.path.join(staged, KEY_NAME)
    key_path = os.path.join(data_dir, KEY_NAME)
    if os.path.isfile(staged_key):
        if os.path.exists(key_path):
            shutil.copyfile(key_path, os.path.join(folder, f"pre-restore-{stamp}.key"))
            os.chmod(os.path.join(folder, f"pre-restore-{stamp}.key"), 0o600)
        os.replace(staged_key, key_path)
        os.chmod(key_path, 0o600)
    shutil.rmtree(staged, ignore_errors=True)
    log.warning(json.dumps({"event": "restored_backup", "previous": f"backups/pre-restore-{stamp}.db"}))
    return True
