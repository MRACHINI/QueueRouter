"""Backups: download, automatic copies, restore (staged, applied on the next start)."""

import asyncio
import io
import json
import os
import time
import zipfile

import httpx

from app.backup import apply_staged_restore, auto_backup, list_backups
from app.config import Bootstrap
from app.state import open_store
from app.store import Store
from tests.test_admin import OWNER, csrf_of, login


def test_auto_backup_keeps_the_newest(tmp_path):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    day = 86_400
    for n in range(5):
        auto_backup(store, str(tmp_path), keep=3, now=1_800_000_000 + n * day)
    assert auto_backup(store, str(tmp_path), keep=3, now=1_800_000_000 + 4 * day) is None  # once a day
    autos = sorted(f.name for f in list_backups(str(tmp_path)) if f.name.startswith("auto-"))
    assert len(autos) == 3 and autos[-1] == "auto-2027-01-19.db"
    assert auto_backup(store, str(tmp_path), keep=0) is None
    store.close()


def test_apply_staged_restore(tmp_path):
    data = str(tmp_path)
    store = open_store(Bootstrap(data_dir=data, admin_username="a", admin_password="x" * 12), None)
    store.create_admin("old", "h", "owner", "t")
    os.makedirs(tmp_path / "restore")
    store.backup_to(str(tmp_path / "restore" / "queuerouter.db"))  # the "backup" has user "old"
    store.create_admin("newer", "h", "viewer", "t")
    store.close()
    assert apply_staged_restore(data)
    assert not (tmp_path / "restore").exists()
    reopened = Store.open(data, None)
    names = {u.username for u in reopened.list_admins()}
    assert "old" in names and "newer" not in names
    assert any(f.name.startswith("pre-restore-") for f in list_backups(data))
    reopened.close()
    assert not apply_staged_restore(data)  # nothing staged


async def test_download_and_restore(make_stack):
    stack = make_stack(bootstrap=OWNER)
    restarts = []
    stack.app.state.restart = lambda: restarts.append(time.time())
    data_dir = stack.app.state.bootstrap.data_dir
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = await c.get("/admin/backup")
        assert page.status_code == 200 and "include the encryption key" in page.text
        csrf = csrf_of(page.text)

        r = await c.post("/admin/backup/download", data={"csrf": csrf, "include_key": "on"})
        assert r.headers["content-type"] == "application/zip"
        archive = zipfile.ZipFile(io.BytesIO(r.content))
        assert set(archive.namelist()) == {"queuerouter.db", "secret.key", "manifest.json"}
        assert json.loads(archive.read("manifest.json"))["includes_key"] is True

        # garbage is refused with a reason; nothing is staged
        r = await c.post(
            "/admin/backup/restore", data={"csrf": csrf}, files={"file": ("x.db", b"not a database", "application/x")}
        )
        assert "isn" in r.text and "QueueRouter database" in r.text
        assert not await asyncio.to_thread(os.path.exists, os.path.join(data_dir, "restore"))

        # a zip without its key whose secrets were encrypted with another key is refused
        other = io.BytesIO()
        with zipfile.ZipFile(other, "w") as zf:
            zf.writestr("queuerouter.db", archive.read("queuerouter.db"))
            zf.writestr("secret.key", b"q" * 43 + b"=")  # valid-looking but wrong key
        r = await c.post(
            "/admin/backup/restore", data={"csrf": csrf}, files={"file": ("b.zip", other.getvalue(), "application/zip")}
        )
        assert "different key" in r.text or "key in the backup is invalid" in r.text

        good = (await c.post("/admin/backup/download", data={"csrf": csrf, "include_key": "on"})).content
        r = await c.post(
            "/admin/backup/restore", data={"csrf": csrf}, files={"file": ("backup.zip", good, "application/zip")}
        )
        assert "Restoring" in r.text
        assert await asyncio.to_thread(os.path.isfile, os.path.join(data_dir, "restore", "queuerouter.db"))
        for _ in range(40):
            if restarts:
                break
            await asyncio.sleep(0.1)
        assert restarts  # QueueRouter restarts itself to load it

        # listed files can be downloaded; names can't escape /data/backups
        r = await c.get("/admin/backup/files/..%2Fsecret.key")
        assert r.status_code in (303, 404)

        # only owners
        csrf = csrf_of((await c.get("/admin/users")).text)
        await c.post(
            "/admin/users", data={"csrf": csrf, "username": "adm", "role": "admin", "password": "admin password!"}
        )
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as admin:
        await login(admin, stack.url, "adm", "admin password!")
        assert (await admin.get("/admin/backup")).status_code == 403
