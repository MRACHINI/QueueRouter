"""Admin, jobs, restore and configuration fixes from the third code review: out-of-range form values are a
"not saved" page (not a 500), a failed "Add upstream" leaves no pool behind, restoring an unreadable zip says so,
a restore interrupted between database and key finishes on the next start, retried jobs really run again,
"spare capacity" jobs can't crowd out the others, and settings from the environment are checked."""

import io
import ipaddress
import os
import time
import zipfile

import httpx
import pytest
from cryptography.fernet import Fernet

from app import backup
from app.config import Bootstrap, Settings
from app.jobs import new_job
from app.state import open_store
from tests.test_admin import OWNER, csrf_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


async def test_out_of_range_values_and_a_failed_add_upstream(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        client = store.list_clients()[0]
        csrf = csrf_of((await c.get("/admin/clients")).text)
        r = await c.post(f"/admin/clients/{client.id}", data={"csrf": csrf, "expires": "9999-12-31"})
        assert r.status_code == 400 and "out of range" in r.text  # the day after can't be stored

        pools = len(store.list_pools())
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        form = {"csrf": csrf, "name": "second", "preset": "custom", "base_url": "http://127.0.0.1:9/v1"}
        r = await c.post("/admin/upstreams", data={**form, "api_key": "k", "params": "{not json"})
        assert "e=rules_invalid" in r.headers["location"]
        assert len(store.list_pools()) == pools  # no pool left behind
        r = await c.post("/admin/upstreams", data={**form, "api_key": "k", "pool": "new", "max_concurrency": "1"})
        assert "m=created" in r.headers["location"], r.headers["location"]


async def test_wrong_passwords_lock_a_name_out_from_that_address_only(make_stack):
    """Someone else's wrong passwords for "owner" block that name from their address, not the owner's sign-in."""
    proxied = {**OWNER, "trusted_proxies": (ipaddress.ip_network("127.0.0.1/32"),)}
    stack = make_stack(bootstrap=proxied)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        attacker = {"X-Forwarded-For": "203.0.113.9"}
        for _ in range(6):
            r = await c.post("/admin/login", data={"username": "owner", "password": "guess"}, headers=attacker)
        assert "Too many attempts" in r.text
        owner = {"X-Forwarded-For": "198.51.100.7"}
        data = {"username": "owner", "password": OWNER["admin_password"], "next": "/admin"}
        r = await c.post("/admin/login", data=data, headers=owner)
        assert r.status_code == 303, r.text


async def test_showing_a_key_needs_the_content_permission(make_stack):
    """A client's key reads its job results and files: Show is for admins who may read logged content."""
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        owner = store.get_admin(username="owner")
        csrf = csrf_of((await c.get("/admin/clients")).text)
        await c.post("/admin/clients", data={"csrf": csrf, "name": "kept", "priority": "normal"})
        kept = next(x for x in store.list_clients() if x.name == "kept")
        store.update_admin(owner.id, "test", can_view_content=False)
        page = (await c.get("/admin/clients")).text
        assert "data-show-key" not in page and "Showing keys needs" in page
        r = await c.post(f"/admin/clients/{kept.id}/key", headers={"X-CSRF-Token": csrf})
        assert r.status_code == 403
        store.update_admin(owner.id, "test", can_view_content=True)
        r = await c.post(f"/admin/clients/{kept.id}/key", headers={"X-CSRF-Token": csrf})
        assert r.status_code == 200 and r.json()["key"]


def _encrypted_zip() -> bytes:
    """A backup zip whose entries say they're password-protected (Python can't write real ones)."""
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as zf:
        zf.writestr(backup.DB_NAME, b"x" * 100)
    raw = bytearray(data.getvalue())
    for signature, offset in ((b"PK\x03\x04", 6), (b"PK\x01\x02", 8)):
        at = raw.find(signature)
        raw[at + offset] |= 1  # general purpose flag: encrypted
    return bytes(raw)


def test_restore_of_an_unreadable_zip_and_an_interrupted_one(tmp_path):
    upload = tmp_path / "upload.zip"
    upload.write_bytes(_encrypted_zip())
    with pytest.raises(backup.RestoreError, match="password-protected"):
        backup.stage_restore(str(tmp_path), str(upload), Fernet(Fernet.generate_key()), False)

    # the database was swapped in, then the start stopped before the key: the next start finishes it
    data = tmp_path / "data"
    (data / backup.RESTORE_DIR).mkdir(parents=True)
    (data / backup.KEY_NAME).write_bytes(b"old-key")
    (data / backup.RESTORE_DIR / backup.KEY_NAME).write_bytes(b"new-key")
    assert backup.apply_staged_restore(str(data))
    assert (data / backup.KEY_NAME).read_bytes() == b"new-key"
    assert not os.path.exists(data / backup.RESTORE_DIR)
    saved = [n for n in os.listdir(data / "backups") if n.endswith(".key")]
    assert len(saved) == 1 and (data / "backups" / saved[0]).read_bytes() == b"old-key"
    assert not backup.apply_staged_restore(str(data))  # nothing staged any more


def test_retried_jobs_run_again_and_spare_capacity_jobs_wait(tmp_path):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    past = time.time() - 60
    store.create_jobs(
        [
            new_job("c", {"request": CHAT}) | {"id": "job_old", "deadline": past},
            new_job("c", {"request": CHAT}) | {"id": "job_idle", "idle_only": True},
            new_job("c", {"request": CHAT}) | {"id": "job_now"},
        ]
    )
    store.claim_job("job_old")
    store.finish_job("job_old", "expired")
    assert store.retry_job("job_old")
    row = store.get_job("job_old")
    assert row["status"] == "queued" and row["deadline"] is None and row["attempts"] == 0
    assert {r["id"] for r in store.due_jobs(time.time(), 10, idle_too=False)} == {"job_old", "job_now"}
    assert "job_idle" in {r["id"] for r in store.due_jobs(time.time(), 10)}
    store.close()


def test_settings_from_the_environment_are_checked():
    for name, value in (("MAX_BODY_MB", "nan"), ("MAX_CONCURRENCY", "0"), ("AGING_S", "-5"), ("MAX_RETRIES", "-2")):
        with pytest.raises(ValueError, match=name):
            Settings.from_env({name: value})
    assert Settings.from_env({"MAX_RETRIES": "-1"}).max_retries == -1
