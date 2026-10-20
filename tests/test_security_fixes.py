"""Fixes from the v0.6-v0.14 security review."""

import io
import time
import zipfile

import httpx
import pytest

from app import backup, jobs
from app.config import Bootstrap
from app.state import open_store
from tests.test_admin import OWNER, csrf_of, key_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


async def test_deleted_client_data_is_not_inherited(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)
        bob = await new_client(c, csrf, "bob")
        job = (await c.post("/v1/jobs", headers=bob, json={"request": CHAT, "run_at": time.time() + 3600})).json()
        await c.post("/v1/schedules", headers=bob, json={"name": "n", "cron": "@daily", "request": CHAT})
        file = (
            await c.post("/v1/files", headers=bob, files={"file": ("a.jsonl", b"{}\n")}, data={"purpose": "batch"})
        ).json()
        store.save_response("resp_old", "bob", [{"role": "user", "content": "secret"}])

        bob_id = next(cl.id for cl in store.list_clients() if cl.name == "bob")
        await c.post(f"/admin/clients/{bob_id}/delete", data={"csrf": csrf})
        new_bob = await new_client(c, csrf, "bob")
        assert (await c.get(f"/v1/jobs/{job['id']}", headers=new_bob)).status_code == 404
        assert (await c.get("/v1/schedules", headers=new_bob)).json()["data"] == []
        assert (await c.get(f"/v1/files/{file['id']}", headers=new_bob)).status_code == 404
        assert store.load_response("resp_old", "bob") is None


def test_rename_moves_jobs(tmp_path):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    store.create_jobs([jobs.new_job("alice", {"request": CHAT}) | {"id": "job_x"}])
    with store.tx() as db:
        db.execute("INSERT INTO clients (name, key_hash, key_prefix, created_at) VALUES ('alice', 'h', 'qr-', 0)")
    client_id = store.query("SELECT id FROM clients WHERE name = 'alice'")[0]["id"]
    store.update_client(client_id, "t", name="alicia")
    assert store.get_job("job_x")["client"] == "alicia"
    store.close()


async def test_body_limits(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        big = b"username=x&password=" + b"a" * (5 * 1024 * 1024)
        form = {"content-type": "application/x-www-form-urlencoded"}
        assert await refused(lambda: c.post("/admin/login", content=big, headers=form))  # by Content-Length

        async def chunks():  # no Content-Length: counted while reading
            for _ in range(10):
                yield b"a" * (1024 * 1024)

        assert await refused(lambda: c.post("/admin/login/2fa", content=chunks(), headers=form))

        stack.app.state.store.set_tunables({"max_body_mb": 0.01}, "t")
        stack.app.state.reload()
        upload = {"file": ("a.jsonl", b"x" * 50_000)}
        assert await refused(
            lambda: c.post("/v1/files", headers=stack.headers, files=upload, data={"purpose": "batch"})
        )

        r = await c.post(
            "/admin/login", data={"username": "owner", "password": OWNER["admin_password"], "next": "/admin"}
        )
        assert r.status_code == 303  # normal forms still work


def test_callback_checks():
    for bad in ("https://hooks.example.com:8443/x", "https://hooks.example.com:22/x", "http://hooks.example.com/x"):
        with pytest.raises(jobs.JobError):
            jobs.check_callback(bad)
    assert jobs.check_callback("https://hooks.example.com:443/x")
    assert jobs.pinned_url("https://hooks.example.com/a?b=1", "93.184.216.34") == "https://93.184.216.34/a?b=1"
    assert jobs.pinned_url("https://hooks.example.com/a", "2606:2800::1") == "https://[2606:2800::1]/a"
    with pytest.raises(jobs.JobError):
        jobs.new_job("c", {"request": CHAT, "callback_secret": {"not": "a string"}})


async def test_callback_is_pinned_and_quiet(tmp_path, monkeypatch):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    store.create_jobs(
        [jobs.new_job("c", {"request": CHAT, "callback_url": "https://hooks.example.com/done"}) | {"id": "job_cb"}]
    )
    assert "hooks.example.com" not in store.query("SELECT callback_url FROM jobs")[0]["callback_url"]  # encrypted
    store.finish_job("job_cb", "succeeded", result={"ok": True})
    seen = []

    async def fake_resolve(host):
        return "93.184.216.34"

    def handler(request):
        seen.append(request)
        if len(seen) > 1:
            raise httpx.ConnectError("refused")
        return httpx.Response(204)

    monkeypatch.setattr(jobs, "public_address", fake_resolve)
    monkeypatch.setattr(jobs, "CALLBACK_RETRY_S", ())
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    runner = jobs.JobRunner(store, None, lambda: http, lambda body: (None, None, None))
    await runner._callback("job_cb")
    request = seen[0]
    assert request.url.host == "93.184.216.34" and request.headers["host"] == "hooks.example.com"
    assert request.extensions["sni_hostname"] == "hooks.example.com"
    assert store.get_job("job_cb")["callback_status"] == "delivered"
    await runner._callback("job_cb")
    assert store.get_job("job_cb")["callback_status"] == "failed"  # not "ConnectError"
    await http.aclose()
    store.close()


async def test_otel_headers_cleared_when_endpoint_changes(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    store.set_tunables(
        {"otel_endpoint": "https://collector.example.com", "otel_headers": "authorization=Bearer k"}, "t"
    )
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/settings")).text)
        await c.post("/admin/settings", data={"csrf": csrf, "otel_endpoint": "https://collector.example.com"})
        assert store.get_tunables()["otel_headers"] == "authorization=Bearer k"  # unchanged URL: kept
        await c.post("/admin/settings", data={"csrf": csrf, "otel_endpoint": "https://attacker.example.net"})
        assert store.get_tunables()["otel_headers"] == ""


def test_restore_zip_size_limit(tmp_path, monkeypatch):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("queuerouter.db", b"\0" * 200_000)  # compresses to almost nothing
    upload = tmp_path / "bomb.zip"
    upload.write_bytes(buffer.getvalue())
    monkeypatch.setattr(backup, "MAX_RESTORE_DB_BYTES", 100_000)
    with pytest.raises(backup.RestoreError, match="too large"):
        backup.stage_restore(str(tmp_path), str(upload), store.fernet, False)
    store.close()


async def new_client(c: httpx.AsyncClient, csrf: str, name: str) -> dict:
    page = await c.post("/admin/clients", data={"csrf": csrf, "name": name, "priority": "normal"})
    return {"Authorization": f"Bearer {key_of(page.text)}"}


async def refused(send) -> bool:
    """Refused without being read: 413 (our limit), 400 (the form parser's own 1 MB field limit),
    or the connection closed while we were still sending."""
    try:
        return (await send()).status_code in (400, 413)
    except (httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError):
        return True
