import ipaddress
import re

import httpx
import pytest

OWNER = {"admin_username": "owner", "admin_password": "correct horse battery"}


def csrf_of(html: str) -> str:
    match = re.search(r'name="csrf" value="([^"]+)"', html)
    assert match, "no csrf token on page"
    return match.group(1)


async def login(c: httpx.AsyncClient, url: str, username="owner", password="correct horse battery"):
    return await c.post(f"{url}/admin/login", data={"username": username, "password": password, "next": "/admin"})


@pytest.fixture
def admin_stack(make_stack):
    return make_stack(bootstrap=OWNER)


async def test_requires_login_and_sets_security_headers(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        r = await c.get("/admin")
        assert r.status_code == 303 and r.headers["location"].startswith("/admin/login")
        r = await c.get("/admin/login")
        assert r.status_code == 200
        assert "default-src 'self'" in r.headers["content-security-policy"]
        assert r.headers["x-frame-options"] == "DENY" and r.headers["cache-control"] == "no-store"
        assert (await c.get("/admin/api/stats")).status_code == 303


async def test_login_dashboard_and_logout(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        bad = await login(c, admin_stack.url, password="wrong password!!")
        assert "Wrong username or password" in bad.text
        r = await login(c, admin_stack.url)
        assert r.status_code == 303 and r.headers["location"] == "/admin"
        cookie = r.headers["set-cookie"].lower()
        assert "httponly" in cookie and "samesite=strict" in cookie and "path=/admin" in cookie
        page = await c.get("/admin")
        assert page.status_code == 200 and "Dashboard" in page.text
        stats = (await c.get("/admin/api/stats")).json()
        assert stats["max_concurrency"] == 2 and stats["configured"] is True
        assert (await c.get("/admin/static/admin.js")).status_code == 200
        await c.post("/admin/logout", data={"csrf": csrf_of(page.text)})
        assert (await c.get("/admin")).status_code == 303


async def test_csrf_required(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        await login(c, admin_stack.url)
        r = await c.post("/admin/settings", data={"aging_s": "5"})
        assert r.status_code == 403
        r = await c.post("/admin/settings", data={"aging_s": "5", "csrf": "forged"})
        assert r.status_code == 403


async def test_settings_apply_live(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        await login(c, admin_stack.url)
        page = await c.get("/admin/settings")
        form = {"csrf": csrf_of(page.text), "aging_s": "7", "max_retries": "3", "session_aware": "on"}
        r = await c.post("/admin/settings", data=form)
        assert r.status_code == 303 and "m=saved" in r.headers["location"]
        values = admin_stack.app.state.store.get_tunables()
        assert values["aging_s"] == 7 and values["max_retries"] == 3
        assert values["session_aware"] is True and values["fair_share"] is False  # unchecked box = off
        bad = await c.post("/admin/settings", data={"csrf": form["csrf"], "aging_s": "-5"})
        assert "e=invalid" in bad.headers["location"]
        audit = await c.get("/admin/audit")
        assert "settings.update" in audit.text


async def test_roles_and_last_owner_protection(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as owner:
        await login(owner, admin_stack.url)
        page = await owner.get("/admin/users")
        csrf = csrf_of(page.text)
        r = await owner.post(
            "/admin/users",
            data={"csrf": csrf, "username": "watcher", "role": "viewer", "password": "viewer password 1"},
        )
        assert "m=created" in r.headers["location"]
        dup = await owner.post(
            "/admin/users",
            data={"csrf": csrf, "username": "watcher", "role": "viewer", "password": "viewer password 1"},
        )
        assert "e=exists" in dup.headers["location"]

        me = admin_stack.app.state.store.get_admin(username="owner")
        r = await owner.post(f"/admin/users/{me.id}", data={"csrf": csrf, "role": "admin", "enabled": "on"})
        assert "e=last_owner" in r.headers["location"]
        r = await owner.post(f"/admin/users/{me.id}/delete", data={"csrf": csrf})
        assert "e=self" in r.headers["location"]

    async with httpx.AsyncClient(base_url=admin_stack.url) as viewer:
        await login(viewer, admin_stack.url, "watcher", "viewer password 1")
        assert (await viewer.get("/admin")).status_code == 200
        assert (await viewer.get("/admin/settings")).status_code == 403
        assert (await viewer.get("/admin/users")).status_code == 403


async def test_login_throttling(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        for _ in range(5):
            await login(c, admin_stack.url, password="nope nope nope")
        r = await login(c, admin_stack.url)  # right password, but blocked now
        assert "Too many attempts" in r.text


async def test_open_redirect_blocked(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        r = await c.post(
            "/admin/login",
            data={"username": "owner", "password": "correct horse battery", "next": "//evil.example/admin"},
        )
        assert r.headers["location"] == "/admin"


async def test_change_own_password(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        await login(c, admin_stack.url)
        page = await c.get("/admin/account")
        form = {"csrf": csrf_of(page.text), "current": "wrong", "password": "a new long password", "confirm": "x"}
        assert "Current password is wrong" in (await c.post("/admin/account", data=form)).text
        form["current"] = "correct horse battery"
        assert "match" in (await c.post("/admin/account", data=form)).text
        form["confirm"] = "a new long password"
        r = await c.post("/admin/account", data=form)
        assert "m=password" in r.headers["location"]
        assert (await c.get("/admin")).status_code == 200  # fresh session cookie issued
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        assert (await login(c, admin_stack.url, password="a new long password")).status_code == 303


async def test_first_run_setup_token(make_stack):
    stack = make_stack()
    token = stack.app.state.admin_setup["token"]
    assert token
    async with httpx.AsyncClient(base_url=stack.url) as c:
        assert (await c.get("/admin/login")).headers["location"] == "/admin/setup"
        r = await c.post(
            "/admin/setup", data={"token": "nope", "username": "me", "password": "a long password!", "confirm": "x"}
        )
        assert "Wrong setup token" in r.text
        r = await c.post(
            "/admin/setup",
            data={"token": token, "username": "me", "password": "a long password!", "confirm": "a long password!"},
        )
        assert r.status_code == 303
        assert (await c.get("/admin")).status_code == 200
        assert (await c.get("/admin/setup")).headers["location"] == "/admin/login"
    assert stack.app.state.store.get_admin(username="me").role == "owner"


async def test_admin_ip_allow_list(make_stack):
    stack = make_stack(bootstrap={**OWNER, "admin_allowed_ips": (ipaddress.ip_network("10.99.0.0/16"),)})
    async with httpx.AsyncClient(base_url=stack.url) as c:
        assert (await c.get("/admin/login")).status_code == 404
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "m", "messages": []})
        assert r.status_code == 200  # the API itself is unaffected


def key_of(html: str) -> str:
    match = re.search(r'<code id="new-key">([^<]+)</code>', html)
    assert match, "no key on page"
    return match.group(1)


async def test_client_lifecycle_applies_live(admin_stack):
    """Keys created, changed and revoked in the UI work on the API immediately."""
    api = f"{admin_stack.url}/v1/chat/completions"
    body = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    async with httpx.AsyncClient(base_url=admin_stack.url) as c:
        await login(c, admin_stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)

        bad = await c.post("/admin/clients", data={"csrf": csrf, "name": "no spaces!", "priority": "high"})
        assert "e=invalid" in bad.headers["location"]

        created = await c.post(
            "/admin/clients",
            data={"csrf": csrf, "name": "n8n", "priority": "low", "notes": "workflows", "expires": ""},
        )
        key = key_of(created.text)
        assert "opencode.json" in created.text and key.startswith("qr-")
        r = await c.post(api, json=body, headers={"Authorization": f"Bearer {key}"})
        assert r.status_code == 200

        dup = await c.post("/admin/clients", data={"csrf": csrf, "name": "n8n", "priority": "low"})
        assert "e=exists" in dup.headers["location"]

        listing = await c.get("/admin/clients")
        assert "<strong>n8n</strong>" in listing.text and key not in listing.text and key[:6] in listing.text
        client = next(x for x in admin_stack.app.state.store.list_clients() if x.name == "n8n")

        await c.post(f"/admin/clients/{client.id}", data={"csrf": csrf, "priority": "high", "notes": ""})
        r = await c.post(api, json=body, headers={"Authorization": f"Bearer {key}"})
        assert r.status_code == 401  # "enabled" unchecked -> disabled

        await c.post(f"/admin/clients/{client.id}", data={"csrf": csrf, "priority": "high", "enabled": "on"})
        regenerated = await c.post(f"/admin/clients/{client.id}/regenerate", data={"csrf": csrf})
        new_key = key_of(regenerated.text)
        assert (await c.post(api, json=body, headers={"Authorization": f"Bearer {key}"})).status_code == 401
        assert (await c.post(api, json=body, headers={"Authorization": f"Bearer {new_key}"})).status_code == 200

        expired = await c.post(
            f"/admin/clients/{client.id}",
            data={"csrf": csrf, "priority": "high", "enabled": "on", "expires": "2020-01-01"},
        )
        assert "m=saved" in expired.headers["location"]
        assert (await c.post(api, json=body, headers={"Authorization": f"Bearer {new_key}"})).status_code == 401
        assert "2020-01-01" in (await c.get("/admin/clients")).text

        await c.post(f"/admin/clients/{client.id}/delete", data={"csrf": csrf})
        assert "<strong>n8n</strong>" not in (await c.get("/admin/clients")).text


async def test_clients_page_needs_admin_role(admin_stack):
    async with httpx.AsyncClient(base_url=admin_stack.url) as owner:
        await login(owner, admin_stack.url)
        csrf = csrf_of((await owner.get("/admin/users")).text)
        await owner.post(
            "/admin/users", data={"csrf": csrf, "username": "op", "role": "operator", "password": "operator password"}
        )
    async with httpx.AsyncClient(base_url=admin_stack.url) as op:
        await login(op, admin_stack.url, "op", "operator password")
        assert (await op.get("/admin/clients")).status_code == 403
