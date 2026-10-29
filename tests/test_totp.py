"""Two-factor sign-in (TOTP) and recovery codes."""

import re
import time

import httpx

from app import totp
from tests.test_admin import OWNER, csrf_of, login


def test_rfc6238_vector():
    # RFC 6238 SHA-1 test secret "12345678901234567890", T = 59 s -> 94287082 (8 digits; we use the last 6)
    secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
    assert totp.code_at(secret, 59 // 30) == "287082"
    assert totp.verify(secret, "287 082", now=59) == 1
    assert totp.verify(secret, "287082", now=59, after=1) is None  # no replay
    assert totp.verify(secret, "000000", now=59) is None
    assert "otpauth://totp/QueueRouter%3Aowner" in totp.provisioning_uri(secret, "owner")
    assert totp.qr_svg("otpauth://totp/x?secret=AB").startswith("<svg")


async def test_setup_login_recovery_and_reset(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/account")).text)
        setup = await c.post("/admin/account/2fa/start", data={"csrf": csrf})
        assert "<svg" in setup.text
        secret = re.search(r'id="totp-secret">([A-Z2-7 ]+)<', setup.text).group(1).replace(" ", "")
        password = OWNER["admin_password"]
        bad = await c.post("/admin/account/2fa/enable", data={"csrf": csrf, "code": "000000", "current": password})
        assert "didn&#39;t match" in bad.text or "didn't match" in bad.text
        now = time.time()
        code = totp.code_at(secret, int(now // 30) - 1)  # previous step: the current one stays unused
        no_password = await c.post("/admin/account/2fa/enable", data={"csrf": csrf, "code": code})
        assert "Current password is wrong" in no_password.text and not store.get_admin(username="owner").has_2fa
        async with httpx.AsyncClient(base_url=stack.url, timeout=30) as other:  # another browser, signed in
            await login(other, stack.url)
            done = await c.post("/admin/account/2fa/enable", data={"csrf": csrf, "code": code, "current": password})
            assert (await other.get("/admin", follow_redirects=False)).status_code in (302, 303)  # signed out
        assert (await c.get("/admin")).status_code == 200  # this one goes on, with a new session
        codes = re.search(r'id="recovery-codes">([^<]+)<', done.text).group(1).split()
        assert len(codes) == 10
        assert store.get_admin(username="owner").has_2fa

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        step = await login(c, stack.url)
        assert step.status_code == 200 and "authenticator app" in step.text  # password alone isn't enough
        assert (await c.get("/admin", follow_redirects=False)).status_code in (302, 303)
        wrong = await c.post("/admin/login/2fa", data={"code": "123456"})
        assert "didn" in wrong.text
        ok = await c.post("/admin/login/2fa", data={"code": totp.code_at(secret, int(time.time() // 30))})
        assert ok.status_code == 303 and (await c.get("/admin")).status_code == 200

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:  # a recovery code works once
        await login(c, stack.url)
        assert (await c.post("/admin/login/2fa", data={"code": codes[0]})).status_code == 303
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        assert (await c.post("/admin/login/2fa", data={"code": codes[0]})).status_code == 200

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:  # no pending sign-in: refused
        assert "expired" in (await c.post("/admin/login/2fa", data={"code": codes[1]})).text

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:  # turning it off needs the password
        await login(c, stack.url)
        await c.post("/admin/login/2fa", data={"code": codes[1]})
        csrf = csrf_of((await c.get("/admin/account")).text)
        r = await c.post("/admin/account/2fa/disable", data={"csrf": csrf, "current": "wrong"})
        assert "wrong" in r.text and store.get_admin(username="owner").has_2fa
        r = await c.post("/admin/account/2fa/disable", data={"csrf": csrf, "current": OWNER["admin_password"]})
        assert "m=2fa_off" in r.headers["location"] and not store.get_admin(username="owner").has_2fa
