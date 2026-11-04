import asyncio
import json
import logging

import httpx

from app.auth import MAX_APPS, AuthGuard
from tests.test_admin import OWNER, csrf_of, key_of, login


def test_a_block_keeps_what_it_saw():
    guard = AuthGuard(3, 300)
    assert not guard.fail("1.2.3.4", "old-app/1") and not guard.fail("1.2.3.4", "old-app/1")
    assert guard.fail("1.2.3.4", "other/2")  # the third in a minute starts it
    assert guard.is_blocked("1.2.3.4") > 299 and not guard.is_blocked("5.6.7.8")
    for n in range(MAX_APPS + 2):
        guard.refused("1.2.3.4", f"app-{n}")
    [(ip, left, block)] = guard.blocks()
    assert ip == "1.2.3.4" and 299 < left <= 300 and block.refused == MAX_APPS + 2
    assert block.apps == ["old-app/1", "other/2", "app-0", "app-1", "app-2"]  # a few at most
    assert guard.unblock("1.2.3.4") and not guard.is_blocked("1.2.3.4") and not guard.blocks()
    assert not guard.unblock("1.2.3.4")


async def test_failed_keys_and_blocks_show_and_can_be_lifted(make_stack, caplog):
    caplog.set_level(logging.WARNING, logger="queuerouter")
    stack = make_stack(bootstrap=OWNER, auth_fail_limit=3)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)
        key = key_of((await c.post("/admin/clients", data={"csrf": csrf, "name": "laptop"})).text)
        laptop = next(x for x in stack.app.state.store.list_clients() if x.name == "laptop")
        await c.post(f"/admin/clients/{laptop.id}", data={"csrf": csrf, "priority": "normal", "notes": ""})  # off
        old = {"User-Agent": "old-app/1.0"}
        for headers in (old, {**old, "Authorization": "Bearer qr-wrong"}, {**old, "x-api-key": key}):
            assert (await c.get("/v1/models", headers=headers)).status_code == 401
        claude = {**stack.headers, "User-Agent": "Claude/1.0"}
        blocked = await c.get("/v1/models", headers=claude)  # a good key, from the blocked address
        assert blocked.status_code == 429 and "too many failed" in blocked.text

        page = (await c.get("/admin/clients")).text
        assert "Blocked for failed keys" in page and "<code>127.0.0.1</code>" in page
        assert "old-app/1.0" in page and "Claude/1.0" in page and '<td data-label="Turned away">1</td>' in page
        for _ in range(50):
            rows = [r for r in (await c.get("/admin/api/requests")).json()["rows"] if r["status"] == 401]
            if len(rows) == 3:
                break
            await asyncio.sleep(0.1)
        details = [(await c.get(f"/admin/api/requests/{r['id']}")).json() for r in reversed(rows)]  # oldest first
        assert [d["error"].split(";")[0] for d in details] == [
            "no key (Authorization: Bearer, or x-api-key)",
            "unknown key: mistyped, or replaced by New key",
            "the key of laptop, which is disabled",
        ]
        assert "blocked for 5 min" in details[2]["error"] and "blocked" not in details[1]["error"]
        for d in details:
            assert d["ip"] == "127.0.0.1" and d["path"] == "/v1/models" and d["user_agent"] == "old-app/1.0"
        assert key not in json.dumps(details)  # the key is never recorded

        lifted = await c.post("/admin/unblock", data={"csrf": csrf, "ip": "127.0.0.1"})
        assert "m=unblocked" in lifted.headers["location"]
        assert (await c.get("/v1/models", headers=claude)).status_code == 200
        assert "Blocked for failed keys" not in (await c.get("/admin/clients")).text
        again = await c.post("/admin/unblock", data={"csrf": csrf, "ip": "127.0.0.1"})
        assert "m=not_blocked" in again.headers["location"]
    lines = [json.loads(r.getMessage()) for r in caplog.records if '"event": "auth_blocked"' in r.getMessage()]
    assert len(lines) == 1 and lines[0]["ip"] == "127.0.0.1" and lines[0]["apps"] == ["old-app/1.0"]
