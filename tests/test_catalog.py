import logging
import time

import httpx

from app.catalog import FAILED_RETRY_S, Catalogs, _Entry
from tests.test_admin import OWNER, login
from tests.test_upstreams import add_upstream


def test_failed_lists_are_retried_soon(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    c = Catalogs()
    assert c.stale(1, 300)  # never fetched
    c._entries[1] = _Entry(fetched_at=now[0], ok=False, error="ConnectError")
    assert not c.stale(1, 300)
    now[0] += FAILED_RETRY_S + 1
    assert c.stale(1, 300)  # failed: retried after seconds, not after the cache age
    c._entries[1] = _Entry(fetched_at=now[0], ok=True, models=[{"id": "m"}])
    now[0] += 60
    assert not c.stale(1, 300) and c.ids(1) == {"m"}
    now[0] += 300
    assert c.stale(1, 300)


async def test_a_failing_list_is_logged_once_and_its_return_too(make_stack, make_mock, monkeypatch, caplog):
    monkeypatch.setattr("app.catalog.FAILED_RETRY_S", 0)  # asked again at once
    caplog.set_level(logging.INFO, logger="queuerouter")
    stack = make_stack(bootstrap=OWNER)
    second_url, second = make_mock()
    second["models_status"] = 503
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "second", second_url)
        for _ in range(2):
            await c.get("/v1/models", headers=stack.headers)
        assert second["models_asked"] >= 2  # failing all along: said once
        second["models_status"] = 0
        listed = (await c.get("/v1/models", headers=stack.headers)).json()["data"]
        assert "mock-model@second" in [m["id"] for m in listed]
    lines = [r.getMessage() for r in caplog.records if "model_list_" in r.getMessage()]
    assert len(lines) == 2 and '"upstream": "second"' in lines[0] and "HTTP 503" in lines[0]
    assert '"event": "model_list_back"' in lines[1]
