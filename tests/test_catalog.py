import time

from app.catalog import FAILED_RETRY_S, Catalogs, _Entry


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
