"""Monthly quotas per client."""

import calendar
import dataclasses

import httpx

from app.config import ClientPolicy
from app.quotas import MonthlyUsage, Used, over_quota, quota_share, seconds_to_next_month

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


def test_month_math():
    jan31 = calendar.timegm((2026, 1, 31, 23, 59, 0))
    assert seconds_to_next_month(jan31) == 60
    assert seconds_to_next_month(calendar.timegm((2028, 2, 28, 0, 0, 0))) == 2 * 86_400  # leap year


def test_monthly_usage_and_limits():
    usage = MonthlyUsage(lambda model, p, c, cached: (p + c) / 1000)
    now = calendar.timegm((2026, 5, 10, 0, 0, 0))
    rows = [
        {"client": "a", "model": "m", "requests": 3, "prompt_tokens": 100, "completion_tokens": 50, "cached_tokens": 0}
    ]
    usage.refresh(rows, now)
    usage.observe({"client": "a", "ts": now, "prompt_tokens": 10, "completion_tokens": 5, "model": "m"})
    usage.observe({"client": "a", "ts": now - 40 * 86_400, "prompt_tokens": 999})  # last month: ignored
    used = usage.used("a")
    assert (used.requests, used.tokens) == (4, 165) and abs(used.cost - 0.165) < 1e-9
    assert usage.used("b") == Used()

    policy = ClientPolicy(month_tokens=200, month_requests=10)
    assert over_quota(policy, used) is None and round(quota_share(policy, used), 3) == 0.825
    assert "token quota" in over_quota(dataclasses.replace(policy, month_tokens=165), used)
    assert "cost quota" in over_quota(ClientPolicy(month_cost_usd=0.1), used)
    assert quota_share(ClientPolicy(), used) is None


async def test_quota_enforced(make_stack):
    stack = make_stack()
    store = stack.app.state.store
    client = store.list_clients()[0]
    store.update_client(client.id, "test", policy=dataclasses.replace(client.policy, month_requests=2))
    stack.app.state.reload()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        for _ in range(2):
            assert (await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)).status_code == 200
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=CHAT)
        assert r.status_code == 429 and r.json()["error"]["type"] == "monthly_quota_exceeded"
        assert int(r.headers["retry-after"]) > 0
        month = (await c.get("/v1/queue", headers=stack.headers)).json()["month"]
        assert month["requests"] == 2 and month["quota"]["requests"] == 2


def test_quota_alerts(tmp_path):
    from app.alerts import Alerts
    from app.config import Bootstrap
    from app.state import open_store

    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    alerts = Alerts(store, lambda: None)
    alerts.quota("a", 0.5, "2026-05")
    alerts.quota("a", 0.85, "2026-05")
    alerts.quota("a", 0.9, "2026-05")  # once per month
    alerts.quota("a", 1.0, "2026-05")
    titles = [n["title"] for n in store.list_notifications()]
    assert titles == ["Client a used up a monthly quota", "Client a used 85% of a monthly quota"]
    store.close()
