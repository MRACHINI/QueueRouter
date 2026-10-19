"""Prometheus /metrics and secret settings."""

import httpx

from tests.test_admin import OWNER, csrf_of, login


async def test_metrics(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        assert (await c.get("/metrics")).status_code == 404  # off until a token is set

        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/settings")).text)
        r = await c.post("/admin/settings", data={"csrf": csrf, "metrics_token": "prom-secret-1"})
        assert "m=saved" in r.headers["location"]
        raw = store.query("SELECT value FROM settings WHERE key = 'metrics_token'")[0]["value"]
        assert "prom-secret-1" not in raw  # encrypted at rest
        page = (await c.get("/admin/settings")).text
        assert "prom-secret-1" not in page and "leave blank to keep" in page
        await c.post("/admin/settings", data={"csrf": csrf, "metrics_token": ""})  # blank keeps it
        assert store.get_tunables()["metrics_token"] == "prom-secret-1"
        assert "prom-secret-1" not in str([dict(r) for r in store.list_audit(20)])

        await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "mock-model", "messages": []})
        assert (await c.get("/metrics", headers={"Authorization": "Bearer nope"})).status_code == 401
        r = await c.get("/metrics", headers={"Authorization": "Bearer prom-secret-1"})
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
        lines = r.text.splitlines()
        assert any(line.startswith('queuerouter_requests_total{client="') and line.endswith(" 1") for line in lines)
        assert 'queuerouter_queue_wait_seconds_bucket{le="+Inf"} 1' in lines
        assert any(line.startswith("queuerouter_pool_capacity{") for line in lines)
        assert any(line.startswith('queuerouter_upstream_circuit{upstream="') for line in lines)
        assert "# TYPE queuerouter_tokens_total counter" in lines

        await c.post("/admin/settings", data={"csrf": csrf, "metrics_token": "", "clear_metrics_token": "on"})
        assert (await c.get("/metrics", headers={"Authorization": "Bearer prom-secret-1"})).status_code == 404
