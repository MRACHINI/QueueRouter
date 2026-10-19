"""Alerts: rules, in-app notifications, webhooks."""

import asyncio
import hashlib
import hmac
import json
import time

import httpx

from app.alerts import Alerts, Note, webhook_request
from app.config import Bootstrap
from app.state import open_store
from app.store import Webhook
from tests.test_admin import OWNER, csrf_of, login


def hook(kind: str, secret: str | None = None) -> Webhook:
    return Webhook(1, "h", kind, "https://example.com/x", secret, "info", True)


def test_webhook_payloads():
    note = Note("error", "circuit", "Upstream arli is down", "Taken out of rotation: 3 failures → open")
    body, headers = webhook_request(hook("discord"), note)
    assert "Upstream arli is down" in json.loads(body)["content"]
    body, _ = webhook_request(hook("slack"), note)
    assert json.loads(body)["text"].startswith("🚨 *Upstream arli is down*")
    body, headers = webhook_request(hook("ntfy"), note)
    assert body.decode().startswith("Taken out") and headers["priority"] == "urgent"
    assert all(v.isascii() for v in headers.values())
    body, headers = webhook_request(hook("json", secret="s3cret"), note)
    assert json.loads(body)["kind"] == "circuit"
    expected = hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
    assert headers["x-queuerouter-signature"] == f"sha256={expected}"


async def test_rules_cooldowns_and_delivery(tmp_path):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(500 if "fail" in str(request.url) else 204)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    alerts = Alerts(store, lambda: http)
    store.create_webhook("ok", "json", "https://hooks.test/ok", None, "warning", "t")
    store.create_webhook("bad", "slack", "https://hooks.test/fail", None, "info", "t")

    alerts.circuit("arli", "open", "3 failures")
    alerts.circuit("arli", "open", "again")  # within the cooldown: nothing new
    alerts.circuit("arli", "closed", "probe ok")  # recovery always announced
    await alerts.drain()
    titles = [n["title"] for n in store.list_notifications()]
    assert titles == ["Upstream arli recovered", "Upstream arli is down"]
    # the json hook only gets warning+, the failing slack hook got both after retries
    assert [json.loads(r.content)["title"] for r in received if r.url.path == "/ok"] == ["Upstream arli is down"]
    bad = next(h for h in store.list_webhooks() if h.name == "bad")
    assert bad.last_ok is False and "HTTP 500" in bad.last_error
    assert store.unread_notifications() == 2

    # queue rule: must be sustained for the window
    now = time.time()
    alerts.check(queued=50, oldest_wait_s=0, now=now)
    alerts.check(queued=50, oldest_wait_s=0, now=now + 30)
    assert store.unread_notifications() == 2
    alerts.check(queued=50, oldest_wait_s=0, now=now + 61)
    assert store.list_notifications(1)[0]["title"] == "Queue backed up: 50 requests waiting"

    # error rate: needs enough requests
    for i in range(12):
        alerts.observe({"outcome": "error" if i % 2 else "done", "ts": now})
    alerts.check(queued=0, oldest_wait_s=0, now=now + 62)
    assert store.list_notifications(1)[0]["title"] == "50% of requests failed"

    # a disabled rule stays quiet
    store.save_alert_rule("oldest_wait", False, 300, None, 900, "t")
    alerts.reload()
    alerts.check(queued=0, oldest_wait_s=10_000, now=now + 63)
    assert "waited" not in store.list_notifications(1)[0]["title"]
    await alerts.drain()
    await http.aclose()
    store.close()


async def test_admin_alerts_page_and_webhook_test(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = await c.get("/admin/alerts")
        assert page.status_code == 200 and "QueueRouter" in page.text and "started" in page.text
        assert 'id="nav-alerts-count"' in page.text
        csrf = csrf_of(page.text)

        url = f"{stack.mock['url']}/hook"
        r = await c.post(
            "/admin/alerts/webhooks",
            data={"csrf": csrf, "name": "mine", "kind": "json", "url": url, "secret": "k", "min_level": "info"},
        )
        assert "m=created" in r.headers["location"]
        hooks = stack.app.state.store.list_webhooks()
        assert hooks[0].url == url  # stored encrypted, read back intact
        raw = stack.app.state.store.query("SELECT url FROM webhooks")[0]["url"]
        assert "hook" not in raw

        r = await c.post(f"/admin/alerts/webhooks/{hooks[0].id}/test", data={"csrf": csrf})
        assert "m=webhook_ok" in r.headers["location"]
        sent = stack.mock["hooks"][-1]
        assert json.loads(sent["body"])["title"] == "Test notification"
        assert sent["headers"]["x-queuerouter-signature"].startswith("sha256=")

        page = (await c.get("/admin/alerts")).text
        assert url not in page  # never shown in full

        # rules are editable; bad input is refused
        r = await c.post("/admin/alerts/rules/queue_length", data={"csrf": csrf, "threshold": "3", "window_s": "5"})
        assert "m=saved" in r.headers["location"]
        assert stack.app.state.alerts.rules["queue_length"].threshold == 3
        assert stack.app.state.alerts.rules["queue_length"].enabled is False  # checkbox unticked
        r = await c.post("/admin/alerts/rules/queue_length", data={"csrf": csrf, "threshold": "x", "window_s": "5"})
        assert "e=invalid" in r.headers["location"]

        r = await c.post("/admin/alerts/read", data={"csrf": csrf})
        assert stack.app.state.store.unread_notifications() == 0
        await asyncio.sleep(0)
