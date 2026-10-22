"""Opt-in content logging and structured-output repair."""

import asyncio
import json
import time

import httpx
import pytest

from app.structured import repair, repair_reply
from tests.test_admin import OWNER, csrf_of, login


@pytest.mark.parametrize(
    ("broken", "fixed"),
    [
        ('{"a": 1}', '{"a": 1}'),
        ('```json\n{"a": 1}\n```', '{"a": 1}'),
        ('Sure! Here it is: {"a": [1, 2]} Hope that helps.', '{"a": [1, 2]}'),
        ('{"a": 1, "b": [1, 2,],}', '{"a": 1, "b": [1, 2]}'),
        ('{"a": {"b": "cut off', '{"a": {"b": "cut off"}}'),
        ('{"a": 1, "b":', '{"a": 1, "b": null}'),
    ],
)
def test_repair(broken, fixed):
    assert json.loads(repair(broken)) == json.loads(fixed)


def test_repair_reply():
    request = {"response_format": {"type": "json_object"}}
    reply = {"choices": [{"message": {"content": '```json\n{"x": 1,}\n```'}}]}
    assert repair_reply(request, reply) == "repaired" and reply["choices"][0]["message"]["content"] == '{"x": 1}'
    assert repair_reply(request, {"choices": [{"message": {"content": "no json at all"}}]}) == "invalid"
    assert repair_reply({}, {"choices": [{"message": {"content": "plain text"}}]}) is None
    calls = {"choices": [{"message": {"tool_calls": [{"function": {"name": "f", "arguments": '{"q": "x",}'}}]}}]}
    assert repair_reply({}, calls) == "repaired"
    assert calls["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] == '{"q": "x"}'


async def test_structured_repair_in_the_pipeline(make_stack):
    stack = make_stack()
    body = {
        "model": "mock-model",
        "messages": [{"role": "user", "content": "json please"}],
        "response_format": {"type": "json_object"},
    }
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json={**body, "mock_content": '```json\n{"ok": true,}\n```'}
        )
        assert r.headers["x-queuerouter-structured"] == "repaired"
        assert json.loads(r.json()["choices"][0]["message"]["content"]) == {"ok": True}
        r = await c.post(
            "/v1/chat/completions", headers=stack.headers, json={**body, "mock_content": "I can't do JSON"}
        )
        assert r.headers["x-queuerouter-structured"] == "invalid"
        assert r.json()["choices"][0]["message"]["content"] == "I can't do JSON"
        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**body, "mock_content": '{"fine": 1}'})
        assert "x-queuerouter-structured" not in r.headers


async def test_content_logging(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    chat = {"model": "mock-model", "messages": [{"role": "user", "content": "secret plan"}]}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        # off by default: nothing kept, no header
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat)
        assert "x-queuerouter-logging" not in r.headers

        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/clients")).text)
        client_id = store.list_clients()[0].id
        await c.post(f"/admin/clients/{client_id}/logging", data={"csrf": csrf, "enabled": "on", "hours": "2"})
        policy = store.list_clients()[0].policy
        assert policy.logs_content(time.time()) and 7100 < policy.log_content_until - time.time() <= 7200

        # saving the other limits keeps logging on
        await c.post(f"/admin/clients/{client_id}/policy", data={"csrf": csrf, "on_limit": "delay"})
        assert store.list_clients()[0].policy.log_content

        r = await c.post("/v1/chat/completions", headers=stack.headers, json={**chat, "mock_tag": "A"})
        assert r.headers["x-queuerouter-logging"] == "on"
        plain_id = r.headers["x-request-id"]
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json={**chat, "stream": True}) as s:
            stream_id = s.headers["x-request-id"]
            _ = [chunk async for chunk in s.aiter_bytes()]
        for _ in range(50):
            if store.get_content(stream_id):
                break
            await asyncio.sleep(0.1)
        logged = store.get_content(plain_id)
        assert "secret plan" in logged["request"] and "hi A" in logged["response"]
        reassembled = json.loads(store.get_content(stream_id)["response"])
        assert reassembled["content"] == "t0 t1 t2 t3 t4 " and reassembled["reassembled_from_stream"]
        assert "secret plan" not in store.query("SELECT request FROM request_content LIMIT 1")[0]["request"]

        # reading needs the content permission, and is audited
        assert (await c.get(f"/admin/requests/{plain_id}/content")).status_code == 403
        assert "/content" not in (await c.get("/admin/requests")).text.split('id="finished-requests"')[1]
        owner = store.get_admin(username="owner")
        store.update_admin(owner.id, "test", can_view_content=True)
        page = await c.get(f"/admin/requests/{plain_id}/content")
        assert page.status_code == 200 and "secret plan" in page.text
        assert any(r["action"] == "content.view" for r in store.list_audit(10))
        assert f"/admin/requests/{plain_id}/content" in (await c.get("/admin/requests")).text

        await c.post(f"/admin/clients/{client_id}/purge-content", data={"csrf": csrf})
        assert store.get_content(plain_id) is None

        await c.post(f"/admin/clients/{client_id}/logging", data={"csrf": csrf})
        assert not store.list_clients()[0].policy.logs_content(time.time())
