"""opencode's own session headers: grouping and tagging without the plugin."""

import asyncio

import httpx

from app.sessions import session_key

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


def test_subagents_join_their_parent_session():
    parent = session_key({"x-session-id": "ses_parent"}, "c", CHAT)
    child = session_key(
        {"x-session-id": "ses_child", "x-opencode-session-id": "ses_child", "x-parent-session-id": "ses_parent"},
        "c",
        CHAT,
    )
    assert child == parent
    assert session_key({"x-session-id": "ses_other"}, "c", CHAT) != parent


async def test_opencode_requests_are_tagged(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        headers = {**stack.headers, "x-opencode-session-id": "ses_1", "X-Session-Id": "ses_1"}
        await c.post("/v1/chat/completions", headers=headers, json=CHAT)
    for _ in range(50):
        rows = await asyncio.to_thread(stack.app.state.store.list_requests, 5)
        if rows:
            break
        await asyncio.sleep(0.1)
    assert "opencode" in (rows[0]["tags"] or "")
    assert "x-opencode-session-id" in stack.mock["last_headers"]  # still passed on to the provider
