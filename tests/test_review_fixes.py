"""Fixes from the Fable code review: council and compaction robustness, web fetch addresses, configuration
and provider details."""

import json

import httpx

from app import compaction, council, tools, websearch
from app.catalog import fetch_models
from app.config import Settings
from app.limits import overflow_of
from app.upstream import upstream_headers


def test_calculator_and_web_addresses():
    huge = " * ".join(["10**1000"] * 5)
    assert "more than 4,300 digits" in tools.calculate(huge)
    assert tools.calculate("2**10") == "2**10 = 1024"
    # commit hashes and long doc paths are ordinary in addresses; keys and secret parameters are not
    for url in (
        "https://github.com/MRACHINI/QueueRouter/commit/" + "a" * 40,
        "https://docs.example.com/" + "Ab9" * 20,
        "https://example.com/task-runner-something-long-here",
    ):
        assert not websearch._URL_SECRET.search(url), url
    for url in ("https://example.com/?api_key=abc", "https://x.io/?token=1", "https://x.io/ghp_" + "a" * 30):
        assert websearch._URL_SECRET.search(url), url


async def test_fetch_connects_to_the_checked_address(monkeypatch):
    async def resolved(host):
        return "93.184.216.34"

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(url=str(request.url), host=request.headers["host"], sni=request.extensions.get("sni_hostname"))
        return httpx.Response(200, text="hello", headers={"content-type": "text/plain"})

    monkeypatch.setattr(websearch, "public_host", resolved)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        final, text = await websearch.fetch(client, "https://example.com/page?q=1")
    assert final == "https://example.com/page?q=1" and text == "hello"
    assert seen == {"url": "https://93.184.216.34/page?q=1", "host": "example.com", "sni": "example.com"}


def test_compaction_and_council_tolerate_odd_messages():
    odd = {"model": "m", "messages": ["text", {"role": "user", "content": "hi"}]}
    assert not compaction.needed(odd, 10, 5)
    assert compaction.conversation_key("c", "s", odd["messages"]) is None
    bad_calls = {"model": "m", "messages": [{"role": "assistant", "content": "x", "tool_calls": ["oops"]}]}
    assert not compaction.needed(bad_calls, 10, 5)

    body = json.dumps(
        {
            "choices": [
                {"message": {"content": "ok", "tool_calls": ["junk", {"function": "x"}, {"function": {"name": "f"}}]}}
            ]
        }
    ).encode()
    message = council.message_of(body)
    assert message["tool_calls"] == [{"function": {"name": "f"}}]
    assert council.message_of(json.dumps({"choices": [{"message": {"tool_calls": "x"}}]}).encode()) is None
    assert json.loads(council.without_calls(body, "f"))["choices"][0]["message"].get("tool_calls") is None


def test_summary_note_reaches_an_empty_first_message():
    messages = [{"role": "system", "content": "s"}, {"role": "user", "content": None}]
    messages += [{"role": "assistant" if i % 2 else "user", "content": "x" * 4000} for i in range(30)]
    plan = compaction.summary_plan(messages, 2, 20, "the summary")
    shrunk = compaction.with_summary({"model": "m", "messages": messages}, plan)
    assert "the summary" in shrunk["messages"][1]["content"]


async def test_config_and_provider_details():
    blank = Settings.from_env({"MAX_QUEUE_WAIT_S": "", "KEEPALIVE_INTERVAL_S": " ", "SESSION_AWARE": ""})
    assert blank.max_queue_wait_s == Settings.max_queue_wait_s and blank.session_aware == Settings.session_aware

    message = "input length and `max_tokens` exceed context limit: 190000 + 20000 > 200000, decrease input length"
    content = json.dumps({"type": "error", "error": {"type": "invalid_request_error", "message": message}}).encode()
    assert overflow_of(400, content)[:2] == (True, 200000)

    sent = upstream_headers(
        {"api-key": "a", "x-goog-api-key": "b", "openai-organization": "o", "openai-project": "p", "x-title": "t"}, "k"
    )
    assert set(sent) == {"x-title", "authorization", "accept-encoding"}

    def bare_list(request):
        return httpx.Response(200, json=[{"id": "one"}, {"id": 2}])

    async with httpx.AsyncClient(transport=httpx.MockTransport(bare_list)) as client:
        assert await fetch_models(client, "https://example.com/v1", "k") == [{"id": "one"}]
