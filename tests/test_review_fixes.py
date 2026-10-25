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


def test_second_review_config_and_providers():
    from app.config import Bootstrap
    from app.limits import overflow_of
    from app.opencode import model_limits
    from app.params import apply_rules

    payload = {"temperature": 1, "max_tokens": 9000}
    apply_rules(payload, {"clamps": {"temperature": {"max": 0.7}, "max_tokens": {"max": 4096}}}, {})
    assert payload == {"temperature": 0.7, "max_tokens": 4096}  # an int under a float bound isn't cut to 0

    text = "Please reduce the length of the messages or completion. Current length is 9000 while limit is 8192"
    groq = json.dumps({"error": {"message": text}}).encode()
    assert overflow_of(400, groq)[:2] == (True, 8192)
    assert model_limits({"id": "llama", "context_window": 8192, "max_completion_tokens": 1024}, ())["context"] == 8192

    boot = Bootstrap.from_env({"DATA_DIR": "", "LOG_LEVEL": " "})
    assert boot.data_dir == Bootstrap.data_dir and boot.log_level == Bootstrap.log_level.upper()
    assert Settings.from_env({"UPSTREAM_URL": ""}).upstream_url == Settings.upstream_url.rstrip("/")


def test_second_review_admin_helpers():
    import pytest

    from app.admin.routes import _csv_cell, _user_agents

    assert _csv_cell("=HYPERLINK(1)") == "'=HYPERLINK(1)" and _csv_cell("model") == "model" and _csv_cell(3) == 3
    assert _user_agents({"user_agents": "a, b"}) == ("a", "b")
    with pytest.raises(ValueError):
        _user_agents({"user_agents": ",".join(str(i) for i in range(21))})


def test_second_review_adapters_and_council(tmp_path):
    import time
    from types import SimpleNamespace

    import pytest
    from cryptography.fernet import Fernet

    from app import anthropic_api, backup
    from app.admin.security import csrf_ok
    from app.auth import find_client
    from app.config import ClientKey
    from app.main import odd_path, one_line

    request = {
        "model": "m",
        "max_tokens": 10,
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "look:"},
                {"type": "tool_result", "tool_use_id": "c1", "content": [
                    {"type": "text", "text": "out"}, {"type": "image", "source": {"type": "base64", "data": "x"}}]},
            ]},
        ],
    }  # fmt: skip
    messages = anthropic_api.to_chat(request)["messages"]
    assert messages[0]["role"] == "tool" and "out" in messages[0]["content"]
    assert messages[1] == {"role": "user", "content": "look:"}  # the result isn't repeated, no raw image leaks
    cut = {
        "choices": [{"finish_reason": "length", "message": {"tool_calls": [{"id": "c", "function": {"name": "f"}}]}}]
    }
    assert anthropic_api.from_chat(cut, "m")["stop_reason"] == "max_tokens"

    judged = council.judge_payload({"model": "m", "messages": [{"role": "user", "content": "q"}]}, "j", "pick", [])
    assert len(judged["messages"]) == 1 and judged["messages"][0]["content"].startswith("q\n\n")
    call = {"function": {"name": "f", "arguments": {"path": "a.py"}}}
    assert websearch.call_argument(call, "path") == "a.py"
    body = json.dumps({"choices": [{"message": {"tool_calls": [{"id": "1", **call}]}}]}).encode()
    assert council.message_of(body)["tool_calls"][0]["function"]["arguments"] == '{"path": "a.py"}'

    messages = [{"role": "system", "content": "s"}] + [
        {"role": "assistant" if i % 2 == 0 else "user", "content": "x" * 100} for i in range(10)
    ]
    plan = compaction.summary_plan(messages, 1, 6, "the summary")
    shrunk = compaction.with_summary({"model": "m", "messages": messages}, plan)["messages"]
    assert shrunk[1]["role"] == "user" and "the summary" in shrunk[1]["content"]

    assert odd_path("chat/%2e%2e/admin") and not odd_path("chat/completions")
    assert one_line("a\n\nb  c") == "a b c"

    expired = ClientKey("old", "h", expires_at=time.time() - 1)
    from app.auth import hash_key

    assert find_client("k", {hash_key("k"): expired}, False) is None
    assert csrf_ok(SimpleNamespace(csrf="abc"), "é") is False

    damaged = tmp_path / "b.zip"
    damaged.write_bytes(b"PK\x03\x04garbage" + b"\0" * 30 + b"PK\x05\x06" + b"\0" * 18)
    with pytest.raises(backup.RestoreError):
        backup.stage_restore(str(tmp_path), str(damaged), Fernet(Fernet.generate_key()), False)
