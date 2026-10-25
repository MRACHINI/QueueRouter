import pytest

from app.config import Bootstrap, ParsedKey, Settings, parse_access_keys
from app.sessions import session_key


def test_parse_access_keys_formats():
    assert parse_access_keys(("k1", "chat:k2", "batch:k3:low")) == (
        ParsedKey("client1", "k1", "normal"),
        ParsedKey("chat", "k2", "normal"),
        ParsedKey("batch", "k3", "low"),
    )


@pytest.mark.parametrize(
    "entries",
    [("a:k:urgent",), ("a:b:c:d",), (":k",), ("a:k", "a:k2")],
)
def test_parse_access_keys_rejects_bad_entries(entries):
    with pytest.raises(ValueError):
        parse_access_keys(entries)


def test_from_env_scheduling_settings():
    s = Settings.from_env(
        {
            "ARLIAI_API_KEY": "x",
            "ACCESS_KEYS": "opencode:abc:high",
            "AGING_S": "30",
            "SESSION_AWARE": "false",
            "FAIR_SHARE": "no",
        }
    )
    assert s.access_keys == ("opencode:abc:high",)
    assert (s.aging_s, s.session_aware, s.fair_share) == (30, False, False)


def test_bootstrap_from_env():
    b = Bootstrap.from_env({"DATA_DIR": "/tmp/qr", "ADMIN_ALLOWED_IPS": "10.0.0.0/8", "PUBLIC_URL": "https://x/"})
    assert b.data_dir == "/tmp/qr" and b.public_url == "https://x" and b.secret_key is None
    assert [str(n) for n in b.admin_allowed_ips] == ["10.0.0.0/8"]
    assert [str(n) for n in b.trusted_proxies] == ["127.0.0.1/32", "::1/128"]  # only the machine itself


def chat_body(system="sys", first_user="task", *more):
    messages = [{"role": "system", "content": system}, {"role": "user", "content": first_user}]
    messages += [{"role": "assistant", "content": m} for m in more]
    return {"model": "m", "messages": messages}


def test_session_key_stable_as_conversation_grows():
    a = session_key({}, "c", chat_body())
    b = session_key({}, "c", chat_body("sys", "task", "step 1", "step 2"))
    assert a == b and a is not None


def test_session_key_differs_for_other_tasks_and_clients():
    base = session_key({}, "c", chat_body())
    assert session_key({}, "c", chat_body(first_user="title this")) != base
    assert session_key({}, "c", chat_body(system="subagent")) != base
    assert session_key({}, "other", chat_body()) != base


def test_session_key_explicit_header_and_fallbacks():
    assert session_key({"x-session-id": "s1"}, "c", {}) == session_key({"x-session-id": "s1"}, "c", chat_body())
    assert session_key({}, "c", {"model": "m", "prompt": "hello"}) is not None
    assert session_key({}, "c", {"model": "m"}) is None
    assert session_key({}, "c", []) is None


def test_untrusted_proxy_is_ignored_and_reported(caplog):
    import ipaddress
    from types import SimpleNamespace

    from app.auth import client_ip

    request = SimpleNamespace(client=SimpleNamespace(host="172.18.0.5"), headers={"x-forwarded-for": "10.9.9.9"})
    with caplog.at_level("WARNING", logger="queuerouter"):
        assert client_ip(request, (ipaddress.ip_network("127.0.0.1/32"),)) == "172.18.0.5"
    assert "untrusted_proxy" in caplog.text
    assert client_ip(request, (ipaddress.ip_network("172.18.0.0/16"),)) == "10.9.9.9"
