"""opencode integration: generated config, well-known manifest, limits."""

import ipaddress

import httpx

from app.opencode import KEY_ENV, model_limits
from tests.test_admin import OWNER, csrf_of, login
from tests.test_aliases import add_alias
from tests.test_policies import set_policy


def test_model_limits():
    assert model_limits({"max_model_len": 262_144}, [None]) == {"context": 262_144, "output": 16_384}
    assert model_limits({"context_length": 200_000}, [32_768]) == {"context": 32_768, "output": 16_384}
    assert model_limits({"max_model_len": 8_192}, []) == {"context": 8_192, "output": 4_096}
    assert model_limits({"max_model_len": 100_000, "max_output_tokens": 8_000}, []) == {
        "context": 100_000,
        "output": 8_000,
    }
    assert model_limits({}, [131_072]) == {"context": 131_072, "output": 16_384}  # plan cap only
    assert model_limits({}, [None]) is None


async def test_opencode_config(make_stack):
    stack = make_stack(bootstrap={**OWNER, "public_url": "https://qr.example.com"})
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        assert (await c.get("/v1/opencode-config")).status_code == 401
        config = (await c.get("/v1/opencode-config", headers=stack.headers)).json()
        provider = config["provider"]["queuerouter"]
        assert provider["npm"] == "@ai-sdk/openai-compatible"
        assert provider["options"] == {
            "baseURL": "https://qr.example.com/v1",
            "apiKey": f"{{env:{KEY_ENV}}}",
            "timeout": False,
        }
        assert provider["models"]["mock-model"]["limit"] == {"context": 262_144, "output": 16_384}

        # an ArliAI Core plan on the pool caps the advertised context; aliases are included
        await login(c, stack.url)
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        form = {"csrf": csrf, "plan": "Core", "max_concurrency": "", "context_cap": "", "min_spacing_ms": "0"}
        await c.post(f"/admin/pools/{pool.id}", data=form)
        await add_alias(c, "smart", "mock-model")
        models = (await c.get("/v1/opencode-config", headers=stack.headers)).json()["provider"]["queuerouter"]["models"]
        assert models["mock-model"]["limit"]["context"] == 32_768
        assert models["smart"] == {"name": "smart → mock-model", "limit": {"context": 32_768, "output": 16_384}}

        # only allowed models are offered
        await set_policy(c, stack, allowed_models="smart")
        models = (await c.get("/v1/opencode-config", headers=stack.headers)).json()["provider"]["queuerouter"]["models"]
        assert list(models) == ["smart"]


async def test_wellknown_manifest_and_root_redirect(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        root = await c.get("/")
        assert root.status_code == 303 and root.headers["location"] == "/admin"

        posix = (await c.get("/.well-known/opencode")).json()  # no key needed: it holds no secrets
        assert posix["auth"]["env"] == KEY_ENV and posix["auth"]["command"][0] == "sh"
        assert posix["remote_config"] == {
            "url": f"{stack.url}/v1/opencode-config",
            "headers": {"Authorization": f"Bearer {{env:{KEY_ENV}}}"},
        }
        windows = (await c.get("/windows/.well-known/opencode")).json()
        assert windows["auth"]["command"][0] == "powershell"


async def test_wellknown_behind_trusted_proxy(make_stack):
    """Behind a trusted reverse proxy, the public scheme and host are used."""
    stack = make_stack(bootstrap={"trusted_proxies": (ipaddress.ip_network("127.0.0.1/32"),)})
    forwarded = {"X-Forwarded-Proto": "https", "X-Forwarded-Host": "qr.example.com"}
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        manifest = (await c.get("/.well-known/opencode", headers=forwarded)).json()
    assert manifest["remote_config"]["url"] == "https://qr.example.com/v1/opencode-config"
