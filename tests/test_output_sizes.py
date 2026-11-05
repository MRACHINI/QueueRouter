"""Output sizes (the longest answer a model gives): from the provider's list, models.dev, learned from a provider
refusing a bigger answer size (the request then goes again asking for that much), or set by hand."""

import asyncio
import json
import time

import httpx

from app.limits import Limit, ModelLimits, output_limit_of
from app.opencode import listed_output
from app.pricing import PriceCatalog
from tests.test_admin import OWNER, csrf_of, login
from tests.test_model_names import opencode
from tests.test_policies import chat
from tests.test_usage import flushed

SIZES = {
    "synthetic": {
        "api": "https://api.synthetic.new/openai/v1",
        "models": {"hf:org/Model-1017": {"limit": {"context": 524_288, "output": 65_536}}},
    },
    "zai": {
        "api": "https://api.z.ai/api/paas/v4",
        "models": {"glm-9": {"limit": {"context": 1_000_000, "output": 131_072}}},
    },
    "zai-coding-plan": {
        "api": "https://api.z.ai/api/coding/paas/v4",
        "models": {"glm-9": {"limit": {"context": 1_000_000, "output": 98_304}}},
    },
    "anthropic": {"models": {"claude-x": {"limit": {"context": 1_000_000, "output": 128_000}}}},
    "r1": {"models": {"open-9b": {"limit": {"context": 100_000, "output": 8_000}}}},
    "r2": {"models": {"open-9b": {"limit": {"context": 100_000, "output": 32_000}}}},
    "r3": {"models": {"open-9b": {"limit": {"context": 100_000, "output": 100_000}}}},  # as big as the context
}


def catalog_with(raw: dict) -> PriceCatalog:
    catalog = PriceCatalog()
    catalog.use(PriceCatalog.slim(raw), time.time(), PriceCatalog.slim_sizes(raw))
    return catalog


def error(message: str, kind: str = "invalid_request_error") -> bytes:
    return json.dumps({"error": {"message": message, "type": kind}}).encode()


def test_the_output_a_model_list_gives():
    assert listed_output({"max_output_length": 65_536}) == 65_536  # Synthetic
    assert listed_output({"top_provider": {"max_completion_tokens": 8_192}}) == 8_192  # OpenRouter
    assert listed_output({"max_input_tokens": 1_000_000, "max_tokens": 128_000}) == 128_000  # Anthropic
    assert listed_output({"max_output_tokens": 0}) is None and listed_output(None) is None


def test_a_provider_refusing_an_answer_size_says_its_limit():
    openai = "max_tokens is too large: 200000. This model supports at most 16384 completion tokens, whereas you"
    assert output_limit_of(400, error(openai))[0] == 16_384
    anthropic = "max_tokens: 200000 > 128000, which is the maximum allowed number of output tokens for claude-x"
    assert output_limit_of(400, error(anthropic))[0] == 128_000
    deepseek = "Invalid max_tokens value, the valid range of max_tokens is [1, 8192]"
    assert output_limit_of(400, error(deepseek))[0] == 8_192
    groq = (
        "max_tokens must be less than or equal to `32768`, the maximum value for `max_tokens` is less than the "
        "`context_window` for this model"
    )
    assert output_limit_of(400, error(groq))[0] == 32_768
    # not an output limit: the prompt doesn't fit, or the size depends on the prompt, or no number, or not a refusal
    context = "input length and `max_tokens` exceed context limit: 190000 + 20000 > 200000"
    assert output_limit_of(400, error(context))[0] is None
    vllm = (
        "'max_tokens' or 'max_completion_tokens' is too large: 100000. This model's maximum context length is "
        "131072 tokens and your request has 500 input tokens (100000 > 131072 - 500)"
    )
    assert output_limit_of(400, error(vllm))[0] is None
    assert output_limit_of(400, error("max_tokens is too large"))[0] is None
    assert output_limit_of(503, error(openai))[0] is None


def test_learned_and_manual_output_sizes():
    limits = ModelLimits()
    limits.load(
        [
            {"source": "learned", "upstream_id": 1, "model": "m", "context": None, "output": 4_096},
            {"source": "learned", "upstream_id": 1, "model": "c", "context": 32_000, "output": None},
        ]
    )
    assert limits.learned_output == {(1, "m"): 4_096} and limits.learned == {(1, "c"): 32_000}
    assert listed_output(limits.apply("m", 1, {"id": "m", "max_tokens": 64_000})) == 4_096  # over the list's
    limits.manual[(1, "m")] = Limit(None, 2_000)
    assert listed_output(limits.apply("m", 1, None)) == 2_000  # yours wins


def test_models_dev_output_sizes():
    catalog = catalog_with(SIZES)
    assert "r3/open-9b" not in catalog.outputs  # as big as its context: says nothing
    assert catalog.provider_of("https://api.synthetic.new/openai/v1") == "synthetic"
    assert catalog.provider_of("https://api.synthetic.new/v1/") == "synthetic"  # the only one on that host
    assert catalog.provider_of("https://api.z.ai/api/coding/paas/v4") == "zai-coding-plan"  # exactly its address
    assert catalog.provider_of("https://api.z.ai/v1") is None  # two there, neither exactly
    assert catalog.provider_of("https://api.anthropic.com/v1") == "anthropic"  # its own API, no address listed
    assert catalog.provider_of("https://api.refuseless.com/v1") is None

    assert catalog.output("hf:org/Model-1017", "synthetic") == (65_536, "synthetic")  # that provider's own
    assert catalog.output("glm-9", "zai-coding-plan") == (98_304, "zai-coding-plan")
    assert catalog.output("glm-9") == (131_072, "zai")  # else the maker's
    assert catalog.output("tune-x", None, name="GLM 9") == (131_072, "zai")  # by its custom name
    assert catalog.output("open-9b") == (20_000, "median of 2 providers")
    assert catalog.output("unknown-model") is None


def test_an_older_catalog_file_is_fetched_again(tmp_path):
    path = tmp_path / "prices.json"
    path.write_text(json.dumps({"fetched_at": time.time(), "prices": PriceCatalog.slim(SIZES)}))
    old = PriceCatalog(str(path))
    assert old.load_file() and old.stale and old.outputs == {}  # no sizes in it yet: fetched again soon
    fresh = catalog_with(SIZES)
    fresh.path = str(path)
    fresh._save()
    again = PriceCatalog(str(path))
    assert again.load_file() and not again.stale and again.outputs == fresh.outputs


async def test_a_refused_answer_size_is_learned_and_the_request_goes_again(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_models"] = ["other-model"]
    stack.mock["output_limits"] = {"mock-model": 4_096, "other-model": 2_048}
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a", max_tokens=8_000))
        assert r.status_code == 200, r.text
        assert stack.mock["output_refused"] == [8_000]
        assert json.loads(stack.mock["last_body"])["max_tokens"] == 4_096  # sent again, asking for that much
        learned = [row for row in store.list_model_limits() if row["source"] == "learned"]
        assert [(row["model"], row["output"]) for row in learned] == [("mock-model", 4_096)]

        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("b", max_tokens=8_000))
        assert r.status_code == 200 and stack.mock["output_refused"] == [8_000]  # lowered before sending
        assert json.loads(stack.mock["last_body"])["max_tokens"] == 4_096
        assert (await opencode(c, stack))["mock-model"]["limit"]["output"] == 4_096

        body = chat("c", model="other-model", max_tokens=3_000, stream=True)  # streamed the same way
        async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as streamed:
            text = (await streamed.aread()).decode()
        assert streamed.status_code == 200 and "[DONE]" in text and '"error"' not in text
        assert stack.mock["output_refused"] == [8_000, 3_000]

        await flushed(stack, 3)
        rows = await asyncio.to_thread(store.list_requests, 10)
        note = "upstream gives at most 4096 output tokens: asked for 8000, sent again with 4096"
        assert any(row["status"] == 200 and note in (row["error"] or "") for row in rows)  # on the Requests page

        await login(c, stack.url)
        page = (await c.get("/admin/models")).text
        assert "learned output 4,096" in page
        upstream = store.list_upstreams()[0]
        csrf = csrf_of(page)
        form = {"csrf": csrf, "upstream_id": str(upstream.id), "model": "mock-model"}
        await c.post("/admin/model-limits/forget", data=form)
        assert (await opencode(c, stack))["mock-model"]["limit"]["output"] == 16_384  # the default again


async def test_output_sizes_from_the_list_and_from_models_dev(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_model_entries"] = [
        {"id": "syn-model", "object": "model", "context_length": 524_288, "max_output_length": 65_536}
    ]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        assert (await opencode(c, stack))["syn-model"]["limit"] == {"context": 524_288, "output": 65_536}

        raw = {
            "mockprov": {
                "api": f"{stack.mock['url']}/v1",  # models.dev lists this very provider
                "models": {"mock-model": {"limit": {"context": 262_144, "output": 30_000}}},
            }
        }
        stack.app.state.prices.use(PriceCatalog.slim(raw), time.time(), PriceCatalog.slim_sizes(raw))
        assert (await opencode(c, stack))["mock-model"]["limit"]["output"] == 30_000
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("a", max_tokens=100_000))
        assert r.status_code == 200 and json.loads(stack.mock["last_body"])["max_tokens"] == 30_000
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert listed["mock-model"]["max_output_tokens"] == 30_000  # for apps reading /v1/models too

        await login(c, stack.url)
        page = (await c.get("/admin/models")).text
        assert "models.dev: mockprov" in page and "its list" in page
