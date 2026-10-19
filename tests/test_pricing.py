"""Reference prices: models.dev matching, overrides, cost math, and the Usage page."""

import asyncio
import time

import httpx
import pytest

from app.pricing import PriceCatalog, candidate_keys, resolve, summarize_usage, token_cost
from tests.test_admin import OWNER, csrf_of, login

RAW = {
    "deepseek": {"models": {"deepseek-v4-flash": {"cost": {"input": 0.15, "output": 0.6, "cache_read": 0.003}}}},
    "ofox": {"models": {"deepseek/deepseek-v4-flash-0731": {"cost": {"input": 0.44, "output": 1.32}}}},
    "reseller-a": {"models": {"open-model-7b": {"cost": {"input": 0.1, "output": 0.2}}}},
    "reseller-b": {"models": {"open-model-7b": {"cost": {"input": 0.3, "output": 0.6}}}},
    "reseller-c": {"models": {"open-model-7b": {"cost": {"input": 0.2, "output": 0.4}}}},
    "some-coding-plan": {"models": {"open-model-7b": {"cost": {"input": 0, "output": 0}}}},
    "broken": {"models": {"x": {"cost": {"input": "?"}}}},
}


@pytest.fixture
def catalog():
    c = PriceCatalog()
    c.use(PriceCatalog.slim(RAW), time.time())
    return c


def test_candidate_keys():
    assert candidate_keys("DeepSeek-V4-Flash-0731") == ["deepseekv4flash0731", "deepseekv4flash"]
    assert candidate_keys("org/Model-7B") == ["model7b"]


def test_official_price_wins_over_resellers(catalog):
    price = catalog.match("DeepSeek-V4-Flash-0731")
    assert (price.input, price.output, price.cached_input) == (0.15, 0.6, 0.003)
    assert price.source == "official (deepseek)" and price.reference == "deepseek/deepseek-v4-flash"


def test_median_when_creator_does_not_sell_it(catalog):
    price = catalog.match("open-model-7B")
    assert (price.input, price.output) == (0.2, 0.4)
    assert price.cached_input == 0.2  # no cache price listed -> same as input
    assert price.source == "median of 3 providers"  # the $0 "plan" listing is ignored


def test_no_match(catalog):
    assert catalog.match("totally-unknown-model") is None


def test_overrides(catalog):
    manual = {"m": {"reference": None, "input": 1.0, "output": 2.0, "cached_input": None}}
    price = resolve("m", manual, catalog)
    assert (price.input, price.output, price.cached_input, price.source) == (1.0, 2.0, 1.0, "manual")
    referenced = {
        "open-model-7b": {
            "reference": "deepseek/deepseek-v4-flash",
            "input": None,
            "output": None,
            "cached_input": None,
        }
    }
    assert resolve("open-model-7b", referenced, catalog).input == 0.15


def test_token_cost_uses_cached_rate(catalog):
    price = catalog.match("deepseek-v4-flash")
    cost_in, cost_out = token_cost(price, prompt=1_000_000, completion=500_000, cached=800_000)
    assert cost_in == pytest.approx(200_000 * 0.15 / 1e6 + 800_000 * 0.003 / 1e6)
    assert cost_out == pytest.approx(0.3)
    assert token_cost(None, 1, 1, 0) is None


def test_summarize_usage_totals_and_savings(catalog):
    rows = [
        {
            "day": "2026-10-19",
            "client": "oc",
            "model": "DeepSeek-V4-Flash-0731",
            "requests": 3,
            "errors": 0,
            "prompt_tokens": 2_000_000,
            "completion_tokens": 1_000_000,
            "cached_tokens": 0,
        },
        {
            "day": "2026-10-19",
            "client": "oc",
            "model": "unknown",
            "requests": 1,
            "errors": 1,
            "prompt_tokens": 100,
            "completion_tokens": 0,
            "cached_tokens": 0,
        },
    ]
    out, totals, _ = summarize_usage(rows, {}, catalog, by_model=False, plan_per_month=30.0, days=30)
    assert len(out) == 1 and out[0]["requests"] == 4 and out[0]["priced"] is False
    assert totals["cost_in"] == pytest.approx(0.3) and totals["cost_out"] == pytest.approx(0.6)
    assert totals["unpriced_tokens"] == 100
    assert totals["plan_cost"] == pytest.approx(30 * 30 / 30.4375)
    assert totals["savings"] == pytest.approx(0.9 - totals["plan_cost"])


async def test_usage_page_shows_costs(make_stack):
    stack = make_stack(bootstrap=OWNER)
    stack.app.state.prices.use(PriceCatalog.slim(RAW), time.time())
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        # manual price for the mock model: $1 in, $0.10 cached, $2 out per million
        csrf = csrf_of((await c.get("/admin/models")).text)
        form = {
            "csrf": csrf,
            "model": "mock-model",
            "reference": "",
            "input": "1",
            "cached_input": "0.1",
            "output": "2",
        }
        assert "m=saved" in (await c.post("/admin/prices", data=form)).headers["location"]
        page = (await c.get("/admin/models")).text
        assert "manual" in page and "deepseek/deepseek-v4-flash" in page  # reference picker choices

        # plan price on the pool -> savings
        pool = stack.app.state.store.list_pools()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        pool_form = {
            "csrf": csrf,
            "plan": "Pro",
            "max_concurrency": "",
            "context_cap": "",
            "min_spacing_ms": "0",
            "monthly_price": "",
        }
        await c.post(f"/admin/pools/{pool.id}", data=pool_form)
        assert stack.app.state.store.list_pools()[0].monthly_price == 30

        body = {"model": "mock-model", "messages": [{"role": "user", "content": "x"}], "mock_cached": 4}
        await c.post("/v1/chat/completions", headers=stack.headers, json=body)
        store = stack.app.state.store
        for _ in range(100):
            if await asyncio.to_thread(store.list_requests, 10):
                break
            await asyncio.sleep(0.05)
        assert (await asyncio.to_thread(store.list_requests, 10))[0]["cached_tokens"] == 4

        usage = (await c.get("/admin/usage")).text
        assert "At official prices" in usage and "Plan costs more by" in usage and "plan: " in usage
        csv_rows = (await c.get("/admin/usage.csv?by=model")).text.strip().splitlines()
        cost = float(csv_rows[1].split(",")[-1])
        # 10 prompt (4 cached) + 5 completion tokens
        assert cost == pytest.approx((6 * 1 + 4 * 0.1 + 5 * 2) / 1e6, abs=1e-4)
