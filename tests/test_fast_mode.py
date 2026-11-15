"""An alias's fast mode: its model goes to Synthetic as fast:<model>, asked again as it is when that's refused;
fast usage is kept apart and counts double against a weekly quota (the price per token is the same)."""

import httpx
import pytest

from app.pricing import alias_costs, model_costs
from tests.conftest import eventually
from tests.test_admin import OWNER, csrf_of, login
from tests.test_usage import flushed
from tests.test_usage_pages import catalog, usage_row  # noqa: F401 (catalog: a fixture)


async def add_alias(c, name, target, fast=True, fallbacks=()):
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "name": name, "target_model": target, "fallbacks": ", ".join(fallbacks), "on_errors": "on"}
    if fast:
        form["fast"] = "on"
    r = await c.post("/admin/aliases", data=form)
    assert "m=created" in r.headers["location"], r.headers["location"]


def synthetic(stack) -> None:
    store = stack.app.state.store
    store.update_upstream(store.list_upstreams()[0].id, "t", preset="synthetic")
    stack.app.state.reload()


def chat(model, **extra):
    return {"model": model, "messages": [{"role": "user", "content": "hi"}], "mock_tag": "x", **extra}


def last_entry(stack) -> dict:
    return list(stack.app.state.stats.recent)[-1]


async def test_a_fast_alias_goes_to_synthetic_as_fast(make_stack):
    stack = make_stack(bootstrap=OWNER)
    synthetic(stack)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "quick", "mock-model")
        assert next(a for a in store.list_aliases() if a.name == "quick").fast
        assert ">fast</span>" in (await c.get("/admin/models")).text  # its badge
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("quick"))
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
        assert stack.mock["models"] == ["fast:mock-model"]
        entry = last_entry(stack)
        assert entry["fast"] == 1 and entry["target_model"] == "mock-model" and not entry.get("fallback")
        await flushed(stack, 1)

        def usage() -> dict:
            rows = store.query("SELECT alias, model, SUM(requests) AS n FROM usage_daily GROUP BY 1, 2")
            return {(row["alias"], row["model"]): row["n"] for row in rows}

        await eventually(lambda: usage() == {("quick", "fast:mock-model"): 1}, timeout=10)
        rows = (await c.get("/admin/api/requests")).json()["rows"]
        assert [row["fast"] for row in rows] == [1]
        body = (await c.get("/admin/usage/body")).text
        assert "(fast)" in body
        assert "fast:mock-model" not in (await c.get("/admin/models")).text  # priced as the model: no line of its own
        # an alias without it, and the alias's fallbacks, go as they are
        await add_alias(c, "plain", "mock-model", fast=False)
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("plain"))
        assert r.status_code == 200 and stack.mock["models"][-1] == "mock-model" and not last_entry(stack)["fast"]


@pytest.mark.parametrize("stream", [False, True])
async def test_refused_fast_the_same_model_is_asked_again_as_it_is(make_stack, stream):
    stack = make_stack(bootstrap=OWNER, max_retries=0)
    synthetic(stack)
    stack.mock["missing_models"] = ["fast:mock-model"]  # this model has no fast mode
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "quick", "mock-model")  # no fallbacks, no triggers needed
        if stream:
            async with c.stream(
                "POST", "/v1/chat/completions", headers=stack.headers, json=chat("quick", stream=True)
            ) as r:
                text = "".join([chunk async for chunk in r.aiter_text()])
            assert r.status_code == 200 and text.rstrip().endswith("data: [DONE]") and "error" not in text
        else:
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("quick"))
            assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
    assert stack.mock["models"] == ["fast:mock-model", "mock-model"]
    entry = last_entry(stack)
    assert not entry["fast"] and "fast mode refused: HTTP 404" in entry["fallback"]


@pytest.mark.parametrize(
    "trouble",
    [{"mock_fail_models": ["fast:mock-model"]}, {"mock_429_models": {"fast:mock-model": "daily"}}],
    ids=["503", "quota"],
)
async def test_failed_fast_isnt_retried_nor_held_against_the_provider(make_stack, trouble):
    """A 503 or a used-up quota for the fast request: the model as it is at once, the provider still up and the
    model not blocked (its fast mode failed, not Synthetic)."""
    stack = make_stack(bootstrap=OWNER, max_retries=3)
    synthetic(stack)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "quick", "mock-model")
        for _ in range(4):  # more than it takes to open the circuit, were these failures
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("quick", **trouble))
            assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "hi x"
    assert stack.mock["models"] == ["fast:mock-model", "mock-model"] * 4
    assert last_entry(stack)["retries"] == 0


async def test_refused_fast_then_the_fallbacks(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=0)
    synthetic(stack)
    stack.mock["missing_models"] = ["fast:big-model"]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "quick", "big-model", fallbacks=["mock-model"])
        ask = chat("quick", mock_fail_models=["big-model"])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=ask)
        assert r.status_code == 200 and r.headers["x-queuerouter-model"] == "mock-model"
    assert stack.mock["models"] == ["fast:big-model", "big-model", "mock-model"]  # the fallback isn't sent fast


async def test_fast_mode_is_only_for_synthetic(make_stack):
    stack = make_stack(bootstrap=OWNER)  # its upstream is no Synthetic
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_alias(c, "quick", "mock-model")
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat("quick"))
        assert r.status_code == 200
    assert stack.mock["models"] == ["mock-model"] and not last_entry(stack)["fast"]


def test_fast_usage_costs_the_same_and_counts_double_against_the_quota(catalog):  # noqa: F811
    rows = [
        usage_row("glm-4.7", 1_000_000, 0, 0, alias="quick"),
        usage_row("fast:glm-4.7", 1_000_000, 0, 0, alias="quick"),
    ]
    (quick,) = alias_costs(rows, {}, catalog)
    assert quick["cost"] == pytest.approx(2) and quick["quota_cost"] == pytest.approx(3)
    labels = sorted(m["label"] for m in quick["models"])
    assert labels[0].endswith("glm-4.7") and labels[1] == f"{labels[0]} (fast)"
    models, total = model_costs(rows, {}, catalog)
    assert sorted(m["quota_cost"] for m in models) == [pytest.approx(1), pytest.approx(2)]
    assert total["cost"] == pytest.approx(2) and total["quota_cost"] == pytest.approx(3)
