"""Synthetic as a web search provider, and a backup provider that answers when the first one fails (its limit, its
key, down), the first then left out for a while."""

import json
import time

import httpx
import pytest

from app import websearch
from app.websearch import Found, SearchError, search_any
from tests.test_admin import OWNER
from tests.test_council import ASK, rows_with, search_setup

PLAN = [("synthetic", "k", ""), ("searxng", "", "http://searx")]


def fake(monkeypatch, failing: dict):
    """``websearch.search`` replaced: a provider in ``failing`` raises that SearchError; the calls are returned."""
    asked = []

    async def search(client, provider, key, base_url, query, count):
        asked.append(provider)
        if provider in failing:
            raise failing[provider]
        return [{"title": provider, "url": f"https://{provider}.example", "snippet": query}]

    async def quota(client, key):
        return {"limit": 250, "used": 250, "renews_at": time.time() + 1200}

    monkeypatch.setattr(websearch, "search", search)
    monkeypatch.setattr(websearch, "synthetic_quota", quota)
    return asked


async def test_the_backup_answers_and_the_first_is_left_out(monkeypatch):
    over = SearchError("synthetic returned HTTP 429 (check the key and the plan's credits)", 429)
    asked = fake(monkeypatch, {"synthetic": over})
    skips: dict = {}
    found = await search_any(None, PLAN, "python 3.14", 5, skips)
    assert isinstance(found, Found) and found.provider == "searxng" and found.failed == [str(over)]
    assert asked == ["synthetic", "searxng"]
    assert 1100 < skips["synthetic"] - time.monotonic() <= 1200  # until its quota renews, as it says
    found = await search_any(None, PLAN, "again", 5, skips)
    assert asked == ["synthetic", "searxng", "searxng"] and found.failed == []  # not asked again meanwhile
    skips["synthetic"] = time.monotonic() - 1  # its hour is over: asked first again
    asked.clear()
    await search_any(None, PLAN, "later", 5, skips)
    assert asked[0] == "synthetic"


async def test_other_failures_and_no_backup(monkeypatch):
    down = SearchError("synthetic: ConnectError")
    asked = fake(monkeypatch, {"synthetic": down})
    skips: dict = {}
    await search_any(None, PLAN, "q", 5, skips)
    assert 50 < skips["synthetic"] - time.monotonic() <= websearch.SKIP_DOWN_S  # down: a minute
    asked.clear()
    fake(monkeypatch, {})  # it's back, but still left out: the backup answers, and a success keeps the skip
    found = await search_any(None, PLAN, "q", 5, skips)
    assert found.provider == "searxng" and "synthetic" in skips
    # both failing: the one left out is still tried, last; the error names both
    asked = fake(monkeypatch, {"synthetic": down, "searxng": SearchError("searxng returned HTTP 500", 500)})
    with pytest.raises(SearchError, match="searxng returned HTTP 500; synthetic: ConnectError"):
        await search_any(None, PLAN, "q", 5, skips)
    assert asked == ["searxng", "synthetic"]
    # one provider only: no skipping (it's asked each time), and its status is kept for the caller
    skips.clear()
    limit = SearchError("synthetic returned HTTP 429", 429)
    fake(monkeypatch, {"synthetic": limit})
    with pytest.raises(SearchError) as failed:
        await search_any(None, PLAN[:1], "q", 5, skips)
    assert failed.value.status == 429 and skips == {}
    # the same provider twice is one; off is none; a refused query reaches no provider
    asked = fake(monkeypatch, {})
    assert (await search_any(None, [PLAN[1], PLAN[1], ("off", "", "")], "q", 5, {})).provider == "searxng"
    with pytest.raises(SearchError, match="web search is off"):
        await search_any(None, [("off", "", ""), ("", "", "")], "q", 5, {})
    asked.clear()
    with pytest.raises(SearchError, match="refused"):
        await search_any(None, PLAN, "api_key=abc", 5, {})
    assert asked == []


async def test_synthetic_search_and_its_quota(make_stack, monkeypatch):
    stack = make_stack()
    monkeypatch.setattr(websearch, "SYNTHETIC_URL", f"{stack.mock['url']}/v2")
    async with httpx.AsyncClient(timeout=10) as c:
        found = await websearch.search(c, "synthetic", "syn-key", "", "winfsp unlink", 5)
        assert found == [
            {"title": "Synthetic result for winfsp unlink", "url": "https://example.com/s",
             "snippet": "page text about winfsp unlink"}
        ]  # fmt: skip
        assert stack.mock["synthetic_keys"] == ["Bearer syn-key"]
        quota = await websearch.synthetic_quota(c, "syn-key")
        assert quota["limit"] == 250 and quota["used"] == 1 and 1700 < quota["renews_at"] - time.time() <= 1800
        with pytest.raises(SearchError, match="needs a key"):
            await websearch.search(c, "synthetic", "", "", "q", 5)
        stack.mock["synthetic_status"] = 429
        with pytest.raises(SearchError) as failed:
            await websearch.search(c, "synthetic", "syn-key", "", "q", 5)
        assert failed.value.status == 429


async def test_a_council_searches_through_the_backup(make_stack, monkeypatch):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    monkeypatch.setattr(websearch, "SYNTHETIC_URL", f"{stack.mock['url']}/v2")
    stack.mock["searching_models"] = ["mock-model"]
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        settings = {"search_provider": "synthetic", "search_backup": "searxng"}  # no key: the Synthetic upstream's
        await search_setup(c, stack, "mock-model, small-model", **settings)
        upstream = store.list_upstreams()[0]
        store.update_upstream(upstream.id, "t", preset="synthetic")
        stack.app.state.reload()
        ask = {"model": "team", "messages": ASK}
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=ask)
        assert r.status_code == 200, r.text
        assert stack.mock["synthetic_searches"] == ["latest benchmarks"] and "searches" not in stack.mock
        assert stack.mock["synthetic_keys"] == ["Bearer upstream-secret"]  # its upstream's key
        assert "https://example.com/s" in stack.mock["judge_inputs"][-1]
        page = (await c.get("/admin/settings")).text
        assert "Synthetic: 249 of 250 searches left this hour (renews " in page

        stack.mock["synthetic_status"] = 429  # its hourly quota is used up
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=ask)
        assert r.status_code == 200, r.text
        assert stack.mock["searches"] == ["latest benchmarks"]  # the backup answered
        assert "https://example.com/1" in stack.mock["judge_inputs"][-1]
        asked = len(stack.mock["synthetic_searches"])
        r = await c.post("/v1/chat/completions", headers=stack.headers, json=ask)
        assert r.status_code == 200 and len(stack.mock["synthetic_searches"]) == asked  # left out: not asked again
        assert len(stack.mock["searches"]) == 2
        page = (await c.get("/admin/settings")).text
        assert "synthetic failed its last search: the other provider answers for" in page
        assert 'name="search_backup"' in page and '<option value="searxng" selected>' in page
    rows = await rows_with(store, lambda row: row["council"] and "from searxng" in (row["timeline"] or ""))
    notes = [note for row in rows if row["timeline"] for _, note, _ in json.loads(row["timeline"])]
    assert any(n.endswith("1 results from searxng (synthetic returned HTTP 429 (check the key and the plan's credits))")
               for n in notes)  # fmt: skip
