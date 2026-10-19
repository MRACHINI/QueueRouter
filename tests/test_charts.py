"""Server-rendered usage charts and the dashboard's live history."""

from datetime import date

import httpx

from app.admin.charts import compact, day_range, usage_charts
from tests.test_admin import OWNER, login


def test_day_range_and_compact():
    assert day_range(3, date(2026, 3, 1)) == ["2026-02-27", "2026-02-28", "2026-03-01"]
    assert compact(1_250_000) == "1.25M" and compact(999) == "999" and compact(0.5) == "0.5"


def test_usage_charts():
    rows = [
        {"day": "2026-03-01", "requests": 4, "errors": 1, "prompt_tokens": 1000, "completion_tokens": 200, "cost": 0},
        {"day": "2026-03-01", "requests": 2, "errors": 0, "prompt_tokens": 500, "completion_tokens": 100, "cost": 0},
        {"day": "2025-01-01", "requests": 9, "errors": 9, "prompt_tokens": 9, "completion_tokens": 9, "cost": 0},
    ]
    charts = usage_charts(rows, 7, date(2026, 3, 1))
    assert len(charts) == 2  # no cost chart without prices
    tokens, requests = charts
    assert "2026-03-01 · in: 1.5k · out: 300" in tokens  # same-day rows summed, old rows ignored
    assert "ok: 5 · errors: 1" in requests
    assert tokens.count("<g>") == 7  # every day of the period, even empty ones
    assert "<script" not in tokens


async def test_usage_page_and_live_history(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = await c.get("/admin/usage?days=7")
        assert page.status_code == 200 and page.text.count('class="chart"') == 2
        dash = await c.get("/admin")
        assert 'id="live-chart"' in dash.text
        live = (await c.get("/admin/api/stats")).json()
        assert isinstance(live["history"], list)
