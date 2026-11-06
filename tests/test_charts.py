"""Server-rendered usage charts and the dashboard's live history."""

from datetime import date

import httpx

from app.admin.charts import compact, day_range, usage_charts
from tests.test_admin import OWNER, login


def test_day_range_and_compact():
    assert day_range(3, date(2026, 3, 1)) == ["2026-02-27", "2026-02-28", "2026-03-01"]
    assert compact(1_250_000) == "1.25M" and compact(999) == "999" and compact(0.5) == "0.5"


def test_usage_charts():
    totals = {
        "2026-03-01": {"requests": 6, "errors": 1, "prompt_tokens": 1500, "completion_tokens": 300, "cost": 0},
        "2025-01-01": {"requests": 9, "errors": 9, "prompt_tokens": 9, "completion_tokens": 9, "cost": 0},
    }
    charts = usage_charts(totals, day_range(7, date(2026, 3, 1)))
    assert len(charts) == 2  # no cost chart without prices
    tokens, requests = charts
    assert "Tokens per day" in tokens and "2026-03-01 · in: 1.5k · out: 300" in tokens  # days outside: left out
    assert "ok: 5 · errors: 1" in requests
    assert tokens.count("<g>") == 7  # every day of the period, even empty ones
    assert "<script" not in tokens


def test_usage_charts_per_hour():
    totals = {"2026-03-01T14": {"requests": 1, "errors": 0, "prompt_tokens": 10, "completion_tokens": 2, "cost": 0.5}}
    charts = usage_charts(totals, ["2026-03-01T13", "2026-03-01T14"], "hour")
    tokens, _, cost = charts
    assert "Tokens per hour" in tokens and "2026-03-01 14:00 · in: 10 · out: 2" in tokens
    assert ">14:00</text>" in tokens and "Cost per hour" in cost and tokens.count("<g>") == 2


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
