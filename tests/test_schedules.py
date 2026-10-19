"""Recurring jobs: cron parsing and schedules."""

import asyncio
import calendar
import time

import httpx
import pytest

from app.cron import CronError, parse
from tests.test_admin import OWNER, csrf_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


def at(*parts) -> float:
    return float(calendar.timegm((*parts, 0)))


def test_cron():
    saturday_noon = at(2026, 10, 17, 12, 0)
    assert parse("*/15 * * * *").next_after(at(2026, 10, 17, 12, 7)) == at(2026, 10, 17, 12, 15)
    assert parse("0 9 * * mon-fri").next_after(saturday_noon) == at(2026, 10, 19, 9, 0)  # monday
    assert parse("@daily").next_after(saturday_noon) == at(2026, 10, 18, 0, 0)
    assert parse("30 2 1 jan,jul *").next_after(saturday_noon) == at(2027, 1, 1, 2, 30)
    # both day fields restricted: either matches (the 1st, or a sunday)
    assert parse("0 0 1 * sun").next_after(saturday_noon) == at(2026, 10, 18, 0, 0)
    assert parse("0 0 * * 7").next_after(saturday_noon) == at(2026, 10, 18, 0, 0)  # 7 = sunday too
    assert parse("0 0 29 2 *").next_after(at(2026, 3, 1, 0, 0)) == at(2028, 2, 29, 0, 0)
    for bad in ("* * * *", "61 * * * *", "0 0 31 2 *", "*/0 * * * *", "0 0 * * funday"):
        with pytest.raises(CronError):
            parse(bad)


async def test_schedules_api(make_stack):
    stack = make_stack()
    h, store = stack.headers, stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post(
            "/v1/schedules", headers=h, json={"name": "every minute", "cron": "* * * * *", "request": CHAT}
        )
        assert r.status_code == 201
        schedule = r.json()
        assert 0 < schedule["next_run"] - time.time() <= 60 and schedule["enabled"]
        assert (
            await c.post("/v1/schedules", headers=h, json={"name": "x", "cron": "bad", "request": CHAT})
        ).status_code == 400

        # due: the worker creates a job and moves the schedule on
        store.set_schedule(schedule["id"], next_run=time.time() - 1)
        for _ in range(50):
            row = store.get_schedule(schedule["id"])
            if row["last_job_id"]:
                break
            await asyncio.sleep(0.1)
        assert row["next_run"] > time.time()
        job = (await c.get(f"/v1/jobs/{row['last_job_id']}", headers=h)).json()
        assert "schedule" in job["tags"]

        manual = await c.post(f"/v1/schedules/{schedule['id']}/run", headers=h)
        assert manual.status_code == 202
        assert store.get_schedule(schedule["id"])["next_run"] == row["next_run"]  # rhythm unchanged

        paused = await c.patch(f"/v1/schedules/{schedule['id']}", headers=h, json={"enabled": False})
        assert paused.json()["enabled"] is False and paused.json()["next_run"] is None
        assert len((await c.get("/v1/schedules", headers=h)).json()["data"]) == 1
        assert (await c.delete(f"/v1/schedules/{schedule['id']}", headers=h)).json()["deleted"]
        assert (await c.get(f"/v1/schedules/{schedule['id']}", headers=h)).status_code == 404


async def test_schedules_admin(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    client = store.list_clients()[0].name
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/jobs")).text)
        form = {"csrf": csrf, "name": "nightly", "client": client, "cron": "0 2 * * *", "request": '{"model": "m"}'}
        assert "m=created" in (await c.post("/admin/schedules", data=form)).headers["location"]
        bad = await c.post("/admin/schedules", data={**form, "request": "not json"})
        assert "e=schedule_invalid" in bad.headers["location"]
        page = (await c.get("/admin/jobs")).text
        assert "nightly" in page and "0 2 * * *" in page
        schedule = store.list_schedules()[0]
        await c.post(f"/admin/schedules/{schedule['id']}/toggle", data={"csrf": csrf})
        assert not store.get_schedule(schedule["id"])["enabled"]
        await c.post(f"/admin/schedules/{schedule['id']}/delete", data={"csrf": csrf})
        assert store.list_schedules() == []
