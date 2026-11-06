"""The Usage page's periods (UTC, to the hour) and the hourly usage totals behind them, filled from the request log
on upgrade for the days it holds all of."""

import time
from datetime import UTC, datetime

import httpx
import pytest
from cryptography.fernet import Fernet

from app.admin import periods
from app.store import _ADD_USAGE, HOURLY_USAGE, MIGRATIONS, Store
from tests.test_admin import OWNER, login

NOW = datetime(2026, 11, 6, 14, 25, tzinfo=UTC).timestamp()


def at(text: str) -> int:
    return int(datetime.fromisoformat(f"{text}+00:00").timestamp())


@pytest.mark.parametrize(
    "choice, start, end",
    [
        ("24h", "2026-11-05T15:00", "2026-11-06T15:00"),  # the current hour and the 23 before it
        ("", "2026-11-05T15:00", "2026-11-06T15:00"),  # the default
        ("nonsense", "2026-11-05T15:00", "2026-11-06T15:00"),
        ("1d", "2026-11-06T00:00", "2026-11-07T00:00"),  # today
        ("7d", "2026-10-30T15:00", "2026-11-06T15:00"),
        ("30d", "2026-10-07T15:00", "2026-11-06T15:00"),
        ("custom", "2026-10-31T00:00", "2026-11-07T00:00"),  # no days given: the last 7, today included
    ],
)
def test_periods(choice, start, end):
    period = periods.resolve(choice, now=NOW)
    assert (period.start, period.end) == (at(start), at(end))
    assert period.choice == (choice if choice in periods.CHOICES else "24h")


def test_a_custom_period():
    period = periods.resolve("custom", "2026-11-01", "6", "2026-11-03", "18", now=NOW)
    assert (period.start, period.end) == (at("2026-11-01T06:00"), at("2026-11-03T18:00"))
    assert (period.first, period.first_hour, period.last, period.last_hour) == ("2026-11-01", 6, "2026-11-03", 18)
    assert period.text(NOW) == "From 2026-11-01 06:00 to 2026-11-03 18:00 UTC"
    assert len(period.day_slots()) == 3 and period.hours == 60
    whole_days = periods.resolve("custom", "2026-11-01", "", "2026-11-01", "", now=NOW)  # both days count
    assert (whole_days.start, whole_days.end) == (at("2026-11-01T00:00"), at("2026-11-02T00:00"))
    assert whole_days.hour_slots()[0] == "2026-11-01T00" and len(whole_days.hour_slots()) == 24
    swapped = periods.resolve("custom", "2026-11-03", "0", "2026-11-01", "24", now=NOW)
    assert (swapped.start, swapped.end) == (at("2026-11-02T00:00"), at("2026-11-03T00:00"))
    same = periods.resolve("custom", "2026-11-03", "5", "2026-11-03", "5", now=NOW)
    assert same.hours == 1
    long = periods.resolve("custom", "2020-01-01", "x", "2026-11-01", "99", now=NOW)
    assert long.end == at("2026-11-02T00:00") and long.end - long.start == periods.MOST_DAYS * periods.DAY
    assert (long.first_hour, long.last_hour) == (0, 24)
    bad = periods.resolve("custom", "soon", "", "later", "", now=NOW)
    assert (bad.first, bad.last) == ("2026-10-31", "2026-11-06")
    assert periods.resolve("24h", now=NOW).text(NOW) == "Since 2026-11-05 15:00 UTC"


def old_store(tmp_path) -> Store:
    store = Store(str(tmp_path / "old.db"), Fernet(Fernet.generate_key()))
    for version, script in enumerate(MIGRATIONS[: HOURLY_USAGE - 1], 1):
        store._db.executescript(f"BEGIN;\n{script}\nPRAGMA user_version={version};\nCOMMIT;")
    return store


def used(store, rid, ts, logged=True, outcome="done", council=None, tokens=10, client="ali"):
    """A finished request as the version before kept it: in the request log (unless that was off) and the daily
    totals (unless refused or a council)."""
    if logged:
        store._db.execute(
            "INSERT INTO requests (id, ts, client, model, upstream, outcome, council, prompt_tokens, "
            "completion_tokens, cached_tokens, cache_write_tokens) VALUES (?, ?, ?, 'm', 'up', ?, ?, ?, 1, 0, 0)",
            (rid, ts, client, outcome, council, tokens),
        )
    if outcome != "refused" and not council:
        day = time.strftime("%Y-%m-%d", time.gmtime(ts))
        counts = (client, "m", "up", 0 if outcome == "done" else 1, tokens, 1, 0, 0)
        store._db.execute(_ADD_USAGE.format(table="usage_daily", slot="day"), (day, *counts))


def test_the_upgrade_fills_hours_for_the_days_the_log_holds(tmp_path):
    store = old_store(tmp_path)
    midnight = int(time.time() // 86_400) * 86_400
    today, yesterday, before = (time.strftime("%Y-%m-%d", time.gmtime(midnight - n * 86_400)) for n in (0, 1, 2))
    used(store, "t1", midnight + 1)
    used(store, "t2", midnight + 2, outcome="error", client="bilal")
    used(store, "t3", midnight + 3, outcome="refused")  # not usage
    used(store, "t4", midnight + 4, council="pick 2/3")  # its members' calls are
    used(store, "y1", midnight - 86_400 + 5 * 3600, tokens=7)
    used(store, "b1", midnight - 2 * 86_400 + 3600)
    used(store, "b2", midnight - 2 * 86_400 + 7200, logged=False)  # the log was off for a while that day
    store.migrate()
    assert store.schema_version() == len(MIGRATIONS)
    assert store.hourly_usage_from() == yesterday  # the day before didn't match: whole days up to there
    hours = {(r["hour"], r["client"]): tuple(r)[4:] for r in store.query("SELECT * FROM usage_hourly")}
    assert hours == {
        (f"{today}T00", "ali"): (1, 0, 10, 1, 0, 0),
        (f"{today}T00", "bilal"): (1, 1, 10, 1, 0, 0),
        (f"{yesterday}T05", "ali"): (1, 0, 7, 1, 0, 0),
    }
    rows = store.usage_between(midnight - 3 * 86_400, midnight + 86_400)
    assert [(r["day"], r["hour"], r["requests"]) for r in rows if r["day"] == before] == [(before, None, 2)]
    assert sum(r["requests"] for r in rows) == 5

    store.record_requests([finished("n", time.time())])
    assert sum(r["requests"] for r in store.usage_between(midnight, midnight + 86_400)) == 3


def finished(rid, ts):
    return {"id": rid, "ts": ts, "client": "ali", "model": "m", "stream": 0, "retries": 0, "outcome": "done"}


def test_a_log_that_holds_everything_needs_no_whole_days(tmp_path):
    store = old_store(tmp_path)
    midnight = int(time.time() // 86_400) * 86_400
    used(store, "a", midnight - 40 * 86_400)
    used(store, "b", midnight + 1)
    store.migrate()
    assert store.hourly_usage_from() == ""
    assert all(r["hour"] for r in store.usage_between(midnight - 41 * 86_400, midnight + 86_400))


async def test_whole_days_are_said_and_charted_per_day(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    now = time.time()
    today, yesterday = (time.strftime("%Y-%m-%d", time.gmtime(now - n * 86_400)) for n in (0, 1))
    store.set_meta("usage_hourly_from", today)  # as after an upgrade with the request log off
    store.record_requests([finished("old", now - 86_400)])
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        two_days = f"range=custom&from={yesterday}&to={today}"  # 48 hours: per hour, but for the whole day
        body = (await c.get(f"/admin/usage/body?{two_days}")).text
        assert f"Usage before {today} is kept per day, so those days count whole." in body
        assert "Tokens per day" in body and ">1<" in body
        body = (await c.get(f"/admin/usage/body?range=custom&from={today}&to={today}")).text
        assert "kept per day" not in body and "Tokens per hour" in body
