"""The Usage page's period: the last 24 hours, today, a week, 30 days, or days you pick, all UTC and to the hour."""

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

HOUR = 3600
DAY = 86_400
CHOICES = {"24h": "Last 24 hours", "1d": "Today", "7d": "1 week", "30d": "30 days", "custom": "Custom"}
ROLLING = {"24h": 1, "7d": 7, "30d": 30}  # days, ending with the current hour
CUSTOM_DAYS = 7  # what Custom starts with: the last 7 days, today included
MOST_DAYS = 366  # the longest custom period
HOURLY_UP_TO = 48  # hours: a period this short is charted per hour


@dataclass(frozen=True)
class Period:
    choice: str
    start: int  # UTC seconds, on the hour
    end: int  # not included; on the hour
    first: str  # the custom fields, as the form shows them: days and hours
    first_hour: int
    last: str
    last_hour: int

    @property
    def hours(self) -> int:
        return (self.end - self.start) // HOUR

    def elapsed_hours(self, now: float) -> float:
        """Hours of the period that have passed (what a monthly plan has cost for it)."""
        return max(min(self.end, now) - self.start, 0) / HOUR

    def hour_slots(self) -> list[str]:
        return [stamp(t, "%Y-%m-%dT%H") for t in range(self.start, self.end, HOUR)]

    def day_slots(self) -> list[str]:
        first, last = (datetime.fromtimestamp(t, UTC).date() for t in (self.start, self.end - 1))
        return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]

    def text(self, now: float) -> str:
        start = stamp(self.start, "%Y-%m-%d %H:%M")
        if self.end > now:
            return f"Since {start} UTC"
        return f"From {start} to {stamp(self.end, '%Y-%m-%d %H:%M')} UTC"


def stamp(t: float, fmt: str) -> str:
    return time.strftime(fmt, time.gmtime(t))


def _day(text: str, fallback: date) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        return fallback


def _hour(text: str, fallback: int, least: int, most: int) -> int:
    try:
        return min(max(int(text), least), most)
    except ValueError:
        return fallback


def _midnight(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


def resolve(
    choice: str, first: str = "", first_hour: str = "", last: str = "", last_hour: str = "", now: float | None = None
) -> Period:
    """The period chosen (the last 24 hours when unknown). Custom runs from ``first`` at ``first_hour`` to ``last``
    at ``last_hour`` (24 = the day's end, so whole days count both); dates that don't parse fall back to the last 7
    days, a start after the end swaps them, and a period over a year keeps its last year."""
    now = time.time() if now is None else now
    hour = int(now // HOUR) * HOUR
    today = datetime.fromtimestamp(now, UTC).date()
    first_day = _day(first, today - timedelta(days=CUSTOM_DAYS - 1))
    last_day = _day(last, today)
    from_hour = _hour(first_hour, 0, 0, 23)
    to_hour = _hour(last_hour, 24, 1, 24)
    if choice == "custom":
        start, end = _midnight(first_day) + from_hour * HOUR, _midnight(last_day) + to_hour * HOUR
        if start > end:
            start, end = end, start
        end = max(end, start + HOUR)
        start = max(start, end - MOST_DAYS * DAY)
    elif choice == "1d":
        start = _midnight(today)
        end = start + DAY
    else:
        choice = choice if choice in ROLLING else "24h"
        end = hour + HOUR
        start = end - ROLLING[choice] * DAY
    return Period(choice, start, end, first_day.isoformat(), from_hour, last_day.isoformat(), to_hour)
