"""A small cron expression parser (UTC): ``minute hour day-of-month month day-of-week``.

Supports ``*``, lists (``1,15``), ranges (``1-5``), steps (``*/15``, ``10-50/10``), names
(``jan``…, ``mon``…), and ``@hourly`` / ``@daily`` / ``@weekly`` / ``@monthly`` / ``@yearly``.
As in classic cron, when both day fields are restricted a day matching either one runs.
"""

from __future__ import annotations

import calendar
import time
from dataclasses import dataclass

ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
DAYS = {d.lower(): (i + 1) % 7 for i, d in enumerate(calendar.day_abbr)}  # mon=1 … sun=0
FIELDS = (
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day", 1, 31, {}),
    ("month", 1, 12, MONTHS),
    ("weekday", 0, 7, DAYS),
)


class CronError(ValueError):
    pass


def _field(text: str, name: str, low: int, high: int, names: dict[str, int]) -> frozenset[int]:
    values: set[int] = set()
    for part in text.lower().split(","):
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise CronError(f"bad step in {name}: {text!r}")
            step = int(step_text)
        if part == "*":
            start, end = low, high
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = _value(a, name, names), _value(b, name, names)
        else:
            start = _value(part, name, names)
            end = high if step > 1 else start
        if not (low <= start <= high and low <= end <= high) or start > end:
            raise CronError(f"{name} out of range ({low}-{high}): {text!r}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


def _value(text: str, name: str, names: dict[str, int]) -> int:
    if text in names:
        return names[text]
    if not text.isdigit():
        raise CronError(f"bad {name}: {text!r}")
    return int(text)


@dataclass(frozen=True)
class Cron:
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]
    any_day: bool
    any_weekday: bool

    def _day_matches(self, year: int, month: int, day: int) -> bool:
        weekday = (calendar.weekday(year, month, day) + 1) % 7  # sunday = 0
        in_days, in_weekdays = day in self.days, weekday in self.weekdays
        if self.any_day and self.any_weekday:
            return True
        if self.any_day:
            return in_weekdays
        if self.any_weekday:
            return in_days
        return in_days or in_weekdays

    def next_after(self, ts: float) -> float:
        """The first run strictly after ``ts`` (epoch seconds, UTC)."""
        t = int(ts // 60 * 60 + 60)
        limit = t + 5 * 366 * 86_400
        while t < limit:
            g = time.gmtime(t)
            if g.tm_mon not in self.months:
                year, month = (g.tm_year + 1, 1) if g.tm_mon == 12 else (g.tm_year, g.tm_mon + 1)
                t = calendar.timegm((year, month, 1, 0, 0, 0))
            elif not self._day_matches(g.tm_year, g.tm_mon, g.tm_mday):
                t = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, 0, 0, 0)) + 86_400
            elif g.tm_hour not in self.hours:
                t = calendar.timegm((g.tm_year, g.tm_mon, g.tm_mday, g.tm_hour, 0, 0)) + 3600
            elif g.tm_min not in self.minutes:
                t += 60
            else:
                return float(t)
        raise CronError("this schedule never runs")


def parse(expression: str) -> Cron:
    text = ALIASES.get(expression.strip().lower(), expression.strip())
    parts = text.split()
    if len(parts) != 5:
        raise CronError("a cron expression has 5 fields: minute hour day month weekday (or @daily...)")
    sets = [_field(p, *spec) for p, spec in zip(parts, FIELDS, strict=True)]
    weekdays = frozenset(0 if d == 7 else d for d in sets[4])
    cron = Cron(sets[0], sets[1], sets[2], sets[3], weekdays, parts[2] == "*", parts[4] == "*")
    cron.next_after(time.time())  # never-running schedules (e.g. 31 feb) are refused up front
    return cron
