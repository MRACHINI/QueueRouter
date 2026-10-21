"""Monthly quotas per client: requests, tokens and cost (at official prices) per calendar month (UTC).

The month's totals are read from the daily usage rollup every minute; requests finishing in
between are added in memory, so a quota holds without a database read per request.
"""

from __future__ import annotations

import calendar
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field


def month_of(ts: float) -> str:
    return time.strftime("%Y-%m", time.gmtime(ts))


def seconds_to_next_month(now: float) -> int:
    t = time.gmtime(now)
    days = calendar.monthrange(t.tm_year, t.tm_mon)[1]
    start_next = calendar.timegm((t.tm_year, t.tm_mon, days, 0, 0, 0)) + 86_400
    return max(1, int(start_next - now))


@dataclass
class Used:
    requests: int = 0
    tokens: int = 0
    cost: float = 0.0

    def add(self, requests: int, tokens: int, cost: float) -> None:
        self.requests += requests
        self.tokens += tokens
        self.cost += cost


@dataclass
class MonthlyUsage:
    cost_of: Callable[[str, int, int, int], float]  # (model, prompt, completion, cached) -> USD
    month: str = ""
    base: dict[str, Used] = field(default_factory=dict)
    extra: dict[str, Used] = field(default_factory=dict)

    def refresh(self, rows: Iterable, now: float | None = None) -> None:
        """``rows``: this month's usage_daily rows (client, model, requests, prompt/completion/cached tokens)."""
        self.month = month_of(now or time.time())
        base: dict[str, Used] = {}
        for r in rows:
            prompt, completion, cached = r["prompt_tokens"] or 0, r["completion_tokens"] or 0, r["cached_tokens"] or 0
            base.setdefault(r["client"], Used()).add(
                r["requests"] or 0, prompt + completion, self.cost_of(r["model"], prompt, completion, cached)
            )
        self.base, self.extra = base, {}

    def observe(self, entry: dict) -> None:
        """A finished request (counted until the next refresh reads it from the database)."""
        if entry.get("outcome") == "refused":  # turned away at the door: not usage
            return
        if month_of(entry.get("ts") or time.time()) != self.month:
            return
        prompt, completion = entry.get("prompt_tokens") or 0, entry.get("completion_tokens") or 0
        model = entry.get("target_model") or entry.get("model") or ""
        cost = self.cost_of(model, prompt, completion, entry.get("cached_tokens") or 0)
        self.extra.setdefault(entry.get("client") or "default", Used()).add(1, prompt + completion, cost)

    def used(self, client: str) -> Used:
        base, extra = self.base.get(client, Used()), self.extra.get(client, Used())
        return Used(base.requests + extra.requests, base.tokens + extra.tokens, base.cost + extra.cost)


def over_quota(policy, used: Used) -> str | None:
    """Which monthly quota is used up, if any."""
    if policy.month_requests is not None and used.requests >= policy.month_requests:
        return f"monthly request quota ({policy.month_requests:,}) reached"
    if policy.month_tokens is not None and used.tokens >= policy.month_tokens:
        return f"monthly token quota ({policy.month_tokens:,}) reached"
    if policy.month_cost_usd is not None and used.cost >= policy.month_cost_usd:
        return f"monthly cost quota (${policy.month_cost_usd:,.2f} at official prices) reached"
    return None


def quota_share(policy, used: Used) -> float | None:
    """Highest share of any monthly quota used (1.0 = all), or None without quotas."""
    shares = [
        used.requests / policy.month_requests if policy.month_requests else None,
        used.tokens / policy.month_tokens if policy.month_tokens else None,
        used.cost / policy.month_cost_usd if policy.month_cost_usd else None,
    ]
    known = [s for s in shares if s is not None]
    return max(known) if known else None
