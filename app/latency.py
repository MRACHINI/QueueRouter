"""Response times per model and upstream, from the request log: the Response times page and the slow-first-token
alert. Timings are the provider's own (from when QueueRouter sent the request), streamed requests only; see
``RequestLog`` in app/main.py for what each one measures."""

from __future__ import annotations

import math
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

WINDOWS = {"1h": 3_600, "24h": 86_400, "7d": 7 * 86_400}
BUCKETS = {"1h": 300, "24h": 3_600, "7d": 6 * 3_600}  # one chart point per this many seconds
TREND_LINES = 5  # the busiest model/upstream pairs get a line on the chart
ALERT_MIN_REQUESTS = 5


def percentile(values: list[float], p: float) -> float | None:
    """Nearest-rank percentile (p in 0..100) of unsorted values; None for none."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]


@dataclass
class Summary:
    model: str
    upstream: str
    requests: int  # finished requests (any outcome)
    timed: int  # of them streamed and timed
    headers_med: float | None  # ms in the provider's own queue
    first_med: float | None  # ms to the first token
    first_p95: float | None
    thinking_med: float | None  # ms from the first token to the answer, where the model thought first
    speed_med: float | None  # tokens/s
    speed_p5: float | None  # the slowest 5%
    stall_p95: float | None  # ms, longest pause between tokens
    retried: int  # requests that waited to retry
    backoff_s: float  # total seconds waited before retries


def _value(row: Mapping, key: str) -> float | None:
    value = row[key]
    return float(value) if value is not None else None


def summarize(rows: Iterable[Mapping]) -> list[Summary]:
    """One line per (model, upstream), the busiest first. ``rows`` from ``Store.response_rows``."""
    groups: dict[tuple[str, str], list[Mapping]] = {}
    for row in rows:
        groups.setdefault((row["model"] or "—", row["upstream"] or "—"), []).append(row)
    out = []
    for (model, upstream), items in groups.items():
        timed = [r for r in items if r["outcome"] == "done" and r["first_token_ms"] is not None]

        def values(key: str, rows: list[Mapping] = timed) -> list[float]:
            return [v for r in rows if (v := _value(r, key)) is not None]

        thinking = [
            r["answer_ms"] - r["first_token_ms"]
            for r in timed
            if r["answer_ms"] is not None and r["answer_ms"] > r["first_token_ms"]
        ]
        backoffs = values("backoff_ms", items)
        out.append(
            Summary(
                model=model,
                upstream=upstream,
                requests=len(items),
                timed=len(timed),
                headers_med=percentile(values("headers_ms"), 50),
                first_med=percentile(values("first_token_ms"), 50),
                first_p95=percentile(values("first_token_ms"), 95),
                thinking_med=percentile(thinking, 50) if len(thinking) * 2 >= len(timed) else None,
                speed_med=percentile(values("tokens_per_s"), 50),
                speed_p5=percentile(values("tokens_per_s"), 5),
                stall_p95=percentile(values("stall_ms"), 95),
                retried=sum(1 for v in backoffs if v > 0),
                backoff_s=sum(backoffs) / 1000,
            )
        )
    return sorted(out, key=lambda s: (-s.requests, s.model, s.upstream))


def bucket_starts(window: str, now: float | None = None) -> list[float]:
    """The chart's points: bucket start times (UTC epoch), oldest first, the current bucket last."""
    step = BUCKETS[window]
    now = now or time.time()
    last = now - now % step
    return [last - step * i for i in range(WINDOWS[window] // step - 1, -1, -1)]


def trend(rows: Iterable[Mapping], window: str, now: float | None = None) -> tuple[list[float], list[tuple[str, list]]]:
    """(bucket starts, [(model @ upstream, median first-token seconds per bucket or None)]) for the busiest pairs."""
    starts = bucket_starts(window, now)
    step = BUCKETS[window]
    per_pair: dict[str, list[list[float]]] = {}
    counts: dict[str, int] = {}
    for row in rows:
        if row["outcome"] != "done" or row["first_token_ms"] is None:
            continue
        i = int((row["ts"] - starts[0]) // step)
        if not 0 <= i < len(starts):
            continue
        name = f"{row['model']} @ {row['upstream']}"
        per_pair.setdefault(name, [[] for _ in starts])[i].append(row["first_token_ms"] / 1000)
        counts[name] = counts.get(name, 0) + 1
    busiest = sorted(counts, key=lambda n: (-counts[n], n))[:TREND_LINES]
    return starts, [(name, [percentile(b, 50) for b in per_pair[name]]) for name in busiest]


def slow_pairs(rows: Iterable[Mapping], limit_s: float) -> list[tuple[Summary, float]]:
    """(summary, worst-5% first token in seconds) where that passes ``limit_s``, with enough requests to say so."""
    return [
        (s, s.first_p95 / 1000)
        for s in summarize(rows)
        if s.timed >= ALERT_MIN_REQUESTS and s.first_p95 is not None and s.first_p95 / 1000 > limit_s
    ]
