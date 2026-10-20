"""In-memory counters and rolling latency stats for /stats."""

from __future__ import annotations

import time
from collections import Counter, deque

BUCKETS_S = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600)  # Prometheus histogram upper bounds


class Histogram:
    def __init__(self) -> None:
        self.counts = [0] * (len(BUCKETS_S) + 1)  # last = +Inf
        self.total = 0.0
        self.n = 0

    def observe(self, seconds: float) -> None:
        self.n += 1
        self.total += seconds
        for i, bound in enumerate(BUCKETS_S):
            if seconds <= bound:
                self.counts[i] += 1
                return
        self.counts[-1] += 1


def _summary(values: deque[float]) -> dict[str, int]:
    if not values:
        return {"avg": 0, "p95": 0, "max": 0}
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
    return {"avg": round(sum(ordered) / len(ordered)), "p95": round(p95), "max": round(ordered[-1])}


class ClientStats:
    def __init__(self) -> None:
        self.outcomes: Counter[str] = Counter()
        self.prompt_tokens = 0
        self.completion_tokens = 0

    def snapshot(self) -> dict:
        return {
            "outcomes": dict(self.outcomes),
            "tokens": {"prompt": self.prompt_tokens, "completion": self.completion_tokens},
        }


class Stats:
    def __init__(self, window: int = 1000) -> None:
        self.started = time.time()
        self.outcomes: Counter[str] = Counter()
        self.retries = 0
        self.wait_ms: deque[float] = deque(maxlen=window)
        self.upstream_ms: deque[float] = deque(maxlen=window)
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.clients: dict[str, ClientStats] = {}
        self.recent: deque[dict] = deque(maxlen=50)  # last request log entries, for the dashboard
        self.sink = None  # optional callable(entry) receiving every finished request (usage log)
        self.content_sink = None  # optional callable(request id, client, request bytes, reply bytes)
        self.queue_hist = Histogram()  # seconds waiting for a slot
        self.upstream_hist = Histogram()  # seconds from slot to last byte (completed requests)
        self.ttft_hist = Histogram()  # seconds from arrival to the first byte of the answer

    def record(
        self,
        outcome: str,
        wait_ms: float | None,
        upstream_ms: float | None,
        retries: int,
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
        client: str = "default",
    ) -> None:
        self.outcomes[outcome] += 1
        self.retries += retries
        if wait_ms is not None:
            self.wait_ms.append(wait_ms)
            self.queue_hist.observe(wait_ms / 1000)
        if upstream_ms is not None and outcome == "done":
            self.upstream_ms.append(upstream_ms)
            self.upstream_hist.observe(upstream_ms / 1000)
        self.prompt_tokens += prompt_tokens or 0
        self.completion_tokens += completion_tokens or 0
        per_client = self.clients.setdefault(client, ClientStats())
        per_client.outcomes[outcome] += 1
        per_client.prompt_tokens += prompt_tokens or 0
        per_client.completion_tokens += completion_tokens or 0

    def avg_upstream_ms(self, recent: int = 50) -> float:
        """Average duration of recent completed upstream calls (for wait estimates)."""
        values = list(self.upstream_ms)[-recent:]
        return sum(values) / len(values) if values else 0.0

    def snapshot(self) -> dict:
        return {
            "outcomes": dict(self.outcomes),
            "retries": self.retries,
            "wait_ms": _summary(self.wait_ms),
            "upstream_ms": _summary(self.upstream_ms),
            "tokens": {"prompt": self.prompt_tokens, "completion": self.completion_tokens},
            "per_client": {name: c.snapshot() for name, c in self.clients.items()},
            "uptime_s": round(time.time() - self.started),
        }
