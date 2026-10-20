"""Prometheus metrics (text exposition format 0.0.4), written by hand: no client library needed.

Counters and histograms count since QueueRouter started (Prometheus handles the resets).
"""

from __future__ import annotations

from .stats import BUCKETS_S, Histogram, Stats

CIRCUIT_STATES = ("closed", "half-open", "open")


def _escape(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(labels: dict[str, object]) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items()) + "}"


class Writer:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def family(self, name: str, kind: str, help_text: str) -> None:
        self.lines.append(f"# HELP {name} {help_text}")
        self.lines.append(f"# TYPE {name} {kind}")

    def sample(self, name: str, value: float, **labels: object) -> None:
        number = int(value) if float(value).is_integer() else value
        self.lines.append(f"{name}{_labels(labels)} {number}")

    def histogram(self, name: str, help_text: str, hist: Histogram) -> None:
        self.family(name, "histogram", help_text)
        cumulative = 0
        for bound, count in zip(BUCKETS_S, hist.counts, strict=False):
            cumulative += count
            self.sample(f"{name}_bucket", cumulative, le=f"{bound:g}")
        self.sample(f"{name}_bucket", hist.n, le="+Inf")
        self.sample(f"{name}_sum", round(hist.total, 3))
        self.sample(f"{name}_count", hist.n)

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


def render(stats: Stats, live: dict, jobs: dict[str, int], version: str) -> str:
    """``live`` is the dashboard state (queue, pools, upstreams, mode); ``jobs`` = count by status."""
    w = Writer()
    w.family("queuerouter_build_info", "gauge", "QueueRouter version.")
    w.sample("queuerouter_build_info", 1, version=version)
    w.family("queuerouter_uptime_seconds", "gauge", "Seconds since start.")
    w.sample("queuerouter_uptime_seconds", live.get("uptime_s", 0))
    w.family("queuerouter_mode", "gauge", "Operating mode (1 = current).")
    for mode in live.get("modes", ()):
        w.sample("queuerouter_mode", int(mode == live.get("mode")), mode=mode)

    w.family("queuerouter_requests_active", "gauge", "Requests holding an upstream slot.")
    w.sample("queuerouter_requests_active", live.get("active", 0))
    w.family("queuerouter_requests_queued", "gauge", "Requests waiting for a slot, by priority lane.")
    for lane, n in (live.get("queued_by_lane") or {}).items():
        w.sample("queuerouter_requests_queued", n, lane=lane)
    w.family("queuerouter_oldest_wait_seconds", "gauge", "How long the oldest queued request has waited.")
    w.sample("queuerouter_oldest_wait_seconds", live.get("oldest_wait_s", 0))
    w.family("queuerouter_slots_capacity", "gauge", "Parallel requests allowed, in total.")
    w.sample("queuerouter_slots_capacity", live.get("max_concurrency", 0))

    pools = live.get("pools") or {}
    w.family("queuerouter_pool_active", "gauge", "Requests in flight per limit pool.")
    for name, pool in pools.items():
        w.sample("queuerouter_pool_active", pool.get("active", 0), pool=name)
    w.family("queuerouter_pool_capacity", "gauge", "Parallel requests allowed per limit pool.")
    for name, pool in pools.items():
        w.sample("queuerouter_pool_capacity", pool.get("capacity", 0), pool=name)

    upstreams = live.get("upstreams") or []
    w.family("queuerouter_upstream_active", "gauge", "Requests in flight per upstream.")
    for u in upstreams:
        w.sample("queuerouter_upstream_active", u.get("active", 0), upstream=u["name"], pool=u["pool"])
    w.family("queuerouter_upstream_circuit", "gauge", "Circuit breaker state per upstream (1 = current).")
    for u in upstreams:
        for state in CIRCUIT_STATES:
            w.sample("queuerouter_upstream_circuit", int(u.get("state") == state), upstream=u["name"], state=state)
    w.family("queuerouter_upstream_usable", "gauge", "1 if the upstream is enabled and not paused.")
    for u in upstreams:
        w.sample("queuerouter_upstream_usable", int(bool(u.get("usable"))), upstream=u["name"])

    w.family("queuerouter_requests_total", "counter", "Finished requests by client and outcome.")
    for client, c in sorted(stats.clients.items()):
        for outcome, n in sorted(c.outcomes.items()):
            w.sample("queuerouter_requests_total", n, client=client, outcome=outcome)
    w.family("queuerouter_tokens_total", "counter", "Tokens by client and kind.")
    for client, c in sorted(stats.clients.items()):
        w.sample("queuerouter_tokens_total", c.prompt_tokens, client=client, kind="prompt")
        w.sample("queuerouter_tokens_total", c.completion_tokens, client=client, kind="completion")
    w.family("queuerouter_retries_total", "counter", "Upstream retries.")
    w.sample("queuerouter_retries_total", stats.retries)
    w.histogram("queuerouter_queue_wait_seconds", "Time waiting for a slot.", stats.queue_hist)
    w.histogram("queuerouter_upstream_seconds", "Time from getting a slot to the last byte.", stats.upstream_hist)
    w.histogram(
        "queuerouter_time_to_first_byte_seconds", "Time from arrival to the first byte of the answer.", stats.ttft_hist
    )

    w.family("queuerouter_jobs", "gauge", "Async jobs by status (kept ones).")
    for status, n in sorted(jobs.items()):
        w.sample("queuerouter_jobs", n, status=status)
    w.family("queuerouter_alerts_unread", "gauge", "Unread notifications in the admin.")
    w.sample("queuerouter_alerts_unread", live.get("alerts_unread", 0))
    return w.text()
