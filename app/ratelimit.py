"""Per-client rolling-window rate limits: requests and tokens, per minute and per day.

A request counts when it is dispatched (gets a slot), not while it waits. Tokens count when a
response finishes (that's when usage is known), so token limits gate the *next* requests.
"""

from __future__ import annotations

import time
from collections import deque

from .config import ClientPolicy

MINUTE, DAY = 60.0, 86_400.0


class RateLimiter:
    def __init__(self) -> None:
        self._requests: dict[str, deque[float]] = {}
        self._tokens: dict[str, deque[tuple[float, int]]] = {}

    def record_request(self, client: str, now: float | None = None) -> None:
        self._requests.setdefault(client, deque()).append(now if now is not None else time.time())

    def record_tokens(self, client: str, tokens: int, now: float | None = None) -> None:
        if tokens > 0:
            self._tokens.setdefault(client, deque()).append((now if now is not None else time.time(), tokens))

    def seed(self, client: str, requests: list[float], tokens: list[tuple[float, int]]) -> None:
        """Restore the last 24 h after a restart (from the usage log)."""
        self._requests[client] = deque(sorted(requests))
        self._tokens[client] = deque(sorted(tokens))

    def wait_s(self, client: str, policy: ClientPolicy, now: float | None = None) -> float:
        """Seconds until ``client`` may start another request (0 = now)."""
        if not policy.rate_limited:
            return 0.0
        now = now if now is not None else time.time()
        self._prune(client, now)
        requests = self._requests.get(client, deque())
        tokens = self._tokens.get(client, deque())
        wait = 0.0
        for limit, window in ((policy.rpm, MINUTE), (policy.rpd, DAY)):
            if limit is not None:
                in_window = [t for t in requests if t > now - window]
                if len(in_window) >= limit:
                    # the oldest request that must expire before we're under the limit again
                    wait = max(wait, in_window[len(in_window) - limit] + window - now)
        for limit, window in ((policy.tpm, MINUTE), (policy.tpd, DAY)):
            if limit is not None:
                in_window = [(t, n) for t, n in tokens if t > now - window]
                used = sum(n for _, n in in_window)
                for t, n in in_window:  # expire oldest usage until we're under the limit
                    if used < limit:
                        break
                    used -= n
                    wait = max(wait, t + window - now)
        return max(wait, 0.0)

    def usage(self, client: str, now: float | None = None) -> dict[str, int]:
        now = now if now is not None else time.time()
        self._prune(client, now)
        requests = self._requests.get(client, deque())
        tokens = self._tokens.get(client, deque())
        return {
            "requests_minute": sum(1 for t in requests if t > now - MINUTE),
            "requests_day": len(requests),
            "tokens_minute": sum(n for t, n in tokens if t > now - MINUTE),
            "tokens_day": sum(n for _, n in tokens),
        }

    def _prune(self, client: str, now: float) -> None:
        requests = self._requests.get(client)
        while requests and requests[0] <= now - DAY:
            requests.popleft()
        tokens = self._tokens.get(client)
        while tokens and tokens[0][0] <= now - DAY:
            tokens.popleft()
