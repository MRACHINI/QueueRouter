"""Per-upstream circuit breakers: stop sending traffic to an upstream that keeps failing.

closed     normal traffic
open       after ``threshold`` consecutive failures (5xx, connection errors, timeouts); no
           traffic for ``cooldown_s``. 429 is "busy", not a failure.
half-open  after the cooldown, exactly one trial request (or health probe) is let through:
           success closes the circuit, failure opens it again.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class _State:
    failures: int = 0
    opened_at: float | None = None
    trial_in_flight: bool = False
    last_error: str = ""


class Circuits:
    def __init__(self, threshold: int = 5, cooldown_s: float = 60) -> None:
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self._states: dict[int, _State] = {}
        self.on_change = None  # optional callback(upstream_id, new_state, reason)

    def _get(self, upstream_id: int) -> _State:
        return self._states.setdefault(upstream_id, _State())

    def state(self, upstream_id: int) -> str:
        s = self._get(upstream_id)
        if s.opened_at is None:
            return "closed"
        if time.monotonic() - s.opened_at >= self.cooldown_s:
            return "half-open"
        return "open"

    def allows(self, upstream_id: int) -> bool:
        """Whether a request may be sent now (pure check, doesn't reserve the trial)."""
        current = self.state(upstream_id)
        if current == "closed":
            return True
        return current == "half-open" and not self._get(upstream_id).trial_in_flight

    def begin(self, upstream_id: int) -> None:
        """A request was dispatched; in half-open state it becomes the single trial."""
        if self.state(upstream_id) == "half-open":
            self._get(upstream_id).trial_in_flight = True

    def success(self, upstream_id: int) -> None:
        s = self._get(upstream_id)
        was_open = s.opened_at is not None
        s.failures, s.opened_at, s.trial_in_flight, s.last_error = 0, None, False, ""
        if was_open:
            self._notify(upstream_id, "closed", "recovered")

    def failure(self, upstream_id: int, reason: str) -> None:
        s = self._get(upstream_id)
        s.last_error = reason
        if s.opened_at is not None:  # failed trial (or a straggler) -> open again
            s.opened_at, s.trial_in_flight = time.monotonic(), False
            self._notify(upstream_id, "open", reason)
            return
        s.failures += 1
        if s.failures >= self.threshold:
            s.opened_at = time.monotonic()
            self._notify(upstream_id, "open", reason)

    def abandon(self, upstream_id: int) -> None:
        """A trial ended without a verdict (e.g. the client disconnected)."""
        self._get(upstream_id).trial_in_flight = False

    def describe(self, upstream_id: int) -> dict:
        s = self._get(upstream_id)
        return {"state": self.state(upstream_id), "failures": s.failures, "last_error": s.last_error}

    def _notify(self, upstream_id: int, state: str, reason: str) -> None:
        if self.on_change:
            self.on_change(upstream_id, state, reason)
