"""Wait queue in front of a fixed number of upstream slots.

Ordering when a slot frees up (lowest wins):
  1. effective lane   - high / normal / low, bumped one lane up per ``aging_s`` waited, so
                        nothing starves;
  2. session activity - requests from a session that is mid-task (a request in flight or
                        finished within ``session_window_s``) go before brand-new sessions:
                        agents finish what they started instead of everyone crawling;
  3. fair share       - the client granted least recently goes first;
  4. arrival order    - FIFO.

The hand-off is direct (a released slot goes straight to the chosen waiter) and safe against
the grant/cancel race. Everything runs on one event loop, so no locks are needed.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field

LANES = {"high": 0, "normal": 1, "low": 2}
LANE_NAMES = {v: k for k, v in LANES.items()}

_ids = itertools.count(1)


@dataclass(eq=False)
class Ticket:
    request_id: str
    future: asyncio.Future[None]
    client: str = "default"
    lane: int = LANES["normal"]
    session: str | None = None
    enqueued_at: float = field(default_factory=time.monotonic)
    granted_at: float | None = None
    released: bool = False
    seq: int = field(default_factory=lambda: next(_ids))

    @property
    def granted(self) -> bool:
        return self.future.done() and not self.future.cancelled()

    @property
    def wait_s(self) -> float:
        end = self.granted_at if self.granted_at is not None else time.monotonic()
        return end - self.enqueued_at


class SlotQueue:
    def __init__(
        self,
        max_concurrency: int,
        min_spacing_ms: int = 0,
        aging_s: float = 120,
        session_window_s: float = 60,
        session_aware: bool = True,
        fair_share: bool = True,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.min_spacing_s = min_spacing_ms / 1000
        self.aging_s = aging_s
        self.session_window_s = session_window_s
        self.session_aware = session_aware
        self.fair_share = fair_share

        self.active = 0
        self._waiters: list[Ticket] = []
        self._running: set[Ticket] = set()
        self._session_inflight: dict[str, int] = {}
        self._session_last_done: dict[str, float] = {}
        self._client_last_grant: dict[str, float] = {}
        self._last_start = 0.0
        self._spacing_lock = asyncio.Lock()

    # -- ordering ------------------------------------------------------------------------

    def _session_active(self, session: str | None, now: float) -> bool:
        if not self.session_aware or session is None:
            return False
        if self._session_inflight.get(session):
            return True
        last = self._session_last_done.get(session)
        return last is not None and now - last <= self.session_window_s

    def _rank(self, client: str, lane: int, session: str | None, waited: float, seq: int, now: float) -> tuple:
        bump = int(waited // self.aging_s) if self.aging_s > 0 else 0
        effective_lane = max(0, lane - bump)
        new_session = 0 if self._session_active(session, now) else 1
        last_grant = self._client_last_grant.get(client, 0.0) if self.fair_share else 0.0
        return (effective_lane, new_session, last_grant, seq)

    def _ticket_rank(self, t: Ticket, now: float) -> tuple:
        return self._rank(t.client, t.lane, t.session, now - t.enqueued_at, t.seq, now)

    def _ordered(self) -> list[Ticket]:
        now = time.monotonic()
        return sorted(self._waiters, key=lambda t: self._ticket_rank(t, now))

    # -- introspection -------------------------------------------------------------------

    @property
    def queued(self) -> int:
        return len(self._waiters)

    def position(self, ticket: Ticket) -> int:
        """1-based position in the current dispatch order, or 0 if not waiting."""
        for i, t in enumerate(self._ordered(), 1):
            if t is ticket:
                return i
        return 0

    def predict_position(self, client: str, lane: int, session: str | None) -> int:
        """Where a request would land if it arrived now (0 = it would start immediately)."""
        now = time.monotonic()
        rank = self._rank(client, lane, session, 0.0, next(_ids), now)
        ahead = sum(1 for t in self._waiters if self._ticket_rank(t, now) < rank)
        if ahead == 0 and self.active < self.max_concurrency:
            return 0
        return ahead + 1

    def oldest_wait_s(self) -> float:
        return max((t.wait_s for t in self._waiters), default=0.0)

    def snapshot(self) -> dict:
        by_lane = {name: 0 for name in LANES}
        by_client: dict[str, dict[str, int]] = {}
        for t in self._waiters:
            by_lane[LANE_NAMES[t.lane]] += 1
            by_client.setdefault(t.client, {"queued": 0, "active": 0})["queued"] += 1
        for t in self._running:
            by_client.setdefault(t.client, {"queued": 0, "active": 0})["active"] += 1
        return {"queued_by_lane": by_lane, "clients": by_client}

    # -- core operations -----------------------------------------------------------------

    def enqueue(
        self, request_id: str, client: str = "default", lane: int = LANES["normal"], session: str | None = None
    ) -> Ticket:
        """Create a ticket; it is granted immediately if it is first in line and a slot is free."""
        ticket = Ticket(request_id, asyncio.get_running_loop().create_future(), client, lane, session)
        self._waiters.append(ticket)
        self._dispatch()
        return ticket

    async def wait(self, ticket: Ticket, timeout: float | None = None) -> bool:
        """Wait up to ``timeout`` seconds for the grant. Returns whether it was granted.

        Does not cancel the ticket on timeout, so callers can wait in steps (e.g. to send
        keep-alives). Callers must always finish with ``cancel()`` in a ``finally`` block.
        """
        if not ticket.future.done():
            await asyncio.wait({ticket.future}, timeout=timeout)
        return ticket.granted

    async def apply_spacing(self) -> None:
        """Enforce MIN_SPACING_MS between consecutive upstream starts."""
        if self.min_spacing_s <= 0:
            return
        async with self._spacing_lock:
            delay = self._last_start + self.min_spacing_s - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_start = time.monotonic()

    def cancel(self, ticket: Ticket) -> None:
        """Finish with a ticket: leaves the queue if waiting, releases the slot if granted.

        Idempotent, so it is safe to call unconditionally in ``finally``.
        """
        try:
            self._waiters.remove(ticket)
        except ValueError:
            pass
        if not ticket.future.done():
            ticket.future.cancel()
        elif ticket.granted:
            self.release(ticket)

    def release(self, ticket: Ticket) -> None:
        """Return a granted slot and hand it straight to the next waiter. Idempotent."""
        if ticket.released or not ticket.granted:
            return
        ticket.released = True
        self.active -= 1
        self._running.discard(ticket)
        if ticket.session is not None:
            now = time.monotonic()
            remaining = self._session_inflight.get(ticket.session, 1) - 1
            if remaining > 0:
                self._session_inflight[ticket.session] = remaining
            else:
                self._session_inflight.pop(ticket.session, None)
            self._session_last_done[ticket.session] = now
            self._prune(now)
        self._dispatch()

    # -- internals -----------------------------------------------------------------------

    def _grant(self, ticket: Ticket) -> None:
        now = time.monotonic()
        self.active += 1
        self._running.add(ticket)
        ticket.granted_at = now
        self._client_last_grant[ticket.client] = now
        if ticket.session is not None:
            self._session_inflight[ticket.session] = self._session_inflight.get(ticket.session, 0) + 1
        ticket.future.set_result(None)

    def _dispatch(self) -> None:
        while self.active < self.max_concurrency and self._waiters:
            ticket = self._ordered()[0]
            self._waiters.remove(ticket)
            if not ticket.future.done():
                self._grant(ticket)

    def _prune(self, now: float) -> None:
        if len(self._session_last_done) > 10_000:
            cutoff = now - self.session_window_s
            self._session_last_done = {s: t for s, t in self._session_last_done.items() if t >= cutoff}
