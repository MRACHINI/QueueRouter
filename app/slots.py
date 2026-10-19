"""Wait queue in front of the upstreams' concurrency slots.

Slots belong to **limit pools** (one per provider account): several upstream entries can share
one pool, and a request may be served by any of its *candidate* upstreams.

Ordering when a slot frees up (lowest wins):
  1. effective lane   - high / normal / low, bumped one lane up per ``aging_s`` waited, so
                        nothing starves;
  2. session activity - requests from a session that is mid-task (a request in flight or
                        finished within ``session_window_s``) go before brand-new sessions:
                        agents finish what they started instead of everyone crawling;
  3. fair share       - the client granted least recently goes first;
  4. arrival order    - FIFO.

A waiter whose candidates are all busy doesn't block a later waiter that another pool can
serve. The hand-off is direct (a released slot goes straight to the chosen waiter) and safe
against the grant/cancel race. Everything runs on one event loop, so no locks are needed.
"""

from __future__ import annotations

import asyncio
import itertools
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field

LANES = {"high": 0, "normal": 1, "low": 2}
LANE_NAMES = {v: k for k, v in LANES.items()}
STRATEGIES = ("least-busy", "weighted", "ordered")

_ids = itertools.count(1)


@dataclass(frozen=True)
class Candidate:
    """An upstream that can serve a request, and the pool whose slot it uses."""

    upstream_id: int
    pool_id: int
    weight: int = 1
    order: int = 0


DEFAULT_CANDIDATES = (Candidate(0, 0),)


@dataclass(eq=False)
class Ticket:
    request_id: str
    future: asyncio.Future[None]
    client: str = "default"
    lane: int = LANES["normal"]
    session: str | None = None
    candidates: tuple[Candidate, ...] = DEFAULT_CANDIDATES
    enqueued_at: float = field(default_factory=time.monotonic)
    granted_at: float | None = None
    released: bool = False
    seq: int = field(default_factory=lambda: next(_ids))
    target: Candidate | None = None

    @property
    def granted(self) -> bool:
        return self.future.done() and not self.future.cancelled()

    @property
    def wait_s(self) -> float:
        end = self.granted_at if self.granted_at is not None else time.monotonic()
        return end - self.enqueued_at


@dataclass
class _Pool:
    capacity: int
    spacing_s: float = 0.0
    active: int = 0
    last_start: float = 0.0
    lock: asyncio.Lock | None = None


class SlotQueue:
    def __init__(
        self,
        max_concurrency: int = 1,
        min_spacing_ms: int = 0,
        aging_s: float = 120,
        session_window_s: float = 60,
        session_aware: bool = True,
        fair_share: bool = True,
        strategy: str = "least-busy",
        available: Callable[[int], bool] | None = None,
        on_grant: Callable[[Candidate], None] | None = None,
    ) -> None:
        self.aging_s = aging_s
        self.session_window_s = session_window_s
        self.session_aware = session_aware
        self.fair_share = fair_share
        self.strategy = strategy
        self.available = available or (lambda upstream_id: True)
        self.on_grant = on_grant

        self._pools: dict[int, _Pool] = {0: _Pool(max_concurrency, min_spacing_ms / 1000)}
        self._waiters: list[Ticket] = []
        self._running: set[Ticket] = set()
        self._session_inflight: dict[str, int] = {}
        self._session_last_done: dict[str, float] = {}
        self._client_last_grant: dict[str, float] = {}

    # -- configuration -------------------------------------------------------------------

    def reconfigure(
        self,
        max_concurrency: int,
        min_spacing_ms: int,
        aging_s: float,
        session_window_s: float,
        session_aware: bool,
        fair_share: bool,
    ) -> None:
        """Single-pool convenience (pool 0); see ``set_pools`` for several."""
        self.set_scheduling(aging_s, session_window_s, session_aware, fair_share, self.strategy)
        self.set_pools({0: (max_concurrency, min_spacing_ms)})

    def set_scheduling(
        self, aging_s: float, session_window_s: float, session_aware: bool, fair_share: bool, strategy: str
    ) -> None:
        self.aging_s = aging_s
        self.session_window_s = session_window_s
        self.session_aware = session_aware
        self.fair_share = fair_share
        self.strategy = strategy if strategy in STRATEGIES else "least-busy"

    def set_pools(self, pools: dict[int, tuple[int, int]]) -> None:
        """Apply {pool_id: (max_concurrency, min_spacing_ms)} live. Raising a limit starts
        waiters at once; lowering it lets running requests finish. Removed pools keep their
        running requests but accept nothing new."""
        for pool_id, (capacity, spacing_ms) in pools.items():
            pool = self._pools.get(pool_id)
            if pool is None:
                self._pools[pool_id] = _Pool(capacity, spacing_ms / 1000)
            else:
                pool.capacity, pool.spacing_s = capacity, spacing_ms / 1000
        for pool_id, pool in list(self._pools.items()):
            if pool_id not in pools:
                if pool.active:
                    pool.capacity = 0
                else:
                    del self._pools[pool_id]
        self._dispatch()

    def poke(self) -> None:
        """Re-run dispatch (e.g. after a circuit breaker's cooldown ended)."""
        self._dispatch()

    # -- introspection -------------------------------------------------------------------

    @property
    def active(self) -> int:
        return sum(p.active for p in self._pools.values())

    @property
    def max_concurrency(self) -> int:
        return sum(p.capacity for p in self._pools.values())

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
        if ahead == 0 and any(p.active < p.capacity for p in self._pools.values()):
            return 0
        return ahead + 1

    def oldest_wait_s(self) -> float:
        return max((t.wait_s for t in self._waiters), default=0.0)

    def snapshot(self) -> dict:
        by_lane = {name: 0 for name in LANES}
        by_client: dict[str, dict[str, int]] = {}
        by_upstream: dict[int, int] = {}
        for t in self._waiters:
            by_lane[LANE_NAMES[t.lane]] += 1
            by_client.setdefault(t.client, {"queued": 0, "active": 0})["queued"] += 1
        for t in self._running:
            by_client.setdefault(t.client, {"queued": 0, "active": 0})["active"] += 1
            if t.target is not None:
                by_upstream[t.target.upstream_id] = by_upstream.get(t.target.upstream_id, 0) + 1
        pools = {pid: {"active": p.active, "capacity": p.capacity} for pid, p in self._pools.items()}
        return {"queued_by_lane": by_lane, "clients": by_client, "pools": pools, "upstreams_active": by_upstream}

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

    # -- core operations -----------------------------------------------------------------

    def enqueue(
        self,
        request_id: str,
        client: str = "default",
        lane: int = LANES["normal"],
        session: str | None = None,
        candidates: tuple[Candidate, ...] = DEFAULT_CANDIDATES,
        enqueued_at: float | None = None,
        seq: int | None = None,
    ) -> Ticket:
        """Create a ticket; it is granted immediately if it is first in line and a slot is free.

        ``enqueued_at``/``seq`` let a request re-enter the queue (e.g. failing over to another
        upstream) without losing its place.
        """
        ticket = Ticket(request_id, asyncio.get_running_loop().create_future(), client, lane, session, candidates)
        if enqueued_at is not None:
            ticket.enqueued_at = enqueued_at
        if seq is not None:
            ticket.seq = seq
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

    async def apply_spacing(self, ticket: Ticket | None = None) -> None:
        """Enforce the pool's minimum gap between consecutive upstream starts."""
        pool_id = ticket.target.pool_id if ticket is not None and ticket.target is not None else 0
        pool = self._pools.get(pool_id)
        if pool is None or pool.spacing_s <= 0:
            return
        if pool.lock is None:
            pool.lock = asyncio.Lock()
        async with pool.lock:
            delay = pool.last_start + pool.spacing_s - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            pool.last_start = time.monotonic()

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
        self._running.discard(ticket)
        pool = self._pools.get(ticket.target.pool_id) if ticket.target else None
        if pool is not None:
            pool.active -= 1
            if pool.capacity == 0 and pool.active == 0 and ticket.target.pool_id != 0:
                del self._pools[ticket.target.pool_id]
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

    def _free(self, pool_id: int) -> int:
        pool = self._pools.get(pool_id)
        return pool.capacity - pool.active if pool else 0

    def _pick(self, options: list[Candidate]) -> Candidate:
        if len(options) == 1:
            return options[0]
        if self.strategy == "ordered":
            return min(options, key=lambda c: c.order)
        if self.strategy == "weighted":
            return random.choices(options, weights=[max(c.weight, 1) for c in options])[0]
        return max(options, key=lambda c: (self._free(c.pool_id), c.weight, -c.order))

    def _grant(self, ticket: Ticket, target: Candidate) -> None:
        now = time.monotonic()
        self._pools[target.pool_id].active += 1
        self._running.add(ticket)
        ticket.target = target
        ticket.granted_at = now
        self._client_last_grant[ticket.client] = now
        if ticket.session is not None:
            self._session_inflight[ticket.session] = self._session_inflight.get(ticket.session, 0) + 1
        if self.on_grant:
            self.on_grant(target)
        ticket.future.set_result(None)

    def _dispatch(self) -> None:
        if not self._waiters or not any(self._free(pid) > 0 for pid in self._pools):
            return
        for ticket in self._ordered():
            if ticket.future.done():
                self._waiters.remove(ticket)
                continue
            options = [c for c in ticket.candidates if self._free(c.pool_id) > 0 and self.available(c.upstream_id)]
            if not options:
                continue
            self._waiters.remove(ticket)
            self._grant(ticket, self._pick(options))
            if not any(self._free(pid) > 0 for pid in self._pools):
                break

    def _prune(self, now: float) -> None:
        if len(self._session_last_done) > 10_000:
            cutoff = now - self.session_window_s
            self._session_last_done = {s: t for s, t in self._session_last_done.items() if t >= cutoff}
