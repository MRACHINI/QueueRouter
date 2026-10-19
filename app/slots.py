"""FIFO wait queue in front of a fixed number of upstream slots.

Unlike a bare ``asyncio.Semaphore`` this guarantees strict FIFO hand-off, exposes queue
positions, and is safe against the grant/cancel race (a waiter cancelled in the same tick it
was granted gives its slot back instead of leaking it).

Everything runs on one event loop, so no locks are needed for the bookkeeping.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from dataclasses import dataclass, field

_ids = itertools.count(1)


@dataclass(eq=False)
class Ticket:
    request_id: str
    future: asyncio.Future[None]
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
    def __init__(self, max_concurrency: int, min_spacing_ms: int = 0) -> None:
        self.max_concurrency = max_concurrency
        self.min_spacing_s = min_spacing_ms / 1000
        self.active = 0
        self._waiters: deque[Ticket] = deque()
        self._last_start = 0.0
        self._spacing_lock = asyncio.Lock()

    # -- introspection -------------------------------------------------------------------

    @property
    def queued(self) -> int:
        return len(self._waiters)

    def position(self, ticket: Ticket) -> int:
        """1-based position in the queue, or 0 if not waiting."""
        for i, t in enumerate(self._waiters, 1):
            if t is ticket:
                return i
        return 0

    def oldest_wait_s(self) -> float:
        return self._waiters[0].wait_s if self._waiters else 0.0

    # -- core operations -----------------------------------------------------------------

    def enqueue(self, request_id: str) -> Ticket:
        """Create a ticket; granted immediately if a slot is free and nobody is waiting."""
        ticket = Ticket(request_id, asyncio.get_running_loop().create_future())
        if self.active < self.max_concurrency and not self._waiters:
            self._grant(ticket)
        else:
            self._waiters.append(ticket)
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
        self._dispatch()

    # -- internals -----------------------------------------------------------------------

    def _grant(self, ticket: Ticket) -> None:
        self.active += 1
        ticket.granted_at = time.monotonic()
        ticket.future.set_result(None)

    def _dispatch(self) -> None:
        while self.active < self.max_concurrency and self._waiters:
            ticket = self._waiters.popleft()
            if not ticket.future.done():
                self._grant(ticket)
