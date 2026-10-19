import asyncio
import time

from app.slots import SlotQueue


async def test_grants_immediately_when_free():
    q = SlotQueue(2)
    a, b = q.enqueue("a"), q.enqueue("b")
    assert a.granted and b.granted
    assert q.active == 2 and q.queued == 0


async def test_fifo_handoff():
    q = SlotQueue(1)
    first = q.enqueue("first")
    waiting = [q.enqueue(str(i)) for i in range(3)]
    assert [q.position(t) for t in waiting] == [1, 2, 3]

    order = []

    async def run(ticket):
        await q.wait(ticket)
        order.append(ticket.request_id)
        q.cancel(ticket)

    tasks = [asyncio.create_task(run(t)) for t in reversed(waiting)]  # start in reverse on purpose
    await asyncio.sleep(0)
    q.cancel(first)
    await asyncio.gather(*tasks)
    assert order == ["0", "1", "2"]
    assert q.active == 0


async def test_newcomer_cannot_jump_queue():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    waiter = q.enqueue("waiter")
    q.release(holder)  # slot goes straight to waiter
    newcomer = q.enqueue("newcomer")
    assert waiter.granted and not newcomer.granted


async def test_cancel_queued_does_not_leak():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    queued = q.enqueue("queued")
    q.cancel(queued)
    assert q.queued == 0
    q.cancel(holder)
    assert q.active == 0


async def test_cancel_after_grant_releases():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    waiter = q.enqueue("waiter")
    q.release(holder)
    assert waiter.granted
    q.cancel(waiter)  # e.g. client vanished in the same tick it was granted
    assert q.active == 0


async def test_release_is_idempotent():
    q = SlotQueue(2)
    t = q.enqueue("t")
    q.cancel(t)
    q.cancel(t)
    q.release(t)
    assert q.active == 0


async def test_wait_timeout_keeps_ticket():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    waiter = q.enqueue("waiter")
    assert await q.wait(waiter, 0.01) is False
    assert q.position(waiter) == 1
    q.release(holder)
    assert await q.wait(waiter, 0.01) is True


async def test_task_cancellation_while_waiting():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    waiter = q.enqueue("waiter")

    async def run():
        try:
            await q.wait(waiter)
        finally:
            q.cancel(waiter)

    task = asyncio.create_task(run())
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert q.queued == 0
    q.release(holder)
    assert q.active == 0


async def test_spacing():
    q = SlotQueue(5, min_spacing_ms=50)
    start = time.monotonic()
    for i in range(3):
        q.enqueue(str(i))
        await q.apply_spacing()
    assert time.monotonic() - start >= 0.09
