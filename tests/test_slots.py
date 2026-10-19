import asyncio
import time

from app.slots import LANES, SlotQueue


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


# -- scheduling order ------------------------------------------------------------------------


def drain_order(q: SlotQueue, holder, tickets) -> list[str]:
    """Release slots one by one and record which tickets get granted, in order."""
    order = []
    current = holder
    for _ in tickets:
        q.cancel(current)
        current = next(t for t in tickets if t.granted and t.request_id not in order)
        order.append(current.request_id)
    q.cancel(current)
    return order


async def test_priority_lanes():
    q = SlotQueue(1)
    holder = q.enqueue("holder")
    low = q.enqueue("low", lane=LANES["low"])
    normal = q.enqueue("normal", lane=LANES["normal"])
    high = q.enqueue("high", lane=LANES["high"])
    assert [q.position(t) for t in (high, normal, low)] == [1, 2, 3]
    assert drain_order(q, holder, [low, normal, high]) == ["high", "normal", "low"]


async def test_aging_prevents_starvation():
    q = SlotQueue(1, aging_s=0.05)
    holder = q.enqueue("holder")
    old_low = q.enqueue("old-low", lane=LANES["low"])
    await asyncio.sleep(0.12)  # two aging steps: low -> normal -> high
    fresh_normal = q.enqueue("fresh-normal", lane=LANES["normal"])
    assert drain_order(q, holder, [old_low, fresh_normal]) == ["old-low", "fresh-normal"]


async def test_active_session_goes_before_new_session():
    q = SlotQueue(1, session_window_s=10)
    first = q.enqueue("agent-step-1", session="agent")
    q.cancel(first)  # the agent's session just finished a request -> "mid-task"
    holder = q.enqueue("holder", session="other")
    newcomer = q.enqueue("new-session", session="new")
    agent_next = q.enqueue("agent-step-2", session="agent")
    assert drain_order(q, holder, [newcomer, agent_next]) == ["agent-step-2", "new-session"]


async def test_session_awareness_can_be_disabled():
    q = SlotQueue(1, session_aware=False)
    first = q.enqueue("agent-step-1", session="agent")
    q.cancel(first)
    holder = q.enqueue("holder", session="other")
    newcomer = q.enqueue("new-session", session="new")
    agent_next = q.enqueue("agent-step-2", session="agent")
    assert drain_order(q, holder, [newcomer, agent_next]) == ["new-session", "agent-step-2"]


async def test_fair_share_between_clients():
    q = SlotQueue(1, session_aware=False)
    holder = q.enqueue("holder", client="swarm")
    swarm = [q.enqueue(f"swarm-{i}", client="swarm") for i in range(3)]
    chat = q.enqueue("chat-0", client="chat")
    # chat was never served, so it goes before the swarm's backlog despite arriving last
    order = drain_order(q, holder, [*swarm, chat])
    assert order[0] == "chat-0"
    assert order[1:] == ["swarm-0", "swarm-1", "swarm-2"]


async def test_predict_position_and_snapshot():
    q = SlotQueue(1)
    assert q.predict_position("c", LANES["normal"], None) == 0
    q.enqueue("holder", client="a")
    q.enqueue("waiting", client="a", lane=LANES["low"])
    assert q.predict_position("b", LANES["high"], None) == 1
    assert q.predict_position("a", LANES["low"], None) == 2  # behind a's own earlier request
    assert q.predict_position("b", LANES["low"], None) == 1  # fair share: b hasn't been served yet
    snap = q.snapshot()
    assert snap["queued_by_lane"] == {"high": 0, "normal": 0, "low": 1}
    assert snap["clients"] == {"a": {"queued": 1, "active": 1}}
