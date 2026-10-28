"""Queue fixes from the third code review: a retry frees its slot while it waits, a retry or failover counts once
for rate limits and keeps its place, only the half-open trial may give the trial back, and a request that has
waited long enough goes before fresh top-lane ones whatever their sessions."""

import asyncio

import httpx

from app.circuit import Circuits
from app.slots import LANES, SlotQueue
from tests.test_admin import OWNER, login
from tests.test_policies import set_policy
from tests.test_slots import drain_order
from tests.test_upstreams import add_upstream, chat


async def test_a_retry_frees_its_slot_while_it_waits(make_stack):
    """One slot; A gets 503 twice and waits before asking again: B is served meanwhile, not after A."""
    stack = make_stack(limit=1, retry_base_delay_s=0.5, retry_max_delay_s=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        api = "/v1/chat/completions"
        first = asyncio.ensure_future(c.post(api, headers=stack.headers, json=chat("a", mock_key="ka", mock_fail=2)))
        await asyncio.sleep(0.2)  # A has failed once and is waiting to ask again
        second = await c.post(api, headers=stack.headers, json=chat("b"))
        assert second.status_code == 200
        assert not first.done()  # B finished while A was still waiting
        assert (await first).status_code == 200
    starts = [tag for event, tag in stack.mock["log"] if event == "start"]
    assert starts == ["b", "a"]


async def test_a_failover_counts_once_for_rate_limits(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER, lb_strategy="ordered")
    stack.mock["chat_status"] = 503
    other_url, other = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "other", other_url)
        await set_policy(c, stack, rpm=2, on_limit="reject")
        for tag in ("one", "two"):  # each fails over once: two grants, one request
            r = await c.post("/v1/chat/completions", headers=stack.headers, json=chat(tag))
            assert r.status_code == 200, r.text
    assert other["served"] == 2


def test_only_the_trial_gives_it_back():
    c = Circuits(threshold=1, cooldown_s=0)
    assert not c.begin(1)  # closed: an ordinary request
    c.failure(1, "HTTP 503")
    assert c.state(1) == "half-open" and c.begin(1)  # this one is the trial
    assert not c.allows(1) and not c.begin(1)


async def test_a_long_wait_goes_before_fresh_top_lane_sessions():
    q = SlotQueue(1, aging_s=0.05, session_window_s=10)
    first = q.enqueue("agent-1", lane=LANES["high"], session="agent")
    q.cancel(first)  # the agent's session is mid-task: its next steps rank first in their lane
    holder = q.enqueue("holder", lane=LANES["high"], session="other")
    old = q.enqueue("old", lane=LANES["high"], session="new")
    await asyncio.sleep(0.07)  # one aging step past the top lane
    agent = q.enqueue("agent-2", lane=LANES["high"], session="agent")
    assert drain_order(q, holder, [old, agent]) == ["old", "agent-2"]
    again = q.enqueue("again", seq=old.seq, enqueued_at=old.enqueued_at)
    assert again.requeued and not old.requeued
    q.cancel(again)
