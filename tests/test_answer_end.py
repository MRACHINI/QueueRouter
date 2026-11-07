"""The end of a streamed answer waits for the usage right behind it, so an app that hangs up as soon as it has the
end still leaves the request's token counts, and the request counts as done, not cancelled."""

import asyncio
import time

import httpx

from app.upstream import END_MAX_S, END_WAIT_S, AnswerEnd, UsageSniffer
from tests.test_admin import OWNER, login
from tests.test_fallback import chat as alias_chat
from tests.test_nested_aliases import save_alias
from tests.test_usage import chat, flushed

CONTENT = b'data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}\n\n'
FINISH = b'data: {"choices":[{"index":0,"delta":{"content":"!"},"finish_reason":"stop"}]}\n\n'
USAGE = b'data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n'
DONE = b"data: [DONE]\n\n"
EARLY = b'data: {"choices":[{"index":0,"delta":{"content":"a"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}\n\n'
CHOICE1 = b'data: {"choices":[{"index":1,"delta":{"content":"yo"},"finish_reason":null}]}\n\n'
FINISH1 = b'data: {"choices":[{"index":1,"delta":{"content":"."},"finish_reason":"stop"}]}\n\n'


def passed(end, sniffer, chunk, now):
    sniffer.feed(chunk, now)
    return end.take(chunk, sniffer, now)


def test_the_end_waits_for_the_usage_behind_it():
    sniffer, end = UsageSniffer(), AnswerEnd()
    assert passed(end, sniffer, CONTENT, 0.0) == CONTENT  # before the end: as it comes
    assert passed(end, sniffer, FINISH, 0.1) == b"" and end.holding
    assert passed(end, sniffer, USAGE, 0.15) == FINISH + USAGE and not end.holding  # together, with the usage read
    assert sniffer.completion_tokens == 2 and end.had_end(sniffer)
    assert passed(end, sniffer, DONE, 0.2) == DONE


def test_the_end_doesnt_wait_when_nothing_is_missing():
    sniffer, end = UsageSniffer(), AnswerEnd()
    assert passed(end, sniffer, FINISH + USAGE + DONE, 0.0) == FINISH + USAGE + DONE  # all at once: no wait
    sniffer, end = UsageSniffer(), AnswerEnd()  # a provider that sends no usage: the stream's [DONE] ends the wait
    assert passed(end, sniffer, FINISH, 0.0) == b""
    assert passed(end, sniffer, DONE, 0.05) == FINISH + DONE


def test_usage_from_before_the_end_doesnt_count():
    sniffer, end = UsageSniffer(), AnswerEnd()  # a provider that also sends running counts along the way
    assert passed(end, sniffer, EARLY, 0.0) == EARLY
    assert passed(end, sniffer, FINISH, 0.1) == b""  # waits for the final usage all the same
    assert passed(end, sniffer, USAGE, 0.2) == FINISH + USAGE and sniffer.completion_tokens == 2


def test_the_end_waits_a_little():
    sniffer, end = UsageSniffer(), AnswerEnd()
    assert end.left(0.0) == 0  # nothing waits
    assert passed(end, sniffer, FINISH, 0.0) == b""
    assert end.left(0.4) == END_WAIT_S - 0.4 and end.left(END_WAIT_S + 1) == 0  # the caller's tick comes by it
    assert end.release(END_WAIT_S) == FINISH and not end.holding  # the tick: nothing came soon after
    assert end.release() == b"" and end.had_end(sniffer)
    # ends wait END_MAX_S in all over a stream, then go at once
    for start in (10.0, 20.0):
        assert passed(end, sniffer, FINISH, start) == b""
        assert end.release(start + END_WAIT_S) == FINISH
    assert passed(end, sniffer, FINISH, 30.0) == FINISH and END_WAIT_S * 3 >= END_MAX_S


def test_with_several_choices_each_end_waits_and_the_answer_isnt_held_up():
    sniffer, end = UsageSniffer(), AnswerEnd()
    assert passed(end, sniffer, FINISH, 0.0) == b""  # choice 0 ends
    assert passed(end, sniffer, CHOICE1, 0.05) == FINISH + CHOICE1  # choice 1 goes on: nothing waits for it
    assert not end.holding and not end.had_end(sniffer)
    assert passed(end, sniffer, FINISH1, 0.1) == b""  # the last end waits for its usage again
    assert passed(end, sniffer, USAGE, 0.15) == FINISH1 + USAGE and end.had_end(sniffer)
    sniffer, end = UsageSniffer(), AnswerEnd()  # an end that went after waiting, then more of the answer came
    assert passed(end, sniffer, FINISH, 0.0) == b""
    assert end.release(END_WAIT_S) == FINISH and end.had_end(sniffer)
    assert passed(end, sniffer, CHOICE1, 1.5) == CHOICE1 and end.went_on(sniffer) and not end.had_end(sniffer)


async def stream_until(c, stack, body, stop):
    """Streams ``body`` and hangs up at the first line for which ``stop(line, seen)`` says so (``seen``: the lines
    before it), as apps do; the time it hung up."""
    seen: list[str] = []
    async with c.stream("POST", "/v1/chat/completions", headers=stack.headers, json=body) as r:
        assert r.status_code == 200
        async for line in r.aiter_lines():
            if stop(line, seen):
                return time.monotonic()
            seen.append(line)
    raise AssertionError("the stream ended first")


def at_the_end(line, seen):
    return '"finish_reason": "stop"' in line


async def newest_row(c, stack, count):
    await flushed(stack, count)
    return (await c.get("/admin/api/requests")).json()["rows"][0]


async def test_an_app_that_leaves_at_the_end_still_leaves_its_tokens(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        # the usage comes 0.3 s after the end (and the stream's [DONE] later still): the end waits for it
        body = chat("t", stream=True, mock_chunks=5, mock_usage_delay=0.3, mock_done_delay=2)
        await stream_until(c, stack, body, at_the_end)
        row = await newest_row(c, stack, 1)
        assert row["outcome"] == "done" and row["stopped"] is None
        assert row["prompt_tokens"] == 10 and row["completion_tokens"] == 5
        # usage that doesn't come within a second: the end goes on without it, still a request done
        started = time.monotonic()
        body = chat("t", stream=True, mock_chunks=2, mock_delay=0.01, mock_usage_delay=3)
        ended = await stream_until(c, stack, body, at_the_end)
        assert END_WAIT_S - 0.1 <= ended - started < END_WAIT_S + 1.5  # it waited about a second, then went on
        row = await newest_row(c, stack, 2)
        assert row["outcome"] == "done" and row["completion_tokens"] is None


async def test_an_app_that_leaves_while_the_end_waits_is_cancelled(make_stack):
    stack = make_stack(bootstrap=OWNER)  # keep-alives every 0.2 s (conftest) while the end waits

    def at_a_keepalive_after_the_answer(line, seen):
        return line.startswith(": keep-alive") and any('"content"' in s for s in seen)

    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        body = chat("t", stream=True, mock_chunks=2, mock_delay=0.01, mock_usage_delay=3, mock_done_delay=3)
        await stream_until(c, stack, body, at_a_keepalive_after_the_answer)  # it never got the end
        row = await newest_row(c, stack, 1)
    assert row["outcome"] == "cancelled" and row["stopped"] == "after answer"


async def test_a_forwarded_stream_left_at_the_end_is_forwarded_and_done(make_stack):
    stack = make_stack(bootstrap=OWNER, max_retries=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "plain", "mock-model")
        await save_alias(c, "outer", "big-model", ["plain"])
        body = alias_chat("outer", stream=True, mock_fail_models=["big-model"], mock_usage_delay=0.3, mock_done_delay=2)
        await stream_until(c, stack, body, at_the_end)
        await flushed(stack, 2)
        rows = (await c.get("/admin/api/requests")).json()["rows"]
    outer = next(r for r in rows if r["outcome"] == "forwarded")
    inner = next(r for r in rows if r["id"] != outer["id"])
    assert outer["stopped"] is None and inner["outcome"] == "done" and inner["completion_tokens"] == 5
    usage = await asyncio.to_thread(stack.app.state.store.usage_between, 0, 4e9)
    assert sum(u["requests"] for u in usage) == 1 and sum(u["errors"] for u in usage) == 0
