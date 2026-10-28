"""Smaller fixes from the third code review: adapters (tool calls without an index or with object arguments,
images in tool output), the council judge's prompt, provider status codes, tracing during a collector outage."""

import httpx

from app import council, responses_api, tracing
from app.sse import arguments_text, tool_call_delta
from app.upstream import FAILURE_STATUS


def test_tool_calls_without_an_index_or_with_object_arguments():
    ids: dict[str, int] = {}
    first = tool_call_delta({"id": "a", "function": {"name": "f", "arguments": '{"x":'}}, ids)
    more = tool_call_delta({"function": {"arguments": "1}"}}, ids)
    second = tool_call_delta({"id": "b", "function": {"name": "g", "arguments": {"y": 2}}}, ids)
    assert (first[0], more[0], second[0]) == (0, 0, 1)
    assert second[2] == '{"y": 2}' and more[2] == "1}"
    assert tool_call_delta({"index": 3, "function": None}, ids)[:1] == (3,)
    assert arguments_text({"a": 1}) == '{"a": 1}' and arguments_text(None) == "{}" and arguments_text("") == "{}"


def test_images_in_tool_output_become_a_note():
    messages = responses_api.input_messages(
        [
            {"type": "function_call", "call_id": "c1", "name": "shot", "arguments": "{}"},
            {
                "type": "function_call_output",
                "call_id": "c1",
                "output": [
                    {"type": "input_text", "text": "the page:"},
                    {"type": "input_image", "image_url": "data:image/png;base64," + "A" * 5000},
                ],
            },
        ]
    )
    assert messages[-1] == {"role": "tool", "tool_call_id": "c1", "content": "the page:\n[An image was returned here.]"}


def test_judge_prompt_joins_a_message_with_parts_and_drops_the_apps_format():
    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": "q"}, {"type": "image_url"}]}],
        "response_format": {"type": "json_schema"},
        "stop": ["\n"],
        "n": 2,
    }
    judged = council.judge_payload(payload, "j", "pick", [("a", {"content": "x"})])
    assert len(judged["messages"]) == 1  # not a second user message in a row
    parts = judged["messages"][0]["content"]
    assert parts[:2] == payload["messages"][0]["content"] and council.JUDGE_MARK in parts[-1]["text"]
    assert not {"response_format", "stop", "n"} & set(judged)
    combined = council.judge_payload(payload, "j", "combine", [("a", {"content": "x"})])
    assert combined["response_format"] == {"type": "json_schema"}  # the answer the app gets: its format


def test_overloaded_and_cloudflare_statuses_are_failures():
    assert {529, 520, 524} <= FAILURE_STATUS and 429 not in FAILURE_STATUS


async def test_spans_wait_for_the_collector():
    exporter = tracing.Exporter()
    exporter.pending.extend({"name": f"s{i}"} for i in range(3))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503))) as http:
        await exporter.flush(http, "http://collector", "", "qr", "1")
    assert [s["name"] for s in exporter.pending] == ["s0", "s1", "s2"]
    sent = []

    def collector(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(collector)) as http:
        await exporter.flush(http, "http://collector", "", "qr", "1")
    assert not exporter.pending and len(sent) == 1
