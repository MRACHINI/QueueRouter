"""A fake OpenAI-compatible upstream that enforces a hard concurrency limit like ArliAI.

Request body extras (all optional) steer its behaviour:
  "mock_delay":   seconds between streamed chunks / before a non-stream reply (default 0.05)
  "mock_chunks":  number of content chunks to stream (default 5)
  "mock_status":  force this HTTP status
  "mock_fail":    fail this many times per "mock_key" with 503 before succeeding
"""

from __future__ import annotations

import asyncio
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse


def create_mock(limit: int = 2) -> FastAPI:
    app = FastAPI()
    state = {"active": 0, "peak": 0, "rejected": 0, "served": 0, "fails": {}, "log": [], "aborted": 0}
    app.state.mock = state

    def over_limit() -> JSONResponse | None:
        if state["active"] >= limit:
            state["rejected"] += 1
            return JSONResponse({"error": {"message": "too many parallel requests"}}, status_code=429)
        return None

    @app.post("/hook")
    async def hook(request: Request):
        """Webhook receiver (alerts tests); answers with the status given in ?status=."""
        state.setdefault("hooks", []).append({"body": await request.body(), "headers": dict(request.headers)})
        return JSONResponse({}, status_code=int(request.query_params.get("status", 200)))

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": "mock-model", "object": "model", "max_model_len": 262144}]}

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        raw = await request.body()
        state["last_body"] = raw
        state["last_auth"] = request.headers.get("authorization")
        state["last_headers"] = {k.lower(): v for k, v in request.headers.items()}
        body = json.loads(raw)
        if rejected := over_limit():
            return rejected
        if body.get("mock_status"):
            return JSONResponse({"error": {"message": "forced"}}, status_code=body["mock_status"])
        key = body.get("mock_key")
        if key and state["fails"].get(key, 0) < body.get("mock_fail", 0):
            state["fails"][key] = state["fails"].get(key, 0) + 1
            return JSONResponse({"error": {"message": "temporarily unavailable"}}, status_code=503)
        # Per-model behaviour (for fallback chains): fail, block by policy, or refuse in text.
        model = body.get("model")
        state.setdefault("models", []).append(model)
        if model in body.get("mock_fail_models", []):
            return JSONResponse({"error": {"message": "model overloaded"}}, status_code=503)
        if model in body.get("mock_policy_models", []):
            return JSONResponse({"error": {"message": "Request blocked by content policy"}}, status_code=400)
        refuse = model in body.get("mock_refuse_models", [])
        messages = body.get("messages") or [{}]
        if body.get("tools") and messages[-1].get("role") == "user":
            return _tool_call(body)  # agents: call the first tool
        if messages[-1].get("role") == "tool":
            tag = f"tool said {messages[-1].get('content')}"
            body.setdefault("mock_tag", tag)

        delay = body.get("mock_delay", 0.05)
        chunks = body.get("mock_chunks", 5)
        tag = body.get("mock_tag")
        words = ["I can't ", "help with ", "that request."] if refuse else [f"t{i} " for i in range(chunks)]
        chunks = len(words)
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        state["log"].append(("start", tag))

        if not body.get("stream"):
            try:
                await asyncio.sleep(delay * chunks)
            finally:
                state["active"] -= 1
            state["served"] += 1
            state["log"].append(("end", tag))
            return {
                "id": "x",
                "object": "chat.completion",
                "model": body.get("model"),
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": body.get("mock_content") or ("".join(words) if refuse else f"hi {tag}"),
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": chunks,
                    "prompt_tokens_details": {"cached_tokens": body.get("mock_cached", 0)},
                },
            }

        async def gen():
            finished = False
            try:
                for i in range(chunks):
                    await asyncio.sleep(delay)
                    chunk = {
                        "id": "x",
                        "object": "chat.completion.chunk",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {"content": words[i]},
                                "finish_reason": body.get("mock_finish", "stop") if i == chunks - 1 else None,
                            }
                        ],
                    }
                    yield f"data: {json.dumps(chunk)}\n\n"
                usage = {
                    "id": "x",
                    "object": "chat.completion.chunk",
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": chunks,
                        "prompt_tokens_details": {"cached_tokens": body.get("mock_cached", 0)},
                    },
                }
                yield f"data: {json.dumps(usage)}\n\n"
                yield "data: [DONE]\n\n"
                finished = True
            finally:
                state["active"] -= 1
                if finished:
                    state["served"] += 1
                else:
                    state["aborted"] += 1
                state["log"].append(("end", tag))

        return StreamingResponse(gen(), media_type="text/event-stream")

    return app


def _tool_call(body: dict):
    """Answer by calling the first tool with {"q": "x"} (arguments streamed in two pieces)."""
    name = body["tools"][0]["function"]["name"]
    usage = {"prompt_tokens": 12, "completion_tokens": 3}
    if not body.get("stream"):
        call = {"id": "call_1", "type": "function", "function": {"name": name, "arguments": '{"q": "x"}'}}
        return {
            "id": "x",
            "object": "chat.completion",
            "model": body.get("model"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": None, "tool_calls": [call]},
                    "finish_reason": "tool_calls",
                }
            ],
            "usage": usage,
        }

    def chunk(delta: dict, finish=None, **extra) -> str:
        choices = [{"index": 0, "delta": delta, "finish_reason": finish}] if delta is not None else []
        return f"data: {json.dumps({'id': 'x', 'object': 'chat.completion.chunk', 'choices': choices, **extra})}\n\n"

    def gen():
        yield chunk({"role": "assistant", "content": "Let me check."})
        first = {"index": 0, "id": "call_1", "type": "function", "function": {"name": name, "arguments": '{"q": '}}
        yield chunk({"tool_calls": [first]})
        yield chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"x"}'}}]}, "tool_calls")
        yield chunk(None, usage=usage)
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")
