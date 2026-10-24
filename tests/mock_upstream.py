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

    @app.post("/v1/traces")
    async def traces(request: Request):
        """OTLP/HTTP receiver (tracing tests)."""
        state.setdefault("traces", []).append({"body": await request.body(), "headers": dict(request.headers)})
        return JSONResponse({})

    @app.post("/hook")
    async def hook(request: Request):
        """Webhook receiver (alerts tests); answers with the status given in ?status=."""
        state.setdefault("hooks", []).append({"body": await request.body(), "headers": dict(request.headers)})
        return JSONResponse({}, status_code=int(request.query_params.get("status", 200)))

    @app.get("/v1/models")
    async def models():
        extra = [{"id": m, "object": "model"} for m in state.get("extra_models", [])]  # no size reported
        return {"object": "list", "data": [{"id": "mock-model", "object": "model", "max_model_len": 262144}, *extra]}

    @app.get("/page")
    async def page():
        """A web page for QueueRouter's web_fetch."""
        from fastapi.responses import HTMLResponse

        html = (
            "<html><head><title>Install docs</title><script>var x=1;</script></head><body>"
            "<nav>Home | Blog</nav><h1>Install</h1><p>Use Python 3.14 or newer.</p></body></html>"
        )
        return HTMLResponse(html)

    @app.get("/search")
    async def searxng(request: Request):
        """A SearXNG-style search (format=json), for QueueRouter's web search."""
        query = request.query_params.get("q", "")
        state.setdefault("searches", []).append(query)
        if state.get("search_status"):
            return JSONResponse({"error": "denied"}, status_code=state["search_status"])
        results = [
            {"title": f"Result for {query}", "url": "https://example.com/1", "content": f"snippet about {query}"}
        ]
        return {"results": results}

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        """Its own parallel counter (providers like ArliAI limit embeddings separately)."""
        body = json.loads(await request.body())
        state["embed_active"] = state.get("embed_active", 0) + 1
        state["embed_peak"] = max(state.get("embed_peak", 0), state["embed_active"])
        try:
            await asyncio.sleep(body.get("mock_delay", 0.05))
        finally:
            state["embed_active"] -= 1
        state["embedded"] = state.get("embedded", 0) + 1
        return {
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
            "model": body.get("model"),
            "usage": {"prompt_tokens": 3, "total_tokens": 3},
        }

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        raw = await request.body()
        state["last_body"] = raw
        state["last_auth"] = request.headers.get("authorization")
        state["last_headers"] = {k.lower(): v for k, v in request.headers.items()}
        body = json.loads(raw)
        if rejected := over_limit():
            return rejected
        # Context limits set by a test: state["context_limits"][model] = (tokens, style) rejects longer
        # prompts (about 4 characters per token) the way that provider words it.
        limit, style = (state.get("context_limits") or {}).get(body.get("model"), (None, None))
        if limit and len(json.dumps(body.get("messages") or [])) / 4 > limit:
            state.setdefault("overflowed", []).append(body.get("model"))
            return _overflow(limit, style)
        # Summary requests (compaction): answer "summary <n>" and remember what was asked.
        first = (body.get("messages") or [{}])[0]
        if str(first.get("content", "")).startswith("You compact the history"):
            state.setdefault("summary_inputs", []).append(body["messages"][1]["content"])
            body["mock_content"] = f"summary {len(state['summary_inputs'])}"
            body["mock_delay"] = state.get("summary_delay", 0.05)  # per chunk of 5: a slow summariser
        # Orchestrator (scripted by the test: state["orch_script"], one step per request) and its members.
        first_content = str((body.get("messages") or [{}])[0].get("content", ""))
        if "You lead a team" in first_content:
            turn = state.setdefault("orch_turn", 0)
            state["orch_turn"] = turn + 1
            state.setdefault("orch_requests", []).append(body)
            script = state.get("orch_script") or [{"answer": "final from orchestrator"}]
            step = script[min(turn, len(script) - 1)]
            if "app_call" in step:  # end the turn with one of the app's own tools
                name, arguments = step["app_call"]
                function = {"name": name, "arguments": json.dumps(arguments)}
                call = {"id": f"a{turn}", "type": "function", "function": function}
                message = {"role": "assistant", "content": None, "tool_calls": [call]}
                choice = {"index": 0, "message": message, "finish_reason": "tool_calls"}
                return {"id": "x", "object": "chat.completion", "model": body.get("model"), "choices": [choice]}
            if "answer" in step:
                message = {"role": "assistant", "content": step["answer"]}
                choice = {"index": 0, "message": message, "finish_reason": "stop"}
                usage = {"prompt_tokens": 10, "completion_tokens": 3}
                return {
                    "id": "x",
                    "object": "chat.completion",
                    "model": body.get("model"),
                    "choices": [choice],
                    "usage": usage,
                }
            else:
                calls = []
                for n, (name, arguments) in enumerate(step["calls"]):
                    function = {"name": name, "arguments": json.dumps(arguments)}
                    calls.append({"id": f"o{turn}_{n}", "type": "function", "function": function})
                message = {"role": "assistant", "content": step.get("note"), "tool_calls": calls}
                choice = {"index": 0, "message": message, "finish_reason": "tool_calls"}
                return {"id": "x", "object": "chat.completion", "model": body.get("model"), "choices": [choice]}
        elif first_content.startswith("You are a member of a team") and all(
            str((t.get("function") or {}).get("name", "")).startswith("queuerouter_") for t in body.get("tools") or []
        ):
            task = str(body["messages"][1].get("content", ""))
            state.setdefault("member_tasks", []).append((body.get("model"), task))
            body["mock_content"] = f"answer from {body.get('model')}: {task[:60]}"
        # Council members that search (QueueRouter's own web search tool): ask first, answer with the results.
        offered = {(t.get("function") or {}).get("name") for t in body.get("tools") or []}
        if "queuerouter_web_search" in offered and body.get("model") in state.get("searching_models", []):
            last = (body.get("messages") or [{}])[-1]
            if last.get("role") != "tool" or state.get("always_search"):
                query = state.get("search_query", "latest benchmarks")
                arguments = json.dumps({"query": query})
                function = {"name": "queuerouter_web_search", "arguments": arguments}
                call = {"id": f"s{len(body['messages'])}", "type": "function", "function": function}
                message = {"role": "assistant", "content": None, "tool_calls": [call]}
                choice = {"index": 0, "message": message, "finish_reason": "tool_calls"}
                return {"id": "x", "object": "chat.completion", "model": body.get("model"), "choices": [choice]}
            body["mock_content"] = (
                f"answer from {body.get('model')} citing {str(last.get('content', '')).splitlines()[0][:80]}"
            )
        # Model council: the judge's prompt (pick: answer with a number; combine: an answer), and members
        # that say who they are.
        last_message = (body.get("messages") or [{}])[-1]
        if str(last_message.get("content", "")).startswith("[QueueRouter council]"):
            state.setdefault("judge_inputs", []).append(last_message["content"])
            picking = "number of the best" in last_message["content"]
            body["mock_content"] = state.get("judge_answer", "2") if picking else "combined answer"
        elif state.get("echo_model"):
            body.setdefault("mock_content", f"answer from {body.get('model')}")
        if body.get("mock_status"):
            return JSONResponse({"error": {"message": "forced"}}, status_code=body["mock_status"])
        key = body.get("mock_key")
        if key and state["fails"].get(key, 0) < body.get("mock_fail", 0):
            state["fails"][key] = state["fails"].get(key, 0) + 1
            return JSONResponse({"error": {"message": "temporarily unavailable"}}, status_code=503)
        # Per-model behaviour (for fallback chains): fail, block by policy, or refuse in text.
        model = body.get("model")
        state.setdefault("models", []).append(model)
        # Google-style 429s: a used-up daily quota, or a per-minute limit for the first N tries.
        if (body.get("mock_429_models") or {}).get(model) == "daily":
            return JSONResponse(
                _google_429("GenerateRequestsPerDayPerProjectPerModel-FreeTier", "45s"), status_code=429
            )
        if times := body.get("mock_429_times"):
            seen = state["fails"].get(f"429:{key}", 0)
            if seen < times:
                state["fails"][f"429:{key}"] = seen + 1
                return JSONResponse(_google_429("GenerateRequestsPerMinutePerProjectPerModel", "0.1s"), status_code=429)
        if model in body.get("mock_fail_models", []):
            return JSONResponse({"error": {"message": "model overloaded"}}, status_code=503)
        if model in body.get("mock_policy_models", []):
            return JSONResponse({"error": {"message": "Request blocked by content policy"}}, status_code=400)
        refuse = model in body.get("mock_refuse_models", [])
        messages = body.get("messages") or [{}]
        real_tools = {n for n in offered if not str(n).startswith("queuerouter_")}  # not QueueRouter's own tools
        if real_tools and body.get("tool_choice") != "none" and messages[-1].get("role") == "user":
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


def _google_429(quota_id: str, retry_delay: str) -> list:
    """A 429 shaped like Google's OpenAI-compatible endpoint returns it (a list with one error)."""
    return [
        {
            "error": {
                "code": 429,
                "message": f"You exceeded your current quota. Quota exceeded for metric: {quota_id}",
                "status": "RESOURCE_EXHAUSTED",
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
                        "violations": [
                            {"quotaMetric": "generativelanguage.googleapis.com/requests", "quotaId": quota_id}
                        ],
                    },
                    {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": retry_delay},
                ],
            }
        }
    ]


def _overflow(limit: int, style: str) -> JSONResponse:
    """A "prompt too long" error as different providers word it."""
    if style == "google":
        message = f"The input token count (9999999) exceeds the maximum number of tokens allowed ({limit})."
        return JSONResponse([{"error": {"code": 400, "message": message, "status": "INVALID_ARGUMENT"}}], 400)
    if style == "anthropic":
        return JSONResponse({"error": {"message": f"prompt is too long: 9999999 tokens > {limit} maximum"}}, 400)
    if style == "unstated":
        return JSONResponse({"error": {"message": "Input is too long for this model."}}, 413)
    message = f"This model's maximum context length is {limit} tokens. However, you requested more."
    return JSONResponse({"error": {"message": message, "code": "context_length_exceeded"}}, 400)
