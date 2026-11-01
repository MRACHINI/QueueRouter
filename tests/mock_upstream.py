"""A fake OpenAI-compatible upstream that enforces a hard concurrency limit like ArliAI.

Request body extras (all optional) steer its behaviour:
  "mock_delay":   seconds between streamed chunks / before a non-stream reply (default 0.05)
  "mock_chunks":  number of content chunks to stream (default 5)
  "mock_think":   stream this many reasoning_content chunks first (default 0)
  "mock_status":  force this HTTP status
  "mock_fail":    fail this many times per "mock_key" with 503 before succeeding
  state["daily_quota"]: answer every request with a daily-quota 429 (set on the mock's state)
  state["models_status"]: answer GET /v1/models with this status (no usable model list)
  state["missing_models"]: answer requests for these models with a 404
  state["text_only_models"]: refuse requests with images for these models (400); state["saw_images"]: who got some

POST /v1/messages is Claude's own API (see claude_messages), steered by state:
  state["claude_status"]: answer with this status; state["claude_busy"]: this many 429s first;
  state["claude_tools"], state["claude_tool_input"]: which tools to call, the first offered (default: the very
  first one), and with what;
  state["claude_stream_error"]: an error event after the first text;
  state["claude_refuses"]: stop with a refusal; state["claude_delay"]: seconds between streamed pieces
"""

from __future__ import annotations

import asyncio
import json
import re

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

CLAUDE_FIELDS = {
    "model",
    "max_tokens",
    "messages",
    "system",
    "tools",
    "tool_choice",
    "stop_sequences",
    "temperature",
    "top_p",
    "top_k",
    "stream",
    "cache_control",
    "output_config",
}
CLAUDE_ERRORS = {
    401: "authentication_error",
    429: "rate_limit_error",
    500: "api_error",
    529: "overloaded_error",
}


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
        if state.get("models_status"):  # a provider without a usable model list
            return JSONResponse({"error": {"message": "no model list"}}, status_code=state["models_status"])
        extra = [{"id": m, "object": "model"} for m in state.get("extra_models", [])]  # no size reported
        extra += state.get("extra_model_entries", [])  # whole entries, e.g. {"id", "reasoning": true, "engine"}
        return {"object": "list", "data": [{"id": "mock-model", "object": "model", "max_model_len": 262144}, *extra]}

    @app.get("/v1/credits")
    async def credits(request: Request):
        """A provider account endpoint (like OpenRouter's): only reachable when an admin lets it through."""
        return {"credits": 5, "key": request.headers.get("authorization", "")}

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

    @app.post("/v1/messages")
    async def claude_messages(request: Request):
        """Claude's own API, as strict about its format as the real one (_claude_refusal), with the answers and the
        stream events it sends: thinking blocks without text first, then the text or a tool call."""
        body = json.loads(await request.body())
        headers = {k.lower(): v for k, v in request.headers.items()}
        state.setdefault("claude_requests", []).append(body)
        state["last_headers"] = headers
        if not headers.get("x-api-key"):
            return _claude_error(401, "x-api-key header is required")
        if headers.get("anthropic-version") != "2023-06-01":
            return _claude_error(400, "anthropic-version: header is required")
        if refusal := _claude_refusal(body):
            return _claude_error(400, refusal)
        if state.get("claude_status"):
            return _claude_error(state["claude_status"], "forced")
        if state.get("claude_busy"):
            state["claude_busy"] -= 1
            return _claude_error(429, "busy", {"retry-after": "0"})
        model = body["model"]
        limit, _ = (state.get("context_limits") or {}).get(model, (None, None))
        if limit and len(json.dumps(body["messages"])) / 4 > limit:
            state.setdefault("overflowed", []).append(model)
            return _claude_error(400, f"prompt is too long: 9999999 tokens > {limit} maximum")
        state.setdefault("models", []).append(model)
        usage = _claude_usage(state, body)
        last = body["messages"][-1]["content"]
        answered = isinstance(last, list) and any(block["type"] == "tool_result" for block in last)
        calls = body.get("tools") and not answered and (body.get("tool_choice") or {}).get("type") != "none"
        offered = [t["name"] for t in body.get("tools") or []]
        wanted = [name for name in state.get("claude_tools", []) if name in offered]
        tool = (wanted or offered)[0] if calls else None
        arguments = state.get("claude_tool_input", {"q": "x"})
        tool_id = f"toolu_{len(body['messages'])}"
        stop = "refusal" if state.get("claude_refuses") else "tool_use" if tool else "end_turn"
        words = ["I can't ", "help with ", "that."] if stop == "refusal" else ["hi ", "from ", "claude"]
        if not body.get("stream"):
            content = [{"type": "thinking", "thinking": "", "signature": "EqQBCg"}]
            content.append({"type": "text", "text": "Let me check." if tool else "".join(words)})
            if tool:
                content.append({"type": "tool_use", "id": tool_id, "name": tool, "input": arguments})
            return {
                "id": "msg_01",
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": content,
                "stop_reason": stop,
                "stop_sequence": None,
                "usage": {**usage, "output_tokens": 7},
            }

        def event(kind: str, **fields) -> str:
            return f"event: {kind}\ndata: {json.dumps({'type': kind, **fields})}\n\n"

        async def gen():
            delay = state.get("claude_delay", 0)
            message = {"id": "msg_01", "type": "message", "role": "assistant", "model": model, "content": []}
            message |= {"stop_reason": None, "stop_sequence": None, "usage": {**usage, "output_tokens": 1}}
            state["claude_active"] = state.get("claude_active", 0) + 1
            try:
                yield event("message_start", message=message)
                yield event("ping")
                yield event("content_block_start", index=0, content_block={"type": "thinking", "thinking": ""})
                yield event("content_block_delta", index=0, delta={"type": "thinking_delta", "thinking": ""})
                yield event("content_block_delta", index=0, delta={"type": "signature_delta", "signature": "EqQBCg"})
                yield event("content_block_stop", index=0)
                yield event("content_block_start", index=1, content_block={"type": "text", "text": ""})
                for word in ["Let me check."] if tool else words:
                    await asyncio.sleep(delay)
                    yield event("content_block_delta", index=1, delta={"type": "text_delta", "text": word})
                    if state.get("claude_stream_error"):
                        yield event("error", error={"type": "overloaded_error", "message": "Overloaded"})
                        return
                yield event("content_block_stop", index=1)
                if tool:
                    block = {"type": "tool_use", "id": tool_id, "name": tool, "input": {}}
                    yield event("content_block_start", index=2, content_block=block)
                    text = json.dumps(arguments)
                    for piece in ("", text[:6], text[6:]):
                        delta = {"type": "input_json_delta", "partial_json": piece}
                        yield event("content_block_delta", index=2, delta=delta)
                    yield event("content_block_stop", index=2)
                yield event("a_new_event_kind", something=1)  # "handle unknown event types gracefully"
                delta = {"stop_reason": stop, "stop_sequence": None}
                yield event("message_delta", delta=delta, usage={"output_tokens": 7})
                yield event("message_stop")
            finally:
                state["claude_active"] -= 1

        return StreamingResponse(gen(), media_type="text/event-stream")

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
        always = state.get("overflow_always")  # a provider repeating the request's own text in its error
        if limit and (always or len(json.dumps(body.get("messages") or [])) / 4 > limit):
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
        # A member that reads a page although it wasn't offered the tool (a search result told it to).
        rogue = body.get("model") in state.get("rogue_fetch_models", [])
        if rogue and (body.get("messages") or [{}])[-1].get("role") != "tool":
            function = {"name": "queuerouter_web_fetch", "arguments": json.dumps({"url": "https://example.com/?d=x"})}
            call = {"id": "f1", "type": "function", "function": function}
            message = {"role": "assistant", "content": None, "tool_calls": [call]}
            choice = {"index": 0, "message": message, "finish_reason": "tool_calls"}
            return {"id": "x", "object": "chat.completion", "model": body.get("model"), "choices": [choice]}
        # Model council: the judge's prompt (pick: answer with a number; combine: an answer), and members
        # that say who they are.
        last_message = (body.get("messages") or [{}])[-1]
        content = last_message.get("content", "")
        if isinstance(content, list):  # a message with parts (attachments): the judge's prompt is a text part
            content = "\n".join(str(p.get("text", "")) for p in content if isinstance(p, dict))
        if "[QueueRouter council]" in content:  # added to the last user message
            state.setdefault("judge_inputs", []).append(content)
            picking = "number of the best" in content
            body["mock_content"] = state.get("judge_answer", "2") if picking else "combined answer"
        elif state.get("echo_model"):
            body.setdefault("mock_content", f"answer from {body.get('model')}")
        forced = body.get("mock_status") or state.get("chat_status")  # state: this mock only (503 here, 200 there)
        if forced:
            return JSONResponse({"error": {"message": "forced"}}, status_code=forced)
        key = body.get("mock_key")
        if key and state["fails"].get(key, 0) < body.get("mock_fail", 0):
            state["fails"][key] = state["fails"].get(key, 0) + 1
            return JSONResponse({"error": {"message": "temporarily unavailable"}}, status_code=503)
        # Per-model behaviour (for fallback chains): fail, block by policy, or refuse in text.
        model = body.get("model")
        state.setdefault("models", []).append(model)
        if model in state.get("missing_models", []):  # an Anthropic-style "no such model"
            return JSONResponse({"error": {"type": "not_found_error", "message": f"model: {model}"}}, status_code=404)
        if "image_url" in json.dumps(body.get("messages")):
            if model in state.get("text_only_models", []):  # like Synthetic for a text-only model
                message = f"Model {model} does not appear to support image inputs."
                return JSONResponse({"error": {"message": message}}, status_code=400)
            state.setdefault("saw_images", []).append(model)
        # Google-style 429s: a used-up daily quota, or a per-minute limit for the first N tries.
        if state.get("daily_quota") or (body.get("mock_429_models") or {}).get(model) == "daily":
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

        thoughts = int(body.get("mock_think", 0))

        async def gen():
            finished = False
            try:
                for i in range(thoughts):
                    await asyncio.sleep(delay)
                    delta = {"reasoning_content": f"r{i} "}
                    thought = {"id": "x", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]}
                    yield f"data: {json.dumps(thought)}\n\n"
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
                        "completion_tokens": chunks + thoughts,
                        "prompt_tokens_details": {"cached_tokens": body.get("mock_cached", 0)},
                    },
                }
                if (body.get("stream_options") or {}).get("include_usage"):  # like most providers: only if asked
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


def _claude_error(status: int, message: str, headers: dict | None = None) -> JSONResponse:
    kind = CLAUDE_ERRORS.get(status, "invalid_request_error")
    body = {"type": "error", "error": {"type": kind, "message": message}, "request_id": "req_mock"}
    return JSONResponse(body, status_code=status, headers=headers)


def _claude_refusal(body: dict) -> str | None:
    """What the real API answers 400 to, for a request's format."""
    extra = set(body) - CLAUDE_FIELDS
    if extra:
        return f"{sorted(extra)[0]}: Extra inputs are not permitted"
    if not isinstance(body.get("max_tokens"), int):
        return "max_tokens: Field required"
    if "temperature" in body and body["temperature"] > 1:
        return "temperature: Input should be less than or equal to 1"
    for tool in body.get("tools") or []:
        if set(tool) - {"name", "description", "input_schema"} or tool["input_schema"].get("type") != "object":
            return "tools.0: a tool is a name, a description and an object input_schema"
    messages = body.get("messages") or []
    if not messages:
        return "messages: at least one message is required"
    if messages[-1]["role"] != "user":
        return "This model does not support assistant message prefill. The conversation must end with a user message."
    waiting: set[str] = set()  # tool calls of the last assistant turn
    for i, message in enumerate(messages):
        if message["role"] not in ("user", "assistant"):
            return f"messages.{i}.role: Input should be 'user' or 'assistant'"
        blocks = message["content"]
        if not blocks:
            return f"messages.{i}: all messages must have non-empty content"
        results = [block.get("tool_use_id") for block in blocks if block["type"] == "tool_result"]
        if message["role"] == "user":
            if set(results) != waiting:
                return f"messages.{i}: tool_use ids were found without tool_result blocks immediately after"
            if any(block["type"] == "tool_result" for block in blocks[len(results) :]):
                return f"messages.{i}: tool_result blocks must come first"
            waiting = set()
        for block in blocks:
            if block["type"] == "text" and not block["text"].strip():
                return f"messages.{i}: text content blocks must be non-empty"
            if block["type"] == "tool_use":
                if not re.fullmatch(r"[a-zA-Z0-9_-]+", block["id"]):
                    return f"messages.{i}.content.id: String should match pattern '^[a-zA-Z0-9_-]+$'"
                if not isinstance(block["input"], dict):
                    return f"messages.{i}.content.input: Input should be a valid dictionary"
                waiting.add(block["id"])
    return None


def _claude_usage(state: dict, body: dict) -> dict:
    """Tokens, at about 4 characters each. With ``cache_control`` it's what automatic caching does: the longest earlier
    prompt this one starts with is read, the rest written (only an unchanged start can be read)."""
    parts = [body.get("tools"), body.get("system"), *body["messages"]]
    prompt = "".join(json.dumps(part) for part in parts)
    if "cache_control" not in body:
        fresh = {"input_tokens": len(prompt) // 4}
        return {**fresh, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
    saved = state.setdefault("claude_cache", [])
    read = max((len(earlier) for earlier in saved if prompt.startswith(earlier)), default=0)
    saved.append(prompt)
    written = (len(prompt) - read) // 4
    return {"input_tokens": 3, "cache_creation_input_tokens": written, "cache_read_input_tokens": read // 4}


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
