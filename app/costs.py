"""What a request cost and what a model costs, in the forms apps read (DESIGN.md, v0.90.0). No app reads all three,
so QueueRouter gives each: ``usage.cost`` in a chat answer (OpenRouter's field: opencode, Cline, Goose and Open WebUI
take it from any endpoint), ``pricing`` in /v1/models (OpenRouter's: USD per token, as strings) and LiteLLM's
/model/info (USD per token, as numbers: oh-my-pi, Roo Code, Crush). The prices are those opencode is told
(main.price_on): yours, else nothing extra on a monthly plan, else the provider's own, else the reference table."""

from __future__ import annotations

import json
from collections.abc import Callable

from .pricing import CACHE_WRITE_RATE, Price, token_costs
from .upstream import usage_counts

CostOf = Callable[[int | None, int | None, int | None, int | None], float | None]

# What a model does, as LiteLLM's /model/info names it (``mode``). Apps leave what isn't "chat" out of their chat
# models (oh-my-pi, Crush).
MODES = {
    "image": "image_generation",
    "video": "video_generation",
    "speech": "audio_speech",
    "transcription": "audio_transcription",
}
# What a chat model is said to take (``supported_openai_params``): oh-my-pi sends a thinking level only to a model
# whose list has reasoning_effort, and tools only to one whose list has them (QueueRouter doesn't know which models
# can't call tools: the provider says so when asked)
CHAT_PARAMS = ("messages", "stream", "tools", "tool_choice", "temperature", "top_p", "max_tokens", "stop")


def per_token(per_million: float) -> str:
    """USD per token as OpenRouter writes it: a plain decimal string ("0.0000015"), "0" for nothing."""
    return f"{per_million / 1e6:.12f}".rstrip("0").rstrip(".") or "0"


def pricing(price: Price) -> dict[str, str]:
    """OpenRouter's ``pricing`` for a /v1/models entry. A write to the provider's cache costs what QueueRouter
    counts for one (pricing.token_costs), where the provider reports any."""
    return {
        "prompt": per_token(price.input),
        "completion": per_token(price.output),
        "input_cache_read": per_token(price.cached_input),
        "input_cache_write": per_token(price.input * CACHE_WRITE_RATE),
    }


def request_cost(
    price: Price | None, prompt: int | None, completion: int | None, cached: int | None, written: int | None
) -> float | None:
    """What a request's tokens cost at ``price`` (USD), None when the model has no price."""
    costs = token_costs(price, prompt or 0, completion or 0, cached or 0, written or 0)
    return None if costs is None else round(sum(costs), 8)


def _priced(data, cost_of: CostOf) -> bool:
    """Puts ``usage.cost`` in a chat answer or chunk; True when it did. A cost the provider states itself stays
    (OpenRouter's is what it charged; Perplexity's is an object of its own)."""
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict) or "cost" in usage:
        return False
    prompt, completion, cached, written = usage_counts(usage)
    if prompt is None and completion is None:
        return False
    cost = cost_of(prompt, completion, cached, written)
    if cost is None:
        return False
    usage["cost"] = cost
    return True


def _dump(data) -> bytes:
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    except UnicodeEncodeError:  # half of a surrogate pair, escaped in the answer: escaped again
        return json.dumps(data, separators=(",", ":")).encode()


def with_cost(content: bytes, cost_of: CostOf) -> bytes:
    """A whole chat answer with what it cost in its usage; as it is when it has no token counts or isn't JSON."""
    if b'"usage"' not in content:
        return content
    try:
        data = json.loads(content)
    except ValueError:
        return content
    return _dump(data) if _priced(data, cost_of) else content


class CostWriter:
    """Passes an SSE stream on line by line, putting ``usage.cost`` in each chunk that carries token counts (the
    last one, for an app that asked for usage). Everything else goes through unchanged; a partial line waits for
    its end."""

    def __init__(self, cost_of: CostOf) -> None:
        self._buffer = b""
        self._cost_of = cost_of

    def feed(self, chunk: bytes) -> bytes:
        self._buffer += chunk
        cut = self._buffer.rfind(b"\n") + 1
        if not cut:
            if len(self._buffer) > 1_000_000:  # never hold a runaway line: pass it on as it is
                rest, self._buffer = self._buffer, b""
                return rest
            return b""
        ready, self._buffer = self._buffer[:cut], self._buffer[cut:]
        if b'"usage"' not in ready:
            return ready
        return b"".join(self._line(line) for line in ready.splitlines(keepends=True))

    def flush(self) -> bytes:
        rest, self._buffer = self._buffer, b""
        return self._line(rest) if rest else rest

    def _line(self, line: bytes) -> bytes:
        if not line.startswith(b"data:") or b'"usage"' not in line:
            return line
        try:
            data = json.loads(line[5:])
        except ValueError:
            return line
        if not _priced(data, self._cost_of):
            return line
        return b"data: " + _dump(data) + line[len(line.rstrip(b"\r\n")) :]


def model_info(entry: dict, price: Price | None, context: int | None, output: int | None, made: str | None) -> dict:
    """One model as LiteLLM's /model/info lists it, from its /v1/models ``entry``: ``model_name`` is the ID apps
    send; sizes, what it reads and prices are in ``model_info`` (USD per token, numbers). ``made``: "image", "video",
    "speech" or "transcription" for a model that isn't a chat model. A price of nothing (a plan) isn't stated: apps
    read 0 as unknown and fall back to a table of their own."""
    info: dict = {"id": entry["id"], "mode": MODES.get(made or "", "chat")}
    if context:
        info["max_input_tokens"] = context
    if output:
        info["max_output_tokens"] = info["max_tokens"] = output
    architecture = entry.get("architecture") if isinstance(entry.get("architecture"), dict) else {}
    reads = architecture.get("input_modalities", entry.get("input_modalities"))
    if isinstance(reads, list):
        info["supports_vision"] = "image" in reads
    if made is None:
        thinks = bool(entry.get("supports_reasoning") or entry.get("reasoning"))
        info["supports_reasoning"] = thinks
        info["supported_openai_params"] = [*CHAT_PARAMS, *(["reasoning_effort"] if thinks else [])]
        info["supports_function_calling"] = True
    if price is not None and (price.input or price.output):
        info["input_cost_per_token"] = round(price.input / 1e6, 12)
        info["output_cost_per_token"] = round(price.output / 1e6, 12)
        info["cache_read_input_token_cost"] = round(price.cached_input / 1e6, 12)
        info["cache_creation_input_token_cost"] = round(price.input * CACHE_WRITE_RATE / 1e6, 12)
        info["supports_prompt_caching"] = price.cached_input < price.input
    return {
        "model_name": entry["id"],
        # the model QueueRouter is asked for is the one it names: apps want this filled (Roo Code skips a row
        # without it), and one that isn't "openai/..." keeps oh-my-pi on chat completions
        "litellm_params": {"model": f"queuerouter/{entry['id']}", "custom_llm_provider": "queuerouter"},
        "model_info": info,
    }
