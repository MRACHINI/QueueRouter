"""Provider presets: sensible defaults when adding an upstream. Every field stays editable."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Plan:
    name: str
    max_concurrency: int
    context_cap: int | None
    monthly_price: float = 0.0  # USD
    embedding_concurrency: int | None = None  # its own parallel limit for /v1/embeddings


@dataclass(frozen=True)
class Preset:
    key: str
    label: str
    base_url: str
    max_concurrency: int
    key_required: bool = True
    plans: tuple[Plan, ...] = field(default_factory=tuple)
    note: str = ""
    params: str = ""  # parameter rules for requests to this provider (e.g. fields it rejects)
    max_retries: int | None = None  # 429 retries before giving up (None = the global budget)


# ArliAI plans from arliai.com/pricing (October 2026). Embedding requests have their own parallel limit
# (the account page shows "Embedding Request" next to "Parallel Request"), assumed equal to the chat one.
ARLIAI_PLANS = (
    Plan("Free", 1, 12_288, 0, 1),
    Plan("Core", 1, 32_768, 15, 1),
    Plan("Plus", 1, 131_072, 20, 1),
    Plan("Pro", 2, 262_144, 30, 2),
    Plan("Max", 6, 524_288, 160, 6),
)

PRESETS: dict[str, Preset] = {
    p.key: p
    for p in (
        Preset(
            "arliai",
            "ArliAI",
            "https://api.arliai.com/v1",
            2,
            plans=ARLIAI_PLANS,
            note="Parallel requests and context cap depend on your plan.",
        ),
        Preset("openrouter", "OpenRouter", "https://openrouter.ai/api/v1", 8),
        Preset("openai", "OpenAI", "https://api.openai.com/v1", 8, params='{"strip": ["top_k"]}'),
        Preset("groq", "Groq", "https://api.groq.com/openai/v1", 4),
        Preset("together", "Together AI", "https://api.together.xyz/v1", 8),
        Preset("deepinfra", "DeepInfra", "https://api.deepinfra.com/v1/openai", 8),
        Preset("mistral", "Mistral", "https://api.mistral.ai/v1", 4),
        Preset(
            "gemini",
            "Google Gemini (AI Studio)",
            "https://generativelanguage.googleapis.com/v1beta/openai",
            2,
            params='{"strip": ["store", "top_k"]}',
            max_retries=2,
            note="Key from aistudio.google.com. Free tier: a few requests per minute and per day; "
            "a used-up daily quota fails fast instead of waiting. Models are listed as models/gemini-....",
        ),
        Preset(
            "anthropic",
            "Anthropic (Claude)",
            "https://api.anthropic.com/v1",
            8,
            # Claude 4.7 and later answer 400 to any temperature below 1 or top_p other than the default
            params='{"strip": ["temperature", "top_p", "top_k", "store"]}',
            note="Key from platform.claude.com (Claude Console). Uses Anthropic's OpenAI-compatible endpoint: tools, "
            "streaming and images work; prompt caching isn't available and thinking isn't returned. temperature and "
            "top_p are removed (newer Claude models reject them). Use a key created in a workspace (personal keys "
            "not tied to one need an extra header QueueRouter doesn't send yet).",
        ),
        Preset(
            "nvidia",
            "NVIDIA (build.nvidia.com)",
            "https://integrate.api.nvidia.com/v1",
            2,
            note="The free API is limited per minute (about 40 requests/min): keep parallel requests low and "
            "set a minimum spacing between requests on the pool.",
        ),
        Preset(
            "ollama",
            "Ollama",
            "http://ollama:11434/v1",
            1,
            key_required=False,
            note="Concurrency = OLLAMA_NUM_PARALLEL on that server.",
        ),
        Preset("lmstudio", "LM Studio", "http://lmstudio:1234/v1", 1, key_required=False),
        Preset("vllm", "vLLM", "http://vllm:8000/v1", 8, key_required=False),
        Preset(
            "llamacpp",
            "llama.cpp server",
            "http://llamacpp:8080/v1",
            1,
            key_required=False,
            note="Concurrency = the server's --parallel slots.",
        ),
        Preset("koboldcpp", "KoboldCpp", "http://koboldcpp:5001/v1", 1, key_required=False),
        Preset("tabbyapi", "TabbyAPI", "http://tabbyapi:5000/v1", 1),
        Preset("custom", "Custom (OpenAI-compatible)", "https://", 1),
    )
}


def plan_for(preset_key: str | None, plan_name: str | None) -> Plan | None:
    preset = PRESETS.get(preset_key or "")
    if not preset or not plan_name:
        return None
    return next((p for p in preset.plans if p.name == plan_name), None)
