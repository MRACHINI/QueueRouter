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
    max_retries: int | None = None  # retries on this upstream (429 / 5xx / errors); None = the global budget


# ArliAI plans from arliai.com/pricing (October 2026). Embedding requests have their own parallel limit
# (the account page shows "Embedding Request" next to "Parallel Request"), assumed equal to the chat one.
ARLIAI_PLANS = (
    Plan("Free", 1, 12_288, 0, 1),
    Plan("Core", 1, 32_768, 15, 1),
    Plan("Plus", 1, 131_072, 20, 1),
    Plan("Pro", 2, 262_144, 30, 2),
    Plan("Max", 6, 524_288, 160, 6),
)
# Synthetic's subscriptions from synthetic.new/pricing (November 2026): a weekly credit quota each; it publishes no
# parallel-request limit, so each plan starts at the preset's
SYNTHETIC_PLANS = (
    Plan("Go", 4, None, 9),
    Plan("Standard", 4, None, 30),
    Plan("Big", 4, None, 90),
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
        Preset(
            "synthetic",
            "Synthetic",
            "https://api.synthetic.new/openai/v1",
            4,
            plans=SYNTHETIC_PLANS,
            note="Models are named hf:… (e.g. hf:zai-org/GLM-4.7). Thinking levels: low, medium, high (no off). "
            "Parallel requests aren't published: 4 is a start; lower it if it answers 429. Usage past a plan's "
            "weekly quota is billed at API prices.",
        ),
        Preset(
            "refuseless",
            "Refuseless",
            "https://api.refuseless.com/v1",
            4,
            note="Prepaid credits. Its model list states each model's thinking levels (off, low, high, max), image "
            "input and your prices, and QueueRouter uses them. Out of credits (402), the request moves to another "
            "upstream. Parallel requests aren't published: 4 is a start.",
        ),
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
            note="Key from platform.claude.com (Claude Console). Uses Claude's own Messages API: tools, streaming, "
            "images and prompt caching work (conversations are cached by themselves); thinking isn't returned. "
            "temperature and top_p are removed (newer Claude models reject them). Use a key created in a workspace "
            "(personal keys not tied to one need an extra header QueueRouter doesn't send yet).",
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


def all_plans() -> list[tuple[str, str, Plan]]:
    """Every provider's plans, for the pickers: (value "<preset>:<plan>", label "<provider> <plan>", plan)."""
    return [(f"{p.key}:{plan.name}", f"{p.label} {plan.name}", plan) for p in PRESETS.values() for plan in p.plans]


def plan_choice(value: str | None) -> tuple[str, Plan] | None:
    """(its label, the plan) for a picker's value ("synthetic:Go"; a bare name is ArliAI's, as before); None for
    "custom limits" or one that doesn't exist."""
    if not value:
        return None
    key, _, name = value.partition(":") if ":" in value else ("arliai", "", value)
    plan = plan_for(key, name)
    return (f"{PRESETS[key].label} {plan.name}", plan) if plan else None
