"""A live test of a model's thinking levels on an upstream (Upstreams page > Check models > Test thinking levels).

What levels a model takes, and whether "off" turns its thinking off, is otherwise read from the provider's list or
a reference list by name, or guessed: Qwen Cloud's list says nothing, a reference names another host's levels, and
some providers accept a level and ignore it. The test asks the model one small question once per level, each the
way the upstream is asked (``thinking.translate``; an API QueueRouter doesn't know is tried the three ways), and
once with no level at all, and reports what came back: refused (the provider's own words), or answered, with how
much it thought. From that: the levels it takes, whether off works, and for an unknown API which way it honours.
It's evidence, not proof: one answer per level varies, and a level accepted may still be ignored, so the result is
shown for the admin to save as the model's levels (Models page), never applied by itself.

The requests are real ones (a dozen at most, small), through the normal pipeline but sent as they are (no rules,
no fitting: ``Admission.raw``), so they show on the Requests page and count as usage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from . import thinking

QUESTION = (
    "A farmer has 17 sheep, sells 5, buys 23 more, then loses a third of them (rounded down). "
    "How many sheep are left? Answer with the number and one short sentence."
)
MAX_TOKENS = 2048  # room to think at the higher levels; the answer itself is a line
WAY_NAMES = {
    "openai": "reasoning_effort",
    "openrouter": "reasoning.effort",
    "vllm": "chat template switch",
    "refuseless": "reasoning_effort (Refuseless's levels)",
    "synthetic": "reasoning_effort (Synthetic's levels)",
}
# The levels tried each way: every one opencode can send, or the provider's own set
LEVELS = {
    "openai": thinking.EFFORTS,
    "openrouter": thinking.EFFORTS,
    "vllm": ("none", *thinking.VLLM_EFFORTS),
    "refuseless": thinking.REFUSELESS_EFFORTS,
    "synthetic": thinking.SYNTHETIC_EFFORTS,
}
GUESSES = {"openai": thinking.EFFORTS, "openrouter": ("none", "high"), "vllm": ("none", "high")}  # an unknown API
UNTESTABLE = {
    "claude": "Claude's levels come from its own model list (capabilities.effort): there's nothing to find out.",
    "none": "Thinking levels aren't sent to this upstream (Upstreams page: Thinking levels are sent).",
}


@dataclass(frozen=True)
class Probe:
    way: str  # how the level is asked (WAY_NAMES); "" for the request with no level
    level: str  # the level asked; "" for the request with no level
    body: dict

    @property
    def sent(self) -> str:
        """What the request says about thinking, as sent."""
        said = {k: v for k, v in self.body.items() if k not in ("model", "messages", "max_tokens")}
        return json.dumps(said, separators=(", ", ": ")) if said else "nothing"


@dataclass(frozen=True)
class Result:
    probe: Probe
    status: int
    error: str = ""  # the provider's words, when it didn't answer
    thinking_chars: int = 0  # of the reasoning it returned
    thinking_tokens: int | None = None  # what its usage says it spent thinking
    output_tokens: int | None = None

    @property
    def accepted(self) -> bool:
        return self.status == 200

    @property
    def refused(self) -> bool:
        """Turned down for what it asked (not busy, not broken)."""
        return self.status in (400, 404, 422)

    @property
    def thought(self) -> bool:
        return self.accepted and (self.thinking_chars > 0 or (self.thinking_tokens or 0) > 0)


def plan(model: str, style: str) -> list[Probe]:
    """The requests of a test for ``model`` on an upstream asked the ``style`` way (thinking.style_of): one with no
    level, then one per level; [] for a style there's nothing to test on (UNTESTABLE)."""
    if style in UNTESTABLE:
        return []
    base = {"model": model, "messages": [{"role": "user", "content": QUESTION}], "max_tokens": MAX_TOKENS}
    ways = {style: LEVELS[style]} if style in LEVELS else GUESSES
    probes = [Probe("", "", base)]
    for way, levels in ways.items():
        for level in levels:
            body = {**base, "reasoning_effort": level}
            thinking.translate(body, way)
            probes.append(Probe(way, level, body))
    return probes


def read(probe: Probe, status: int, content: bytes) -> Result:
    """What came back for a probe."""
    try:
        answer = json.loads(content)
    except ValueError:
        answer = None
    if not isinstance(answer, dict):
        answer = {}
    if status != 200:
        error = answer.get("error")
        message = error.get("message") if isinstance(error, dict) else error if isinstance(error, str) else None
        message = message or answer.get("message") or content[:300].decode("utf-8", "replace") or "no error text"
        return Result(probe, status, " ".join(str(message).split())[:300])
    choices = answer.get("choices") if isinstance(answer.get("choices"), list) else []
    message = choices[0].get("message") if choices and isinstance(choices[0], dict) else None
    message = message if isinstance(message, dict) else {}
    reasoning = message.get("reasoning_content") or message.get("reasoning")
    usage = answer.get("usage") if isinstance(answer.get("usage"), dict) else {}
    details = usage.get("completion_tokens_details")
    spent = details.get("reasoning_tokens") if isinstance(details, dict) else None
    return Result(
        probe,
        status,
        thinking_chars=len(reasoning) if isinstance(reasoning, str) else 0,
        thinking_tokens=spent if isinstance(spent, int) else None,
        output_tokens=usage.get("completion_tokens") if isinstance(usage.get("completion_tokens"), int) else None,
    )


def _controls(results: list[Result], plain: Result | None) -> str:
    """Whether a way's results show the level steering the model: "yes" (off gave no thinking where there's
    thinking otherwise), "no" (off still thought), "" (can't tell: nothing thought, or off wasn't answered)."""
    off = next((r for r in results if r.probe.level == "none"), None)
    others = [r for r in results if r.probe.level != "none"]
    thinks = any(r.thought for r in others) or bool(plain and plain.thought)
    if off is None or not off.accepted or not thinks:
        return ""
    return "no" if off.thought else "yes"


def verdict(results: list[Result], style: str) -> dict:
    """What a test found: per way, the levels answered and refused and whether off works; which way to send levels
    (for an API QueueRouter doesn't know); the levels to save for the model; and what it all means, in sentences."""
    plain = next((r for r in results if not r.probe.way), None)
    ways = []
    for way in dict.fromkeys(r.probe.way for r in results if r.probe.way):
        mine = [r for r in results if r.probe.way == way]
        ways.append(
            {
                "way": way,
                "name": WAY_NAMES[way],
                "accepted": [r.probe.level for r in mine if r.accepted],
                "refused": [r.probe.level for r in mine if r.refused],
                "failed": [r.probe.level for r in mine if not r.accepted and not r.refused],
                "controls": _controls(mine, plain),
                "thinks": any(r.thought for r in mine),
            }
        )
    notes: list[str] = []
    guessed = style not in LEVELS
    best = next((w for w in ways if w["controls"] == "yes"), None)
    if best is None and not guessed and ways:
        best = ways[0]
    suggested: tuple[str, ...] = ()
    if plain is not None and not plain.accepted:
        notes.append(
            f"The model didn't answer a plain request ({plain.status}: {plain.error}), so nothing can be said."
        )
        best = None
    elif not any(r.thought for r in results):
        notes.append(
            "No thinking came back at any level: the model doesn't think, or the provider doesn't return or count it."
        )
        best = None if guessed else best
    if best is not None:
        levels = [lv for lv in best["accepted"] if lv != "none" or best["controls"] != "no"]
        suggested = thinking.ordered(levels)
        if best["refused"]:
            notes.append(f"Refused: {', '.join(best['refused'])}. It takes: {', '.join(best['accepted']) or 'none'}.")
        elif best["accepted"]:
            notes.append(
                "Every level was accepted. That alone doesn't prove each is honoured: compare how much it thought."
            )
        if best["controls"] == "yes":
            notes.append('"none" turns its thinking off.')
        elif best["controls"] == "no":
            notes.append("\"none\" was accepted but it still thought: off doesn't work here, so it's left out.")
        if guessed:
            notes.append(f"This API honours levels sent as {best['name']}: choose that on the Upstreams page.")
            if len(GUESSES[best["way"]]) < len(thinking.EFFORTS):
                notes.append("Only off and high were tried that way: choose it, then test again for every level.")
    elif guessed and any(r.thought for r in results):
        notes.append("It thinks, but no way of asking turned that off or was refused: the level looks ignored here.")
    if any(w["failed"] for w in ways):
        notes.append("Some requests failed for another reason (busy, an error): test again for those levels.")
    return {
        "ways": ways,
        "way": best["way"] if best is not None and guessed else "",
        "levels": ", ".join(suggested),
        "notes": notes,
    }
