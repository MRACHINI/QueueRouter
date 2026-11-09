"""Thinking levels ("reasoning effort"): which a model offers, the opencode selector for them, and how each
upstream is asked for one.

Where they come from, first that knows:
1. the administrator's own list for the model (Models page);
2. the upstream's own model list: OpenRouter's ``reasoning`` block (``supported_efforts``, ``default_effort``,
   ``mandatory``), ArliAI's ``"reasoning": true`` (thinking on / off), Refuseless's ``reasoning_efforts`` and
   ``reasoning_default`` (``off`` is ``none``);
3. OpenRouter's public model list as a reference for the same model elsewhere (no key; read once a day), matched
   by name like the strength index (app/ranking.py).

opencode sends the chosen level as ``reasoning_effort``. Providers differ, so it's translated per upstream:
OpenRouter takes ``reasoning: {effort}``; vLLM servers (ArliAI) switch thinking through the chat template and
know only low / medium / high; OpenAI-style APIs take ``reasoning_effort`` as it is; Refuseless takes its own
levels (``off``, ``low``, ``high``, ``max``; any other is refused); Synthetic takes low / medium / high and has no
off switch; Claude's own API (the Anthropic preset) takes its levels as ``output_config.effort`` (app/claude.py),
which its model list states. A level the model doesn't have (an app's own, one an alias's rule forces, Claude
Code's effort) becomes the ``nearest`` it has, from below: "max" is the model's highest, whatever that is.

The reference list also carries Artificial Analysis scores: the strength order uses them when there's no key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass

import httpx

from .ranking import NameIndex

log = logging.getLogger("queuerouter")

REFERENCE_URL = "https://openrouter.ai/api/v1/models"
REFRESH_S = 86_400
EFFORTS = ("none", "minimal", "low", "medium", "high", "xhigh", "max")  # what opencode can send, in order
VLLM_EFFORTS = ("low", "medium", "high")  # what vLLM accepts as reasoning_effort
SYNTHETIC_EFFORTS = ("low", "medium", "high")  # what Synthetic documents (no off switch)
REFUSELESS_EFFORTS = ("none", "low", "high", "max")  # Refuseless's own (sent as off / low / high / max)
CLAUDE_EFFORTS = ("low", "medium", "high", "xhigh", "max")  # Claude's output_config.effort
TOGGLE = (("off", "none"), ("thinking", "high"))  # an on / off model: two variants


@dataclass(frozen=True)
class Thinking:
    """A model's thinking control: ``variants`` [(name shown in opencode, reasoning_effort sent)]."""

    variants: tuple[tuple[str, str], ...]
    default: str | None = None
    source: str = ""  # "yours", the upstream's name, "OpenRouter (reference)"

    @property
    def summary(self) -> str:
        names = " · ".join(name for name, _ in self.variants) or "none"
        return names + (f" (default {self.default})" if self.default else "")


def ordered(efforts) -> tuple[str, ...]:
    """Levels in EFFORTS order; "off" (Refuseless) is "none"."""
    known = {str(e).lower() for e in efforts or () if isinstance(e, str)}
    known |= {"none"} if "off" in known else set()
    return tuple(e for e in EFFORTS if e in known)


def from_refuseless(entry: dict | None) -> tuple[tuple[str, ...], str | None] | None:
    """(efforts, default) from Refuseless's model list: ``reasoning_efforts`` and ``reasoning_default`` on each
    entry, "off" as "none"; None when it lists none."""
    entry = entry or {}
    efforts = ordered(entry.get("reasoning_efforts") if isinstance(entry.get("reasoning_efforts"), list) else ())
    if not efforts:
        return None
    default = entry.get("reasoning_default")
    default = ordered([default])[0] if isinstance(default, str) and ordered([default]) else None
    return efforts, default


def levels(efforts: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((e, e) for e in efforts)


def from_block(block, params=()) -> tuple[tuple[str, ...], str | None] | None:
    """(efforts, default) from an OpenRouter ``reasoning`` block; None when it says nothing usable. A model that
    can think but lists no levels gets on / off when thinking isn't mandatory."""
    if block is True:
        return ("none", "high"), None
    if not isinstance(block, dict):
        return None
    efforts = ordered(block.get("supported_efforts"))
    default = block.get("default_effort") if isinstance(block.get("default_effort"), str) else None
    if efforts:
        return efforts, default
    reasons = "reasoning" in (params or ()) or block.get("default_enabled") is not None
    if reasons and block.get("mandatory") is False:
        return ("none", "high"), None
    return None


def from_claude(entry: dict | None) -> tuple[tuple[str, ...], str | None] | None:
    """(efforts, default) from Claude's own model list (``capabilities.effort``: each level ``supported`` or not);
    None when it doesn't say."""
    capabilities = (entry or {}).get("capabilities")
    effort = capabilities.get("effort") if isinstance(capabilities, dict) else None
    if not isinstance(effort, dict) or effort.get("supported") is False:
        return None
    levels_ = tuple(e for e in CLAUDE_EFFORTS if isinstance(effort.get(e), dict) and effort[e].get("supported"))
    return (levels_, None) if levels_ else None


def nearest(offered, wanted) -> str | None:
    """The level of ``offered`` nearest ``wanted`` from below (the lowest when all are above): for an app that says
    how hard to think without picking one of the model's levels. Thinking is only turned off when that's asked."""
    rank = {e: i for i, e in enumerate(EFFORTS)}
    if wanted not in rank:
        return None
    known = sorted((e for e in offered if e in rank and (e != "none" or wanted == "none")), key=rank.__getitem__)
    below = [e for e in known if rank[e] <= rank[wanted]]
    return below[-1] if below else known[0] if known else None


PRESET_STYLES = {  # an upstream's provider (app/presets.py), for an address that doesn't say (a proxy in between)
    "openrouter": "openrouter",
    "arliai": "fixed",
    "openai": "openai",
    "gemini": "openai",
    "groq": "openai",
    "refuseless": "refuseless",
    "synthetic": "synthetic",
}


def style_of(base_url: str, entry: dict | None, preset: str | None = None) -> str:
    """How an upstream is asked for a level: openrouter | vllm | openai | refuseless | synthetic | none (ignored: no
    levels offered, the field is dropped) | fixed (no levels offered, requests passed on as they are) | unknown. By
    its address, else by its provider (``preset``)."""
    url = (base_url or "").lower()
    if "openrouter.ai" in url:
        return "openrouter"
    if "api.anthropic.com" in url:
        return "none"
    if "arliai.com" in url:
        # tested live (2026-10): its reasoning models think whatever is sent (chat-template switch, reasoning,
        # thinking, reasoning_effort, /no_think), and effort makes no steady difference: a selector would mislead
        return "fixed"
    if "refuseless.com" in url:
        return "refuseless"
    if "synthetic.new" in url:
        return "synthetic"
    if (entry or {}).get("engine") == "vllm":
        return "vllm"
    if "api.openai.com" in url or "generativelanguage.googleapis.com" in url or "api.groq.com" in url:
        return "openai"
    return PRESET_STYLES.get(preset or "", "unknown")


def for_style(style: str, found: tuple[tuple[str, ...], str | None]) -> tuple[tuple[str, str], ...]:
    """The variants to offer on an upstream of ``style`` for a model with ``found`` levels."""
    efforts, _ = found
    if style == "claude":  # its own levels only (thinking can't be turned off on the newest models): two at least
        own = tuple(e for e in efforts if e in CLAUDE_EFFORTS)
        return levels(own) if len(own) > 1 else ()
    if style == "vllm":  # on / off always; levels only as vLLM knows them
        middle = tuple(e for e in VLLM_EFFORTS if e in efforts or e == "high" and {"xhigh", "max"} & set(efforts))
        if len(middle) <= 1:
            return TOGGLE
        return (("none", "none"), *levels(middle))
    if style == "synthetic":  # levels only as Synthetic knows them, and no off switch
        middle = tuple(e for e in SYNTHETIC_EFFORTS if e in efforts or e == "high" and {"xhigh", "max"} & set(efforts))
        return levels(middle) if len(middle) > 1 else ()
    if style == "refuseless":  # its own levels (its model list states them)
        return levels(tuple(e for e in efforts if e in REFUSELESS_EFFORTS))
    if efforts == ("none", "high"):
        return TOGGLE
    return levels(efforts)


def translate(payload: dict, style: str) -> bool:
    """Rewrite ``reasoning_effort`` for an upstream of ``style``; True when something changed. A level the model
    doesn't have was lowered to one it has before this (main.build_request, ``nearest``); for Refuseless and
    Synthetic, whose models' levels may be unknown, a level they never take becomes the nearest one they do."""
    effort = payload.get("reasoning_effort")
    if not isinstance(effort, str) or style in ("openai", "unknown", "fixed"):
        return False
    effort = effort.lower()
    if style == "none":
        payload.pop("reasoning_effort")
        return True
    if style == "refuseless":  # off / low / high / max; any other is refused
        level = nearest(REFUSELESS_EFFORTS, effort)
        if level is None:
            payload.pop("reasoning_effort")
        else:
            payload["reasoning_effort"] = "off" if level == "none" else level
        return True
    if style == "synthetic":  # low / medium / high; no off switch: "none" asks for nothing (its default)
        level = nearest(SYNTHETIC_EFFORTS, effort) if effort != "none" else None
        if level is None:
            payload.pop("reasoning_effort")
        else:
            payload["reasoning_effort"] = level
        return True
    if style == "openrouter":
        payload.pop("reasoning_effort")
        reasoning = payload.get("reasoning") if isinstance(payload.get("reasoning"), dict) else {}
        payload["reasoning"] = {**reasoning, "effort": effort}
        return True
    # vllm: thinking on / off through the chat template (Qwen, GLM: enable_thinking; DeepSeek: thinking)
    on = effort != "none"
    kwargs = payload.get("chat_template_kwargs") if isinstance(payload.get("chat_template_kwargs"), dict) else {}
    payload["chat_template_kwargs"] = {**kwargs, "enable_thinking": on, "thinking": on}
    if on:
        payload["reasoning_effort"] = {"minimal": "low", "xhigh": "high", "max": "high"}.get(effort, effort)
        if payload["reasoning_effort"] not in VLLM_EFFORTS:
            payload.pop("reasoning_effort")
    else:
        payload.pop("reasoning_effort")
    return True


def takes_images(entry: dict | None) -> bool | None:
    """Whether a model list entry says the model reads images (True / False), or None when it doesn't say:
    ArliAI's ``vlm``, Refuseless's ``vision``, OpenRouter's ``architecture.input_modalities``, a plain
    ``input_modalities``."""
    entry = entry or {}
    for key in ("vlm", "vision"):
        if isinstance(entry.get(key), bool):
            return entry[key]
    for holder in (entry.get("architecture"), entry):
        modalities = holder.get("input_modalities") if isinstance(holder, dict) else None
        if isinstance(modalities, list):
            return "image" in modalities
    return None


class Reference:
    """OpenRouter's public model list, slimmed: thinking levels and Artificial Analysis scores per model."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.models: list[dict] = []  # {"id", "name", "hf", "reasoning", "params", "scores"}
        self.fetched_at = 0.0
        self.error = ""
        self.names = NameIndex()

    @staticmethod
    def slim(items) -> list[dict]:
        models = []
        for item in items or []:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                continue
            aa = (
                ((item.get("benchmarks") or {}).get("artificial_analysis") or {})
                if isinstance(item.get("benchmarks"), dict)
                else {}
            )
            scores = {
                name: float(value)
                for name in ("intelligence", "coding", "agentic")
                if isinstance(value := aa.get(f"{name}_index"), int | float)
            }
            params = item.get("supported_parameters")
            models.append(
                {
                    "id": item["id"],
                    "name": str(item.get("name") or "").split(": ", 1)[-1],  # "DeepSeek: DeepSeek V4.1 Flash"
                    "hf": str(item.get("hugging_face_id") or ""),
                    "reasoning": item.get("reasoning") if isinstance(item.get("reasoning"), dict) else None,
                    "params": [p for p in params if isinstance(p, str)] if isinstance(params, list) else [],
                    "images": takes_images(item),
                    "scores": scores,
                }
            )
        return models

    def use(self, models: list[dict], fetched_at: float) -> None:
        self.models, self.fetched_at = models, fetched_at
        # by the name without its organisation ("deepseek/…", "deepseek-ai/…") as well as the display name
        self.names = NameIndex(((m["id"].rsplit("/", 1)[-1], m["hf"].rsplit("/", 1)[-1], m["name"]), m) for m in models)

    def load_file(self) -> bool:
        if not self.path or not os.path.exists(self.path):
            return False
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.use(list(data["models"]), float(data["fetched_at"]))
            if self.models and "images" not in self.models[0]:  # saved by an older version: read it again soon
                self.fetched_at = 0.0
            return True
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log.warning(json.dumps({"event": "reference_models_unreadable", "error": repr(exc)[:200]}))
            return False

    @property
    def stale(self) -> bool:
        return time.time() - self.fetched_at > REFRESH_S

    async def refresh(self, client: httpx.AsyncClient) -> bool:
        try:
            response = await client.get(REFERENCE_URL, headers={"accept": "application/json"}, timeout=60)
            response.raise_for_status()
            models = self.slim(response.json().get("data"))
        except (httpx.HTTPError, ValueError, AttributeError) as exc:
            self.error = type(exc).__name__
            log.warning(json.dumps({"event": "reference_models_refresh_failed", "error": self.error}))
            return False
        if not models:
            self.error = "the list was empty"
            return False
        self.error = ""
        self.use(models, time.time())
        if self.path:
            try:
                await asyncio.to_thread(self._save)
            except OSError as exc:
                log.warning(json.dumps({"event": "reference_models_unsaved", "error": repr(exc)[:200]}))
        return True

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": self.fetched_at, "models": self.models}, f)
        os.replace(tmp, self.path)

    def thinking(self, model_id: str) -> tuple[tuple[str, ...], str | None] | None:
        """Levels for a model by name (the strongest-matching entry that has any)."""
        found = self.names.find(model_id)
        for entry in found[0] if found else []:
            if (levels_ := from_block(entry["reasoning"], entry["params"])) is not None:
                return levels_
        return None

    def images(self, model_id: str) -> bool | None:
        """Whether a model reads images, by name (None: not in the list)."""
        found = self.names.find(model_id)
        said = [m.get("images") for m in (found[0] if found else [])]
        return True if True in said else False if False in said else None

    def index_entries(self) -> list[dict]:
        """The Artificial Analysis scores in the strength index's own shape (app/ranking.py Scores)."""
        return [
            {"name": m["name"], "slug": m["id"].rsplit("/", 1)[-1], "creator": "", "scores": m["scores"]}
            for m in self.models
            if m["scores"]
        ]
