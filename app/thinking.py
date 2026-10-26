"""Thinking levels ("reasoning effort"): which a model offers, the opencode selector for them, and how each
upstream is asked for one.

Where they come from, first that knows:
1. the administrator's own list for the model (Models page);
2. the upstream's own model list: OpenRouter's ``reasoning`` block (``supported_efforts``, ``default_effort``,
   ``mandatory``), ArliAI's ``"reasoning": true`` (thinking on / off);
3. OpenRouter's public model list as a reference for the same model elsewhere (no key; read once a day), matched
   by name like the strength index (app/ranking.py).

opencode sends the chosen level as ``reasoning_effort``. Providers differ, so it's translated per upstream:
OpenRouter takes ``reasoning: {effort}``; vLLM servers (ArliAI) switch thinking through the chat template and
know only low / medium / high; OpenAI-style APIs take ``reasoning_effort`` as it is; Claude's OpenAI-compatible
endpoint ignores it (no levels are offered there).

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
    known = {str(e).lower() for e in efforts or () if isinstance(e, str)}
    return tuple(e for e in EFFORTS if e in known)


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


def style_of(base_url: str, entry: dict | None) -> str:
    """How an upstream is asked for a level: openrouter | vllm | openai | none (ignored: no levels offered, the
    field is dropped) | fixed (no levels offered, requests passed on as they are) | unknown."""
    url = (base_url or "").lower()
    if "openrouter.ai" in url:
        return "openrouter"
    if "api.anthropic.com" in url:
        return "none"  # its OpenAI-compatible endpoint ignores reasoning_effort
    if "arliai.com" in url:
        # tested live (2026-10): its reasoning models think whatever is sent (chat-template switch, reasoning,
        # thinking, reasoning_effort, /no_think), and effort makes no steady difference: a selector would mislead
        return "fixed"
    if (entry or {}).get("engine") == "vllm":
        return "vllm"
    if "api.openai.com" in url or "generativelanguage.googleapis.com" in url or "api.groq.com" in url:
        return "openai"
    return "unknown"


def for_style(style: str, found: tuple[tuple[str, ...], str | None]) -> tuple[tuple[str, str], ...]:
    """The variants to offer on an upstream of ``style`` for a model with ``found`` levels."""
    efforts, _ = found
    if style == "vllm":  # on / off always; levels only as vLLM knows them
        middle = tuple(e for e in VLLM_EFFORTS if e in efforts or e == "high" and {"xhigh", "max"} & set(efforts))
        if len(middle) <= 1:
            return TOGGLE
        return (("none", "none"), *levels(middle))
    if efforts == ("none", "high"):
        return TOGGLE
    return levels(efforts)


def translate(payload: dict, style: str) -> bool:
    """Rewrite ``reasoning_effort`` for an upstream of ``style``; True when something changed."""
    effort = payload.get("reasoning_effort")
    if not isinstance(effort, str) or style in ("openai", "unknown", "fixed"):
        return False
    effort = effort.lower()
    if style == "none":
        payload.pop("reasoning_effort")
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
    ArliAI's ``vlm``, OpenRouter's ``architecture.input_modalities``, a plain ``input_modalities``."""
    entry = entry or {}
    if isinstance(entry.get("vlm"), bool):
        return entry["vlm"]
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
