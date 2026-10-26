"""Model strength, for ordering the opencode model list (strongest first).

The scores come from Artificial Analysis (its free API: an Intelligence, Coding and Agentic index per model),
fetched once a day with the administrator's free key, or from scores the administrator sets per model on the
Models page (they win). Provider names rarely equal the index's names, so both are reduced to a bare form:
organisation prefixes, fine-tune tags ("Derestricted", "Abliterated"...), precision / quantisation tags,
trailing versions and dates are dropped, then the longest index name the model's name starts with is used.
That way ``GLM-4.6-Derestricted-v5`` gets GLM-4.6's score (shown as inherited), while GLM-4.6V doesn't.

opencode's desktop app sorts a provider's models by name, so the order is carried in the names: ``01 · …``,
``02 · …``; aliases (the entries picked on purpose) come first as ``00 · …``; models without a score last.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections.abc import Iterable

import httpx

log = logging.getLogger("queuerouter")

INDEX_URL = "https://artificialanalysis.ai/api/v2/language/models/free"
INDEXES = {
    "intelligence": "artificial_analysis_intelligence_index",
    "coding": "artificial_analysis_coding_index",
    "agentic": "artificial_analysis_agentic_index",
}
REFRESH_S = 86_400
MAX_PAGES = 20  # the free tier allows 100 requests a day
CREDIT = "Artificial Analysis"
CREDIT_URL = "https://artificialanalysis.ai/"

# Name parts that say how a model was tuned, packaged or served, not which model it is.
_TAGS = {
    "derestricted", "abliterated", "uncensored", "unslop", "heretic", "rp", "roleplay", "creative",
    "instruct", "chat", "it", "hf", "fp4", "fp8", "fp16", "bf16", "int4", "int8", "awq", "gptq", "gguf",
    "exl2", "mlx", "nvfp4", "w4a16", "w8a8", "dynamic", "quantized", "distill", "latest", "preview",
}  # fmt: skip
_VERSION = re.compile(r"v\d+(\.\d+)*|\d{4}|\d{6,8}")  # "v5", "1017", "2507", "20251017"


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def bare(model_id: str) -> tuple[str, bool]:
    """(the model's bare name, whether a tuning tag was dropped to get it). Trailing versions and dates go only
    at the end (they don't make it another model); tags anywhere."""
    name = model_id.rsplit("/", 1)[-1].replace("(", " ").replace(")", " ")
    tokens = [t for t in re.split(r"[-_\s:]+", name) if t]
    kept = [t for t in tokens if t.lower() not in _TAGS]
    tagged = len(kept) != len(tokens)
    while len(kept) > 1 and (_VERSION.fullmatch(kept[-1].lower()) or kept[-1].lower() == "free"):
        kept.pop()  # "-free" / ":free": the same model, offered for free
    return _norm("".join(kept)), tagged


def _index_keys(text: str) -> set[str]:
    """An index name and its plain form: "GLM-4.6 (Reasoning)" is GLM-4.6 too."""
    plain = re.sub(r"\([^)]*\)|\b(non-?)?(reasoning|thinking)\b", " ", text, flags=re.I)
    return {k for k in (_norm(text), _norm(plain)) if k}


class NameIndex:
    """Entries found by a model's name, loosely: exactly first (versions and dates kept), else the longest entry
    name the model's bare name starts with, at a word boundary (GLM-4 doesn't claim GLM-4.6)."""

    def __init__(self, entries: Iterable[tuple[Iterable[str], dict]] = ()) -> None:
        keys: dict[str, list[dict]] = {}
        for texts, entry in entries:
            for text in texts:
                for key in _index_keys(text or ""):
                    if entry not in keys.setdefault(key, []):
                        keys[key].append(entry)
        self._exact = keys
        self._keys = sorted(keys.items(), key=lambda x: -len(x[0]))

    def find(self, model_id: str) -> tuple[list[dict], bool] | None:
        """(the entries of the best name, inherited?) — inherited when the model is a variant (a fine-tune,
        another version) of what was found."""
        whole = _norm(model_id.rsplit("/", 1)[-1])
        if whole in self._exact:
            return self._exact[whole], False
        name, tagged = bare(model_id)
        if not name:
            return None
        for key, entries in self._keys:
            if name.startswith(key) and not name[len(key) : len(key) + 1].isdigit():
                return entries, tagged or name != key
        return None


class Scores:
    """The index, kept in a small JSON file in the data directory between restarts."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.models: list[dict] = []  # {"name", "slug", "creator", "scores": {index: value}}
        self.fetched_at = 0.0
        self.error = ""
        self.names = NameIndex()

    def use(self, models: list[dict], fetched_at: float) -> None:
        self.models, self.fetched_at = models, fetched_at
        self.names = NameIndex(((e.get("slug"), e.get("name")), e) for e in models)

    def load_file(self) -> bool:
        if not self.path or not os.path.exists(self.path):
            return False
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.use(list(data["models"]), float(data["fetched_at"]))
            return True
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log.warning(json.dumps({"event": "model_scores_unreadable", "error": repr(exc)[:200]}))
            return False

    @property
    def stale(self) -> bool:
        return time.time() - self.fetched_at > REFRESH_S

    async def refresh(self, client: httpx.AsyncClient, key: str) -> bool:
        """Fetch every page of the free model list; keep the last good copy on failure."""
        models: list[dict] = []
        try:
            for page in range(1, MAX_PAGES + 1):
                response = await client.get(
                    INDEX_URL,
                    params={"page": page},
                    headers={"x-api-key": key, "accept": "application/json"},
                    timeout=30,
                )
                if response.status_code != 200:
                    raise ValueError(f"HTTP {response.status_code}")
                body = response.json()
                for item in body.get("data") or []:
                    if not isinstance(item, dict):
                        continue
                    evaluations = item.get("evaluations") or {}
                    scores = {
                        name: float(value)
                        for name, field in INDEXES.items()
                        if isinstance(value := evaluations.get(field), int | float)
                    }
                    if scores:
                        creator = (item.get("model_creator") or {}).get("name") or ""
                        models.append(
                            {"name": str(item.get("name") or ""), "slug": str(item.get("slug") or ""),
                             "creator": str(creator), "scores": scores}
                        )  # fmt: skip
                if not (body.get("pagination") or {}).get("has_more"):
                    break
        except (httpx.HTTPError, ValueError, AttributeError) as exc:
            self.error = str(exc)[:200] if isinstance(exc, ValueError) else type(exc).__name__
            log.warning(json.dumps({"event": "model_scores_refresh_failed", "error": self.error}))
            return False
        if not models:
            self.error = "the index returned no models"
            return False
        self.error = ""
        self.use(models, time.time())
        if self.path:
            try:
                await asyncio.to_thread(self._save)
            except OSError as exc:  # kept in memory: it's read again tomorrow
                log.warning(json.dumps({"event": "model_scores_unsaved", "error": repr(exc)[:200]}))
        return True

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": self.fetched_at, "models": self.models}, f)
        os.replace(tmp, self.path)

    def score(self, model_id: str, index: str) -> tuple[float, str, bool] | None:
        """(score, the index's name for it, inherited?) or None. Several entries of one name (reasoning or
        not): the strongest."""
        found = self.names.find(model_id)
        scored = [e for e in (found[0] if found else []) if index in e["scores"]]
        if not scored:
            return None
        entry = max(scored, key=lambda e: e["scores"][index])
        return entry["scores"][index], entry["name"], found[1]


def numbered(ranked: list[tuple[str, float]], aliases: list[str], unranked: list[str]) -> list[tuple[str, str]]:
    """[(model or alias, its prefix)] in opencode order: aliases ("00"), the ranked strongest first (01, 02…,
    ties by name), then the rest without a number."""
    width = max(2, len(str(len(ranked))))
    order = [(a, "0" * width) for a in sorted(aliases)]
    for i, (model, _) in enumerate(sorted(ranked, key=lambda x: (-x[1], x[0])), 1):
        order.append((model, str(i).zfill(width)))
    order += [(m, "") for m in sorted(unranked)]
    return order
