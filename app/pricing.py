"""Reference prices: what the tokens would have cost at official per-token API prices.

ArliAI is flat-rate; these numbers show the value you get. Prices come from models.dev (the
catalog opencode uses), matched by model name, with manual overrides from the admin UI:

1. manual input/output prices for the model, else
2. a manually chosen models.dev reference (``provider/model``), else
3. automatic match: the model creator's own API price ("official"), or the median across
   providers when the creator doesn't sell it (open-weight models).
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
import tempfile
import time
from dataclasses import dataclass

import httpx

from . import __version__

log = logging.getLogger("queuerouter")

CATALOG_URL = "https://models.dev/api.json"
USER_AGENT = f"QueueRouter/{__version__} (+https://github.com/MRACHINI/QueueRouter)"  # default UAs get 403
REFRESH_S = 86_400

# First-party providers in models.dev, i.e. the companies that make the models. A model sold by
# its creator is priced at the creator's rate; otherwise the median of resellers is used.
OFFICIAL = (
    "deepseek",
    "openai",
    "anthropic",
    "google",
    "mistral",
    "xai",
    "zai",
    "zhipuai",
    "moonshotai",
    "alibaba",
    "minimax",
    "cohere",
    "ai21",
    "meta",
    "llama",
    "nvidia",
    "inception",
    "perplexity",
)
_DATE_SUFFIX = re.compile(r"[-_:](\d{4}|\d{6}|\d{8}|\d{4}-\d{2}-\d{2})$")


@dataclass(frozen=True)
class Price:
    """USD per million tokens."""

    input: float
    output: float
    cached_input: float
    source: str  # human-readable: "manual", "official (deepseek)", "median of 7 providers", ...
    reference: str | None = None  # models.dev "provider/model" when it came from there


def _norm(model_id: str) -> str:
    return re.sub(r"[^a-z0-9]", "", model_id.rsplit("/", 1)[-1].lower())


def candidate_keys(model_id: str) -> list[str]:
    """Normalized lookup keys: the full name, then without a date/version suffix (-0731...)."""
    base = model_id.rsplit("/", 1)[-1].lower()
    keys = [_norm(base)]
    stripped = _DATE_SUFFIX.sub("", base)
    if stripped != base:
        keys.append(_norm(stripped))
    return keys


def _usable(cost: list[float]) -> bool:
    return cost[0] > 0 or cost[1] > 0  # coding/token "plans" list everything at 0


class PriceCatalog:
    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.prices: dict[str, list[float]] = {}  # "provider/model" -> [input, output, cache_read]
        self.index: dict[str, list[str]] = {}
        self.fetched_at = 0.0

    # -- loading ---------------------------------------------------------------------------

    @staticmethod
    def slim(raw: dict) -> dict[str, list[float]]:
        """Keep only what we need from the ~6 MB catalog: per-token prices."""
        prices: dict[str, list[float]] = {}
        for provider_id, provider in raw.items():
            for model_id, model in (provider.get("models") or {}).items():
                cost = model.get("cost") or {}
                if not isinstance(cost.get("input"), int | float) or not isinstance(cost.get("output"), int | float):
                    continue
                cached = cost.get("cache_read")
                prices[f"{provider_id}/{model_id}"] = [
                    float(cost["input"]),
                    float(cost["output"]),
                    float(cached) if isinstance(cached, int | float) else float(cost["input"]),
                ]
        return prices

    def use(self, prices: dict[str, list[float]], fetched_at: float) -> None:
        self.prices, self.fetched_at = prices, fetched_at
        index: dict[str, list[str]] = {}
        for ref in prices:
            index.setdefault(_norm(ref.split("/", 1)[1]), []).append(ref)
        self.index = index

    def load_file(self) -> bool:
        if not self.path or not os.path.exists(self.path):
            return False
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.use(data["prices"], float(data["fetched_at"]))
            return True
        except (OSError, ValueError, KeyError) as exc:
            log.warning(json.dumps({"event": "price_catalog_unreadable", "error": repr(exc)[:200]}))
            return False

    @property
    def stale(self) -> bool:
        return time.time() - self.fetched_at > REFRESH_S

    async def refresh(self, client: httpx.AsyncClient) -> bool:
        try:
            response = await client.get(CATALOG_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
            response.raise_for_status()
            prices = self.slim(response.json())
        except (httpx.HTTPError, ValueError) as exc:
            log.warning(json.dumps({"event": "price_catalog_refresh_failed", "error": repr(exc)[:200]}))
            return False
        if not prices:
            return False
        self.use(prices, time.time())
        if self.path:
            self._save()
        log.info(json.dumps({"event": "price_catalog_refreshed", "models": len(prices)}))
        return True

    def _save(self) -> None:
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".prices-", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"fetched_at": self.fetched_at, "prices": self.prices}, f)
        os.replace(tmp, self.path)

    # -- lookups ---------------------------------------------------------------------------

    def lookup(self, reference: str) -> Price | None:
        cost = self.prices.get(reference)
        if cost is None:
            return None
        return Price(*cost, source=f"models.dev ({reference})", reference=reference)

    def match(self, model_id: str) -> Price | None:
        # Candidates for each name variant, most specific first ("...-0731", then without it).
        variants = [[r for r in self.index.get(k, []) if _usable(self.prices[r])] for k in candidate_keys(model_id)]
        # The creator's own price wins over any reseller, across all variants: resellers often list
        # dated ids ("deepseek-v4-flash-0731") while the creator lists the plain one.
        for provider in OFFICIAL:
            for refs in variants:
                official = [r for r in refs if r.split("/", 1)[0] == provider]
                if official:
                    ref = min(official, key=len)  # the plainest id, e.g. not "...-vision-exp"
                    return Price(*self.prices[ref], source=f"official ({provider})", reference=ref)
        for refs in variants:
            if not refs:
                continue
            costs = [self.prices[r] for r in refs]
            return Price(
                statistics.median(c[0] for c in costs),
                statistics.median(c[1] for c in costs),
                statistics.median(c[2] for c in costs),
                source=f"median of {len(refs)} provider{'s' if len(refs) > 1 else ''}",
            )
        return None

    def references(self, limit_to_official: bool = True) -> list[str]:
        """Choices for the manual reference picker."""
        refs = sorted(r for r in self.prices if _usable(self.prices[r]))
        if limit_to_official:
            refs = [r for r in refs if r.split("/", 1)[0] in OFFICIAL]
        return refs


def resolve(model: str, overrides: dict, catalog: PriceCatalog) -> Price | None:
    """The price to use for ``model``: manual price > manual reference > automatic match."""
    row = overrides.get(model)
    if row is not None:
        if row["input"] is not None and row["output"] is not None:
            cached = row["cached_input"] if row["cached_input"] is not None else row["input"]
            return Price(row["input"], row["output"], cached, source="manual")
        if row["reference"]:
            return catalog.lookup(row["reference"])
    return catalog.match(model)


def token_costs(price: Price | None, prompt: int, completion: int, cached: int) -> tuple[float, float, float] | None:
    """(normal input, cached input, output) in USD; cached prompt tokens at the cheaper cached rate."""
    if price is None:
        return None
    cached = min(cached or 0, prompt or 0)
    normal = ((prompt or 0) - cached) * price.input / 1e6
    return normal, cached * price.cached_input / 1e6, (completion or 0) * price.output / 1e6


def token_cost(price: Price | None, prompt: int, completion: int, cached: int) -> tuple[float, float] | None:
    """(input cost, output cost) in USD."""
    costs = token_costs(price, prompt, completion, cached)
    return None if costs is None else (costs[0] + costs[1], costs[2])


PRICE_PER_MONTH_DAYS = 30.4375


def _empty_usage() -> dict:
    return {
        "requests": 0,
        "errors": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "cached_tokens": 0,
        "input_tokens": 0,  # prompt tokens that weren't cached
        "cost_in": 0.0,
        "cost_cached": 0.0,
        "cost_out": 0.0,
        "cost": 0.0,
        "priced": True,
        "unpriced_tokens": 0,
    }


def _add_usage(out: dict, row, costs: tuple[float, float, float] | None) -> None:
    for field in ("requests", "errors", "prompt_tokens", "completion_tokens", "cached_tokens"):
        out[field] += row[field] or 0
    out["input_tokens"] += max((row["prompt_tokens"] or 0) - (row["cached_tokens"] or 0), 0)
    if costs is None:
        out["priced"] = False
        out["unpriced_tokens"] += (row["prompt_tokens"] or 0) + (row["completion_tokens"] or 0)
    else:
        out["cost_in"] += costs[0]
        out["cost_cached"] += costs[1]
        out["cost_out"] += costs[2]
        out["cost"] += sum(costs)


def summarize_usage(rows, overrides: dict, catalog: PriceCatalog, by_model: bool, plan_per_month: float, days: int):
    """Usage rows (per day/client/model) -> display rows with costs, plus totals.

    Cost is only "known" for rows whose model has a price; ``unpriced_tokens`` says how many
    tokens are missing from the total, so it's never silently low.
    """
    prices: dict[str, Price | None] = {}
    grouped: dict[tuple, dict] = {}
    totals = _empty_usage()
    for row in rows:
        model = row["model"]
        if model not in prices:
            prices[model] = resolve(model, overrides, catalog)
        costs = token_costs(prices[model], row["prompt_tokens"], row["completion_tokens"], row["cached_tokens"])
        key = (row["day"], row["client"], model) if by_model else (row["day"], row["client"])
        fresh = {"day": row["day"], "client": row["client"], "model": model if by_model else None}
        _add_usage(grouped.setdefault(key, {**fresh, **_empty_usage()}), row, costs)
        _add_usage(totals, row, costs)
    totals["plan_cost"] = plan_per_month * days / PRICE_PER_MONTH_DAYS
    totals["savings"] = totals["cost"] - totals["plan_cost"]
    return list(grouped.values()), totals, prices


def model_costs(rows, overrides: dict, catalog: PriceCatalog) -> tuple[list[dict], dict]:
    """Usage rows -> one row per model, dearest first, plus their total."""
    prices: dict[str, Price | None] = {}
    grouped: dict[str, dict] = {}
    total = _empty_usage()
    for row in rows:
        model = row["model"]
        if model not in prices:
            prices[model] = resolve(model, overrides, catalog)
        costs = token_costs(prices[model], row["prompt_tokens"], row["completion_tokens"], row["cached_tokens"])
        _add_usage(grouped.setdefault(model, {"model": model, **_empty_usage()}), row, costs)
        _add_usage(total, row, costs)
    return sorted(grouped.values(), key=lambda r: (-r["cost"], -r["prompt_tokens"], r["model"])), total


def request_costs(rows, calls, overrides: dict, catalog: PriceCatalog) -> list[dict]:
    """Request rows -> dicts with their tokens split and what each part cost, at today's prices. A council has no
    model of its own to price: it adds up ``calls``, the rows of its members' and judge's calls (their ``parent``)."""
    prices: dict[str, Price | None] = {}
    by_parent: dict[str, list] = {}
    for call in calls:
        by_parent.setdefault(call["parent"], []).append(call)
    out = []
    for row in rows:
        made = by_parent.get(row["id"], []) if row["council"] else []
        used = _empty_usage()
        for part in made or [row]:
            model = part["target_model"] or part["model"]
            if model not in prices:
                prices[model] = resolve(model, overrides, catalog)
            costs = token_costs(prices[model], part["prompt_tokens"], part["completion_tokens"], part["cached_tokens"])
            _add_usage(used, {**dict(part), "requests": 0, "errors": 0}, costs)
        tokens = used["prompt_tokens"] or used["completion_tokens"]
        out.append(
            {
                **dict(row),
                "calls": len(made),
                "tokens_in": used["input_tokens"] if tokens else None,
                "tokens_cached": used["cached_tokens"] if tokens else None,
                "tokens_out": used["completion_tokens"] if tokens else None,
                "cost_in": used["cost_in"],
                "cost_cached": used["cost_cached"],
                "cost_out": used["cost_out"],
                "cost": used["cost"] if tokens else None,
                "priced": used["priced"],
            }
        )
    return out
