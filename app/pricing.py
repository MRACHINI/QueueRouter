"""Reference prices: what the tokens would have cost at official per-token API prices.

ArliAI is flat-rate; these numbers show the value you get. Prices come from models.dev (the
catalog opencode uses), matched by model name, with manual overrides from the admin UI:

1. manual input/output prices for the model, else
2. a manually chosen models.dev reference (``provider/model``), else
3. automatic match: the model creator's own API price ("official"), or the median across
   providers when the creator doesn't sell it (open-weight models).

The same catalog gives output sizes for models whose provider's list doesn't say (``PriceCatalog.output``):
that provider's own figure when models.dev lists the provider, else the creator's, else the median.
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

import httpx

from . import __version__
from .names import EVERY, ModelNames

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
# Providers models.dev lists without an API address (apps use their own SDKs), by the host of their API.
OWN_API_HOSTS = {
    "api.anthropic.com": "anthropic",
    "api.openai.com": "openai",
    "api.groq.com": "groq",
    "generativelanguage.googleapis.com": "google",
    "api.mistral.ai": "mistral",
    "api.deepinfra.com": "deepinfra",
}


@dataclass(frozen=True)
class Price:
    """USD per million tokens."""

    input: float
    output: float
    cached_input: float
    source: str  # human-readable: "manual", "official (deepseek)", "median of 7 providers", ...
    reference: str | None = None  # models.dev "provider/model" when it came from there
    writes_cache: bool = False  # on an upstream that bills cache writes (Claude): what's new in a prompt costs more


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
    """models.dev's prices, and its output sizes (``outputs``) with each provider's API address (``apis``)."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.prices: dict[str, list[float]] = {}  # "provider/model" -> [input, output, cache_read]
        self.index: dict[str, list[str]] = {}
        self.outputs: dict[str, int] = {}  # "provider/model" -> the longest answer, where below its context
        self.output_index: dict[str, list[str]] = {}
        self.apis: dict[str, str] = {}  # provider -> its OpenAI-compatible address
        self._found: dict[tuple, Any] = {}  # output() and provider_of(), worked out once per catalog
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

    @staticmethod
    def slim_sizes(raw: dict) -> dict:
        """Output sizes (only those below the model's context: one as big as the context says nothing) and the
        providers' API addresses."""
        outputs: dict[str, int] = {}
        apis: dict[str, str] = {}
        for provider_id, provider in raw.items():
            if isinstance(provider.get("api"), str):
                apis[provider_id] = provider["api"]
            for model_id, model in (provider.get("models") or {}).items():
                limit = model.get("limit") if isinstance(model.get("limit"), dict) else {}
                output, context = limit.get("output"), limit.get("context")
                if isinstance(output, int) and output > 0 and not (isinstance(context, int) and output >= context):
                    outputs[f"{provider_id}/{model_id}"] = output
        return {"outputs": outputs, "apis": apis}

    def use(self, prices: dict[str, list[float]], fetched_at: float, sizes: dict | None = None) -> None:
        self.prices, self.fetched_at = prices, fetched_at
        index: dict[str, list[str]] = {}
        for ref in prices:
            index.setdefault(_norm(ref.split("/", 1)[1]), []).append(ref)
        self.index = index
        if sizes is not None:
            self.outputs, self.apis = dict(sizes.get("outputs") or {}), dict(sizes.get("apis") or {})
            output_index: dict[str, list[str]] = {}
            for ref in self.outputs:
                output_index.setdefault(_norm(ref.split("/", 1)[1]), []).append(ref)
            self.output_index = output_index
        self._found = {}

    def load_file(self) -> bool:
        if not self.path or not os.path.exists(self.path):
            return False
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            # a file from before output sizes were kept: used, and fetched again soon (counts as stale)
            fetched_at = float(data["fetched_at"]) if "sizes" in data else 0.0
            self.use(data["prices"], fetched_at, data.get("sizes"))
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
            raw = response.json()
            prices, sizes = self.slim(raw), self.slim_sizes(raw)
        except (httpx.HTTPError, ValueError, AttributeError) as exc:
            log.warning(json.dumps({"event": "price_catalog_refresh_failed", "error": repr(exc)[:200]}))
            return False
        if not prices:
            return False
        self.use(prices, time.time(), sizes)
        if self.path:
            self._save()
        log.info(json.dumps({"event": "price_catalog_refreshed", "models": len(prices)}))
        return True

    def _save(self) -> None:
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".prices-", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            sizes = {"outputs": self.outputs, "apis": self.apis}
            json.dump({"fetched_at": self.fetched_at, "prices": self.prices, "sizes": sizes}, f)
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

    def provider_of(self, base_url: str) -> str | None:
        """The models.dev provider an upstream's address belongs to: the one with exactly that API address, else the
        only one on that host, else one models.dev lists without an address (it has its own API)."""
        key = ("provider", base_url)
        if key not in self._found:
            parts = urllib.parse.urlsplit(base_url)
            host, path = (parts.hostname or "").lower(), parts.path.rstrip("/")
            same = [
                (provider, urllib.parse.urlsplit(api).path.rstrip("/"))
                for provider, api in self.apis.items()
                if (urllib.parse.urlsplit(api).hostname or "").lower() == host
            ]
            exact = [provider for provider, api_path in same if api_path == path]
            if len(exact) == 1:
                self._found[key] = exact[0]
            elif len(same) == 1:
                self._found[key] = same[0][0]
            else:
                self._found[key] = OWN_API_HOSTS.get(host) if not same else None
        return self._found[key]

    def output(self, model_id: str, provider: str | None = None, name: str | None = None) -> tuple[int, str] | None:
        """The longest answer models.dev gives for a model, and whose figure it is: that provider's own for the
        model, else (by its custom ``name`` if it has one) the creator's, else the median of the providers stating
        one. None when models.dev has none."""
        key = ("output", model_id, provider, name)
        if key not in self._found:
            self._found[key] = self._output(model_id, provider, name)
        return self._found[key]

    def _output(self, model_id: str, provider: str | None, name: str | None) -> tuple[int, str] | None:
        if provider:
            if (own := self.outputs.get(f"{provider}/{model_id}")) is not None:
                return own, provider
            for k in candidate_keys(model_id):
                mine = [r for r in self.output_index.get(k, []) if r.split("/", 1)[0] == provider]
                if mine:
                    return self.outputs[min(mine, key=len)], provider
        variants = [self.output_index.get(k, []) for k in candidate_keys(name or model_id)]
        for maker in OFFICIAL:
            for refs in variants:
                official = [r for r in refs if r.split("/", 1)[0] == maker]
                if official:
                    return self.outputs[min(official, key=len)], maker
        for refs in variants:
            if refs:
                size = int(statistics.median(self.outputs[r] for r in refs))
                return size, f"median of {len(refs)} provider{'s' if len(refs) > 1 else ''}"
        return None

    def references(self, limit_to_official: bool = True) -> list[str]:
        """Choices for the manual reference picker."""
        refs = sorted(r for r in self.prices if _usable(self.prices[r]))
        if limit_to_official:
            refs = [r for r in refs if r.split("/", 1)[0] in OFFICIAL]
        return refs


def resolve(
    model: str, overrides: dict, catalog: PriceCatalog, upstream_id: int = EVERY, name: str | None = None
) -> Price | None:
    """The price to use for ``model`` on that upstream: manual price > manual reference > automatic match, by its
    custom ``name`` if it has one. ``overrides``: {(upstream id, model): row}; yours for this provider wins over
    yours for every provider."""
    for key in ((upstream_id, model), (EVERY, model)):
        row = overrides.get(key)
        if row is None:
            continue
        if row["input"] is not None and row["output"] is not None:
            cached = row["cached_input"] if row["cached_input"] is not None else row["input"]
            return Price(row["input"], row["output"], cached, source="manual")
        if row["reference"]:
            return catalog.lookup(row["reference"])
    return catalog.match(name or model)


class _PriceBook:
    """Prices of (model, upstream) pairs, each worked out once."""

    def __init__(self, overrides: dict, catalog: PriceCatalog, names: ModelNames) -> None:
        self.overrides, self.catalog, self.names = overrides, catalog, names
        self.prices: dict[tuple[str, str | None], Price | None] = {}

    def of(self, model: str, upstream: str | None) -> Price | None:
        key = (model, upstream)
        if key not in self.prices:
            upstream_id, name = self.names.id_of(upstream), self.names.name(model, upstream)
            self.prices[key] = resolve(model, self.overrides, self.catalog, upstream_id, name)
        return self.prices[key]

    def costs(self, row) -> tuple[float, float, float] | None:
        """What a usage or request row cost: the model that answered, on the upstream that served it."""
        target = row["target_model"] if "target_model" in row.keys() else None
        price = self.of(self.names.unpin(target or row["model"])[0], row["upstream"])
        return token_costs(
            price, row["prompt_tokens"], row["completion_tokens"], row["cached_tokens"], row["cache_write_tokens"]
        )


def token_costs(
    price: Price | None, prompt: int, completion: int, cached: int, written: int = 0
) -> tuple[float, float, float] | None:
    """(normal input, cached input, output) in USD; cached prompt tokens at the cheaper cached rate. Tokens written
    to the cache are normal input at a higher rate (providers report them where writes cost extra)."""
    if price is None:
        return None
    cached = min(cached or 0, prompt or 0)
    written = min(written or 0, (prompt or 0) - cached)
    normal = ((prompt or 0) - cached + written * (CACHE_WRITE_RATE - 1)) * price.input / 1e6
    return normal, cached * price.cached_input / 1e6, (completion or 0) * price.output / 1e6


def token_cost(
    price: Price | None, prompt: int, completion: int, cached: int, written: int = 0
) -> tuple[float, float] | None:
    """(input cost, output cost) in USD."""
    costs = token_costs(price, prompt, completion, cached, written)
    return None if costs is None else (costs[0] + costs[1], costs[2])


CACHE_WRITE_RATE = 1.25  # Claude: what a write to its 5-minute prompt cache costs, in input prices
PRICE_PER_MONTH_DAYS = 30.4375
MIX_DAYS = 30  # usage that decides the token mix below
MIN_MIX_TOKENS = 1_000_000  # less says little: DEFAULT_MIX is used
DEFAULT_MIX = (0.20, 0.79, 0.01)  # shares of normal input, cached input and output tokens, typical of coding agents


def token_mix(prompt: int, cached: int, completion: int) -> tuple[float, float, float]:
    """(normal input, cached input, output) shares of all tokens used."""
    total = prompt + completion
    if total < MIN_MIX_TOKENS:
        return DEFAULT_MIX
    cached = min(cached, prompt)
    return (prompt - cached) / total, cached / total, completion / total


def blended_price(price: Price, mix: tuple[float, float, float]) -> float:
    """USD per million tokens of that mix: one figure that compares models for the work actually done. An agent's
    tokens are mostly cached input, so input and output prices alone would mislead. Where cache writes are billed,
    nearly all normal input is a write: it's what the next request will read."""
    normal = price.input * (CACHE_WRITE_RATE if price.writes_cache else 1)
    return mix[0] * normal + mix[1] * price.cached_input + mix[2] * price.output


def price_text(price: Price | None, mix: tuple[float, float, float]) -> str:
    """The price in an opencode name: ``$0.203``, ``plan`` (a monthly plan: nothing extra per request), ``$?``."""
    if price is None:
        return "$?"
    return "plan" if price.source == "plan" else f"${blended_price(price, mix):.3f}"


def _empty_usage() -> dict:
    return {
        "requests": 0,
        "errors": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "cached_tokens": 0,
        "cache_write_tokens": 0,  # among input_tokens
        "input_tokens": 0,  # prompt tokens that weren't cached
        "cost_in": 0.0,
        "cost_cached": 0.0,
        "cost_out": 0.0,
        "cost": 0.0,
        "priced": True,
        "unpriced_tokens": 0,
    }


def _add_usage(out: dict, row, costs: tuple[float, float, float] | None) -> None:
    for field in ("requests", "errors", "prompt_tokens", "completion_tokens", "cached_tokens", "cache_write_tokens"):
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


def summarize_usage(
    rows,
    overrides: dict,
    catalog: PriceCatalog,
    by_model: bool,
    plan_per_month: float,
    days: int,
    names: ModelNames | None = None,
):
    """Usage rows (per day/client/model/upstream) -> display rows with costs, plus totals.

    Cost is only "known" for rows whose model has a price; ``unpriced_tokens`` says how many
    tokens are missing from the total, so it's never silently low.
    """
    names = names or ModelNames()
    book = _PriceBook(overrides, catalog, names)
    grouped: dict[tuple, dict] = {}
    totals = _empty_usage()
    for row in rows:
        model, upstream = row["model"], row["upstream"]
        costs = book.costs(row)
        key = (row["day"], row["client"], upstream, model) if by_model else (row["day"], row["client"])
        fresh = {
            "day": row["day"],
            "client": row["client"],
            "model": model if by_model else None,
            "upstream": upstream if by_model else None,
            "label": names.label(model, upstream) if by_model else None,
        }
        _add_usage(grouped.setdefault(key, {**fresh, **_empty_usage()}), row, costs)
        _add_usage(totals, row, costs)
    totals["plan_cost"] = plan_per_month * days / PRICE_PER_MONTH_DAYS
    totals["savings"] = totals["cost"] - totals["plan_cost"]
    return list(grouped.values()), totals, book.prices


def model_costs(rows, overrides: dict, catalog: PriceCatalog, names: ModelNames | None = None):
    """Usage rows -> one row per model on a provider, dearest first, plus their total."""
    names = names or ModelNames()
    book = _PriceBook(overrides, catalog, names)
    grouped: dict[tuple[str, str], dict] = {}
    total = _empty_usage()
    for row in rows:
        model, upstream = row["model"], row["upstream"]
        costs = book.costs(row)
        fresh = {"model": model, "upstream": upstream, "label": names.label(model, upstream)}
        _add_usage(grouped.setdefault((upstream, model), {**fresh, **_empty_usage()}), row, costs)
        _add_usage(total, row, costs)
    return sorted(grouped.values(), key=lambda r: (-r["cost"], -r["prompt_tokens"], r["label"])), total


def request_costs(rows, calls, overrides: dict, catalog: PriceCatalog, names: ModelNames | None = None) -> list[dict]:
    """Request rows -> dicts with their label, their tokens split and what each part cost, at today's prices. A council
    has no model of its own to price: it adds up ``calls``, the rows of its members' and judge's calls (their
    ``parent``)."""
    names = names or ModelNames()
    book = _PriceBook(overrides, catalog, names)
    by_parent: dict[str, list] = {}
    for call in calls:
        by_parent.setdefault(call["parent"], []).append(call)
    out = []
    for row in rows:
        made = by_parent.get(row["id"], []) if row["council"] else []
        used = _empty_usage()
        for part in made or [row]:
            _add_usage(used, {**dict(part), "requests": 0, "errors": 0}, book.costs(part))
        tokens = used["prompt_tokens"] or used["completion_tokens"]
        out.append(
            {
                **dict(row),
                "label": names.of_request(row["model"], row["target_model"], row["upstream"]),
                "calls": len(made),
                "tokens_in": used["input_tokens"] if tokens else None,
                "tokens_cached": used["cached_tokens"] if tokens else None,
                "tokens_written": used["cache_write_tokens"] if tokens else None,
                "tokens_out": used["completion_tokens"] if tokens else None,
                "cost_in": used["cost_in"],
                "cost_cached": used["cost_cached"],
                "cost_out": used["cost_out"],
                "cost": used["cost"] if tokens else None,
                "priced": used["priced"],
            }
        )
    return out
