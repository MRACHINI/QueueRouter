"""What a model is called. A model on a provider (upstream) is one entry: the same ID on two providers is two entries,
each with its own optional custom name, price, score and limits, and is shown as ``Provider|name`` on QueueRouter's
pages and ``name|Provider`` in opencode (where the number, score and price come first).

A custom name is what people see and what a model's automatic score, price and thinking levels are looked up by. It's
also the ID apps know the model by (``PublicIds``): spaces as dashes, and ``@Provider`` added when more than one
provider offers it, so a request never goes to a provider the app didn't pick. The real ID stays what's stored and
sent to the provider, and apps may still send it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

SEPARATOR = "|"
EVERY = 0  # the upstream_id of a setting for every provider of a model (those made before settings were per provider)


class ModelNames:
    def __init__(
        self, upstreams: Mapping[int, str] | None = None, custom: Mapping[tuple[int, str], str] | None = None
    ) -> None:
        self.upstreams = dict(upstreams or {})  # id -> name
        self.ids = {name: upstream_id for upstream_id, name in self.upstreams.items()}
        self.custom = dict(custom or {})  # (upstream id, model id) -> custom name

    def id_of(self, upstream: str | None) -> int:
        """The upstream's id; EVERY for one that's gone (old usage keeps its name, not an id) or none."""
        return self.ids.get(upstream or "", EVERY)

    def unpin(self, model: str | None) -> tuple[str, str | None]:
        """ "model@upstream" -> ("model", "upstream"), when what follows the last "@" is an upstream's name."""
        bare, _, upstream = (model or "").rpartition("@")
        return (bare, upstream) if bare and upstream in self.ids else (model or "", None)

    def name(self, model: str | None, upstream: str | None = None) -> str:
        """The model's custom name on that upstream, else its ID."""
        bare, pinned = self.unpin(model)
        return self.custom.get((self.id_of(upstream or pinned), bare), bare)

    def label(self, model: str | None, upstream: str | None = None) -> str:
        """``Provider|name``; just the name when no provider is known ("-": usage without one)."""
        bare, pinned = self.unpin(model)
        provider = upstream if upstream and upstream != "-" else pinned
        return f"{provider}{SEPARATOR}{self.name(bare, provider)}" if provider else self.name(bare)

    def _on(self, model: str, upstreams: Iterable[str]) -> tuple[str, str]:
        """(``A, B +2``, the first custom name among them) for a model several providers may serve."""
        names = list(dict.fromkeys(upstreams))
        return providers(names), next((n for n in (self.name(model, u) for u in names) if n != model), model)

    def label_on(self, model: str, upstreams: Iterable[str]) -> str:
        """``A, B +2|name``: for a choice several providers may serve."""
        shown, custom = self._on(model, upstreams)
        return f"{shown}{SEPARATOR}{custom}" if shown else custom

    def listed(self, model: str, upstreams: Iterable[str]) -> str:
        """``name|Provider``, how opencode shows it: the name after the number, score and price."""
        shown, custom = self._on(model, upstreams)
        return f"{custom}{SEPARATOR}{shown}" if shown else custom

    def of_request(self, model: str | None, target: str | None, upstream: str | None) -> str:
        """What a request asked for and got: ``Provider|name``, or ``alias → Provider|name``."""
        asked, pinned = self.unpin(model)
        served = self.unpin(target)[0] or asked
        label = self.label(served, upstream or pinned)
        return label if asked == served else f"{asked} → {label}"


def providers(upstreams: Iterable[str]) -> str:
    """``A, B +2``: the providers a choice may go to."""
    names = list(dict.fromkeys(upstreams))
    return ", ".join(names[:3]) + (f" +{len(names) - 3}" if len(names) > 3 else "")


def app_id(name: str) -> str:
    """A custom name as apps send it: spaces as dashes (``DeepSeek V4.1 Flash`` → ``DeepSeek-V4.1-Flash``), and an
    "@" too, so a name never reads as ``model@Provider``."""
    return "-".join(name.replace("@", " ").split())


@dataclass
class PublicIds:
    """The IDs apps know models by, for the models the providers offer."""

    of: dict[tuple[str, str], str] = field(default_factory=dict)  # (provider, real ID) -> the ID apps use
    to: dict[str, tuple[str, str]] = field(default_factory=dict)  # an ID for apps, or with @Provider -> real, provider
    shared: dict[str, tuple[str, ...]] = field(default_factory=dict)  # an ID several providers offer -> the IDs to pick
    clashes: dict[tuple[str, str], str] = field(default_factory=dict)  # a name that can't be an ID -> what has it
    several: set[str] = field(default_factory=set)  # real IDs more than one provider offers


def public_ids(offered: Mapping[str, Iterable[str]], names: ModelNames, aliases: Iterable[str] = ()) -> PublicIds:
    """``offered``: {provider: the real IDs it offers}. A model's ID for apps is its custom name, else its real ID,
    with ``@Provider`` when several providers offer one. On one provider a real ID wins over a name, and of models
    given the same name the first by real ID keeps it: the others are known by their real ID (``clashes``). So is a
    model named like an alias, which wins in requests."""
    taken_by_alias = set(aliases)
    ids = PublicIds()
    base: dict[tuple[str, str], str] = {}
    for upstream, models in offered.items():
        real = sorted(set(models))
        claimed = {m: m for m in real}  # every real ID first: a name never takes another model's
        upstream_id = names.id_of(upstream)
        for model in real:
            name = names.custom.get((upstream_id, model))
            wanted = app_id(name) if name else model
            if wanted != model and (wanted in claimed or wanted in taken_by_alias):
                ids.clashes[(upstream, model)] = "an alias" if wanted not in claimed else claimed[wanted]
                wanted = model
            claimed.setdefault(wanted, model)
            base[(upstream, model)] = wanted
    on: dict[str, list[str]] = {}  # an ID -> the providers offering a model by it
    for (upstream, _), wanted in base.items():
        on.setdefault(wanted, []).append(upstream)
    for (upstream, model), wanted in base.items():  # an alias's name is the alias's: the model needs its provider
        public = wanted if len(on[wanted]) == 1 and wanted not in taken_by_alias else f"{wanted}@{upstream}"
        ids.of[(upstream, model)] = public
        ids.to[f"{wanted}@{upstream}"] = ids.to[public] = (model, upstream)
    for wanted, upstreams in on.items():
        if len(upstreams) > 1:
            ids.shared[wanted] = tuple(sorted(f"{wanted}@{u}" for u in upstreams))
    reals: dict[str, list[str]] = {}  # a real ID several providers offer, and not an ID apps use: the same choice
    for upstream, model in base:
        reals.setdefault(model, []).append(upstream)
    for model, upstreams in reals.items():
        if len(upstreams) > 1:
            ids.several.add(model)
            if model not in ids.to and model not in ids.shared:
                ids.shared[model] = tuple(sorted(ids.of[(u, model)] for u in upstreams))
    return ids
