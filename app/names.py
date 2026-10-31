"""What a model is called. A model on a provider (upstream) is one entry: the same ID on two providers is two entries,
each with its own optional custom name, price, score and limits, and is shown as ``Provider|name`` everywhere.

A custom name is what people see and what a model's automatic score, price and thinking levels are looked up by. The
real ID stays what's stored, sent to the provider, and what opencode sends back, so naming a model breaks nothing.
It isn't a name apps can ask for: several models may share one (aliases are for that).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

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

    def label_on(self, model: str, upstreams: Iterable[str]) -> str:
        """For a choice several providers may serve: ``A, B +2|name`` (the first custom name among them)."""
        names = list(dict.fromkeys(upstreams))
        if len(names) < 2:
            return self.label(model, names[0] if names else None)
        shown = ", ".join(names[:3]) + (f" +{len(names) - 3}" if len(names) > 3 else "")
        custom = next((n for n in (self.name(model, u) for u in names) if n != model), model)
        return f"{shown}{SEPARATOR}{custom}"

    def of_request(self, model: str | None, target: str | None, upstream: str | None) -> str:
        """What a request asked for and got: ``Provider|name``, or ``alias → Provider|name``."""
        asked, pinned = self.unpin(model)
        served = self.unpin(target)[0] or asked
        label = self.label(served, upstream or pinned)
        return label if asked == served else f"{asked} → {label}"
