"""Guard rails for agents: tags, per-run budgets and loop detection.

A *run* is one agent task: ``X-Run-Id`` if the client sends it, otherwise the conversation
(session). Budgets stop a runaway agent; loop detection catches one repeating itself.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field

TAG = re.compile(r"[a-z0-9][a-z0-9._:-]{0,63}")
MAX_TAGS = 10
RUN_IDLE_S = 3600  # a run with no request for this long is forgotten
LOOP_WINDOW_S = 300


def parse_tags(*sources: str | None) -> list[str]:
    """Comma-separated tags from several sources, normalized, de-duplicated, invalid ones dropped."""
    tags: list[str] = []
    for source in sources:
        for raw in (source or "").split(","):
            tag = raw.strip().lower()
            if tag and TAG.fullmatch(tag) and tag not in tags:
                tags.append(tag)
    return tags[:MAX_TAGS]


def run_key(headers: Mapping[str, str], client: str, session: str | None) -> str | None:
    explicit = headers.get("x-run-id")
    if explicit:
        return f"{client}:run:{explicit[:128]}"
    return f"{client}:session:{session}" if session else None


def request_fingerprint(payload: object) -> str | None:
    """Same model + messages + tools = same request (ids/timestamps are not part of these)."""
    if not isinstance(payload, dict):
        return None
    material = {k: payload.get(k) for k in ("model", "messages", "prompt", "tools")}
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode()).hexdigest()[:24]


@dataclass
class Run:
    started: float
    last: float
    requests: int = 0
    tokens: int = 0
    recent: deque = field(default_factory=deque)  # (time, fingerprint) for loop detection


class Runs:
    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}

    def get(self, key: str) -> Run:
        now = time.time()
        run = self._runs.get(key)
        if run is None or now - run.last > RUN_IDLE_S:
            run = self._runs[key] = Run(started=now, last=now)
        if len(self._runs) > 50_000:
            self._prune(now)
        return run

    def over_budget(
        self, run: Run, max_tokens: int | None, max_requests: int | None, max_minutes: float | None
    ) -> str | None:
        """Why this run may not make another request, or None."""
        if max_requests is not None and run.requests >= max_requests:
            return f"this run already made {run.requests} requests (limit {max_requests})"
        if max_tokens is not None and run.tokens >= max_tokens:
            return f"this run already used {run.tokens:,} tokens (limit {max_tokens:,})"
        if max_minutes is not None and time.time() - run.started > max_minutes * 60:
            return f"this run has been going for more than {max_minutes:g} minutes"
        return None

    def note_request(self, run: Run, fingerprint: str | None) -> int:
        """Count a request; returns how many times this exact request was seen in the window."""
        now = time.time()
        run.requests += 1
        run.last = now
        while run.recent and run.recent[0][0] < now - LOOP_WINDOW_S:
            run.recent.popleft()
        repeats = 1 + sum(1 for _, fp in run.recent if fp == fingerprint) if fingerprint else 0
        if fingerprint:
            run.recent.append((now, fingerprint))
        return repeats

    @staticmethod
    def note_tokens(run: Run, tokens: int) -> None:
        run.tokens += max(tokens, 0)

    def snapshot(self) -> list[dict]:
        now = time.time()
        return [
            {"run": key, "requests": r.requests, "tokens": r.tokens, "minutes": round((now - r.started) / 60, 1)}
            for key, r in sorted(self._runs.items(), key=lambda kv: -kv[1].last)[:50]
            if now - r.last <= RUN_IDLE_S
        ]

    def _prune(self, now: float) -> None:
        self._runs = {k: r for k, r in self._runs.items() if now - r.last <= RUN_IDLE_S}
