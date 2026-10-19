"""Session keys: which requests belong to the same conversation / agent task.

Used by the scheduler to let sessions that are mid-task finish before new ones start.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

_MAX_MATERIAL = 4000


def _hash(*parts: object) -> str:
    material = json.dumps(parts, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(material.encode()).hexdigest()[:16]


def _first(messages: list, roles: tuple[str, ...]) -> str | None:
    for message in messages:
        if isinstance(message, dict) and message.get("role") in roles:
            content = message.get("content")
            text = content if isinstance(content, str) else json.dumps(content, default=str)
            return text[:_MAX_MATERIAL]
    return None


def session_key(headers: Mapping[str, str], client: str, payload: object) -> str | None:
    """``X-Session-Id`` if the client sends one; otherwise derived from the conversation start.

    A conversation keeps the same system prompt and first user message as it grows, so
    hashing those (plus client and model) groups an agent's successive calls together, while
    subagents and side tasks (titles, summaries) get their own sessions.
    """
    explicit = headers.get("x-session-id")
    if explicit:
        return _hash("explicit", client, explicit[:256])
    if not isinstance(payload, dict):
        return None
    model = payload.get("model")
    messages = payload.get("messages")
    if isinstance(messages, list) and messages:
        system = _first(messages, ("system", "developer"))
        user = _first(messages, ("user",))
        if system is None and user is None:
            return None
        return _hash("chat", client, model, system, user)
    prompt = payload.get("prompt")
    if isinstance(prompt, str) and prompt:
        return _hash("prompt", client, model, prompt[:_MAX_MATERIAL])
    return None
