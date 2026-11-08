"""What the checks of an answer read of a request: a message's text, and long text clipped.

An alias's answer check (its rules, the built-in refusal phrases, then its taggers: app/answer_rules.py,
app/taggers.py) reads an answer before the app gets it; these helpers are shared with the council's judge and the
rule maker.
"""

from __future__ import annotations

import json


def clip(text: str, most: int) -> str:
    """``text``, its middle left out when it's longer than ``most`` characters."""
    if len(text) <= most:
        return text
    half = most // 2
    return f"{text[:half]}\n[… {len(text) - 2 * half:,} characters left out …]\n{text[-half:]}"


def text_of(content: object) -> str:
    """A message's text: a string, or its text parts (an image is noted, not described)."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for part in content:
        if isinstance(part, dict):
            if isinstance(part.get("text"), str):
                parts.append(part["text"])
            elif part.get("type") in ("image_url", "image", "input_image"):
                parts.append("[an image]")
    return "\n".join(parts)


def last_user_text(body: bytes) -> str:
    """The text of the last user message of a chat completions request."""
    try:
        messages = json.loads(body).get("messages")
    except (ValueError, AttributeError):
        return ""
    for message in reversed(messages if isinstance(messages, list) else []):
        if isinstance(message, dict) and message.get("role") == "user":
            return text_of(message.get("content"))
    return ""
