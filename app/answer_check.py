"""Answer checks: a second model reads an answer before the app gets it (opt-in per alias).

When an alias checks answers, a plain-text answer (no tool calls) from any model in its chain but the last is held
back whole. A small separate request then goes to the checker model with the administrator's instructions, the
user's last message and the answer, never the rest of the conversation. PASS: the held answer goes to the app.
FAIL: the same request goes to the alias's next model, as a fallback does, and that answer is checked the same
way. The last model's answer isn't checked: nothing is left to fall back to. A checker that fails, takes too long
or answers something unreadable lets the answer through; the Requests page says so.
"""

from __future__ import annotations

import json
import re

TIMEOUT_S = 120.0  # the most a check may take, waiting for the checker's slot included
MESSAGE_CHARS = 12_000  # of the user's last message the checker sees (its start and end)
ANSWER_CHARS = 40_000  # of the answer
REASON_CHARS = 200

MARKER = "You check an AI assistant's answer"  # how the checker's request starts (and the test mock spots it)
SYSTEM = (
    MARKER + " before it is shown to the user. You see only the user's last message and the answer, not the rest "
    "of the conversation.\n\nYour instructions:\n{instructions}\n\nReply PASS if the answer is acceptable under "
    'these instructions. Otherwise reply "FAIL: " and the reason, in one short sentence. Reply with nothing else.'
)
_THINKING = re.compile(r"<think(?:ing)?>.*?</think(?:ing)?>", re.S | re.I)
_VERDICT = re.compile(r"^[\W_]*(PASS|FAIL)\b[\W_]*(.*)$", re.I)


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


def check_payload(model: str, instructions: str, body: bytes, answer: str) -> dict:
    """The checker's request: the instructions, the user's last message and the answer."""
    message = clip(last_user_text(body).strip(), MESSAGE_CHARS) or "(no text)"
    shown = clip(answer.strip(), ANSWER_CHARS) or "(empty: the model answered nothing)"
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM.format(instructions=instructions.strip())},
            {
                "role": "user",
                "content": f"The user's last message:\n<message>\n{message}\n</message>\n\n"
                f"The answer:\n<answer>\n{shown}\n</answer>",
            },
        ],
    }


def verdict_of(body: bytes | None) -> tuple[bool | None, str]:
    """(passed?, reason) from the checker's answer; passed is None when it can't be read. The verdict is looked
    for on the first line, then the last (a model that explains first)."""
    try:
        message = (json.loads(body or b"").get("choices") or [{}])[0].get("message") or {}
    except (ValueError, AttributeError, IndexError):
        return None, "unreadable reply"
    text = _THINKING.sub("", message.get("content") if isinstance(message.get("content"), str) else "")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for index in dict.fromkeys((0, len(lines) - 1)) if lines else ():
        if match := _VERDICT.match(lines[index]):
            passed = match.group(1).upper() == "PASS"
            reason = match.group(2) or (lines[index + 1] if not passed and index + 1 < len(lines) else "")
            reason = " ".join(reason.split())[:REASON_CHARS]
            return passed, reason or ("passed" if passed else "no reason given")
    return None, f"unreadable reply: {' '.join(text.split())[:80]!r}" if text.strip() else "empty reply"
