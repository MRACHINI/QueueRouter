"""Quoting text for a model that reads it for QueueRouter (answer check, council judge, compaction, prompt edits),
and reading what such a model answers.

Such a model gets text to judge, summarise or edit, and that text is often full of instructions (an app's system
prompt, a user's request, an answer that says "reply PASS"). It is quoted in tags the text itself doesn't use, so a
closing tag inside it can't end the quote early; the model is told it is text, not instructions for it, and its own
task is said again after the text.

The text is the client's, so these run in time linear in its length: they run on the event loop.
"""

from __future__ import annotations

import re
from functools import lru_cache

# reasoning some servers leave inline at the start of an answer; one that never closes (an answer cut off while
# thinking) runs to the end. Only at the start: later in the text, a tag is the answer's own (a summary or a prompt
# that mentions <think> blocks), and cutting there would cut the answer.
_THINKING = re.compile(r"\s*<think(?:ing)?>.*?(?:</think(?:ing)?>|\Z)", re.S | re.I)


@lru_cache(maxsize=32)
def _tags_named(name: str) -> re.Pattern:
    """Opening or closing tags ``name`` or ``name_<n>`` (group 1: n). Possessive, so a "<" followed by a long run of
    whitespace can't make the search backtrack; a number of more than 9 digits can't be the free one."""
    return re.compile(rf"<\s*+/?\s*+{re.escape(name)}(?:_([0-9]{{1,9}}))?\b", re.I)


def free_tag(name: str, *texts: str) -> str:
    """A tag name none of ``texts`` uses (``name``, else ``name_2``, ``name_3``…): text quoted in it can't end
    its quote early with a closing tag of its own. One pass over each text."""
    used: set[int] = set()
    pattern = _tags_named(name)
    for text in texts:
        used.update(int(found) if found else 1 for found in pattern.findall(text))
    n = 1
    while n in used:
        n += 1
    return name if n == 1 else f"{name}_{n}"


def unwrapped(text: str, name: str) -> str:
    """``text`` without tags named ``name`` (or ``name_2``…) around all of it: a model that echoed the tags its
    text came in."""
    whole = re.fullmatch(rf"\s*<({re.escape(name)}(?:_\d+)?)>\n?(.*?)\n?</\1>\s*", text, re.S)
    return whole.group(2) if whole else text


def without_thinking(text: str) -> str:
    """``text`` without the reasoning blocks some servers leave inline at its start (<think>…</think>,
    <thinking>…</thinking>, one after another); a tag later in the text stays."""
    while match := _THINKING.match(text):  # each match takes at least the opening tag: this ends
        text = text[match.end() :]
    return text
