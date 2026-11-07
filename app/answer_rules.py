"""Answer rules: an alias's own checks for refusals (or anything else unwanted), by exact text or a pattern.

A rule is made once on the Rules page and attached, in order, to aliases. For an alias's answer, its rules are tried
first, then the built-in refusal phrases (the "refused" trigger), then its checker model if it has one
(app/answer_check.py): the first rule that matches fails the answer, and the same request goes to the alias's next
model, as when its checker fails it. The last model's answer isn't checked: nothing is left to fall back to.

A rule looks at the start of the answer (its first START_CHARS characters: only that much is held back before the
answer streams on) or anywhere in it (the whole answer is held back, as for the checker). It reads the answer's
text and the separate ``refusal`` field some providers use; when the alias checks answers that call tools, also each
call, as ``[tool call] name(arguments)``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache

import regex

KINDS = ("contains", "starts", "pattern")
KIND_LABELS = {"contains": "Contains", "starts": "Starts with", "pattern": "Pattern"}
SCOPES = ("start", "anywhere")
SCOPE_LABELS = {"start": "at the start", "anywhere": "anywhere"}
# what an alias's answer check reads: answers with text (or a refusal), answers that call tools, empty ones
ANSWER_KINDS = ("text", "tools", "empty")
ANSWER_KIND_LABELS = {
    "text": "text answers",
    "tools": "answers that call tools",
    "empty": "empty answers (nothing but thinking, or stopped by a filter)",
}
DEFAULT_ANSWER_KINDS = ("text", "empty")  # as before: an answer that calls tools is passed on as it comes

START_CHARS = 400  # what a rule "at the start" reads (and all that's held back when only such rules apply)
NAME_MAX = 64
FIND_MAX = 2_000
NOTES_MAX = 300
RULES_PER_ALIAS = 30
PATTERN_TIMEOUT_S = 0.25  # the most one pattern may take on one answer: a slower one counts as no match


class RuleError(ValueError):
    """What's wrong with a rule, in words for its editor."""


@dataclass(frozen=True)
class Rule:
    id: int
    name: str
    kind: str
    find: str
    case: bool = False  # match case (off: also ' and ’ count as the same)
    scope: str = "start"
    enabled: bool = True
    notes: str = ""
    hits: int = 0
    last_hit: float | None = None
    created_at: float = 0.0

    @property
    def whole(self) -> bool:
        """It needs the whole answer (not just its start)."""
        return self.scope == "anywhere" and self.kind != "starts"

    def summary(self) -> str:
        where = "" if self.kind == "starts" else f" ({SCOPE_LABELS.get(self.scope, self.scope)})"
        return f"{KIND_LABELS.get(self.kind, self.kind)} {self.find!r}{where}{'' if self.case else ', any case'}"


@lru_cache(maxsize=256)
def _pattern(find: str, case: bool) -> regex.Pattern:
    return regex.compile(find, regex.MULTILINE | (0 if case else regex.IGNORECASE))


def _loose(text: str) -> str:
    return text.replace("’", "'").casefold()


def clean_rule(raw: dict) -> dict:
    """A rule's fields from its editor (strings and "on"), checked; RuleError says what's wrong."""
    name = " ".join(str(raw.get("name") or "").split())
    if not name or len(name) > NAME_MAX:
        raise RuleError(f"Give the rule a name (up to {NAME_MAX} characters).")
    kind = str(raw.get("kind") or "")
    if kind not in KINDS:
        raise RuleError("Choose what the rule looks for.")
    find = str(raw.get("find") or "").replace("\r\n", "\n")
    if not find.strip() or len(find) > FIND_MAX:
        raise RuleError(f"Give the text or pattern to look for (up to {FIND_MAX:,} characters).")
    case = raw.get("case") == "on"
    if kind == "pattern":
        try:
            _pattern(find, case)
        except regex.error as exc:
            raise RuleError(f"The pattern isn't valid: {exc}.") from exc
    scope = str(raw.get("scope") or "start")
    if scope not in SCOPES:
        raise RuleError("Choose where the rule looks.")
    notes = str(raw.get("notes") or "")[:NOTES_MAX]
    return {"name": name, "kind": kind, "find": find, "case": case, "scope": scope, "notes": notes}


def matches(rule: Rule, answer: str) -> bool:
    """Whether ``rule`` finds what it looks for in ``answer`` (the answer as the check reads it)."""
    text = answer.lstrip()
    if not rule.whole:
        text = text[:START_CHARS]
    if rule.kind == "pattern":
        try:
            return _pattern(rule.find, rule.case).search(text, timeout=PATTERN_TIMEOUT_S) is not None
        except (TimeoutError, regex.error):
            return False
    find = rule.find if rule.case else _loose(rule.find)
    text = text if rule.case else _loose(text)
    return text.startswith(find.lstrip()) if rule.kind == "starts" else find in text


def first_match(rules: Iterable[Rule], answer: str) -> Rule | None:
    """The first of ``rules`` (in order, those on) that matches ``answer``."""
    return next((r for r in rules if r.enabled and matches(r, answer)), None)


def kinds_of(raw: Iterable[str] | None) -> tuple[str, ...]:
    """What an alias's check reads, in a fixed order; unknown kinds are left out, and none at all means the
    default."""
    picked = {k for k in raw or () if k in ANSWER_KINDS}
    return tuple(k for k in ANSWER_KINDS if k in picked) or DEFAULT_ANSWER_KINDS
