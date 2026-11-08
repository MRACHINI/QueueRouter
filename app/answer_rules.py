"""Answer rules: an alias's own checks of its answers, by exact text, a list of words or a pattern.

A rule is made on the Rules page, by hand or by an alias's rule maker (app/rule_maker.py), and attached, in order, to
aliases. For an alias's answer its rules are tried in order and the first one that matches decides what happens:

  fail       the answer fails: the same request goes to the alias's next model
  pass       the answer passes: the app gets it, nothing else runs
  pass_free  the answer passes the checker model (which isn't asked), but the free checks still run (the built-in
             refusal phrases, with the alias's "refused" trigger)
  ask        the checker model decides (also when the alias asks it only for answers a rule sends to it)

With no rule deciding, the built-in refusal phrases run, then the checker model if the alias has one (for every such
answer, or only for those a rule sends to it). The last model's answer isn't checked: nothing is left to fall back to.

A rule is on, watching (it decides nothing: what it would have done is compared with the checker's verdict, to see
whether it can be trusted) or off. A rule the model made starts watching, and the alias decides what happens next: you
approve it, or it turns on by itself once it has agreed with the checker often enough (``next_state``). A rule the
model made that is on and starts disagreeing with the checker (spot checks: some answers a rule decided go to the
checker too) goes back to watching by itself.

A rule looks at the start of the answer (its first START_CHARS characters) or anywhere in it. It reads the answer's
text and the separate ``refusal`` field some providers use; when the alias checks answers that call tools, also each
call, as ``[tool call] name(arguments)``. "Doesn't" turns a rule around: it matches when the text isn't there.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import lru_cache

import regex

KINDS = ("contains", "any", "starts", "pattern")
KIND_LABELS = {"contains": "Contains", "any": "Any of these", "starts": "Starts with", "pattern": "Pattern"}
SCOPES = ("start", "anywhere")
SCOPE_LABELS = {"start": "at the start", "anywhere": "anywhere"}
ACTIONS = ("fail", "pass", "pass_free", "ask")
ACTION_LABELS = {
    "fail": "Fail it (the next model answers)",
    "pass": "Pass it (nothing else runs)",
    "pass_free": "Pass it, but run the free checks",
    "ask": "Ask the checker model",
}
ACTION_SHORT = {"fail": "fails", "pass": "passes", "pass_free": "passes (free checks run)", "ask": "asks the checker"}
ACTION_WOULD = {
    "fail": "failed it",
    "pass": "passed it",
    "pass_free": "passed it (free checks run)",
    "ask": "asked the checker",
}
STATES = ("on", "watch", "off")
STATE_LABELS = {"on": "on", "watch": "watching", "off": "off"}
# what an alias's answer check reads: answers with text (or a refusal), answers that call tools, empty ones
ANSWER_KINDS = ("text", "tools", "empty")
ANSWER_KIND_LABELS = {
    "text": "text answers",
    "tools": "answers that call tools",
    "empty": "empty answers (nothing but thinking, or stopped by a filter)",
}
DEFAULT_ANSWER_KINDS = ("text", "empty")  # as before: an answer that calls tools is passed on as it comes
CHECK_WHEN = ("undecided", "asked")  # when an alias asks its checker model
CHECK_WHEN_LABELS = {
    "undecided": "for every answer no rule decided",
    "asked": "only for answers a rule sends to it",
}
LEARN_MODES = ("off", "propose", "auto")  # what an alias's rule maker does with the rules it makes
LEARN_LABELS = {
    "off": "off",
    "propose": "it proposes them, I turn them on",
    "auto": "they turn on by themselves once proven",
}

START_CHARS = 400  # what a rule "at the start" reads (and all that's held back when only such rules apply)
NAME_MAX = 64
FIND_MAX = 2_000
NOTES_MAX = 300
RULES_PER_ALIAS = 40
PATTERN_TIMEOUT_S = 0.25  # the most one pattern may take on one answer: a slower one counts as no match
SPOT_DEFAULT = 5  # % of the answers decided without the checker that it reads too, in the background
RECENT = 20  # outcomes kept per rule (agreed with the checker or not) for turning it off by itself
PROMOTE_AGREED, PROMOTE_DISAGREED = 20, 1  # watching: turns on by itself after this many agreements, at most this…
DEMOTE_DISAGREED = 2  # on: back to watching once it disagreed more than this many of its last RECENT times
REJECTED_KEEP = 200  # rules a rule maker made that you deleted, remembered per alias so it doesn't propose them again


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
    state: str = "on"
    notes: str = ""
    hits: int = 0  # answers it decided
    last_hit: float | None = None
    created_at: float = 0.0
    action: str = "fail"
    negate: bool = False  # "doesn't": it matches when the text isn't there
    made_by: int | None = None  # the alias whose rule maker made it (None: made by hand)
    why: str = ""  # the rule maker's reason
    sample_fails: int = 0  # of the kept answers, when it was made: failed ones it matched…
    sample_passes: int = 0  # …and passed ones
    agreed: int = 0  # times the checker's verdict agreed with what it did (or would have done)
    disagreed: int = 0
    recent: str = ""  # its last RECENT outcomes, "1" agreed, "0" not
    awaiting: bool = False  # made by the model, waiting for you to turn it on
    note: str = ""  # why its state last changed by itself

    @property
    def enabled(self) -> bool:
        return self.state == "on"

    @property
    def whole(self) -> bool:
        """It needs the whole answer (not just its start)."""
        return self.scope == "anywhere" and self.kind != "starts"

    @property
    def words(self) -> list[str]:
        return [w.strip() for w in self.find.splitlines() if w.strip()] if self.kind == "any" else [self.find]

    def summary(self) -> str:
        if self.kind == "any":
            what = "any of " + ", ".join(repr(w) for w in self.words[:6]) + (" …" if len(self.words) > 6 else "")
        else:
            what = f"{KIND_LABELS.get(self.kind, self.kind).lower()} {self.find!r}"
        where = "" if self.kind == "starts" else f" ({SCOPE_LABELS.get(self.scope, self.scope)})"
        negated = "doesn't " if self.negate else ""
        return f"{negated}{what}{where}{'' if self.case else ', any case'}"


@dataclass
class Decision:
    """What an alias's rules made of an answer: the first rule on that matched (``rule``; None: none did) and the
    rules watching that matched it."""

    rule: Rule | None = None
    watched: list[Rule] = field(default_factory=list)

    @property
    def action(self) -> str | None:
        return self.rule.action if self.rule is not None else None


@lru_cache(maxsize=256)
def _pattern(find: str, case: bool) -> regex.Pattern:
    return regex.compile(find, regex.MULTILINE | (0 if case else regex.IGNORECASE))


def _loose(text: str) -> str:
    return text.replace("’", "'").casefold()


def _flag(raw: dict, key: str) -> bool:
    value = raw.get(key)
    return value is True or value == "on"


def clean_rule(raw: dict) -> dict:
    """A rule's fields from its editor (strings and "on") or the rule maker, checked; RuleError says what's wrong."""
    name = " ".join(str(raw.get("name") or "").split())
    if not name or len(name) > NAME_MAX:
        raise RuleError(f"Give the rule a name (up to {NAME_MAX} characters).")
    kind = str(raw.get("kind") or "")
    if kind not in KINDS:
        raise RuleError("Choose what the rule looks for.")
    find = raw.get("find")
    if isinstance(find, list):  # the rule maker's words
        find = "\n".join(str(w) for w in find)
    find = str(find or "").replace("\r\n", "\n")
    if kind == "any":
        find = "\n".join(w.strip() for w in find.splitlines() if w.strip())
    if not find.strip() or len(find) > FIND_MAX:
        raise RuleError(f"Give the text, words or pattern to look for (up to {FIND_MAX:,} characters).")
    case = _flag(raw, "case")
    if kind == "pattern":
        try:
            _pattern(find, case)
        except regex.error as exc:
            raise RuleError(f"The pattern isn't valid: {exc}.") from exc
    scope = str(raw.get("scope") or "start")
    if scope not in SCOPES:
        raise RuleError("Choose where the rule looks.")
    action = str(raw.get("action") or "fail")
    if action not in ACTIONS:
        raise RuleError("Choose what the rule does.")
    state = str(raw.get("state") or "on")
    if state not in STATES:
        raise RuleError("Choose whether the rule is on, watching or off.")
    notes = str(raw.get("notes") or "")[:NOTES_MAX]
    return {
        "name": name,
        "kind": kind,
        "find": find,
        "case": case,
        "scope": scope,
        "action": action,
        "negate": _flag(raw, "negate"),
        "state": state,
        "notes": notes,
    }


def matches(rule: Rule, answer: str) -> bool:
    """Whether ``rule`` matches ``answer`` (the answer as the check reads it); "doesn't" turns it around."""
    try:
        return found(rule, answer)
    except (TimeoutError, regex.error):
        return False  # a pattern that can't say counts as no match, "doesn't" or not


def found(rule: Rule, answer: str, timeout: float = PATTERN_TIMEOUT_S) -> bool:
    """Like ``matches``, but a pattern that takes longer than ``timeout`` raises TimeoutError (the rule maker drops
    such a pattern: it would slow every answer). The search lets other threads run meanwhile."""
    text = answer.lstrip()
    if not rule.whole:
        text = text[:START_CHARS]
    if rule.kind == "pattern":
        hit = _pattern(rule.find, rule.case).search(text, timeout=timeout, concurrent=True) is not None
    else:
        hay = text if rule.case else _loose(text)
        needles = [w if rule.case else _loose(w) for w in rule.words]
        if rule.kind == "starts":
            hit = hay.startswith(needles[0].lstrip())
        else:
            hit = any(n in hay for n in needles if n)
    return hit != rule.negate


def rule_key(kind: str, find: str, negate: bool) -> list:
    """What makes two rules the same rule, for spotting a rule maker's repeats: what they look for, and how."""
    return [kind, find.casefold(), bool(negate)]


def decide(rules: Iterable[Rule], answer: str) -> Decision:
    """The first rule on that matches ``answer`` decides; every rule watching that matches is noted."""
    decision = Decision()
    for rule in rules:
        if rule.state == "off" or (rule.state == "on" and decision.rule is not None):
            continue
        if not matches(rule, answer):
            continue
        if rule.state == "watch":
            decision.watched.append(rule)
        else:
            decision.rule = rule
    return decision


def first_match(rules: Iterable[Rule], answer: str) -> Rule | None:
    """The first of ``rules`` (in order, those on) that matches ``answer``."""
    return next((r for r in rules if r.enabled and matches(r, answer)), None)


def agrees(action: str, failed: bool) -> bool:
    """Whether the checker's verdict agrees with what a rule did (or would have done): a fail rule with a failed
    answer, a pass rule with a passed one, and a rule that asks the checker with a failed one (it was right to ask)."""
    return failed if action in ("fail", "ask") else not failed


def next_state(rule: Rule, agreed: bool, learn: str, learn_pass: bool) -> dict:
    """``rule`` after one more outcome: its counts, its recent outcomes, and its state when it changes by itself (a
    rule the model made: watching, it turns on once proven, if its alias lets it; on, it goes back to watching when
    it starts disagreeing). A rule that asks the checker never changes by itself: it can't make a wrong decision."""
    recent = (rule.recent + ("1" if agreed else "0"))[-RECENT:]
    fields = {
        "agreed": rule.agreed + int(agreed),
        "disagreed": rule.disagreed + int(not agreed),
        "recent": recent,
    }
    if rule.made_by is None or rule.action == "ask":
        return fields
    passing = rule.action in ("pass", "pass_free")
    if rule.state == "watch" and learn == "auto" and (learn_pass or not passing):
        if fields["agreed"] >= PROMOTE_AGREED and fields["disagreed"] <= PROMOTE_DISAGREED:
            agreed, disagreed = fields["agreed"], fields["disagreed"]
            note = f"turned on by itself: it agreed with the checker {agreed} times, {disagreed} not"
            fields.update(state="on", awaiting=False, note=note)
    elif rule.state == "on" and (missed := recent.count("0")) > DEMOTE_DISAGREED:
        note = f"back to watching by itself: it disagreed with the checker {missed} of its last {len(recent)} times"
        fields.update(state="watch", note=note)
    return fields


def kinds_of(raw: Iterable[str] | None) -> tuple[str, ...]:
    """What an alias's check reads, in a fixed order; unknown kinds are left out, and none at all means the
    default."""
    picked = {k for k in raw or () if k in ANSWER_KINDS}
    return tuple(k for k in ANSWER_KINDS if k in picked) or DEFAULT_ANSWER_KINDS
