"""Prompt edits: changes made to the system prompt of requests before they go to the model.

An edit is made once (Edits page) and attached, in order, to aliases, client groups and clients: an alias's edits
run first, then its group's, then the client's own. They change the app's system and developer messages only, never
the user's or the model's. Kinds:

  text     find exact text and replace it (blank: delete it); first or every match, matching case or not
  pattern  the same with a regular expression (``\\1`` in the replacement), for parts that change between requests
  add      add text before or after the system prompt, replace it whole, or use it only when there is none
  rewrite  a model rewrites the prompt (or only the parts a pattern finds) by the administrator's instructions; its
           result is kept, so the same text is rewritten once
  learn    a model reads the prompt by paragraph and makes text and pattern edits of its own, kept under the edit; it
           reads only paragraphs it hasn't read before, so after the first requests it is rarely asked. It also marks
           the parts that change between requests (a date, a folder) as "keep" patterns, so they don't count as new

A model edit either waits for its model (the request waits too, up to a time limit) or lets the request go with what
it has while the model works in the background for the next one. A model that fails isn't asked again for a minute.
Patterns run with a time limit (the ``regex`` module), so a pattern that would take ages can't stall the server.
"""

from __future__ import annotations

import asyncio
import bisect
import contextlib
import difflib
import hashlib
import itertools
import json
import logging
import re
import time
from collections import Counter, OrderedDict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from functools import lru_cache

import regex

log = logging.getLogger("queuerouter")

KINDS = ("text", "pattern", "add", "rewrite", "learn")
KIND_LABELS = {
    "text": "Text",
    "pattern": "Pattern",
    "add": "Add",
    "rewrite": "Model: rewrite",
    "learn": "Model: make edits",
}
MADE_KINDS = ("text", "pattern", "keep")  # what a "learn" edit's model makes
ADD_MODES = ("before", "after", "replace", "empty")
ADD_LABELS = {
    "before": "before the system prompt",
    "after": "after the system prompt",
    "replace": "instead of the system prompt",
    "empty": "only when there is no system prompt (or it's empty)",
}
STATES = ("on", "off", "pending")
SYSTEM_ROLES = ("system", "developer")
LEARN_MODES = ("both", "apply", "learn")  # a "make edits" edit: learn and apply / apply only / learn only
LEARN_LABELS = {
    "both": "Learn and apply",
    "apply": "Apply only (no model calls)",
    "learn": "Learn only (a dry run)",
}
SEEN_AROUND = 200  # characters of the text around a made edit's match that are kept to show where it was
SEEN_LINES = 3  # ...at most this many lines of it on each side

FIND_MAX = 2_000  # characters of a find text or pattern
TEXT_MAX = 50_000  # of a replacement or added text
INSTRUCTIONS_MAX = 4_000
NAME_MAX = 64
TIMEOUT_DEFAULT, TIMEOUT_MIN, TIMEOUT_MAX = 60, 5, 600  # seconds a model edit may make the request wait
PATTERN_TIMEOUT_S = 0.25  # the most one pattern may take on one text
RETRY_AFTER_S = 60.0  # a model edit whose model failed isn't asked again for this long
PARAGRAPH_MAX = 4_000  # characters: a longer paragraph is read in pieces
LEARN_CHARS = 60_000  # of new paragraphs sent to the model at once (the rest wait for the next request)
MADE_MAX = 20  # edits one answer may add
HELD_SHARE = 0.5  # a made edit that would leave less than this share of the text is held for approval
KEEP_MAX = 500  # characters of a keep pattern
CONTEXT = 60  # characters shown around a change
HUNKS_MAX = 40  # changes recorded per edit and text
RESULTS_KEPT = 500  # rewrite results kept in memory (the rest are read from the database)

REWRITE_MARKER = "You rewrite part of the system prompt"  # how a rewrite request starts (the test mock spots it)
REWRITE_SYSTEM = (
    REWRITE_MARKER + " that an AI app sends to its model. An administrator's instructions say how:\n"
    "<instructions>\n{instructions}\n</instructions>\n\n"
    "The user's message is that text. Reply with the rewritten text only: no notes, no quotes, no code fences. "
    "Keep everything the instructions don't ask you to change."
)
LEARN_MARKER = "You write edits for the system prompt"
LEARN_SYSTEM = (
    LEARN_MARKER + " that an AI app sends to its model. An administrator's instructions say what to change:\n"
    "<instructions>\n{instructions}\n</instructions>\n\n"
    "You get paragraphs of that prompt you haven't seen before, and the edits that already exist. Reply with JSON "
    "only, in this shape:\n"
    '{{"edits": [{{"find": "exact text from the paragraphs", "replace": "what it becomes"}}, '
    '{{"pattern": "a regular expression (Python syntax)", "replace": "what it becomes"}}], '
    '"keep": ["a regular expression for a part that changes from one request to the next and needs no edit"]}}\n'
    "Rules:\n"
    "- Only make the edits the instructions ask for; most paragraphs need none.\n"
    '- Copy "find" exactly, character for character, from the paragraphs: short, but long enough to be unique.\n'
    '- "replace" may be empty, to delete the text.\n'
    '- Use "pattern" only for text to change that differs between requests (a date, a name, a folder).\n'
    '- Under "keep", list parts that differ between requests (dates, times, folders, ids) and need no edit.\n'
    "- Don't repeat an existing edit.\n"
    '- With nothing to change, reply {{"edits": [], "keep": []}}.'
)
_THINKING = re.compile(r"<think(?:ing)?>.*?</think(?:ing)?>", re.S | re.I)
_FENCE = re.compile(r"^\s*```[\w-]*\n(.*?)\n?```\s*$", re.S)
_PARAGRAPHS = re.compile(r"\n[ \t]*\n")
_GROUP_REF = re.compile(r"\\(?:g<([^>]*)>|(\d{1,2}))")


class EditError(ValueError):
    """What's wrong with an edit's settings, in words for the editor."""


@dataclass(frozen=True)
class Edit:
    """One edit (``parent_id``: made by that "learn" edit's model). ``settings`` per kind: find, replace, all,
    case (text, pattern, keep); mode, text (add); model, instructions, part, wait, on_fail, timeout_s (rewrite);
    model, instructions, wait, approve, timeout_s (learn)."""

    id: int
    name: str
    kind: str
    state: str = "on"
    settings: dict = field(default_factory=dict, compare=False, hash=False)
    parent_id: int | None = None
    notes: str = ""
    hits: int = 0
    last_hit: float | None = None
    created_at: float = 0.0
    children: tuple[Edit, ...] = ()

    @property
    def on(self) -> bool:
        return self.state == "on"

    @property
    def label(self) -> str:
        return self.name or f"#{self.id}"

    def setting(self, key: str, default=None):
        value = self.settings.get(key, default)
        return default if value is None else value

    @property
    def wait(self) -> bool:
        return bool(self.setting("wait", True))

    @property
    def timeout_s(self) -> float:
        return float(self.setting("timeout_s", TIMEOUT_DEFAULT))

    def summary(self) -> str:
        """One line for lists: what it does."""
        s = self.settings
        if self.kind in ("text", "pattern"):
            how = "" if s.get("all", True) else " (first)"
            return f"{_short(s.get('find', ''))} → {_short(s.get('replace', '')) or '(deleted)'}{how}"
        if self.kind == "keep":
            return f"keep {_short(s.get('find', ''))}"
        if self.kind == "add":
            return f"{ADD_LABELS.get(s.get('mode', ''), '')}: {_short(s.get('text', ''))}"
        part = f", only {_short(s.get('part', ''))}" if s.get("part") else ""
        mode = {"apply": " (apply only)", "learn": " (learn only)"}.get(s.get("learn", "both"), "")
        return f"{s.get('model', '')}{part}{mode}: {_short(s.get('instructions', ''), 80)}"


def _short(text: str, most: int = 40) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= most else text[: most - 1] + "…"


# -- settings ------------------------------------------------------------------------------------------------------


@lru_cache(maxsize=512)
def compile_pattern(find: str, case: bool = True) -> regex.Pattern:
    """A pattern as edits run it: ^ and $ match at line ends; case-insensitive when ``case`` is off."""
    return regex.compile(find, regex.M | regex.V0 | (0 if case else regex.I))


def _matcher(kind: str, find: str, case: bool) -> regex.Pattern:
    return compile_pattern(find if kind == "pattern" or kind == "keep" else regex.escape(find), case)


def check_replacement(compiled: regex.Pattern, replace: str) -> None:
    """Group references in a replacement that the pattern doesn't have (they'd fail only when it matches)."""
    for match in _GROUP_REF.finditer(replace):
        ref = match.group(1) if match.group(1) is not None else match.group(2)
        if ref.isdecimal():
            if int(ref) > compiled.groups:
                raise EditError(f"The replacement uses group {ref}, but the pattern has {compiled.groups}.")
        elif ref not in compiled.groupindex:
            raise EditError(f"The replacement uses group <{ref}>, which the pattern doesn't have.")


def check_pattern(find: str, case: bool = True) -> regex.Pattern:
    if not find:
        raise EditError("Give the pattern to find.")
    if len(find) > FIND_MAX:
        raise EditError(f"The pattern is longer than {FIND_MAX:,} characters.")
    try:
        return compile_pattern(find, case)
    except (regex.error, OverflowError, RecursionError) as exc:
        raise EditError(f"The pattern isn't valid: {exc}.") from exc


def _flag(raw, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("on", "1", "true", "yes")


def clean_settings(kind: str, raw: dict) -> dict:
    """An edit's settings from its editor (strings and "on"), checked; EditError says what's wrong."""
    if kind not in KINDS and kind not in MADE_KINDS:
        raise EditError("Unknown kind of edit.")

    def text(key: str, most: int) -> str:
        value = str(raw.get(key) or "").replace("\r\n", "\n")
        if len(value) > most:
            raise EditError(f"{key.capitalize()} is longer than {most:,} characters.")
        return value

    if kind in ("text", "pattern", "keep"):
        find, case = text("find", FIND_MAX), _flag(raw.get("case"), True)
        if kind == "text" and not find:
            raise EditError("Give the text to find.")
        settings: dict = {"find": find, "case": case}
        if kind == "keep":
            check_pattern(find, case)
            return settings
        replace = text("replace", TEXT_MAX)
        if kind == "pattern":
            check_replacement(check_pattern(find, case), replace)
        return {**settings, "replace": replace, "all": _flag(raw.get("all"), True)}
    if kind == "add":
        mode = str(raw.get("mode") or "")
        if mode not in ADD_MODES:
            raise EditError("Choose where the text goes.")
        added = text("text", TEXT_MAX)
        if not added.strip():
            raise EditError("Give the text to add.")
        return {"mode": mode, "text": added}
    model = " ".join(str(raw.get("model") or "").split())
    if not model or len(model) > 200:
        raise EditError("Choose the model that does the edit.")
    instructions = text("instructions", INSTRUCTIONS_MAX).strip()
    if not instructions:
        raise EditError("Give the model its instructions.")
    try:
        timeout = int(float(str(raw.get("timeout_s") or TIMEOUT_DEFAULT)))
    except ValueError as exc:
        raise EditError("The time limit is a number of seconds.") from exc
    if not TIMEOUT_MIN <= timeout <= TIMEOUT_MAX:
        raise EditError(f"The time limit is {TIMEOUT_MIN} to {TIMEOUT_MAX} seconds.")
    settings = {"model": model, "instructions": instructions, "wait": _flag(raw.get("wait"), True)}
    settings["timeout_s"] = timeout
    if kind == "rewrite":
        part = text("part", FIND_MAX)
        if part:
            check_pattern(part)
        on_fail = str(raw.get("on_fail") or "keep")
        if on_fail not in ("keep", "refuse"):
            raise EditError("Choose what happens when the model fails.")
        return {**settings, "part": part, "on_fail": on_fail}
    learn = str(raw.get("learn") or "both")
    if learn not in LEARN_MODES:
        raise EditError("Choose what the edit does: learn, apply, or both.")
    return {**settings, "approve": _flag(raw.get("approve"), False), "learn": learn}


def around(text: str, start: int, end: int) -> list[str]:
    """The text before and after ``text[start:end]`` (a few lines each side): where a made edit was found."""
    before = text[max(0, start - SEEN_AROUND) : start].split("\n")[-SEEN_LINES:]
    after = text[end : end + SEEN_AROUND].split("\n")[:SEEN_LINES]
    head = "\n".join(before)
    tail = "\n".join(after)
    if start - len(head) > 0:
        head = "…" + head
    if end + len(tail) < len(text):
        tail += "…"
    return [head, tail]


# -- where edits work: the system and developer messages -----------------------------------------------------------


@dataclass
class Slot:
    """One text of a system or developer message: its content, or one text part of it."""

    message: dict
    part: int | None = None

    def get(self) -> str:
        content = self.message.get("content")
        if self.part is None:
            return content if isinstance(content, str) else ""
        item = content[self.part] if isinstance(content, list) and self.part < len(content) else None
        return (item.get("text") or "") if isinstance(item, dict) else ""

    def set(self, text: str) -> None:
        if self.part is None:
            self.message["content"] = text
        else:
            content = list(self.message["content"])  # the app's list isn't changed in place
            content[self.part] = {**content[self.part], "text": text}
            self.message["content"] = content

    @property
    def key(self) -> tuple[int, int | None]:
        return id(self.message), self.part


def slots_of(messages: list) -> list[Slot]:
    found = []
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in SYSTEM_ROLES:
            continue
        content = message.get("content")
        if isinstance(content, str) or content is None:
            found.append(Slot(message))
        elif isinstance(content, list):
            for i, item in enumerate(content):
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    found.append(Slot(message, i))
    return found


def leading(messages: list) -> list[dict]:
    """The system prompt: the system and developer messages before the conversation."""
    first = []
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in SYSTEM_ROLES:
            break
        first.append(message)
    return first


def paragraphs(text: str) -> list[str]:
    """A text by paragraph (blank lines), a long one in pieces of whole lines."""
    pieces = []
    for paragraph in _PARAGRAPHS.split(text):
        if not paragraph.strip():
            continue
        while len(paragraph) > PARAGRAPH_MAX:
            cut = paragraph.rfind("\n", 0, PARAGRAPH_MAX)
            cut = cut if cut > 0 else PARAGRAPH_MAX
            pieces.append(paragraph[:cut])
            paragraph = paragraph[cut:].lstrip("\n")
        if paragraph.strip():
            pieces.append(paragraph)
    return pieces


def paragraph_hash(paragraph: str, keeps: Iterable[Edit] = ()) -> str:
    """What identifies a paragraph: its text with the parts that change between requests (keep patterns) left out."""
    masked = paragraph.strip()
    for keep in keeps:
        try:
            masked = _matcher("keep", keep.settings.get("find", ""), keep.settings.get("case", True)).sub(
                "\x00", masked, timeout=PATTERN_TIMEOUT_S
            )
        except (TimeoutError, regex.error):
            continue
    return hashlib.sha256(masked.encode("utf-8", "replace")).hexdigest()[:32]


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:32]


# -- one request's edits -------------------------------------------------------------------------------------------


Hunk = list  # [text before, old, new, text after]


@dataclass
class Outcome:
    """What a request's edits did. ``summary`` is stored with the request (Requests page, Live tab); ``detail``
    (with content logging on) holds the texts, for the highlighted view."""

    ran: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    missed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    before: int = 0
    after: int = 0
    refusal: str | None = None
    detail: dict | None = None
    hits: Counter = field(default_factory=Counter)
    finals: list[tuple[int, int | None, str]] = field(default_factory=list)  # changed texts (message index, part)
    inserted: dict | None = None  # a system message added at the start

    def summary(self) -> str | None:
        """Stored with the request (whole JSON: the Live tab's filters read it)."""
        if not self.ran:
            return None
        data = {"changed": self.changed[:30], "missed": self.missed[:30], "notes": self.notes[:10]}
        data["size"] = [self.before, self.after]
        return json.dumps(data, ensure_ascii=False)

    def replay(self, messages: list) -> None:
        """The same changes on a fresh copy of the app's messages (a request forwarded to another alias)."""
        if self.inserted is not None:
            messages.insert(0, dict(self.inserted))
        for index, part, text in self.finals:
            if 0 <= index < len(messages) and isinstance(messages[index], dict):
                message = messages[index] = dict(messages[index])
                Slot(message, part).set(text)


class Run:
    """The edits of one request, applied to its messages in order."""

    def __init__(self, messages: list, record: bool) -> None:
        self.messages = messages
        self.record = record
        self.outcome = Outcome()
        self.originals = {slot.key: slot.get() for slot in slots_of(messages)}
        self.outcome.before = sum(len(t) for t in self.originals.values())
        self.steps: list[dict] = []
        self.first_spot: tuple[str, int, int] | None = None

    def slot_number(self, slot: Slot) -> int:
        return next((i for i, s in enumerate(slots_of(self.messages)) if s.key == slot.key), 0) + 1

    def note(self, text: str) -> None:
        self.outcome.notes.append(text[:300])

    def step(self, edit: Edit, slot: Slot, hunks: list[Hunk], more: int = 0) -> None:
        if self.record and hunks:
            self.steps.append({"edit": edit.label, "n": self.slot_number(slot), "hunks": hunks, "more": more})

    # -- text and pattern edits (the administrator's and those a model made)

    def substitute(self, edit: Edit, report: Edit | None = None) -> tuple[int, int]:
        """(places found, places changed) over every system text."""
        settings = edit.settings
        try:
            matcher = _matcher(edit.kind, settings.get("find", ""), settings.get("case", True))
        except (regex.error, OverflowError, RecursionError) as exc:
            self.note(f"{(report or edit).label}: its pattern can't be used ({exc})")
            return 0, 0
        found = changed = 0
        literal = edit.kind == "text"
        replace = settings.get("replace", "")
        self.first_spot = None  # (text, start, end) of its first match: where a made edit was found
        for slot in slots_of(self.messages):
            text = slot.get()
            if not text:
                continue
            spots: list[tuple[int, int, str]] = []

            def swap(match, spots=spots, text=text) -> str:
                new = replace if literal else match.expand(replace)
                spots.append((match.start(), match.end(), new))
                if self.first_spot is None:
                    self.first_spot = (text, match.start(), match.end())
                return new

            try:
                result = matcher.sub(swap, text, count=0 if settings.get("all", True) else 1, timeout=PATTERN_TIMEOUT_S)
            except TimeoutError:
                self.note(f"{(report or edit).label}: its pattern took too long and was skipped")
                continue
            except (regex.error, IndexError) as exc:
                self.note(f"{(report or edit).label}: {exc}")
                continue
            found += len(spots)
            real = [s for s in spots if text[s[0] : s[1]] != s[2]]
            if not real:
                continue
            changed += len(real)
            slot.set(result)
            hunks = [[text[max(0, a - CONTEXT) : a], text[a:b], new, text[b : b + CONTEXT]] for a, b, new in real]
            self.step(report or edit, slot, hunks[:HUNKS_MAX], max(0, len(hunks) - HUNKS_MAX))
        return found, changed

    def text_edit(self, edit: Edit) -> None:
        found, changed = self.substitute(edit)
        self.done(edit, changed > 0, missed=found == 0)

    # -- add

    def add(self, edit: Edit) -> None:
        mode, added = edit.settings.get("mode"), edit.settings.get("text", "")
        first = leading(self.messages)
        texts = [slot for message in first for slot in slots_of([message])]
        if mode == "empty" and any(slot.get().strip() for slot in texts):
            self.done(edit, False)
            return
        if not texts:  # no system prompt: one is added
            if first:  # system messages without text (an empty list of parts): the first gets it
                first[0]["content"] = added
                slot = Slot(first[0])
            else:
                message = {"role": "system", "content": added}
                self.messages.insert(0, message)
                self.outcome.inserted = message
                slot = Slot(message)
            self.step(edit, slot, [["", "", added, ""]])
            self.done(edit, True)
            return
        if mode in ("replace", "empty"):  # empty: the system prompt is there, but blank
            slot, old = texts[0], texts[0].get()
            slot.set(added)
            for other in texts[1:]:  # the rest of the system prompt goes: the text is all of it
                if other.get():
                    self.step(edit, other, [["", other.get()[:TEXT_MAX], "", ""]])
                    other.set("")
            self.step(edit, slot, [["", old, added, ""]])
        elif mode == "before":
            slot, old = texts[0], texts[0].get()
            slot.set(f"{added}\n\n{old}" if old else added)
            self.step(edit, slot, [["", "", f"{added}\n\n" if old else added, old[:CONTEXT]]])
        else:
            slot, old = texts[-1], texts[-1].get()
            slot.set(f"{old}\n\n{added}" if old else added)
            self.step(edit, slot, [[old[-CONTEXT:], "", f"\n\n{added}" if old else added, ""]])
        self.done(edit, True)

    # -- a model's rewrite

    def pieces(self, edit: Edit) -> list[tuple[Slot, str, int]]:
        """What a rewrite edit rewrites: each system text, or the parts of them its pattern finds (slot, text, where
        in the slot's text)."""
        part = edit.settings.get("part") or ""
        found: list[tuple[Slot, str, int]] = []
        for slot in slots_of(self.messages):
            text = slot.get()
            if not text.strip():
                continue
            if not part:
                found.append((slot, text, 0))
                continue
            try:
                matches = list(compile_pattern(part).finditer(text, timeout=PATTERN_TIMEOUT_S))
            except TimeoutError:
                self.note(f"{edit.label}: its pattern took too long and was skipped")
                continue
            found += [(slot, m.group(0), m.start()) for m in matches if m.group(0).strip()]
        return found

    def put(self, edit: Edit, slot: Slot, old: str, new: str, start: int = 0) -> None:
        """``old`` (found at ``start`` in the slot's text) becomes ``new``."""
        text = slot.get()
        slot.set(text[:start] + new + text[start + len(old) :])
        if self.record:
            hunks = changes(old, new)
            for hunk in hunks:
                if start and not hunk[0]:
                    hunk[0] = text[max(0, start - CONTEXT) : start]
            self.step(edit, slot, hunks[:HUNKS_MAX], max(0, len(hunks) - HUNKS_MAX))

    def done(self, edit: Edit, changed: bool, missed: bool = False) -> None:
        self.outcome.ran.append(edit.label)
        if changed:
            self.outcome.changed.append(edit.label)
            self.outcome.hits[edit.id] += 1
        elif missed:
            self.outcome.missed.append(edit.label)

    def refuse(self, reason: str) -> None:
        self.outcome.refusal = reason

    def finish(self) -> Outcome:
        slots = slots_of(self.messages)
        self.outcome.after = sum(len(s.get()) for s in slots)
        index = {id(m): i for i, m in enumerate(self.messages)}
        for slot in slots:
            text = slot.get()
            if text != self.originals.get(slot.key, None) and id(slot.message) in index:
                self.outcome.finals.append((index[id(slot.message)], slot.part, text))
        if self.record and self.steps:  # every system text, so the page shows the whole prompt
            shown = []
            for n, slot in enumerate(slots, 1):
                before, after = self.originals.get(slot.key, ""), slot.get()
                role = slot.message.get("role", "system")
                shown.append({"n": n, "role": role, "before": before, "after": after})
            self.outcome.detail = {"slots": shown, "steps": self.steps}
        return self.outcome


def changes(old: str, new: str) -> list[Hunk]:
    """The changes from ``old`` to ``new`` as hunks, each with the unchanged text around it."""
    out: list[Hunk] = []
    segments = diff_segments(old, new)
    for i, (op, a, b) in enumerate(segments):
        if op == "same":
            continue
        before = segments[i - 1][1][-CONTEXT:] if i and segments[i - 1][0] == "same" else ""
        after = segments[i + 1][1][:CONTEXT] if i + 1 < len(segments) and segments[i + 1][0] == "same" else ""
        out.append([before, a, b, after])
    return out


_WORDS = re.compile(r"\s+|\w+|[^\w\s]")


def diff_segments(old: str, new: str) -> list[tuple[str, str, str]]:
    """``old`` → ``new`` as (op, old text, new text): "same", or "swap" (old replaced by new; either may be empty).
    By line first, then by word inside changed lines, so a long prompt is quick to compare."""
    out: list[tuple[str, str, str]] = []

    def push(op: str, a: str, b: str) -> None:
        if out and out[-1][0] == op:
            out[-1] = (op, out[-1][1] + a, out[-1][2] + b)
        else:
            out.append((op, a, b))

    old_lines, new_lines = old.splitlines(keepends=True), new.splitlines(keepends=True)
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False).get_opcodes():
        a, b = "".join(old_lines[i1:i2]), "".join(new_lines[j1:j2])
        if tag == "equal":
            push("same", a, a)
            continue
        if not a or not b or len(a) + len(b) > 20_000:
            push("swap", a, b)
            continue
        old_words, new_words = _WORDS.findall(a), _WORDS.findall(b)
        words = difflib.SequenceMatcher(None, old_words, new_words, autojunk=False)
        pieces: list[tuple[str, str, str]] = []
        for wtag, k1, k2, m1, m2 in words.get_opcodes():
            wa, wb = "".join(old_words[k1:k2]), "".join(new_words[m1:m2])
            gap_only = len(pieces) > 1 and pieces[-1][0] == "same" and not pieces[-1][1].strip()
            if wtag != "equal" and gap_only and pieces[-2][0] == "swap":
                _, gap, _ = pieces.pop()  # changes apart by spaces only read as one
                _, pa, pb = pieces.pop()
                pieces.append(("swap", pa + gap + wa, pb + gap + wb))
            else:
                pieces.append(("same" if wtag == "equal" else "swap", wa, wb))
        kept = sum(len(p[1].strip()) for p in pieces if p[0] == "same")
        if kept < 0.3 * len(a.strip()):  # mostly rewritten: the lines whole read better than shreds
            pieces = [("swap", a, b)]
        for piece in pieces:
            push(*piece)
    return out


# -- what models answer --------------------------------------------------------------------------------------------


def reply_text(body: bytes | None) -> str | None:
    """The text of a chat completion's answer (thinking left out); None when there is none."""
    try:
        message = (json.loads(body or b"").get("choices") or [{}])[0].get("message") or {}
    except (ValueError, AttributeError, IndexError):
        return None
    content = message.get("content")
    if isinstance(content, list):
        content = "".join(p.get("text") or "" for p in content if isinstance(p, dict))
    if not isinstance(content, str):
        return None
    text = _THINKING.sub("", content).strip()
    return text or None


def unfenced(text: str) -> str:
    match = _FENCE.match(text)
    return match.group(1) if match else text


def rewrite_messages(edit: Edit, text: str) -> list[dict]:
    system = REWRITE_SYSTEM.format(instructions=edit.settings.get("instructions", "").strip())
    return [{"role": "system", "content": system}, {"role": "user", "content": text}]


def learn_messages(edit: Edit, unread: list[str]) -> list[dict]:
    system = LEARN_SYSTEM.format(instructions=edit.settings.get("instructions", "").strip())
    existing = []
    for child in edit.children:
        s = child.settings
        if child.kind == "keep":
            existing.append(f"- keep {json.dumps(s.get('find', ''), ensure_ascii=False)}")
        else:
            what = "find" if child.kind == "text" else "pattern"
            old, new = json.dumps(s.get("find", ""), ensure_ascii=False), json.dumps(s.get("replace", ""))
            existing.append(f"- {what} {old} → {new}{' (turned off)' if child.state == 'off' else ''}")
    shown = "\n".join(existing) or "(none yet)"
    blocks = "\n".join(f"<paragraph>\n{p}\n</paragraph>" for p in unread)
    user = f"Existing edits:\n{shown}\n\nNew paragraphs:\n{blocks}"
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def parse_made(text: str, edit: Edit, unread: list[str]) -> list[tuple[str, dict, str]] | None:
    """The edits a model made, as (kind, settings, state), checked: a find must be in the paragraphs it was given
    and a pattern must match there; one already there isn't made again. None: the answer can't be read."""
    raw = unfenced(text.strip())
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end < start:
        return None
    try:
        data = json.loads(raw[start : end + 1])
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    given = "\n\n".join(unread)
    starts = list(itertools.accumulate((len(p) + 2 for p in unread[:-1]), initial=0))

    def seen_in(start: int, end: int) -> list[str]:
        """The text around a match in ``given``, within its own paragraph (the others may not be next to it)."""
        i = bisect.bisect_right(starts, start) - 1
        return around(unread[i], start - starts[i], min(end - starts[i], len(unread[i])))

    have = {(c.kind, c.settings.get("find"), c.settings.get("replace")) for c in edit.children}
    made: list[tuple[str, dict, str]] = []
    approve = bool(edit.settings.get("approve"))
    items = data.get("edits") if isinstance(data.get("edits"), list) else []
    for item in items[:MADE_MAX]:
        if not isinstance(item, dict) or not isinstance(item.get("replace", ""), str):
            continue
        kind = "text" if isinstance(item.get("find"), str) else "pattern"
        find = item.get("find") if kind == "text" else item.get("pattern")
        if not isinstance(find, str):
            continue
        try:
            settings = clean_settings(kind, {"find": find, "replace": item.get("replace", ""), "all": "on"})
            probe = Edit(0, "", kind, settings=settings)
            run = Run([{"role": "system", "content": given}], record=False)
            found, changed = run.substitute(probe)
        except EditError:
            continue
        key = (kind, settings["find"], settings["replace"])
        if not found or key in have:
            continue
        have.add(key)
        left = run.messages[0]["content"]
        held = approve or (len(given) > 200 and len(left) < HELD_SHARE * len(given))
        if run.first_spot is not None:
            settings["seen"] = seen_in(run.first_spot[1], run.first_spot[2])
        made.append((kind, settings, "pending" if held else "on"))
    keeps = data.get("keep") if isinstance(data.get("keep"), list) else []
    for find in keeps[:MADE_MAX]:
        if not isinstance(find, str) or not find or len(find) > KEEP_MAX or ("keep", find, None) in have:
            continue
        try:
            match = check_pattern(find).search(given, timeout=PATTERN_TIMEOUT_S)
        except (EditError, TimeoutError):
            continue
        if match is None:
            continue
        have.add(("keep", find, None))
        made.append(("keep", {"find": find, "case": True, "seen": seen_in(match.start(), match.end())}, "on"))
    return made


# -- between requests ----------------------------------------------------------------------------------------------


Ask = Callable[[Edit, list[dict]], Awaitable[str | None]]  # a model edit's request: the answer's text, or None


@dataclass
class EditStore:
    """What the engine keeps in the database (each a plain function; they're run off the event loop)."""

    seen: Callable[[int], set[str]]  # the paragraph hashes a learn edit has read
    add_made: Callable[[int, list[tuple[str, dict, str]], list[str]], list[Edit]]  # made edits + the read hashes
    result: Callable[[int, str], str | None]  # a kept rewrite
    keep_result: Callable[[int, str, str], None]
    reload: Callable[[], None]  # the configuration again, with the made edits
    set_seen: Callable[[list[tuple[int, list[str]]]], None] = lambda fills: None  # where made edits were found


class Engine:
    """Runs a request's edits; keeps what learn edits have read, rewrite results, model calls in flight (requests
    with the same new text share one) and which model edits failed lately."""

    def __init__(self, keep: EditStore) -> None:
        self.keep = keep
        self.seen: dict[int, set[str]] = {}
        self.results: OrderedDict[tuple[int, str], str] = OrderedDict()
        self.calls: dict[tuple, asyncio.Future] = {}
        self.failed: dict[int, float] = {}
        self.hits: Counter = Counter()
        self.last_hit: dict[int, float] = {}
        self.tasks: set[asyncio.Task] = set()
        self.placed: set[int] = set()  # made edits whose place (``seen``) was kept since this start

    def forget(self) -> None:
        """After a change on the Edits page: read again from the database when next needed."""
        self.seen.clear()
        self.results.clear()
        self.failed.clear()

    def take_hits(self) -> dict[int, tuple[int, float]]:
        taken = {i: (n, self.last_hit.get(i, time.time())) for i, n in self.hits.items()}
        self.hits.clear()
        return taken

    async def apply(self, messages: list, chosen: list[Edit], ask: Ask, record: bool) -> Outcome:
        run = Run(messages, record)
        for edit in chosen:
            if edit.kind in ("text", "pattern"):
                run.text_edit(edit)
            elif edit.kind == "add":
                run.add(edit)
            elif edit.kind == "rewrite":
                await self.rewrite(run, edit, ask)
            elif edit.kind == "learn":
                await self.learn(run, edit, ask)
            if run.outcome.refusal:
                break
        outcome = run.finish()
        now = time.time()
        for edit_id, n in outcome.hits.items():
            self.hits[edit_id] += n
            self.last_hit[edit_id] = now
        return outcome

    def failing(self, edit: Edit) -> bool:
        at = self.failed.get(edit.id)
        return at is not None and time.monotonic() - at < RETRY_AFTER_S

    def start(self, key: tuple, make: Callable[[], Awaitable]) -> asyncio.Future:
        """One model call per key at a time: requests asking the same share it."""
        task = self.calls.get(key)
        if task is None:
            task = asyncio.ensure_future(make())
            self.calls[key] = task
            self.tasks.add(task)

            def done(t: asyncio.Future, key=key) -> None:
                self.calls.pop(key, None)
                self.tasks.discard(t)
                if not t.cancelled() and t.exception() is not None:
                    log.warning(json.dumps({"event": "prompt_edit_failed", "error": repr(t.exception())[:200]}))

            task.add_done_callback(done)
        return task

    async def waited(self, edit: Edit, task: asyncio.Future, run: Run, doing: str):
        """The call's result if the edit waits for it (None after its time limit: it goes on in the background)."""
        if not edit.wait:
            run.note(f"{edit.label}: {doing} in the background, for the next requests")
            return None
        started = time.monotonic()
        try:
            result = await asyncio.wait_for(asyncio.shield(task), edit.timeout_s)
        except TimeoutError:
            run.note(f"{edit.label}: the model took over {round(edit.timeout_s)} s; it goes on in the background")
            return None
        except Exception:  # its failure is noted by the call itself
            return None
        run.note(f"{edit.label}: waited {time.monotonic() - started:.1f} s for the model")
        return result

    # -- rewrite

    async def result_of(self, edit: Edit, key: str) -> str | None:
        found = self.results.get((edit.id, key))
        if found is not None:
            self.results.move_to_end((edit.id, key))
            return found
        found = await asyncio.to_thread(self.keep.result, edit.id, key)
        if found is not None:
            self.remember(edit.id, key, found)
        return found

    def remember(self, edit_id: int, key: str, text: str) -> None:
        self.results[(edit_id, key)] = text
        while len(self.results) > RESULTS_KEPT:
            self.results.popitem(last=False)

    async def rewrite(self, run: Run, edit: Edit, ask: Ask) -> None:
        pieces = run.pieces(edit)
        if not pieces:
            run.done(edit, False, missed=bool(edit.settings.get("part")))
            return
        changed = failed = 0
        for slot, text, at in reversed(pieces):  # from the end, so earlier places stay where they were
            key = text_hash(text)
            result = await self.result_of(edit, key)
            if result is None:
                if self.failing(edit):
                    failed += 1
                    continue

                def call(text=text, key=key):
                    return self.call_rewrite(edit, text, key, ask)

                result = await self.waited(edit, self.start(("rewrite", edit.id, key), call), run, "rewriting")
                if result is None:
                    failed += 1 if edit.wait else 0
                    continue
            if result != text:
                run.put(edit, slot, text, result, at)
                changed += 1
        if failed and edit.settings.get("on_fail") == "refuse":
            why = "its model failed or took too long"
            run.refuse(f"prompt edit '{edit.label}' couldn't rewrite the system prompt ({why})")
        elif failed:
            run.note(f"{edit.label}: sent without its rewrite (the model failed or took too long)")
        run.done(edit, changed > 0)

    async def call_rewrite(self, edit: Edit, text: str, key: str, ask: Ask) -> str | None:
        answer = await ask(edit, rewrite_messages(edit, text))
        result = unfenced(answer).strip("\n") if answer else ""
        if not result.strip():
            self.failed[edit.id] = time.monotonic()
            log.warning(json.dumps({"event": "prompt_edit_failed", "edit": edit.label, "reason": "no answer"}))
            return None
        self.failed.pop(edit.id, None)
        self.remember(edit.id, key, result)
        await asyncio.to_thread(self.keep.keep_result, edit.id, key, result)
        return result

    # -- learn

    async def read_of(self, edit: Edit) -> set[str]:
        seen = self.seen.get(edit.id)
        if seen is None:
            seen = await asyncio.to_thread(self.keep.seen, edit.id)
            self.seen[edit.id] = seen
        return seen

    async def learn(self, run: Run, edit: Edit, ask: Ask) -> None:
        seen = await self.read_of(edit)
        keeps = [c for c in edit.children if c.kind == "keep" and c.on]
        unread: dict[str, str] = {}
        size = 0
        for slot in slots_of(run.messages):
            for paragraph in paragraphs(slot.get()):
                digest = paragraph_hash(paragraph, keeps)
                if digest in seen or digest in unread or size + len(paragraph) > LEARN_CHARS:
                    continue
                unread[digest] = paragraph
                size += len(paragraph)
        mode = edit.settings.get("learn", "both")  # both / apply (no model) / learn (requests unchanged)
        made: list[Edit] = []
        if unread and mode == "apply":
            run.note(f"{edit.label}: {len(unread)} paragraphs it hasn't read (apply only: the model isn't asked)")
        elif unread and not self.failing(edit):
            key = ("learn", edit.id, tuple(sorted(unread)))
            task = self.start(key, lambda: self.call_learn(edit, list(unread.values()), ask))
            if mode == "learn":  # its edits change nothing yet: the request doesn't wait for them
                run.note(f"{edit.label}: learning only, reading {len(unread)} new paragraphs in the background")
            else:
                made = await self.waited(edit, task, run, f"reading {len(unread)} new paragraphs") or []
            if made:
                fresh = [m for m in made if m.kind != "keep"]
                held = sum(1 for m in fresh if m.state == "pending")
                run.note(
                    f"{edit.label}: the model made {len(fresh)} edits{f', {held} held for approval' if held else ''}"
                )
        elif unread:
            run.note(f"{edit.label}: {len(unread)} new paragraphs not read (its model failed lately)")
        children = [c for c in (*edit.children, *made) if c.on and c.kind in ("text", "pattern")]
        fills = self.where_kept(run, keeps)
        if mode == "learn":  # a dry run on a copy of the system messages: what its edits would have done
            copies = [dict(m) for m in run.messages if isinstance(m, dict) and m.get("role") in SYSTEM_ROLES]
            scratch = Run(copies, record=False)
            would = sum(scratch.substitute(child)[1] for child in children)
            if would:
                run.note(f"{edit.label}: learning only; its edits would have changed this request ({would} places)")
            run.done(edit, False)
        else:
            found = changed = 0
            for child in children:
                f, c = self.child_hit(run, child, edit, fills)
                found, changed = found + f, changed + c
            run.done(edit, changed > 0, missed=bool(children) and found == 0)
        if fills:
            try:
                await asyncio.to_thread(self.keep.set_seen, fills)
            except Exception as exc:  # only for its page: never fail the request over it
                log.warning(json.dumps({"event": "edit_seen_write_failed", "error": repr(exc)[:200]}))

    def child_hit(self, run: Run, child: Edit, parent: Edit, fills: list) -> tuple[int, int]:
        found, changed = run.substitute(child, report=parent)
        if changed:
            self.hits[child.id] += 1
            self.last_hit[child.id] = time.time()
        if found and run.first_spot is not None and "seen" not in child.settings and child.id not in self.placed:
            self.placed.add(child.id)  # made before 0.69: where it was found is kept now, once
            fills.append((child.id, around(*run.first_spot)))
        return found, changed

    def where_kept(self, run: Run, keeps: list[Edit]) -> list:
        """Where "keep" patterns made before 0.69 match, kept once (their edit's page shows it)."""
        fills: list = []
        for keep in keeps:
            if "seen" in keep.settings or keep.id in self.placed:
                continue
            for slot in slots_of(run.messages):
                text = slot.get()
                try:
                    match = _matcher("keep", keep.settings.get("find", ""), True).search(
                        text, timeout=PATTERN_TIMEOUT_S
                    )
                except (TimeoutError, regex.error):
                    break
                if match is not None:
                    self.placed.add(keep.id)
                    fills.append((keep.id, around(text, match.start(), match.end())))
                    break
        return fills

    async def call_learn(self, edit: Edit, unread: list[str], ask: Ask) -> list[Edit]:
        answer = await ask(edit, learn_messages(edit, unread))
        made = parse_made(answer, edit, unread) if answer else None
        if made is None:
            self.failed[edit.id] = time.monotonic()
            reason = "no answer" if not answer else "an answer that isn't the JSON asked for"
            log.warning(json.dumps({"event": "prompt_edit_failed", "edit": edit.label, "reason": reason}))
            return []
        self.failed.pop(edit.id, None)
        keeps = [c for c in edit.children if c.kind == "keep" and c.on]
        keeps += [Edit(0, "", "keep", settings=s) for kind, s, state in made if kind == "keep" and state == "on"]
        hashes = sorted({paragraph_hash(p, keeps) for p in unread})
        stored = await asyncio.to_thread(self.keep.add_made, edit.id, made, hashes)
        self.keep.reload()  # the next requests have the made edits...
        if edit.id in self.seen:  # ...and then don't read these paragraphs again
            self.seen[edit.id].update(hashes)
        return stored

    async def close(self) -> None:
        for task in list(self.tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


# -- trying an edit (Edits page) -----------------------------------------------------------------------------------


def try_edits(
    text: str, chosen: list[Edit], results: Callable[[int, str], str | None], seen: Callable[[int], set]
) -> Outcome:
    """``chosen`` on a system prompt, without asking any model: a rewrite uses its kept result (if this text has
    one), a learn edit its made edits, and the notes say what a model would be asked."""
    messages = [{"role": "system", "content": text}]
    run = Run(messages, record=True)
    for edit in chosen:
        model = edit.settings.get("model")
        if edit.kind in ("text", "pattern"):
            run.text_edit(edit)
        elif edit.kind == "add":
            run.add(edit)
        elif edit.kind == "rewrite":
            pieces = run.pieces(edit)
            changed = asked = 0
            for slot, piece, at in reversed(pieces):
                kept = results(edit.id, text_hash(piece))
                if kept is None:
                    asked += 1
                elif kept != piece:
                    run.put(edit, slot, piece, kept, at)
                    changed += 1
            if asked:
                run.note(f"{edit.label}: {asked} text(s) would go to {model} (no kept rewrite for them yet)")
            run.done(edit, changed > 0, missed=bool(edit.settings.get("part")) and not pieces)
        elif edit.kind == "learn":
            keeps = [c for c in edit.children if c.kind == "keep" and c.on]
            read = seen(edit.id)
            texts = [p for s in slots_of(run.messages) for p in paragraphs(s.get())]
            new = [p for p in texts if paragraph_hash(p, keeps) not in read]
            mode = edit.settings.get("learn", "both")
            if new and mode == "apply":
                run.note(f"{edit.label}: {len(new)} paragraph(s) it hasn't read (apply only: not sent to {model})")
            elif new:
                run.note(f"{edit.label}: {len(new)} paragraph(s) it hasn't read would go to {model}")
            if mode == "learn":
                run.note(f"{edit.label}: learning only, so requests aren't changed yet; here is what it would do")
            children = [c for c in edit.children if c.on and c.kind in ("text", "pattern")]
            found = changed = 0
            for child in children:
                f, c = run.substitute(child, report=edit)
                found, changed = found + f, changed + c
            run.done(edit, changed > 0, missed=bool(children) and found == 0)
    return run.finish()
