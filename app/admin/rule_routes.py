"""The Rules page: answer rules (app/answer_rules.py), their editor, each alias's rule maker (app/rule_maker.py), and
trying the rules on a pasted answer."""

from __future__ import annotations

import sqlite3
import time
from types import SimpleNamespace

from fastapi import FastAPI, Request

from .. import answer_rules as rules_mod
from ..answer_rules import (
    ACTION_LABELS,
    ACTION_SHORT,
    ACTION_WOULD,
    ACTIONS,
    KIND_LABELS,
    KINDS,
    LEARN_LABELS,
    RULES_PER_ALIAS,
    SCOPE_LABELS,
    SCOPES,
    STATE_LABELS,
    STATES,
    RuleError,
)
from ..store import Store
from .edit_routes import Helpers

TRY_MAX = 100_000  # characters of an answer to try the rules on
STALE_S = 30 * 86_400  # a rule in use that hasn't decided an answer for this long is marked "quiet"

MESSAGES = {
    "proposing": "Its rule maker is at work; its rules show here when it's done (a minute or so).",
    "samples_forgotten": "Its kept answers are gone.",
}


def rule_choices(store: Store) -> list[dict]:
    """The rules, for the picker in the alias editor."""
    tags = {t.id: t for t in store.list_tags()}
    return [
        {
            "id": str(r.id),
            "label": r.name,
            "note": f"{KIND_LABELS.get(r.kind, r.kind)}, {ACTION_SHORT.get(r.action, r.action)}"
            + (f" {tags[r.tag].name}" if r.tags and r.tag in tags else "")
            + ("" if r.state == "on" else f" · {STATE_LABELS.get(r.state, r.state)}"),
        }
        for r in store.list_rules()
    ]


def rule_ids(form: dict, store: Store) -> tuple[int, ...]:
    """The rules picked in a form (comma-separated ids), in order; ones that don't exist are left out."""
    known = {r.id for r in store.list_rules()}
    picked = [int(v) for v in str(form.get("answer_rules", "")).replace("\n", ",").split(",") if v.strip().isdecimal()]
    ids = tuple(i for i in dict.fromkeys(picked) if i in known)
    if len(ids) > RULES_PER_ALIAS:
        raise ValueError("answer_rules")
    return ids


def mount_rule_pages(app: FastAPI, store: Store, h: Helpers) -> None:
    page, require, require_post, back = h.page, h.require, h.require_post, h.back

    def find(rule_id: int):
        return next((r for r in store.list_rules() if r.id == rule_id), None)

    def labels() -> dict:
        return {
            "kinds": KINDS,
            "kind_labels": KIND_LABELS,
            "scopes": SCOPES,
            "scope_labels": SCOPE_LABELS,
            "actions": ACTIONS,
            "action_labels": ACTION_LABELS,
            "action_short": ACTION_SHORT,
            "action_would": ACTION_WOULD,
            "states": STATES,
            "state_labels": STATE_LABELS,
            "start_chars": rules_mod.START_CHARS,
        }

    def editor(request: Request, session, rule, error: str | None = None):
        aliases = {a.id: a.name for a in store.list_aliases()}
        return page(
            request,
            "rule_edit.html",
            session,
            r=rule,
            new=rule.id is None,
            error=error,
            users=store.rule_users().get(rule.id, []) if rule.id is not None else [],
            made_for=aliases.get(rule.made_by) if getattr(rule, "made_by", None) is not None else None,
            tags=store.list_tags(),
            **labels(),
        )

    def cleaned(form: dict) -> dict:
        return rules_mod.clean_rule(form, [t.id for t in store.list_tags()])

    def draft(form: dict, rule_id: int | None = None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""
        return SimpleNamespace(
            id=rule_id,
            name=str(form.get("name", "")),
            kind=str(form.get("kind", "contains")),
            find=str(form.get("find", "")),
            case=form.get("case") == "on",
            scope=str(form.get("scope", "start")),
            action=str(form.get("action", "fail")),
            negate=form.get("negate") == "on",
            state=str(form.get("state", "on")),
            notes=str(form.get("notes", "")),
            made_by=None,
            why="",
            tag=int(form["tag"]) if str(form.get("tag", "")).isdecimal() else None,
        )

    def listing(request: Request, session, tried: dict | None = None, note: str | None = None):
        aliases = store.list_aliases()
        names = {a.id: a.name for a in aliases}
        learning = store.rule_learning()
        counts = store.sample_counts()
        running = getattr(request.app.state, "makers_running", set())
        makers = [
            {
                "alias": a,
                "mode": LEARN_LABELS.get(a.learn_rules, a.learn_rules),
                "fails": counts.get(a.id, {}).get("fail", 0),
                "passes": counts.get(a.id, {}).get("pass", 0),
                "state": learning.get(a.id, {}),
                "running": a.id in running,
                "checker": a.checks,
            }
            for a in aliases
            if a.learn_rules != "off"
        ]
        return page(
            request,
            "rules.html",
            session,
            rules=store.list_rules(),
            users=store.rule_users(),
            aliases=[a for a in aliases if a.answer_rules],
            made_for=names,
            makers=makers,
            quiet_before=time.time() - STALE_S,
            tried=tried,
            note=note,
            tag_names={t.id: t.name for t in store.list_tags()},
            **labels(),
        )

    @app.get("/admin/rules")
    async def rules_page(request: Request):
        session = require(request, "admin")
        return listing(request, session)

    @app.post("/admin/rules/try")
    async def rules_try(request: Request):
        """The rules on a pasted answer, as an alias tries them (its rules, in its order) or all of them: which
        match, which decides and how, and what the ones watching would have done."""
        session, form = await require_post(request, "admin")
        answer = str(form.get("answer", ""))[:TRY_MAX].replace("\r\n", "\n")
        alias_name = str(form.get("alias", ""))
        every = {r.id: r for r in store.list_rules()}
        alias = next((a for a in store.list_aliases() if a.name == alias_name), None)
        order = [every[i] for i in alias.answer_rules if i in every] if alias else list(every.values())
        decision = rules_mod.decide(order, answer)
        rows = [{"rule": r, "match": r.state != "off" and rules_mod.matches(r, answer)} for r in order]
        tried = {
            "answer": answer,
            "alias": alias_name,
            "rows": rows,
            "decides": decision.rule,
            "tagged": decision.tagged,
        }
        return listing(request, session, tried)

    @app.post("/admin/rules/propose")
    async def rules_propose(request: Request):
        """An alias's rule maker, now (in the background)."""
        session, form = await require_post(request, "admin")
        try:
            alias_id = int(str(form.get("alias_id", "")))
        except ValueError:
            return back("/admin/rules", error="invalid")
        start = getattr(request.app.state, "start_rule_maker", None)
        why = start(alias_id) if start else "the rule maker isn't available"
        if why:
            return listing(request, session, note=f"Its rule maker didn't start: {why}.")
        store.audit(session.user.username, "rules.propose", str(alias_id))
        return back("/admin/rules", "proposing")

    @app.post("/admin/rules/samples/{alias_id}/forget")
    async def rules_forget_samples(alias_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.forget_samples(alias_id, session.user.username)
        return back("/admin/rules", "samples_forgotten")

    @app.get("/admin/rules/new")
    async def rule_new(request: Request):
        session = require(request, "admin")
        return editor(request, session, draft({"kind": "contains", "scope": "start", "action": "fail", "state": "on"}))

    @app.post("/admin/rules")
    async def rules_create(request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.create_rule(session.user.username, **cleaned(form))
        except RuleError as exc:
            return editor(request, session, draft(form), str(exc))
        except sqlite3.IntegrityError:
            return editor(request, session, draft(form), "That name is already taken.")
        h.reload()
        return back("/admin/rules", "created")

    @app.get("/admin/rules/{rule_id}")
    async def rule_page(rule_id: int, request: Request):
        session = require(request, "admin")
        rule = find(rule_id)
        if rule is None:
            return back("/admin/rules", error="invalid")
        return editor(request, session, rule)

    @app.post("/admin/rules/{rule_id}")
    async def rules_update(rule_id: int, request: Request):
        session, form = await require_post(request, "admin")
        if find(rule_id) is None:
            return back("/admin/rules", error="invalid")
        try:
            store.update_rule(rule_id, session.user.username, **cleaned(form))
        except RuleError as exc:
            return editor(request, session, draft(form, rule_id), str(exc))
        except sqlite3.IntegrityError:
            return editor(request, session, draft(form, rule_id), "That name is already taken.")
        h.reload()
        return back("/admin/rules", "saved")

    @app.post("/admin/rules/{rule_id}/state")
    async def rules_state(rule_id: int, request: Request):
        """On (turning on a rule the model made is approving it), watching or off."""
        session, form = await require_post(request, "admin")
        state = str(form.get("state", ""))
        rule = find(rule_id)
        if state not in STATES or (rule is not None and rule.tags and state == "watch"):  # a rule that tags: on or off
            return back("/admin/rules", error="invalid")
        try:
            store.set_rule_state(rule_id, session.user.username, state)
        except KeyError:
            return back("/admin/rules", error="invalid")
        h.reload()
        return back("/admin/rules", "saved")

    @app.post("/admin/rules/{rule_id}/delete")
    async def rules_delete(rule_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_rule(rule_id, session.user.username)
        except KeyError:
            return back("/admin/rules", error="invalid")
        h.reload()
        return back("/admin/rules", "deleted")
