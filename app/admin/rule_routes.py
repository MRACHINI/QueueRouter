"""The Rules page: answer rules (app/answer_rules.py), their editor, and trying them on a pasted answer."""

from __future__ import annotations

import sqlite3
import time
from types import SimpleNamespace

from fastapi import FastAPI, Request

from .. import answer_rules as rules_mod
from ..answer_rules import KIND_LABELS, KINDS, RULES_PER_ALIAS, SCOPE_LABELS, SCOPES, RuleError
from ..store import Store
from .edit_routes import Helpers

TRY_MAX = 100_000  # characters of an answer to try the rules on
STALE_S = 30 * 86_400  # a rule in use that hasn't failed an answer for this long is marked "quiet"


def rule_choices(store: Store) -> list[dict]:
    """The rules, for the picker in the alias editor."""
    return [
        {"id": str(r.id), "label": r.name, "note": KIND_LABELS.get(r.kind, r.kind) + ("" if r.enabled else " · off")}
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

    def editor(request: Request, session, rule, error: str | None = None):
        return page(
            request,
            "rule_edit.html",
            session,
            r=rule,
            new=rule.id is None,
            error=error,
            kinds=KINDS,
            kind_labels=KIND_LABELS,
            scopes=SCOPES,
            scope_labels=SCOPE_LABELS,
            users=store.rule_users().get(rule.id, []) if rule.id is not None else [],
            start_chars=rules_mod.START_CHARS,
        )

    def draft(form: dict, rule_id: int | None = None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""
        return SimpleNamespace(
            id=rule_id,
            name=str(form.get("name", "")),
            kind=str(form.get("kind", "contains")),
            find=str(form.get("find", "")),
            case=form.get("case") == "on",
            scope=str(form.get("scope", "start")),
            enabled=form.get("enabled") == "on" or rule_id is None,
            notes=str(form.get("notes", "")),
            hits=0,
            last_hit=None,
        )

    def listing(request: Request, session, tried: dict | None = None):
        aliases = [a for a in store.list_aliases() if a.answer_rules]
        return page(
            request,
            "rules.html",
            session,
            rules=store.list_rules(),
            kind_labels=KIND_LABELS,
            users=store.rule_users(),
            aliases=aliases,
            quiet_before=time.time() - STALE_S,
            tried=tried,
            start_chars=rules_mod.START_CHARS,
        )

    @app.get("/admin/rules")
    async def rules_page(request: Request):
        session = require(request, "admin")
        return listing(request, session)

    @app.post("/admin/rules/try")
    async def rules_try(request: Request):
        """The rules on a pasted answer, as an alias tries them (its rules, in its order) or all of them: which
        match, and the first (the one that fails the answer)."""
        session, form = await require_post(request, "admin")
        answer = str(form.get("answer", ""))[:TRY_MAX].replace("\r\n", "\n")
        alias_name = str(form.get("alias", ""))
        every = {r.id: r for r in store.list_rules()}
        alias = next((a for a in store.list_aliases() if a.name == alias_name), None)
        order = [every[i] for i in alias.answer_rules if i in every] if alias else list(every.values())
        rows = [{"rule": r, "match": r.enabled and rules_mod.matches(r, answer)} for r in order]
        first = rules_mod.first_match(order, answer)
        tried = {"answer": answer, "alias": alias_name, "rows": rows, "first": first.name if first else None}
        return listing(request, session, tried)

    @app.get("/admin/rules/new")
    async def rule_new(request: Request):
        session = require(request, "admin")
        return editor(request, session, draft({"kind": "contains", "scope": "start"}))

    @app.post("/admin/rules")
    async def rules_create(request: Request):
        session, form = await require_post(request, "admin")
        try:
            fields = rules_mod.clean_rule(form)
            store.create_rule(session.user.username, **fields)
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
            fields = rules_mod.clean_rule(form)
            store.update_rule(rule_id, session.user.username, enabled=form.get("enabled") == "on", **fields)
        except RuleError as exc:
            return editor(request, session, draft(form, rule_id), str(exc))
        except sqlite3.IntegrityError:
            return editor(request, session, draft(form, rule_id), "That name is already taken.")
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
