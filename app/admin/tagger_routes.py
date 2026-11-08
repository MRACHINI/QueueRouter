"""The Taggers page: tags, taggers (app/taggers.py), their editors, trying a tagger on a pasted answer, and the chain of
taggers in the alias editor."""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

from fastapi import FastAPI, Request

from .. import taggers as taggers_mod
from ..answer_rules import ANSWER_KIND_LABELS, ANSWER_KINDS
from ..store import Store
from ..taggers import (
    DO_LABELS,
    ENDS,
    LINES_MAX,
    PICK_LABELS,
    PICKS,
    STEPS_MAX,
    TURNS_MAX,
    WHEN_LABELS,
    WHENS,
    TaggerError,
)
from .edit_routes import Helpers

TRY_MAX = 100_000  # characters of a pasted message, system prompt or answer
BLANK_LINES = 2  # empty decisions shown under each step of the alias editor's chain, to add more

MESSAGES = {
    "tag_in_use": "A rule gives that tag: change the rule (or delete it) first.",
}


def chain_from(form: dict, store: Store) -> tuple[taggers_mod.Step, ...]:
    """An alias's chain from its editor; TaggerError says what's wrong. A form without it (no editor) has none."""
    taggers = {t.id: t for t in store.list_taggers()}
    tags = {t.id: t for t in store.list_tags()}
    return taggers_mod.clean_chain(taggers_mod.chain_rows(form), taggers, tags)


def chain_slots(rows: list[dict]) -> list[dict]:
    """The editor's rows, with an empty step to add a tagger (up to STEPS_MAX) and empty decisions under each."""
    shown = [dict(r) for r in rows if r.get("tagger")]
    if len(shown) < STEPS_MAX:
        shown.append({"tagger": "", "order": str(len(shown) + 1), "lines": [], "otherwise": "next", "no_reply": "next"})
    for row in shown:
        lines = [ln for ln in row.get("lines") or () if str(ln.get("tags") or "").strip()]
        blanks = max(0, min(BLANK_LINES, LINES_MAX - len(lines)))
        row["lines"] = lines + [{"when": "any", "tags": "", "do": "next"} for _ in range(blanks)]
    return shown


def chain_context(store: Store) -> dict:
    """What the alias editor's chain needs to show its choices."""
    return {
        "taggers": store.list_taggers(),
        "tag_names": [t.name for t in store.list_tags()],
        "whens": WHENS,
        "when_labels": WHEN_LABELS,
        "ends": ENDS,
        "do_labels": DO_LABELS,
        "steps_max": STEPS_MAX,
    }


def mount_tagger_pages(app: FastAPI, store: Store, h: Helpers) -> None:
    page, require, require_post, back = h.page, h.require, h.require_post, h.back

    def labels() -> dict:
        return {
            "answer_kinds": ANSWER_KINDS,
            "answer_kind_labels": ANSWER_KIND_LABELS,
            "picks": PICKS,
            "pick_labels": PICK_LABELS,
            "turns_max": TURNS_MAX,
            "default_prompt": taggers_mod.DEFAULT_PROMPT,
        }

    def listing(request: Request, session):
        tags = store.list_tags()
        taggers = store.list_taggers()
        by_id = {t.id: t for t in tags}
        giving: dict[int, list[str]] = {}
        for tagger in taggers:
            for t in tagger.tags:
                giving.setdefault(t, []).append(tagger.name)
        rules: dict[int, list[str]] = {}
        for rule in store.list_rules():
            if rule.tags and rule.tag is not None:
                rules.setdefault(rule.tag, []).append(rule.name)
        return page(
            request,
            "taggers.html",
            session,
            tags=tags,
            taggers=taggers,
            tag_by_id=by_id,
            giving=giving,
            rules=rules,
            users=store.tagger_users(),
            model_label=h.model_label,
            **labels(),
        )

    @app.get("/admin/taggers")
    async def taggers_page(request: Request):
        session = require(request, "admin")
        return listing(request, session)

    # -- tags --------------------------------------------------------------------------------------------------------

    def tag_editor(request: Request, session, tag, error: str | None = None):
        return page(request, "tag_edit.html", session, t=tag, new=tag.id is None, error=error)

    def tag_draft(form: dict, tag_id: int | None = None) -> SimpleNamespace:
        return SimpleNamespace(id=tag_id, name=str(form.get("name", "")), meaning=str(form.get("meaning", "")))

    @app.get("/admin/tags/new")
    async def tag_new(request: Request):
        session = require(request, "admin")
        return tag_editor(request, session, tag_draft({}))

    @app.post("/admin/tags")
    async def tags_create(request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.create_tag(session.user.username, **taggers_mod.clean_tag(form))
        except TaggerError as exc:
            return tag_editor(request, session, tag_draft(form), str(exc))
        except sqlite3.IntegrityError:
            return tag_editor(request, session, tag_draft(form), "That name is already taken.")
        h.reload()
        return back("/admin/taggers", "created")

    @app.get("/admin/tags/{tag_id}")
    async def tag_page(tag_id: int, request: Request):
        session = require(request, "admin")
        tag = next((t for t in store.list_tags() if t.id == tag_id), None)
        if tag is None:
            return back("/admin/taggers", error="invalid")
        return tag_editor(request, session, tag)

    @app.post("/admin/tags/{tag_id}")
    async def tags_update(tag_id: int, request: Request):
        session, form = await require_post(request, "admin")
        try:
            store.update_tag(tag_id, session.user.username, **taggers_mod.clean_tag(form))
        except TaggerError as exc:
            return tag_editor(request, session, tag_draft(form, tag_id), str(exc))
        except sqlite3.IntegrityError:
            return tag_editor(request, session, tag_draft(form, tag_id), "That name is already taken.")
        except KeyError:
            return back("/admin/taggers", error="invalid")
        h.reload()
        return back("/admin/taggers", "saved")

    @app.post("/admin/tags/{tag_id}/delete")
    async def tags_delete(tag_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_tag(tag_id, session.user.username)
        except KeyError:
            return back("/admin/taggers", error="invalid")
        except ValueError:
            return back("/admin/taggers", error="tag_in_use")
        h.reload()
        return back("/admin/taggers", "deleted")

    # -- taggers -----------------------------------------------------------------------------------------------------

    def tagger_editor(request: Request, session, tagger, error: str | None = None, tried: dict | None = None):
        clients = [c for c in store.list_clients() if c.enabled and not c.expired]
        return page(
            request,
            "tagger_edit.html",
            session,
            t=tagger,
            new=tagger.id is None,
            error=error,
            tags=store.list_tags(),
            users=store.tagger_users().get(tagger.id, []) if tagger.id is not None else [],
            choices=h.model_choices(),
            clients=clients,
            tried=tried,
            **labels(),
        )

    def tagger_draft(form: dict, tagger_id: int | None = None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""
        return SimpleNamespace(
            id=tagger_id,
            name=str(form.get("name", "")),
            model=str(form.get("model", "")).strip(),
            instructions=str(form.get("instructions", "")),
            prompt=str(form.get("prompt", "")),
            tags=tuple(int(v) for v in form.get("tags", ()) if str(v).isdecimal()),
            pick=str(form.get("pick", "any")),
            reads=tuple(k for k in ANSWER_KINDS if k in form.get("reads", ())),
            message=form.get("message") == "on",
            turns=str(form.get("turns", "0")),
            system=form.get("system") == "on",
            reason=form.get("reason") == "on",
            notes=str(form.get("notes", "")),
        )

    async def tagger_form(request: Request, role: str) -> tuple:
        """The session and the editor's fields, with the ticked tags and answer types as lists."""
        session, form = await require_post(request, role)
        raw = await request.form()
        fields = dict(form)
        fields["tags"] = [str(v) for v in raw.getlist("tags")]
        fields["reads"] = [str(v) for v in raw.getlist("reads")]
        return session, fields

    @app.get("/admin/taggers/new")
    async def tagger_new(request: Request):
        session = require(request, "admin")
        empty = tagger_draft({"pick": "one", "message": "on", "reason": "on"})
        empty.reads = ANSWER_KINDS
        return tagger_editor(request, session, empty)

    @app.post("/admin/taggers")
    async def taggers_create(request: Request):
        session, form = await tagger_form(request, "admin")
        tag_ids = [t.id for t in store.list_tags()]
        try:
            store.create_tagger(session.user.username, **taggers_mod.clean_tagger(form, tag_ids))
        except TaggerError as exc:
            return tagger_editor(request, session, tagger_draft(form), str(exc))
        except sqlite3.IntegrityError:
            return tagger_editor(request, session, tagger_draft(form), "That name is already taken.")
        h.reload()
        return back("/admin/taggers", "created")

    def find(tagger_id: int):
        return next((t for t in store.list_taggers() if t.id == tagger_id), None)

    @app.get("/admin/taggers/{tagger_id}")
    async def tagger_page(tagger_id: int, request: Request):
        session = require(request, "admin")
        tagger = find(tagger_id)
        if tagger is None:
            return back("/admin/taggers", error="invalid")
        return tagger_editor(request, session, tagger)

    @app.post("/admin/taggers/{tagger_id}")
    async def taggers_update(tagger_id: int, request: Request):
        session, form = await tagger_form(request, "admin")
        if find(tagger_id) is None:
            return back("/admin/taggers", error="invalid")
        tag_ids = [t.id for t in store.list_tags()]
        try:
            store.update_tagger(tagger_id, session.user.username, **taggers_mod.clean_tagger(form, tag_ids))
        except TaggerError as exc:
            return tagger_editor(request, session, tagger_draft(form, tagger_id), str(exc))
        except sqlite3.IntegrityError:
            return tagger_editor(request, session, tagger_draft(form, tagger_id), "That name is already taken.")
        h.reload()
        return back("/admin/taggers", "saved")

    @app.post("/admin/taggers/{tagger_id}/delete")
    async def taggers_delete(tagger_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_tagger(tagger_id, session.user.username)
        except KeyError:
            return back("/admin/taggers", error="invalid")
        h.reload()
        return back("/admin/taggers", "deleted")

    @app.post("/admin/taggers/{tagger_id}/try")
    async def taggers_try(tagger_id: int, request: Request):
        """The tagger as saved, on a pasted message and answer, asked for real (as the client picked): what its
        model was sent, what it replied, and the tags read from it."""
        session, form = await require_post(request, "admin")
        tagger = find(tagger_id)
        if tagger is None:
            return back("/admin/taggers", error="invalid")
        model = " ".join(str(form.get("try_model", "")).split())[:200] or tagger.model
        kind = str(form.get("try_kind", "text"))
        kind = kind if kind in ANSWER_KINDS else "text"
        message = str(form.get("try_message", ""))[:TRY_MAX].replace("\r\n", "\n")
        system = str(form.get("try_system", ""))[:TRY_MAX].replace("\r\n", "\n")
        answer = str(form.get("try_answer", ""))[:TRY_MAX].replace("\r\n", "\n")
        client = str(form.get("try_client", ""))
        tried = {"model": model, "kind": kind, "message": message, "system": system, "answer": answer, "client": client}
        if not model:
            return tagger_editor(request, session, tagger, tried=tried | {"problem": "Name the model to ask."})
        messages = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": message}]
        body = json.dumps({"messages": messages}).encode()
        tags = {t.id: t for t in store.list_tags()}
        payload = taggers_mod.payload(model, tagger, tags, body, answer, kind)
        tried["sent"] = payload["messages"]
        run = getattr(request.app.state, "try_tagger", None)
        if run is None:
            return tagger_editor(request, session, tagger, tried=tried | {"problem": "Trying isn't available."})
        store.audit(session.user.username, "tagger.try", tagger.name, {"model": model, "client": client})
        replied, why = await run(client, payload)
        if replied is None:
            return tagger_editor(request, session, tagger, tried=tried | {"problem": f"No reply: {why}."})
        try:
            choice = (json.loads(replied).get("choices") or [{}])[0]
            text = str((choice.get("message") or {}).get("content") or "")
        except (ValueError, AttributeError, IndexError):
            text = ""
        reply = taggers_mod.reply_of(replied, tagger, tags)
        tried.update(
            reply=text,
            tags=taggers_mod.names(reply.tags or (), tags) if reply.tags is not None else None,
            reason=reply.reason,
            ignored=reply.ignored,
            unreadable=reply.problem,
        )
        return tagger_editor(request, session, tagger, tried=tried)
