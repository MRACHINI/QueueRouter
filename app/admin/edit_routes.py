"""The Edits pages: prompt edits (app/edits.py), what models made under them, trying them on a prompt, the Live tab
of requests they changed, and a request's changes highlighted."""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .. import edits as edits_mod
from ..edits import ADD_LABELS, ADD_MODES, KIND_LABELS, KINDS, LEARN_LABELS, LEARN_MODES, EditError
from ..store import Store

EDIT_NAME = re.compile(r"[^\x00-\x1f\x7f]{1,64}")
CHECKBOXES = ("all", "case", "wait", "approve")
LIVE_PAGE, LIVE_CATCH_UP = 50, 300  # rows fetched at a time / at most after being away
FOLD_OVER, FOLD_KEEP = 900, 240  # an unchanged run longer than this shows its ends only (the rest opens)
TRY_MAX = 400_000  # characters of a prompt to try edits on
EDITS_PER_OWNER = 30  # edits one alias, group or client may use
STALE_S = 7 * 86_400  # an edit in use that hasn't changed a request for this long is marked "stale"

MESSAGES = {
    "approved": "The held edits are on.",
    "reread": "It reads every paragraph again with the next request.",
    "forgotten": "Its made edits are gone; it reads every paragraph again with the next request.",
    "results_forgotten": "Its kept rewrites are gone; each text is rewritten again when next seen.",
}


@dataclass
class Helpers:
    """What the admin pages share (app/admin/routes.py mount_admin)."""

    page: Callable[..., HTMLResponse]
    require: Callable
    require_post: Callable
    back: Callable[..., RedirectResponse]
    reload: Callable[[], None]
    model_choices: Callable[[], list[dict]]
    model_label: Callable[[str], str]


def edit_choices(store: Store) -> list[dict]:
    """The edits, for the pickers in the alias, group and client editors."""
    return [
        {"id": str(e.id), "label": e.name, "note": KIND_LABELS.get(e.kind, e.kind) + ("" if e.on else " · off")}
        for e in store.list_edits()
    ]


def edit_ids(form: dict, store: Store) -> tuple[int, ...]:
    """The edits picked in a form (comma-separated ids), in order; ones that don't exist are left out."""
    known = {e.id for e in store.list_edits()}
    picked = [int(v) for v in str(form.get("edits", "")).replace("\n", ",").split(",") if v.strip().isdecimal()]
    ids = tuple(i for i in dict.fromkeys(picked) if i in known)
    if len(ids) > EDITS_PER_OWNER:
        raise ValueError("edits")
    return ids


def view_segments(before: str, after: str) -> list[tuple]:
    """A text's changes for a page: ("same", text), ("fold", start, middle, end) for a long unchanged run, or
    ("swap", removed, added)."""
    shown: list[tuple] = []
    for op, a, b in edits_mod.diff_segments(before, after):
        if op == "same" and len(a) > FOLD_OVER:
            shown.append(("fold", a[:FOLD_KEEP], a[FOLD_KEEP:-FOLD_KEEP], a[-FOLD_KEEP:]))
        else:
            shown.append((op, a, b))
    return shown


def changes_view(detail: dict | None) -> dict | None:
    """A request's (or a try's) recorded changes, ready for edit_changes.html."""
    if not detail:
        return None
    slots = [
        {**slot, "segments": view_segments(slot.get("before", ""), slot.get("after", ""))}
        for slot in detail.get("slots", [])
        if isinstance(slot, dict)
    ]
    steps = [step for step in detail.get("steps", []) if isinstance(step, dict)]
    return {"slots": slots, "steps": steps}


def summary_of(text: str | None) -> dict:
    try:
        data = json.loads(text or "{}")
    except ValueError:
        data = {}
    return data if isinstance(data, dict) else {}


def settings_form(kind: str, form: dict) -> dict:
    """The settings fields of the editor's form, checkboxes as on / off (an unticked one isn't sent at all)."""
    raw = {key: form.get(key) for key in form}
    for box in CHECKBOXES:
        raw[box] = "on" if form.get(box) == "on" else "off"
    return edits_mod.clean_settings(kind, raw)


def mount_edit_pages(app: FastAPI, store: Store, h: Helpers) -> None:
    page, require, require_post, back = h.page, h.require, h.require_post, h.back

    def draft(form: dict, edit_id: int | None = None) -> SimpleNamespace:
        """What was submitted, to show the editor again after an error (nothing is saved)."""
        settings = {key: form.get(key, "") for key in form}
        for box in CHECKBOXES:
            settings[box] = form.get(box) == "on"
        kind = str(form.get("kind", "text"))
        return SimpleNamespace(
            id=edit_id,
            name=str(form.get("name", "")).strip(),
            kind=kind if kind in KINDS else "text",
            state="on" if form.get("enabled") == "on" or edit_id is None else "off",
            settings=settings,
            notes=str(form.get("notes", "")),
            children=(),
            on=form.get("enabled") == "on",
        )

    def blank(kind: str) -> SimpleNamespace:
        settings = {"all": True, "case": True, "wait": True, "approve": False, "timeout_s": 60, "on_fail": "keep"}
        settings["mode"] = "after"
        return SimpleNamespace(
            id=None, name="", kind=kind, state="on", settings=settings, notes="", children=(), on=True
        )

    def editor(request: Request, session, edit, error: str | None = None, tried: dict | None = None):
        made = list(edit.children) if edit.id is not None else []
        logged = []
        if session.user.can_view_content:
            logged = [dict(r) for r in store.logged_requests()]
        return page(
            request,
            "edit_edit.html",
            session,
            e=edit,
            new=edit.id is None,
            error=error,
            kinds=KINDS,
            kind_labels=KIND_LABELS,
            add_modes=ADD_MODES,
            add_labels=ADD_LABELS,
            learn_modes=LEARN_MODES,
            learn_labels=LEARN_LABELS,
            choices=h.model_choices(),
            users=store.edit_users().get(edit.id, []) if edit.id is not None else [],
            made=made,
            held=sum(1 for m in made if m.state == "pending"),
            read=store.edit_seen_count().get(edit.id, 0) if edit.id is not None else 0,
            kept=store.edit_result_count().get(edit.id, 0) if edit.id is not None else 0,
            tried=tried,
            logged=logged,
            may_read=session.user.can_view_content,
            stale_before=time.time() - STALE_S,
        )

    def find(edit_id: int):
        return next((e for e in store.list_edits() if e.id == edit_id), None)

    # -- the list, and the Live tab ---------------------------------------------------------------------------------

    @app.get("/admin/edits")
    async def edits_page(request: Request):
        session = require(request, "admin")
        every = store.list_edits()
        stale_before = time.time() - STALE_S
        users = store.edit_users()
        return page(
            request, "edits.html", session, edits=every, kind_labels=KIND_LABELS, users=users, stale_before=stale_before
        )

    @app.get("/admin/edits/live")
    async def edits_live(request: Request):
        session = require(request, "admin")
        clients = sorted(c.name for c in store.list_clients())
        names = [e.name for e in store.list_edits()]
        query = request.query_params
        chosen = {"edit": query.get("edit", ""), "client": query.get("client", ""), "missed": query.get("missed", "")}
        return page(
            request, "edits_live.html", session, clients=clients, names=names, chosen=chosen, catch_up=LIVE_CATCH_UP
        )

    @app.get("/admin/api/edits/live")
    async def api_edits_live(request: Request):
        """Requests whose prompt edits ran, newest first: a page of the newest, the page older than a row
        (``?before=<ts>&before_id=<id>``) or those written since a time (``?after=<ts>``)."""
        session = require(request, "admin")
        query = request.query_params
        try:
            after = float(query["after"]) if "after" in query else None
            before = (float(query["before"]), query.get("before_id", "")) if "before" in query else None
        except ValueError:
            return JSONResponse({"error": {"message": "after and before are timestamps", "code": 400}}, 400)
        limit = LIVE_CATCH_UP if after is not None else LIVE_PAGE
        filters = {
            "client": query.get("client", ""),
            "edit": query.get("edit", ""),
            "missed": bool(query.get("missed")),
        }

        def load() -> dict:
            rows = store.edited_requests(limit + 1, after, before, **filters)
            ids = [r["id"] for r in rows[:limit]]
            logged = store.content_ids(ids) if session.user.can_view_content else set()
            shown = []
            for r in rows[:limit]:
                summary = summary_of(r["edits"])
                shown.append(
                    {
                        "id": r["id"],
                        "ts": r["ts"],
                        "client": r["client"],
                        "model": h.model_label(r["model"] or ""),
                        "target": h.model_label(r["target_model"]) if r["target_model"] else None,
                        "via": r["via"],
                        "outcome": r["outcome"],
                        "changed": summary.get("changed") or [],
                        "missed": summary.get("missed") or [],
                        "notes": summary.get("notes") or [],
                        "size": summary.get("size") or [0, 0],
                        "view": r["id"] in logged,
                    }
                )
            return {"rows": shown, "more": len(rows) > limit}

        return JSONResponse(await asyncio.to_thread(load))

    @app.get("/admin/requests/{request_id}/edits")
    async def request_edits(request_id: str, request: Request):
        """A request's prompt edits: its system prompt with every change highlighted, then each edit's own."""
        session = require(request, "admin")
        if not session.user.can_view_content:
            return back("/admin/edits/live", error="content_missing")
        content = await asyncio.to_thread(store.get_content, request_id[:40])
        if content is None or not content.get("edits"):
            return back("/admin/edits/live", error="content_missing")
        store.audit(session.user.username, "content.view", request_id, {"client": content["client"], "edits": True})
        row = await asyncio.to_thread(store.request_row, request_id[:40])
        summary = summary_of(row["edits"] if row else None)
        view = changes_view(content["edits"])
        return page(request, "request_edits.html", session, content=content, summary=summary, view=view)

    # -- one edit ---------------------------------------------------------------------------------------------------

    @app.get("/admin/edits/new")
    async def edit_new(request: Request):
        session = require(request, "admin")
        kind = request.query_params.get("kind", "text")
        return editor(request, session, blank(kind if kind in KINDS else "text"))

    @app.post("/admin/edits")
    async def edits_create(request: Request):
        session, form = await require_post(request, "admin")
        kind, name = str(form.get("kind", "")), str(form.get("name", "")).strip()
        try:
            if not EDIT_NAME.fullmatch(name):
                raise EditError("Give the edit a name (up to 64 characters).")
            settings = settings_form(kind, form)
            notes = str(form.get("notes", "")).strip()[:500]
            edit_id = store.create_edit(name, kind, settings, session.user.username, notes)
        except EditError as exc:
            return editor(request, session, draft(form), str(exc))
        except sqlite3.IntegrityError:
            return editor(request, session, draft(form), "That name is already taken.")
        h.reload()
        return back(f"/admin/edits/{edit_id}", "created")

    @app.get("/admin/edits/{edit_id}")
    async def edit_page(edit_id: int, request: Request):
        session = require(request, "admin")
        edit = find(edit_id)
        if edit is None:
            return back("/admin/edits", error="invalid")
        return editor(request, session, edit)

    @app.post("/admin/edits/{edit_id}")
    async def edits_update(edit_id: int, request: Request):
        session, form = await require_post(request, "admin")
        edit = find(edit_id)
        if edit is None:
            return back("/admin/edits", error="invalid")
        kind, name = str(form.get("kind", "")), str(form.get("name", "")).strip()
        try:
            if not EDIT_NAME.fullmatch(name):
                raise EditError("Give the edit a name (up to 64 characters).")
            settings = settings_form(kind, form)
            fields = {"name": name, "kind": kind, "settings": settings}
            fields["state"] = "on" if form.get("enabled") == "on" else "off"
            fields["notes"] = str(form.get("notes", "")).strip()[:500]
            store.update_edit(edit_id, session.user.username, **fields)
        except EditError as exc:
            return editor(request, session, draft(form, edit_id), str(exc))
        except sqlite3.IntegrityError:
            return editor(request, session, draft(form, edit_id), "That name is already taken.")
        if edit.kind == "learn" and kind != "learn":  # what its model made belongs to a learn edit
            store.delete_made_edit(edit_id, None, session.user.username)
        h.reload()
        return back(f"/admin/edits/{edit_id}", "saved")

    @app.post("/admin/edits/{edit_id}/delete")
    async def edits_delete(edit_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        try:
            store.delete_edit(edit_id, session.user.username)
        except KeyError:
            return back("/admin/edits", error="invalid")
        h.reload()
        return back("/admin/edits", "deleted")

    @app.post("/admin/edits/{edit_id}/try")
    async def edits_try(edit_id: int, request: Request):
        """The edit on a prompt pasted here or taken from a logged request, without asking any model."""
        session, form = await require_post(request, "admin")
        edit = find(edit_id)
        if edit is None:
            return back("/admin/edits", error="invalid")
        text = str(form.get("sample", "")).replace("\r\n", "\n")[:TRY_MAX]
        source = str(form.get("from_request", "")).strip()[:40]
        if source and session.user.can_view_content:
            content = await asyncio.to_thread(store.get_content, source)
            if content is not None:
                store.audit(session.user.username, "content.view", source, {"client": content["client"], "try": True})
                text = system_prompt_of(content)
        outcome = await asyncio.to_thread(edits_mod.try_edits, text, [edit], store.edit_result, store.edit_seen)
        detail = outcome.detail or {
            "slots": [{"n": 1, "role": "system", "before": text, "after": text}] if text else [],
            "steps": [],
        }
        tried = {
            "text": text,
            "view": changes_view(detail),
            "changed": bool(outcome.changed),
            "missed": bool(outcome.missed),
            "notes": outcome.notes,
            "size": (outcome.before, outcome.after),
        }
        return editor(request, session, edit, tried=tried)

    # -- what a learn edit's model made -----------------------------------------------------------------------------

    @app.post("/admin/edits/{edit_id}/made/{made_id}")
    async def made_update(edit_id: int, made_id: int, request: Request):
        session, form = await require_post(request, "admin")
        edit = find(edit_id)
        child = next((c for c in edit.children if c.id == made_id), None) if edit else None
        if child is None:
            return back("/admin/edits", error="invalid")
        action = str(form.get("action", "save"))
        try:
            if action in ("on", "off"):
                store.update_made_edit(edit_id, made_id, session.user.username, state=action)
            elif action == "delete":
                store.delete_made_edit(edit_id, made_id, session.user.username)
            else:
                kept = {"all": "on", "case": "on" if child.settings.get("case", True) else ""}
                settings = settings_form(child.kind, {**form, **kept})
                if child.settings.get("seen"):  # where the model found it, as it was
                    settings["seen"] = child.settings["seen"]
                store.update_made_edit(edit_id, made_id, session.user.username, settings=settings)
        except EditError as exc:
            return editor(request, session, edit, f"Made edit #{made_id}: {exc}")
        except KeyError:
            return back(f"/admin/edits/{edit_id}", error="invalid")
        h.reload()
        return back(f"/admin/edits/{edit_id}#made", "saved")

    @app.post("/admin/edits/{edit_id}/approve")
    async def made_approve(edit_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.approve_made_edits(edit_id, session.user.username)
        h.reload()
        return back(f"/admin/edits/{edit_id}#made", "approved")

    @app.post("/admin/edits/{edit_id}/reread")
    async def made_reread(edit_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.forget_read(edit_id, session.user.username)
        h.reload()
        return back(f"/admin/edits/{edit_id}#made", "reread")

    @app.post("/admin/edits/{edit_id}/forget")
    async def made_forget(edit_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.delete_made_edit(edit_id, None, session.user.username)
        h.reload()
        return back(f"/admin/edits/{edit_id}#made", "forgotten")

    @app.post("/admin/edits/{edit_id}/forget-results")
    async def results_forget(edit_id: int, request: Request):
        session, _ = await require_post(request, "admin")
        store.forget_edit_results(edit_id, session.user.username)
        h.reload()
        return back(f"/admin/edits/{edit_id}", "results_forgotten")


def system_prompt_of(content: dict) -> str:
    """The system prompt a logged request came with: as the app sent it when its edits were recorded, else as
    it was sent on (the logged request)."""
    recorded = (content.get("edits") or {}).get("slots") or []
    befores = [str(s.get("before", "")) for s in recorded if isinstance(s, dict) and s.get("before")]
    if befores:
        return "\n\n".join(befores)
    try:
        messages = json.loads(content.get("request") or "{}").get("messages") or []
    except (ValueError, AttributeError):
        return ""
    return "\n\n".join(slot.get() for slot in edits_mod.slots_of(edits_mod.leading(messages)))
