"""An alias's display name (what opencode's model list and the Models page show) and renaming an alias in place,
with what QueueRouter keeps that names it following."""

import json
import sqlite3

import httpx
import pytest

from app.config import ClientPolicy
from app.store import Store
from tests.test_admin import OWNER, csrf_of, login
from tests.test_nested_aliases import save_alias

HI = [{"role": "user", "content": "hi"}]


async def resave(c, alias_id, **fields):
    """The alias editor's form for an alias that sends to mock-model, saved with ``fields``."""
    csrf = csrf_of((await c.get("/admin/models")).text)
    form = {"csrf": csrf, "target_model": "mock-model", "on_errors": "on", "enabled": "on", **fields}
    return await c.post(f"/admin/aliases/{alias_id}", data=form)


def test_renaming_an_alias_takes_what_names_it_along(tmp_path):
    store = Store.open(str(tmp_path), None)
    plain = store.create_alias("plain", "mock-model", (), "t")
    store.create_alias(
        "outer",
        "plain",
        (),
        "t",
        fallbacks=("plain", "other"),
        council="pick",
        council_members=("plain", "mock-model"),
        council_judge="plain",
        council_roles="plain = skeptic: doubt it\nmock-model = fan\nplainly = other",
        compact="summarise",
        compact_model="plain",
        check_model="plain",
    )
    group = store.create_group("g", "t", ClientPolicy(allowed_models=("plain", "mock-model")))
    edit = store.create_edit("tone", "rewrite", {"model": "plain", "instructions": "x"}, "t")
    other = store.create_edit("cut", "rewrite", {"model": "plain@upstream", "instructions": "x"}, "t")
    job = {"id": "j1", "client": "c", "endpoint": "/v1/chat/completions", "request": {"model": "plain"}}
    store.create_jobs([job])
    store.create_schedule(
        {"id": "s1", "client": "c", "name": "n", "cron": "* * * * *", "endpoint": "/v1/chat/completions",
         "request": {"model": "plain", "messages": []}, "next_run": 0}
    )  # fmt: skip
    store.set_model_score("plain", "t", 50.0)

    store.rename_alias(plain, "simple", "t")
    aliases = {a.name: a for a in store.list_aliases()}
    assert set(aliases) == {"simple", "outer"}
    outer = aliases["outer"]
    assert outer.target_model == "simple" and outer.fallbacks == ("simple", "other")
    assert outer.council_members == ("simple", "mock-model") and outer.council_judge == "simple"
    assert outer.council_roles == "simple = skeptic: doubt it\nmock-model = fan\nplainly = other"
    assert outer.compact_model == "simple" and outer.check_model == "simple"
    assert next(g for g in store.list_groups() if g.id == group).policy.allowed_models == ("simple", "mock-model")
    edits = {e.id: e for e in store.list_edits()}
    assert edits[edit].settings["model"] == "simple"
    assert edits[other].settings["model"] == "plain@upstream"  # a provider's model by that name: not the alias
    assert json.loads(store.decrypt(store.get_job("j1")["request"]))["model"] == "simple"
    assert json.loads(store.decrypt(store.get_schedule("s1")["request"]))["model"] == "simple"
    assert (0, "simple") in store.list_model_scores() and (0, "plain") not in store.list_model_scores()

    # a name another alias has: nothing changes
    with pytest.raises(sqlite3.IntegrityError):
        store.rename_alias(plain, "outer", "t")
    assert {a.name for a in store.list_aliases()} == {"simple", "outer"}
    # the old name was also a provider's model: the settings by that name are the model's, and stay
    store.rename_alias(plain, "basic", "t", own_settings=False)
    assert (0, "simple") in store.list_model_scores() and (0, "basic") not in store.list_model_scores()


async def test_a_display_name_and_a_rename_in_the_editor(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await save_alias(c, "plain", "mock-model", label="  Coder   GLM ")
        await save_alias(c, "outer", "plain")
        plain = next(a for a in store.list_aliases() if a.name == "plain")
        assert plain.label == "Coder GLM"  # spaces are fine (runs of them made one)
        models = (await c.get("/v1/opencode-config", headers=stack.headers)).json()["provider"]["queuerouter"]
        assert "Coder GLM" in models["models"]["plain"]["name"]  # shown by its display name; apps send "plain"
        page = (await c.get("/admin/models")).text
        assert "<strong>Coder GLM</strong> <code>plain</code>" in page

        r = await resave(c, plain.id, name="plain", label="a|b")
        assert "A display name is up to 64 characters, without |." in r.text
        r = await resave(c, plain.id, name="has space", label="Coder GLM")
        assert "An alias name uses letters" in r.text
        r = await resave(c, plain.id, name="outer", label="Coder GLM")
        assert "That name is already taken." in r.text

        r = await resave(c, plain.id, name="simple", label="Coder GLM")
        assert "m=saved" in r.headers["location"]
        aliases = {a.name: a for a in store.list_aliases()}
        assert set(aliases) == {"simple", "outer"} and aliases["outer"].target_model == "simple"
        ok = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "simple", "messages": HI})
        assert ok.status_code == 200
        forwarded = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "outer", "messages": HI})
        assert forwarded.status_code == 200  # the alias that used the old name uses the new one
        gone = await c.post("/v1/chat/completions", headers=stack.headers, json={"model": "plain", "messages": HI})
        assert gone.status_code == 404
