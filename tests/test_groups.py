"""Client groups: the models and limits of several clients at once (a plan); a client's own limit wins."""

import json

import httpx
from cryptography.fernet import Fernet

from app.config import ClientPolicy, with_group
from app.store import MIGRATIONS, Store
from tests.test_admin import OWNER, csrf_of, login
from tests.test_policies import chat, set_group, set_policy


def test_a_client_gets_its_groups_models_and_its_limits_where_it_has_none():
    group = ClientPolicy(allowed_models=("a", "b"), rpm=60, on_limit="reject", month_tokens=1000, params='{"x": 1}')
    own = ClientPolicy(allowed_models=("c",), rpm=5, tags="mine", user_agents=("app/",))
    got = with_group(own, group)
    assert got.allowed_models == ("a", "b")  # always the group's
    assert (got.rpm, got.on_limit, got.month_tokens, got.params) == (5, "reject", 1000, '{"x": 1}')
    assert (got.tags, got.user_agents) == ("mine", ("app/",))  # the key's own
    assert with_group(ClientPolicy(on_limit="delay"), group).on_limit == "delay"  # set: wins over the group's
    assert with_group(ClientPolicy(max_queued=0), ClientPolicy(max_queued=9)).max_queued == 0  # 0 is a value
    assert with_group(own, None) == own


def test_the_upgrade_turns_model_lists_into_groups(tmp_path):
    store = Store(str(tmp_path / "old.db"), Fernet(Fernet.generate_key()))
    for version, script in enumerate(MIGRATIONS[:37], 1):
        store._db.executescript(f"BEGIN;\n{script}\nPRAGMA user_version={version};\nCOMMIT;")
    before = {
        "ali": {"allowed_models": ["fast", "glm-4.7"], "rpm": 10, "on_limit": "delay", "loop_action": "warn"},
        "bilal": {"allowed_models": ["fast", "glm-4.7"], "on_limit": "reject"},
        "Everything": {"allowed_models": ["smart"]},
        "dana": {"allowed_models": [], "tpd": 5},
        "eve": {},
    }
    for name, policy in before.items():
        store._db.execute(
            "INSERT INTO clients (name, key_hash, key_prefix, policy, created_at) VALUES (?, ?, 'qr-', ?, 0)",
            (name, f"hash-{name}", json.dumps(policy)),
        )
    store.migrate()
    assert store.schema_version() == len(MIGRATIONS)
    groups = {g.id: g for g in store.list_groups()}
    clients = {c.name: c for c in store.list_clients()}
    in_group = {name: groups[c.group_id].name for name, c in clients.items()}
    assert in_group == {
        "ali": "ali",  # the first client with that list
        "bilal": "ali",
        "Everything": "Everything",
        "dana": "All models",
        "eve": "All models",
    }
    assert groups[clients["ali"].group_id].policy.allowed_models == ("fast", "glm-4.7")
    assert groups[clients["eve"].group_id].policy == ClientPolicy()
    ali = clients["ali"].policy
    assert ali.allowed_models == () and (ali.rpm, ali.on_limit, ali.loop_action) == (10, "", "")
    for name, client in clients.items():  # what each client gets is what it had
        had = ClientPolicy.from_json(json.dumps(before[name]))
        gets = with_group(client.policy, groups[client.group_id].policy)
        assert (gets.allowed_models, gets.rpm, gets.tpd) == (had.allowed_models, had.rpm, had.tpd)
        assert (gets.on_limit or "delay", gets.loop_action or "warn") == (had.on_limit or "delay", "warn")


async def test_a_groups_models_and_limits_reach_its_clients_at_once(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    ask = lambda tag: c.post("/v1/chat/completions", headers=stack.headers, json=chat(tag))  # noqa: E731
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        [first] = store.list_groups()
        assert first.name == "All models" and store.list_clients()[0].group_id == first.id  # ACCESS_KEYS' client

        await set_policy(c, stack, allowed_models="other-model")  # client1 moves to a group with that model
        assert (await ask("a")).status_code == 403
        await set_group(c, stack, "client1-models", models="other-model, mock-model", month_requests=1)
        assert (await ask("b")).status_code == 200
        r = await ask("c")
        assert r.status_code == 429 and r.json()["error"]["type"] == "monthly_quota_exceeded"
        await set_policy(c, stack, month_requests=5)  # the client's own wins
        assert (await ask("d")).status_code == 200

        page = (await c.get("/admin/clients")).text
        assert "client1-models" in page and 'placeholder="group: 1"' in page
        await set_group(c, stack, "client1-models")  # every model, new ones too; no limits
        assert "mock-model" in [m["id"] for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]]
        assert store.list_groups()[1].policy == ClientPolicy()


async def test_creating_editing_and_deleting_groups(make_stack):
    stack = make_stack(bootstrap=OWNER)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        csrf = csrf_of((await c.get("/admin/groups/new")).text)
        empty = await c.post("/admin/groups", data={"csrf": csrf, "name": "Nothing", "models": ""})
        assert empty.status_code == 200 and "Pick at least one model" in empty.text  # shown again, not saved
        assert 'value="Nothing"' in empty.text and len(store.list_groups()) == 1
        taken = await c.post("/admin/groups", data={"csrf": csrf, "name": "All models", "every_model": "on"})
        assert "already taken" in taken.text

        coding = await set_group(c, stack, "Coding", models="mock-model", rpm=30, on_limit="reject")
        group = next(g for g in store.list_groups() if g.id == coding)
        assert (group.policy.allowed_models, group.policy.rpm, group.policy.on_limit) == (("mock-model",), 30, "reject")
        editor = (await c.get(f"/admin/groups/{coding}")).text
        assert "mock-model" in editor and 'value="30"' in editor

        new = {"csrf": csrf, "name": "newbie", "priority": "normal", "group_id": coding}
        assert (await c.post("/admin/clients", data=new)).status_code == 200
        assert next(x for x in store.list_clients() if x.name == "newbie").group_id == coding
        r = await c.post("/admin/clients", data={"csrf": csrf, "name": "lost", "priority": "normal", "group_id": "999"})
        assert "e=invalid" in r.headers["location"]

        r = await c.post(f"/admin/groups/{coding}/delete", data={"csrf": csrf})
        assert "e=group_in_use" in r.headers["location"]
        everyone = next(g.id for g in store.list_groups() if g.name == "All models")
        newbie = next(x for x in store.list_clients() if x.name == "newbie")
        form = {"csrf": csrf, "name": "newbie", "priority": "normal", "enabled": "on", "group_id": everyone}
        assert "m=saved" in (await c.post(f"/admin/clients/{newbie.id}", data=form)).headers["location"]
        assert "m=deleted" in (await c.post(f"/admin/groups/{coding}/delete", data={"csrf": csrf})).headers["location"]
        r = await c.post(f"/admin/groups/{everyone}/delete", data={"csrf": csrf})
        assert "e=group_in_use" in r.headers["location"]
        assert [g.name for g in store.list_groups()] == ["All models"]
