"""Strength order for the opencode model list (app/ranking.py): index matching, numbering, the config."""

import httpx

from app import ranking
from tests.test_admin import OWNER, login
from tests.test_fallback import add_chain


def index_entry(name, slug, intelligence, coding=None):
    scores = {"intelligence": intelligence, **({"coding": coding} if coding is not None else {})}
    return {"name": name, "slug": slug, "creator": "", "scores": scores}


def book():
    scores = ranking.Scores()
    scores.use(
        [
            index_entry("GLM-4.6 (Reasoning)", "glm-4-6-reasoning", 56),
            index_entry("GLM-4.6 (Non-reasoning)", "glm-4-6", 45),
            index_entry("GLM-4.6V", "glm-4-6v", 40),
            index_entry("GLM-4", "glm-4", 20),
            index_entry("DeepSeek V4.1 Flash", "deepseek-v4-1-flash", 60, coding=50),
            index_entry("Kimi K3.1 Pro", "kimi-k3-1-pro", 70),
        ],
        0,
    )
    return scores


def test_names_match_their_base_model():
    scores = book()
    assert ranking.bare("GLM-4.6-Derestricted-v5") == ("glm46", True)
    assert scores.score("GLM-4.6-Derestricted-v5", "intelligence") == (56, "GLM-4.6 (Reasoning)", True)
    assert scores.score("GLM-4.6", "intelligence") == (56, "GLM-4.6 (Reasoning)", False)  # the stronger variant
    assert scores.score("GLM-4.6V-Flash", "intelligence")[1] == "GLM-4.6V"  # not GLM-4.6
    assert scores.score("zai-org/GLM-4.7", "intelligence") is None  # GLM-4 doesn't claim it
    assert scores.score("DeepSeek-V4.1-Flash-1017", "coding") == (50, "DeepSeek V4.1 Flash", False)
    assert scores.score("moonshotai/kimi-k3.1-pro", "intelligence")[0] == 70
    assert scores.score("deepseek-v4.1-flash-free", "coding") == (50, "DeepSeek V4.1 Flash", False)
    assert scores.score("nvidia/kimi-k3.1-pro:free", "intelligence")[0] == 70
    assert scores.score("Kimi K3.1 Pro", "agentic") is None  # that index isn't there
    order = ranking.numbered([("a", 10), ("b", 30), ("c", 30)], ["team"], ["z", "y"])
    assert order == [("team", "00"), ("b", "01"), ("c", "02"), ("a", "03"), ("y", ""), ("z", "")]
    assert ranking.label("03", 45.1, False, "m · up") == "03 · 45.1 · m · up"
    assert ranking.label("04", 56, True, "GLM-4.6-Derestricted-v5") == "04 · ~56.0 · GLM-4.6-Derestricted-v5"
    assert ranking.label("00", None, False, "team → m") == "00 · team → m"
    assert ranking.label("", None, False, "z") == "z"


async def test_index_is_read_page_by_page(tmp_path):
    pages = {
        "1": {"data": [{"name": "A", "slug": "a", "evaluations": {"artificial_analysis_intelligence_index": 50}}],
              "pagination": {"has_more": True}},
        "2": {"data": [{"name": "B", "slug": "b", "evaluations": {"artificial_analysis_coding_index": 40}},
                       {"name": "C", "slug": "c", "evaluations": {}}],
              "pagination": {"has_more": False}},
    }  # fmt: skip
    seen = []

    def handler(request):
        seen.append((request.url.params["page"], request.headers["x-api-key"]))
        return httpx.Response(200, json=pages[request.url.params["page"]])

    scores = ranking.Scores(str(tmp_path / "scores.json"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await scores.refresh(client, "k")
    assert seen == [("1", "k"), ("2", "k")] and [m["name"] for m in scores.models] == ["A", "B"]
    again = ranking.Scores(str(tmp_path / "scores.json"))
    assert again.load_file() and again.score("B", "coding")[0] == 40

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401))) as client:
        assert not await scores.refresh(client, "bad")
    assert scores.error == "HTTP 401" and len(scores.models) == 2  # the last good copy is kept


async def test_opencode_list_is_numbered_by_strength(make_stack):
    stack = make_stack(bootstrap=OWNER, opencode_rank=True, models_cache_s=0)
    stack.mock["extra_models"] = ["small-model", "other-model"]
    store = stack.app.state.store
    store.set_model_score("mock-model", "t", 70)
    store.set_model_score("small-model", "t", 50)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_chain(c, "smart", "mock-model", ["other-model"], ["errors"])
        models = (await c.get("/v1/opencode-config", headers=stack.headers)).json()["provider"]["queuerouter"]["models"]
        assert list(models) == ["smart", "mock-model", "small-model", "other-model"]
        assert models["smart"]["name"].startswith("00 · smart")
        assert models["mock-model"]["name"].startswith("01 · 70.0 · mock-model")
        assert models["small-model"]["name"].startswith("02 · 50.0 · small-model")
        assert not models["other-model"]["name"][0].isdigit()
        page = (await c.get("/admin/models")).text
        assert "Strength order in opencode" in page and "Artificial Analysis" in page and "Thinking levels" in page


async def test_settings_page_saves_the_ranking_switch(make_stack):
    """The whole Settings form as a browser sends it (every field as rendered), with the switch ticked."""
    import re

    from tests.test_admin import csrf_of

    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        page = (await c.get("/admin/settings")).text
        form = {"csrf": csrf_of(page)}
        for tag in re.findall(r"<input[^>]*>", page):
            name = re.search(r'name="([^"]+)"', tag)
            if not name or name.group(1) == "csrf":
                continue
            kind = re.search(r'type="([^"]+)"', tag).group(1)
            if kind == "checkbox":
                if "checked" in tag:
                    form[name.group(1)] = "on"
            elif kind != "password":
                form[name.group(1)] = re.search(r'value="([^"]*)"', tag).group(1) if "value=" in tag else ""
        for select in re.findall(r'<select name="([^"]+)">(.*?)</select>', page, re.S):
            chosen = re.search(r'<option value="([^"]+)" selected', select[1]) or re.search(
                r'value="([^"]+)"', select[1]
            )
            form[select[0]] = chosen.group(1)
        form["opencode_rank"] = "on"
        r = await c.post("/admin/settings", data=form)
        assert "m=saved" in r.headers["location"], r.headers["location"]
        assert stack.app.state.store.get_tunables()["opencode_rank"] is True
        assert "ordering is off" not in (await c.get("/admin/models")).text
