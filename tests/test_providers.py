"""Refuseless and Synthetic (their presets, thinking levels, model list fields), a thinking level fitted to what the
model has (a forced "max" is its highest), an out-of-credit 402 handled as a used-up quota, plans of any provider,
and switching an upstream to a known provider."""

import json

import httpx

from app import thinking
from app.presets import PRESETS, all_plans, plan_choice
from tests.test_admin import OWNER, csrf_of, login
from tests.test_upstreams import add_upstream, chat, spread

ABLITERATED = {
    "id": "abl-model",
    "object": "model",
    "reasoning_efforts": ["off", "low", "high"],
    "reasoning_default": "low",
    "vision": True,
    "context_length": 131072,
    "pricing": {"input_per_m": 0.5, "output_per_m": 2.0, "cached_input_per_m": 0.05},
}


def test_refuseless_and_synthetic_levels():
    assert thinking.ordered(["max", "OFF", "low"]) == ("none", "low", "max")  # "off" is "none"
    assert thinking.from_refuseless(ABLITERATED) == (("none", "low", "high"), "low")
    assert thinking.from_refuseless({"id": "x"}) is None
    assert thinking.style_of("https://api.refuseless.com/v1", None) == "refuseless"
    assert thinking.style_of("https://api.synthetic.new/openai/v1", None) == "synthetic"
    assert thinking.style_of("http://proxy:8080/v1", None, "refuseless") == "refuseless"  # by its provider
    assert thinking.style_of("http://proxy:8080/v1", None, "custom") == "unknown"
    found = thinking.from_refuseless(ABLITERATED)
    assert thinking.for_style("refuseless", found) == (("none", "none"), ("low", "low"), ("high", "high"))
    assert thinking.for_style("synthetic", (("none", "low", "high", "max"), None)) == (("low", "low"), ("high", "high"))
    assert thinking.for_style("synthetic", (("none", "high"), None)) == ()  # one level and no off: nothing to pick
    for asked, sent in (("none", "off"), ("medium", "low"), ("xhigh", "high"), ("max", "max"), ("minimal", "low")):
        payload = {"reasoning_effort": asked}
        assert thinking.translate(payload, "refuseless") and payload == {"reasoning_effort": sent}
    for asked, sent in (("max", "high"), ("minimal", "low"), ("medium", "medium")):
        payload = {"reasoning_effort": asked}
        assert thinking.translate(payload, "synthetic") and payload == {"reasoning_effort": sent}
    payload = {"reasoning_effort": "none"}
    assert thinking.translate(payload, "synthetic") and payload == {}  # no off switch: its default
    assert thinking.takes_images({"vision": False}) is False and thinking.takes_images(ABLITERATED) is True


def test_presets_and_plans():
    assert PRESETS["refuseless"].base_url == "https://api.refuseless.com/v1"
    assert PRESETS["synthetic"].base_url == "https://api.synthetic.new/openai/v1"
    values = {value: label for value, label, _ in all_plans()}
    assert values["synthetic:Standard"] == "Synthetic Standard" and values["arliai:Pro"] == "ArliAI Pro"
    label, plan = plan_choice("synthetic:Big")
    assert label == "Synthetic Big" and plan.monthly_price == 90
    assert plan_choice("Pro")[0] == "ArliAI Pro"  # a bare name is ArliAI's, as before
    assert plan_choice("synthetic:Huge") is None and plan_choice("") is None


async def as_refuseless(c, stack):
    """The stack's own upstream, switched to the Refuseless provider in its row on the Upstreams page."""
    upstream = stack.app.state.store.list_upstreams()[0]
    csrf = csrf_of((await c.get("/admin/upstreams")).text)
    form = {"csrf": csrf, "preset": "refuseless", "enabled": "on", "weight": "1"}
    r = await c.post(f"/admin/upstreams/{upstream.id}", data=form)
    assert "m=saved" in r.headers["location"]
    return upstream


async def test_a_refuseless_model_list_and_its_levels(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_model_entries"] = [ABLITERATED]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        upstream = await as_refuseless(c, stack)
        changed = next(u for u in stack.app.state.store.list_upstreams() if u.id == upstream.id)
        assert changed.preset == "refuseless" and changed.base_url == upstream.base_url  # its own address kept
        config = (await c.get("/v1/opencode-config?plugin=2", headers=stack.headers)).json()
        entry = config["provider"]["queuerouter"]["models"]["abl-model"]
        assert entry["thinking"]["variants"] == [["none", "none"], ["low", "low"], ["high", "high"]]
        assert entry["cost"] == {"input": 0.5, "output": 2.0, "cache_read": 0.05}  # its own prices
        sent = []
        for asked in ("max", "none", "medium", "high"):
            body = {"model": "abl-model", "messages": [{"role": "user", "content": "hi"}], "reasoning_effort": asked}
            assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
            sent.append(json.loads(stack.mock["last_body"]).get("reasoning_effort"))
        assert sent == ["high", "off", "low", "high"]  # its own levels: "max" is its highest
        rows = [e for e in stack.app.state.stats.recent if e.get("model") == "abl-model"]
        assert rows[0]["thinking"] == "max → high (the nearest abl-model has on upstream)"
        assert rows[-1]["thinking"] is None  # a level it has: unchanged


async def test_a_forced_max_is_each_models_highest(make_stack):
    stack = make_stack(bootstrap=OWNER, models_cache_s=0)
    stack.mock["extra_model_entries"] = [ABLITERATED]
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await as_refuseless(c, stack)
        csrf = csrf_of((await c.get("/admin/models")).text)
        form = {"csrf": csrf, "name": "deep", "target_model": "abl-model"}
        form["params"] = json.dumps({"overrides": {"reasoning_effort": "max"}})
        assert "m=created" in (await c.post("/admin/aliases", data=form)).headers["location"]
        assert "abl-model" in (await c.get("/v1/models", headers=stack.headers)).text  # its list, read again
        body = {"model": "deep", "messages": [{"role": "user", "content": "hi"}], "reasoning_effort": "low"}
        assert (await c.post("/v1/chat/completions", headers=stack.headers, json=body)).status_code == 200
        assert json.loads(stack.mock["last_body"])["reasoning_effort"] == "high"


async def test_taking_a_providers_defaults(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        upstream = stack.app.state.store.list_upstreams()[0]
        csrf = csrf_of((await c.get("/admin/upstreams")).text)
        # a provider whose address goes nowhere from here (a real one's would get the test's key)
        form = {"csrf": csrf, "preset": "ollama", "preset_defaults": "on", "enabled": "on", "weight": "1"}
        form["params"] = '{"strip": ["top_k"]}'
        assert "m=saved" in (await c.post(f"/admin/upstreams/{upstream.id}", data=form)).headers["location"]
        changed = next(u for u in stack.app.state.store.list_upstreams() if u.id == upstream.id)
        assert changed.preset == "ollama" and changed.base_url == "http://ollama:11434/v1"
        assert changed.params == ""  # the provider's own rules (none), in place of the row's
        r = await c.post(f"/admin/upstreams/{upstream.id}", data={**form, "preset": "nope"})
        assert "e=invalid" in r.headers["location"]
        page = (await c.get("/admin/upstreams")).text
        assert '<option value="ollama" selected>Ollama</option>' in page
        assert '<option value="synthetic:Go"' in page  # Synthetic's plans in the pickers


async def test_out_of_credit_moves_to_another_upstream(make_stack, make_mock):
    stack = make_stack(limit=1, bootstrap=OWNER, lb_strategy="ordered")
    other_url, other = make_mock(limit=1)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await login(c, stack.url)
        await add_upstream(c, "backup", other_url)
        model = await spread(c)
        stack.mock["chat_status"] = 402  # out of credits on the first upstream
        api = f"{stack.url}/v1/chat/completions"
        for i in range(2):
            r = await c.post(api, headers=stack.headers, json=chat(f"c{i}", model=model))
            assert r.status_code == 200, r.text
        assert other["served"] == 2
        rows = [e for e in stack.app.state.stats.recent if e.get("model") == model and not e.get("parent")]
        assert rows[0]["error"].startswith("402 from upstream")
        assert rows[1]["error"] is None  # it's skipped now, as a used-up quota is: not asked again
