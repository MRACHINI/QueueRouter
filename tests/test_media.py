"""Images on Qwen Cloud through OpenAI's image API (app/media.py): the request in Qwen's own form, its answer as
OpenAI's (links, or base64 fetched), the price per image on the answer and the request, image and video models
marked in /v1/models."""

import base64

import httpx

from app import media
from tests.conftest import eventually
from tests.test_admin import OWNER, login
from tests.test_usage import flushed


def qwen(stack, *models: str) -> None:
    store = stack.app.state.store
    store.update_upstream(store.list_upstreams()[0].id, "t", preset="qwen")
    stack.mock["extra_model_entries"] = [{"id": m, "object": "model"} for m in models]
    stack.app.state.reload()


def test_what_a_model_makes():
    assert media.kind("qwen-image-3.0-pro", None) == "image"
    assert media.kind("wan2.7-image", None) == "image" and media.kind("gpt-image-1", None) == "image"
    for video in ("wan3.0-video", "wan2.7-t2v", "happyhorse-1.0-video-edit", "vidu/viduq2-pro-fast_img2video"):
        assert media.kind(video, None) == "video", video
    for chat in ("qwen4-max", "qwen3-vl-plus", "deepseek-v4-flash", "mock-model"):
        assert media.kind(chat, None) is None, chat
    assert media.kind("anything", {"architecture": {"output_modalities": ["image"]}}) == "image"  # its list says
    assert media.kind("text-image-x", {"output_modalities": ["text", "image"]}) is None  # it writes too: chat
    assert media.marked({"id": "a"}, "wan2.7-image") == {"id": "a", "output_modalities": ["image"]}
    assert media.marked({"id": "b"}, "qwen4-max") == {"id": "b"}


def test_an_image_request_in_qwens_form():
    body = media.image_request(
        {
            "model": "wan2.7-image",
            "prompt": "an apple",
            "n": 2,
            "size": "1536x1024",
            "response_format": "b64_json",
            "seed": 7,
            "input_references": [{"type": "image_url", "url": "data:image/png;base64,AAAA"}],
            "image": "https://example.com/a.png",
        }
    )
    assert body == {
        "model": "wan2.7-image",
        "input": {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"text": "an apple"},
                        {"image": "https://example.com/a.png"},
                        {"image": "data:image/png;base64,AAAA"},
                    ],
                }
            ]
        },
        "parameters": {"watermark": False, "n": 2, "size": "1536*1024", "seed": 7},
    }
    assert media.image_request({"model": "m", "prompt": "x", "size": "2K"})["parameters"]["size"] == "2K"
    assert "size" not in media.image_request({"model": "m", "prompt": "x", "size": "auto"})["parameters"]
    try:
        media.image_request({"model": "m", "prompt": " "})
        raise AssertionError("no prompt was taken")
    except media.Unsupported:
        pass


def test_image_prices():
    assert media.image_cost("qwen-image-3.0-pro", 1, "1024*1024", 0) == 0.04
    assert media.image_cost("qwen-image-3.0-pro", 2, "2048*2048", 1) == 0.153  # 2K, and an input image
    assert media.image_cost("wan2.7-image", 3, "2K", 0) == 0.09
    assert media.image_cost("someone-else-image", 1, "1024*1024", 0) is None
    assert media.api_root("https://token-plan.maas.qwencloudapi.com/compatible-mode/v1") == (
        "https://token-plan.maas.qwencloudapi.com/api/v1"
    )


async def test_images_through_qwens_own_api(make_stack):
    stack = make_stack(models_cache_s=0, bootstrap=OWNER)
    qwen(stack, "qwen-image-3.0-pro", "wan2.7-image", "wan3.0-video")
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        listed = {m["id"]: m for m in (await c.get("/v1/models", headers=stack.headers)).json()["data"]}
        assert listed["qwen-image-3.0-pro"]["output_modalities"] == ["image"]  # oh-my-pi's image tool takes it
        assert listed["wan3.0-video"]["output_modalities"] == ["video"]
        assert "output_modalities" not in listed["mock-model"]

        # as oh-my-pi asks: base64, which QueueRouter fetches from the link Qwen gives
        ask = {"model": "qwen-image-3.0-pro", "prompt": "an apple", "n": 1, "size": "1024x1024"}
        r = await c.post("/v1/images/generations", headers=stack.headers, json={**ask, "response_format": "b64_json"})
        assert r.status_code == 200, r.text
        answer = r.json()
        assert base64.b64decode(answer["data"][0]["b64_json"]).startswith(b"\x89PNG")
        assert answer["data"][0]["media_type"] == "image/png"
        assert answer["usage"] == {"images": 1, "size": "1024*1024", "cost": 0.04}
        sent = stack.mock["qwen_image_requests"][-1]
        assert sent["input"]["messages"][0]["content"] == [{"text": "an apple"}]
        assert sent["parameters"] == {"watermark": False, "n": 1, "size": "1024*1024"}
        assert stack.mock["last_headers"]["authorization"].startswith("Bearer ")

        # links when not asked otherwise; an edit (oh-my-pi's form) takes its input images along
        r = await c.post("/v1/images/generations", headers=stack.headers, json={**ask, "n": 2})
        names = [d["url"].split("/files/")[1] for d in r.json()["data"]]
        assert names == ["image-0.png?Expires=1", "image-1.png?Expires=1"]
        assert r.json()["usage"] == {"images": 2, "size": "1024*1024", "cost": 0.08}
        edit = {**ask, "input_references": [{"type": "image_url", "url": "data:image/png;base64,AAAA"}]}
        r = await c.post("/v1/images/edits", headers=stack.headers, json=edit)
        assert r.status_code == 200 and r.json()["usage"]["cost"] == 0.043
        content = stack.mock["qwen_image_requests"][-1]["input"]["messages"][0]["content"]
        assert content[1] == {"image": "data:image/png;base64,AAAA"}

        # what QueueRouter can't send, and Qwen's own refusal, as OpenAI's errors
        form = {"model": "qwen-image-3.0-pro", "prompt": "x"}
        r = await c.post("/v1/images/edits", headers=stack.headers, files={"image": ("a.png", b"x")}, data=form)
        assert r.status_code == 400 and "as JSON" in r.json()["error"]["message"]
        stack.mock["qwen_image_status"] = 400
        r = await c.post("/v1/images/generations", headers=stack.headers, json=ask)
        assert r.status_code == 400 and r.json()["error"]["message"] == "forced"
        stack.mock["qwen_image_status"] = 0

        # priced per image on the Requests and Usage pages
        await flushed(stack, 5)
        store = stack.app.state.store

        def spent() -> float:
            return store.query("SELECT COALESCE(SUM(media_cost), 0) FROM usage_daily")[0][0]

        await eventually(lambda: round(spent(), 6) == 0.163, timeout=10)
        costs = sorted(r["media_cost"] for r in store.list_requests(20) if r["media_cost"])
        assert costs == [0.04, 0.043, 0.08]
        made = sorted((r["images"], r["media_units"]) for r in store.list_requests(20) if r["images"])
        assert made == [(1, "1 × 1024*1024"), (1, "1 × 1024*1024"), (2, "2 × 1024*1024")]
        assert tuple(store.query("SELECT SUM(images), SUM(videos) FROM usage_daily")[0]) == (4, 0)
        await login(c, stack.url)
        page = (await c.get("/admin/usage/body")).text
        assert "Images and videos" in page and "Images · videos" in page


async def test_other_providers_get_image_requests_as_they_are(make_stack):
    """Not on Qwen: the request goes to that provider's own /images/generations, untouched."""
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post(
            "/v1/images/generations", headers=stack.headers, json={"model": "mock-model", "prompt": "an apple"}
        )
    assert "qwen_image_requests" not in stack.mock
    assert r.status_code in (200, 404, 405)  # whatever the mock answers there: QueueRouter didn't translate it


def test_a_video_request_in_qwens_form():
    body = media.video_request(
        {
            "model": "wan2.7-i2v",
            "prompt": "the cat walks",
            "duration": 8,
            "resolution": "720p",
            "aspect_ratio": "16:9",
            "seed": 3,
            "frame_images": [
                {"type": "image_url", "image_url": {"url": "https://x/a.png"}, "frame_type": "first_frame"}
            ],
            "input_references": [
                {"type": "audio_url", "audio_url": {"url": "https://x/a.mp3"}},
                {"type": "image_url", "image_url": {"url": "https://x/ref.png"}},
            ],
        }
    )
    assert body == {
        "model": "wan2.7-i2v",
        "input": {
            "prompt": "the cat walks",
            "media": [
                {"type": "first_frame", "url": "https://x/a.png"},
                {"type": "driving_audio", "url": "https://x/a.mp3"},
                {"type": "reference_image", "url": "https://x/ref.png"},
            ],
        },
        "parameters": {"watermark": False, "duration": 8, "seed": 3, "resolution": "720P", "ratio": "16:9"},
    }
    # models that take pixels, an edit's video, Vidu's image
    pix = media.video_request({"model": "pixverse/pixverse-v6-t2v", "prompt": "x", "resolution": "1080p"})
    assert pix["parameters"]["size"] == "1920*1080" and "resolution" not in pix["parameters"]
    refs = [{"type": "video_url", "video_url": "https://x/v"}]
    edit = media.video_request({"model": "wan2.7-videoedit", "prompt": "x", "input_references": refs})
    assert edit["input"]["media"] == [{"type": "video", "url": "https://x/v"}]
    frames = [{"image_url": "https://x/i"}]
    vidu = media.video_request({"model": "vidu/viduq2-pro-fast_img2video", "frame_images": frames})
    assert vidu["input"] == {"media": [{"type": "image", "url": "https://x/i"}]}
    try:
        media.video_request({"model": "wan3.0-video"})
        raise AssertionError("nothing to make a video from was taken")
    except media.Unsupported:
        pass


def test_video_prices():
    assert media.video_cost("wan3.0-video", 5, 720) == 0.35
    assert media.video_cost("wan3.0-video-prime", 10, "1080") == 2.8
    assert media.video_cost("wan2.7-r2v-2026-06-12", 4, 720) == 0.4  # a dated one, priced as the model
    assert media.video_cost("pixverse/pixverse-v6-t2v", 5, 720, audio=True) == 0.27
    assert media.video_cost("wan2.1-vace-plus", 5, 480) == 0.5
    assert media.video_cost("wan3.0-video", 5, 360) == 0.175  # below its lowest: its lowest
    assert media.video_cost("unknown-video", 5, 720) is None and media.video_cost("wan3.0-video", 0, 720) is None


async def test_videos_through_qwens_task_api(make_stack):
    stack = make_stack(models_cache_s=0)
    qwen(stack, "wan3.0-video", "wan2.7-t2v")
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        listed = (await c.get("/v1/videos/models", headers=stack.headers)).json()["data"]
        assert sorted(m["id"] for m in listed) == ["wan2.7-t2v", "wan3.0-video"]

        ask = {"model": "wan3.0-video", "prompt": "a red ball", "duration": 5, "resolution": "720p"}
        r = await c.post("/v1/videos", headers=stack.headers, json=ask)
        assert r.status_code == 200, r.text
        started = r.json()
        assert started["status"] == "pending" and started["id"].startswith("video-")
        assert stack.mock["last_headers"]["x-dashscope-async"] == "enable"
        sent = stack.mock["qwen_video_requests"][-1]
        assert sent["input"] == {"prompt": "a red ball"}
        assert sent["parameters"] == {"watermark": False, "duration": 5, "resolution": "720P"}

        job_url = f"/v1/videos/{started['id']}"
        running = (await c.get(job_url, headers=stack.headers)).json()
        assert running["status"] == "in_progress" and "unsigned_urls" not in running
        r = await c.get(f"{job_url}/content", headers=stack.headers)
        assert r.status_code == 409  # not ready yet
        done = (await c.get(job_url, headers=stack.headers)).json()
        assert done["status"] == "completed" and done["usage"] == {"cost": 0.35}
        assert done["unsigned_urls"] == [f"{stack.url}{job_url}/content"]
        again = (await c.get(job_url, headers=stack.headers)).json()
        assert again["usage"] == {"cost": 0.35}  # priced once, said each time
        r = await c.get(f"{job_url}/content", headers=stack.headers)
        assert r.status_code == 200 and r.headers["content-type"] == "video/mp4" and b"ftypmp42" in r.content

        # only its client sees a job
        other = stack.app.state.store.create_client("other", "test")
        stack.app.state.reload()
        assert (await c.get(job_url, headers={"Authorization": f"Bearer {other}"})).status_code == 404
        assert (await c.get("/v1/videos/video-unknown", headers=stack.headers)).status_code == 404

        # a refused video costs nothing
        stack.mock["video_fail"] = True
        failed = (await c.post("/v1/videos", headers=stack.headers, json=ask)).json()
        for _ in range(2):  # running
            await c.get(f"/v1/videos/{failed['id']}", headers=stack.headers)
        result = (await c.get(f"/v1/videos/{failed['id']}", headers=stack.headers)).json()
        assert result["status"] == "failed" and "usage" not in result

        # the video's price once on the Requests and Usage pages: the two starts and one finished video
        await flushed(stack, 3)
        store = stack.app.state.store

        def spent() -> float:
            return store.query("SELECT COALESCE(SUM(media_cost), 0) FROM usage_daily")[0][0]

        await eventually(lambda: round(spent(), 6) == 0.35, timeout=10)
        made = [r for r in store.list_requests(20) if r["media_cost"]]
        assert len(made) == 1 and made[0]["target_model"] == "wan3.0-video" and made[0]["path"] == job_url
        assert made[0]["video_seconds"] == 5.0 and made[0]["media_units"] == "5 s at 720P"
        totals = store.query("SELECT SUM(videos), SUM(video_seconds), SUM(images) FROM usage_daily")[0]
        assert tuple(totals) == (1, 5.0, 0)  # the finished video, not its two starts
