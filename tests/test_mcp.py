"""QueueRouter's MCP server for images and videos (/mcp/media, app/mcp.py): the protocol, signing in with the key in
a header or in the address, and its tools going through the same pipeline as /v1/images and /v1/videos, with
download links that send to the provider's file while it lasts and to QueueRouter's own copy after."""

import asyncio
import re

import httpx

from app import mcp
from tests.conftest import ACCESS_KEY, eventually
from tests.test_media import qwen


def call(method: str, params: dict | None = None, message_id: int | None = 1) -> dict:
    message = {"jsonrpc": "2.0", "method": method, "params": params or {}}
    if message_id is not None:
        message["id"] = message_id
    return message


def test_the_protocol():
    async def tool(name, args):
        return mcp.text(f"{name} {args}")

    def handled(message):
        return asyncio.run(mcp.handle(message, tool, "9.9.9"))

    hello = handled(call("initialize", {"protocolVersion": "2025-03-26"}))["result"]
    assert hello["protocolVersion"] == "2025-03-26" and hello["serverInfo"]["version"] == "9.9.9"
    assert hello["capabilities"] == {"tools": {"listChanged": False}} and "check_video" in hello["instructions"]
    assert (
        handled(call("initialize", {"protocolVersion": "1999-01-01"}))["result"]["protocolVersion"] == mcp.PROTOCOLS[0]
    )
    names = [t["name"] for t in handled(call("tools/list"))["result"]["tools"]]
    assert names == ["list_media_models", "generate_image", "generate_video", "check_video"]
    assert handled(call("tools/call", {"name": "check_video", "arguments": {"job_id": "x"}}))["result"] == mcp.text(
        "check_video {'job_id': 'x'}"
    )
    assert handled(call("ping"))["result"] == {}
    assert handled(call("notifications/initialized", message_id=None)) is None
    assert handled(call("tools/call", {"name": "nope"}))["error"]["code"] == mcp.INVALID_PARAMS
    assert handled(call("sampling/createMessage"))["error"]["code"] == mcp.NO_METHOD
    assert handled({"id": 3, "method": "ping"})["error"]["code"] == mcp.INVALID_REQUEST


def said(answer: dict) -> str:
    return "\n".join(block["text"] for block in answer["result"]["content"] if block["type"] == "text")


async def test_images_and_videos_through_mcp(make_stack):
    stack = make_stack(models_cache_s=0)
    qwen(stack, "qwen-image-3.0-pro", "wan3.0-video")
    keyed = f"/mcp/media/{ACCESS_KEY}"
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:

        async def tool(name: str, **args) -> dict:
            r = await c.post(keyed, json=call("tools/call", {"name": name, "arguments": args}))
            assert r.status_code == 200, r.text
            return r.json()

        # signing in: the key as a header, or in the address; anything else is refused
        r = await c.post("/mcp/media", headers=stack.headers, json=call("initialize"))
        assert r.status_code == 200 and r.json()["result"]["serverInfo"]["name"] == "queuerouter-media"
        assert (await c.post(keyed, json=call("tools/list"))).json()["result"]["tools"][0][
            "name"
        ] == "list_media_models"
        assert (await c.post("/mcp/media/not-a-key", json=call("tools/list"))).status_code == 401
        assert (await c.post("/mcp/media", json=call("tools/list"))).status_code == 401
        assert (await c.post(keyed, json=call("notifications/initialized", message_id=None))).status_code == 202
        assert (await c.get(keyed)).status_code == 405
        assert (await c.post(keyed, content=b"not json")).json()["error"]["code"] == mcp.PARSE_ERROR

        listed = said(await tool("list_media_models"))
        assert "qwen-image-3.0-pro (image): $0.04 (1K) / $0.075 (2K) per image" in listed
        assert "wan3.0-video (video): 480p $0.035/s, 720p $0.07/s, 1080p $0.14/s; 2 to 30 s" in listed
        assert "mock-model" not in listed

        # an image: shown in the app, and a link to QueueRouter's copy
        answer = await tool("generate_image", prompt="an apple", size="1024x1024")
        image = answer["result"]["content"][0]
        assert image["type"] == "image" and image["mimeType"] == "image/png" and not answer["result"]["isError"]
        text = said(answer)
        assert "Made 1 image (1024*1024) with qwen-image-3.0-pro, $0.040" in text
        link = re.search(r"https?://\S+/media/\S+", text)[0]
        await eventually(
            lambda: any(stack.app.state.store.query("SELECT file FROM media_files WHERE file IS NOT NULL"))
        )
        got = await c.get(link)
        assert (
            got.status_code == 200 and got.content.startswith(b"\x89PNG") and got.headers["content-type"] == "image/png"
        )
        assert (await c.get(link[:-6] + "AAAAAA")).status_code == 404  # changed: not a link of QueueRouter's
        sent = stack.mock["qwen_image_requests"][-1]
        assert sent["input"]["messages"][0]["content"] == [{"text": "an apple"}]

        # a video: started, followed, then a link that sends to the provider's file, later to QueueRouter's copy
        started = said(await tool("generate_video", prompt="a red ball", duration=2, resolution="480p"))
        assert "Started a 2 s 480p video with wan3.0-video, about $0.07." in started
        job_id = re.search(r"job_id: (\S+)", started)[1]
        assert stack.mock["qwen_video_requests"][-1]["parameters"]["resolution"] == "480P"
        assert "Still in progress" in said(await tool("check_video", job_id=job_id))
        assert "Still in progress" in said(await tool("check_video", job_id=job_id))
        done = said(await tool("check_video", job_id=job_id))
        assert "Completed: 5 s at 720P with wan3.0-video, $0.350." in done  # what the mock reports it made
        link = re.search(r"https?://\S+/media/\S+", done)[0]
        r = await c.get(link)
        assert r.status_code == 302 and "/files/video.mp4" in r.headers["location"]
        store = stack.app.state.store
        await eventually(lambda: store.media_file(job_id) is not None and store.media_file(job_id)["file"] is not None)
        with store.tx() as db:  # the provider's file is gone: QueueRouter's copy from now on
            db.execute("UPDATE media_files SET provider_until = 0 WHERE id = ?", (job_id,))
        r = await c.get(link)
        assert r.status_code == 200 and r.headers["content-type"] == "video/mp4" and b"ftypmp42" in r.content
        r = await c.get(f"/v1/videos/{job_id}/content", headers=stack.headers)
        assert r.status_code == 200 and b"ftypmp42" in r.content  # the API serves QueueRouter's copy too

        # what a tool can't do comes back as the tool's error, for the model to read
        failed = (await tool("check_video", job_id="video-unknown"))["result"]
        assert failed["isError"] and "not found" in failed["content"][0]["text"]
        assert (await tool("generate_image", prompt=" "))["result"]["isError"]
