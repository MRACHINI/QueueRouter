"""Images on Qwen Cloud, for apps that speak OpenAI's image API (oh-my-pi's image tool, Open WebUI...).

Qwen Cloud's OpenAI-compatible address has no ``/images/generations`` (its token-plan host answers 404), so a request
to ``/v1/images/generations`` or ``/v1/images/edits`` for an upstream there goes to its own image API instead, the one
Qwen-Image and Wan 2.7 share (synchronous): ``{host}/api/v1/services/aigc/multimodal-generation/generation``, the
prompt and any input images as one user message, ``n``/``size``/``seed``... as ``parameters``. The answer, the
images' temporary links (24 hours), comes back as OpenAI's ``data`` list: links, or the images themselves as base64
when the app asks for ``b64_json`` (QueueRouter fetches them). Tested live with the user's key (2026-11).

Images are priced per image, not per token: what one cost goes in the answer's ``usage.cost`` (oh-my-pi shows it) and
on the request as ``media_cost`` (the Requests and Usage pages, app/pricing.py).

Which models make images (or videos) is said in /v1/models as ``output_modalities``, from the provider's list when it
says, else from the model's name: oh-my-pi gives a model that outputs only images to its image tool.
"""

from __future__ import annotations

import base64
import json
import re
import time
import urllib.parse

import httpx

QWEN_HOSTS = ("qwencloudapi.com", "dashscope.aliyuncs.com", "dashscope-intl.aliyuncs.com")
IMAGE_PATHS = ("images/generations", "images/edits")
IMAGE_ENDPOINT = "services/aigc/multimodal-generation/generation"
MAX_IMAGE_BYTES = 40 * 1024 * 1024  # one image fetched for b64_json

# USD per image made, at 1K and 2K (Qwen Cloud's model pages, Singapore, 2026-11), and per input image (edits)
IMAGE_PRICES = {
    "qwen-image-3.0-pro": (0.04, 0.075),
    "wan2.7-image": (0.03, 0.03),
    "wan2.7-image-pro": (0.075, 0.075),
}
INPUT_IMAGE_PRICES = {"qwen-image-3.0-pro": 0.003}
TWO_K_PIXELS = 1_600_000  # more than this is a 2K image (1K: about 1024*1024)

_IMAGE_NAME = re.compile(r"(^|[-_./])(image|t2i|i2i)([-_.]|\d|$)|flux|seedream|dall-e|imagen")
_VIDEO_NAME = re.compile(r"(^|[-_./])(video|videoedit|t2v|i2v|r2v|vace)([-_.]|\d|$)|img2video|happyhorse|pixverse")


class Unsupported(ValueError):
    """A request the translation can't make (answered with a 400)."""


def qwen(upstream) -> bool:
    """Whether an upstream is on Qwen Cloud (or Alibaba's Model Studio, the same API): its provider, else its
    address."""
    url = (upstream.base_url or "").lower()
    return upstream.preset == "qwen" or any(host in url for host in QWEN_HOSTS)


def api_root(base_url: str) -> str:
    """Qwen's own API next to its OpenAI-compatible one: https://host/compatible-mode/v1 -> https://host/api/v1."""
    parts = urllib.parse.urlsplit(base_url)
    return f"{parts.scheme}://{parts.netloc}/api/v1"


def kind(model_id: str, entry: dict | None) -> str | None:
    """What a model makes, "image" or "video" (else None): what its list entry says it outputs, else its name."""
    entry = entry or {}
    architecture = entry.get("architecture") if isinstance(entry.get("architecture"), dict) else {}
    for said in (entry.get("output_modalities"), architecture.get("output_modalities")):
        if isinstance(said, list) and said:
            outputs = {str(m).lower() for m in said}
            if "text" in outputs:
                return None
            return "image" if "image" in outputs else "video" if "video" in outputs else None
    name = str(model_id or "").lower()
    if _VIDEO_NAME.search(name):
        return "video"
    if _IMAGE_NAME.search(name):
        return "image"
    return None


def marked(entry: dict, model_id: str) -> dict:
    """A /v1/models entry that says what its model outputs when it makes images or videos (``output_modalities``),
    unless it says so already."""
    made = kind(model_id, entry)
    if made is None or "output_modalities" in entry:
        return entry
    return {**entry, "output_modalities": [made]}


# -- request ---------------------------------------------------------------------------------


def _urls(value) -> list[str]:
    """Input image addresses (links or data: URLs) however an app sends them: a string, ``{"url"}``,
    ``{"image_url": "..."}`` or ``{"image_url": {"url"}}``, or a list of those."""
    if isinstance(value, list):
        return [url for item in value for url in _urls(item)]
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, dict):
        inner = value.get("image_url", value.get("url"))
        return _urls(inner)
    return []


def input_images(payload: dict) -> list[str]:
    """The images an edit starts from: OpenAI's ``image`` / ``images``, oh-my-pi's ``input_references``."""
    return [url for name in ("image", "images", "input_references") for url in _urls(payload.get(name))]


def image_request(payload: dict) -> dict:
    """An OpenAI image request (generation, or an edit sent as JSON) as the body of Qwen's image API."""
    prompt = payload.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise Unsupported("prompt is required")
    content: list[dict] = [{"text": prompt}]
    content += [{"image": url} for url in input_images(payload)]
    parameters: dict = {"watermark": bool(payload.get("watermark", False))}  # Qwen's default would add one
    if isinstance(n := payload.get("n"), int) and n > 0:
        parameters["n"] = n
    size = payload.get("size")
    if isinstance(size, str) and size.strip() and size.strip().lower() != "auto":
        parameters["size"] = re.sub(r"\s*[xX×]\s*", "*", size.strip())  # 1024x1024 -> 1024*1024; 2K stays
    for name in ("negative_prompt", "seed", "prompt_extend"):
        if payload.get(name) is not None:
            parameters[name] = payload[name]
    return {
        "model": payload.get("model"),
        "input": {"messages": [{"role": "user", "content": content}]},
        "parameters": parameters,
    }


# -- answer ----------------------------------------------------------------------------------


def image_cost(model: str, count: int, size: str | None, inputs: int) -> float | None:
    """What ``count`` images of ``size`` ("1024*1024") cost, with ``inputs`` images given; None for a model with no
    known price."""
    prices = IMAGE_PRICES.get(str(model or "").lower())
    if prices is None:
        return None
    pixels = 0
    if isinstance(size, str) and (found := re.fullmatch(r"\s*(\d+)\s*[*xX]\s*(\d+)\s*", size)):
        pixels = int(found[1]) * int(found[2])
    each = prices[1] if pixels > TWO_K_PIXELS else prices[0]
    return round(count * each + inputs * INPUT_IMAGE_PRICES.get(str(model).lower(), 0.0), 6)


def cost_of(body: bytes) -> float | None:
    """``usage.cost`` of a translated answer (the request's ``media_cost``)."""
    try:
        cost = (json.loads(body).get("usage") or {}).get("cost")
    except (ValueError, AttributeError):
        return None
    return float(cost) if isinstance(cost, int | float) and cost >= 0 else None


def _error(response: httpx.Response, status: int, message: str, kind_: str | None = None) -> httpx.Response:
    body = {"error": {"message": message, "type": kind_ or "upstream_error", "code": status}}
    headers = {k: v for k, v in response.headers.items() if k.lower() == "retry-after"}  # a 429's wait, kept
    return httpx.Response(status, json=body, headers=headers, request=response.request)


async def _fetched(client: httpx.AsyncClient, url: str) -> tuple[str, str] | None:
    """An image's bytes as base64 and its type, or None when it can't be had (the link is given instead)."""
    if urllib.parse.urlsplit(url).scheme not in ("http", "https"):
        return None
    try:
        async with client.stream("GET", url, timeout=120) as response:
            if response.status_code != 200:
                return None
            data = b""
            async for chunk in response.aiter_bytes():
                data += chunk
                if len(data) > MAX_IMAGE_BYTES:
                    return None
            kind_ = response.headers.get("content-type", "image/png").split(";")[0].strip() or "image/png"
            return base64.b64encode(data).decode(), kind_
    except httpx.HTTPError:
        return None


async def image_answer(response: httpx.Response, client: httpx.AsyncClient, payload: dict) -> httpx.Response:
    """Qwen's answer as OpenAI's: ``data`` with each image as ``url`` or, when ``response_format`` is b64_json,
    ``b64_json`` (fetched), and ``usage`` with the image count and ``cost``. Qwen's error (``code``, ``message``)
    becomes OpenAI's ``error``, its status kept."""
    try:
        content = await response.aread()
    finally:
        await response.aclose()
    try:
        answer = json.loads(content)
    except ValueError:
        answer = None
    if not isinstance(answer, dict):
        answer = {}
    if response.status_code != 200:
        message = str(answer.get("message") or content[:300].decode("utf-8", "replace") or "no error text")
        return _error(response, response.status_code, message, answer.get("code"))
    output = answer.get("output") if isinstance(answer.get("output"), dict) else {}
    urls = [
        part["image"]
        for choice in output.get("choices") or []
        if isinstance(choice, dict)
        for part in ((choice.get("message") or {}).get("content") or [])
        if isinstance(part, dict) and isinstance(part.get("image"), str)
    ]
    if not urls:
        message = str(answer.get("message") or output.get("message") or "the provider returned no image")
        return _error(response, 502, message, answer.get("code"))
    data = []
    want_b64 = payload.get("response_format") == "b64_json"
    for url in urls:
        got = await _fetched(client, url) if want_b64 else None
        data.append({"b64_json": got[0], "media_type": got[1]} if got else {"url": url})
    usage = answer.get("usage") if isinstance(answer.get("usage"), dict) else {}
    count = usage.get("image_count") if isinstance(usage.get("image_count"), int) else len(urls)
    size = usage.get("size") if isinstance(usage.get("size"), str) else None
    cost = image_cost(str(payload.get("model") or ""), count, size, len(input_images(payload)))
    summary: dict = {"images": count}
    if size:
        summary["size"] = size
    if cost is not None:
        summary["cost"] = cost
    body = {"created": int(time.time()), "data": data, "usage": summary}
    return httpx.Response(200, json=body, request=response.request)


def image_units(body: bytes) -> tuple[int | None, str | None]:
    """How many images an answer has, and what it made as the Requests page shows it ("2 × 1024*1024"). Any
    provider's: its ``usage`` when it says (QueueRouter's own translation does), else its ``data`` list."""
    try:
        answer = json.loads(body)
    except ValueError:
        return None, None
    if not isinstance(answer, dict):
        return None, None
    usage = answer.get("usage") if isinstance(answer.get("usage"), dict) else {}
    count = usage.get("images")
    if not isinstance(count, int):
        count = len(answer["data"]) if isinstance(answer.get("data"), list) else None
    if not count:
        return None, None
    size = usage.get("size") if isinstance(usage.get("size"), str) else None
    return count, f"{count} × {size}" if size else f"{count} image{'s' if count > 1 else ''}"


def video_units(task: dict) -> tuple[float | None, str | None]:
    """A finished task's seconds, and what it made as shown ("5 s at 720P")."""
    usage = task.get("usage") if isinstance(task.get("usage"), dict) else {}
    seconds = usage.get("output_video_duration") or usage.get("duration")
    if not isinstance(seconds, int | float) or seconds <= 0:
        return None, None
    height = _height(usage.get("SR") or usage.get("resolution"))
    return float(seconds), f"{seconds:g} s at {height}P" if height else f"{seconds:g} s"


# -- video -----------------------------------------------------------------------------------
# OpenRouter's asynchronous video API (what oh-my-pi's video client speaks, and other apps): POST /v1/videos starts a
# job, GET /v1/videos/{id} says how it's going, GET /v1/videos/{id}/content is the video. On Qwen Cloud a job is a
# task of its own video API (tested live, 2026-11): POST video-synthesis with ``X-DashScope-Async: enable``, then
# GET tasks/{task_id}; its MP4's link lasts 24 hours. QueueRouter keeps which task is whose (store.media_jobs), and
# prices a video once, when it's first seen done, from the seconds and resolution Qwen reports.

VIDEO_PATH = "videos"
VIDEO_ENDPOINT = "services/aigc/video-generation/video-synthesis"
TASKS = "tasks"
STATUSES = {  # Qwen's task status -> OpenRouter's job status
    "PENDING": "pending",
    "RUNNING": "in_progress",
    "SUCCEEDED": "completed",
    "FAILED": "failed",
    "CANCELED": "cancelled",
    "UNKNOWN": "expired",  # older than a day, or never there
}
FINISHED = ("completed", "failed", "cancelled", "expired")

# USD per second of video by resolution (Qwen Cloud's model pages, Singapore, 2026-11; 0: one price for all)
VIDEO_PRICES = {
    "wan3.0-video": {480: 0.035, 720: 0.07, 1080: 0.14},  # 30% off its list price then
    "wan3.0-video-prime": {480: 0.068, 720: 0.14, 1080: 0.28},
    "wan2.7-t2v": {720: 0.10, 1080: 0.15},
    "wan2.7-i2v": {720: 0.10, 1080: 0.15},
    "wan2.7-r2v": {720: 0.10, 1080: 0.15},
    "wan2.7-videoedit": {720: 0.10, 1080: 0.15},
    "wan2.1-vace-plus": {0: 0.10},
    "happyhorse-1.1-t2v": {480: 0.07, 720: 0.14, 1080: 0.18},
    "happyhorse-1.0-video-edit": {720: 0.14, 1080: 0.24},
    "pixverse/pixverse-v6-t2v": {360: 0.023, 540: 0.032, 720: 0.040, 1080: 0.080},
    "vidu/viduq2-pro-fast_img2video": {720: 0.016, 1080: 0.032},
}
VIDEO_AUDIO_PRICES = {"pixverse/pixverse-v6-t2v": {360: 0.032, 480: 0.040, 720: 0.054, 1080: 0.103}}  # with sound
_DATED = re.compile(r"-\d{4}-\d{2}-\d{2}$")  # wan2.7-r2v-2026-06-12: priced as wan2.7-r2v
# models that take a size in pixels (1280*720) rather than a resolution and a ratio
_SIZED = re.compile(r"pixverse|wanx?2\.[0-6]")
_SIZES = {  # (height, ratio) -> width*height, as Wan's tables give them
    (360, "16:9"): "640*360",
    (480, "16:9"): "832*480",
    (480, "9:16"): "480*832",
    (480, "1:1"): "624*624",
    (540, "16:9"): "960*540",
    (720, "16:9"): "1280*720",
    (720, "9:16"): "720*1280",
    (720, "1:1"): "960*960",
    (720, "4:3"): "1088*832",
    (720, "3:4"): "832*1088",
    (1080, "16:9"): "1920*1080",
    (1080, "9:16"): "1080*1920",
    (1080, "1:1"): "1440*1440",
    (1080, "4:3"): "1632*1248",
    (1080, "3:4"): "1248*1632",
}


def _height(resolution) -> int | None:
    found = re.fullmatch(r"\s*(\d{3,4})\s*[pP]?\s*", str(resolution or ""))
    return int(found[1]) if found else None


def video_request(payload: dict) -> dict:
    """An OpenRouter video request as the body of Qwen's video API: the prompt, its images, videos and sound as
    ``input.media`` (each marked with what it is for the model), ``parameters`` as Qwen names them."""
    model = str(payload.get("model") or "")
    prompt = payload.get("prompt")
    edits = "videoedit" in model or "video-edit" in model
    media: list[dict] = []
    for frame in payload.get("frame_images") or []:
        role = frame.get("frame_type") if isinstance(frame, dict) else None
        for url in _urls(frame):
            media.append({"type": "image" if model.startswith("vidu/") else role or "first_frame", "url": url})
    for ref in payload.get("input_references") or []:
        kind_ = ref.get("type") if isinstance(ref, dict) else None
        if kind_ == "video_url":
            urls, role = _urls(ref.get("video_url")), "video" if edits else "reference_video"
        elif kind_ == "audio_url":
            urls, role = _urls(ref.get("audio_url")), "driving_audio"
        else:
            urls, role = _urls(ref), "reference_image"
        media += [{"type": role, "url": url} for url in urls]
    if not (isinstance(prompt, str) and prompt.strip()) and not media:
        raise Unsupported("a prompt or an image to start from is required")
    parameters: dict = {"watermark": bool(payload.get("watermark", False))}  # Qwen's default would add one
    if isinstance(duration := payload.get("duration"), int | float) and duration > 0:
        parameters["duration"] = int(duration)
    if isinstance(payload.get("seed"), int):
        parameters["seed"] = payload["seed"]
    if payload.get("prompt_extend") is not None:
        parameters["prompt_extend"] = payload["prompt_extend"]
    height = _height(payload.get("resolution"))
    ratio = payload.get("aspect_ratio") if isinstance(payload.get("aspect_ratio"), str) else None
    size = payload.get("size") if isinstance(payload.get("size"), str) else None
    if _SIZED.search(model):
        size = size or _SIZES.get((height or 720, ratio or "16:9")) or _SIZES.get((height or 720, "16:9"))
        if size:
            parameters["size"] = re.sub(r"\s*[xX×]\s*", "*", size)
    else:
        if height:
            parameters["resolution"] = f"{height}P"
        if ratio:
            parameters["ratio"] = ratio
        if not height and size and (found := re.fullmatch(r"\s*(\d+)\s*[*xX]\s*(\d+)\s*", size)):
            parameters["resolution"] = f"{min(int(found[1]), int(found[2]))}P"  # pixels: their height
    content: dict = {}
    if isinstance(prompt, str) and prompt.strip():
        content["prompt"] = prompt
    if media:
        content["media"] = media
    return {"model": model, "input": content, "parameters": parameters}


def video_cost(model: str, seconds, height, audio: bool = False) -> float | None:
    """What ``seconds`` of video at ``height`` (480, 720, 1080) cost; None for a model with no known price."""
    key = str(model or "").lower()
    key = key if key in VIDEO_PRICES else _DATED.sub("", key)
    table = (VIDEO_AUDIO_PRICES.get(key) if audio else None) or VIDEO_PRICES.get(key)
    if not table or not isinstance(seconds, int | float) or seconds <= 0:
        return None
    if 0 in table:
        rate = table[0]
    else:
        tall = _height(height) or 0
        rate = table[max((h for h in table if h <= tall), default=min(table))]
    return round(seconds * rate, 6)


def task_cost(model: str, task: dict, audio: bool) -> float | None:
    """A finished task's price, from the seconds and the resolution (``SR``) it reports."""
    usage = task.get("usage") if isinstance(task.get("usage"), dict) else {}
    seconds = usage.get("output_video_duration") or usage.get("duration")
    return video_cost(model, seconds, usage.get("SR") or usage.get("resolution"), audio)


def job(job_id: str, status: str, base: str, cost: float | None = None, error: str | None = None) -> dict:
    """A job as OpenRouter's video API gives it: where to ask again, and once it's done where the video is."""
    answer: dict = {"id": job_id, "status": status, "polling_url": f"{base}/v1/videos/{job_id}"}
    if status == "completed":
        answer["unsigned_urls"] = [f"{base}/v1/videos/{job_id}/content"]
    if cost is not None:
        answer["usage"] = {"cost": cost}
    if error:
        answer["error"] = error
    return answer


def task_of(content: bytes) -> dict:
    """Qwen's answer about a task (its ``output``, its ``usage``), or {} when it isn't one."""
    try:
        answer = json.loads(content)
    except ValueError:
        return {}
    return answer if isinstance(answer, dict) else {}


def error_answer(response: httpx.Response, content: bytes) -> httpx.Response:
    """Qwen's error (``code``, ``message``) as OpenAI's ``error``, its status kept."""
    answer = task_of(content)
    message = str(answer.get("message") or content[:300].decode("utf-8", "replace") or "no error text")
    return _error(response, response.status_code, message, answer.get("code"))


# -- QueueRouter's copies and what the MCP tools say (app/mcp.py) -------------------------------

PROVIDER_KEEPS_S = 23 * 3600  # a provider's link is used this long (Qwen keeps its files a day)
EXTENSIONS = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/quicktime": ".mov",
}
VIDEO_LENGTHS = (  # a model's name starts with -> the lengths it takes
    ("wan3.0", "2 to 30 s"),
    ("wan2.7", "2 to 15 s"),
    ("wan2.6", "5, 10 or 15 s"),
    ("wan2.5", "5 or 10 s"),
    ("wan2.", "5 s"),
    ("pixverse", "up to 15 s"),
)


def price_text(model: str, kind: str) -> str:
    """What one image, or a second of video at each resolution, costs on this model, and a video's lengths."""
    key = str(model or "").lower()
    if kind == "image":
        prices = IMAGE_PRICES.get(key)
        if prices is None:
            return "price unknown"
        return (
            f"${prices[0]:g} per image"
            if prices[0] == prices[1]
            else f"${prices[0]:g} (1K) / ${prices[1]:g} (2K) per image"
        )
    table = VIDEO_PRICES.get(key if key in VIDEO_PRICES else _DATED.sub("", key))
    rates = (
        "price unknown"
        if table is None
        else ", ".join(
            f"${rate:g}/s" if height == 0 else f"{height}p ${rate:g}/s" for height, rate in sorted(table.items())
        )
    )
    lengths = next((said for prefix, said in VIDEO_LENGTHS if key.startswith(prefix)), None)
    return f"{rates}; {lengths}" if lengths else rates
