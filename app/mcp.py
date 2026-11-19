"""QueueRouter's own MCP server for images and videos (``/mcp/media``), like Higgsfield's connector: apps that take
MCP connectors (the Claude app, Claude Code, oh-my-pi, opencode...) get tools that make images and videos through
QueueRouter, as one of its clients: the same queue, limits, Requests and Usage pages and costs as /v1/images and
/v1/videos (app/media.py).

MCP's streamable HTTP transport, the plain-JSON way: each POST is one JSON-RPC message, answered with one JSON
response (a notification with 202 and no body); there's no stream of the server's own, so GET is refused (405). No
sessions: every call carries the client's key, as ``Authorization: Bearer`` or in the address (``/mcp/media/<key>``,
for apps whose connector takes only an address, like the Claude app). The tools themselves are in app/main.py
(``mcp_tool``); this module is the protocol and what the tools are.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

PROTOCOLS = ("2025-06-18", "2025-03-26", "2024-11-05")  # newest first: an older client gets its own
SERVER = {"name": "queuerouter-media", "title": "QueueRouter media"}
INSTRUCTIONS = (
    "Makes images and videos through QueueRouter. Call list_media_models first to see the models this key can use and "
    "what they cost. Images come back at once. A video takes one to three minutes: generate_video starts it, then call "
    "check_video with its job_id about every 20 seconds until it's completed or failed. Say what a video will cost "
    "before starting one over $1."
)

_STRING = {"type": "string"}
TOOLS = [
    {
        "name": "list_media_models",
        "title": "List image and video models",
        "description": "The image and video models this key can use, with their prices (per image, or per second of "
        "video at each resolution) and the lengths a video can have.",
        "inputSchema": {"type": "object", "properties": {}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "generate_image",
        "title": "Generate an image",
        "description": "Make one or more images from a prompt, optionally from input images (an edit, or references). "
        "Returns the images, a download link for each and what they cost. Takes about 10 to 60 seconds.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {**_STRING, "description": "What the image shows: subject, scene, style, lighting."},
                "model": {**_STRING, "description": "An image model's id from list_media_models (default: the first)."},
                "size": {**_STRING, "description": "Width x height, e.g. 1024x1024, 1536x1024, or 2K."},
                "n": {"type": "integer", "minimum": 1, "maximum": 4, "description": "How many images (default 1)."},
                "input_images": {
                    "type": "array",
                    "items": _STRING,
                    "description": "Images to start from or follow: https links or data: URLs.",
                },
                "negative_prompt": {**_STRING, "description": "What to keep out of the image."},
                "seed": {"type": "integer"},
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "generate_video",
        "title": "Start a video",
        "description": "Start making a video from a prompt, optionally from a first frame or reference images. Returns "
        "a job_id at once; the video takes one to three minutes: follow it with check_video.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {**_STRING, "description": "What happens: subject, action, camera, lighting, mood."},
                "model": {**_STRING, "description": "A video model's id from list_media_models (default: the first)."},
                "duration": {"type": "integer", "minimum": 2, "description": "Seconds (Wan 3.0: 2 to 30; default 5)."},
                "resolution": {**_STRING, "enum": ["480p", "720p", "1080p"], "description": "Default 720p."},
                "aspect_ratio": {**_STRING, "enum": ["16:9", "9:16", "1:1", "4:3", "3:4"]},
                "first_frame": {
                    **_STRING,
                    "description": "The video starts from this image: an https link or data: URL.",
                },
                "reference_images": {
                    "type": "array",
                    "items": _STRING,
                    "description": "Who or what should appear (people, products, style): https links or data: URLs.",
                },
                "seed": {"type": "integer"},
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "check_video",
        "title": "Check a video",
        "description": "How a video started with generate_video is going. Once completed: its download link, length, "
        "resolution and what it cost. A failed video costs nothing.",
        "inputSchema": {
            "type": "object",
            "properties": {"job_id": {**_STRING, "description": "The job_id generate_video gave."}},
            "required": ["job_id"],
        },
        "annotations": {"readOnlyHint": True},
    },
]
TOOL_NAMES = {tool["name"] for tool in TOOLS}

PARSE_ERROR, INVALID_REQUEST, NO_METHOD, INVALID_PARAMS = -32700, -32600, -32601, -32602
INLINE_BYTES = 1_000_000  # an image this size or smaller is in the tool's answer itself (the app shows it); else a link


def text(message: str, error: bool = False) -> dict:
    """A tool's answer: one text block (``isError``: the tool ran and failed, which the model sees and can act on)."""
    return {"content": [{"type": "text", "text": message}], "isError": error}


def _reply(message_id, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def error(message_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


async def handle(message, call_tool: Callable[[str, dict], Awaitable[dict]], version: str) -> dict | None:
    """One JSON-RPC message -> its response, or None for a notification (answered 202). ``call_tool`` runs a tool
    by its name with its arguments and gives its result (``content``, ``isError``)."""
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
        message_id = message.get("id") if isinstance(message, dict) else None
        return error(message_id, INVALID_REQUEST, "not a JSON-RPC 2.0 request")
    method, message_id = message["method"], message.get("id")
    params = message.get("params") if isinstance(message.get("params"), dict) else {}
    if "id" not in message:  # a notification (initialized, cancelled...): nothing to answer
        return None
    if method == "initialize":
        asked = params.get("protocolVersion")
        result = {
            "protocolVersion": asked if asked in PROTOCOLS else PROTOCOLS[0],
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {**SERVER, "version": version},
            "instructions": INSTRUCTIONS,
        }
        return _reply(message_id, result)
    if method == "ping":
        return _reply(message_id, {})
    if method == "tools/list":
        return _reply(message_id, {"tools": TOOLS})
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
        if name not in TOOL_NAMES:
            return error(message_id, INVALID_PARAMS, f"unknown tool: {name}")
        return _reply(message_id, await call_tool(name, arguments))
    if method in ("resources/list", "prompts/list"):  # asked by some apps whatever the capabilities say
        return _reply(message_id, {"resources" if method.startswith("resources") else "prompts": []})
    return error(message_id, NO_METHOD, f"method not found: {method}")
