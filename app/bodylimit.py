"""Request-size limits enforced while the body is read.

Form parsing (admin pages, file uploads) buffers what it reads, partly into /tmp, which is
memory in the usual read-only container. Checking sizes afterwards is too late: a client could
send gigabytes before any route code runs. This pure ASGI middleware refuses a too-large body
up front (Content-Length) or as soon as more bytes arrive than allowed (chunked uploads).
"""

from __future__ import annotations

import json
from collections.abc import Callable


class TooLarge(Exception):
    pass


class BodyLimit:
    def __init__(self, app, limit_for: Callable[[str], int | None]) -> None:
        self.app = app
        self.limit_for = limit_for  # path -> max bytes, or None for no limit here

    async def __call__(self, scope, receive, send):
        limit = self.limit_for(scope["path"]) if scope["type"] == "http" else None
        if limit is None:
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers") or [])
        declared = headers.get(b"content-length", b"")
        if declared.isdigit() and int(declared) > limit:
            return await _refuse(send, limit)

        received = 0
        started = False

        async def counting_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise TooLarge
            return message

        async def tracking_send(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, counting_receive, tracking_send)
        except TooLarge:
            if not started:
                await _refuse(send, limit)


async def _refuse(send, limit: int) -> None:
    body = json.dumps(
        {"error": {"message": f"request body is larger than {limit:,} bytes", "type": "request_too_large", "code": 413}}
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json"), (b"connection", b"close")],
        }
    )
    await send({"type": "http.response.body", "body": body})
