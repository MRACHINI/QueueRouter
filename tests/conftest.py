"""Runs the mock upstream and QueueRouter as real HTTP servers on random local ports."""

from __future__ import annotations

import asyncio
import dataclasses
import threading
import time

import pytest
import uvicorn

from app.config import Bootstrap, Settings
from app.main import create_app
from tests.mock_upstream import create_mock

ACCESS_KEY = "test-key"


class ServerThread:
    def __init__(self, app) -> None:
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> str:
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.01)
        port = self.server.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    def __exit__(self, *exc) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


@dataclasses.dataclass
class Stack:
    url: str
    mock: dict
    headers: dict
    app: object = None


@pytest.fixture
def make_stack(tmp_path_factory):
    """Factory: make_stack(limit=2, **settings_overrides) -> Stack.

    Each stack gets a fresh data directory; the settings are imported as on a first start.
    """
    servers: list[ServerThread] = []

    def factory(limit: int = 2, bootstrap: dict | None = None, **overrides) -> Stack:
        mock_app = create_mock(limit)
        mock_server = ServerThread(mock_app)
        mock_url = mock_server.__enter__()
        servers.append(mock_server)
        settings = Settings(
            upstream_url=f"{mock_url}/v1",
            upstream_api_key="upstream-secret",
            max_concurrency=limit,
            access_keys=(ACCESS_KEY,),
            keepalive_interval_s=0.2,
            retry_base_delay_s=0.05,
            retry_max_delay_s=0.2,
        )
        settings = dataclasses.replace(settings, **overrides)
        options = {"data_dir": str(tmp_path_factory.mktemp("data")), "log_level": "WARNING", **(bootstrap or {})}
        boot = Bootstrap(**options)
        app = create_app(boot, seed=settings)
        proxy_server = ServerThread(app)
        proxy_url = proxy_server.__enter__()
        servers.append(proxy_server)
        return Stack(proxy_url, mock_app.state.mock, {"Authorization": f"Bearer {ACCESS_KEY}"}, app)

    yield factory
    for server in reversed(servers):
        server.__exit__(None, None, None)


@pytest.fixture
def make_mock():
    """Factory: make_mock(limit) -> (base URL incl. /v1, state) for extra mock upstreams."""
    servers: list[ServerThread] = []

    def factory(limit: int = 1) -> tuple[str, dict]:
        app = create_mock(limit)
        server = ServerThread(app)
        url = server.__enter__()
        servers.append(server)
        return f"{url}/v1", app.state.mock

    yield factory
    for server in servers:
        server.__exit__(None, None, None)


async def eventually(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)
