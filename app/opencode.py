"""opencode integration: generated provider config and the /.well-known/opencode manifest.

opencode doesn't discover models from a custom provider's /v1/models, so QueueRouter writes the
provider block for it: every model and alias the client may use, with context/output limits.

- ``GET /v1/opencode-config`` (client key): the config, for ``OPENCODE_CONFIG`` or as remote config.
- ``GET /.well-known/opencode``: lets ``opencode auth login https://<domain>`` store the client key
  and pull the config above on every start (``remote_config``), so model lists stay current.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Iterable
from pathlib import Path

PROVIDER_ID = "queuerouter"
KEY_ENV = "QUEUEROUTER_KEY"
DEFAULT_OUTPUT = 16_384
CONTEXT_KEYS = ("context_length", "max_model_len", "max_context_length")
OUTPUT_KEYS = ("max_output_tokens", "max_completion_tokens")

# The login command only hands opencode the key from $QUEUEROUTER_KEY (opencode runs it without a
# shell and stores whatever it prints). Windows has no `sh`, hence a PowerShell variant.
_MISSING = f"Set {KEY_ENV} to your QueueRouter client key (Admin > Clients), then run the login again."
LOGIN_COMMANDS = {
    "posix": [
        "sh",
        "-c",
        f'if [ -z "${KEY_ENV}" ]; then echo "{_MISSING}" >&2; exit 1; fi; printf %s "${KEY_ENV}"',
    ],
    "windows": [
        "powershell",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        f"if (-not $env:{KEY_ENV}) {{ [Console]::Error.WriteLine('{_MISSING}'); exit 1 }}; "
        f"[Console]::Out.Write($env:{KEY_ENV})",
    ],
}


def model_limits(entry: dict | None, caps: Iterable[int | None]) -> dict[str, int] | None:
    """opencode ``limit`` for a model: its own context (capped by the plans of the pools that
    serve it) and output size. None when the context size isn't known."""
    context = None
    for key in CONTEXT_KEYS:
        value = (entry or {}).get(key)
        if isinstance(value, int) and value > 0:
            context = value
            break
    known_caps = [c for c in caps if c]
    if known_caps:
        context = min([context, *known_caps]) if context else min(known_caps)
    if not context:
        return None
    output = next(
        (v for k in OUTPUT_KEYS if isinstance(v := (entry or {}).get(k), int) and v > 0),
        min(DEFAULT_OUTPUT, context // 2),
    )
    return {"context": context, "output": min(output, context)}


def provider_config(base_url: str, models: dict[str, dict]) -> dict:
    """The opencode config (``provider`` block) for ``models`` = {id: {"name", "limit"?}}."""
    return {
        "$schema": "https://opencode.ai/config.json",
        "provider": {
            PROVIDER_ID: {
                "npm": "@ai-sdk/openai-compatible",
                "name": "QueueRouter",
                "options": {
                    "baseURL": f"{base_url}/v1",
                    "apiKey": f"{{env:{KEY_ENV}}}",
                    "timeout": False,  # queued requests may wait longer than opencode's default
                },
                "models": models,
            }
        },
    }


def wellknown_manifest(base_url: str, platform: str) -> dict:
    return {
        "auth": {"command": LOGIN_COMMANDS[platform], "env": KEY_ENV},
        "remote_config": {
            "url": f"{base_url}/v1/opencode-config",
            "headers": {"Authorization": f"Bearer {{env:{KEY_ENV}}}"},
        },
    }


PLUGIN_PATH = "/opencode/queuerouter.js"
_PLUGIN_TEMPLATE = Path(__file__).with_name("opencode_plugin.js")


@functools.cache
def _plugin_template() -> str:
    return _PLUGIN_TEMPLATE.read_text(encoding="utf-8")


def plugin_source(base_url: str) -> str:
    """The opencode plugin (app/opencode_plugin.js) pointed at this QueueRouter."""
    return _plugin_template().replace("__QUEUEROUTER_BASE__", json.dumps(base_url)[1:-1])


def plugin_install_commands(base_url: str) -> dict[str, str]:
    """One-line installs into opencode's global plugin folder (~/.config/opencode/plugin)."""
    url = f"{base_url}{PLUGIN_PATH}"
    return {
        "posix": f"mkdir -p ~/.config/opencode/plugin && curl -fsSL {url} -o ~/.config/opencode/plugin/queuerouter.js",
        "windows": (
            '$d="$HOME/.config/opencode/plugin"; New-Item -ItemType Directory -Force $d | Out-Null; '
            f'Invoke-WebRequest {url} -OutFile "$d/queuerouter.js"'
        ),
    }
