"""opencode integration: generated provider config and the /.well-known/opencode manifest.

opencode doesn't discover models from a custom provider's /v1/models, so QueueRouter writes the
provider block for it: every model and alias the client may use, with context/output limits.

- ``GET /v1/opencode-config`` (client key): the config, for ``OPENCODE_CONFIG`` or as remote config.
- ``GET /.well-known/opencode``: lets ``opencode auth login https://<domain>`` store the client key
  and pull the config above on every start (``remote_config``), so model lists stay current. Only for
  opencode 1.x: 2.x runs the login in its background service and keeps the key out of the plugin's reach,
  so ``plugin_install_commands`` is the way in.
"""

from __future__ import annotations

import functools
import hashlib
import json
import shlex
from collections.abc import Iterable
from pathlib import Path

PROVIDER_ID = "queuerouter"
KEY_ENV = "QUEUEROUTER_KEY"
KEY_FILE = "queuerouter.key"  # beside the plugin, which reads it
KEY_PLACEHOLDER = "YOUR_CLIENT_KEY"
DEFAULT_OUTPUT = 16_384
CONTEXT_KEYS = (
    "context_length",
    "max_model_len",
    "max_context_length",
    "context_window",
    "max_input_tokens",
)  # Groq, Anthropic
# max_output_length: Synthetic; max_tokens (last): Anthropic
OUTPUT_KEYS = ("max_output_tokens", "max_completion_tokens", "max_output_length", "max_tokens")

# The login command only hands opencode the key from $QUEUEROUTER_KEY (opencode runs it without a
# shell and stores whatever it prints). Windows has no `sh`, hence a PowerShell variant.
_MISSING = (
    "opencode 2.x: use the one-line install on the Clients page of QueueRouter instead. "
    f"Older opencode: set {KEY_ENV} to your client key, then run the login again."
)
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


def listed_output(entry: dict | None) -> int | None:
    """The longest answer a model list item gives: under one of OUTPUT_KEYS, else OpenRouter's
    ``top_provider.max_completion_tokens``. None when it doesn't say."""
    entry = entry or {}
    size = next((v for k in OUTPUT_KEYS if isinstance(v := entry.get(k), int) and v > 0), None)
    top = entry.get("top_provider")
    if size is None and isinstance(top, dict) and isinstance(top.get("max_completion_tokens"), int):
        size = top["max_completion_tokens"] if top["max_completion_tokens"] > 0 else None
    return size


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
    output = listed_output(entry) or min(DEFAULT_OUTPUT, context // 2)
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


PLUGIN_HEADER = "X-QueueRouter-Plugin"  # on /v1/opencode-config: the current plugin's fingerprint


@functools.cache
def _plugin_template() -> str:
    return _PLUGIN_TEMPLATE.read_text(encoding="utf-8")


@functools.cache
def plugin_fingerprint() -> str:
    """Changes only when the plugin itself does: installed copies update themselves when it differs."""
    return hashlib.sha256(_plugin_template().encode()).hexdigest()[:16]


def plugin_source(base_url: str) -> str:
    """The opencode plugin (app/opencode_plugin.js) pointed at this QueueRouter."""
    return (
        _plugin_template()
        .replace("__QUEUEROUTER_BASE__", json.dumps(base_url)[1:-1])
        .replace("__QUEUEROUTER_PLUGIN__", plugin_fingerprint())
    )


def plugin_install_commands(base_url: str, key: str = KEY_PLACEHOLDER) -> dict[str, str]:
    """One-line installs into opencode's global plugin folder (~/.config/opencode/plugin): the plugin, and the client
    key in a file beside it, so nothing goes in the environment and opencode's own files stay as they are."""
    url = f"{base_url}{PLUGIN_PATH}"
    powershell_key = key.replace("'", "''")
    return {
        "posix": (
            f"mkdir -p ~/.config/opencode/plugin && curl -fsSL {url} -o ~/.config/opencode/plugin/queuerouter.js && "
            f"(umask 077; printf %s {shlex.quote(key)} > ~/.config/opencode/plugin/{KEY_FILE})"
        ),
        "windows": (
            '$d="$HOME/.config/opencode/plugin"; New-Item -ItemType Directory -Force $d | Out-Null; '
            f'Invoke-WebRequest {url} -OutFile "$d/queuerouter.js"; '
            # ascii: Windows PowerShell's utf8 would start the file with a BOM
            f"""Set-Content "$d/{KEY_FILE}" '{powershell_key}' -Encoding ascii"""
        ),
    }
