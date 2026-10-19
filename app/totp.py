"""Time-based one-time passwords (RFC 6238: SHA-1, 30 s, 6 digits), as authenticator apps use."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
from urllib.parse import quote, urlencode

import segno

STEP_S = 30
DIGITS = 6
DRIFT_STEPS = 1  # accept the previous and next code too (clock drift, typing time)
RECOVERY_CODES = 10


def new_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def _key(secret: str) -> bytes:
    secret = secret.strip().replace(" ", "").upper()
    return base64.b32decode(secret + "=" * (-len(secret) % 8))


def code_at(secret: str, counter: int) -> str:
    digest = hmac.new(_key(secret), struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(value % 10**DIGITS).zfill(DIGITS)


def verify(secret: str, code: str, now: float | None = None, after: int = -1) -> int | None:
    """The time step ``code`` matches (only steps later than ``after``, so a code can't be
    replayed), or None."""
    code = "".join(ch for ch in str(code) if ch.isdigit())
    if len(code) != DIGITS:
        return None
    current = int((now if now is not None else time.time()) // STEP_S)
    for counter in range(current - DRIFT_STEPS, current + DRIFT_STEPS + 1):
        if counter > after and hmac.compare_digest(code_at(secret, counter), code):
            return counter
    return None


def provisioning_uri(secret: str, account: str, issuer: str = "QueueRouter") -> str:
    label = quote(f"{issuer}:{account}")
    return f"otpauth://totp/{label}?" + urlencode({"secret": secret, "issuer": issuer, "digits": DIGITS})


def qr_svg(uri: str) -> str:
    """Inline SVG QR code (black on white so every phone camera reads it, also in dark mode)."""
    return segno.make(uri, error="m").svg_inline(scale=5, border=3, dark="#000000", light="#ffffff")


def new_recovery_codes() -> list[str]:
    return [f"{secrets.token_hex(3)}-{secrets.token_hex(3)}" for _ in range(RECOVERY_CODES)]


def hash_recovery(code: str) -> str:
    return hashlib.sha256(code.strip().lower().replace(" ", "").encode()).hexdigest()
