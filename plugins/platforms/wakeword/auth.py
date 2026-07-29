from __future__ import annotations

import hashlib
import hmac
import stat
import time
from pathlib import Path
def compute_signature(secret: bytes, body: bytes, ts: int, nonce: str) -> str:
    """Return HMAC-SHA256 over the multipart body, timestamp, and nonce."""
    payload = body + b":" + str(ts).encode("ascii") + b":" + nonce.encode("ascii")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def verify_signature(
    secret: bytes,
    body: bytes,
    ts: int,
    nonce: str,
    given: str,
    *,
    max_age_seconds: int | None = None,
) -> bool:
    """Verify body, timestamp, nonce, and optional timestamp freshness."""
    stale = max_age_seconds is not None and abs(time.time() - ts) > max_age_seconds
    if stale:
        return False
    return hmac.compare_digest(compute_signature(secret, body, ts, nonce), given)


def load_secret(path: Path) -> bytes | None:
    """Load a 32-byte hexadecimal key only when its mode is exactly 0600."""
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        raw = path.read_text(encoding="ascii").strip()
        secret = bytes.fromhex(raw)
    except (OSError, UnicodeError, ValueError):
        return None
    return secret if mode == 0o600 and len(secret) == 32 else None
