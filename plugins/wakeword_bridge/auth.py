from __future__ import annotations

import hashlib
import hmac
import time


def compute_signature(secret: bytes, body: bytes, ts: int) -> str:
    """Return the request signature for a body and Unix timestamp."""
    payload = body + b":" + str(ts).encode("ascii")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def verify_signature(
    secret: bytes,
    body: bytes,
    ts: int,
    given: str,
    *,
    max_age_seconds: int = 30,
) -> bool:
    """Accept a matching signature only within the timestamp freshness window."""
    if abs(time.time() - ts) > max_age_seconds:
        return False
    expected = compute_signature(secret, body, ts)
    return hmac.compare_digest(expected, given)
