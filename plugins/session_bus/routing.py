"""Envelope helpers for session-routing messages.

Every message that crosses a gateway boundary is wrapped in an envelope
whose canonical shape is::

    {
      "msg_id": "<uuid4>",
      "ts":     <unix-epoch float>,
      "ts_iso": <ISO8601 UTC>,
      "from":   "<gateway_id>/<session_key>",
      "to":     "<gateway_id>/<session_key>",
      "payload": { ... arbitrary JSON ... },
      "kind":   "session_route",
      "v":      1,
    }

Phase 1 does NOT sign these envelopes — LAN + Courier VPN are the access
boundary. The shape is fixed in advance so Phase 2 (mTLS / shared-token
signatures) can hook into ``envelope_signable_bytes()`` without a wire
format change. Signers MUST byte-encode the envelope with the canonical
``json.dumps(..., sort_keys=True, separators=(",", ":"))`` form so the
broker and the recipient agree on the signed digest.

Versioning: v0.1.0 (Phase 1).
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ENVELOPE_KIND = "session_route"
ENVELOPE_VERSION = 1


def build_envelope(
    *,
    from_address: str,
    to_address: str,
    payload: Dict[str, Any],
    msg_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a canonical envelope dict for a routed message.

    ``msg_id`` is generated as a uuid4 when not provided — the caller
    keeps the reference for dedup/audit (e.g. inline echo of the tool
    result so the agent can match replies to sends).
    """
    now = time.time()
    return {
        "msg_id": msg_id or str(uuid.uuid4()),
        "ts": now,
        "ts_iso": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "from": from_address,
        "to": to_address,
        "payload": payload,
        "kind": ENVELOPE_KIND,
        "v": ENVELOPE_VERSION,
    }


def validate_envelope(envelope: Any) -> None:
    """Raise ``ValueError`` if ``envelope`` does not match the canonical shape.

    Strict on type contracts so the recipient can rely on every field
    being present and well-typed. The broker will pass malformed JSON
    through unchanged — this is the recipient-side gate.
    """
    if not isinstance(envelope, dict):
        raise ValueError(
            f"envelope must be dict, got {type(envelope).__name__}"
        )

    msg_id = envelope.get("msg_id")
    if not isinstance(msg_id, str) or len(msg_id) != 36:
        raise ValueError(
            f"envelope.msg_id must be a 36-char uuid string, got {msg_id!r}"
        )

    ts = envelope.get("ts")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        raise ValueError(f"envelope.ts must be a number, got {ts!r}")

    from_address = envelope.get("from")
    if not isinstance(from_address, str) or not from_address:
        raise ValueError("envelope.from must be a non-empty string")

    to_address = envelope.get("to")
    if not isinstance(to_address, str) or not to_address:
        raise ValueError("envelope.to must be a non-empty string")

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("envelope.payload must be a dict")

    version = envelope.get("v")
    if version != ENVELOPE_VERSION:
        raise ValueError(
            f"envelope.v must be {ENVELOPE_VERSION}, got {version!r}"
        )


def envelope_signable_bytes(envelope: Dict[str, Any]) -> bytes:
    """Return canonical UTF-8 bytes for signing.

    Used by Phase 2 mTLS hooks. The byte form is deterministic across
    processes (sort_keys + compact separators) so a signature computed
    by the sender matches a verification by the recipient without a
    separate canonicalization protocol.
    """
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def envelope_to_headers(envelope: Dict[str, Any]) -> Dict[str, str]:
    """Project the envelope onto a flat dict suitable for NATS headers.

    Headers carry only the routing-relevant fields (msg_id, from, to,
    kind, v) — the full payload stays in the message body so subscribers
    that filter by header do not pay the deserialization cost.
    """
    return {
        "msg_id": str(envelope.get("msg_id", "")),
        "from": str(envelope.get("from", "")),
        "to": str(envelope.get("to", "")),
        "kind": str(envelope.get("kind", ENVELOPE_KIND)),
        "v": str(envelope.get("v", ENVELOPE_VERSION)),
    }