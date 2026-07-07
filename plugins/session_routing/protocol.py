"""Typed message protocol registry for session-routing (v0.3.0).

Replaces the Phase-1 "arbitrary JSON payload" contract (routing.py) with
a CLOSED registry of payload types. Every payload that crosses a gateway
boundary carries the cross-cutting fields::

    type:             one of the registered types below
    channel_id:       "<sender_session_id>:<hash8(peer_address)>"
    session_id:       sender's session id (always present)
    in_reply_to:      envelope msg_id being replied to, or None
    protocol_version: 1

Registered types:

    handshake.request / .ack / .established / .reject / .bye
    message.text          — visible back-channel text (injected as a
                            synthetic user turn on the recipient side)
    message.error         — peer-to-peer protocol error (logged, never
                            injected; NEVER answered with another
                            message.error — loop guard)
    message.ack_delivery  — delivery confirmation for a message.text
    delegate.task         — RESERVED: receive returns unsupported_type
    delegate.result       — RESERVED: receive returns unsupported_type

Reserved namespace ``app.<plugin>.<type>`` is a Phase-2 naming
convention for plugin-defined types; Phase 1 ships no validator for it
and the dispatcher answers them with ``unknown_type`` like any other
unregistered type (explicitly: NO silent drops).

Error codes carried by ``message.error``::

    unknown_type      — payload.type not in the registry
    unsupported_type  — registered but not dispatchable in Phase 1
    validation_error  — cross-cutting/typed field missing or malformed
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

PROTOCOL_VERSION = 1

HANDSHAKE_TYPES = frozenset({
    "handshake.request",
    "handshake.ack",
    "handshake.established",
    "handshake.reject",
    "handshake.bye",
})

MESSAGE_TYPES = frozenset({
    "message.text",
    "message.error",
    "message.ack_delivery",
})

# Registered but not dispatchable in Phase 1: the receive side answers
# with message.error{error_code: unsupported_type} instead of executing.
UNSUPPORTED_TYPES = frozenset({
    "delegate.task",
    "delegate.result",
})

REGISTERED_TYPES = HANDSHAKE_TYPES | MESSAGE_TYPES | UNSUPPORTED_TYPES

ERROR_CODE_UNKNOWN_TYPE = "unknown_type"
ERROR_CODE_UNSUPPORTED_TYPE = "unsupported_type"
ERROR_CODE_VALIDATION = "validation_error"
# The outer ENVELOPE (not the payload) failed routing.validate_envelope
# — e.g. a hand-rolled publish that bypassed session_route_send.
ERROR_CODE_INVALID_ENVELOPE = "invalid_envelope"
# Envelope + payload valid, but the addressed session_key is not live
# on the recipient gateway (ended in flight, or presence was stale at
# send time). The message was acked and will NOT be injected.
ERROR_CODE_NO_LIVE_SESSION = "no_live_session"
# The session is live but the synthetic-turn enqueue failed; the
# message was durably deduped and will NOT be redelivered or injected.
ERROR_CODE_DELIVERY_FAILED = "delivery_failed"

_APP_NAMESPACE_RE = re.compile(r"^app\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-.]+$")

# Per-type REQUIRED extra fields (beyond the cross-cutting set) and
# their accepted Python types. Optional extras are not validated —
# closed registry, open payload tail (forward-compatible additions).
_TYPE_REQUIRED_FIELDS: Dict[str, Dict[str, tuple]] = {
    "handshake.request": {"nonce": (str,), "capabilities": (list,)},
    "handshake.ack": {"nonce": (str,), "capabilities": (list,)},
    "handshake.established": {"nonce": (str,)},
    "handshake.reject": {"reason": (str,)},
    "handshake.bye": {},  # reason optional
    "message.text": {"body": (str,)},
    "message.error": {"error_code": (str,)},
    "message.ack_delivery": {},  # in_reply_to carries the correlation
    "delegate.task": {},
    "delegate.result": {},
}


class ProtocolError(ValueError):
    """Raised by ``validate_payload_strict`` on malformed payloads."""

    def __init__(self, error_code: str, detail: str) -> None:
        super().__init__(f"{error_code}: {detail}")
        self.error_code = error_code
        self.detail = detail


def is_registered_type(payload_type: Any) -> bool:
    return isinstance(payload_type, str) and payload_type in REGISTERED_TYPES


def is_app_namespaced(payload_type: Any) -> bool:
    """True for the reserved Phase-2 ``app.<plugin>.<type>`` shape."""
    return isinstance(payload_type, str) and bool(
        _APP_NAMESPACE_RE.match(payload_type)
    )


def validate_payload(payload: Any) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validate a payload against the closed registry.

    Returns ``(ok, error_code, detail)``. Never raises — the dispatcher
    turns a failure into a ``message.error`` response (or a log line),
    it must not crash the consumer loop.
    """
    if not isinstance(payload, dict):
        return (
            False,
            ERROR_CODE_VALIDATION,
            f"payload must be dict, got {type(payload).__name__}",
        )

    ptype = payload.get("type")
    if not isinstance(ptype, str) or not ptype:
        return (False, ERROR_CODE_VALIDATION, "payload.type missing or empty")
    if ptype not in REGISTERED_TYPES:
        # app.* is reserved naming, still unknown in Phase 1 — same
        # error code so senders get a uniform contract.
        return (False, ERROR_CODE_UNKNOWN_TYPE, f"unknown payload type {ptype!r}")

    # Cross-cutting fields. The closed-registry spec requires channel_id
    # and session_id on every payload, BUT interop with v0.2 senders
    # (whose ``session_route_send`` wrapper produced minimal payloads
    # with only ``body`` + ``type``) means we have to tolerate missing
    # cross-cutting fields and let the dispatcher fill them in from
    # envelope context. We still reject the obviously malformed
    # (non-string-when-present).
    for field in ("channel_id", "session_id"):
        if field in payload:
            value = payload[field]
            if value is not None and (not isinstance(value, str) or not value):
                return (
                    False,
                    ERROR_CODE_VALIDATION,
                    f"payload.{field} must be a non-empty string when present",
                )
    if "in_reply_to" in payload and payload["in_reply_to"] is not None:
        if not isinstance(payload["in_reply_to"], str):
            return (
                False,
                ERROR_CODE_VALIDATION,
                "payload.in_reply_to must be a string or null",
            )
    # protocol_version is optional for interop: pre-v0.3 senders did
    # not emit it (they used envelope-level ``v: 1``). The dispatcher
    # normalises to PROTOCOL_VERSION on receive.
    if "protocol_version" in payload and payload["protocol_version"] is not None:
        if payload["protocol_version"] != PROTOCOL_VERSION:
            return (
                False,
                ERROR_CODE_VALIDATION,
                f"payload.protocol_version must be {PROTOCOL_VERSION}, got {payload['protocol_version']!r}",
            )

    # Per-type required extras.
    for field, types in _TYPE_REQUIRED_FIELDS[ptype].items():
        value = payload.get(field)
        if not isinstance(value, types):
            expected = "/".join(t.__name__ for t in types)
            return (
                False,
                ERROR_CODE_VALIDATION,
                f"payload.{field} must be {expected} for {ptype}",
            )
    return (True, None, None)


def validate_payload_strict(payload: Any) -> None:
    """Raise ``ProtocolError`` instead of returning a tuple."""
    ok, error_code, detail = validate_payload(payload)
    if not ok:
        raise ProtocolError(error_code or ERROR_CODE_VALIDATION, detail or "")


def build_message_text(
    *,
    channel_id: str,
    session_id: str,
    body: str,
    in_reply_to: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "type": "message.text",
        "channel_id": channel_id,
        "session_id": session_id,
        "in_reply_to": in_reply_to,
        "protocol_version": PROTOCOL_VERSION,
        "body": body,
    }


def build_message_error(
    *,
    channel_id: str,
    session_id: str,
    error_code: str,
    in_reply_to: Optional[str],
    detail: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "type": "message.error",
        "channel_id": channel_id,
        "session_id": session_id,
        "in_reply_to": in_reply_to,
        "protocol_version": PROTOCOL_VERSION,
        "error_code": error_code,
    }
    if detail:
        payload["detail"] = detail
    return payload


def build_ack_delivery(
    *,
    channel_id: str,
    session_id: str,
    in_reply_to: str,
) -> Dict[str, Any]:
    return {
        "type": "message.ack_delivery",
        "channel_id": channel_id,
        "session_id": session_id,
        "in_reply_to": in_reply_to,
        "protocol_version": PROTOCOL_VERSION,
    }
