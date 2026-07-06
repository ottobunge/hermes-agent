"""Handshake protocol for session-routing back-channels (v0.3.0).

Minimal 3-way handshake carried inside routed envelopes::

    initiator                          responder
    ── handshake.request ────────────▶  validates, opens channel
    ◀─ handshake.ack ────────────────  echoes nonce, own capabilities
    ── handshake.established ────────▶  both sides ESTABLISHED

Plus ``handshake.reject`` (responder refuses the request) and
``handshake.bye`` (either side closes an established channel).

Frames are payload dicts (they travel in ``envelope["payload"]``); the
closed type registry that validates them lives in ``protocol.py``. This
module owns frame CONSTRUCTION and the per-channel state machine:
nonce correlation, legal transitions, timeout, and the simultaneous-
request race.

Race determinism: when both sides send ``handshake.request`` at the
same time, the side with the lexicographically SMALLER canonical
address wins as initiator; the other side abandons its own request and
answers the winner's (``resolve_race``). Both sides compute the same
verdict from the same two strings — no coordination round needed.

No signing in Phase 1 — ``routing.envelope_signable_bytes`` is the seam
for Phase 2.
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

PROTOCOL_VERSION = 1
DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 30.0

# Capabilities advertised when the caller doesn't pass an explicit set.
DEFAULT_CAPABILITIES = ["text", "ack_delivery"]


class HandshakeError(ValueError):
    """Raised on invalid frames or illegal state transitions."""


class HandshakeState(str, Enum):
    """Per-side handshake progress (finer-grained than the KV
    ChannelState — INITIATING covers REQUEST_SENT and ACK_SENT)."""

    IDLE = "IDLE"
    REQUEST_SENT = "REQUEST_SENT"      # initiator: waiting for ack
    ACK_SENT = "ACK_SENT"              # responder: waiting for established
    ESTABLISHED = "ESTABLISHED"
    REJECTED = "REJECTED"
    CLOSED = "CLOSED"


def new_nonce() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Frame builders — all cross-cutting fields present on every frame
# ---------------------------------------------------------------------------

def _base_frame(
    frame_type: str,
    *,
    channel_id: str,
    session_id: str,
    in_reply_to: Optional[str] = None,
) -> Dict[str, Any]:
    if not channel_id:
        raise HandshakeError(f"{frame_type}: channel_id is required")
    if not session_id:
        raise HandshakeError(f"{frame_type}: session_id is required")
    return {
        "type": frame_type,
        "channel_id": channel_id,
        "session_id": session_id,
        "in_reply_to": in_reply_to,
        "protocol_version": PROTOCOL_VERSION,
    }


def build_request(
    *,
    channel_id: str,
    session_id: str,
    nonce: str,
    capabilities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    frame = _base_frame(
        "handshake.request", channel_id=channel_id, session_id=session_id
    )
    frame["nonce"] = nonce
    frame["capabilities"] = list(capabilities or DEFAULT_CAPABILITIES)
    return frame


def build_ack(
    *,
    channel_id: str,
    session_id: str,
    nonce: str,
    in_reply_to: str,
    capabilities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    frame = _base_frame(
        "handshake.ack",
        channel_id=channel_id,
        session_id=session_id,
        in_reply_to=in_reply_to,
    )
    frame["nonce"] = nonce
    frame["capabilities"] = list(capabilities or DEFAULT_CAPABILITIES)
    return frame


def build_established(
    *,
    channel_id: str,
    session_id: str,
    nonce: str,
    in_reply_to: str,
) -> Dict[str, Any]:
    frame = _base_frame(
        "handshake.established",
        channel_id=channel_id,
        session_id=session_id,
        in_reply_to=in_reply_to,
    )
    frame["nonce"] = nonce
    return frame


def build_reject(
    *,
    channel_id: str,
    session_id: str,
    in_reply_to: str,
    reason: str,
) -> Dict[str, Any]:
    frame = _base_frame(
        "handshake.reject",
        channel_id=channel_id,
        session_id=session_id,
        in_reply_to=in_reply_to,
    )
    frame["reason"] = reason
    return frame


def build_bye(
    *,
    channel_id: str,
    session_id: str,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    frame = _base_frame(
        "handshake.bye", channel_id=channel_id, session_id=session_id
    )
    frame["reason"] = reason
    return frame


# ---------------------------------------------------------------------------
# Race determinism
# ---------------------------------------------------------------------------

def resolve_race(my_address: str, peer_address: str) -> str:
    """Deterministic winner for simultaneous handshake.requests.

    Returns ``"mine"`` when THIS side's request should proceed (peer
    must abandon theirs and answer ours), ``"theirs"`` otherwise. Both
    sides evaluate the same comparison, so exactly one side wins.
    """
    if my_address == peer_address:
        raise HandshakeError(
            f"race between identical addresses: {my_address!r}"
        )
    return "mine" if my_address < peer_address else "theirs"


# ---------------------------------------------------------------------------
# Per-channel state machine
# ---------------------------------------------------------------------------

class HandshakeChannel:
    """One side's handshake progress for a single channel.

    The machine validates frames and transitions; it does NOT publish —
    callers (the ``session_establish`` tool, the receive-side
    dispatcher) send the returned frames over NATS and persist channel
    state to KV. ``now`` is injectable for timeout tests.
    """

    def __init__(
        self,
        *,
        channel_id: str,
        session_id: str,
        my_address: str,
        peer_address: str,
        timeout_seconds: float = DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
        now: Optional[float] = None,
    ) -> None:
        self.channel_id = channel_id
        self.session_id = session_id
        self.my_address = my_address
        self.peer_address = peer_address
        self.timeout_seconds = timeout_seconds
        self.state = HandshakeState.IDLE
        self.nonce: Optional[str] = None
        self.request_msg_id: Optional[str] = None
        self.peer_capabilities: List[str] = []
        self.started_at: Optional[float] = float(now) if now is not None else None
        self.reject_reason: Optional[str] = None

    # -- initiator side ----------------------------------------------------

    def start(
        self,
        *,
        capabilities: Optional[List[str]] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """IDLE → REQUEST_SENT. Returns the request frame to publish."""
        self._require_state("start", HandshakeState.IDLE)
        self.nonce = new_nonce()
        self.started_at = float(now) if now is not None else time.time()
        self.state = HandshakeState.REQUEST_SENT
        return build_request(
            channel_id=self.channel_id,
            session_id=self.session_id,
            nonce=self.nonce,
            capabilities=capabilities,
        )

    def on_ack(
        self,
        payload: Dict[str, Any],
        *,
        request_msg_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """REQUEST_SENT → ESTABLISHED. Returns the established frame.

        Validates the nonce echo, and — when the caller recorded the
        envelope msg_id of its request — the ack's ``in_reply_to``
        correlation. Raises ``HandshakeError`` on either mismatch, and
        the state does NOT advance (a forged/stale ack cannot open the
        channel).
        """
        self._require_state("on_ack", HandshakeState.REQUEST_SENT)
        if payload.get("type") != "handshake.ack":
            raise HandshakeError(
                f"on_ack: expected handshake.ack, got {payload.get('type')!r}"
            )
        if payload.get("nonce") != self.nonce:
            raise HandshakeError(
                "on_ack: nonce mismatch — expected "
                f"{self.nonce!r}, got {payload.get('nonce')!r}"
            )
        expected_reply_to = request_msg_id or self.request_msg_id
        if expected_reply_to and payload.get("in_reply_to") != expected_reply_to:
            raise HandshakeError(
                "on_ack: in_reply_to mismatch — expected "
                f"{expected_reply_to!r}, got {payload.get('in_reply_to')!r}"
            )
        self.peer_capabilities = list(payload.get("capabilities") or [])
        self.state = HandshakeState.ESTABLISHED
        return build_established(
            channel_id=self.channel_id,
            session_id=self.session_id,
            nonce=self.nonce or "",
            in_reply_to=str(payload.get("_envelope_msg_id") or expected_reply_to or ""),
        )

    def on_reject(self, payload: Dict[str, Any]) -> None:
        """REQUEST_SENT → REJECTED (terminal)."""
        self._require_state("on_reject", HandshakeState.REQUEST_SENT)
        if payload.get("type") != "handshake.reject":
            raise HandshakeError(
                f"on_reject: expected handshake.reject, got {payload.get('type')!r}"
            )
        self.reject_reason = payload.get("reason")
        self.state = HandshakeState.REJECTED

    # -- responder side ------------------------------------------------------

    def on_request(
        self,
        payload: Dict[str, Any],
        *,
        request_envelope_msg_id: str,
        capabilities: Optional[List[str]] = None,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """IDLE → ACK_SENT. Returns the ack frame to publish."""
        self._require_state("on_request", HandshakeState.IDLE)
        if payload.get("type") != "handshake.request":
            raise HandshakeError(
                f"on_request: expected handshake.request, got {payload.get('type')!r}"
            )
        nonce = payload.get("nonce")
        if not isinstance(nonce, str) or not nonce:
            raise HandshakeError("on_request: request carries no nonce")
        self.nonce = nonce
        self.peer_capabilities = list(payload.get("capabilities") or [])
        self.started_at = float(now) if now is not None else time.time()
        self.state = HandshakeState.ACK_SENT
        return build_ack(
            channel_id=self.channel_id,
            session_id=self.session_id,
            nonce=nonce,
            in_reply_to=request_envelope_msg_id,
            capabilities=capabilities,
        )

    def on_established(self, payload: Dict[str, Any]) -> None:
        """ACK_SENT → ESTABLISHED (responder completes)."""
        self._require_state("on_established", HandshakeState.ACK_SENT)
        if payload.get("type") != "handshake.established":
            raise HandshakeError(
                "on_established: expected handshake.established, got "
                f"{payload.get('type')!r}"
            )
        if payload.get("nonce") != self.nonce:
            raise HandshakeError(
                "on_established: nonce mismatch — expected "
                f"{self.nonce!r}, got {payload.get('nonce')!r}"
            )
        self.state = HandshakeState.ESTABLISHED

    # -- either side ---------------------------------------------------------

    def close(self, *, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Any live state → CLOSED. Returns a bye frame when the channel
        was ESTABLISHED (peers of half-open channels time out on their
        own). Idempotent: closing a CLOSED/REJECTED channel is a no-op
        returning None — concurrent closes converge."""
        if self.state in (HandshakeState.CLOSED, HandshakeState.REJECTED):
            return None
        was_established = self.state == HandshakeState.ESTABLISHED
        self.state = HandshakeState.CLOSED
        if not was_established:
            return None
        return build_bye(
            channel_id=self.channel_id,
            session_id=self.session_id,
            reason=reason,
        )

    def on_bye(self, payload: Dict[str, Any]) -> None:
        """Peer closed. Any state → CLOSED (idempotent)."""
        if payload.get("type") != "handshake.bye":
            raise HandshakeError(
                f"on_bye: expected handshake.bye, got {payload.get('type')!r}"
            )
        self.state = HandshakeState.CLOSED

    def is_expired(self, *, now: Optional[float] = None) -> bool:
        """True when a pending handshake outlived ``timeout_seconds``.

        Only REQUEST_SENT / ACK_SENT can expire; settled states never do.
        """
        if self.state not in (HandshakeState.REQUEST_SENT, HandshakeState.ACK_SENT):
            return False
        if self.started_at is None:
            return False
        now = now if now is not None else time.time()
        return (now - self.started_at) > self.timeout_seconds

    # -- internals -----------------------------------------------------------

    def _require_state(self, op: str, expected: HandshakeState) -> None:
        if self.state != expected:
            raise HandshakeError(
                f"{op}: illegal in state {self.state.value} "
                f"(requires {expected.value})"
            )
