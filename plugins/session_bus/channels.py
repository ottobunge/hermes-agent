"""Channel identity + state for session-routing back-channels.

A *channel* is one side's view of a back-channel between two sessions.
Each side keys its own state by a **local** channel id::

    channel_id = "<local_session_id>:<peer_hash>"

where ``peer_hash`` is the first 8 hex chars of ``sha256(peer_address)``.
The id is session-scoped (channels die with sessions) but peer-distinct,
so one session can hold channels to multiple peers without the state
records overwriting each other (Kimi BLOCKER 3 / Fable concern 5).

NATS KV key sanitization
------------------------
NATS KV keys must match ``[-/_=\\.a-zA-Z0-9]+`` — ``:`` is FORBIDDEN.
The wire format (envelope payloads, tool results) keeps the colon; only
the ``session_channels`` bucket key is sanitized via
``channel_id_to_kv_key`` (``:`` → ``.``). Session ids
(``YYYYMMDD_HHMMSS_hex``) and hex hashes never contain ``.``, so the
mapping is bijective and ``kv_key_to_channel_id`` can invert it.

State machine (per channel)::

    INITIATING ──▶ ESTABLISHED ──▶ CLOSED
        │
        └──▶ REJECTED (terminal)

KV is lifecycle/dedupe state ONLY — never delivery authority. Live
session lookup (``enqueue_internal_session_event``) decides whether a
message can be injected now.

Versioning: v0.3.0.
"""

from __future__ import annotations

import hashlib
import time
from enum import Enum
from typing import Any, Dict, List, Optional

# Bounded per-channel dedupe window. JetStream can redeliver more than
# the immediate predecessor, so a single last_msg_id is insufficient
# (Kimi BLOCKER 2); 100 ids comfortably covers redelivery bursts while
# keeping the KV row small.
RECENT_MSG_IDS_CAP = 100

PEER_HASH_LEN = 8


class ChannelState(str, Enum):
    """Lifecycle states persisted in the ``session_channels`` KV bucket."""

    INITIATING = "INITIATING"
    ESTABLISHED = "ESTABLISHED"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"


# Legal transitions. Terminal states have no exits; CLOSED→CLOSED and
# REJECTED→REJECTED are tolerated by is_valid_transition so concurrent
# closes (both sides send handshake.bye) converge without errors.
_TRANSITIONS = {
    ChannelState.INITIATING: {
        ChannelState.ESTABLISHED,
        ChannelState.CLOSED,
        ChannelState.REJECTED,
    },
    ChannelState.ESTABLISHED: {ChannelState.CLOSED},
    ChannelState.CLOSED: set(),
    ChannelState.REJECTED: set(),
}


def is_valid_transition(current: ChannelState, new: ChannelState) -> bool:
    """True when ``current → new`` is legal (idempotent self-loops on
    terminal states allowed)."""
    if current == new and current in (ChannelState.CLOSED, ChannelState.REJECTED):
        return True
    return new in _TRANSITIONS.get(current, set())


def peer_hash(peer_address: str) -> str:
    """First 8 hex chars of sha256(peer_address) — the channel_id suffix."""
    return hashlib.sha256(peer_address.encode("utf-8")).hexdigest()[:PEER_HASH_LEN]


def channel_id_for(session_id: str, peer_address: str) -> str:
    """Local channel id: ``<local_session_id>:<peer_hash>``."""
    if not session_id:
        raise ValueError("channel_id_for requires a non-empty session_id")
    if not peer_address:
        raise ValueError("channel_id_for requires a non-empty peer_address")
    return f"{session_id}:{peer_hash(peer_address)}"


def channel_id_to_kv_key(channel_id: str) -> str:
    """NATS-KV-safe key for a channel id (``:`` → ``.``).

    KV keys forbid ``:``; wire format keeps it. Bijective because
    session ids and hex hashes never contain ``.``.
    """
    return channel_id.replace(":", ".")


def kv_key_to_channel_id(kv_key: str) -> str:
    """Inverse of ``channel_id_to_kv_key``.

    The peer-hash suffix is the segment after the LAST ``.`` — session
    ids contain ``_`` but never ``.``, so rsplit is unambiguous.
    """
    head, sep, tail = kv_key.rpartition(".")
    if not sep:
        return kv_key
    return f"{head}:{tail}"


def build_channel_record(
    *,
    channel_id: str,
    session_id: str,
    peer_address: str,
    state: ChannelState,
    capabilities: Optional[List[str]] = None,
    opened_at: Optional[float] = None,
) -> Dict[str, Any]:
    """Canonical ``session_channels`` KV value for one channel."""
    return {
        "channel_id": channel_id,
        "session_id": session_id,
        "peer_address": peer_address,
        "peer_hash": peer_hash(peer_address),
        "state": ChannelState(state).value,
        "opened_at": opened_at if opened_at is not None else time.time(),
        "last_msg_id": None,
        "recent_msg_ids": [],
        "capabilities": list(capabilities or []),
    }


def record_msg_id(record: Dict[str, Any], msg_id: str) -> Dict[str, Any]:
    """Append ``msg_id`` to the channel's bounded dedupe window (in place).

    ``recent_msg_ids`` is FIFO-trimmed to ``RECENT_MSG_IDS_CAP``; the
    101st id evicts the oldest. Also refreshes ``last_msg_id``.
    """
    recent = record.setdefault("recent_msg_ids", [])
    if msg_id not in recent:
        recent.append(msg_id)
        del recent[:-RECENT_MSG_IDS_CAP]
    record["last_msg_id"] = msg_id
    return record


def has_seen_msg_id(record: Optional[Dict[str, Any]], msg_id: str) -> bool:
    """Dedupe check against the channel's persisted window."""
    if not record or not msg_id:
        return False
    return msg_id in (record.get("recent_msg_ids") or [])


def apply_transition(record: Dict[str, Any], new_state: ChannelState) -> Dict[str, Any]:
    """Transition a channel record in place; raises on illegal moves."""
    current = ChannelState(record.get("state", ChannelState.INITIATING))
    new_state = ChannelState(new_state)
    if not is_valid_transition(current, new_state):
        raise ValueError(
            f"illegal channel transition {current.value} → {new_state.value} "
            f"for {record.get('channel_id')!r}"
        )
    record["state"] = new_state.value
    return record


# ---------------------------------------------------------------------------
# Async KV glue (per-call client, mirrors presence.py's pattern)
# ---------------------------------------------------------------------------

async def load_channel(
    *, servers: List[str], channel_id: str
) -> Optional[Dict[str, Any]]:
    """Read one channel row by its wire-format id (sanitized here)."""
    from plugins.session_bus.nats_client import NATSRoutingClient

    async with NATSRoutingClient(servers=servers) as client:
        return await client.read_channel(channel_id_to_kv_key(channel_id))


async def save_channel(
    *, servers: List[str], record: Dict[str, Any]
) -> None:
    """Persist one channel row (key derived from record['channel_id'])."""
    from plugins.session_bus.nats_client import NATSRoutingClient

    channel_id = record["channel_id"]
    async with NATSRoutingClient(servers=servers) as client:
        await client.write_channel(
            kv_key=channel_id_to_kv_key(channel_id), record=record
        )


async def channels_for_session(
    *, servers: List[str], session_id: str
) -> List[Dict[str, Any]]:
    """All channel rows whose local session is ``session_id``."""
    from plugins.session_bus.nats_client import NATSRoutingClient

    async with NATSRoutingClient(servers=servers) as client:
        rows = await client.list_channels()
    return [r for r in rows if r.get("session_id") == session_id]


async def close_channels_for_session(
    *,
    servers: List[str],
    session_id: str,
    my_address: str,
    reason: str = "session ended",
) -> int:
    """Close every open channel owned by ``session_id``.

    ESTABLISHED channels fire a ``handshake.bye`` to the peer before the
    KV row transitions to CLOSED; INITIATING channels just close (the
    peer's pending handshake times out on its own). Returns the number
    of channels closed. Best-effort: a bye publish failure still closes
    the local row — the peer's liveness checks are the backstop.
    """
    from plugins.session_bus import address as _address
    from plugins.session_bus import handshake as _handshake
    from plugins.session_bus import routing as _routing
    from plugins.session_bus.nats_client import NATSRoutingClient

    closed = 0
    async with NATSRoutingClient(servers=servers) as client:
        rows = await client.list_channels()
        for record in rows:
            if record.get("session_id") != session_id:
                continue
            state = record.get("state")
            if state in (ChannelState.CLOSED.value, ChannelState.REJECTED.value):
                continue

            if state == ChannelState.ESTABLISHED.value:
                try:
                    peer = record["peer_address"]
                    sender_gw, _ = _address.parse(my_address)
                    _, peer_sk = _address.parse(peer)
                    bye = _handshake.build_bye(
                        channel_id=record["channel_id"],
                        session_id=session_id,
                        reason=reason,
                    )
                    envelope = _routing.build_envelope(
                        from_address=my_address,
                        to_address=peer,
                        payload=bye,
                    )
                    await client.publish_routed(
                        subject=_address.handshake_subject(sender_gw, peer_sk),
                        payload=envelope,
                        headers=_routing.envelope_to_headers(envelope),
                    )
                except Exception:  # noqa: BLE001 — close anyway
                    pass

            apply_transition(record, ChannelState.CLOSED)
            record.pop("_kv_key", None)
            await client.write_channel(
                kv_key=channel_id_to_kv_key(record["channel_id"]),
                record=record,
            )
            closed += 1
    return closed
