"""Live-session presence registry for session-routing.

A presence entry is one row in the ``session_presence`` KV bucket — keyed
by gateway_id, value is JSON::

    {
      "agent_id":     "hermes-conrad",       # optional, cosmetic
      "session_key":  "agent:main:telegram:dm:189562939:39702",
      "platform":     "telegram",
      "last_seen":    1730000000.123,        # Unix-epoch seconds
      "inbox_consumer": "inbox-gw-thinkpad", # durable name we own
      "advertising_address":                # canonical addr other agents
        "gw-thinkpad/agent:main:telegram:dm:189562939:39702",
    }

The heartbeat loop (run in the gateway's startup hook — see __init__.py's
``on_gateway_start``) periodically calls ``update_presence``. Readers
(``list_live``, called by ``session_routing_list`` tool) filter by
freshness against ``ttl_seconds`` so a crashed gateway falls out of the
list within ~3× the heartbeat.

Why KV and not JetStream subjects for presence:
  - KV is enumerable (``.keys()``). JetStream subjects can't be listed.
  - KV entries have TTL out of the box (the broker purges after ``ttl``
    beats even if nobody's filtering).
  - We get per-gateway metadata without parsing message envelopes.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from plugins.session_bus.address import encode_subject
from plugins.session_bus.nats_client import (
    NATSRoutingClient,
    NATSRoutingUnreachable,
    is_fresh,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


def build_presence_entry(
    *,
    agent_id: str,
    gateway_id: str,
    session_key: str,
    platform: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
    live_sessions: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build the canonical presence payload for one gateway tick.

    ``last_seen`` is Unix-epoch seconds (float). We do NOT use the ISO
    string here because ``is_fresh()`` parses floats and we want to keep
    the math simple. The ISO string is also written (``last_seen_iso``)
    for human readers on KV dump.

    ``live_sessions`` (v0.3.0) lists ALL session_keys currently live on
    the gateway — presence is one row per gateway, and a single
    ``session_key`` cannot answer "is peer session X live here?" for
    cross-host sends targeting arbitrary sessions. Backward compatible:
    ``session_key`` stays populated (falling back to the first live
    session) so pre-v0.3.0 readers keep working.
    """
    now = time.time()
    sessions = [s for s in (live_sessions or []) if s]
    entry: Dict[str, Any] = {
        "agent_id": agent_id,
        "gateway_id": gateway_id,
        "session_key": session_key or (sessions[0] if sessions else ""),
        "live_sessions": sessions if sessions else ([session_key] if session_key else []),
        "platform": platform or "",
        "last_seen": now,
        "last_seen_iso": utc_now_iso(),
    }
    if extra:
        entry.update(extra)
    return entry


def inbox_consumer_name(gateway_id: str) -> str:
    """Standard name for THIS gateway's inbox durable consumer.

    Stable across restarts so a gateway that comes back online resumes
    where its last pull left off.
    """
    return f"inbox-{gateway_id}"


def advertising_address(gateway_id: str, session_key: str) -> str:
    """Canonical address that OTHER gateways should target to reach us.

    Returned as a ``<gateway_id>/<session_key>`` string. Per-sender
    subject prefixes are computed at publish time (each sender's
    gateway_id is in the subject prefix), so the right thing to
    advertise is the recipient's canonical address — not a single
    subject string. The presence entry persists this string verbatim;
    a sender's ``session_route_send`` parses it and emits
    ``from.<their-gw>.<our-sk>.deliver``.
    """
    return f"{gateway_id}/{session_key}"


async def update_presence(
    *,
    servers: List[str],
    gateway_id: str,
    agent_id: str,
    session_key: str,
    platform: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
    live_sessions: Optional[List[str]] = None,
    timeout: float = 3.0,
) -> None:
    """Write THIS gateway's presence row. Best-effort; logs on failure."""
    entry = build_presence_entry(
        agent_id=agent_id,
        gateway_id=gateway_id,
        session_key=session_key,
        platform=platform,
        extra=extra,
        live_sessions=live_sessions,
    )
    entry["inbox_consumer"] = inbox_consumer_name(gateway_id)
    entry["advertising_address"] = advertising_address(gateway_id, session_key)
    async with NATSRoutingClient(servers=servers) as client:
        try:
            await client.update_presence(gateway_id=gateway_id, presence_json=entry)
        except NATSRoutingUnreachable as e:
            logger.warning("session_routing: presence update failed: %s", e)


async def list_live(
    *,
    servers: List[str],
    ttl_seconds: int,
    now: Optional[float] = None,
    gateway_id_filter: Optional[str] = None,
    timeout: float = 3.0,
) -> List[Dict[str, Any]]:
    """Enumerate LIVE sessions, optionally filtered by gateway_id.

    Freshness is enforced here (against ``now``/``ttl_seconds``); the
    caller does NOT need to re-filter. Each entry is augmented with
    ``_gateway_id`` for downstream use.
    """
    now = now if now is not None else time.time()
    async with NATSRoutingClient(servers=servers) as client:
        entries = await client.list_presence()
    out: List[Dict[str, Any]] = []
    for e in entries:
        gid = e.get("_gateway_id")
        if not gid:
            continue
        if gateway_id_filter and gid != gateway_id_filter:
            continue
        if not is_fresh(e, ttl_seconds=ttl_seconds, now=now):
            continue
        out.append(e)
    return out


async def resolve_target(
    *,
    servers: List[str],
    address: str,
    ttl_seconds: int,
    now: Optional[float] = None,
    timeout: float = 3.0,
) -> Optional[Dict[str, Any]]:
    """Look up a single address; return its presence entry iff fresh.

    Returns None when the target is offline OR the gateway_id is unknown
    OR the entry has aged out — same shape of failure for the caller,
    which is exactly what we want for ``session_route_send`` to fail
    closed with a structured ``no_live_session_for_address`` error.
    """
    from plugins.session_bus.address import parse  # local import keeps
    # address.py importable without broker deps.

    gateway_id, session_key = parse(address)
    live = await list_live(
        servers=servers,
        ttl_seconds=ttl_seconds,
        now=now,
        gateway_id_filter=gateway_id,
    )
    for e in live:
        # v0.3.0: presence is one row per gateway carrying ALL live
        # session_keys; match against the list. Legacy single
        # ``session_key`` rows (pre-live_sessions writers) still match.
        if session_key in (e.get("live_sessions") or []):
            return e
        if e.get("session_key") == session_key:
            return e
    return None
