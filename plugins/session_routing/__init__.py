"""session-routing plugin — addressed message routing between Hermes sessions.

Companion to ``session-bridge``: that plugin is broadcast / observe for
typed events; this one is point-to-point delivery addressed by
``<gateway_id>/<session_key>``. Both share the same NATS JetStream
stream (``SESSIONS``) so installing one does NOT double-quota the
operator's broker.

Three model tools:

  - ``session_handle``       — return THIS session's canonical address
                               so the agent can hand it to a peer.
  - ``session_route_send``   — publish a routed message to a peer.
                               Recipient's allow-list is broker-side
                               enforced (the subscriber only matches
                               subjects from trusted gateway_ids).
  - ``session_routing_list`` — enumerate live sessions, freshness-filtered.

v0.3.0 adds the full back-channel protocol:

  - ``session_establish``    — 3-way handshake opening a typed channel
                               to a peer session (tools.py).
  - Gateway lifecycle wiring — ``on_gateway_start`` starts the presence
                               heartbeat (one row per gateway with ALL
                               live session_keys) and the deferred-ack
                               ``InboxRunner`` feeding the receive-side
                               ``BackChannelDispatcher`` (runtime.py).
  - ``on_gateway_stop``      — stops both before the agent drain.
  - ``on_session_finalize``  — closes (handshake.bye) every open channel
                               owned by the finalized session.

Scope:
  - No auth (LAN + Courier VPN is the access boundary).
  - One JetStream stream (reused from session-bridge), three KV buckets
    (``session_presence``, ``session_allow``, ``session_channels``).

Versioning: v0.3.0.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from plugins.session_routing import tools as _tools

logger = logging.getLogger(__name__)


_TOOLS = (
    (
        "session_handle",
        _tools.SESSION_HANDLE_SCHEMA,
        _tools.handle_session_handle,
        "📍",
    ),
    (
        "session_route_send",
        _tools.SESSION_ROUTE_SEND_SCHEMA,
        _tools.handle_session_route_send,
        "✉️",
    ),
    (
        "session_routing_list",
        _tools.SESSION_ROUTING_LIST_SCHEMA,
        _tools.handle_session_routing_list,
        "📋",
    ),
    (
        "session_inbox_status",
        _tools.SESSION_INBOX_STATUS_SCHEMA,
        _tools.handle_session_inbox_status,
        "💓",
    ),
    (
        "session_establish",
        _tools.SESSION_ESTABLISH_SCHEMA,
        _tools.handle_session_establish,
        "🤝",
    ),
)


def _broker_servers() -> List[str]:
    """Resolve NATS server list. Mirrors session-bridge's precedence."""
    raw = (
        os.environ.get("HERMES_NATS_URLS")
        or os.environ.get("NATS_URLS")
        or "nats://127.0.0.1:4222"
    )
    return [s.strip() for s in raw.split(",") if s.strip()]


def check_session_routing_requirements() -> bool:
    """Service gate: tools only appear in the schema when NATS is reachable.

    Three gates mirror session-bridge:

      1. The optional ``nats-py`` dep is importable. Without it the
         plugin still loads (the plugin.yaml ships to users without
         nats-py installed), but the tools stay hidden so the model
         doesn't see a tool that would explode at call-time.
      2. ``HERMES_NATS_URLS`` or ``NATS_URLS`` is set, OR we fall back
         to the loopback default. Operators who run the broker on a
         different host MUST set the env var; the default is for
         single-host LAN deployments.
      3. ``NATSRoutingClient(...).ping(timeout=1.0)`` returns True.
         A real broker round-trip — gateway hides the tool when the
         broker is down so the model gets a stable "tool not
         available" instead of a noisy failure mid-turn.
    """
    try:
        import nats  # noqa: F401
    except ImportError:
        return False

    if not (
        os.environ.get("HERMES_NATS_URLS")
        or os.environ.get("NATS_URLS")
    ):
        return False

    try:
        import asyncio
        from plugins.session_routing.nats_client import NATSRoutingClient
        servers = _broker_servers()
        return asyncio.run(
            NATSRoutingClient(servers=servers).ping(timeout=1.0)
        )
    except Exception as e:  # noqa: BLE001 — true "any failure is offline"
        logger.debug("session_routing: NATS ping failed: %s", e)
        return False


def register(ctx) -> None:
    """Register the model tools + gateway lifecycle hooks."""

    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset="session_routing",
            schema=schema,
            handler=handler,
            check_fn=check_session_routing_requirements,
            emoji=emoji,
        )

    # Lifecycle wiring (v0.3.0): the gateway awaits awaitable returns
    # from on_gateway_start/stop; on_session_finalize schedules its own
    # cleanup task. All failures are contained in runtime.py — a downed
    # broker never blocks gateway startup/shutdown.
    from plugins.session_routing.runtime import runtime

    ctx.register_hook("on_gateway_start", runtime.on_gateway_start)
    ctx.register_hook("on_gateway_stop", runtime.on_gateway_stop)
    ctx.register_hook("on_session_finalize", runtime.on_session_finalize)