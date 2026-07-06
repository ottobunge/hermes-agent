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

Phase 1 scope:
  - No auth (LAN + Courier VPN is the access boundary).
  - One JetStream stream (reused from session-bridge), two KV buckets
    (``session_presence``, ``session_allow``).
  - Heartbeat presence: NOT started by this plugin in v0.18.0 because
    the runtime does not expose an ``on_gateway_start`` hook. We
    document the gap below and SKIP the registration.

Versioning: v0.1.0 (Phase 1).
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
    """Register the three tools. No CLI surface yet."""

    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset="session_routing",
            schema=schema,
            handler=handler,
            check_fn=check_session_routing_requirements,
            emoji=emoji,
        )


# TODO(Phase 2): wire presence heartbeat via ctx.register_hook.
#
# The plugin manifest references ``on_gateway_start`` for the heartbeat
# loop (presence.update_presence on a fixed cadence) but as of v0.18.0
# the runtime's VALID_HOOKS set in hermes_cli/plugins.py does NOT
# include that hook. The closest existing surfaces are
# ``on_session_start`` / ``on_session_end``, which fire PER session —
# not what we want for a per-gateway heartbeat that runs whether or
# not any session is active.
#
# When the runtime exposes ``on_gateway_start`` (or the equivalent
# generic lifecycle hook), replace this stub with:
#
#     async def on_gateway_start(ctx) -> None:
#         servers = _broker_servers()
#         heartbeat_seconds = read_heartbeat_seconds(default=30)
#         # asyncio.create_task(presence_loop(servers, ...))  # noqa
#
# and add ``ctx.register_hook("on_gateway_start", on_gateway_start)``
# to ``register()``. Until then, presence is the responsibility of an
# out-of-band loop (e.g. a systemd timer or a separate process).
def _unused_phase2_hook_stub(ctx) -> None:  # pragma: no cover
    """Placeholder so the file stays importable while we wait for the hook."""
    raise NotImplementedError("on_gateway_start hook not available in v0.18.0")