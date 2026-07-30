"""session-bus — single NATS-based inter-agent bus for Hermes Agent.

Replaces session-bridge (broadcast / observe) and session-routing
(addressed point-to-point) with one unified plugin exposing two
toolsets:

  session_bus_broadcast (bus_emit, bus_observe)
    Typed event bus. Per-call ephemeral connections. Subject prefix is
    `from.<HERMES_AGENT_ID>.`.

  session_bus_routing (bus_handle, bus_route_send, bus_route_list,
                       bus_establish)
    Addressed point-to-point delivery with handshake. Persistent inbox
    subscriber + KV presence + allow-list. Subjects follow the
    `<gateway_id>/<session_key>` addressing scheme.

Both toolsets share a single NATS JetStream connection and a single
stream ("SESSIONS"). Installing the plugin does NOT double the broker
quota.

Gateway lifecycle wiring (on_gateway_start, on_gateway_stop,
on_session_finalize) is provided by runtime.py — the inbox subscriber
and presence heartbeat are spawned on gateway start, drained on stop.
"""

import logging

from plugins.session_bus.tools import broadcast as _broadcast
from plugins.session_bus.tools import routing as _routing

logger = logging.getLogger("session_bus")


# Test introspection: register() appends (name, toolset, schema, handler)
# tuples here so tests can assert what the plugin exposes without mocking
# the entire register() flow. This is the same convention the legacy
# session-bridge and session-routing plugins used.
_TOOLS: list = []


def register(ctx) -> None:
    """Register model tools + gateway lifecycle hooks.

    Called once per process by the plugin loader. Tools auto-hide when
    the broker is unreachable (handled by their own check_fn).

    Note: ``runtime`` is imported lazily inside this function because
    loading it at module-import time would trigger the long-lived inbox
    runner and presence heartbeat to start before the gateway has wired
    up its platform adapters.

    The import pulls the ``runtime`` *instance* (module-level singleton
    bound at runtime.py:505) so the hook functions are bound methods,
    not module-level functions. This matches the legacy session-routing
    plugin's pattern.
    """
    from plugins.session_bus.runtime import runtime

    # Broadcast toolset — per-tool ephemeral connection, no shared state.
    _register(ctx, "bus_emit", "session_bus_broadcast",
              _broadcast.EMIT_SCHEMA, _broadcast.handle_emit)
    _register(ctx, "bus_observe", "session_bus_broadcast",
              _broadcast.OBSERVE_SCHEMA, _broadcast.handle_observe)

    # Routing toolset — persistent inbox, KV presence, allow-list.
    _register(ctx, "bus_handle", "session_bus_routing",
              _routing.HANDLE_SCHEMA, _routing.handle_bus_handle)
    _register(ctx, "bus_route_send", "session_bus_routing",
              _routing.ROUTE_SEND_SCHEMA, _routing.handle_bus_route_send)
    _register(ctx, "bus_route_list", "session_bus_routing",
              _routing.ROUTE_LIST_SCHEMA, _routing.handle_bus_route_list)
    _register(ctx, "bus_establish", "session_bus_routing",
              _routing.ESTABLISH_SCHEMA, _routing.handle_bus_establish)

    # Lifecycle hooks — start the inbox + presence on gateway boot, stop
    # them on shutdown. on_session_finalize closes any open channels owned
    # by the finalized session.
    ctx.register_hook("on_gateway_start", runtime.on_gateway_start)
    ctx.register_hook("on_gateway_stop", runtime.on_gateway_stop)
    ctx.register_hook("on_session_finalize", runtime.on_session_finalize)

    logger.info("session-bus registered: %d tools + 3 hooks", len(_TOOLS))


def _register(ctx, name, toolset, schema, handler) -> None:
    """Register a single tool and record it for test introspection."""
    ctx.register_tool(name=name, toolset=toolset, schema=schema, handler=handler)
    _TOOLS.append((name, toolset, schema, handler))
