"""Tools package for session-bus.

Two submodules:

  - broadcast: bus_emit, bus_observe (typed event bus)
  - routing:   bus_handle, bus_route_send, bus_route_list, bus_establish
                (addressed point-to-point delivery)

Re-exports the model-tool handlers and schemas under the package
namespace so ``plugins.session_bus.tools.handle_bus_*`` resolves from
both submodules. Tests that patch ``plugins.session_bus.tools.*``
expect everything reachable from the package root.

Also re-exports ``NATSRoutingClient`` under its old name for tests
that patch the inner namespace. The canonical name remains in
``plugins.session_bus.nats_client.NATSRoutingClient``.
"""

# --- NATS client re-exports (test compatibility) ---
from plugins.session_bus.nats_client import NATSRoutingClient, SessionBusClient

# --- broadcast tools ---
from plugins.session_bus.tools.broadcast import (
    EMIT_SCHEMA,
    OBSERVE_SCHEMA,
    handle_emit,
    handle_observe,
)

# --- routing tools ---
from plugins.session_bus.tools.routing import (
    HANDLE_SCHEMA,
    ROUTE_SEND_SCHEMA,
    ROUTE_LIST_SCHEMA,
    ESTABLISH_SCHEMA,
    handle_bus_handle,
    handle_bus_route_send,
    handle_bus_route_list,
    handle_bus_establish,
)

# Back-compat aliases: the old session-routing tests referenced
# handle_session_* and SESSION_*_SCHEMA. Keep them working so the
# deprecation shim in plugins/session_routing/ keeps loading.
handle_session_handle = handle_bus_handle
handle_session_route_send = handle_bus_route_send
handle_session_routing_list = handle_bus_route_list
handle_session_establish = handle_bus_establish
SESSION_HANDLE_SCHEMA = HANDLE_SCHEMA
SESSION_ROUTE_SEND_SCHEMA = ROUTE_SEND_SCHEMA
SESSION_ROUTE_LIST_SCHEMA = ROUTE_LIST_SCHEMA
SESSION_ESTABLISH_SCHEMA = ESTABLISH_SCHEMA

__all__ = [
    "EMIT_SCHEMA", "OBSERVE_SCHEMA",
    "HANDLE_SCHEMA", "ROUTE_SEND_SCHEMA", "ROUTE_LIST_SCHEMA", "ESTABLISH_SCHEMA",
    "handle_emit", "handle_observe",
    "handle_bus_handle", "handle_bus_route_send",
    "handle_bus_route_list", "handle_bus_establish",
    "NATSRoutingClient", "SessionBusClient",
    # Back-compat aliases
    "handle_session_handle", "handle_session_route_send",
    "handle_session_routing_list", "handle_session_establish",
    "SESSION_HANDLE_SCHEMA", "SESSION_ROUTE_SEND_SCHEMA",
    "SESSION_ROUTE_LIST_SCHEMA", "SESSION_ESTABLISH_SCHEMA",
]
