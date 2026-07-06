"""Tool schemas + sync tool handlers for the session-routing plugin.

Three tools are exposed to the model:

  - ``session_handle``        — returns THIS session's canonical address
                                (no broker call). Lets the agent say
                                "here's how you reach me" without any
                                network round trip.

  - ``session_route_send``    — delivers a typed payload to another
                                session by canonical address. Validates
                                the address, checks presence, builds
                                the envelope, and publishes to the
                                JetStream stream the recipient is
                                subscribed to. The recipient's allow-
                                list is broker-side enforced.

  - ``session_routing_list``  — enumerates live sessions whose
                                presence heartbeat is fresh. Used by
                                discovery flows ("who can I talk to?").

Per the project's tool conventions, the handlers are SYNC from the
model's perspective (Hermes tool dispatch is synchronous). Each opens
a fresh NATS client per call so a downed broker cannot wedge the
agent — ``asyncio.run`` drives the async work.

Error contract: handlers NEVER raise to the model. Every failure path
returns ``{"ok": false, "error": "...", ...}`` so the model can read
the structured cause and decide whether to retry.

Versioning: v0.1.0 (Phase 1).
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

from plugins.session_routing import address, presence, routing
from plugins.session_routing.allow import read_presence_ttl_seconds
from plugins.session_routing.nats_client import (
    NATSRoutingClient,
    NATSRoutingUnreachable,
    DEFAULT_PRESENCE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SESSION_HANDLE_SCHEMA: Dict[str, Any] = {
    "name": "session_handle",
    "description": (
        "Return THIS session's canonical routing address so another "
        "agent can deliver a message to it via session_route_send. "
        "Format: '<gateway_id>/<session_key>'. No broker call — this "
        "is a pure identity lookup that reads HERMES_SESSION_KEY and "
        "the resolved gateway_id."
    ),
    "parameters": {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    },
}


SESSION_ROUTE_SEND_SCHEMA: Dict[str, Any] = {
    "name": "session_route_send",
    "description": (
        "Deliver a typed message to another live Hermes session. "
        "`target` is the recipient's canonical address "
        "('<gateway_id>/<session_key>') — obtained previously from "
        "session_handle or session_routing_list. `content` is a "
        "JSON-serializable dict (the message body). `reply_to` is an "
        "optional caller-chosen token the recipient can echo back so "
        "the agent can correlate reply streams. Returns "
        "{ok: true, seq, stream: 'SESSIONS'} on success or a "
        "structured error (broker_unreachable, bad_address, "
        "no_live_session_for_address) on failure."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "target": {
                "type": "string",
                "description": (
                    "Recipient canonical address "
                    "('<gateway_id>/<session_key>'). Must parse via "
                    "the canonical address grammar."
                ),
            },
            "content": {
                "type": "object",
                "description": (
                    "JSON-serializable dict payload. Phase 1 does "
                    "NOT accept raw strings — payloads are typed "
                    "dicts so the recipient can schema-validate."
                ),
            },
            "reply_to": {
                "type": "string",
                "description": (
                    "Optional correlation token echoed in the "
                    "recipient's reply stream."
                ),
            },
        },
        "required": ["target", "content"],
        "additionalProperties": False,
    },
}


SESSION_ROUTING_LIST_SCHEMA: Dict[str, Any] = {
    "name": "session_routing_list",
    "description": (
        "Enumerate live Hermes sessions known to the broker, "
        "optionally filtered by gateway_id. Each entry includes the "
        "session_key, platform, and last_seen timestamp. Entries "
        "with stale heartbeats (older than the configured "
        "presence_ttl_seconds) are excluded. Returns "
        "{ok: true, live: [...]} on success."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "gateway_id_filter": {
                "type": "string",
                "description": (
                    "If set, only entries from this gateway_id are "
                    "returned. Omit for the full live set."
                ),
            },
        },
        "required": [],
        "additionalProperties": False,
    },
}


# ---------------------------------------------------------------------------
# Broker resolution
# ---------------------------------------------------------------------------

def _broker_servers() -> List[str]:
    """Resolve the NATS server list the same way session-bridge does.

    Precedence: ``HERMES_NATS_URLS`` → ``NATS_URLS`` → loopback default.
    Returning a list so we hand it straight to the async client.
    """
    raw = (
        os.environ.get("HERMES_NATS_URLS")
        or os.environ.get("NATS_URLS")
        or "nats://127.0.0.1:4222"
    )
    return [s.strip() for s in raw.split(",") if s.strip()]


def _presence_ttl() -> int:
    """Read presence TTL from config; default ``DEFAULT_PRESENCE_TTL_SECONDS``."""
    try:
        return read_presence_ttl_seconds(default=DEFAULT_PRESENCE_TTL_SECONDS)
    except Exception as e:  # noqa: BLE001
        logger.debug("session_routing: ttl read failed (%s) — using default", e)
        return DEFAULT_PRESENCE_TTL_SECONDS


def _run_async(coro):
    """Run a coroutine in either sync or async context.

    Mirrors session_bridge's helper. The model's tool dispatch is
    synchronous and not currently running inside a loop, so the
    ``asyncio.run`` branch is what fires from a real model call. The
    in-loop branch raises so a misuse is loud rather than silently
    spawning a nested event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError(
        "session_routing tools are sync; from async callers wrap with "
        "asyncio.run(handler(...))."
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def handle_session_handle(**_kwargs: Any) -> Dict[str, Any]:
    """Return the calling session's canonical address.

    No broker call. Reads ``HERMES_SESSION_KEY`` and resolves the
    gateway_id via ``address.resolve_gateway_id``.
    """
    session_key = address.resolve_session_key()
    if not session_key:
        return {"ok": False, "error": "no_session"}

    try:
        gateway_id = address.resolve_gateway_id()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": "no_gateway_id", "detail": str(e)}

    try:
        full_address = address.build(gateway_id, session_key)
    except address.AddressError as e:
        return {"ok": False, "error": "bad_address", "detail": str(e)}

    return {
        "ok": True,
        "address": full_address,
        "gateway_id": gateway_id,
        "session_key": session_key,
    }


def handle_session_route_send(
    target: str,
    content: Dict[str, Any],
    reply_to: Optional[str] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Validate, resolve, build, and publish a routed message.

    Steps:
      1. ``address.parse(target)`` — refuse malformed addresses.
      2. ``presence.resolve_target`` — refuse sends to offline sessions.
      3. ``routing.build_envelope`` — canonical envelope shape.
      4. ``NATSRoutingClient.publish_routed`` — JetStream publish.

    The publish call includes a ``reply_to``-derived correlation
    token in headers (so the recipient can route a typed reply
    without parsing the body). The full content lives in the
    envelope payload.
    """

    if not isinstance(content, dict):
        return {
            "ok": False,
            "error": "bad_payload",
            "detail": f"content must be dict, got {type(content).__name__}",
        }

    try:
        address.parse(target)
    except address.AddressError as e:
        return {"ok": False, "error": "bad_address", "detail": str(e)}

    servers = _broker_servers()
    ttl_seconds = _presence_ttl()

    async def _send() -> Dict[str, Any]:
        try:
            entry = await presence.resolve_target(
                servers=servers,
                address=target,
                ttl_seconds=ttl_seconds,
            )
        except NATSRoutingUnreachable as e:
            return {"ok": False, "error": "broker_unreachable", "detail": str(e)}

        if entry is None:
            return {"ok": False, "error": "no_live_session_for_address"}

        try:
            sender = address.my_address()
        except address.AddressError as e:
            return {"ok": False, "error": "no_session", "detail": str(e)}

        envelope = routing.build_envelope(
            from_address=sender,
            to_address=target,
            payload=content,
        )

        headers: Dict[str, str] = routing.envelope_to_headers(envelope)
        if reply_to:
            headers["reply_to"] = reply_to

        # Subject shape is ``from.<sender_gw>.<recipient_session_key>.deliver``.
        # The recipient's inbox filter is ``from.<allowed_sender>.>``, so the
        # SENDER's gateway_id must be in the prefix (here ``sender`` is the
        # full canonical address of the calling session; we pick the part
        # before the slash as the sender_gw).
        sender_gw, _recipient_sk = address.parse(sender)
        _target_gw, recipient_sk = address.parse(target)
        subject = address.encode_subject(sender_gw, recipient_sk, verb="deliver")

        async with NATSRoutingClient(servers=servers) as client:
            ack = await client.publish_routed(
                subject=subject,
                payload=envelope,
                headers=headers,
            )
        return {"ok": True, **ack}

    try:
        return _run_async(_send())
    except NATSRoutingUnreachable as e:
        logger.warning("session_route_send: broker unreachable: %s", e)
        return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("session_route_send failed")
        return {"ok": False, "error": "send_failed", "detail": repr(e)}


def handle_session_routing_list(
    gateway_id_filter: Optional[str] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Enumerate live sessions, freshness-filtered.

    ``gateway_id_filter`` narrows the result to one gateway. Returns
    the same shape as ``presence.list_live`` (each entry is a
    presence JSON dict augmented with ``_gateway_id``).
    """
    servers = _broker_servers()
    ttl_seconds = _presence_ttl()

    async def _list() -> List[Dict[str, Any]]:
        return await presence.list_live(
            servers=servers,
            ttl_seconds=ttl_seconds,
            gateway_id_filter=gateway_id_filter,
        )

    try:
        live = _run_async(_list())
    except NATSRoutingUnreachable as e:
        logger.warning("session_routing_list: broker unreachable: %s", e)
        return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("session_routing_list failed")
        return {"ok": False, "error": "list_failed", "detail": repr(e)}

    return {"ok": True, "live": live}