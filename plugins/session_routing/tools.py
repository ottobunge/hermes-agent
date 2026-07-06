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


SESSION_ESTABLISH_SCHEMA: Dict[str, Any] = {
    "name": "session_establish",
    "description": (
        "Open (or reuse) a typed back-channel to another live Hermes "
        "session via a 3-way handshake. `target` is the peer's "
        "canonical address ('<gateway_id>/<session_key>', from "
        "session_routing_list or handed over by the user). Blocks up "
        "to `timeout_seconds` waiting for the peer's ack. Returns "
        "{ok: true, channel_id, peer_address, established_at, "
        "peer_capabilities} on success — after which "
        "session_route_send delivers visible messages the peer's user "
        "sees in their chat. Idempotent: re-establishing to the same "
        "target returns the existing channel. On failure returns "
        "{ok: false, error: rejected|timeout|broker_unreachable|"
        "bad_address|no_live_session_for_address|no_session}. If "
        "`initial_message` is given it is sent as the first "
        "message.text right after the handshake completes."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "target": {
                "type": "string",
                "description": (
                    "Peer canonical address ('<gateway_id>/<session_key>')."
                ),
            },
            "initial_message": {
                "type": "string",
                "description": (
                    "Optional first message, delivered as message.text "
                    "immediately after the channel is established."
                ),
            },
            "capabilities": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Capabilities to advertise (default: text, "
                    "ack_delivery)."
                ),
            },
            "timeout_seconds": {
                "type": "number",
                "description": (
                    "How long to wait for the peer's handshake ack "
                    "(default 30)."
                ),
            },
        },
        "required": ["target"],
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


ESTABLISH_POLL_INTERVAL_SECONDS = 0.5


def _resolve_local_session_id(session_key: str) -> Optional[str]:
    """This session's session_id (for channel_id construction).

    In-gateway: resolved through the plugin runtime's session store
    (task-safe). CLI/tests: ``HERMES_SESSION_ID`` env fallback.
    """
    if session_key:
        try:
            from plugins.session_routing.runtime import runtime
            gw = runtime._gateway
            if gw is not None:
                entry = gw.session_store.get_entry(session_key)
                if entry is not None and getattr(entry, "session_id", ""):
                    return entry.session_id
        except Exception as e:  # noqa: BLE001
            logger.debug("session_establish: store lookup failed: %s", e)
    return os.environ.get("HERMES_SESSION_ID", "").strip() or None


def handle_session_establish(
    target: str,
    initial_message: Optional[str] = None,
    capabilities: Optional[List[str]] = None,
    timeout_seconds: float = 30.0,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Open (or reuse) a back-channel to ``target`` via the 3-way handshake.

    The tool publishes ``handshake.request`` and then POLLS the
    ``session_channels`` KV: the receive-side dispatcher (inbox runner)
    is what processes the peer's ``handshake.ack`` and transitions the
    channel to ESTABLISHED — this handler never consumes from the inbox
    itself, so it can't race the durable consumer's cursor.
    """
    from plugins.session_routing import channels as _channels
    from plugins.session_routing import handshake as _handshake
    from plugins.session_routing import protocol as _protocol

    try:
        address.parse(target)
    except address.AddressError as e:
        return {"ok": False, "error": "bad_address", "detail": str(e)}

    try:
        my_address = address.my_address()
    except address.AddressError as e:
        return {"ok": False, "error": "no_session", "detail": str(e)}
    if target.strip() == my_address:
        return {
            "ok": False,
            "error": "bad_address",
            "detail": "cannot establish a back-channel with this session itself",
        }

    session_key = address.resolve_session_key()
    local_session_id = _resolve_local_session_id(session_key)
    if not local_session_id:
        return {
            "ok": False,
            "error": "no_session",
            "detail": "could not determine this session's session_id",
        }

    servers = _broker_servers()
    ttl_seconds = _presence_ttl()
    target_clean = target.strip()
    channel_id = _channels.channel_id_for(local_session_id, target_clean)

    async def _establish() -> Dict[str, Any]:
        try:
            entry = await presence.resolve_target(
                servers=servers, address=target_clean, ttl_seconds=ttl_seconds,
            )
        except NATSRoutingUnreachable as e:
            return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
        if entry is None:
            return {"ok": False, "error": "no_live_session_for_address"}

        kv_key = _channels.channel_id_to_kv_key(channel_id)
        sender_gw, _ = address.parse(my_address)
        _, peer_sk = address.parse(target_clean)

        async with NATSRoutingClient(servers=servers) as client:
            record = await client.read_channel(kv_key)
            if record and record.get("state") == _channels.ChannelState.ESTABLISHED.value:
                # Idempotent: same target + same session → existing channel.
                result = _success_result(record)
                if initial_message:
                    result["initial_message_msg_id"] = await _send_text(
                        client, sender_gw, peer_sk, initial_message
                    )
                return result

            # Fresh handshake (also for CLOSED/REJECTED/stale-INITIATING
            # records: a new nonce supersedes; the responder re-acks).
            machine = _handshake.HandshakeChannel(
                channel_id=channel_id,
                session_id=local_session_id,
                my_address=my_address,
                peer_address=target_clean,
                timeout_seconds=timeout_seconds,
            )
            request = machine.start(capabilities=capabilities)
            record = _channels.build_channel_record(
                channel_id=channel_id,
                session_id=local_session_id,
                peer_address=target_clean,
                state=_channels.ChannelState.INITIATING,
            )
            record["role"] = "initiator"
            record["nonce"] = request["nonce"]
            # KV row BEFORE publish: the dispatcher validates the ack's
            # nonce against this record.
            await client.write_channel(kv_key=kv_key, record=record)

            envelope = routing.build_envelope(
                from_address=my_address,
                to_address=target_clean,
                payload=request,
            )
            await client.publish_routed(
                subject=address.handshake_subject(sender_gw, peer_sk),
                payload=envelope,
                headers=routing.envelope_to_headers(envelope),
            )

        # Poll KV until the dispatcher lands the terminal state.
        deadline = asyncio.get_event_loop().time() + max(timeout_seconds, 0.1)
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(ESTABLISH_POLL_INTERVAL_SECONDS)
            record = await _channels.load_channel(
                servers=servers, channel_id=channel_id
            )
            state = (record or {}).get("state")
            if state == _channels.ChannelState.ESTABLISHED.value:
                result = _success_result(record)
                if initial_message:
                    async with NATSRoutingClient(servers=servers) as client:
                        result["initial_message_msg_id"] = await _send_text(
                            client, sender_gw, peer_sk, initial_message
                        )
                return result
            if state == _channels.ChannelState.REJECTED.value:
                return {
                    "ok": False,
                    "error": "rejected",
                    "reason": record.get("reject_reason"),
                }
        return {"ok": False, "error": "timeout", "timeout_seconds": timeout_seconds}

    def _success_result(record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": True,
            "channel_id": channel_id,
            "peer_address": target_clean,
            "established_at": record.get("opened_at"),
            "peer_capabilities": list(record.get("capabilities") or []),
        }

    async def _send_text(
        client: "NATSRoutingClient",
        sender_gw: str,
        peer_sk: str,
        body: str,
    ) -> Optional[str]:
        payload = _protocol.build_message_text(
            channel_id=channel_id,
            session_id=local_session_id,
            body=body,
        )
        envelope = routing.build_envelope(
            from_address=my_address, to_address=target_clean, payload=payload,
        )
        try:
            await client.publish_routed(
                subject=address.encode_subject(sender_gw, peer_sk, verb="deliver"),
                payload=envelope,
                headers=routing.envelope_to_headers(envelope),
            )
            return envelope["msg_id"]
        except NATSRoutingUnreachable as e:
            logger.warning(
                "session_establish: initial_message publish failed: %s", e
            )
            return None

    try:
        return _run_async(_establish())
    except NATSRoutingUnreachable as e:
        logger.warning("session_establish: broker unreachable: %s", e)
        return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("session_establish failed")
        return {"ok": False, "error": "establish_failed", "detail": repr(e)}


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