"""Receive-side dispatcher for session-routing back-channels (v0.3.0).

Wired as the ``InboxRunner.on_message`` callback (deferred-ack mode).
Routes inbound envelopes by ``payload.type`` (protocol.py registry):

    handshake.*          → channel state machine transitions in the
                           ``session_channels`` KV (responder replies
                           handshake.ack / handshake.established)
    message.text         → synthetic user turn into the addressed
                           session via the gateway's
                           ``enqueue_internal_session_event`` helper
                           (adapter FIFO — NEVER direct
                           ``_handle_message`` invocation), then a
                           message.ack_delivery back to the sender
    message.error        → structured log only (peer-to-peer protocol
                           error; never injected as a user turn)
    message.ack_delivery → debug log only
    delegate.task/.result→ message.error{unsupported_type} (Phase 1)
    unknown type         → message.error{unknown_type} (loop-guarded)

Ack ordering (bug fix #1): the InboxRunner only acks the broker message
after ``handle()`` returns without raising. So the contract here is:

    validate → dedupe → update KV (record msg_id) → enqueue → return

* Return normally  == "durably handled or permanently skippable" → ack.
* Raise            == "transient failure, want redelivery" → no ack.

Dedupe by envelope ``msg_id`` against the channel's persisted
``recent_msg_ids`` window happens BEFORE the ack (and BEFORE enqueue),
so JetStream redelivery of the same envelope injects exactly one
synthetic turn even across gateway restarts.

Loop guard: a ``message.error`` inbound is never answered with another
``message.error``, and outbound error responses are deduped by the bad
envelope's msg_id — two incompatible versions cannot bounce errors
forever.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, List, Optional

from plugins.session_routing import address as _address
from plugins.session_routing import channels as _channels
from plugins.session_routing import handshake as _handshake
from plugins.session_routing import protocol as _protocol
from plugins.session_routing import routing as _routing
from plugins.session_routing.channels import ChannelState
from plugins.session_routing.nats_client import (
    NATSRoutingClient,
    NATSRoutingUnreachable,
)

logger = logging.getLogger(__name__)

BACK_CHANNEL_PREFIX = "[back-channel from {peer}] "

# In-memory fallback dedupe for error responses when no channel record
# can be created (e.g. the bad envelope addresses an unknown session).
_ERROR_REPLY_MEMORY_CAP = 500


class BackChannelDispatcher:
    """Routes one inbound envelope; owned by the plugin runtime.

    Collaborators are injected as narrow callables so unit tests don't
    need a GatewayRunner:

      resolve_session_id(session_key) -> Optional[str]
          Local session_key → session_id (None = session unknown here).
      enqueue_event(session_key, event) -> bool
          The gateway's ``enqueue_internal_session_event`` seam.
      publish_notification(session_key, text, kind) -> awaitable | None
          The gateway's ``publish_internal_notification`` seam: a
          USER-visible system notification on the session's platform
          (never a session turn). Optional — None means the platform
          side-channel is off (bare/legacy wiring) and dispatch runs
          exactly as before. Failures are swallowed: visibility must
          never affect envelope handling.
    """

    def __init__(
        self,
        *,
        servers: List[str],
        my_gateway_id: str,
        resolve_session_id: Callable[[str], Optional[str]],
        enqueue_event: Callable[[str, Any], bool],
        publish_notification: Optional[Callable[[str, str, str], Any]] = None,
        capabilities: Optional[List[str]] = None,
    ) -> None:
        self._servers = servers
        self._my_gateway_id = my_gateway_id
        self._resolve_session_id = resolve_session_id
        self._enqueue_event = enqueue_event
        self._publish_notification = publish_notification
        self._capabilities = list(capabilities or _handshake.DEFAULT_CAPABILITIES)
        self._error_replied_msg_ids: List[str] = []

    # ------------------------------------------------------------------
    # Entry point (InboxRunner callback)
    # ------------------------------------------------------------------

    async def handle(
        self,
        envelope: Dict[str, Any],
        subject: str,
        headers: Dict[str, str],
    ) -> None:
        """Process one envelope. Raise = redeliver; return = ack."""
        try:
            _routing.validate_envelope(envelope)
        except ValueError as e:
            # Malformed envelope: no msg_id/from to reply to reliably —
            # log loudly (NOT a silent drop) and ack so it can't wedge
            # the consumer. It will never become valid on redelivery.
            logger.warning(
                "session_routing dispatcher: malformed envelope on %s "
                "dropped: %s", subject, e,
            )
            return

        msg_id = envelope["msg_id"]
        peer_address = envelope["from"]
        to_address = envelope["to"]

        try:
            to_gw, to_session_key = _address.parse(to_address)
        except _address.AddressError as e:
            logger.warning(
                "session_routing dispatcher: unparseable to-address %r "
                "(msg_id=%s) dropped: %s", to_address, msg_id, e,
            )
            return
        if to_gw != self._my_gateway_id:
            # The subject filter is sender-scoped, so every allowed
            # gateway's consumer sees this copy. Not addressed to us —
            # ack and move on (the owning gateway processes its copy).
            logger.debug(
                "session_routing dispatcher: envelope %s addressed to %s "
                "(not %s) — skipping", msg_id, to_gw, self._my_gateway_id,
            )
            return

        payload = envelope.get("payload") or {}
        ok, error_code, detail = _protocol.validate_payload(payload)
        if not ok:
            await self._reply_error_loop_guarded(
                envelope=envelope,
                to_session_key=to_session_key,
                peer_address=peer_address,
                error_code=error_code or _protocol.ERROR_CODE_VALIDATION,
                detail=detail,
            )
            return

        local_session_id = self._resolve_session_id(to_session_key)
        if not local_session_id:
            # Session not live on this gateway. KV presence said
            # otherwise at send time, or the session ended in flight.
            # Log + ack: redelivery cannot resurrect the session.
            logger.info(
                "session_routing dispatcher: no live session %r for "
                "envelope %s (%s) — dropping",
                to_session_key, msg_id, payload.get("type"),
            )
            return

        # Normalise cross-cutting payload fields that v0.2 senders may
        # have omitted. We resolve channel_id + session_id from
        # envelope context (the same source of truth the build helpers
        # use) so downstream code can rely on them being present.
        channel_id = _channels.channel_id_for(local_session_id, peer_address)
        payload.setdefault("channel_id", channel_id)
        payload.setdefault("session_id", local_session_id)
        payload.setdefault("protocol_version", _protocol.PROTOCOL_VERSION)

        kv_key = _channels.channel_id_to_kv_key(channel_id)

        async with NATSRoutingClient(servers=self._servers) as client:
            record = await client.read_channel(kv_key)

            # Dedupe BEFORE any state change / enqueue / ack.
            if _channels.has_seen_msg_id(record, msg_id):
                logger.debug(
                    "session_routing dispatcher: duplicate envelope %s on "
                    "%s — already processed, skipping", msg_id, channel_id,
                )
                return

            ptype = payload["type"]
            if ptype in _protocol.HANDSHAKE_TYPES:
                await self._handle_handshake(
                    client=client,
                    envelope=envelope,
                    payload=payload,
                    record=record,
                    kv_key=kv_key,
                    channel_id=channel_id,
                    local_session_id=local_session_id,
                    to_session_key=to_session_key,
                    peer_address=peer_address,
                )
            elif ptype == "message.text":
                await self._handle_message_text(
                    client=client,
                    envelope=envelope,
                    payload=payload,
                    record=record,
                    kv_key=kv_key,
                    channel_id=channel_id,
                    local_session_id=local_session_id,
                    to_session_key=to_session_key,
                    peer_address=peer_address,
                )
            elif ptype == "message.error":
                # Peer-to-peer protocol error: structured log ONLY.
                # Never injected as a user turn, never answered with
                # another message.error (loop guard).
                logger.warning(
                    "session_routing dispatcher: message.error from %s on "
                    "%s: error_code=%s detail=%s in_reply_to=%s",
                    peer_address, channel_id,
                    payload.get("error_code"), payload.get("detail"),
                    payload.get("in_reply_to"),
                )
                await self._record_seen(client, kv_key, record, envelope,
                                        channel_id, local_session_id,
                                        peer_address)
            elif ptype == "message.ack_delivery":
                logger.debug(
                    "session_routing dispatcher: delivery ack from %s for %s",
                    peer_address, payload.get("in_reply_to"),
                )
                await self._record_seen(client, kv_key, record, envelope,
                                        channel_id, local_session_id,
                                        peer_address)
            elif ptype in _protocol.UNSUPPORTED_TYPES:
                await self._record_seen(client, kv_key, record, envelope,
                                        channel_id, local_session_id,
                                        peer_address)
                await self._publish_payload(
                    client=client,
                    to_address=peer_address,
                    from_session_key=to_session_key,
                    payload=_protocol.build_message_error(
                        channel_id=channel_id,
                        session_id=local_session_id,
                        error_code=_protocol.ERROR_CODE_UNSUPPORTED_TYPE,
                        in_reply_to=msg_id,
                        detail=f"{ptype} not yet supported in Phase 1",
                    ),
                )
            else:  # registered but unhandled — should be unreachable
                logger.error(
                    "session_routing dispatcher: registered type %r has no "
                    "route (msg_id=%s)", ptype, msg_id,
                )

    # ------------------------------------------------------------------
    # handshake.*
    # ------------------------------------------------------------------

    async def _handle_handshake(
        self,
        *,
        client: NATSRoutingClient,
        envelope: Dict[str, Any],
        payload: Dict[str, Any],
        record: Optional[Dict[str, Any]],
        kv_key: str,
        channel_id: str,
        local_session_id: str,
        to_session_key: str,
        peer_address: str,
    ) -> None:
        ptype = payload["type"]
        msg_id = envelope["msg_id"]
        my_address = _address.build(self._my_gateway_id, to_session_key)

        if ptype == "handshake.request":
            if record is not None and record.get("state") == ChannelState.INITIATING.value:
                if record.get("role") == "initiator":
                    # Simultaneous requests: deterministic winner.
                    if _handshake.resolve_race(my_address, peer_address) == "mine":
                        # Our request wins; peer will answer it. Ignore
                        # theirs (do NOT record msg_id: a retry after
                        # the race settles must still be answerable).
                        logger.info(
                            "session_routing dispatcher: handshake race on "
                            "%s — our request wins, ignoring peer's",
                            channel_id,
                        )
                        return
                    # Theirs wins — fall through and adopt responder role.
            if record is None or record.get("state") in (
                ChannelState.CLOSED.value,
                ChannelState.REJECTED.value,
                ChannelState.INITIATING.value,
            ):
                record = _channels.build_channel_record(
                    channel_id=channel_id,
                    session_id=local_session_id,
                    peer_address=peer_address,
                    state=ChannelState.INITIATING,
                    capabilities=list(payload.get("capabilities") or []),
                )
                record["role"] = "responder"
            # ESTABLISHED + re-request: keep state, re-ack (idempotent
            # for a peer that lost our previous ack).
            record["nonce"] = payload.get("nonce")
            record["capabilities"] = list(payload.get("capabilities") or [])
            _channels.record_msg_id(record, msg_id)
            await client.write_channel(kv_key=kv_key, record=record)
            await self._publish_payload(
                client=client,
                to_address=peer_address,
                from_session_key=to_session_key,
                payload=_handshake.build_ack(
                    channel_id=channel_id,
                    session_id=local_session_id,
                    nonce=str(payload.get("nonce")),
                    in_reply_to=msg_id,
                    capabilities=self._capabilities,
                ),
                verb=_address.HANDSHAKE_VERB,
            )
            return

        if ptype == "handshake.ack":
            if record is None or record.get("state") != ChannelState.INITIATING.value:
                logger.info(
                    "session_routing dispatcher: unexpected handshake.ack on "
                    "%s (state=%s) — ignoring",
                    channel_id, record.get("state") if record else None,
                )
                return
            if payload.get("nonce") != record.get("nonce"):
                logger.warning(
                    "session_routing dispatcher: handshake.ack nonce mismatch "
                    "on %s — ignoring (possible stale/forged ack)", channel_id,
                )
                return
            record["capabilities"] = list(payload.get("capabilities") or [])
            _channels.apply_transition(record, ChannelState.ESTABLISHED)
            _channels.record_msg_id(record, msg_id)
            await client.write_channel(kv_key=kv_key, record=record)
            await self._publish_payload(
                client=client,
                to_address=peer_address,
                from_session_key=to_session_key,
                payload=_handshake.build_established(
                    channel_id=channel_id,
                    session_id=local_session_id,
                    nonce=str(record.get("nonce")),
                    in_reply_to=msg_id,
                ),
                verb=_address.HANDSHAKE_VERB,
            )
            await self._notify(
                to_session_key,
                f"✅ back-channel established with {peer_address}",
                "lifecycle",
            )
            return

        if ptype == "handshake.established":
            if record is None:
                logger.info(
                    "session_routing dispatcher: handshake.established for "
                    "unknown channel %s — ignoring", channel_id,
                )
                return
            if payload.get("nonce") != record.get("nonce"):
                logger.warning(
                    "session_routing dispatcher: handshake.established nonce "
                    "mismatch on %s — ignoring", channel_id,
                )
                return
            transitioned = False
            if record.get("state") == ChannelState.INITIATING.value:
                _channels.apply_transition(record, ChannelState.ESTABLISHED)
                transitioned = True
            _channels.record_msg_id(record, msg_id)
            await client.write_channel(kv_key=kv_key, record=record)
            if transitioned:  # idempotent re-confirms stay silent
                await self._notify(
                    to_session_key,
                    f"✅ back-channel established with {peer_address}",
                    "lifecycle",
                )
            return

        if ptype == "handshake.reject":
            if record is not None and record.get("state") == ChannelState.INITIATING.value:
                _channels.apply_transition(record, ChannelState.REJECTED)
                record["reject_reason"] = payload.get("reason")
                _channels.record_msg_id(record, msg_id)
                await client.write_channel(kv_key=kv_key, record=record)
            return

        if ptype == "handshake.bye":
            if record is not None and record.get("state") in (
                ChannelState.INITIATING.value,
                ChannelState.ESTABLISHED.value,
            ):
                _channels.apply_transition(record, ChannelState.CLOSED)
                _channels.record_msg_id(record, msg_id)
                await client.write_channel(kv_key=kv_key, record=record)
                await self._notify(
                    to_session_key,
                    f"❌ back-channel closed with {peer_address}",
                    "lifecycle",
                )
            return

    # ------------------------------------------------------------------
    # message.text
    # ------------------------------------------------------------------

    async def _handle_message_text(
        self,
        *,
        client: NATSRoutingClient,
        envelope: Dict[str, Any],
        payload: Dict[str, Any],
        record: Optional[Dict[str, Any]],
        kv_key: str,
        channel_id: str,
        local_session_id: str,
        to_session_key: str,
        peer_address: str,
    ) -> None:
        msg_id = envelope["msg_id"]

        if record is None:
            # No channel: tolerate (v0.2.0 senders / direct sends) by
            # opening an implicit ESTABLISHED record so dedupe has a
            # home. Strictness here would break session_route_send
            # compatibility.
            record = _channels.build_channel_record(
                channel_id=channel_id,
                session_id=local_session_id,
                peer_address=peer_address,
                state=ChannelState.ESTABLISHED,
            )
            record["role"] = "implicit"

        # Update dedupe window in KV BEFORE enqueue: if the enqueue
        # succeeds but the ack is lost, redelivery hits the window and
        # the session sees exactly one synthetic turn.
        _channels.record_msg_id(record, msg_id)
        await client.write_channel(kv_key=kv_key, record=record)

        body = str(payload.get("body") or "")
        event = self._build_synthetic_event(
            text=BACK_CHANNEL_PREFIX.format(peer=peer_address) + body,
            metadata={
                "back_channel": True,
                "peer_address": peer_address,
                "channel_id": channel_id,
                "envelope_msg_id": msg_id,
            },
        )
        enqueued = self._enqueue_event(to_session_key, event)
        if not enqueued:
            # KV already recorded the msg_id; raising would redeliver
            # into the same dedupe wall. Log loudly instead. No user
            # notification either: "received" would be a lie when the
            # turn was never injected.
            logger.warning(
                "session_routing dispatcher: enqueue failed for %s "
                "(msg_id=%s) — message recorded but not injected",
                to_session_key, msg_id,
            )
            return

        # User-visible mirror of the raw message (side-channel, not a
        # session turn) — the agent's reply is a separate artifact.
        await self._notify(
            to_session_key,
            f"🔄 back-channel from {peer_address}\n{body}",
            "back_channel_in",
        )

        # Delivery confirmation for sender-side correlation.
        await self._publish_payload(
            client=client,
            to_address=peer_address,
            from_session_key=to_session_key,
            payload=_protocol.build_ack_delivery(
                channel_id=channel_id,
                session_id=local_session_id,
                in_reply_to=msg_id,
            ),
        )

    def _build_synthetic_event(self, *, text: str, metadata: Dict[str, Any]):
        """Synthetic user-turn event. ``source`` stays None — the
        gateway helper fills it from the session's recorded origin.
        Imported lazily so protocol-level unit tests don't need the
        gateway package."""
        from gateway.platforms.base import MessageEvent

        return MessageEvent(
            text=text,
            source=None,
            internal=True,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _notify(self, session_key: str, text: str, kind: str) -> None:
        """Mirror a back-channel event into the user's chat (side-channel).

        Best-effort by contract: the notification is pure display —
        the envelope was already durably handled, so a broken platform
        must not raise (raising here would trigger redelivery into the
        dedupe wall). Accepts sync or async collaborators.
        """
        if self._publish_notification is None:
            return
        try:
            result = self._publish_notification(session_key, text, kind)
            if inspect.isawaitable(result):
                await result
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "session_routing dispatcher: notification publish failed "
                "for %s: %s", session_key, e,
            )

    async def _record_seen(
        self,
        client: NATSRoutingClient,
        kv_key: str,
        record: Optional[Dict[str, Any]],
        envelope: Dict[str, Any],
        channel_id: str,
        local_session_id: str,
        peer_address: str,
    ) -> None:
        """Persist the envelope msg_id into the channel dedupe window."""
        if record is None:
            record = _channels.build_channel_record(
                channel_id=channel_id,
                session_id=local_session_id,
                peer_address=peer_address,
                state=ChannelState.ESTABLISHED,
            )
            record["role"] = "implicit"
        _channels.record_msg_id(record, envelope["msg_id"])
        await client.write_channel(kv_key=kv_key, record=record)

    async def _reply_error_loop_guarded(
        self,
        *,
        envelope: Dict[str, Any],
        to_session_key: str,
        peer_address: str,
        error_code: str,
        detail: Optional[str],
    ) -> None:
        """message.error response with the two loop guards applied."""
        msg_id = envelope["msg_id"]
        payload = envelope.get("payload") or {}

        # Guard 1: never answer a message.error with another one.
        if isinstance(payload, dict) and payload.get("type") == "message.error":
            logger.warning(
                "session_routing dispatcher: invalid message.error from %s "
                "(msg_id=%s, %s: %s) — logged, not answered",
                peer_address, msg_id, error_code, detail,
            )
            return

        # Guard 2: dedupe error responses by the bad envelope's msg_id.
        local_session_id = self._resolve_session_id(to_session_key)
        record = None
        kv_key = None
        channel_id = "unknown"
        try:
            async with NATSRoutingClient(servers=self._servers) as client:
                if local_session_id:
                    channel_id = _channels.channel_id_for(
                        local_session_id, peer_address
                    )
                    kv_key = _channels.channel_id_to_kv_key(channel_id)
                    record = await client.read_channel(kv_key)
                    if _channels.has_seen_msg_id(record, msg_id):
                        return  # already answered this bad envelope
                    await self._record_seen(
                        client, kv_key, record, envelope,
                        channel_id, local_session_id, peer_address,
                    )
                else:
                    # No local session → no channel record to dedupe in.
                    # Bounded in-memory fallback.
                    if msg_id in self._error_replied_msg_ids:
                        return
                    self._error_replied_msg_ids.append(msg_id)
                    del self._error_replied_msg_ids[:-_ERROR_REPLY_MEMORY_CAP]

                logger.warning(
                    "session_routing dispatcher: %s from %s (msg_id=%s): %s",
                    error_code, peer_address, msg_id, detail,
                )
                await self._publish_payload(
                    client=client,
                    to_address=peer_address,
                    from_session_key=to_session_key,
                    payload=_protocol.build_message_error(
                        channel_id=channel_id,
                        session_id=local_session_id or "unknown",
                        error_code=error_code,
                        in_reply_to=msg_id,
                        detail=detail,
                    ),
                )
        except NATSRoutingUnreachable as e:
            # Best-effort: an unreachable broker for the ERROR REPLY must
            # not turn a permanently-bad envelope into infinite
            # redelivery. Log and ack.
            logger.warning(
                "session_routing dispatcher: could not send %s reply for "
                "%s: %s", error_code, msg_id, e,
            )

    async def _publish_payload(
        self,
        *,
        client: NATSRoutingClient,
        to_address: str,
        from_session_key: str,
        payload: Dict[str, Any],
        verb: str = _address.DELIVER_VERB,
    ) -> None:
        """Wrap ``payload`` in an envelope and publish to the peer."""
        from_address = _address.build(self._my_gateway_id, from_session_key)
        _, peer_session_key = _address.parse(to_address)
        envelope = _routing.build_envelope(
            from_address=from_address,
            to_address=to_address,
            payload=payload,
        )
        subject = _address.encode_subject(
            self._my_gateway_id, peer_session_key, verb=verb
        )
        try:
            await client.publish_routed(
                subject=subject,
                payload=envelope,
                headers=_routing.envelope_to_headers(envelope),
            )
        except NATSRoutingUnreachable as e:
            # Replies are best-effort; the peer's timeout is the backstop.
            logger.warning(
                "session_routing dispatcher: reply publish (%s) to %s "
                "failed: %s", payload.get("type"), to_address, e,
            )
