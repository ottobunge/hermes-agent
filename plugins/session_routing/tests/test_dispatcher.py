"""Offline tests for the receive-side dispatcher (adapter-FIFO-only).

The NATS client is faked at the dispatcher's import seam; gateway
interaction is captured through the injected ``resolve_session_id`` /
``enqueue_event`` callables. Covers the plan's QA scenarios: injection
with prefix+metadata, redelivery dedupe, loop-guarded unknown-type
errors, unsupported delegate.*, message.error logging, and the
handshake state transitions.
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from plugins.session_routing import channels as _channels
from plugins.session_routing import handshake as _handshake
from plugins.session_routing import protocol as _protocol
from plugins.session_routing import routing as _routing
from plugins.session_routing.channels import ChannelState
from plugins.session_routing.dispatcher import BackChannelDispatcher

MY_GW = "gw-thinkpad"
PEER_GW = "gw-agent-vm"
MY_SK = "agent:main:telegram:dm:1:1"
PEER_SK = "agent:main:telegram:dm:2:2"
MY_ADDR = f"{MY_GW}/{MY_SK}"
PEER_ADDR = f"{PEER_GW}/{PEER_SK}"
MY_SID = "20260706_174653_5ed43bcb"
PEER_SID = "20260706_174909_fadd9fd3"


class FakeClient:
    """In-memory KV + publish capture, patched over NATSRoutingClient."""

    store: Dict[str, Dict[str, Any]] = {}
    published: List[Dict[str, Any]] = []

    def __init__(self, servers, name="fake"):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def read_channel(self, kv_key):
        record = self.store.get(kv_key)
        return dict(record) if record else None

    async def write_channel(self, *, kv_key, record):
        type(self).store[kv_key] = dict(record)

    async def publish_routed(self, *, subject, payload, headers=None, timeout=5.0):
        type(self).published.append({"subject": subject, "envelope": payload})
        return {"seq": len(self.published), "stream": "SESSIONS"}


def _envelope(payload, *, to_address=MY_ADDR, from_address=PEER_ADDR, msg_id=None):
    return _routing.build_envelope(
        from_address=from_address,
        to_address=to_address,
        payload=payload,
        msg_id=msg_id,
    )


def _text_payload(body="hello", channel_id=None, session_id=PEER_SID):
    return _protocol.build_message_text(
        channel_id=channel_id or f"{PEER_SID}:deadbeef",
        session_id=session_id,
        body=body,
    )


class DispatcherHarness(unittest.TestCase):
    def setUp(self):
        FakeClient.store = {}
        FakeClient.published = []
        self.enqueued: List[Any] = []
        self.enqueue_ok = True
        self.notify_ok = True
        self.notifications: List[Any] = []
        self.ops: List[str] = []  # cross-collaborator ordering trace
        self.session_ids = {MY_SK: MY_SID}

        self.dispatcher = BackChannelDispatcher(
            servers=["nats://x:4222"],
            my_gateway_id=MY_GW,
            resolve_session_id=lambda sk: self.session_ids.get(sk),
            enqueue_event=self._enqueue,
            publish_notification=self._notify,
        )

    def _enqueue(self, session_key, event):
        if not self.enqueue_ok:
            return False
        self.enqueued.append((session_key, event))
        self.ops.append("enqueue")
        return True

    async def _notify(self, session_key, text, kind):
        if not self.notify_ok:
            raise RuntimeError("notification pipe broken")
        self.notifications.append((session_key, text, kind))
        self.ops.append("notify")

    def _handle(self, envelope, subject="from.gw-agent-vm.sk.deliver"):
        with patch(
            "plugins.session_routing.dispatcher.NATSRoutingClient", FakeClient
        ):
            asyncio.run(self.dispatcher.handle(envelope, subject, {}))

    def _published_types(self):
        return [p["envelope"]["payload"]["type"] for p in FakeClient.published]

    def _local_record(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        return FakeClient.store.get(_channels.channel_id_to_kv_key(channel_id))


class MessageText(DispatcherHarness):
    def test_injects_synthetic_turn_with_prefix_and_metadata(self):
        envelope = _envelope(_text_payload("what's the status?"))
        self._handle(envelope)

        self.assertEqual(len(self.enqueued), 1)
        session_key, event = self.enqueued[0]
        self.assertEqual(session_key, MY_SK)
        self.assertEqual(
            event.text, f"[back-channel from {PEER_ADDR}] what's the status?"
        )
        self.assertTrue(event.internal)
        self.assertIsNone(event.source)
        self.assertEqual(event.metadata["back_channel"], True)
        self.assertEqual(event.metadata["peer_address"], PEER_ADDR)
        self.assertEqual(event.metadata["envelope_msg_id"], envelope["msg_id"])
        self.assertEqual(
            event.metadata["channel_id"],
            _channels.channel_id_for(MY_SID, PEER_ADDR),
        )

    def test_delivery_ack_published_after_enqueue(self):
        envelope = _envelope(_text_payload())
        self._handle(envelope)
        self.assertEqual(self._published_types(), ["message.ack_delivery"])
        ack = FakeClient.published[0]["envelope"]["payload"]
        self.assertEqual(ack["in_reply_to"], envelope["msg_id"])

    def test_redelivery_injects_exactly_once(self):
        envelope = _envelope(_text_payload())
        self._handle(envelope)
        self._handle(envelope)  # broker redelivery, same msg_id
        self.assertEqual(len(self.enqueued), 1)
        # ack_delivery also sent only once
        self.assertEqual(self._published_types(), ["message.ack_delivery"])

    def test_msg_id_recorded_in_kv_before_enqueue_visibility(self):
        envelope = _envelope(_text_payload())
        self._handle(envelope)
        record = self._local_record()
        self.assertIsNotNone(record)
        self.assertIn(envelope["msg_id"], record["recent_msg_ids"])

    def test_enqueue_failure_is_logged_not_raised_and_no_ack_delivery(self):
        self.enqueue_ok = False
        envelope = _envelope(_text_payload())
        self._handle(envelope)  # must not raise
        self.assertEqual(self.enqueued, [])
        self.assertEqual(self._published_types(), [])


class AddressingAndValidation(DispatcherHarness):
    def test_envelope_for_other_gateway_skipped(self):
        envelope = _envelope(
            _text_payload(), to_address=f"gw-elsewhere/{MY_SK}"
        )
        self._handle(envelope)
        self.assertEqual(self.enqueued, [])
        self.assertEqual(FakeClient.published, [])

    def test_unknown_session_dropped(self):
        envelope = _envelope(
            _text_payload(), to_address=f"{MY_GW}/agent:unknown:session"
        )
        self._handle(envelope)
        self.assertEqual(self.enqueued, [])
        self.assertEqual(FakeClient.published, [])

    def test_malformed_envelope_does_not_raise(self):
        self._handle({"not": "an envelope"})
        self.assertEqual(self.enqueued, [])

    def test_unknown_type_gets_loop_guarded_error(self):
        payload = _text_payload()
        payload["type"] = "message.evil"
        envelope = _envelope(payload)
        self._handle(envelope)

        self.assertEqual(self.enqueued, [])
        self.assertEqual(self._published_types(), ["message.error"])
        error = FakeClient.published[0]["envelope"]["payload"]
        self.assertEqual(error["error_code"], "unknown_type")
        self.assertEqual(error["in_reply_to"], envelope["msg_id"])

        # Redelivery of the SAME bad envelope → no second error reply.
        self._handle(envelope)
        self.assertEqual(self._published_types(), ["message.error"])

    def test_message_error_never_answered_with_message_error(self):
        # Even a MALFORMED message.error (missing error_code) is only
        # logged — never answered (loop guard 1).
        payload = _protocol.build_message_error(
            channel_id=f"{PEER_SID}:deadbeef",
            session_id=PEER_SID,
            error_code="unknown_type",
            in_reply_to=None,
        )
        del payload["error_code"]
        envelope = _envelope(payload)
        self._handle(envelope)
        self.assertEqual(FakeClient.published, [])
        self.assertEqual(self.enqueued, [])

    def test_valid_message_error_logged_not_injected(self):
        payload = _protocol.build_message_error(
            channel_id=f"{PEER_SID}:deadbeef",
            session_id=PEER_SID,
            error_code="unknown_type",
            in_reply_to="some-msg",
            detail="peer complained",
        )
        envelope = _envelope(payload)
        self._handle(envelope)
        self.assertEqual(self.enqueued, [])       # never a user turn
        self.assertEqual(FakeClient.published, [])  # never answered
        # But deduped: msg_id landed in the channel window.
        record = self._local_record()
        self.assertIn(envelope["msg_id"], record["recent_msg_ids"])

    def test_delegate_task_returns_unsupported_type(self):
        payload = {
            "type": "delegate.task",
            "channel_id": f"{PEER_SID}:deadbeef",
            "session_id": PEER_SID,
            "in_reply_to": None,
            "protocol_version": 1,
            "task": "do something powerful",
        }
        envelope = _envelope(payload)
        self._handle(envelope)
        self.assertEqual(self.enqueued, [])
        self.assertEqual(self._published_types(), ["message.error"])
        error = FakeClient.published[0]["envelope"]["payload"]
        self.assertEqual(error["error_code"], "unsupported_type")
        self.assertIn("Phase 1", error["detail"])


class HandshakeFlow(DispatcherHarness):
    def _request_envelope(self, nonce="nonce-1"):
        request = _handshake.build_request(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce=nonce,
        )
        return _envelope(request)

    def test_request_creates_responder_record_and_acks(self):
        envelope = self._request_envelope()
        self._handle(envelope)

        record = self._local_record()
        self.assertIsNotNone(record)
        self.assertEqual(record["state"], "INITIATING")
        self.assertEqual(record["role"], "responder")
        self.assertEqual(record["nonce"], "nonce-1")

        self.assertEqual(self._published_types(), ["handshake.ack"])
        published = FakeClient.published[0]
        ack = published["envelope"]["payload"]
        self.assertEqual(ack["nonce"], "nonce-1")
        self.assertEqual(ack["in_reply_to"], envelope["msg_id"])
        self.assertTrue(published["subject"].endswith(".handshake"))
        # Reply addressed back to the peer's session.
        self.assertEqual(published["envelope"]["to"], PEER_ADDR)
        self.assertEqual(published["envelope"]["from"], MY_ADDR)

    def test_established_completes_responder_side(self):
        self._handle(self._request_envelope())
        established = _handshake.build_established(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce="nonce-1",
            in_reply_to="whatever",
        )
        self._handle(_envelope(established))
        self.assertEqual(self._local_record()["state"], "ESTABLISHED")

    def test_established_with_bad_nonce_ignored(self):
        self._handle(self._request_envelope())
        established = _handshake.build_established(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce="forged",
            in_reply_to="whatever",
        )
        self._handle(_envelope(established))
        self.assertEqual(self._local_record()["state"], "INITIATING")

    def test_ack_transitions_initiator_and_replies_established(self):
        # Seed OUR initiator record (as session_establish would).
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.INITIATING,
        )
        record["role"] = "initiator"
        record["nonce"] = "our-nonce"
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        ack = _handshake.build_ack(
            channel_id=f"{PEER_SID}:22222222",
            session_id=PEER_SID,
            nonce="our-nonce",
            in_reply_to="env-req",
            capabilities=["text"],
        )
        self._handle(_envelope(ack))

        stored = self._local_record()
        self.assertEqual(stored["state"], "ESTABLISHED")
        self.assertEqual(stored["capabilities"], ["text"])
        self.assertEqual(self._published_types(), ["handshake.established"])

    def test_ack_with_bad_nonce_ignored(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.INITIATING,
        )
        record["role"] = "initiator"
        record["nonce"] = "our-nonce"
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        ack = _handshake.build_ack(
            channel_id=f"{PEER_SID}:22222222",
            session_id=PEER_SID,
            nonce="forged",
            in_reply_to="env-req",
        )
        self._handle(_envelope(ack))
        self.assertEqual(self._local_record()["state"], "INITIATING")
        self.assertEqual(FakeClient.published, [])

    def test_simultaneous_request_race_is_deterministic(self):
        # WE initiated already...
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.INITIATING,
        )
        record["role"] = "initiator"
        record["nonce"] = "our-nonce"
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        # ...and the peer's request arrives. MY_ADDR > PEER_ADDR
        # lexicographically ("gw-t..." > "gw-a..."), so THEIR request
        # wins and we must adopt the responder role and ack.
        self._handle(self._request_envelope(nonce="their-nonce"))
        stored = self._local_record()
        self.assertEqual(stored["role"], "responder")
        self.assertEqual(stored["nonce"], "their-nonce")
        self.assertEqual(self._published_types(), ["handshake.ack"])

    def test_bye_closes_established_channel(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.ESTABLISHED,
        )
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        bye = _handshake.build_bye(
            channel_id=f"{PEER_SID}:33333333", session_id=PEER_SID
        )
        self._handle(_envelope(bye))
        self.assertEqual(self._local_record()["state"], "CLOSED")

    def test_reject_terminates_initiator(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.INITIATING,
        )
        record["role"] = "initiator"
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        reject = _handshake.build_reject(
            channel_id=f"{PEER_SID}:44444444",
            session_id=PEER_SID,
            in_reply_to="env-req",
            reason="channel_busy",
        )
        self._handle(_envelope(reject))
        stored = self._local_record()
        self.assertEqual(stored["state"], "REJECTED")
        self.assertEqual(stored["reject_reason"], "channel_busy")


class Notifications(DispatcherHarness):
    """Code-published, user-visible system notifications (F3 delta).

    The dispatcher mirrors every accepted back-channel event to the
    platform via the injected ``publish_notification`` collaborator
    (gateway's ``publish_internal_notification``). Notifications are a
    side-channel display — they NEVER enter session history, and their
    failure must never affect envelope handling.
    """

    def test_inbound_text_publishes_visible_notification(self):
        envelope = _envelope(_text_payload("what's the status?"))
        self._handle(envelope)

        self.assertEqual(len(self.notifications), 1)
        session_key, text, kind = self.notifications[0]
        self.assertEqual(session_key, MY_SK)
        self.assertEqual(
            text, f"🔄 back-channel from {PEER_ADDR}\nwhat's the status?"
        )
        self.assertEqual(kind, "back_channel_in")

    def test_notification_published_after_enqueue(self):
        self._handle(_envelope(_text_payload()))
        self.assertEqual(self.ops, ["enqueue", "notify"])

    def test_duplicate_envelope_notifies_exactly_once(self):
        envelope = _envelope(_text_payload())
        self._handle(envelope)
        self._handle(envelope)  # broker redelivery, same msg_id
        self.assertEqual(len(self.notifications), 1)

    def test_enqueue_failure_suppresses_notification(self):
        # If the turn was NOT injected, telling the user "received" would
        # be misleading — the warning log covers the operational side.
        self.enqueue_ok = False
        self._handle(_envelope(_text_payload()))
        self.assertEqual(self.notifications, [])

    def test_notification_failure_never_breaks_dispatch(self):
        self.notify_ok = False
        envelope = _envelope(_text_payload())
        self._handle(envelope)  # must not raise
        self.assertEqual(len(self.enqueued), 1)
        # ack_delivery still goes back to the sender.
        self.assertEqual(self._published_types(), ["message.ack_delivery"])

    def test_dispatcher_without_publisher_still_works(self):
        bare = BackChannelDispatcher(
            servers=["nats://x:4222"],
            my_gateway_id=MY_GW,
            resolve_session_id=lambda sk: self.session_ids.get(sk),
            enqueue_event=self._enqueue,
        )
        with patch(
            "plugins.session_routing.dispatcher.NATSRoutingClient", FakeClient
        ):
            asyncio.run(bare.handle(_envelope(_text_payload()), "s", {}))
        self.assertEqual(len(self.enqueued), 1)

    def _seed_initiator_record(self, nonce="our-nonce"):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.INITIATING,
        )
        record["role"] = "initiator"
        record["nonce"] = nonce
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

    def test_ack_established_notifies_lifecycle(self):
        # Initiator side: peer's ack lands, channel goes ESTABLISHED.
        self._seed_initiator_record()
        ack = _handshake.build_ack(
            channel_id=f"{PEER_SID}:22222222",
            session_id=PEER_SID,
            nonce="our-nonce",
            in_reply_to="env-req",
        )
        self._handle(_envelope(ack))
        self.assertEqual(
            self.notifications,
            [(MY_SK, f"✅ back-channel established with {PEER_ADDR}", "lifecycle")],
        )

    def test_ack_with_bad_nonce_no_notification(self):
        self._seed_initiator_record()
        ack = _handshake.build_ack(
            channel_id=f"{PEER_SID}:22222222",
            session_id=PEER_SID,
            nonce="forged",
            in_reply_to="env-req",
        )
        self._handle(_envelope(ack))
        self.assertEqual(self.notifications, [])

    def test_established_transition_notifies_lifecycle(self):
        # Responder side: request → (we ack) → peer confirms established.
        request = _handshake.build_request(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce="nonce-1",
        )
        self._handle(_envelope(request))
        self.assertEqual(self.notifications, [])  # not established yet

        established = _handshake.build_established(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce="nonce-1",
            in_reply_to="whatever",
        )
        self._handle(_envelope(established))
        self.assertEqual(
            self.notifications,
            [(MY_SK, f"✅ back-channel established with {PEER_ADDR}", "lifecycle")],
        )

        # An idempotent re-confirm (new msg_id, state already ESTABLISHED)
        # must not produce a second ✅.
        again = _handshake.build_established(
            channel_id=f"{PEER_SID}:11111111",
            session_id=PEER_SID,
            nonce="nonce-1",
            in_reply_to="whatever",
        )
        self._handle(_envelope(again))
        self.assertEqual(len(self.notifications), 1)

    def test_bye_notifies_closed(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.ESTABLISHED,
        )
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        bye = _handshake.build_bye(
            channel_id=f"{PEER_SID}:33333333", session_id=PEER_SID
        )
        self._handle(_envelope(bye))
        self.assertEqual(
            self.notifications,
            [(MY_SK, f"❌ back-channel closed with {PEER_ADDR}", "lifecycle")],
        )

    def test_bye_on_unknown_channel_no_notification(self):
        bye = _handshake.build_bye(
            channel_id=f"{PEER_SID}:33333333", session_id=PEER_SID
        )
        self._handle(_envelope(bye))
        self.assertEqual(self.notifications, [])


if __name__ == "__main__":
    unittest.main()
