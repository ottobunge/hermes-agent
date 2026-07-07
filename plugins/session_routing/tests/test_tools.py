"""Offline tests for the session_establish tool handler.

The broker and the receive-side dispatcher are simulated: FakeClient
captures publishes and, on seeing our handshake.request, flips the KV
record to ESTABLISHED (exactly the transition the real dispatcher
performs when the peer's ack arrives). The tool itself only publishes
and polls KV — that seam is what we exercise.
"""

from __future__ import annotations

import unittest
from typing import Any, Dict, List
from unittest.mock import patch

from plugins.session_routing import channels as _channels
from plugins.session_routing import presence as _presence
from plugins.session_routing import tools as _tools
from plugins.session_routing.channels import ChannelState

MY_GW = "gw-thinkpad"
MY_SK = "agent:main:telegram:dm:1:1"
MY_SID = "20260706_174653_5ed43bcb"
PEER_ADDR = "gw-agent-vm/agent:main:telegram:dm:2:2"

ENV = {
    "HERMES_NATS_URLS": "nats://127.0.0.1:4222",
    "HERMES_GATEWAY_ID": MY_GW,
    "HERMES_SESSION_KEY": MY_SK,
    "HERMES_SESSION_ID": MY_SID,
}


class FakeClient:
    """KV + publish fake; acks handshakes like the peer's dispatcher."""

    store: Dict[str, Dict[str, Any]] = {}
    published: List[Dict[str, Any]] = []
    respond_with: str = "ack"  # ack | reject | silence

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
        inner = payload.get("payload") or {}
        if inner.get("type") == "handshake.request":
            # Simulate peer ack + our dispatcher's transition.
            kv_key = _channels.channel_id_to_kv_key(inner["channel_id"])
            record = self.store.get(kv_key)
            if record is None:
                return {"seq": 1, "stream": "SESSIONS"}
            if type(self).respond_with == "ack":
                record["state"] = ChannelState.ESTABLISHED.value
                record["capabilities"] = ["text", "ack_delivery"]
            elif type(self).respond_with == "reject":
                record["state"] = ChannelState.REJECTED.value
                record["reject_reason"] = "channel_busy"
            type(self).store[kv_key] = record
        return {"seq": len(self.published), "stream": "SESSIONS"}


async def _fake_resolve_target(**kwargs):
    return {"gateway_id": "gw-agent-vm", "live_sessions": ["x"]}


async def _fake_resolve_target_offline(**kwargs):
    return None


class EstablishHarness(unittest.TestCase):
    def setUp(self):
        FakeClient.store = {}
        FakeClient.published = []
        FakeClient.respond_with = "ack"

    def _call(self, resolve=_fake_resolve_target, **kwargs):
        kwargs.setdefault("target", PEER_ADDR)
        kwargs.setdefault("timeout_seconds", 3.0)
        with patch.dict("os.environ", ENV), \
             patch("plugins.session_routing.tools.NATSRoutingClient", FakeClient), \
             patch("plugins.session_routing.nats_client.NATSRoutingClient", FakeClient), \
             patch.object(_presence, "resolve_target", resolve):
            return _tools.handle_session_establish(**kwargs)

    def _published_types(self):
        return [
            p["envelope"]["payload"]["type"] for p in FakeClient.published
        ]


class HappyPath(EstablishHarness):
    def test_establish_returns_channel_and_capabilities(self):
        result = self._call()
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["peer_address"], PEER_ADDR)
        self.assertEqual(
            result["channel_id"], _channels.channel_id_for(MY_SID, PEER_ADDR)
        )
        self.assertEqual(result["peer_capabilities"], ["text", "ack_delivery"])
        self.assertIsNotNone(result["established_at"])
        self.assertEqual(self._published_types(), ["handshake.request"])
        # Request rode the handshake verb subject.
        self.assertTrue(
            FakeClient.published[0]["subject"].endswith(".handshake")
        )

    def test_initial_message_sent_after_established(self):
        result = self._call(initial_message="hello agent-vm")
        self.assertTrue(result["ok"], result)
        self.assertEqual(
            self._published_types(), ["handshake.request", "message.text"]
        )
        text = FakeClient.published[1]["envelope"]["payload"]
        self.assertEqual(text["body"], "hello agent-vm")
        self.assertEqual(result["initial_message_msg_id"],
                         FakeClient.published[1]["envelope"]["msg_id"])
        self.assertTrue(
            FakeClient.published[1]["subject"].endswith(".deliver")
        )

    def test_idempotent_reestablish_returns_existing_channel(self):
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = _channels.build_channel_record(
            channel_id=channel_id,
            session_id=MY_SID,
            peer_address=PEER_ADDR,
            state=ChannelState.ESTABLISHED,
            capabilities=["text"],
        )
        FakeClient.store[_channels.channel_id_to_kv_key(channel_id)] = record

        result = self._call()
        self.assertTrue(result["ok"])
        self.assertEqual(result["channel_id"], channel_id)
        # No new handshake was started.
        self.assertEqual(self._published_types(), [])

    def test_kv_record_written_before_request_publish(self):
        self._call()
        channel_id = _channels.channel_id_for(MY_SID, PEER_ADDR)
        record = FakeClient.store[_channels.channel_id_to_kv_key(channel_id)]
        self.assertEqual(record["role"], "initiator")
        self.assertIn("nonce", record)


class FailurePaths(EstablishHarness):
    def test_bad_address(self):
        result = self._call(target="not-an-address")
        self.assertEqual(result["error"], "bad_address")

    def test_self_target_rejected(self):
        result = self._call(target=f"{MY_GW}/{MY_SK}")
        self.assertEqual(result["error"], "bad_address")
        self.assertIn("itself", result["detail"])

    def test_offline_peer(self):
        result = self._call(resolve=_fake_resolve_target_offline)
        self.assertEqual(result["error"], "no_live_session_for_address")

    def test_peer_rejection_surfaces_reason(self):
        FakeClient.respond_with = "reject"
        result = self._call()
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "rejected")
        self.assertEqual(result["reason"], "channel_busy")

    def test_timeout_returns_structured_error(self):
        FakeClient.respond_with = "silence"
        result = self._call(timeout_seconds=1.0)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "timeout")
        self.assertEqual(result["timeout_seconds"], 1.0)

    def test_no_session_env(self):
        env = dict(ENV)
        env.pop("HERMES_SESSION_KEY")
        env.pop("HERMES_SESSION_ID")
        with patch.dict("os.environ", env, clear=False) as patched:
            patched.pop("HERMES_SESSION_KEY", None)
            patched.pop("HERMES_SESSION_ID", None)
            import os
            os.environ.pop("HERMES_SESSION_KEY", None)
            os.environ.pop("HERMES_SESSION_ID", None)
            result = _tools.handle_session_establish(target=PEER_ADDR)
        self.assertEqual(result["error"], "no_session")


class OutboundNotifications(unittest.TestCase):
    """↗ notifications for outgoing back-channel sends (F3 delta).

    The tool handlers run sync (their own asyncio.run loop), so the
    notification hops onto the gateway loop via the runtime module's
    ``publish_notification_threadsafe`` — patched here to capture calls.
    """

    def setUp(self):
        FakeClient.store = {}
        FakeClient.published = []
        FakeClient.respond_with = "ack"
        self.notifications: List[Any] = []

    def _fake_notify(self, session_key, text, kind="info", gateway=None):
        self.notifications.append((session_key, text, kind))
        return True

    def _patches(self, resolve=_fake_resolve_target):
        return (
            patch.dict("os.environ", ENV),
            patch("plugins.session_routing.tools.NATSRoutingClient", FakeClient),
            patch(
                "plugins.session_routing.nats_client.NATSRoutingClient",
                FakeClient,
            ),
            patch.object(_presence, "resolve_target", resolve),
            patch(
                "plugins.session_routing.runtime."
                "publish_notification_threadsafe",
                self._fake_notify,
            ),
        )

    def _text_content(self, body="ping"):
        return {
            "type": "message.text",
            "channel_id": f"{MY_SID}:deadbeef",
            "session_id": MY_SID,
            "in_reply_to": None,
            "protocol_version": 1,
            "body": body,
        }

    def test_route_send_message_text_notifies_sender_session(self):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_route_send(
                target=PEER_ADDR, content=self._text_content("ping")
            )
        self.assertTrue(result["ok"], result)
        self.assertEqual(
            self.notifications,
            [(MY_SK, f"↗ back-channel to {PEER_ADDR}\nping", "back_channel_out")],
        )

    def test_route_send_non_text_content_no_notification(self):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_route_send(
                target=PEER_ADDR,
                content={"type": "message.ack_delivery", "in_reply_to": "x"},
            )
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.notifications, [])

    def test_route_send_failure_no_notification(self):
        p1, p2, p3, p4, p5 = self._patches(resolve=_fake_resolve_target_offline)
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_route_send(
                target=PEER_ADDR, content=self._text_content()
            )
        self.assertFalse(result["ok"])
        self.assertEqual(self.notifications, [])

    def test_establish_initial_message_notifies(self):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_establish(
                target=PEER_ADDR,
                initial_message="hello over there",
                timeout_seconds=3.0,
            )
        self.assertTrue(result["ok"], result)
        self.assertEqual(
            self.notifications,
            [(
                MY_SK,
                f"↗ back-channel to {PEER_ADDR}\nhello over there",
                "back_channel_out",
            )],
        )

    def test_establish_without_initial_message_no_notification(self):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_establish(
                target=PEER_ADDR, timeout_seconds=3.0
            )
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.notifications, [])


class SessionHandleShareText(unittest.TestCase):
    """share_text — paste-ready session id for the operator (F3 delta).

    The operator's flow is copy/paste: ask one bot for its session id,
    paste the block to the other bot. ``share_text`` is that block,
    verbatim — the agent should not have to reformat anything.
    """

    def test_share_text_matches_expected_format(self):
        with patch.dict("os.environ", ENV):
            result = _tools.handle_session_handle()
        self.assertTrue(result["ok"], result)
        self.assertEqual(
            result["share_text"],
            f"My session id is: `{MY_GW}/{MY_SK}`\n"
            f"  gateway_id: `{MY_GW}`\n"
            f"  session_key: `{MY_SK}`\n"
            "You can use this to start a back-channel with me by asking "
            "another agent to `session_establish` targeting this address.",
        )

    def test_share_text_contains_canonical_address_and_gateway_id(self):
        with patch.dict("os.environ", ENV):
            result = _tools.handle_session_handle()
        self.assertTrue(result["ok"], result)
        self.assertIn(f"`{result['address']}`", result["share_text"])
        self.assertIn(f"`{result['gateway_id']}`", result["share_text"])

    def test_no_session_has_no_share_text(self):
        env = {k: v for k, v in ENV.items() if k != "HERMES_SESSION_KEY"}
        with patch.dict("os.environ", env, clear=True):
            result = _tools.handle_session_handle()
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "no_session")
        self.assertNotIn("share_text", result)


class SchemaSurface(unittest.TestCase):
    def test_schema_shape(self):
        schema = _tools.SESSION_ESTABLISH_SCHEMA
        self.assertEqual(schema["name"], "session_establish")
        params = schema["parameters"]
        self.assertEqual(params["required"], ["target"])
        self.assertIn("initial_message", params["properties"])
        self.assertIn("timeout_seconds", params["properties"])
        self.assertFalse(params["additionalProperties"])

    def test_registered_in_plugin_tools(self):
        import plugins.session_routing as plugin
        names = [t[0] for t in plugin._TOOLS]
        self.assertIn("session_establish", names)


class SendSidePayloadValidation(OutboundNotifications):
    """session_route_send fails closed on payloads the recipient's
    dispatcher would reject — the error surfaces in the TOOL RESULT
    instead of dying receive-side where the sender can't see it."""

    def _send(self, content):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            return _tools.handle_session_route_send(
                target=PEER_ADDR, content=content
            )

    def test_content_without_type_rejected_before_publish(self):
        # The live-incident shape: free-form dict, no registered type.
        result = self._send({"kind": "back_channel_probe", "text": "hi"})
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_payload")
        self.assertEqual(result["error_code"], "validation_error")
        self.assertIn("message.text", result["hint"])
        self.assertEqual(FakeClient.published, [])

    def test_unregistered_type_rejected(self):
        result = self._send({"type": "session.message", "body": "hi"})
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_payload")
        self.assertEqual(result["error_code"], "unknown_type")
        self.assertEqual(FakeClient.published, [])

    def test_empty_channel_id_rejected(self):
        # The mirror-image live-incident shape (peer sent channel_id "").
        result = self._send(
            {"type": "message.text", "body": "hi", "channel_id": ""}
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_payload")
        self.assertEqual(FakeClient.published, [])

    def test_omitted_content_rejected(self):
        p1, p2, p3, p4, p5 = self._patches()
        with p1, p2, p3, p4, p5:
            result = _tools.handle_session_route_send(target=PEER_ADDR)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_payload")

    def test_minimal_message_text_still_sends(self):
        # v0.2-interop minimum: type + body (dispatcher fills the rest).
        result = self._send({"type": "message.text", "body": "hi"})
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(FakeClient.published), 1)


if __name__ == "__main__":
    unittest.main()
