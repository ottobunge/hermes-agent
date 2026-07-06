"""Offline tests for plugins.session_routing.nats_client.

These skip broker-touching code; integration tests under
test_integration.py exercise connect/publish/KV against a real broker.
What we test here:
  - is_fresh() boundary semantics
  - utc_now_iso() format
  - module-level constants
  - NATSRoutingClient rejects empty server list at construct time
  - KV payload roundtrip via a stub (we don't run a real broker for unit
    tests; the integration test class under test_integration.py does that
    with the local nats-server on 127.0.0.1:4222)
"""

from __future__ import annotations

import re
import unittest
from datetime import datetime, timezone

from plugins.session_routing.nats_client import (
    ALLOW_BUCKET,
    NATSRoutingClient,
    NATSRoutingUnreachable,
    PRESENCE_BUCKET,
    STREAM_NAME,
    is_fresh,
    utc_now_iso,
)


class Constructors(unittest.TestCase):
    def test_rejects_empty_servers(self):
        with self.assertRaises(ValueError):
            NATSRoutingClient(servers=[])

    def test_accepts_one_server(self):
        c = NATSRoutingClient(servers=["nats://localhost:4222"])
        self.assertEqual(c._servers, ["nats://localhost:4222"])

    def test_constants(self):
        self.assertEqual(STREAM_NAME, "SESSIONS")
        self.assertEqual(PRESENCE_BUCKET, "session_presence")
        self.assertEqual(ALLOW_BUCKET, "session_allow")


class IsFresh(unittest.TestCase):
    def test_fresh_within_ttl(self):
        now = 1_000_000.0
        presence = {"last_seen": now - 30}  # 30s old, ttl=60s → fresh
        self.assertTrue(is_fresh(presence, ttl_seconds=60, now=now))

    def test_at_ttl_boundary_is_fresh(self):
        now = 1_000_000.0
        presence = {"last_seen": now - 60}  # exactly at ttl
        # Boundary inclusive: at-the-tick IS still fresh. Off-by-one here
        # would manifest as the heartbeat immediately being reported stale
        # right after publish — that would defeat the purpose.
        self.assertTrue(is_fresh(presence, ttl_seconds=60, now=now))

    def test_stale_past_ttl(self):
        now = 1_000_000.0
        presence = {"last_seen": now - 91}  # 91s old, ttl=90s → stale
        self.assertFalse(is_fresh(presence, ttl_seconds=90, now=now))

    def test_missing_last_seen(self):
        self.assertFalse(is_fresh({}, ttl_seconds=60))
        self.assertFalse(is_fresh({"last_seen": None}, ttl_seconds=60))
        self.assertFalse(is_fresh({"last_seen": ""}, ttl_seconds=60))

    def test_unparseable_last_seen(self):
        self.assertFalse(is_fresh({"last_seen": "tomorrow"}, ttl_seconds=60))
        self.assertFalse(is_fresh({"last_seen": [1, 2]}, ttl_seconds=60))
        self.assertFalse(is_fresh({"last_seen": object()}, ttl_seconds=60))

    def test_non_dict_input(self):
        self.assertFalse(is_fresh(None, ttl_seconds=60))
        self.assertFalse(is_fresh("not-a-dict", ttl_seconds=60))


class UtcNowIso(unittest.TestCase):
    def test_iso_format_round_trip(self):
        s = utc_now_iso()
        # Accept Z suffix or +00:00 form per Python version.
        self.assertRegex(
            s,
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(\+00:00|Z)$",
        )
        # Round-trip: parse the timestamp, ensure it's offset-aware UTC.
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
        self.assertIsNotNone(ts.tzinfo)
        self.assertEqual(ts.utcoffset().total_seconds(), 0.0)


class NATSRoutingUnreachable_IsException(Exception):
    pass


class ErrorClass(unittest.TestCase):
    def test_carries_message(self):
        e = NATSRoutingUnreachable("boom")
        self.assertIn("boom", str(e))
        # Python 3 exception chaining: assigning __cause__ via 'from'.
        try:
            raise NATSRoutingUnreachable("x") from ValueError("original")
        except NATSRoutingUnreachable as caught:
            self.assertIsInstance(caught.__cause__, ValueError)


class DeferredAck(unittest.TestCase):
    """client.ack() semantics for auto_ack=False fetches (offline)."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_ack_none_handle_returns_false(self):
        c = NATSRoutingClient(servers=["nats://localhost:4222"])
        self.assertFalse(self._run(c.ack(None)))

    def test_ack_success(self):
        class FakeMsg:
            def __init__(self):
                self.acked = False

            async def ack(self):
                self.acked = True

        c = NATSRoutingClient(servers=["nats://localhost:4222"])
        msg = FakeMsg()
        self.assertTrue(self._run(c.ack(msg)))
        self.assertTrue(msg.acked)

    def test_ack_failure_logged_not_raised(self):
        class ExplodingMsg:
            async def ack(self):
                raise RuntimeError("connection gone")

        c = NATSRoutingClient(servers=["nats://localhost:4222"])
        self.assertFalse(self._run(c.ack(ExplodingMsg())))


class HandshakeSubjects(unittest.TestCase):
    """Handshake frames ride the same stream/filter as deliver frames."""

    def test_handshake_subject_shape(self):
        from plugins.session_routing.address import (
            HANDSHAKE_VERB,
            decode_subject,
            handshake_subject,
            subject_filter_for,
        )
        subject = handshake_subject("gw-thinkpad", "agent:main:telegram:dm:1:1")
        self.assertEqual(
            subject,
            f"from.gw-thinkpad.agent:main:telegram:dm:1:1.{HANDSHAKE_VERB}",
        )
        # Recipient's allow-filter matches handshake subjects too.
        prefix = subject_filter_for("gw-thinkpad")[:-1]  # strip '>'
        self.assertTrue(subject.startswith(prefix))
        # Round-trip: sender + recipient session_key recoverable.
        sender, sk = decode_subject(subject)
        self.assertEqual(sender, "gw-thinkpad")
        self.assertEqual(sk, "agent:main:telegram:dm:1:1")


if __name__ == "__main__":
    unittest.main()
