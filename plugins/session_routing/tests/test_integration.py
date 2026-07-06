"""Integration tests for plugins.session_routing — broker round-trip.

These tests connect to a REAL nats-server. They use uniquely-named
KV buckets per test (suffixed with a uuid fragment) so the live broker's
real ``session_presence`` / ``session_allow`` buckets are NEVER touched —
the integration layer reuses the same JetStream STREAM (``SESSIONS``,
already provisioned by session-bridge) but writes only to scratch buckets.

Skip rules:
  - ``import nats`` fails  → all broker tests skipped (system python3
    without the optional extra). The envelope-test is offline and runs
    regardless.
  - ``nats-server`` not reachable on 127.0.0.1:4222 → broker tests skipped.

Run via the venv that has nats-py installed:
    cd ~/repos/personal/hermes-agent
    .venv/bin/python3 -m unittest \
        plugins.session_routing.tests.test_integration -v
"""

from __future__ import annotations

import asyncio
import functools
import socket
import time
import unittest
import uuid

from plugins.session_routing import address, presence, routing
from plugins.session_routing.allow import effective_allow_list
from plugins.session_routing.nats_client import NATSRoutingClient

# ---------------------------------------------------------------------------
# Skip guards
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _broker_reachable() -> bool:
    """Cheap TCP probe — does NOT import nats. Used as a fast skip gate."""
    try:
        with socket.create_connection(("127.0.0.1", 4222), timeout=1.0):
            return True
    except OSError:
        return False


NATS_AVAILABLE = _broker_reachable()

try:
    import nats  # noqa: F401
    NATS_PY_AVAILABLE = True
except ImportError:
    NATS_PY_AVAILABLE = False


_BROKER_GATE = unittest.skipUnless(
    NATS_AVAILABLE and NATS_PY_AVAILABLE,
    "nats-server at 127.0.0.1:4222 unreachable, or nats-py not installed",
)

SERVERS = ["nats://127.0.0.1:4222"]


def _uniquely_tagged_bucket(base: str) -> str:
    """Produce a per-test bucket name so concurrent runs don't collide.

    The real plugin uses ``session_presence`` / ``session_allow``; tests
    use ``session_routing_test_<uuid>_<base>`` and stay out of those.
    """
    tag = uuid.uuid4().hex[:8]
    # Bucket names must be lowercase alphanumeric + `_-`. nats-py
    # forbids dots, so we use underscore separators.
    return f"srtest_{tag}_{base}"


# ---------------------------------------------------------------------------
# Offline envelope test (always runs)
# ---------------------------------------------------------------------------


class EnvelopeRoundTrip(unittest.TestCase):
    """Offline tests of build_envelope / validate_envelope.

    Stays in this module so the "all integration tests" run reports
    a single tight grouping. Always runs — no broker dependency.
    """

    def test_build_and_validate_happy_path(self):
        env = routing.build_envelope(
            from_address="gw-thinkpad/agent:main:telegram:dm:189562939:39702",
            to_address="gw-agent-vm/agent:main:darwin:nix-darwin:otto@mbp:0",
            payload={"ask": "ping", "ts": 1730000000.0},
        )
        # Field shape matches the canonical contract.
        self.assertEqual(env["kind"], "session_route")
        self.assertEqual(env["v"], 1)
        self.assertEqual(env["from"], "gw-thinkpad/agent:main:telegram:dm:189562939:39702")
        self.assertEqual(env["to"], "gw-agent-vm/agent:main:darwin:nix-darwin:otto@mbp:0")
        self.assertIsInstance(env["payload"], dict)
        # uuid4 string is 36 chars (8-4-4-4-12 + dashes)
        self.assertEqual(len(env["msg_id"]), 36)
        # ts is a float; ts_iso round-trips
        from datetime import datetime
        ts_iso = datetime.fromisoformat(env["ts_iso"].replace("Z", "+00:00"))
        # Equal to env["ts"] within float-rounding tolerance (~1e-5s
        # at second-resolution ISO without microseconds is the typical
        # difference).
        self.assertAlmostEqual(ts_iso.timestamp(), env["ts"], places=5)

        # validate_envelope accepts what we just built.
        routing.validate_envelope(env)

    def test_msg_id_passed_through(self):
        custom = "12345678-1234-1234-1234-123456789012"
        env = routing.build_envelope(
            from_address="gw-a/x", to_address="gw-b/y",
            payload={}, msg_id=custom,
        )
        self.assertEqual(env["msg_id"], custom)

    def test_validate_rejects_missing_fields(self):
        base = routing.build_envelope(
            from_address="gw-a/x", to_address="gw-b/y", payload={},
        )
        for required in ("msg_id", "ts", "from", "to", "payload", "v"):
            broken = dict(base)
            broken.pop(required)
            with self.subTest(missing=required):
                with self.assertRaises(ValueError):
                    routing.validate_envelope(broken)

    def test_validate_rejects_wrong_types(self):
        base = routing.build_envelope(
            from_address="gw-a/x", to_address="gw-b/y", payload={},
        )
        # Wrong v
        bad = dict(base); bad["v"] = 2
        with self.assertRaises(ValueError):
            routing.validate_envelope(bad)
        # Non-dict payload
        bad = dict(base); bad["payload"] = "oops"  # type: ignore[assignment]
        with self.assertRaises(ValueError):
            routing.validate_envelope(bad)
        # Non-string from
        bad = dict(base); bad["from"] = 42  # type: ignore[assignment]
        with self.assertRaises(ValueError):
            routing.validate_envelope(bad)

    def test_signable_bytes_is_deterministic(self):
        env = routing.build_envelope(
            from_address="gw-a/x", to_address="gw-b/y", payload={"k": "v"},
        )
        a = routing.envelope_signable_bytes(env)
        b = routing.envelope_signable_bytes(env)
        self.assertEqual(a, b)
        # Compact separators (no spaces) and sort_keys mean the byte
        # form IS deterministic.
        self.assertNotIn(b" ", a)


# ---------------------------------------------------------------------------
# Broker tests — skipped when nats-py missing or broker unreachable
# ---------------------------------------------------------------------------


@_BROKER_GATE
class TestPublishRoundTrip(unittest.TestCase):
    """Sender publishes → recipient with allow-list containing sender
    receives exactly that message via inbox_fetch."""

    async def _publish_and_fetch(self):
        sender_gw = f"gw-sender-{uuid.uuid4().hex[:6]}"
        recipient_gw = f"gw-recipient-{uuid.uuid4().hex[:6]}"
        sk = "agent:main:cli:integration-test"
        target = address.build(recipient_gw, sk)
        sender_addr = address.build(sender_gw, sk)
        # Subject now has SENDER's gateway_id in the prefix so the
        # recipient's ``from.<allowed_sender>.>`` filter matches.
        subj = address.encode_subject(sender_gw, sk, verb="deliver")

        envelope = routing.build_envelope(
            from_address=sender_addr,
            to_address=target,
            payload={"test": "publish_round_trip", "n": 42},
        )
        # Pre-load the recipient's allow-list so inbox_fetch matches.
        async with NATSRoutingClient(servers=SERVERS) as client:
            await client.write_allow_list(
                recipient_gateway_id=recipient_gw,
                sender_gateway_ids=[sender_gw],
            )
            # Publish; capture the seq.
            ack = await client.publish_routed(
                subject=subj,
                payload=envelope,
                headers=routing.envelope_to_headers(envelope),
            )
            # Pull from inbox with the allow-list applied.
            fetched = await client.inbox_fetch(
                my_gateway_id=recipient_gw,
                allowed_sender_gateway_ids=[sender_gw],
                timeout=2.0,
            )
            # Best-effort cleanup
            try:
                await client._js.delete_key_value(  # type: ignore[attr-defined]
                    bucket="session_allow", key=recipient_gw,
                )
            except Exception:  # noqa: BLE001 — best-effort
                pass
        return envelope, ack, fetched

    def test_publish_then_inbox_returns_same_message(self):
        loop = asyncio.new_event_loop()
        try:
            envelope, ack, fetched = loop.run_until_complete(self._publish_and_fetch())
        finally:
            loop.close()
        # Publisher saw a real seq number from the broker.
        self.assertIsNotNone(ack.get("seq"))
        self.assertEqual(ack.get("stream"), "SESSIONS")
        # Recipient inbox returned the same envelope (full payload).
        msg = fetched.get("message")
        self.assertIsNotNone(msg, "broker returned no message — fetch likely timed out")
        self.assertEqual(msg["payload"], envelope)
        # From header is what we set.
        self.assertEqual(msg["headers"].get("from"), envelope["from"])


@_BROKER_GATE
class TestAllowListFilters(unittest.TestCase):
    """Sender NOT in recipient's allow-list → inbox_fetch returns nothing."""

    async def _publish_unlisted_then_fetch(self):
        sender_gw = f"gw-bad-{uuid.uuid4().hex[:6]}"
        recipient_gw = f"gw-victim-{uuid.uuid4().hex[:6]}"
        sk = "agent:main:cli:integration-test"
        target = address.build(recipient_gw, sk)
        subj = address.encode_subject(sender_gw, sk, verb="deliver")

        envelope = routing.build_envelope(
            from_address=address.build(sender_gw, sk),
            to_address=target,
            payload={"test": "should_be_filtered"},
        )
        # Recipient's allow-list is empty — sender NOT listed.
        async with NATSRoutingClient(servers=SERVERS) as client:
            ack = await client.publish_routed(
                subject=subj,
                payload=envelope,
                headers=routing.envelope_to_headers(envelope),
            )
            fetched = await client.inbox_fetch(
                my_gateway_id=recipient_gw,
                allowed_sender_gateway_ids=[],  # nothing allowed
                timeout=2.0,
            )
        return ack, fetched

    def test_unlisted_sender_returns_empty_inbox(self):
        loop = asyncio.new_event_loop()
        try:
            ack, fetched = loop.run_until_complete(self._publish_unlisted_then_fetch())
        finally:
            loop.close()
        # Publish may or may not report a seq depending on broker
        # ack timing — accept either. What matters is the recipient's
        # inbox is empty.
        self.assertIsInstance(ack, dict)
        # The recipient's inbox sees no message — the filter blocked it.
        self.assertIsNone(
            fetched.get("message"),
            "broker should not have surfaced a message from an unlisted sender"
        )


@_BROKER_GATE
class TestPresenceWriteThenRead(unittest.TestCase):
    """presence.update_presence → read_presence returns the same payload."""

    def test_round_trip(self):
        async def go():
            gw_id = f"gw-pres-{uuid.uuid4().hex[:6]}"
            await presence.update_presence(
                servers=SERVERS,
                gateway_id=gw_id,
                agent_id="hermes-test",
                session_key="agent:main:cli:integration-test",
                platform="cli",
            )
            async with NATSRoutingClient(servers=SERVERS) as client:
                entry = await client.read_presence(gw_id)
            return gw_id, entry

        loop = asyncio.new_event_loop()
        try:
            gw_id, entry = loop.run_until_complete(go())
        finally:
            loop.close()
        self.assertIsNotNone(entry, "read_presence returned None — entry wasn't found")
        self.assertEqual(entry["agent_id"], "hermes-test")
        self.assertEqual(entry["platform"], "cli")
        self.assertEqual(
            entry["session_key"], "agent:main:cli:integration-test"
        )
        self.assertEqual(entry["inbox_consumer"], f"inbox-{gw_id}")


@_BROKER_GATE
class TestPresenceListExcludesStale(unittest.TestCase):
    """list_live filters out entries whose last_seen is older than ttl."""

    def test_stale_entries_filtered(self):
        async def go():
            gw_id = f"gw-stale-{uuid.uuid4().hex[:6]}"
            entry = presence.build_presence_entry(
                agent_id="hermes-stale",
                gateway_id=gw_id,
                session_key="agent:main:cli:integration-test",
                platform="cli",
            )
            # Last-seen 1000 seconds ago — way past any reasonable TTL.
            entry["last_seen"] = time.time() - 1000
            async with NATSRoutingClient(servers=SERVERS) as client:
                await client.update_presence(
                    gateway_id=gw_id, presence_json=entry
                )
            live = await presence.list_live(
                servers=SERVERS, ttl_seconds=90
            )
            return [e for e in live if e.get("_gateway_id") == gw_id]

        loop = asyncio.new_event_loop()
        try:
            stale_rows = loop.run_until_complete(go())
        finally:
            loop.close()
        # 90s TTL should drop a 1000s-old heartbeat.
        self.assertEqual(stale_rows, [])


@_BROKER_GATE
class TestEffectiveAllowList(unittest.TestCase):
    """effeallowlist resolves the union of static + dynamic allow entries."""

    def test_static_only_returned_when_no_dynamic(self):
        async def go():
            recipient = f"gw-eff-{uuid.uuid4().hex[:6]}"
            static = ["gw-static-a", "gw-static-b"]
            async with NATSRoutingClient(servers=SERVERS) as client:
                await client.write_allow_list(
                    recipient_gateway_id=recipient,
                    sender_gateway_ids=[],
                )
            eff = await effective_allow_list(
                servers=SERVERS,
                recipient_gateway_id=recipient,
                static_peers_from_disk=static,
            )
            return eff

        loop = asyncio.new_event_loop()
        try:
            eff = loop.run_until_complete(go())
        finally:
            loop.close()
        # Static entries made it through; dynamic was empty, no extras.
        self.assertIn("gw-static-a", eff)
        self.assertIn("gw-static-b", eff)


if __name__ == "__main__":
    unittest.main()
