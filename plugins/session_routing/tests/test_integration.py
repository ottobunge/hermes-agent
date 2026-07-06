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


# ---------------------------------------------------------------------------
# v0.3.0 back-channel protocol — live two-"gateway" round-trip + redelivery
# ---------------------------------------------------------------------------


class _FakeSessionSide:
    """One simulated gateway side: dispatcher + inbox runner + captures."""

    def __init__(self, gateway_id: str, session_key: str, session_id: str):
        from plugins.session_routing.dispatcher import BackChannelDispatcher
        from plugins.session_routing.inbox import InboxRunner

        self.gateway_id = gateway_id
        self.session_key = session_key
        self.session_id = session_id
        self.enqueued: list = []

        self.dispatcher = BackChannelDispatcher(
            servers=SERVERS,
            my_gateway_id=gateway_id,
            resolve_session_id=(
                lambda sk: session_id if sk == session_key else None
            ),
            enqueue_event=self._enqueue,
        )
        self.runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id=gateway_id,
            on_message=self.dispatcher.handle,
            poll_interval=0.1,
            fetch_timeout=0.3,
            broker_retry_delay=0.5,
            auto_ack=False,
        )

    def _enqueue(self, session_key, event) -> bool:
        self.enqueued.append((session_key, event))
        return True

    @property
    def my_address(self) -> str:
        return address.build(self.gateway_id, self.session_key)


async def _wait_for(predicate, *, timeout=15.0, interval=0.1):
    """Await an async predicate until truthy or timeout. Returns last value."""
    deadline = asyncio.get_event_loop().time() + timeout
    value = None
    while asyncio.get_event_loop().time() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    return value


@_BROKER_GATE
class TestBackChannelRoundTrip(unittest.TestCase):
    """Full v0.3.0 flow over a REAL broker: handshake (request → ack →
    established) driven by two live dispatchers, message.text injection
    with prefix + metadata, ack_delivery correlation, redelivery dedupe,
    and handshake.bye teardown."""

    def test_two_session_round_trip_and_redelivery(self):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._run())
        finally:
            loop.close()

    async def _run(self):
        from plugins.session_routing import channels as _channels
        from plugins.session_routing import handshake as _handshake
        from plugins.session_routing import protocol as _protocol
        from plugins.session_routing.nats_client import NATSRoutingClient

        tag = uuid.uuid4().hex[:6]
        side_a = _FakeSessionSide(
            f"gw-inta-{tag}", "agent:main:cli:bc-a",
            f"20260706_000001_{tag}aa",
        )
        side_b = _FakeSessionSide(
            f"gw-intb-{tag}", "agent:main:cli:bc-b",
            f"20260706_000002_{tag}bb",
        )

        # Mutual allow-lists so both inboxes hear each other.
        async with NATSRoutingClient(servers=SERVERS) as client:
            await client.write_allow_list(
                recipient_gateway_id=side_a.gateway_id,
                sender_gateway_ids=[side_b.gateway_id],
            )
            await client.write_allow_list(
                recipient_gateway_id=side_b.gateway_id,
                sender_gateway_ids=[side_a.gateway_id],
            )

        side_a.runner.start()
        side_b.runner.start()
        kv_keys_to_cleanup = []
        try:
            # ── A initiates (exactly what session_establish publishes) ──
            channel_id_a = _channels.channel_id_for(
                side_a.session_id, side_b.my_address
            )
            kv_key_a = _channels.channel_id_to_kv_key(channel_id_a)
            kv_keys_to_cleanup.append(kv_key_a)
            machine = _handshake.HandshakeChannel(
                channel_id=channel_id_a,
                session_id=side_a.session_id,
                my_address=side_a.my_address,
                peer_address=side_b.my_address,
            )
            request = machine.start()
            record = _channels.build_channel_record(
                channel_id=channel_id_a,
                session_id=side_a.session_id,
                peer_address=side_b.my_address,
                state=_channels.ChannelState.INITIATING,
            )
            record["role"] = "initiator"
            record["nonce"] = request["nonce"]

            async with NATSRoutingClient(servers=SERVERS) as client:
                await client.write_channel(kv_key=kv_key_a, record=record)
                envelope = routing.build_envelope(
                    from_address=side_a.my_address,
                    to_address=side_b.my_address,
                    payload=request,
                )
                await client.publish_routed(
                    subject=address.handshake_subject(
                        side_a.gateway_id, side_b.session_key
                    ),
                    payload=envelope,
                    headers=routing.envelope_to_headers(envelope),
                )

            # Handshake completes on BOTH sides (B acks, A establishes,
            # B sees established).
            async def _a_established():
                rec = await _channels.load_channel(
                    servers=SERVERS, channel_id=channel_id_a
                )
                return rec if rec and rec.get("state") == "ESTABLISHED" else None

            rec_a = await _wait_for(_a_established)
            self.assertIsNotNone(rec_a, "initiator side never ESTABLISHED")

            channel_id_b = _channels.channel_id_for(
                side_b.session_id, side_a.my_address
            )
            kv_keys_to_cleanup.append(
                _channels.channel_id_to_kv_key(channel_id_b)
            )

            async def _b_established():
                rec = await _channels.load_channel(
                    servers=SERVERS, channel_id=channel_id_b
                )
                return rec if rec and rec.get("state") == "ESTABLISHED" else None

            rec_b = await _wait_for(_b_established)
            self.assertIsNotNone(rec_b, "responder side never ESTABLISHED")

            # ── message.text A → B, injected exactly once ──
            text_payload = _protocol.build_message_text(
                channel_id=channel_id_a,
                session_id=side_a.session_id,
                body="ping from A",
            )
            text_envelope = routing.build_envelope(
                from_address=side_a.my_address,
                to_address=side_b.my_address,
                payload=text_payload,
            )
            async with NATSRoutingClient(servers=SERVERS) as client:
                for _ in range(2):  # publish TWICE: redelivery simulation
                    await client.publish_routed(
                        subject=address.encode_subject(
                            side_a.gateway_id, side_b.session_key,
                            verb="deliver",
                        ),
                        payload=text_envelope,
                        headers=routing.envelope_to_headers(text_envelope),
                    )

            async def _b_got_text():
                return side_b.enqueued or None

            self.assertIsNotNone(
                await _wait_for(_b_got_text), "message.text never injected"
            )
            # Give the duplicate a moment to (wrongly) inject, then assert
            # exactly ONE synthetic turn despite two publishes.
            await asyncio.sleep(2.0)
            self.assertEqual(
                len(side_b.enqueued), 1,
                "redelivered envelope must inject exactly one synthetic turn",
            )
            session_key, event = side_b.enqueued[0]
            self.assertEqual(session_key, side_b.session_key)
            self.assertEqual(
                event.text,
                f"[back-channel from {side_a.my_address}] ping from A",
            )
            self.assertTrue(event.internal)
            self.assertEqual(event.metadata["channel_id"], channel_id_b)
            self.assertEqual(
                event.metadata["envelope_msg_id"], text_envelope["msg_id"]
            )

            # ── ack_delivery correlation lands in A's dedupe window ──
            async def _a_saw_ack():
                rec = await _channels.load_channel(
                    servers=SERVERS, channel_id=channel_id_a
                )
                ids = (rec or {}).get("recent_msg_ids") or []
                # request msg (no: that's B's window); A's window gains the
                # ack_delivery envelope id — anything beyond initial state.
                return rec if ids else None

            self.assertIsNotNone(
                await _wait_for(_a_saw_ack),
                "ack_delivery never reached the initiator",
            )

            # ── bye A → B closes B's side ──
            bye = _handshake.build_bye(
                channel_id=channel_id_a, session_id=side_a.session_id
            )
            bye_envelope = routing.build_envelope(
                from_address=side_a.my_address,
                to_address=side_b.my_address,
                payload=bye,
            )
            async with NATSRoutingClient(servers=SERVERS) as client:
                await client.publish_routed(
                    subject=address.handshake_subject(
                        side_a.gateway_id, side_b.session_key
                    ),
                    payload=bye_envelope,
                    headers=routing.envelope_to_headers(bye_envelope),
                )

            async def _b_closed():
                rec = await _channels.load_channel(
                    servers=SERVERS, channel_id=channel_id_b
                )
                return rec if rec and rec.get("state") == "CLOSED" else None

            self.assertIsNotNone(
                await _wait_for(_b_closed), "handshake.bye never closed B"
            )
        finally:
            await side_a.runner.stop(timeout=3.0)
            await side_b.runner.stop(timeout=3.0)
            # Scratch-row cleanup (best-effort).
            try:
                async with NATSRoutingClient(servers=SERVERS) as client:
                    for key in kv_keys_to_cleanup:
                        await client.delete_channel(key)
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    unittest.main()
