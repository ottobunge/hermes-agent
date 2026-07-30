"""Offline tests for plugins.session_bus.presence.

Just like the nats_client tests, we cover pure-function behavior here. The
integration test under test_integration.py exercises the broker roundtrip.
"""

from __future__ import annotations

import unittest

from plugins.session_bus.presence import (
    advertising_address,
    build_presence_entry,
    inbox_consumer_name,
)


class PresenceEntry(unittest.TestCase):
    def test_basic_shape(self):
        e = build_presence_entry(
            agent_id="hermes-conrad",
            gateway_id="gw-thinkpad",
            session_key="agent:main:telegram:dm:189562939:39702",
            platform="telegram",
        )
        self.assertEqual(e["agent_id"], "hermes-conrad")
        self.assertEqual(e["gateway_id"], "gw-thinkpad")
        self.assertEqual(e["session_key"], "agent:main:telegram:dm:189562939:39702")
        self.assertEqual(e["platform"], "telegram")
        # last_seen is a float within the last few seconds
        import time
        self.assertAlmostEqual(e["last_seen"], time.time(), delta=2.0)
        # ISO string parses back to a tz-aware UTC instant
        from datetime import datetime
        ts = datetime.fromisoformat(e["last_seen_iso"].replace("Z", "+00:00"))
        self.assertEqual(ts.utcoffset().total_seconds(), 0.0)

    def test_platform_empty_when_none(self):
        e = build_presence_entry(
            agent_id="x", gateway_id="gw-x",
            session_key="agent:main:foo", platform=None,
        )
        self.assertEqual(e["platform"], "")

    def test_extra_merged(self):
        e = build_presence_entry(
            agent_id="x", gateway_id="gw-x",
            session_key="agent:main:foo", platform="cli",
            extra={"models": ["m3"], "version": "0.18.0"},
        )
        self.assertEqual(e["models"], ["m3"])
        self.assertEqual(e["version"], "0.18.0")

    def test_live_sessions_listed_and_session_key_backfilled(self):
        sessions = [
            "agent:main:telegram:dm:1:1",
            "agent:main:telegram:dm:2:2",
        ]
        e = build_presence_entry(
            agent_id="x", gateway_id="gw-x",
            session_key="", platform="telegram",
            live_sessions=sessions,
        )
        self.assertEqual(e["live_sessions"], sessions)
        # Backward compat: single-key readers see the first live session.
        self.assertEqual(e["session_key"], sessions[0])

    def test_legacy_single_session_key_becomes_live_sessions(self):
        e = build_presence_entry(
            agent_id="x", gateway_id="gw-x",
            session_key="agent:main:foo", platform="cli",
        )
        self.assertEqual(e["live_sessions"], ["agent:main:foo"])

    def test_empty_everything_yields_empty_list(self):
        e = build_presence_entry(
            agent_id="x", gateway_id="gw-x",
            session_key="", platform=None,
        )
        self.assertEqual(e["live_sessions"], [])
        self.assertEqual(e["session_key"], "")


class ResolveTargetMatching(unittest.TestCase):
    """resolve_target must match any element of live_sessions (v0.3.0)."""

    def _resolve(self, entries, address):
        import asyncio
        from unittest.mock import patch
        from plugins.session_bus import presence as presence_mod

        async def fake_list_live(**kwargs):
            gwf = kwargs.get("gateway_id_filter")
            return [
                e for e in entries
                if not gwf or e.get("_gateway_id") == gwf
            ]

        with patch.object(presence_mod, "list_live", fake_list_live):
            return asyncio.run(
                presence_mod.resolve_target(
                    servers=["nats://x:4222"], address=address, ttl_seconds=90,
                )
            )

    def test_matches_any_live_session(self):
        entry = {
            "_gateway_id": "gw-vm",
            "gateway_id": "gw-vm",
            "session_key": "agent:main:telegram:dm:1:1",
            "live_sessions": [
                "agent:main:telegram:dm:1:1",
                "agent:main:telegram:dm:9:9",
            ],
            "last_seen": 0,
        }
        found = self._resolve([entry], "gw-vm/agent:main:telegram:dm:9:9")
        self.assertIsNotNone(found)
        self.assertEqual(found["_gateway_id"], "gw-vm")

    def test_legacy_entry_without_live_sessions_still_matches(self):
        entry = {
            "_gateway_id": "gw-vm",
            "session_key": "agent:main:telegram:dm:1:1",
            "last_seen": 0,
        }
        found = self._resolve([entry], "gw-vm/agent:main:telegram:dm:1:1")
        self.assertIsNotNone(found)

    def test_unknown_session_returns_none(self):
        entry = {
            "_gateway_id": "gw-vm",
            "session_key": "agent:main:telegram:dm:1:1",
            "live_sessions": ["agent:main:telegram:dm:1:1"],
            "last_seen": 0,
        }
        self.assertIsNone(
            self._resolve([entry], "gw-vm/agent:main:telegram:dm:404:404")
        )


class InboxConsumerName(unittest.TestCase):
    def test_stable_across_calls(self):
        self.assertEqual(inbox_consumer_name("gw-thinkpad"), "inbox-gw-thinkpad")
        # Re-call yields the same — durability by name.
        self.assertEqual(inbox_consumer_name("gw-thinkpad"), "inbox-gw-thinkpad")

    def test_distinct_gateways_distinct_names(self):
        self.assertNotEqual(
            inbox_consumer_name("gw-thinkpad"),
            inbox_consumer_name("gw-agent-vm"),
        )


class AdvertisingAddress(unittest.TestCase):
    def test_shape(self):
        addr = advertising_address(
            gateway_id="gw-thinkpad",
            session_key="agent:main:telegram:dm:189562939:39702",
        )
        self.assertEqual(
            addr,
            "gw-thinkpad/agent:main:telegram:dm:189562939:39702",
        )


if __name__ == "__main__":
    unittest.main()
