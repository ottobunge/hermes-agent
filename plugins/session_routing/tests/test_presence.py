"""Offline tests for plugins.session_routing.presence.

Just like the nats_client tests, we cover pure-function behavior here. The
integration test under test_integration.py exercises the broker roundtrip.
"""

from __future__ import annotations

import unittest

from plugins.session_routing.presence import (
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
