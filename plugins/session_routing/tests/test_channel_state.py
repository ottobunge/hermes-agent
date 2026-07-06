"""Offline tests for channel identity, KV-key sanitization, dedupe window,
state transitions, and session-end close semantics (channels.py)."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List
from unittest.mock import patch

from plugins.session_routing.channels import (
    RECENT_MSG_IDS_CAP,
    ChannelState,
    apply_transition,
    build_channel_record,
    channel_id_for,
    channel_id_to_kv_key,
    close_channels_for_session,
    has_seen_msg_id,
    is_valid_transition,
    kv_key_to_channel_id,
    peer_hash,
    record_msg_id,
)

SID = "20260706_174653_5ed43bcb"
PEER = "gw-agent-vm/agent:main:telegram:dm:189562939:39702"


class ChannelIdentity(unittest.TestCase):
    def test_channel_id_shape(self):
        cid = channel_id_for(SID, PEER)
        session_part, _, hash_part = cid.partition(":")
        self.assertEqual(session_part, SID)
        self.assertEqual(len(hash_part), 8)
        self.assertEqual(hash_part, peer_hash(PEER))

    def test_channel_id_is_peer_distinct(self):
        self.assertNotEqual(
            channel_id_for(SID, PEER),
            channel_id_for(SID, "gw-other/some:session"),
        )

    def test_channel_id_is_deterministic(self):
        self.assertEqual(channel_id_for(SID, PEER), channel_id_for(SID, PEER))

    def test_empty_inputs_raise(self):
        with self.assertRaises(ValueError):
            channel_id_for("", PEER)
        with self.assertRaises(ValueError):
            channel_id_for(SID, "")


class KVKeySanitization(unittest.TestCase):
    """NATS KV keys forbid ':' — wire format keeps it, KV key swaps to '.'."""

    def test_colon_replaced(self):
        cid = channel_id_for(SID, PEER)
        kv_key = channel_id_to_kv_key(cid)
        self.assertNotIn(":", kv_key)
        self.assertIn(".", kv_key)

    def test_round_trip(self):
        cid = channel_id_for(SID, PEER)
        self.assertEqual(kv_key_to_channel_id(channel_id_to_kv_key(cid)), cid)

    def test_kv_key_is_nats_legal(self):
        import re
        kv_key = channel_id_to_kv_key(channel_id_for(SID, PEER))
        self.assertTrue(re.fullmatch(r"[-/_=.a-zA-Z0-9]+", kv_key), kv_key)

    def test_key_without_separator_passes_through(self):
        self.assertEqual(kv_key_to_channel_id("nodots"), "nodots")


class DedupeWindow(unittest.TestCase):
    def _record(self):
        return build_channel_record(
            channel_id=channel_id_for(SID, PEER),
            session_id=SID,
            peer_address=PEER,
            state=ChannelState.ESTABLISHED,
        )

    def test_record_and_check(self):
        record = self._record()
        self.assertFalse(has_seen_msg_id(record, "m-1"))
        record_msg_id(record, "m-1")
        self.assertTrue(has_seen_msg_id(record, "m-1"))
        self.assertEqual(record["last_msg_id"], "m-1")

    def test_cap_evicts_oldest(self):
        record = self._record()
        for i in range(RECENT_MSG_IDS_CAP + 1):  # 101 messages
            record_msg_id(record, f"m-{i}")
        self.assertEqual(len(record["recent_msg_ids"]), RECENT_MSG_IDS_CAP)
        self.assertFalse(has_seen_msg_id(record, "m-0"))  # oldest evicted
        self.assertTrue(has_seen_msg_id(record, f"m-{RECENT_MSG_IDS_CAP}"))

    def test_duplicate_id_not_double_counted(self):
        record = self._record()
        record_msg_id(record, "m-1")
        record_msg_id(record, "m-1")
        self.assertEqual(record["recent_msg_ids"].count("m-1"), 1)

    def test_none_record_never_seen(self):
        self.assertFalse(has_seen_msg_id(None, "m-1"))


class Transitions(unittest.TestCase):
    def test_legal_paths(self):
        self.assertTrue(
            is_valid_transition(ChannelState.INITIATING, ChannelState.ESTABLISHED)
        )
        self.assertTrue(
            is_valid_transition(ChannelState.INITIATING, ChannelState.REJECTED)
        )
        self.assertTrue(
            is_valid_transition(ChannelState.INITIATING, ChannelState.CLOSED)
        )
        self.assertTrue(
            is_valid_transition(ChannelState.ESTABLISHED, ChannelState.CLOSED)
        )

    def test_terminal_states_allow_idempotent_self_loop(self):
        self.assertTrue(is_valid_transition(ChannelState.CLOSED, ChannelState.CLOSED))
        self.assertTrue(
            is_valid_transition(ChannelState.REJECTED, ChannelState.REJECTED)
        )

    def test_illegal_paths(self):
        self.assertFalse(
            is_valid_transition(ChannelState.CLOSED, ChannelState.ESTABLISHED)
        )
        self.assertFalse(
            is_valid_transition(ChannelState.REJECTED, ChannelState.ESTABLISHED)
        )
        self.assertFalse(
            is_valid_transition(ChannelState.ESTABLISHED, ChannelState.INITIATING)
        )

    def test_apply_transition_mutates_record(self):
        record = build_channel_record(
            channel_id=channel_id_for(SID, PEER),
            session_id=SID,
            peer_address=PEER,
            state=ChannelState.INITIATING,
        )
        apply_transition(record, ChannelState.ESTABLISHED)
        self.assertEqual(record["state"], "ESTABLISHED")
        with self.assertRaises(ValueError):
            apply_transition(record, ChannelState.INITIATING)


class FakeChannelsClient:
    """In-memory stand-in for NATSRoutingClient channel + publish ops."""

    store: Dict[str, Dict[str, Any]] = {}
    published: List[Dict[str, Any]] = []

    def __init__(self, servers, name="fake"):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def list_channels(self):
        out = []
        for key, record in self.store.items():
            row = dict(record)
            row["_kv_key"] = key
            out.append(row)
        return out

    async def read_channel(self, kv_key):
        record = self.store.get(kv_key)
        return dict(record) if record else None

    async def write_channel(self, *, kv_key, record):
        self.store[kv_key] = dict(record)

    async def publish_routed(self, *, subject, payload, headers=None, timeout=5.0):
        type(self).published.append({"subject": subject, "payload": payload})
        return {"seq": len(self.published), "stream": "SESSIONS"}


class CloseChannelsForSession(unittest.TestCase):
    MY_ADDR = "gw-thinkpad/agent:main:telegram:dm:1:1"

    def setUp(self):
        FakeChannelsClient.store = {}
        FakeChannelsClient.published = []

    def _put(self, session_id, peer, state):
        record = build_channel_record(
            channel_id=channel_id_for(session_id, peer),
            session_id=session_id,
            peer_address=peer,
            state=state,
        )
        FakeChannelsClient.store[
            channel_id_to_kv_key(record["channel_id"])
        ] = record
        return record

    def _close(self, session_id):
        with patch(
            "plugins.session_routing.nats_client.NATSRoutingClient",
            FakeChannelsClient,
        ):
            return asyncio.run(
                close_channels_for_session(
                    servers=["nats://x:4222"],
                    session_id=session_id,
                    my_address=self.MY_ADDR,
                )
            )

    def test_established_channel_fires_bye_and_closes(self):
        record = self._put(SID, PEER, ChannelState.ESTABLISHED)
        closed = self._close(SID)
        self.assertEqual(closed, 1)
        stored = FakeChannelsClient.store[
            channel_id_to_kv_key(record["channel_id"])
        ]
        self.assertEqual(stored["state"], "CLOSED")
        self.assertNotIn("_kv_key", stored)
        self.assertEqual(len(FakeChannelsClient.published), 1)
        published = FakeChannelsClient.published[0]
        self.assertEqual(
            published["payload"]["payload"]["type"], "handshake.bye"
        )
        self.assertTrue(published["subject"].endswith(".handshake"))

    def test_initiating_channel_closes_without_bye(self):
        self._put(SID, PEER, ChannelState.INITIATING)
        closed = self._close(SID)
        self.assertEqual(closed, 1)
        self.assertEqual(FakeChannelsClient.published, [])

    def test_other_sessions_untouched(self):
        other = self._put("20260101_000000_aaaaaaaa", PEER, ChannelState.ESTABLISHED)
        closed = self._close(SID)
        self.assertEqual(closed, 0)
        stored = FakeChannelsClient.store[
            channel_id_to_kv_key(other["channel_id"])
        ]
        self.assertEqual(stored["state"], "ESTABLISHED")

    def test_already_closed_is_noop(self):
        self._put(SID, PEER, ChannelState.CLOSED)
        self.assertEqual(self._close(SID), 0)
        self.assertEqual(FakeChannelsClient.published, [])


if __name__ == "__main__":
    unittest.main()
