"""Offline tests for InboxRunner's deferred-ack (auto_ack=False) mode.

Fakes the NATS client at the module seam so no broker is needed. The
contract under test (bug fix #1 from the v0.3.0 plan):

  * auto_ack=False is passed through to inbox_fetch
  * the broker ack fires AFTER the callback returns successfully
  * a raising callback suppresses the ack (message left for redelivery)
  * legacy auto_ack=True behavior unchanged (no client.ack call)
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List
from unittest.mock import patch

from plugins.session_bus.inbox import InboxRunner

SERVERS = ["nats://127.0.0.1:4222"]


class FakeAckHandle:
    def __init__(self):
        self.acked = False

    async def ack(self):
        self.acked = True


class FakeRoutingClient:
    """Yields each queued message once, then reports empty."""

    instances: List["FakeRoutingClient"] = []

    def __init__(self, servers, name="fake"):
        self.servers = servers
        self.fetch_calls: List[Dict[str, Any]] = []
        self.ack_calls: List[Any] = []
        FakeRoutingClient.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def inbox_fetch(self, **kwargs):
        self.fetch_calls.append(kwargs)
        queue = type(self)._queue
        if not queue:
            return {"message": None, "consumer": "c", "filter": None}
        message = queue.pop(0)
        if not kwargs.get("auto_ack", True):
            message.setdefault("ack_handle", FakeAckHandle())
        return {"message": message, "consumer": "c", "filter": ["from.x.>"]}

    async def ack(self, handle):
        self.ack_calls.append(handle)
        await handle.ack()
        return True

    _queue: List[Dict[str, Any]] = []


def _msg(payload=None):
    return {
        "subject": "from.gw-peer.sk.deliver",
        "headers": {},
        "payload": payload or {"msg_id": "m-1"},
    }


async def _allow(**kwargs):
    return ["gw-peer"]


class DeferredAckRunner(unittest.TestCase):
    def setUp(self):
        FakeRoutingClient.instances = []
        FakeRoutingClient._queue = []

    def _run_until_drained(self, runner: InboxRunner, *, timeout=5.0):
        async def _go():
            runner.start()
            deadline = asyncio.get_event_loop().time() + timeout
            while FakeRoutingClient._queue:
                if asyncio.get_event_loop().time() > deadline:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)  # let dispatch/ack settle
            await runner.stop(timeout=2.0)

        asyncio.run(_go())

    def _patched(self):
        return (
            patch(
                "plugins.session_bus.inbox.NATSRoutingClient",
                FakeRoutingClient,
            ),
            patch(
                "plugins.session_bus.inbox.effective_allow_list",
                _allow,
            ),
        )

    def test_auto_ack_false_acks_after_successful_callback(self):
        seen = []

        def on_message(payload, subject, headers):
            seen.append(payload)

        FakeRoutingClient._queue = [_msg()]
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=on_message,
            poll_interval=0.01,
            fetch_timeout=0.01,
            auto_ack=False,
        )
        p1, p2 = self._patched()
        with p1, p2:
            self._run_until_drained(runner)

        self.assertEqual(len(seen), 1)
        client = FakeRoutingClient.instances[0]
        self.assertEqual(len(client.ack_calls), 1)
        self.assertTrue(client.ack_calls[0].acked)
        # auto_ack must have been threaded through to inbox_fetch
        self.assertFalse(client.fetch_calls[0]["auto_ack"])

    def test_raising_callback_suppresses_ack(self):
        def on_message(payload, subject, headers):
            raise RuntimeError("handler crashed")

        FakeRoutingClient._queue = [_msg()]
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=on_message,
            poll_interval=0.01,
            fetch_timeout=0.01,
            auto_ack=False,
        )
        p1, p2 = self._patched()
        with p1, p2:
            self._run_until_drained(runner)

        client = FakeRoutingClient.instances[0]
        self.assertEqual(client.ack_calls, [])  # left for redelivery

    def test_auto_ack_true_never_calls_client_ack(self):
        seen = []

        def on_message(payload, subject, headers):
            seen.append(payload)

        FakeRoutingClient._queue = [_msg()]
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=on_message,
            poll_interval=0.01,
            fetch_timeout=0.01,
        )
        p1, p2 = self._patched()
        with p1, p2:
            self._run_until_drained(runner)

        self.assertEqual(len(seen), 1)
        client = FakeRoutingClient.instances[0]
        self.assertEqual(client.ack_calls, [])
        self.assertTrue(client.fetch_calls[0]["auto_ack"])

    def test_dispatch_liveness_stamps_set_on_successful_dispatch(self):
        # last_message_at / last_dispatch_at differentiate "consumer
        # alive but idle" from "alive and delivering" — last_activity_at
        # alone cannot (it stamps on empty fetches for the watchdog).
        def on_message(payload, subject, headers):
            pass

        FakeRoutingClient._queue = [_msg()]
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=on_message,
            poll_interval=0.01,
            fetch_timeout=0.01,
            auto_ack=False,
        )
        self.assertIsNone(runner.last_message_at)
        self.assertIsNone(runner.last_dispatch_at)
        p1, p2 = self._patched()
        with p1, p2:
            self._run_until_drained(runner)

        self.assertIsNotNone(runner.last_message_at)
        self.assertIsNotNone(runner.last_dispatch_at)

    def test_raising_callback_stamps_message_but_not_dispatch(self):
        def on_message(payload, subject, headers):
            raise RuntimeError("handler crashed")

        FakeRoutingClient._queue = [_msg()]
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=on_message,
            poll_interval=0.01,
            fetch_timeout=0.01,
            auto_ack=False,
        )
        p1, p2 = self._patched()
        with p1, p2:
            self._run_until_drained(runner)

        self.assertIsNotNone(runner.last_message_at)   # message was pulled
        self.assertIsNone(runner.last_dispatch_at)     # but never delivered


if __name__ == "__main__":
    unittest.main()
