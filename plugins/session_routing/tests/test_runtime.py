"""Offline tests for the gateway-lifecycle runtime (runtime.py).

A fake gateway + patched InboxRunner/presence keep everything offline;
the live-broker path is covered by test_integration.py.
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from plugins.session_routing.runtime import (
    SessionRoutingRuntime,
    broker_configured,
    broker_servers,
)


class FakeEntry:
    def __init__(self, session_key, session_id):
        self.session_key = session_key
        self.session_id = session_id


class FakeSessionStore:
    def __init__(self, entries: Optional[List[FakeEntry]] = None):
        self.entries = entries or []

    def list_sessions(self, active_minutes=None):
        return list(self.entries)

    def get_entry(self, session_key):
        for e in self.entries:
            if e.session_key == session_key:
                return e
        return None

    def lookup_by_session_id(self, session_id):
        for e in self.entries:
            if e.session_id == session_id:
                return e
        return None


class FakeGateway:
    def __init__(self, entries=None):
        self.session_store = FakeSessionStore(entries)
        self.enqueued: List[Any] = []

    def enqueue_internal_session_event(self, session_key, event):
        self.enqueued.append((session_key, event))
        return True


class FakeInboxRunner:
    instances: List["FakeInboxRunner"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        FakeInboxRunner.instances.append(self)

    def start(self):
        self.started = True

    async def stop(self, *, timeout=None):
        self.stopped = True
        self.started = False

    @property
    def is_running(self):
        return self.started


class BrokerGates(unittest.TestCase):
    def test_not_configured_without_env(self):
        with patch.dict("os.environ", {}, clear=False) as env:
            for key in ("HERMES_NATS_URLS", "NATS_URLS"):
                env.pop(key, None)
            self.assertFalse(broker_configured())

    def test_configured_with_env(self):
        with patch.dict(
            "os.environ", {"HERMES_NATS_URLS": "nats://10.8.0.8:4222"}
        ):
            self.assertTrue(broker_configured())
            self.assertEqual(broker_servers(), ["nats://10.8.0.8:4222"])

    def test_multiple_servers_split(self):
        with patch.dict(
            "os.environ",
            {"HERMES_NATS_URLS": "nats://a:4222, nats://b:4222"},
        ):
            self.assertEqual(
                broker_servers(), ["nats://a:4222", "nats://b:4222"]
            )


class StartStop(unittest.TestCase):
    def setUp(self):
        FakeInboxRunner.instances = []
        self.heartbeats: List[Dict[str, Any]] = []

    def _patches(self):
        async def fake_update_presence(**kwargs):
            self.heartbeats.append(kwargs)

        return (
            patch(
                "plugins.session_routing.runtime.InboxRunner", FakeInboxRunner
            ),
            patch(
                "plugins.session_routing.presence.update_presence",
                fake_update_presence,
            ),
            patch.dict(
                "os.environ",
                {
                    "HERMES_NATS_URLS": "nats://127.0.0.1:4222",
                    "HERMES_GATEWAY_ID": "gw-test",
                },
            ),
        )

    def test_start_wires_inbox_and_heartbeat_then_stop_unwinds(self):
        runtime = SessionRoutingRuntime()
        gateway = FakeGateway(
            entries=[FakeEntry("agent:main:telegram:dm:1:1", "sid-1")]
        )

        async def _go():
            p1, p2, p3 = self._patches()
            with p1, p2, p3:
                awaitable = runtime.on_gateway_start(gateway=gateway)
                self.assertIsNotNone(awaitable)
                await awaitable
                # Inbox runner started in deferred-ack mode.
                runner = FakeInboxRunner.instances[0]
                self.assertTrue(runner.started)
                self.assertFalse(runner.kwargs["auto_ack"])
                self.assertEqual(runner.kwargs["my_gateway_id"], "gw-test")
                # Heartbeat task ticks at least once with live_sessions.
                for _ in range(50):
                    if self.heartbeats:
                        break
                    await asyncio.sleep(0.01)
                self.assertTrue(self.heartbeats)
                beat = self.heartbeats[0]
                self.assertEqual(beat["gateway_id"], "gw-test")
                self.assertEqual(
                    beat["live_sessions"], ["agent:main:telegram:dm:1:1"]
                )

                stop_awaitable = runtime.on_gateway_stop(gateway=gateway)
                self.assertIsNotNone(stop_awaitable)
                await stop_awaitable
                self.assertTrue(runner.stopped)
                self.assertIsNone(runtime._heartbeat_task)

        asyncio.run(_go())

    def test_start_skipped_without_broker_env(self):
        runtime = SessionRoutingRuntime()
        gateway = FakeGateway()
        with patch.dict("os.environ", {}, clear=False) as env:
            for key in ("HERMES_NATS_URLS", "NATS_URLS"):
                env.pop(key, None)
            self.assertIsNone(runtime.on_gateway_start(gateway=gateway))
        self.assertEqual(FakeInboxRunner.instances, [])

    def test_start_without_gateway_kwarg_is_noop(self):
        runtime = SessionRoutingRuntime()
        self.assertIsNone(runtime.on_gateway_start())

    def test_stop_before_start_is_noop(self):
        runtime = SessionRoutingRuntime()
        self.assertIsNone(runtime.on_gateway_stop())

    def test_double_start_is_idempotent(self):
        runtime = SessionRoutingRuntime()
        gateway = FakeGateway()

        async def _go():
            p1, p2, p3 = self._patches()
            with p1, p2, p3:
                await runtime.on_gateway_start(gateway=gateway)
                second = runtime.on_gateway_start(gateway=gateway)
                if second is not None:
                    await second
                self.assertEqual(len(FakeInboxRunner.instances), 1)
                await runtime.on_gateway_stop()

        asyncio.run(_go())


class Collaborators(unittest.TestCase):
    def test_resolve_session_id(self):
        runtime = SessionRoutingRuntime()
        runtime._gateway = FakeGateway(
            entries=[FakeEntry("sk-1", "sid-1")]
        )
        self.assertEqual(runtime._resolve_session_id("sk-1"), "sid-1")
        self.assertIsNone(runtime._resolve_session_id("sk-404"))

    def test_enqueue_event_delegates_to_gateway(self):
        runtime = SessionRoutingRuntime()
        gateway = FakeGateway()
        runtime._gateway = gateway
        self.assertTrue(runtime._enqueue_event("sk-1", object()))
        self.assertEqual(len(gateway.enqueued), 1)

    def test_enqueue_event_swallows_gateway_errors(self):
        class ExplodingGateway:
            def enqueue_internal_session_event(self, sk, event):
                raise RuntimeError("gateway on fire")

        runtime = SessionRoutingRuntime()
        runtime._gateway = ExplodingGateway()
        self.assertFalse(runtime._enqueue_event("sk-1", object()))


class SessionFinalize(unittest.TestCase):
    def test_finalize_closes_channels_for_session(self):
        runtime = SessionRoutingRuntime()
        runtime._gateway = FakeGateway(
            entries=[FakeEntry("agent:main:telegram:dm:1:1", "sid-1")]
        )
        runtime._gateway_id = "gw-test"
        runtime._servers = ["nats://127.0.0.1:4222"]
        closed_calls: List[Dict[str, Any]] = []

        async def fake_close(**kwargs):
            closed_calls.append(kwargs)
            return 2

        async def _go():
            with patch(
                "plugins.session_routing.channels.close_channels_for_session",
                fake_close,
            ), patch.dict(
                "os.environ", {"HERMES_NATS_URLS": "nats://127.0.0.1:4222"}
            ):
                runtime.on_session_finalize(session_id="sid-1")
                # Wait for the scheduled cleanup task.
                for _ in range(50):
                    if closed_calls:
                        break
                    await asyncio.sleep(0.01)

        asyncio.run(_go())
        self.assertEqual(len(closed_calls), 1)
        self.assertEqual(closed_calls[0]["session_id"], "sid-1")
        self.assertEqual(
            closed_calls[0]["my_address"],
            "gw-test/agent:main:telegram:dm:1:1",
        )

    def test_finalize_without_session_id_is_noop(self):
        runtime = SessionRoutingRuntime()
        runtime._gateway = FakeGateway()
        runtime.on_session_finalize(session_id=None)  # must not raise

    def test_finalize_before_start_is_noop(self):
        runtime = SessionRoutingRuntime()
        runtime.on_session_finalize(session_id="sid-1")  # no gateway yet


if __name__ == "__main__":
    unittest.main()
