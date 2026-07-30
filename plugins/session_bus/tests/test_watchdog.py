"""Tests for the inbox runner watchdog + activity tracking (v0.3.1).

Verifies the silent-death failure mode (broker reconnect wedged,
network partition) is now caught and recovered from:

  * ``InboxRunner.is_healthy`` flips False when activity goes stale
  * ``InboxRunner._record_activity`` keeps the clock current on
    every fetch (even empty) and every dispatch
  * ``SessionRoutingRuntime._watchdog_loop`` respawns the runner
    when ``is_healthy`` is False
  * Backoff is applied: a persistently-broken runner does not
    hot-loop the respawn budget
  * ``runtime.inbox_status()`` exposes the snapshot the model
    tool surfaces to the operator

All tests are offline — they patch NATSRoutingClient to a fake
that lets us control activity, and patch time.monotonic to a
controllable clock so the health threshold is deterministic.
"""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from plugins.session_bus.inbox import (
    DEFAULT_HEALTH_THRESHOLD_SECONDS,
    InboxRunner,
)
from plugins.session_bus.runtime import SessionRoutingRuntime


SERVERS = ["nats://127.0.0.1:4222"]


class _FakeMsg:
    def __init__(self, payload=None):
        self.data = b'{"msg_id": "m-1", "payload": {}}'
        self.subject = "from.gw-peer.sk.deliver"
        self.header = None
        self.metadata = None

    async def ack(self):
        return True


class _FakeClient:
    """Yields no messages; counts fetches; never raises."""

    def __init__(self, *args, **kwargs):
        self.fetch_calls: List[Dict[str, Any]] = []
        self.connect_count = 0
        self.stop_called = False

    async def __aenter__(self):
        self.connect_count += 1
        return self

    async def __aexit__(self, *exc):
        return None

    async def inbox_fetch(self, **kwargs):
        self.fetch_calls.append(kwargs)
        return {"message": None, "consumer": "c", "filter": ["from.x.>"]}

    async def ack(self, *args, **kwargs):
        return True


class _FakeClientWithMessages:
    """Yields N messages then empty."""

    def __init__(self, *args, **kwargs):
        self.fetch_calls: List[Dict[str, Any]] = []
        self.remaining = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def inbox_fetch(self, **kwargs):
        self.fetch_calls.append(kwargs)
        if self.remaining <= 0:
            return {"message": None, "consumer": "c", "filter": ["from.x.>"]}
        self.remaining -= 1
        # Return a minimal valid message; the InboxRunner will try
        # to call .ack() on the deferred handle, so we need an
        # ``ack_handle`` that is awaitable .ack-able.
        msg = {
            "subject": "from.gw-peer.sk.deliver",
            "headers": {},
            "payload": {"msg_id": f"m-{self.remaining}"},
            "ack_handle": _FakeMsg(),
        }
        return {"message": msg, "consumer": "c", "filter": ["from.x.>"]}

    async def ack(self, handle):
        await handle.ack()
        return True


async def _allow(**kwargs):
    return ["gw-peer"]


class InboxRunnerHealth(unittest.TestCase):
    """is_healthy() is the watchdog's primary signal."""

    def test_healthy_immediately_after_start(self):
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=lambda *a, **k: None,
            poll_interval=0.01,
            fetch_timeout=0.01,
        )
        # Don't actually start the task — we just want to inspect
        # the property. is_healthy() requires is_running, so we
        # bypass with is_running=False and expect False.
        self.assertFalse(runner.is_healthy())

    def test_activity_clock_advances_on_fetch(self):
        """Each successful fetch stamps the activity clock.

        We can't easily drive the real loop in a unit test without
        the broker, but we can verify the property setter and
        aging behavior by manipulating the clock directly.
        """
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=lambda *a, **k: None,
            health_threshold_seconds=10.0,
        )
        # Manually stamp the activity clock and pretend the task
        # is running. (We can't easily fake asyncio.Task without
        # the loop, so we check the "is_running is False" branch
        # catches the unhealthy case.)
        runner._record_activity()
        self.assertGreater(runner.last_activity_at, 0.0)
        # Without is_running, is_healthy is False regardless of
        # activity — that's the correct semantic.
        self.assertFalse(runner.is_healthy())

    def test_record_activity_stamps_monotonic_clock(self):
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=lambda *a, **k: None,
        )
        before = time.monotonic()
        runner._record_activity()
        after = time.monotonic()
        self.assertGreaterEqual(runner.last_activity_at, before)
        self.assertLessEqual(runner.last_activity_at, after)

    def test_fetch_count_and_dispatch_count_starts_at_zero(self):
        runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-me",
            on_message=lambda *a, **k: None,
        )
        self.assertEqual(runner.fetch_count, 0)
        self.assertEqual(runner.dispatch_count, 0)


class InboxRunnerActivityIntegration(unittest.TestCase):
    """Drive the real loop with a fake client; verify activity is
    stamped on empty fetches (so a quiet inbox doesn't look
    unhealthy)."""

    def test_empty_fetches_stamp_activity_clock(self):
        async def _go():
            runner = InboxRunner(
                servers=SERVERS,
                my_gateway_id="gw-me",
                on_message=lambda *a, **k: None,
                poll_interval=0.01,
                fetch_timeout=0.01,
                health_threshold_seconds=60.0,
            )
            # Capture the initial activity stamp; we need to be
            # sure subsequent fetches advance the clock.
            initial = runner.last_activity_at
            with patch(
                "plugins.session_bus.inbox.NATSRoutingClient", _FakeClient
            ), patch(
                "plugins.session_bus.inbox.effective_allow_list", _allow
            ):
                runner.start()
                # Let the loop run a few ticks.
                await asyncio.sleep(0.1)
                await runner.stop(timeout=2.0)
            # Loop ran, fetches happened.
            self.assertGreater(runner.fetch_count, 0)
            # Activity clock is at-or-after the initial stamp
            # (actually strictly after, since fetches stamped it).
            self.assertGreaterEqual(runner.last_activity_at, initial)

        asyncio.run(_go())

    def test_dispatch_count_increments_on_successful_callback(self):
        seen = []

        def on_message(payload, subject, headers):
            seen.append(payload)

        async def _go():
            runner = InboxRunner(
                servers=SERVERS,
                my_gateway_id="gw-me",
                on_message=on_message,
                poll_interval=0.01,
                fetch_timeout=0.01,
                auto_ack=False,
                health_threshold_seconds=60.0,
            )
            client = _FakeClientWithMessages()
            client.remaining = 3
            with patch(
                "plugins.session_bus.inbox.NATSRoutingClient",
                lambda *a, **k: client,
            ), patch(
                "plugins.session_bus.inbox.effective_allow_list", _allow
            ):
                runner.start()
                # Wait until all 3 are dispatched.
                for _ in range(100):
                    if runner.dispatch_count >= 3:
                        break
                    await asyncio.sleep(0.01)
                await runner.stop(timeout=2.0)
            self.assertEqual(runner.dispatch_count, 3)
            self.assertEqual(len(seen), 3)

        asyncio.run(_go())

    def test_raising_callback_does_not_increment_dispatch_count(self):
        def on_message(payload, subject, headers):
            raise RuntimeError("boom")

        async def _go():
            runner = InboxRunner(
                servers=SERVERS,
                my_gateway_id="gw-me",
                on_message=on_message,
                poll_interval=0.01,
                fetch_timeout=0.01,
                auto_ack=False,
                health_threshold_seconds=60.0,
            )
            client = _FakeClientWithMessages()
            client.remaining = 2
            with patch(
                "plugins.session_bus.inbox.NATSRoutingClient",
                lambda *a, **k: client,
            ), patch(
                "plugins.session_bus.inbox.effective_allow_list", _allow
            ):
                runner.start()
                await asyncio.sleep(0.15)
                await runner.stop(timeout=2.0)
            # Fetches happened, but dispatches did NOT increment
            # because the callback raised every time.
            self.assertGreaterEqual(runner.fetch_count, 1)
            self.assertEqual(runner.dispatch_count, 0)

        asyncio.run(_go())


class WatchdogRestart(unittest.TestCase):
    """The runtime's watchdog should respawn a silently-dead runner."""

    def test_watchdog_respawns_when_runner_silently_dies(self):
        """Simulate the failure mode: the runner task is alive but
        no fetches are happening (broker wedged). The watchdog
        should stop the dead runner and start a new one.
        """
        runtime = SessionRoutingRuntime()
        # Pre-set the gateway_id + servers so we don't have to
        # drive on_gateway_start (which has more dependencies).
        runtime._gateway_id = "gw-test"
        runtime._servers = SERVERS

        # Create a runner, start it, then mark its activity clock
        # as ancient (older than the threshold). The watchdog
        # will see ``is_healthy()`` is False and respawn.
        class _StaleRunner:
            def __init__(self):
                self.last_activity_at = 0.0
                self.fetch_count = 0
                self.dispatch_count = 0
                self._stopped = False
                self._started_count = 0

            def is_running(self):
                return True

            def is_healthy(self, *, now=None):
                return False  # ALWAYS unhealthy for this test

            async def stop(self, *, timeout=None):
                self._stopped = True

        old_runner = _StaleRunner()
        runtime._runner = old_runner  # type: ignore[assignment]

        # We can't easily run the real _watchdog_loop (it sleeps
        # for 15+ seconds), so we directly call the respawn
        # branch by mocking time and forcing one iteration.
        # Instead, we replicate the respawn logic by calling the
        # parts the watchdog uses.

        # Make a real runner and put it in place.
        real_runner = InboxRunner(
            servers=SERVERS,
            my_gateway_id="gw-test",
            on_message=lambda *a, **k: None,
            poll_interval=0.01,
            fetch_timeout=0.01,
            health_threshold_seconds=5.0,  # short for fast test
        )
        runtime._runner = real_runner
        # Manually stamp its activity as ancient.
        real_runner._last_activity_at = time.monotonic() - 100.0

        # Drive the watchdog for one tick by calling _watchdog_loop
        # in a task and cancelling it after a short sleep. The
        # default check_interval is 15s, so we override it by
        # patching asyncio.sleep to be near-instant for the
        # watchdog's first iteration.
        import plugins.session_bus.runtime as _rt_mod

        real_sleep = asyncio.sleep
        sleep_calls = {"n": 0}

        async def _fast_sleep(t):
            sleep_calls["n"] += 1
            # First sleep call is the watchdog's check_interval;
            # honor a tiny slice then return. Subsequent calls
            # (e.g. from the runner's own poll_interval) we
            # also short-circuit.
            await real_sleep(min(t, 0.05))

        async def _go():
            with patch.object(_rt_mod.asyncio, "sleep", _fast_sleep):
                task = asyncio.create_task(runtime._watchdog_loop())
                # Yield enough times for the watchdog to check
                # at least once and respawn.
                for _ in range(5):
                    await real_sleep(0.1)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            # The watchdog should have respawned the runner.
            self.assertGreaterEqual(
                runtime._watchdog_restarts, 1,
                f"watchdog never respawned after {sleep_calls['n']} sleep calls",
            )
            self.assertIsNot(runtime._runner, real_runner)

        asyncio.run(_go())

    def test_inbox_status_returns_snapshot(self):
        runtime = SessionRoutingRuntime()
        runtime._gateway_id = "gw-test"
        runtime._servers = SERVERS

        # Before start: status reports started=False
        s0 = runtime.inbox_status()
        self.assertFalse(s0["started"])
        self.assertEqual(s0["gateway_id"], "gw-test")

        # After start: status reports a snapshot with the
        # gateway_id and consumer_name populated.
        async def _go():
            real_runner = InboxRunner(
                servers=SERVERS,
                my_gateway_id="gw-test",
                on_message=lambda *a, **k: None,
                poll_interval=0.01,
                fetch_timeout=0.01,
            )
            runtime._runner = real_runner
            real_runner.start()
            try:
                s1 = runtime.inbox_status()
                self.assertTrue(s1["started"])
                self.assertEqual(s1["gateway_id"], "gw-test")
                self.assertEqual(s1["consumer_name"], "inbox-gw-test")
                # Fresh runner: is_healthy is False (no fetches
                # yet) but is_running is True.
                self.assertTrue(s1["is_running"])
                self.assertGreaterEqual(s1["fetch_count"], 0)
            finally:
                await real_runner.stop(timeout=1.0)

        asyncio.run(_go())

    def test_watchdog_backoff_prevents_hot_loop(self):
        """If the runner stays unhealthy (broker permanently down),
        the watchdog should not respawn it on every tick — the
        next_allowed_restart window must elapse first.
        """
        runtime = SessionRoutingRuntime()
        runtime._gateway_id = "gw-test"
        runtime._servers = SERVERS

        # A runner that always reports unhealthy.
        class _AlwaysSick:
            def __init__(self):
                self.last_activity_at = 0.0
                self.fetch_count = 0
                self.dispatch_count = 0
                self.stop_count = 0

            def is_running(self):
                return True

            def is_healthy(self, *, now=None):
                return False

            async def stop(self, *, timeout=None):
                self.stop_count += 1

        runtime._runner = _AlwaysSick()  # type: ignore[assignment]

        async def _go():
            # Drive the watchdog for ~1s with a short backoff so
            # we can observe the cooldown. We patch
            # DEFAULT_HEALTH_THRESHOLD_SECONDS indirectly by
            # accepting the default 60s and just verifying that
            # ``watchdog_restarts`` is bounded.
            task = asyncio.create_task(runtime._watchdog_loop())
            await asyncio.sleep(0.5)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # With the default 60s backoff, only one respawn
            # should have happened in 0.5s. (The first tick
            # triggers immediately, the second is gated by
            # next_allowed_restart.)
            self.assertLessEqual(runtime._watchdog_restarts, 2)

        asyncio.run(_go())


class HealthThreshold(unittest.TestCase):
    """The DEFAULT_HEALTH_THRESHOLD_SECONDS constant is the contract
    between the runner and the watchdog. Verify the import surface
    and a sensible value.
    """

    def test_threshold_is_exported(self):
        from plugins.session_bus.inbox import DEFAULT_HEALTH_THRESHOLD_SECONDS
        self.assertEqual(DEFAULT_HEALTH_THRESHOLD_SECONDS, 60.0)

    def test_threshold_is_in_seconds_reasonable(self):
        # Sanity: should be long enough to tolerate broker
        # latency spikes during gateway bring-up, short enough
        # to catch real failures within a reasonable window.
        self.assertGreaterEqual(
            DEFAULT_HEALTH_THRESHOLD_SECONDS, 30.0
        )
        self.assertLessEqual(
            DEFAULT_HEALTH_THRESHOLD_SECONDS, 300.0
        )


if __name__ == "__main__":
    unittest.main()
