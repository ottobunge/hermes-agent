"""Gateway-side recipient consumer for session-routing.

This module owns the long-lived async loop that pulls addressed
messages out of the broker on behalf of a single gateway. It is the
*consumer half* of the protocol — the publisher half lives in
``plugins/session_routing/tools.py`` (``handle_session_route_send``).

Design contract:

  - The runner is the only thing that should be calling
    ``NATSRoutingClient.inbox_fetch()`` from a long-lived task. Tools
    use the same client but with a fresh per-call client, so they do
    not interfere with the durable cursor the runner owns.

  - The allow-list is re-read every empty iteration. Operators who
    ``hermes routing allow-add`` a new peer see the effect on the next
    poll cycle (no gateway restart).

  - Broker failures DO NOT crash the gateway. The runner logs a
    warning, sleeps, and retries. The message stays in the JetStream
    stream and will be redelivered once the broker is back.

  - The runner does NOT touch any Hermes tool surface — it just
    invokes the callback the plugin's ``__init__.py`` wires. This
    keeps it unit-testable with a plain ``asyncio.Queue`` callback and
    keeps the dependency direction one-way (inbox → callback, never
    callback → inbox).

  - Activity tracking: ``self.last_activity_at`` is set on every
    successful fetch AND on every dispatched message. Operators
    (and the runtime watchdog) use ``is_healthy()`` to detect
    silent-death: a runner whose ``last_activity_at`` is older than
    the threshold is considered dead even if the task object is
    non-None. This catches the failure mode where the consumer task
    is "running" but no longer pulling (broker reconnection wedged,
    network partition, etc.). Without this, a dead runner would
    look healthy to ``is_running`` until gateway restart.

Versioning: v0.1.0 (Phase 1), watchdog + activity tracking in v0.3.1.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from plugins.session_bus.allow import effective_allow_list
from plugins.session_bus.nats_client import (
    NATSRoutingClient,
    NATSRoutingUnreachable,
)

logger = logging.getLogger(__name__)

# Defaults mirror session-bridge's "fail fast, log loud, keep going" stance.
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_EMPTY_BACKOFF_MAX = 30.0
DEFAULT_BROKER_RETRY_DELAY = 5.0
DEFAULT_FETCH_TIMEOUT = 1.0

# Watchdog threshold: a runner whose last_activity_at is older than this
# is considered silently-dead even if the task object is non-None. Set
# high enough to tolerate legitimate broker latency spikes during gateway
# bring-up; set low enough to catch real failures quickly.
DEFAULT_HEALTH_THRESHOLD_SECONDS = 60.0


# A callback receives the parsed envelope, the broker subject, and the
# raw headers. Returning a coroutine is supported — the runner awaits
# it before pulling the next message, so a slow handler naturally
# backpressures the consumer.
OnMessageCallback = Callable[
    [Dict[str, Any], str, Dict[str, str]],
    Optional[Awaitable[None]],
]


class InboxRunner:
    """Long-lived async loop that drains the recipient's inbox.

    The loop is intentionally simple: ``fetch one, hand off, repeat``.
    Operators who want fan-out or batching can swap in a different
    runner; the contract (callback per envelope) stays the same.
    """

    def __init__(
        self,
        *,
        servers: List[str],
        my_gateway_id: str,
        on_message: OnMessageCallback,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        empty_backoff_max: float = DEFAULT_EMPTY_BACKOFF_MAX,
        broker_retry_delay: float = DEFAULT_BROKER_RETRY_DELAY,
        fetch_timeout: float = DEFAULT_FETCH_TIMEOUT,
        health_threshold_seconds: float = DEFAULT_HEALTH_THRESHOLD_SECONDS,
        auto_ack: bool = True,
    ) -> None:
        if not servers:
            raise ValueError("InboxRunner requires at least one server URL.")
        if not my_gateway_id:
            raise ValueError("InboxRunner requires a non-empty my_gateway_id.")
        if on_message is None:
            raise ValueError("InboxRunner requires an on_message callback.")

        self._servers = servers
        self._my_gateway_id = my_gateway_id
        self._on_message = on_message
        self._poll_interval = poll_interval
        self._empty_backoff_max = empty_backoff_max
        self._broker_retry_delay = broker_retry_delay
        self._fetch_timeout = fetch_timeout
        # Deferred-ack mode (auto_ack=False): the broker message is acked
        # only AFTER the callback returns without raising, so a callback
        # crash leaves the message in the stream for redelivery instead of
        # silently losing it. The receive-side dispatcher runs in this
        # mode ("dedupe before NATS ack" — its persisted dedupe window
        # makes the redelivery idempotent).
        self._auto_ack = auto_ack

        # Activity tracking — public read-only view of "is the consumer
        # actually pulling and dispatching". Both fields are wall-clock
        # seconds (time.monotonic equivalent via time.time). Set on every
        # successful fetch AND on every dispatched message. Watchdog
        # consults ``last_activity_at`` to detect silent death.
        self._last_activity_at: float = time.monotonic()
        # Dispatch-liveness, tracked separately from loop-liveness:
        # ``last_activity_at`` stamps on EVERY successful fetch (including
        # empty ones — that is correct for the watchdog: the loop is
        # alive), so on its own it cannot distinguish "consumer alive but
        # idle" from "consumer alive and delivering". These two stamps
        # stay None until the corresponding event has happened at least
        # once, so the status tool can expose the difference.
        self._last_message_at: Optional[float] = None   # non-empty fetch
        self._last_dispatch_at: Optional[float] = None  # callback returned OK
        self._fetch_count: int = 0
        self._dispatch_count: int = 0
        self._health_threshold = health_threshold_seconds

        # Lifecycle
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._current_allow: List[str] = []

    # ------------------------------------------------------------------
    # Activity tracking (public surface for watchdog + status tool)
    # ------------------------------------------------------------------

    @property
    def last_activity_at(self) -> float:
        """Monotonic timestamp of the most recent successful fetch or
        dispatch. Used by the watchdog to detect silent death."""
        return self._last_activity_at

    @property
    def last_message_at(self) -> Optional[float]:
        """Monotonic timestamp of the most recent NON-EMPTY fetch, or
        None if this runner has never pulled a message. Unlike
        ``last_activity_at`` this does not advance on empty polls."""
        return self._last_message_at

    @property
    def last_dispatch_at(self) -> Optional[float]:
        """Monotonic timestamp of the most recent successful dispatch
        (callback returned without raising), or None if never."""
        return self._last_dispatch_at

    @property
    def fetch_count(self) -> int:
        """Number of successful broker fetches since this runner was
        started. Useful as a health sanity-check — a runner that has
        never fetched is either freshly-started or wedged."""
        return self._fetch_count

    @property
    def dispatch_count(self) -> int:
        """Number of messages successfully dispatched (callback
        returned without raising) since this runner was started."""
        return self._dispatch_count

    def is_healthy(self, *, now: Optional[float] = None) -> bool:
        """Return True iff the runner has shown activity recently.

        "Recently" = within ``health_threshold_seconds`` of ``now`` (or
        the current monotonic clock if ``now`` is omitted).

        This is the watchdog's primary signal. A runner whose
        ``is_running`` is True (task object alive) but whose
        ``last_activity_at`` is stale is a silent-death case: the
        task is "running" but not actually doing anything useful
        (broker reconnection wedged, network partition, etc.).
        """
        if not self.is_running:
            return False
        current = now if now is not None else time.monotonic()
        age = current - self._last_activity_at
        return age <= self._health_threshold

    def _record_activity(self) -> None:
        """Stamp the activity clock. Called on every successful fetch
        and on every successful dispatch."""
        self._last_activity_at = time.monotonic()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn the consumer task. Idempotent — second call is a no-op."""
        if self._task is not None and not self._task.done():
            logger.debug(
                "session_routing inbox: start() called while already running — no-op"
            )
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run(), name=f"session-routing-inbox-{self._my_gateway_id}"
        )
        # Initial activity stamp so a freshly-started runner doesn't
        # immediately look unhealthy while it's still bootstrapping.
        self._record_activity()
        logger.info(
            "session_routing inbox: started for %s (servers=%s)",
            self._my_gateway_id,
            self._servers,
        )

    async def stop(self, *, timeout: Optional[float] = 10.0) -> None:
        """Signal stop and await the task. Idempotent."""
        if self._task is None:
            return
        if self._task.done():
            self._task = None
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "session_routing inbox: stop timed out after %ss — cancelling",
                timeout,
            )
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        finally:
            self._task = None
        logger.info("session_routing inbox: stopped for %s", self._my_gateway_id)

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ------------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        """Main loop. Never raises — logs and continues on broker error."""
        empty_delay = self._poll_interval
        while not self._stop_event.is_set():
            try:
                allowed = await effective_allow_list(
                    servers=self._servers,
                    recipient_gateway_id=self._my_gateway_id,
                )
                self._current_allow = list(allowed)
            except NATSRoutingUnreachable as e:
                logger.warning(
                    "session_routing inbox: allow-list read failed (%s) — retry in %ss",
                    e,
                    self._broker_retry_delay,
                )
                await self._sleep_or_stop(self._broker_retry_delay)
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "session_routing inbox: allow-list read unexpected error: %r — retry in %ss",
                    e,
                    self._broker_retry_delay,
                )
                await self._sleep_or_stop(self._broker_retry_delay)
                continue

            try:
                async with NATSRoutingClient(servers=self._servers) as client:
                    while not self._stop_event.is_set():
                        try:
                            result = await client.inbox_fetch(
                                my_gateway_id=self._my_gateway_id,
                                allowed_sender_gateway_ids=self._current_allow,
                                timeout=self._fetch_timeout,
                                auto_ack=self._auto_ack,
                            )
                        except NATSRoutingUnreachable as e:
                            logger.warning(
                                "session_routing inbox: broker unreachable (%s) — reconnect in %ss",
                                e,
                                self._broker_retry_delay,
                            )
                            await self._sleep_or_stop(self._broker_retry_delay)
                            break

                        # Successful fetch (even if it returned empty).
                        # Empty results still count as "the broker is
                        # alive and we're connected" — without this
                        # stamp, a quiet inbox would let
                        # ``last_activity_at`` go stale and trip the
                        # watchdog even when the runner is healthy.
                        self._record_activity()
                        self._fetch_count += 1

                        message = result.get("message") if result else None
                        if not message:
                            # Empty queue. Reset exponential backoff, sleep
                            # for the configured poll interval, then retry
                            # — the outer loop will refresh the allow-list
                            # so changes take effect without a restart.
                            empty_delay = self._poll_interval
                            await self._sleep_or_stop(empty_delay)
                            continue

                        empty_delay = self._poll_interval
                        self._last_message_at = time.monotonic()
                        dispatched_ok = await self._dispatch(message)
                        if dispatched_ok:
                            # Stamp activity on successful dispatch too.
                            # This is the operator-visible "we are
                            # actually delivering messages" signal —
                            # a runner that fetches but never
                            # successfully dispatches (callback keeps
                            # raising) is also broken.
                            self._record_activity()
                            self._last_dispatch_at = time.monotonic()
                            self._dispatch_count += 1
                        if not self._auto_ack:
                            # Ack order (deferred mode): the callback has
                            # validated/deduped/enqueued durably — only
                            # now confirm consumption to the broker. On
                            # callback failure we skip the ack and let
                            # JetStream redeliver.
                            ack_handle = message.get("ack_handle")
                            if dispatched_ok and ack_handle is not None:
                                await client.ack(ack_handle)

            except NATSRoutingUnreachable as e:
                logger.warning(
                    "session_routing inbox: client failed (%s) — reconnect in %ss",
                    e,
                    self._broker_retry_delay,
                )
                await self._sleep_or_stop(self._broker_retry_delay)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.exception(
                    "session_routing inbox: unexpected loop error: %r", e
                )
                await self._sleep_or_stop(self._broker_retry_delay)

    async def _dispatch(self, message: Dict[str, Any]) -> bool:
        """Invoke the callback for one delivered message.

        Callback errors are logged but never propagate — a buggy handler
        must not take down the consumer loop. Returns True when the
        callback completed without raising (the deferred-ack loop only
        acks on True).
        """
        subject = message.get("subject", "")
        headers = dict(message.get("headers") or {})
        payload = message.get("payload") or {}
        try:
            result = self._on_message(payload, subject, headers)
            if asyncio.iscoroutine(result):
                await result
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception(
                "session_routing inbox: on_message callback raised: %r", e
            )
            return False

    async def _sleep_or_stop(self, seconds: float) -> None:
        """Sleep up to ``seconds`` or until stop is requested.

        Bounded so an immediate stop() returns promptly even after a
        long backoff sleep.
        """
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
