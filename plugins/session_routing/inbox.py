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

Versioning: v0.1.0 (Phase 1).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from plugins.session_routing.allow import effective_allow_list
from plugins.session_routing.nats_client import (
    NATSRoutingClient,
    NATSRoutingUnreachable,
)

logger = logging.getLogger(__name__)

# Defaults mirror session-bridge's "fail fast, log loud, keep going" stance.
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_EMPTY_BACKOFF_MAX = 30.0
DEFAULT_BROKER_RETRY_DELAY = 5.0
DEFAULT_FETCH_TIMEOUT = 1.0


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

        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._current_allow: List[str] = []

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
                            )
                        except NATSRoutingUnreachable as e:
                            logger.warning(
                                "session_routing inbox: broker unreachable (%s) — reconnect in %ss",
                                e,
                                self._broker_retry_delay,
                            )
                            await self._sleep_or_stop(self._broker_retry_delay)
                            break

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
                        await self._dispatch(message)

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

    async def _dispatch(self, message: Dict[str, Any]) -> None:
        """Invoke the callback for one delivered message.

        Callback errors are logged but never propagate — a buggy handler
        must not take down the consumer loop.
        """
        subject = message.get("subject", "")
        headers = dict(message.get("headers") or {})
        payload = message.get("payload") or {}
        try:
            result = self._on_message(payload, subject, headers)
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception(
                "session_routing inbox: on_message callback raised: %r", e
            )

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