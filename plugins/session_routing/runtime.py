"""Gateway-lifecycle runtime for session-routing (v0.3.0).

Owns the long-lived pieces the plugin starts from ``on_gateway_start``:

  * presence heartbeat — one ``session_presence`` KV row per gateway,
    refreshed every ``DEFAULT_HEARTBEAT_SECONDS``, advertising ALL live
    session_keys (``live_sessions``) so peers can resolve arbitrary
    target sessions on this host.
  * inbox consumer — ``InboxRunner`` in deferred-ack mode feeding the
    receive-side ``BackChannelDispatcher``, which injects back-channel
    text into sessions via the gateway's public
    ``enqueue_internal_session_event`` helper (adapter FIFO).

And the teardown from ``on_gateway_stop`` (stop inbox first — fired at
the START of graceful shutdown, before the agent drain) plus
``on_session_finalize`` channel cleanup (close + handshake.bye for every
open channel owned by a session that is going away).

Note: the plan draft said "on_session_end closes channels", but that
hook fires at the end of EVERY conversation turn — closing there would
kill a back-channel after one exchange. ``on_session_finalize`` fires
when a session is actually finalized (expiry / shutdown), which is the
intended lifetime ("channels die with sessions").

All failures are logged, never raised — a broken broker must not block
gateway startup, shutdown, or session finalization.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

from plugins.session_routing import address as _address
from plugins.session_routing import presence as _presence
from plugins.session_routing.dispatcher import BackChannelDispatcher
from plugins.session_routing.inbox import (
    DEFAULT_HEALTH_THRESHOLD_SECONDS,
    InboxRunner,
)
from plugins.session_routing.nats_client import DEFAULT_HEARTBEAT_SECONDS

logger = logging.getLogger(__name__)


def broker_servers() -> List[str]:
    """Resolve NATS server list. Mirrors session-bridge's precedence."""
    raw = (
        os.environ.get("HERMES_NATS_URLS")
        or os.environ.get("NATS_URLS")
        or "nats://127.0.0.1:4222"
    )
    return [s.strip() for s in raw.split(",") if s.strip()]


def broker_configured() -> bool:
    """Lightweight gate for the lifecycle hooks.

    Deliberately NO broker ping here: the hooks run on the gateway's
    event loop where ``asyncio.run`` is illegal, and a down broker is
    the InboxRunner's retry loop's job, not a reason to skip wiring.
    """
    try:
        import nats  # noqa: F401
    except ImportError:
        return False
    return bool(
        os.environ.get("HERMES_NATS_URLS") or os.environ.get("NATS_URLS")
    )


class SessionRoutingRuntime:
    """One instance per process, wired by ``register()`` in __init__.py."""

    def __init__(self) -> None:
        self._gateway: Any = None
        self._runner: Optional[InboxRunner] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        # Watchdog: detects silent death of the inbox runner (task object
        # alive but no activity — broker reconnect wedged, network
        # partition, etc.) and respawns it with exponential backoff.
        # The previous failure mode (2026-07-07 02:41 CEST) was exactly
        # this: runner stopped pulling, ``is_running`` stayed True, no
        # error logged, 24+ minutes of bilateral coordination lost
        # before the operator noticed.
        self._watchdog_task: Optional[asyncio.Task] = None
        self._watchdog_restarts: int = 0
        self._watchdog_last_check_at: float = 0.0
        self._cleanup_tasks: set = set()
        self._servers: List[str] = []
        self._gateway_id: str = ""

    # -- on_gateway_start ---------------------------------------------------

    def on_gateway_start(self, gateway: Any = None, **_kwargs: Any):
        """Sync hook callback; returns the awaitable the gateway awaits."""
        if gateway is None:
            logger.debug("session_routing runtime: no gateway kwarg — skip")
            return None
        if not broker_configured():
            logger.info(
                "session_routing runtime: nats-py missing or no "
                "HERMES_NATS_URLS/NATS_URLS — inbox + presence not started"
            )
            return None
        return self._start(gateway)

    async def _start(self, gateway: Any) -> None:
        if self._runner is not None and self._runner.is_running:
            logger.debug("session_routing runtime: already started — no-op")
            return
        self._gateway = gateway
        self._servers = broker_servers()
        try:
            self._gateway_id = _address.resolve_gateway_id()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "session_routing runtime: gateway_id resolution failed: %s", e
            )
            return

        # The plugin's check_fn may have cached a False result during the
        # window before the broker was reachable (env-set race / VPN not
        # up at startup). Now that we know the broker works (we just
        # created the inbox consumer), invalidate the cache so the model
        # tools become visible on the next turn instead of staying hidden
        # for the full check_fn TTL.
        try:
            from tools.registry import invalidate_check_fn_cache
            invalidate_check_fn_cache()
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "session_routing runtime: invalidate_check_fn_cache failed: %s", e
            )

        dispatcher = BackChannelDispatcher(
            servers=self._servers,
            my_gateway_id=self._gateway_id,
            resolve_session_id=self._resolve_session_id,
            enqueue_event=self._enqueue_event,
            publish_notification=self._publish_notification,
        )
        self._runner = InboxRunner(
            servers=self._servers,
            my_gateway_id=self._gateway_id,
            on_message=dispatcher.handle,
            auto_ack=False,  # dedupe-before-ack (dispatcher contract)
        )
        self._runner.start()

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"session-routing-presence-{self._gateway_id}",
        )
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(),
            name=f"session-routing-watchdog-{self._gateway_id}",
        )
        logger.info(
            "session_routing runtime: started (gateway_id=%s, servers=%s)",
            self._gateway_id, self._servers,
        )

    # -- on_gateway_stop ------------------------------------------------------

    def on_gateway_stop(self, gateway: Any = None, **_kwargs: Any):
        if self._runner is None and self._heartbeat_task is None:
            return None
        return self._stop()

    async def _stop(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._watchdog_task = None
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._heartbeat_task = None
        if self._runner is not None:
            try:
                await self._runner.stop(timeout=5.0)
            except Exception as e:  # noqa: BLE001
                logger.warning("session_routing runtime: inbox stop failed: %s", e)
            self._runner = None
        logger.info("session_routing runtime: stopped")

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    async def _watchdog_loop(self) -> None:
        """Periodically check the inbox runner's health. If the runner
        has gone silent (no activity within ``health_threshold`` and
        the task is "running"), respawn it.

        Why this exists: ``InboxRunner.is_running`` returns True when
        the task object is alive, but a wedged broker reconnect (or
        similar) can leave the task alive while it stops actually
        pulling. The previous failure mode (2026-07-07 02:41 CEST) was
        exactly this — silent death, no error, lost coordination. The
        watchdog respawns the runner with exponential backoff so a
        persistently-broken broker doesn't hot-loop the respawn.
        """
        import time as _time

        check_interval = max(5.0, DEFAULT_HEALTH_THRESHOLD_SECONDS / 4.0)
        backoff = 1.0
        max_backoff = 60.0
        next_allowed_restart = 0.0
        while True:
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                raise
            self._watchdog_last_check_at = _time.monotonic()
            runner = self._runner
            if runner is None:
                continue
            if runner.is_healthy():
                # Healthy — reset backoff, keep watching.
                backoff = 1.0
                continue
            # Unhealthy. The runner task is "running" but the
            # activity clock is stale, OR the task is dead. Either
            # way, respawn it — but only if we're past the
            # backoff window (so a broker that's permanently down
            # doesn't burn the respawn budget).
            now = _time.monotonic()
            if now < next_allowed_restart:
                logger.debug(
                    "session_routing watchdog: unhealthy but backoff window "
                    "(%.1fs left)",
                    next_allowed_restart - now,
                )
                continue
            logger.warning(
                "session_routing watchdog: inbox runner unhealthy "
                "(is_running=%s, last_activity=%.1fs ago, fetches=%d, "
                "dispatches=%d) — respawning",
                runner.is_running,
                now - runner.last_activity_at,
                runner.fetch_count,
                runner.dispatch_count,
            )
            self._watchdog_restarts += 1
            try:
                await runner.stop(timeout=5.0)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "session_routing watchdog: stop() raised %r — continuing",
                    e,
                )
            self._runner = InboxRunner(
                servers=self._servers,
                my_gateway_id=self._gateway_id,
                on_message=self._rebuild_dispatcher_for_restart,
                auto_ack=False,
            )
            self._runner.start()
            next_allowed_restart = now + backoff
            backoff = min(backoff * 2.0, max_backoff)
            logger.info(
                "session_routing watchdog: respawned runner (total_restarts=%d, "
                "next backoff=%.1fs)",
                self._watchdog_restarts,
                backoff,
            )

    def _rebuild_dispatcher_for_restart(
        self,
        payload: Any,
        subject: str,
        headers: Dict[str, str],
    ) -> Any:
        """Build a fresh dispatcher + invoke handle on respawn.

        We can't keep the old dispatcher's KV client open across
        respawns (the NATS connection it owns is gone), so the
        watchdog constructs a new one each time. The same
        collaborators (resolve_session_id, enqueue_event,
        publish_notification) carry over.
        """
        dispatcher = BackChannelDispatcher(
            servers=self._servers,
            my_gateway_id=self._gateway_id,
            resolve_session_id=self._resolve_session_id,
            enqueue_event=self._enqueue_event,
            publish_notification=self._publish_notification,
        )
        return dispatcher.handle(
            envelope=payload,
            subject=subject,
            headers=headers,
        )

    # ------------------------------------------------------------------
    # Operator visibility
    # ------------------------------------------------------------------

    def inbox_status(self) -> Dict[str, Any]:
        """Return a status snapshot for ``session_inbox_status`` tool.

        The model-facing tool surfaces this to the operator so
        silent-death cases are visible immediately. Includes the
        watchdog's restart count and last-check timestamp.
        """
        import time as _time

        runner = self._runner
        if runner is None:
            return {
                "started": False,
                "gateway_id": self._gateway_id,
            }
        now = _time.monotonic()
        return {
            "started": True,
            "gateway_id": self._gateway_id,
            "is_running": runner.is_running,
            "is_healthy": runner.is_healthy(now=now),
            "last_activity_age_seconds": (
                now - runner.last_activity_at
                if runner.last_activity_at else None
            ),
            "fetch_count": runner.fetch_count,
            "dispatch_count": runner.dispatch_count,
            "watchdog_restarts": self._watchdog_restarts,
            "watchdog_last_check_age_seconds": (
                now - self._watchdog_last_check_at
                if self._watchdog_last_check_at else None
            ),
            "consumer_name": f"inbox-{self._gateway_id}",
        }

    # -- on_session_finalize ---------------------------------------------------

    def on_session_finalize(
        self,
        session_id: Optional[str] = None,
        **_kwargs: Any,
    ) -> None:
        """Close all back-channels owned by a session that is ending.

        Fired from sync hook dispatch; schedules the async close on the
        running loop (gateway). Without a loop (CLI finalize) there is
        nothing to close — this runtime only starts inside a gateway.
        """
        if not session_id or self._gateway is None:
            return
        if not broker_configured():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._close_session_channels(session_id))
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)

    async def _close_session_channels(self, session_id: str) -> None:
        from plugins.session_routing.channels import close_channels_for_session

        my_address = self._address_for_session_id(session_id)
        try:
            closed = await close_channels_for_session(
                servers=self._servers or broker_servers(),
                session_id=session_id,
                my_address=my_address,
            )
            if closed:
                logger.info(
                    "session_routing runtime: closed %d channel(s) for "
                    "finalized session %s", closed, session_id,
                )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "session_routing runtime: channel close for %s failed: %s",
                session_id, e,
            )

    # -- collaborators ---------------------------------------------------------

    def _resolve_session_id(self, session_key: str) -> Optional[str]:
        try:
            entry = self._gateway.session_store.get_entry(session_key)
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "session_routing runtime: session lookup failed: %s", e
            )
            return None
        return entry.session_id if entry is not None else None

    def _enqueue_event(self, session_key: str, event: Any) -> bool:
        try:
            return bool(
                self._gateway.enqueue_internal_session_event(session_key, event)
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "session_routing runtime: enqueue failed for %s: %s",
                session_key, e,
            )
            return False

    async def _publish_notification(
        self, session_key: str, text: str, kind: str
    ) -> None:
        """Dispatcher collaborator: user-visible platform notification.

        Delegates to the gateway's ``publish_internal_notification``
        seam (display side-channel — never a session turn). Duck-typed
        and best-effort: a gateway without the seam or a failing
        platform is logged, never raised — visibility must not affect
        envelope handling.
        """
        publish = getattr(self._gateway, "publish_internal_notification", None)
        if publish is None:
            logger.debug(
                "session_routing runtime: gateway has no "
                "publish_internal_notification — notification dropped"
            )
            return
        try:
            await publish(session_key, text, kind=kind)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "session_routing runtime: notification publish failed "
                "for %s: %s", session_key, e,
            )

    def _live_session_keys(self) -> List[str]:
        try:
            return [
                e.session_key
                for e in self._gateway.session_store.list_sessions()
                if e.session_key
            ]
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "session_routing runtime: list_sessions failed: %s", e
            )
            return []

    def _address_for_session_id(self, session_id: str) -> str:
        try:
            entry = self._gateway.session_store.lookup_by_session_id(session_id)
            if entry is not None and entry.session_key:
                return _address.build(self._gateway_id, entry.session_key)
        except Exception:  # noqa: BLE001
            pass
        # Entry already evicted: bye 'from' falls back to a parseable
        # gateway-scoped address (peers only need it for logging).
        return _address.build(self._gateway_id or "gw-unknown", "gateway")

    # -- heartbeat --------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        agent_id = (
            os.environ.get("HERMES_AGENT_ID")
            or os.environ.get("HERMES_PROFILE")
            or "hermes"
        )
        while True:
            try:
                live = self._live_session_keys()
                await _presence.update_presence(
                    servers=self._servers,
                    gateway_id=self._gateway_id,
                    agent_id=agent_id,
                    session_key=live[0] if live else "",
                    platform="gateway",
                    live_sessions=live,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.debug(
                    "session_routing runtime: heartbeat failed: %s", e
                )
            await asyncio.sleep(DEFAULT_HEARTBEAT_SECONDS)


# Module-level singleton the register() wiring + tests share.
runtime = SessionRoutingRuntime()


def publish_notification_threadsafe(
    session_key: str,
    text: str,
    kind: str = "info",
    gateway: Any = None,
) -> bool:
    """Publish a user-visible notification from OUTSIDE the gateway loop.

    Tool handlers run sync inside their own ``asyncio.run`` loop, so
    they cannot await the gateway's ``publish_internal_notification``
    directly — this schedules it on the gateway's loop
    (``_gateway_loop``, set by GatewayRunner at startup) fire-and-forget.

    ``gateway`` defaults to the runtime singleton's wired gateway.
    Returns True when the notification was scheduled; False when there
    is no gateway / seam / loop to schedule on (CLI runs, legacy
    gateways) — callers treat that as "platform side-channel off".
    """
    gw = gateway if gateway is not None else runtime._gateway
    if gw is None:
        return False
    publish = getattr(gw, "publish_internal_notification", None)
    loop = getattr(gw, "_gateway_loop", None)
    if publish is None or loop is None:
        return False
    try:
        asyncio.run_coroutine_threadsafe(
            publish(session_key, text, kind=kind), loop
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "session_routing runtime: threadsafe notification for %s "
            "failed to schedule: %s", session_key, e,
        )
        return False
    return True
