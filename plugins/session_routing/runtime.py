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
from typing import Any, List, Optional

from plugins.session_routing import address as _address
from plugins.session_routing import presence as _presence
from plugins.session_routing.dispatcher import BackChannelDispatcher
from plugins.session_routing.inbox import InboxRunner
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

        dispatcher = BackChannelDispatcher(
            servers=self._servers,
            my_gateway_id=self._gateway_id,
            resolve_session_id=self._resolve_session_id,
            enqueue_event=self._enqueue_event,
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
