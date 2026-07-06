"""Thin async wrapper around nats-py for the session-bridge plugin.

Designed for short-lived per-tool connections (no in-process queue state),
so the same module looks like:

  client = NATSClient(servers=[...], name=...)
  await client.connect()
  await client.publish(subject, payload, headers)         # JetStream publish
  await client.observe(subject, mode="stream", timeout=5) # JetStream pull
  await client.close()

We deliberately use ``jetstream()`` and ``pull_subscribe()`` rather than
core NATS pub/sub so messages survive consumer restarts and round-trip
through the broker's ack/seq log. This matches the cross-agent wake
guarantee we want (Hermes-Conrad↔Hermes-VMner DMs that survive a host
reboot, just like Raft's server-side queue).

Phase 1 scope:
  - Single broker URL (comma-separated list is allowed but treated as
    a failover sequence).
  - No auth (creds_file param is a placeholder — Phase 2 wires it to the
    shared token from ~/.hermes/.env once the operator chooses to enable
    vars.services.nats.auth in nixos-config).
  - No replay (JetStream keeps the last 1000 messages on stream "SESSIONS"
    — Phase 2 may add a per-session "from seq" param).

Stream topology: we auto-provision a single JetStream stream named
"SESSIONS" that captures all subjects starting with ``peer.``, ``session.``,
or ``system.``. Subjects we publish on but DO NOT match those prefixes
are still delivered (JetStream does not scope by subject unless we add
a filter) — we add explicit subject filters in Phase 2 if subject-namespace
proliferation starts misrouting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

logger = logging.getLogger(__name__)

STREAM_NAME = "SESSIONS"


class NATSUnreachable(Exception):
    """Raised when the broker is unreachable, auth fails, or pub/sub rejects.

    Caught by the tool handlers in plugins/session-bridge/tools.py and
    converted to a structured ``{"ok": False, "error": "broker_unreachable"}``
    result for the model.
    """


class NATSClient:
    def __init__(
        self,
        servers: List[str],
        name: str = "hermes-agent",
        creds_file: Optional[str] = None,
    ) -> None:
        if not servers:
            raise ValueError("NATSClient requires at least one server URL.")
        self._servers = servers
        self._name = name
        self._creds_file = creds_file

        # Populated by connect().
        self._nc: Optional[Any] = None
        self._js: Optional[Any] = None
        # Default durable consumer name template.
        self._consumer_template = lambda subj: f"{name}-{_sanitize(subj)}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, timeout: float = 3.0) -> None:
        """Open the broker connection and provision the SESSIONS stream.

        Times out fast (3s default) so tool handlers don't park in a
        reconnect loop on a downed broker. Re-raise as NATSUnreachable
        for the handler to map to the structured error.
        """
        if self._nc is not None:
            return

        import nats  # type: ignore

        try:
            connect_kwargs: Dict[str, Any] = {
                "servers": self._servers,
                "name": self._name,
                "connect_timeout": timeout,
                "max_reconnect_attempts": 1,  # fail-fast; tool wants quick verdict
            }
            if self._creds_file:
                connect_kwargs["user_credentials"] = self._creds_file

            self._nc = await nats.connect(**connect_kwargs)
            self._js = self._nc.jetstream()

            # Provision the SESSIONS stream. Idempotent: info() will find
            # an existing stream by name and we skip create. We capture
            # only the subjects we use today (Phase 2 may widen).
            try:
                await self._js.find_stream_name(STREAM_NAME)
                # Stream exists — done.
            except Exception:  # noqa: BLE001 — nats-py raises on not-found
                await self._js.add_stream(
                    name=STREAM_NAME,
                    subjects=["peer.*", "session.*", "system.*"],
                    # Reasonable defaults; tune in Phase 2.
                    max_msgs=10_000,
                    max_age=24 * 60 * 60,  # 1 day retention
                    storage="file",
                )
        except Exception as e:  # noqa: BLE001
            self._nc = None
            self._js = None
            raise NATSUnreachable(f"connect failed: {e!r}") from e

    async def close(self) -> None:
        if self._nc is not None:
            try:
                await self._nc.close()
            finally:
                self._nc = None
                self._js = None

    async def __aenter__(self) -> "NATSClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    async def publish(
        self,
        *,
        subject: str,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        if self._js is None:
            raise NATSUnreachable("not connected")

        # Encode payload as JSON. Base16/Base64 wrap kept for Phase 2 when
        # we accept binary payloads.
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        async def _do() -> Any:
            ack = await self._js.publish(
                subject=subject,
                payload=body,
                headers=headers or None,
                timeout=timeout,
            )
            return ack

        ack = await asyncio.wait_for(_do(), timeout=timeout + 1.0)
        return {
            "seq": getattr(ack, "sequence", None),
            "stream": getattr(ack, "stream", STREAM_NAME),
        }

    # ------------------------------------------------------------------
    # Observe (consume)
    # ------------------------------------------------------------------

    async def observe(
        self,
        *,
        subject: str,
        mode: str = "latest",
        timeout: float = 1.0,
        consumer: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Pull one message from the stream and ack it.

        ``mode="latest"`` returns immediately with one pending message (or
        ``{"message": None}`` if the queue is empty).
        ``mode="stream"`` blocks up to ``timeout`` for the first message.

        In both cases, the consumer is durable — calling again advances
        the broker's cursor, and a fresh gateway session resumes from
        the last seq. Phase 2 will add a ``peek`` mode that doesn't ack.
        """
        if self._js is None:
            raise NATSUnreachable("not connected")

        consumer_name = consumer or self._consumer_template(subject)

        # Ensure durable consumer exists. AckPolicy=explicit + deliver
        # policy=all means we replay anything pending from a previous run,
        # not just new publishes.
        try:
            sub = await self._js.pull_subscribe(
                subject=subject,
                durable=consumer_name,
                config={
                    "ack_policy": "explicit",
                    "deliver_policy": "all",
                    "max_waiting": 1,  # we only ever ask for 1
                },
            )
        except Exception as e:  # noqa: BLE001
            # Subject may not match our stream's filter (peek at SESSIONS
            # stream filters). The model-facing error must be readable.
            raise NATSUnreachable(f"subscribe failed for {subject!r}: {e!r}") from e

        wait_ms = int(timeout * 1000) if mode == "stream" else 0
        try:
            msgs = await sub.fetch(batch=1, timeout=wait_ms / 1000 if wait_ms else 0.05)
        except Exception as e:  # noqa: BLE001 — empty-queue is an exception class in nats-py
            # nats-py raises ``nats.errors.TimeoutError`` on empty fetch
            # with no wait. Mapped to "no message" rather than surfaced.
            logger.debug("session_bridge: fetch returned empty: %s", e)
            msgs = []

        if not msgs:
            return {"message": None, "consumer": consumer_name}

        msg = msgs[0]
        # Ack BEFORE returning so a slow model that retries doesn't get a
        # duplicate. (Phase 2 may move to ack-after-successful-return-with-retry.)
        try:
            await msg.ack()
        except Exception as e:  # noqa: BLE001
            logger.warning("session_bridge: ack failed (will redeliver): %s", e)

        # Decode payload.
        try:
            payload = json.loads(msg.data.decode("utf-8"))
        except Exception:  # noqa: BLE001
            payload = {"_raw": msg.data.decode("utf-8", errors="replace")}

        headers_out: Dict[str, str] = {}
        if hasattr(msg, "header") and msg.header:
            for k, v in msg.header.items():
                headers_out[k] = ",".join(v) if isinstance(v, list) else str(v)

        return {
            "message": {
                "subject": msg.subject,
                "sequence": msg.metadata.sequence.stream_seq if msg.metadata else None,
                "timestamp": msg.metadata.timestamp.isoformat() if msg.metadata and msg.metadata.timestamp else None,
                "headers": headers_out,
                "payload": payload,
            },
            "consumer": consumer_name,
        }

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def ping(self, *, timeout: float = 1.0) -> bool:
        """Used by ``check_session_bridge_requirements`` — short roundtrip."""
        try:
            await self.connect(timeout=timeout)
            return True
        except NATSUnreachable:
            return False
        finally:
            await self.close()


def _sanitize(subject: str) -> str:
    """Turn a NATS subject into a valid consumer-name fragment.

    JetStream durable consumer names allow ASCII alphanumeric + ``-`` and
    ``_``. Subjects use ``.`` and ``*``, which we replace.
    """
    out = subject.replace(".", "-").replace("*", "wild")
    return out[:50] or "default"
