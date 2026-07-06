"""Thin async wrapper around nats-py for the session-routing plugin.

Three resources on the broker are needed beyond what session-bridge uses:

  1. JetStream stream ``SESSIONS`` (already provisioned by session-bridge).
     We REUSE it — its subject filter is ``from.>`` which matches our
     ``from.<gateway>.<session_key>.deliver`` shape — so installing
     session-routing does NOT introduce a second stream. Reusing lets us
     avoid double-quota accounting and keeps the operator's mental model
     to one stream.

  2. KV bucket ``session_presence`` — keyed by gateway_id, holds
     ``{agent_id, session_key, last_seen}``. Used by ``session_routing_list``
     to enumerate live sessions (no consumer needed).

  3. KV bucket ``session_allow`` (per-recipient) — keyed by gateway_id,
     holds a JSON-encoded list of gateway_ids the recipient accepts
     inbound from. Loaded once on gateway start so the inbox subscriber
     has the right filter list. Phase 2 may move to a shared token+ACL.

Auth / encryption: deferred to Phase 2 (LAN + Courier VPN only).

Why a separate client from session-bridge's NATSClient:
  session-bridge's client is shaped around per-tool ephemeral connections
  (fetch + ack + close). session-routing's flow needs:
    - persistent-ish KV ops (one round-trip per heartbeat / list-call)
    - inbox pull subscription for a recipient (consumer that matches
      ``from.<trusted>.>`` only)
  Reusing session_bridge.nats_client would force a per-call consumer churn
  that defeats inbox semantics. A second small class is the right shape.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

STREAM_NAME = "SESSIONS"
PRESENCE_BUCKET = "session_presence"
ALLOW_BUCKET = "session_allow"

# Tunable defaults. Per-host overrides via ``session_routing.heartbeat_seconds``
# / ``session_routing.presence_ttl_seconds`` in ``~/.hermes/config.yaml``
# (resolved by the caller; the client itself is config-agnostic).
DEFAULT_HEARTBEAT_SECONDS = 30
DEFAULT_PRESENCE_TTL_SECONDS = 90   # 3 × heartbeat by default
DEFAULT_INBOX_PULL_TIMEOUT = 1.0
DEFAULT_PUBLISH_TIMEOUT = 5.0


class NATSRoutingUnreachable(Exception):
    """Raised when the broker is unreachable, auth fails, or pub/sub rejects.

    Caught by the tool handlers in plugins.session_routing.tools and
    converted to a structured ``{"ok": False, "error": "broker_unreachable"}``
    result for the model.
    """


class NATSRoutingClient:
    """Per-call NATS client for session-routing KV/JetStream operations.

    Constructors take positional ``servers`` (list of broker URLs).
    Callers open the connection in an ``async with`` block; we close after
    the call. Heartbeats are NOT issued by this client — ``presence.py``
    owns the heartbeat loop, calling ``update_presence()`` periodically
    on a fresh client per tick (the per-call connection pattern matches
    the rest of the codebase).
    """

    def __init__(
        self,
        servers: List[str],
        name: str = "hermes-session-routing",
    ) -> None:
        if not servers:
            raise ValueError("NATSRoutingClient requires at least one server URL.")
        self._servers = servers
        self._name = name
        self._nc: Optional[Any] = None
        self._js: Optional[Any] = None
        self._kv_presence = None
        self._kv_allow = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, timeout: float = 3.0) -> None:
        """Open connection, ensure SESSIONS stream + KV buckets exist.

        Idempotent — re-running on an already-connected client is a no-op.
        Each provision step is idempotent on its own (find-or-create).
        """
        if self._nc is not None:
            return

        import nats  # type: ignore

        try:
            self._nc = await nats.connect(
                servers=self._servers,
                name=self._name,
                connect_timeout=timeout,
                max_reconnect_attempts=1,  # fail-fast; tool wants quick verdict
            )
            self._js = self._nc.jetstream()

            # SESSIONS stream is expected to exist (session-bridge creates
            # it). If the operator installs only session-routing, we create
            # a minimal version here so the plugin works standalone. Either
            # way ``add_stream`` with the same name+filter is idempotent at
            # the schema level, but nats-py raises on duplicate config, so
            # we attempt a no-op stream-info lookup first.
            try:
                await self._js.find_stream_name(STREAM_NAME)
            except Exception:
                try:
                    await self._js.add_stream(
                        name=STREAM_NAME,
                        subjects=["from.>"],
                        max_msgs=10_000,
                        max_age=24 * 60 * 60,
                        storage="file",
                    )
                except Exception as e:
                    # Race with session-bridge installing concurrently —
                    # tolerate "already exists" silently.
                    logger.debug("session_routing: add_stream raced: %s", e)

            # KV presence bucket: holds one entry per gateway_id.
            try:
                self._kv_presence = await self._js.create_key_value(
                    bucket=PRESENCE_BUCKET,
                    ttl=DEFAULT_PRESENCE_TTL_SECONDS * 3,  # tolerate a few missed beats
                    history=2,
                    storage="file",
                    description=(
                        "Live-session presence registry for session-routing. "
                        "Each entry keyed by gateway_id, holds agent_id + "
                        "session_key + last_seen."
                    ),
                )
            except Exception as e:
                # Already exists → bind to existing.
                logger.debug("session_routing: bind presence bucket: %s", e)
                self._kv_presence = await self._js.key_value(bucket=PRESENCE_BUCKET)

            # KV allow bucket: per-recipient gateway_id → list of allowed
            # sender gateway_ids.
            try:
                self._kv_allow = await self._js.create_key_value(
                    bucket=ALLOW_BUCKET,
                    history=1,
                    storage="file",
                    description=(
                        "Recipient-side allow-list for session-routing. "
                        "Keyed by recipient gateway_id, value is JSON "
                        "array of allowed sender gateway_ids."
                    ),
                )
            except Exception as e:
                logger.debug("session_routing: bind allow bucket: %s", e)
                self._kv_allow = await self._js.key_value(bucket=ALLOW_BUCKET)

        except Exception as e:  # noqa: BLE001
            await self._cleanup()
            raise NATSRoutingUnreachable(f"connect failed: {e!r}") from e

    async def _cleanup(self) -> None:
        self._kv_presence = None
        self._kv_allow = None
        if self._nc is not None:
            try:
                await self._nc.close()
            except Exception:
                pass
        self._nc = None
        self._js = None

    async def close(self) -> None:
        await self._cleanup()

    async def __aenter__(self) -> "NATSRoutingClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Presence (KV write / read / list)
    # ------------------------------------------------------------------

    async def update_presence(
        self,
        *,
        gateway_id: str,
        presence_json: Dict[str, Any],
    ) -> None:
        """Write THIS gateway's presence entry. Idempotent."""
        if self._kv_presence is None:
            raise NATSRoutingUnreachable("not connected")
        body = json.dumps(presence_json, ensure_ascii=False, separators=(",", ":"))
        try:
            await self._kv_presence.put(gateway_id, body.encode("utf-8"))
        except Exception as e:
            raise NATSRoutingUnreachable(f"presence put failed: {e!r}") from e

    async def read_presence(self, gateway_id: str) -> Optional[Dict[str, Any]]:
        """Read a single gateway's presence entry, or None if absent."""
        if self._kv_presence is None:
            raise NATSRoutingUnreachable("not connected")
        try:
            entry = await self._kv_presence.get(gateway_id)
            if entry is None or entry.value is None:
                return None
            return json.loads(entry.value.decode("utf-8"))
        except Exception as e:
            # nats-py raises if key absent; surface as None to caller.
            logger.debug("session_routing: presence read miss for %s: %s", gateway_id, e)
            return None

    async def list_presence(self) -> List[Dict[str, Any]]:
        """Enumerate all presence entries. Returns parsed dicts.

        Does NOT filter by freshness here — the caller (tool handler) does
        that against ``ttl_seconds`` so the freshness window can be tuned
        without re-querying the broker.
        """
        if self._kv_presence is None:
            raise NATSRoutingUnreachable("not connected")
        try:
            keys = await self._kv_presence.keys()
        except Exception as e:
            raise NATSRoutingUnreachable(f"presence keys failed: {e!r}") from e
        out: List[Dict[str, Any]] = []
        for k in keys:
            # nats-py returns keys as bytes when the underlying protocol is
            # key-value. Decode for downstream JSON-friendliness.
            key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else k
            entry = await self.read_presence(key)
            if entry is not None:
                entry["_gateway_id"] = key
                out.append(entry)
        return out

    # ------------------------------------------------------------------
    # Allow-list (KV write / read)
    # ------------------------------------------------------------------

    async def write_allow_list(
        self,
        *,
        recipient_gateway_id: str,
        sender_gateway_ids: List[str],
    ) -> None:
        """Persist THIS recipient's allow-list. Idempotent (overwrites)."""
        if self._kv_allow is None:
            raise NATSRoutingUnreachable("not connected")
        body = json.dumps(
            {"sender_gateway_ids": sorted(set(sender_gateway_ids))},
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            await self._kv_allow.put(recipient_gateway_id, body)
        except Exception as e:
            raise NATSRoutingUnreachable(f"allow put failed: {e!r}") from e

    async def read_allow_list(self, recipient_gateway_id: str) -> List[str]:
        """Read a recipient's allow-list; empty list if absent."""
        if self._kv_allow is None:
            raise NATSRoutingUnreachable("not connected")
        try:
            entry = await self._kv_allow.get(recipient_gateway_id)
        except Exception as e:
            logger.debug("session_routing: allow read miss for %s: %s", recipient_gateway_id, e)
            return []
        if entry is None or entry.value is None:
            return []
        try:
            data = json.loads(entry.value.decode("utf-8"))
        except Exception:
            return []
        return list(data.get("sender_gateway_ids") or [])

    # ------------------------------------------------------------------
    # Publish routed message (JetStream, stream=SESSIONS)
    # ------------------------------------------------------------------

    async def publish_routed(
        self,
        *,
        subject: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        timeout: float = DEFAULT_PUBLISH_TIMEOUT,
    ) -> Dict[str, Any]:
        """Publish to a routing subject. Returns ``{seq, stream}`` on ack.

        The recipient-side allow-list is NOT consulted here. The publish
        just delivers to the broker; the recipient's subscriber is the
        gate (it only subscribes to allow-listed senders' subject
        patterns). This is the fail-closed shape — misaddressed messages
        never even get read by an unauthorized recipient.
        """
        if self._js is None:
            raise NATSRoutingUnreachable("not connected")
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        async def _do() -> Any:
            return await self._js.publish(
                subject=subject,
                payload=body,
                headers=headers or None,
                timeout=timeout,
            )

        try:
            ack = await asyncio.wait_for(_do(), timeout=timeout + 1.0)
        except Exception as e:
            raise NATSRoutingUnreachable(f"publish failed: {e!r}") from e
        return {
            "seq": getattr(ack, "sequence", None),
            "stream": getattr(ack, "stream", STREAM_NAME),
        }

    # ------------------------------------------------------------------
    # Inbox (recipient-side, allow-filtered pull subscription)
    # ------------------------------------------------------------------

    async def inbox_fetch(
        self,
        *,
        my_gateway_id: str,
        allowed_sender_gateway_ids: List[str],
        timeout: float = DEFAULT_INBOX_PULL_TIMEOUT,
        consumer: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Pull at most one pending message addressed to this gateway.

        Recipient-side gate: we build a subject-filter list ``from.<gw>.>``
        for each allowed sender; we subscribe to ALL OF THEM via a single
        pull subscription per sender (the broker's JetStream API allows
        multiple ``filter_subjects`` per consumer, so we use one consumer
        across the allow-list and the broker only delivers matching
        messages). Unauthorized senders' messages never even match.

        Returns ``{"message": None}`` if the queue is empty.
        """
        if self._js is None:
            raise NATSRoutingUnreachable("not connected")

        consumer_name = consumer or f"inbox-{my_gateway_id}"

        filters = [f"from.{g}.>" for g in sorted(set(allowed_sender_gateway_ids))]
        # If allow-list is empty, the caller has explicitly opted out of
        # inbox delivery. Return empty rather than spinning a consumer
        # that will never match.
        if not filters:
            return {"message": None, "consumer": consumer_name, "filter": None}

        try:
            sub = await self._js.pull_subscribe(
                subject=filters[0],  # primary subject (required by nats-py)
                durable=consumer_name,
                config={
                    "ack_policy": "explicit",
                    "deliver_policy": "all",
                    "max_waiting": 1,
                    # The remaining filters are attached as additional
                    # subject patterns. nats-py expects ``filter_subjects``
                    # to be a list of strings.
                    "filter_subjects": filters[1:],
                },
            )
        except Exception as e:
            raise NATSRoutingUnreachable(f"inbox subscribe failed: {e!r}") from e

        wait_ms = int(timeout * 1000) if timeout > 0 else 0
        try:
            msgs = await sub.fetch(batch=1, timeout=max(wait_ms / 1000, 0.05))
        except Exception as e:
            # nats-py raises TimeoutError on empty queue. Empty is success.
            logger.debug("session_routing: inbox fetch empty: %s", e)
            msgs = []

        if not msgs:
            return {"message": None, "consumer": consumer_name, "filter": filters}

        msg = msgs[0]
        try:
            await msg.ack()
        except Exception as e:
            logger.warning("session_routing: inbox ack failed (will redeliver): %s", e)

        try:
            payload = json.loads(msg.data.decode("utf-8"))
        except Exception:
            payload = {"_raw": msg.data.decode("utf-8", errors="replace")}

        headers_out: Dict[str, str] = {}
        if hasattr(msg, "header") and msg.header:
            for k, v in msg.header.items():
                headers_out[k] = ",".join(v) if isinstance(v, list) else str(v)

        return {
            "message": {
                "subject": msg.subject,
                "sequence": msg.metadata.sequence.stream_seq if msg.metadata else None,
                "timestamp": (
                    msg.metadata.timestamp.isoformat()
                    if msg.metadata and msg.metadata.timestamp
                    else None
                ),
                "headers": headers_out,
                "payload": payload,
            },
            "consumer": consumer_name,
            "filter": filters,
        }

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def ping(self, *, timeout: float = 1.0) -> bool:
        """Round-trip to verify broker reachable. Used by check_fn."""
        try:
            await self.connect(timeout=timeout)
            return True
        except NATSRoutingUnreachable:
            return False
        finally:
            await self.close()


# ---------------------------------------------------------------------------
# Module-level helpers (no broker needed) — useful for callers / tests
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    """ISO8601 timestamp in UTC. Kept here so presence.json can be parsed
    back into a uniform datetime without an extra import from caller code.
    """
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def is_fresh(presence_json: Dict[str, Any], *, ttl_seconds: int, now: Optional[float] = None) -> bool:
    """Return True iff ``presence_json['last_seen']`` is within ``ttl_seconds``
    of ``now`` (default: wall clock).

    Uses Unix-epoch seconds; ``presence_json['last_seen']`` is expected to
    be such (we write it that way in ``update_presence``). Tolerates a
    missing or unparseable ``last_seen`` by returning False.
    """
    raw = presence_json.get("last_seen") if isinstance(presence_json, dict) else None
    try:
        last_seen = float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return False
    if last_seen is None:
        return False
    now = now if now is not None else time.time()
    return (now - last_seen) <= ttl_seconds
