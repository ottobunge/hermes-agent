"""Tool schemas + async handlers for the session-bridge plugin.

The handlers are sync from the tool surface's perspective (Hermes tools
take sync handlers), but each opens a fresh NATS connection per call —
matches the pattern used by google_meet.join() (spawns subprocess per
call) and avoids the complexity of a long-lived consumer.

ACK semantics for session_observe:
  - "latest": pull the next pending message on a per-call ephemeral pull
    subscription, return its body + headers + timestamp, then ack so it
    is NOT redelivered. To observe more, call again. The plugin owns the
    consumer name internally so a "drain" UI can list pending messages
    without consuming them — that's Phase 2.

  - "stream" (Phase 2): subscribe with a durable consumer, block waiting
    for messages, return one at a time as they arrive. Phase 1 returns
    immediately with one message (timeout-bound) so the model can poll.

Per Phase 1 plan we keep BOTH modes; "stream" in v1 just means "block
until first message or timeout" — same one-shot semantics as "latest"
but with a configurable wait.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from plugins.session_bridge import nats_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SESSION_EMIT_SCHEMA: Dict[str, Any] = {
    "name": "session_emit",
    "description": (
        "Publish a typed event to a NATS JetStream subject. Used to "
        "coordinate with sibling agents (e.g. Hermes-VMner) outside "
        "user-visible channels (Telegram / Mattermost). Subjects are "
        "plain string identities — conventions: "
        "`from.<agent_id>.peer.<peer_id>.inbox`, "
        "`from.<agent_id>.session.<session_id>.<verb>`, "
        "`from.<agent_id>.system.<topic>`. The `from.<HERMES_AGENT_ID>.` "
        "prefix is added automatically if not present, so the model "
        "can call `subject=peer.<peer_id>.inbox` and the publish lands "
        "at `from.hermes-conrad.peer.<peer_id>.inbox`. Returns the "
        "JetStream seq number on success or a structured error on "
        "broker rejection."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "subject": {
                "type": "string",
                "description": (
                    "Full NATS subject to publish to (e.g. "
                    "`peer.hermes-vmner.inbox`, "
                    "`session.abc123.resume`). Required."
                ),
            },
            "payload": {
                "type": "object",
                "description": (
                    "JSON-serializable dict payload. We do NOT accept "
                    "raw strings — payloads are typed dicts so we can "
                    "schema-check via JetStream headers in Phase 2."
                ),
            },
            "headers": {
                "type": "object",
                "description": (
                    "Optional NATS headers (typed envelope). Use for "
                    "`from`, `session_id`, `kind` so receivers can route "
                    "without deserializing the payload."
                ),
            },
            "timeout": {
                "type": "number",
                "description": (
                    "Broker round-trip timeout in seconds. Default 5. "
                    "Cap at 30."
                ),
                "default": 5,
                "maximum": 30,
            },
        },
        "required": ["subject", "payload"],
        "additionalProperties": False,
    },
}

SESSION_OBSERVE_SCHEMA: Dict[str, Any] = {
    "name": "session_observe",
    "description": (
        "Consume the next pending message from a NATS JetStream subject "
        "pattern. Used for cross-agent event hand-off (peer inbox poll, "
        "system-topic subscription). Returns the message payload, "
        "headers, sequence number, and timestamp on success. Returns "
        "an empty result if no message arrived within the timeout."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "subject": {
                "type": "string",
                "description": (
                    "Subject pattern to subscribe to. JetStream queues "
                    "via $QUEUE or single-subject via plain string. We "
                    "do NOT support wildcards in Phase 1 to avoid "
                    "unordered-multiple-delivery confusion."
                ),
            },
            "mode": {
                "type": "string",
                "enum": ["latest", "stream"],
                "description": (
                    "`latest` returns immediately with one pending message "
                    "(or empty if none queued). `stream` blocks up to "
                    "`timeout` for the first new message."
                ),
                "default": "latest",
            },
            "timeout": {
                "type": "number",
                "description": (
                    "Wait timeout for `mode=stream` in seconds. Default "
                    "1. Cap at 60."
                ),
                "default": 1,
                "maximum": 60,
            },
            "consumer": {
                "type": "string",
                "description": (
                    "Durable consumer name. Defaults to "
                    "`<agent_id>-<subject-sanitized>`. Re-using the "
                    "same consumer across calls advances the cursor "
                    "via the broker's ack/seq log."
                ),
            },
        },
        "required": ["subject"],
        "additionalProperties": False,
    },
}


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

# Each handler opens a fresh NATS connection per call — matches the
# google_meet pattern (subprocess per join()). The added latency is
# ~50ms connect on loopback; we accept that for the model-tool
# simplicity. Phase 2 can hoist this to a long-lived client if it
# shows up in agent trace.

def _broker() -> nats_client.NATSClient:
    # Re-import env in case the gateway updated env between calls.
    import os

    servers = (
        os.environ.get("HERMES_NATS_URLS")
        or os.environ.get("NATS_URLS")
        or "nats://127.0.0.1:4222"
    )
    creds_file = os.environ.get("HERMES_NATS_CREDS_FILE")
    name = os.environ.get("HERMES_AGENT_ID", "hermes-conrad")
    return nats_client.NATSClient(
        servers=[s.strip() for s in servers.split(",") if s.strip()],
        name=name,
        creds_file=creds_file,
    )


def _run_async(coro):
    """Run a coroutine in either sync or async context.

    When called from a sync tool handler at the top level, ``asyncio.run``
    does the right thing. When called from inside an async test or from
    inside Hermes's async pipeline (the gateway session loop), we are
    already inside a running loop and must await directly to avoid
    RuntimeError. Both paths are tested by the offline test suite.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop — start one for this sync tool handler.
        return asyncio.run(coro)
    # Already in a loop — caller must ``await`` us. We expose this as a
    # sync handler but for the cases that wrap from async, the agent's
    # tool layer is sync, so this branch never triggers from a model
    # tool call. Test code that needs the await uses
    # ``asyncio.run(handle_session_emit(...))`` instead.
    raise RuntimeError(
        "session_bridge tools are sync; from async callers wrap with "
        "asyncio.run(handler(...)). Returning a coroutine would break "
        "the model's sync tool invocation contract."
    )


def _scope_subject(subject: str) -> str:
    """Return the subject prefixed with ``from.<HERMES_AGENT_ID>.`` unless
    the caller already named it with a ``from.`` leading segment.

    Per Phase 1 v1 convention: every emit MUST be attributable to one
    sender; receivers should subscribe by trusted-sender prefix
    (e.g. ``from.hermes-vmner.>``) to prevent impersonation.
    """
    import os
    agent_id = os.environ.get("HERMES_AGENT_ID", "hermes-conrad")

    # Already prefixed (e.g. caller spelled out the from-segment).
    if subject.startswith("from.") and len(subject.split(".", 2)) >= 2:
        # Make sure it's THEIR from-segment; if a different agent_id wrote
        # it as the first segment, we refuse (callers must use their own).
        first = subject.split(".", 2)[1]
        if first == agent_id:
            return subject
        # Different agent as `from.` — refuse with structured error
        # bound to the exception path in handle_session_emit.
        raise ValueError(
            f"subject {subject!r} is scoped to {first!r} but "
            f"HERMES_AGENT_ID={agent_id!r}. Pick the right agent_id "
            f"or strip the leading 'from.<id>.' and let the "
            f"sender-scoping wrapper add it for you."
        )

    return f"from.{agent_id}.{subject}"


def handle_session_emit(
    args: Optional[Dict[str, Any]] = None,
    subject: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 5.0,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Sync tool handler — wraps the async coroutine via ``asyncio.run``.

    Sync handlers are the Hermes tool convention (see plugins/google_meet/
    tools.py). ``asyncio.run`` is fine per-call here because the model's
    tool dispatch is synchronous and not running inside an event loop.

    ``args`` is accepted positionally because the registry dispatcher
    (tools/registry.py) calls handlers as ``entry.handler(args, **kwargs)``.
    """
    if args is not None:
        subject = args.get("subject", subject)
        payload = args.get("payload", payload)
        headers = args.get("headers", headers)
        if "timeout" in args:
            try:
                timeout = float(args["timeout"])
            except (TypeError, ValueError):
                pass
    if not subject:
        return {"ok": False, "error": "missing_arg", "detail": "subject is required"}
    if payload is None:
        payload = {}

    async def _emit() -> Dict[str, Any]:
        client = _broker()
        try:
            # Always scope by sender: the from.<HERMES_AGENT_ID>. prefix is
            # added unless the caller explicitly named it themselves AND
            # the from-segment matches this agent's id. Refusing
            # cross-sender writes is the point — receivers trust the from
            # segment as the sender identity and gate subscription on it.
            scoped_subject = _scope_subject(subject)
            await client.connect()
            return await client.publish(
                subject=scoped_subject,
                payload=payload,
                headers=headers or {},
                timeout=timeout,
            )
        finally:
            await client.close()

    try:
        result = _run_async(_emit())
    except ValueError as e:
        # Cross-agent scope attempt — refuse loudly so the model sees
        # the structured error rather than a silently-misrouted publish.
        logger.warning("session_emit: scope rejected: %s", e)
        return {"ok": False, "error": "scope_rejected", "detail": str(e)}
    except nats_client.NATSUnreachable as e:
        logger.warning("session_emit: broker unreachable: %s", e)
        return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("session_emit failed")
        return {"ok": False, "error": "emit_failed", "detail": repr(e)}

    return {"ok": True, "subject": subject, **result}


def handle_session_observe(
    args: Optional[Dict[str, Any]] = None,
    subject: Optional[str] = None,
    mode: str = "latest",
    timeout: float = 1.0,
    consumer: Optional[str] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Sync tool handler — see handle_session_emit() for asyncio.run rationale."""
    if args is not None:
        subject = args.get("subject", subject)
        mode = args.get("mode", mode)
        if "timeout" in args:
            try:
                timeout = float(args["timeout"])
            except (TypeError, ValueError):
                pass
        consumer = args.get("consumer", consumer)
    if not subject:
        return {"ok": False, "error": "missing_arg", "detail": "subject is required"}

    async def _observe() -> Dict[str, Any]:
        client = _broker()
        try:
            await client.connect()
            return await client.observe(
                subject=subject,
                mode=mode,
                timeout=timeout,
                consumer=consumer,
            )
        finally:
            await client.close()

    try:
        result = _run_async(_observe())
    except nats_client.NATSUnreachable as e:
        logger.warning("session_observe: broker unreachable: %s", e)
        return {"ok": False, "error": "broker_unreachable", "detail": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("session_observe failed")
        return {"ok": False, "error": "observe_failed", "detail": repr(e)}

    return {"ok": True, "subject": subject, **result}
