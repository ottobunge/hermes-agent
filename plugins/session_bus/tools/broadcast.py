"""Broadcast tools for session-bus — bus_emit / bus_observe.

Lifted from the old plugins/session_bridge/tools.py and renamed to
bus_emit / bus_observe under the unified plugin.

Design notes (preserved from session-bridge):
  - Per-tool ephemeral connection. No in-process queue state. The
    bus_observe tool opens a fresh pull subscription, fetches up to N
    messages, then closes — keeps the consumer side stateless so a
    long-lived agent can poll without holding a sub.
  - Subject prefix is auto-applied: every publish goes to
    `from.<HERMES_AGENT_ID>.<subject-suffix>`. Receivers should subscribe
    to `from.<trusted-agent>.>` (no cross-sender wildcard) to keep
    fan-in isolated. See session_bus.address for canonical addressing.
  - Both tools gate on the NATS client being reachable — if the broker
    is down, the tools return a structured error rather than crashing.
"""

import json
import logging
import os
from typing import Any

from plugins.session_bus.nats_client import SessionBusClient

logger = logging.getLogger("session_bus.broadcast")


def _resolve_servers() -> list[str]:
    raw = os.environ.get("HERMES_NATS_URLS") or os.environ.get("NATS_URLS")
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


async def _connect() -> SessionBusClient | None:
    servers = _resolve_servers()
    if not servers:
        return None
    creds = os.environ.get("HERMES_NATS_CREDS_FILE")
    client = SessionBusClient(
        servers=servers,
        name=f"session-bus-broadcast-{os.getpid()}",
        creds_file=creds,
    )
    try:
        await client.connect()
    except Exception as e:
        logger.error("session_bus.broadcast connect failed: %s", e)
        return None
    return client


EMIT_SCHEMA = {
    "name": "bus_emit",
    "description": (
        "Publish a typed event onto the session-bus broadcast stream. "
        "Every emit auto-prefixes the subject with `from.<HERMES_AGENT_ID>.` "
        "so the receiver can attribute the message. Use bus_observe to "
        "subscribe from the same broker."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "subject": {
                "type": "string",
                "description": (
                    "Subject suffix after `from.<HERMES_AGENT_ID>.`. "
                    "Examples: `peer.<peer_id>.inbox`, "
                    "`session.<session_id>.<verb>`, `system.<topic>`."
                ),
            },
            "payload": {
                "type": "object",
                "description": (
                    "JSON-serializable event body. Must contain at least "
                    "`message` (string) and may include any structured "
                    "fields the receiver expects."
                ),
            },
            "headers": {
                "type": "object",
                "description": "Optional NATS headers (key/value strings).",
                "default": None,
            },
        },
        "required": ["subject", "payload"],
    },
}

OBSERVE_SCHEMA = {
    "name": "bus_observe",
    "description": (
        "Pull up to N messages from a JetStream stream subject. Returns "
        "the messages in order, ack'd after read. Use a specific subject "
        "prefix (e.g. `from.<trusted-agent>.system.>`) to scope the "
        "subscription — never use a wildcard that crosses senders."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "subject": {
                "type": "string",
                "description": (
                    "Subject pattern to subscribe to. Examples: "
                    "`from.Hermes-VMner.>`, `from.Hermes-Conrad.session.<sid>.>`."
                ),
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of messages to return (default 10).",
                "default": 10,
            },
            "timeout": {
                "type": "number",
                "description": "Seconds to wait for messages (default 5.0).",
                "default": 5.0,
            },
        },
        "required": ["subject"],
    },
}


async def handle_emit(args: dict[str, Any], **kwargs) -> str:
    """bus_emit handler.

    Prefixes the subject with `from.<HERMES_AGENT_ID>.`, JSON-encodes the
    payload, publishes with QoS 1, returns a structured result.
    """
    client = await _connect()
    if client is None:
        return json.dumps({"error": "session_bus not configured",
                          "hint": "set HERMES_NATS_URLS in the gateway env"})

    subject = args.get("subject", "")
    payload = args.get("payload", {})
    headers = args.get("headers")

    if not subject:
        return json.dumps({"error": "subject is required"})

    agent_id = os.environ.get("HERMES_AGENT_ID", "anonymous")
    full_subject = f"from.{agent_id}.{subject}"

    try:
        seq = await client.publish(full_subject, payload, headers=headers)
        return json.dumps({
            "status": "published",
            "subject": full_subject,
            "seq": seq,
        })
    except Exception as e:
        return json.dumps({"error": str(e), "subject": full_subject})


async def handle_observe(args: dict[str, Any], **kwargs) -> str:
    """bus_observe handler.

    Opens a per-call pull subscription, fetches up to `limit` messages
    within `timeout`, acks them, returns a list. The subscription is
    closed before returning so the agent can poll without bound state.
    """
    client = await _connect()
    if client is None:
        return json.dumps({"error": "session_bus not configured"})

    subject = args.get("subject", "")
    limit = int(args.get("limit", 10))
    timeout = float(args.get("timeout", 5.0))

    if not subject:
        return json.dumps({"error": "subject is required"})

    try:
        msgs = await client.observe(subject, mode="stream",
                                    limit=limit, timeout=timeout)
        out = []
        for m in msgs:
            out.append({
                "subject": m.subject,
                "seq": m.seq,
                "headers": m.headers,
                "payload": m.payload,
            })
        return json.dumps({"status": "ok", "count": len(out),
                          "messages": out})
    except Exception as e:
        return json.dumps({"error": str(e)})
