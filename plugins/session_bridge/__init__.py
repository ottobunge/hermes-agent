"""session-bridge plugin — cross-agent typed coordination bus.

Provides two model tools the agent can call to publish/consume structured
events to/from a NATS JetStream broker. Designed for off-Telegram
agent-to-agent coordination that MUST NOT surface on user-visible channels
(see the two-layer architecture rule in MEMORY.md — Raft for human-readable
agent chat, a separate bus for typed internal coordination).

Subject conventions (publishers MUST prefix every subject with
``from.<agent_id>.``, receivers MUST subscribe with the prefix they trust):

  - ``from.<agent_id>.peer.<peer_id>.inbox``        — point-to-point inbox
                                                    from <agent_id> to
                                                    <peer_id>. Receiving
                                                    agent subscribes to
                                                    ``from.<trusted>.peer.<self>.inbox``
                                                    to hear only the
                                                    senders it trusts.
  - ``from.<agent_id>.session.<session_id>.<verb>`` — per-sender one-shot
                                                    signals for a specific
                                                    session.
  - ``from.<agent_id>.system.<topic>``             — broadcast topics
                                                    attributed to a
                                                    single sender.

Every subject published via ``session_emit`` is auto-prefixed with
``from.<HERMES_AGENT_ID>.``. Receivers that listen via ``session_observe``
should also subscribe to ``from.<specific-agent>.>`` (no wildcards
across senders) when they want a single-sender stream — this is the
fan-in isolation rule that prevents agent C from impersonating agent A
by writing to A's subjects. v1 implements sender-prefixing in
``handle_session_emit``; the receive side just observes whatever
subject the caller names (callers pick the from-prefix they trust).

Latency / durability model:
  - JetStream with a per-tool durable consumer (ack semantics) so messages
    are NOT lost across consumer restarts. This matches the wake-chain
    guarantee we want across agents (Hermes-Conrad↔Hermes-VMner DMs that
    survive a host reboot).
  - Subjects are NOT namespaced per-host today — the LAN+Courier VPN
    boundary plus the future Phase 2 shared token are the access-control
    layers. Treat any subject read/write as accepted.

Versioning: v0.1.0 (Phase 1, no auth, no replay, no wildcard subs).

Auth: deferred to Phase 2 (see ``vars.services.nats.auth`` in nixos-config
modules/system/features/nats-server.nix). Phase 1 runs the broker with no
auth; LAN+Courier VPN are the access boundary. We rely on per-tool
``check_fn`` to gate ``session_emit``/``session_observe`` on NATS being
reachable from the gateway host.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional

from plugins.session_bridge import tools as _tools
from plugins.session_bridge.nats_client import NATSClient, NATSUnreachable

logger = logging.getLogger(__name__)


_TOOLS = (
    (
        "session_emit",
        _tools.SESSION_EMIT_SCHEMA,
        _tools.handle_session_emit,
        "📡",
    ),
    (
        "session_observe",
        _tools.SESSION_OBSERVE_SCHEMA,
        _tools.handle_session_observe,
        "🔭",
    ),
)


def _client() -> NATSClient:
    """Build a NATSClient from env vars. ``HERMES_HOME`` is consulted first,
    then the .env file. The env shape is intentionally minimal — operators
    do NOT need to set every knob. Phase 2 may add ``NATS_TOKEN``.
    """
    import os

    servers = os.environ.get("HERMES_NATS_URLS") or os.environ.get("NATS_URLS")
    if servers is None:
        # Default — the thinkpad NATS broker the operator runs locally.
        # Per-host override: set HERMES_NATS_URLS="nats://thinkpad:4222"
        # in ``~/.hermes/.env`` (sops-encrypted) to point agents at the
        # right box. Sensible LAN default works on this single-broker
        # deployment.
        servers = "nats://127.0.0.1:4222"

    creds_file = os.environ.get("HERMES_NATS_CREDS_FILE")
    name = os.environ.get("HERMES_AGENT_ID", "hermes-conrad")

    return NATSClient(
        servers=[s.strip() for s in servers.split(",") if s.strip()],
        name=name,
        creds_file=creds_file,
    )


def check_session_bridge_requirements() -> bool:
    """Service gate: tools only appear in the schema when NATS is reachable.

    Lazy-imports ``nats-py`` so this plugin's plugin.yaml can ship to users
    who don't have it installed yet (the install UX is the ``hermes plugins
    install session-bridge`` command which pulls the optional dep).
    """
    # First gate: optional dep present?
    try:
        import nats  # noqa: F401
    except ImportError:
        return False

    # Second gate: env vars set so we know where to connect?
    if not (os.environ.get("HERMES_NATS_URLS") or os.environ.get("NATS_URLS")
            or os.environ.get("HERMES_NATS_CREDS_FILE")):
        # No env at all — fall back to LAN-default 127.0.0.1:4222 ONLY if
        # the user has explicitly enabled the plugin in config.yaml (we are
        # being asked because they ran ``hermes plugins enable
        # session-bridge``). Default-gate-on-LAN-localhost would surprise
        # a developer whose broker IS on a different host.
        # The intent is: ``hermes plugins enable session-bridge`` flips a
        # config flag we check; without an env var OR config override, we
        # treat that as user-confirmed-only-when-they've-set-something and
        # return False to force them to be explicit.
        return False

    # Third gate: actual broker reachability (1s timeout). We don't want
    # to block tool schema generation while the gateway sits offline.
    try:
        client = _client()
        return asyncio.run(client.ping(timeout=1.0))
    except Exception as e:  # noqa: BLE001 — true "any failure is offline"
        logger.debug("session_bridge: NATS ping failed: %s", e)
        return False


def register(ctx) -> None:
    """Register the plugin — only the model tools. No CLI surface yet."""

    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset="session_bridge",
            schema=schema,
            handler=handler,
            check_fn=check_session_bridge_requirements,
            emoji=emoji,
        )
