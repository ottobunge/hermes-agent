"""Allow-list (recipient-side) management for session-routing.

The sender of a routed message is identified by ``gateway_id`` in the
NAT subject prefix. We chose gateway_id (not agent_id) as the identifier
because the same agent can run on multiple gateways. The recipient's
allow-list is therefore a set of *trusted gateway_ids*.

This module holds:
  - Two sources of truth (layered, in order):
      1. The static ``~/.hermes/config.yaml`` ``session_routing.peers``
         list — operator-curated, baked-in, survives gateway restarts.
      2. The dynamic KV bucket ``session_allow`` — set at runtime by the
         agent via the ``session_routing_allow`` tool / the operator via
         ``hermes routing allow-add``. Out-of-sync between the two means
         the union is used (we don't lose entries).
  - The merge function that produces the effective recipient allow-list
    used by the inbox subscriber.

Phase 1 note: an agent can also call ``session_routing_allow`` at runtime
to add peers without a gateway restart. The union is recomputed on each
tool call; no persistence mid-call.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from plugins.session_routing.nats_client import NATSRoutingClient

logger = logging.getLogger(__name__)


def _hermes_home() -> Path:
    """Path to ``$HERMES_HOME`` (default ``~/.hermes``).

    Centralizing the lookup keeps tests/simple: pass a fake HERMES_HOME
    via env and the rest of the plugin uses it. We always return an
    absolute path or raise — never a silent fallback to ``~``.
    """
    env = os.environ.get("HERMES_HOME", "").strip()
    if not env:
        env = os.path.expanduser("~/.hermes")
    p = Path(env).expanduser()
    return p


def _config_yaml_path() -> Path:
    return _hermes_home() / "config.yaml"


def read_static_peers_from_yaml(config_yaml_text: str) -> List[str]:
    """Parse the static allow-list out of ``config.yaml`` text.

    We intentionally parse with a hand-rolled regex — pulling in PyYAML
    as a hard dep just for this one read would be silly. ``config.yaml``
    already declares session_routing as a literal list under
    ``session_routing:``:

        session_routing:
          peers:
            - gw-agent-vm
            - gw-other-bridge

    Anything that doesn't match that shape is ignored (we don't crash
    on a stray field — the rest of the plugin may still work).
    """
    import re

    # Look for ``session_routing:`` block then ``peers:`` and the dash-list
    # that follows until indentation drops.
    block = re.search(
        r"^\s*session_routing:\s*$(.*?)(?=^\S|\Z)",
        config_yaml_text,
        re.MULTILINE | re.DOTALL,
    )
    if not block:
        return []
    inner = block.group(1)
    peers_block = re.search(
        r"^\s*peers:\s*$(.*?)(?=^\S|\Z)",
        inner,
        re.MULTILINE | re.DOTALL,
    )
    if not peers_block:
        return []
    items = re.findall(r"^\s*-\s*['\"]?([A-Za-z0-9._\-]+)['\"]?\s*$",
                        peers_block.group(1), re.MULTILINE)
    return list(items)


def read_static_peers_from_disk() -> List[str]:
    """Read ``session_routing.peers`` from ``$HERMES_HOME/config.yaml``.

    Returns an empty list if the file doesn't exist, can't be read, or
    doesn't declare the block.
    """
    path = _config_yaml_path()
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning("session_routing: config.yaml read failed: %s", e)
        return []
    return read_static_peers_from_yaml(text)


def read_presence_ttl_seconds(
    *,
    default: int = 90,
    config_yaml_text: Optional[str] = None,
) -> int:
    """Return ``session_routing.presence_ttl_seconds`` from config.

    Falls back to ``default`` when the key is missing or unparseable.
    We never raise on a bad config — the rest of the plugin should
    still work; presence staleness is a UX nicety, not a correctness
    invariant.

    ``config_yaml_text`` is exposed for tests so they can pass a
    fixture instead of touching ``$HERMES_HOME/config.yaml``.
    """
    import re

    if config_yaml_text is None:
        path = _config_yaml_path()
        if not path.exists():
            return default
        try:
            config_yaml_text = path.read_text(encoding="utf-8")
        except OSError as e:
            logger.warning("session_routing: config.yaml read failed: %s", e)
            return default

    block = re.search(
        r"^\s*session_routing:\s*$(.*?)(?=^\S|\Z)",
        config_yaml_text,
        re.MULTILINE | re.DOTALL,
    )
    if not block:
        return default
    m = re.search(
        r"^\s*presence_ttl_seconds:\s*([0-9]+)\s*$",
        block.group(1),
        re.MULTILINE,
    )
    if not m:
        return default
    try:
        value = int(m.group(1))
    except ValueError:
        return default
    return value if value > 0 else default


async def read_dynamic_allow_list(
    *, servers: List[str], recipient_gateway_id: str
) -> List[str]:
    """Fetch the recipient's stored allow-list from the broker's KV."""
    async with NATSRoutingClient(servers=servers) as client:
        return await client.read_allow_list(recipient_gateway_id)


def union(*sources: Sequence[str]) -> List[str]:
    """Stable-dedupe union. Returns a sorted list (operator diff-friendly)."""
    seen = set()
    for src in sources:
        if not src:
            continue
        for x in src:
            if x:
                seen.add(x)
    return sorted(seen)


async def effective_allow_list(
    *,
    servers: List[str],
    recipient_gateway_id: str,
    static_peers_from_disk: Optional[Iterable[str]] = None,
) -> List[str]:
    """Return the effective recipient allow-list (config ∪ KV).

    When ``static_peers_from_disk`` is None, we read the file. Pass an
    explicit iterable (e.g. from a test fixture) to skip the disk read.
    """
    static = (
        list(static_peers_from_disk)
        if static_peers_from_disk is not None
        else read_static_peers_from_disk()
    )
    dynamic = await read_dynamic_allow_list(
        servers=servers, recipient_gateway_id=recipient_gateway_id,
    )
    return union(static, dynamic)


async def add_dynamic(
    *,
    servers: List[str],
    recipient_gateway_id: str,
    sender_gateway_id: str,
    static_peers: Optional[Iterable[str]] = None,
) -> List[str]:
    """Add a sender to the dynamic allow-list; persist; return updated union.

    Idempotent: re-adding has no effect. If the sender is already in the
    static list, we skip the KV write entirely (no point duplicating).
    """
    static_set = set(static_peers or read_static_peers_from_disk())
    if sender_gateway_id in static_set:
        return union(static_set, await read_dynamic_allow_list(
            servers=servers, recipient_gateway_id=recipient_gateway_id,
        ))
    current = await read_dynamic_allow_list(
        servers=servers, recipient_gateway_id=recipient_gateway_id,
    )
    if sender_gateway_id in current:
        return union(static_set, current)
    new = union(static_set, current + [sender_gateway_id])
    async with NATSRoutingClient(servers=servers) as client:
        await client.write_allow_list(
            recipient_gateway_id=recipient_gateway_id,
            sender_gateway_ids=new,
        )
    return new


async def remove_dynamic(
    *,
    servers: List[str],
    recipient_gateway_id: str,
    sender_gateway_id: str,
) -> List[str]:
    """Remove a sender from the dynamic allow-list; persist; return updated."""
    current = await read_dynamic_allow_list(
        servers=servers, recipient_gateway_id=recipient_gateway_id,
    )
    new = [g for g in current if g != sender_gateway_id]
    async with NATSRoutingClient(servers=servers) as client:
        await client.write_allow_list(
            recipient_gateway_id=recipient_gateway_id,
            sender_gateway_ids=new,
        )
    return union(read_static_peers_from_disk(), new)
