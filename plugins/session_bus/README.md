# session-bus

> Single NATS-based inter-agent bus for Hermes Agent.
> Replaces `session-bridge` + `session-routing` with one unified plugin.

## Why

Two plugins were originally built because broadcast and routed messages
have different consumer patterns (per-tool ephemeral vs persistent
subscriber). They shared the same NATS JetStream broker, the same
stream (`SESSIONS`), and the same nats-py dependency. Maintaining them as
two plugins meant duplicated NATS client code, two `pyproject.toml`
extras, two allowlist entries in `tools/lazy_deps.py`, and operator
friction (two plugins to install for one product).

This plugin unifies them under a single namespace while keeping the
two-toolsets split for operator-level config gating (`hermes tools`
selectively enables `session_bus_broadcast` or `session_bus_routing`).

## Toolsets

### `session_bus_broadcast` — typed event bus

| Tool | Purpose |
| --- | --- |
| `bus_emit` | Publish a JSON event to the bus. Subject prefix `from.<HERMES_AGENT_ID>.` is auto-applied. |
| `bus_observe` | Pull up to N messages from a JetStream subject. Stateless (per-call subscription, ack, close). |

Use for: heartbeat pings, status broadcasts, low-cardinality telemetry
that doesn't need addressing. Subscribers MUST scope to a single trusted
sender (`from.<trusted-agent>.>`) — never use a cross-sender wildcard.

### `session_bus_routing` — addressed point-to-point delivery

| Tool | Purpose |
| --- | --- |
| `bus_handle` | Return THIS session's canonical address (`<gateway_id>/<session_key>`) so a peer agent can target it. |
| `bus_route_send` | Deliver a typed payload to another live session. Recipient's allow-list is broker-side enforced. |
| `bus_route_list` | Enumerate live sessions with freshness-filtered presence. |
| `bus_establish` | 3-way handshake opening a typed channel to a peer session. |

Use for: Session A on Hermes-X wants to send a message to Session C on
Hermes-Y with proper attribution, ack, and visibility on the Telegram
thread (system notifications via `publish_internal_notification`).

## Install

```bash
uv pip install 'hermes-agent[session-bus]'
# or
hermes plugins install session-bus
```

The plugin auto-hides all 6 tools when the broker is unreachable, so
installing without a running NATS broker is safe — the agent sees "tool
not available" instead of mid-turn failures.

## Configuration

```yaml
# ~/.hermes/config.yaml
session_routing:                  # legacy key still respected for one cycle
  gateway_id: gw-thinkpad
  peers: [gw-agent-vm]
  presence_ttl_seconds: 3600
```

The deprecation shim in `plugins/session_routing/__init__.py` translates
the old key to `session_bus` for the duration of the migration cycle.

## Renamed tools

| Old name | New name |
| --- | --- |
| `session_emit` | `bus_emit` |
| `session_observe` | `bus_observe` |
| `session_handle` | `bus_handle` |
| `session_route_send` | `bus_route_send` |
| `session_routing_list` | `bus_route_list` |
| `session_establish` | `bus_establish` |

Update any skills, prompts, or hooks that reference the old names.
The legacy `plugins/session_bridge/` and `plugins/session_routing/`
directories remain as thin deprecation shims that re-export from
`session_bus`, but the old tool names are NOT registered anymore.

## Deprecation timeline

- **v1.0.0** (this release): both old plugin dirs kept as shims.
  Old tool names NOT registered. DeprecationWarning emitted by both
  shims on first import.
- **v1.1.0** (next minor): shims emit a louder warning every session.
- **v1.2.0**: shims removed. Old plugin dirs deleted from the tree.
  Operators must remove `session_bridge` / `session_routing` from
  their `~/.hermes/config.yaml` and update skills/prompts.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       Hermes gateway                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  plugin.register(ctx) ─┐                                               │
│                         │                                               │
│                         ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────┐ │
│  │                    session-bus                                    │ │
│  │                                                                   │ │
│  │  broadcast toolset:   bus_emit ─┐                               │ │
│  │                       bus_observe │                              │ │
│  │                                  │                              │ │
│  │  routing toolset:     bus_handle  │                             │ │
│  │                       bus_route_send  │                          │ │
│  │                       bus_route_list  │                          │ │
│  │                       bus_establish ─┘                          │ │
│  │                                                                   │ │
│  │  shared infra:                                                    │ │
│  │   - nats_client.py (one JetStream client, two connection modes) │ │
│  │   - address.py, protocol.py, channels.py, dispatcher.py,        │ │
│  │     inbox.py, presence.py, runtime.py, allow.py                 │ │
│  │   - one JetStream stream "SESSIONS"                              │ │
│  │   - two KV buckets: session_presence, session_allow              │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   NATS JetStream      │
                    │   broker              │
                    │   (LAN + Courier VPN)│
                    └───────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
        Hermes-X        Hermes-Y        Hermes-Z
        (thinkpad)      (agent-vm)      (...)
```

## Subject conventions

```
from.<agent_id>.peer.<peer_id>.inbox           # point-to-point inbox
from.<agent_id>.session.<session_id>.<verb>    # per-session signals
from.<agent_id>.system.<topic>                # broadcasts
```

`bus_emit` auto-prefixes `from.<HERMES_AGENT_ID>.`. `bus_observe`
subscribers MUST scope to `from.<trusted-agent>.>` to maintain fan-in
isolation (one agent cannot impersonate another).

## Migration checklist for operators

1. Install `hermes-agent[session-bus]` (replaces both
   `session-bridge` and `session-routing` extras).
2. Update any skills or prompt fragments referencing the old tool
   names. Search: `session_emit`, `session_observe`, `session_handle`,
   `session_route_send`, `session_routing_list`, `session_establish`.
3. Confirm `hermes tools list` shows all 6 `bus_*` tools under the
   `session_bus_broadcast` and `session_bus_routing` toolsets.
4. Remove `session_bridge` and `session_routing` entries from
   `~/.hermes/config.yaml` if any.
5. After v1.2.0 lands, drop the `[session-bridge]` / `[session-routing]`
   extras from your `pyproject.toml` install command.

## Authoring policy

This plugin is the canonical implementation of the "two-layer
architecture" rule from Hermes AGENTS.md: user-visible threads (Telegram
etc.) on the surface, NATS-based typed events for off-thread agent
coordination that MUST NOT surface on user-visible channels. Both
toolsets share the same broker connection so installing one does NOT
double the operator's quota.
