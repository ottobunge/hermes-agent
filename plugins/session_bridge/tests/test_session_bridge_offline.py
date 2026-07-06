"""Offline unit tests for session-bridge — no broker required.

These tests assert the schema + handler shapes the model sees. Integration
tests that require a running NATS broker belong in
``tests/gateway/test_session_bridge_live.py`` (Phase 2).

NOTE on import path: ``plugins.session_bridge`` is a regular package
(with ``__init__.py``) inside the ``plugins/`` namespace-package
parentset. setuptools auto-discovery via ``[tool.setuptools.packages.find]``
only picks up subdirs WITHOUT ``__init__.py`` for the editable-install
mapper, so the plugin module isn't on the rebuilt edit-map after a fresh
tree. Tests therefore prepend the repo root and use the import path
``plugins.session_bridge.*`` directly — same module layout the plugin
loader resolves at runtime via ``hermes_cli.plugins.discover_plugins``.
"""

from __future__ import annotations

import asyncio
import json
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Bootstrap: make the ``plugins.session_bridge.*`` import path resolve from
# the repo root, same way the plugin loader does at runtime.
# ---------------------------------------------------------------------------

import os
# tests/test_*.py is at plugins/session_bridge/tests/ → 3 levels up = repo root.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Mirrors nats_client.STREAM_NAME — must match the constant in
# ``plugins/session_bridge/nats_client.py``. Tied to JetStream stream
# we provision on first connect.
STREAM_NAME_FROM_NATS_CLIENT = "SESSIONS"



def test_handlers_present_and_wired():
    """Both tools are listed in __init__._TOOLS with matching schemas."""
    from plugins.session_bridge import _TOOLS, tools as t

    names = {name for (name, *_rest) in _TOOLS}
    assert names == {"session_emit", "session_observe"}

    # Each tool's schema must declare the same ``name`` as the tuple key.
    for (name, schema, _h, _emoji) in _TOOLS:
        assert schema["name"] == name

    # Handlers must be callable.
    assert callable(t.handle_session_emit)
    assert callable(t.handle_session_observe)


def test_schemas_pass_basic_shape_invariants():
    """Each schema has the fields tool-calling parsers require.

    OpenAI-style: top-level is {name, description, parameters: {type, properties, required, additionalProperties}}.
    """
    from plugins.session_bridge.tools import (
        SESSION_EMIT_SCHEMA,
        SESSION_OBSERVE_SCHEMA,
    )

    for schema in (SESSION_EMIT_SCHEMA, SESSION_OBSERVE_SCHEMA):
        assert "name" in schema
        assert "description" in schema
        params = schema["parameters"]
        assert params["type"] == "object"
        assert "properties" in params
        assert "required" in params
        assert params["additionalProperties"] is False


def test_session_emit_requires_subject_and_payload():
    from plugins.session_bridge.tools import SESSION_EMIT_SCHEMA
    required = SESSION_EMIT_SCHEMA["parameters"]["required"]
    assert "subject" in required
    assert "payload" in required


def test_session_observe_modes_are_limited():
    from plugins.session_bridge.tools import SESSION_OBSERVE_SCHEMA
    mode_enum = SESSION_OBSERVE_SCHEMA["parameters"]["properties"]["mode"]["enum"]
    assert mode_enum == ["latest", "stream"]


def test_check_session_bridge_requirements_no_nats_module():
    """When nats-py is not installed, the gate returns False (no schema noise)."""
    from plugins.session_bridge import check_session_bridge_requirements

    with patch.dict(sys.modules, {"nats": None}):
        # ImportError path
        result = check_session_bridge_requirements()
        assert result is False


def test_check_session_bridge_requirements_no_env_defaults_to_false():
    """Plugin is OFF when no env var is set — operators opt in explicitly."""
    from plugins.session_bridge import check_session_bridge_requirements

    # Pretend nats-py is importable.
    fake_nats = MagicMock()
    fake_nats.__version__ = "2.15.0"
    with patch.dict(sys.modules, {"nats": fake_nats}):
        with patch.dict(os.environ, {}, clear=True):
            assert check_session_bridge_requirements() is False


@pytest.mark.asyncio
async def test_session_emit_via_underlying_coroutine_returns_seq():
    """The handler is sync — test the underlying coroutine path directly.

    The sync tool handler can't be called from inside pytest-asyncio
    because it wraps ``asyncio.run(_emit())`` which conflicts with the
    test runner's own running loop. The handler's job is to glue the
    sync tool layer to the async nats-py client; that's a one-liner
    we exercise by mimicking it here: run the same coroutine via await.
    """
    from plugins.session_bridge import tools as t
    from plugins.session_bridge import nats_client

    fake_ack = MagicMock()
    fake_ack.sequence = 42
    fake_ack.stream = "SESSIONS"

    fake_js = MagicMock()
    fake_js.publish = AsyncMock(return_value=fake_ack)
    fake_js.find_stream_name = AsyncMock(return_value=STREAM_NAME_FROM_NATS_CLIENT)
    fake_js.add_stream = AsyncMock()
    fake_js.pull_subscribe = AsyncMock()
    fake_js.stream_info = AsyncMock()

    fake_nc = MagicMock()
    fake_nc.jetstream = MagicMock(return_value=fake_js)
    fake_nc.close = AsyncMock()

    fake_nats_mod = MagicMock()
    fake_nats_mod.connect = AsyncMock(return_value=fake_nc)

    with patch.dict(sys.modules, {"nats": fake_nats_mod}):
        with patch.dict(
            os.environ,
            {"HERMES_NATS_URLS": "nats://127.0.0.1:4222", "HERMES_AGENT_ID": "hermes-conrad"},
            clear=True,
        ):
            client = t._broker()
            await client.connect()
            result = await client.publish(
                subject="peer.hermes-vmner.inbox",
                payload={"kind": "ping", "from": "hermes-conrad"},
                headers={"from": "hermes-conrad"},
                timeout=5,
            )
            await client.close()

    assert result["seq"] == 42
    assert result["stream"] == "SESSIONS"

    fake_js.publish.assert_awaited_once()
    call = fake_js.publish.await_args
    assert call.kwargs["subject"] == "peer.hermes-vmner.inbox"
    # Payload should be JSON-encoded UTF-8.
    body = call.kwargs["payload"]
    decoded = json.loads(body.decode("utf-8"))
    assert decoded == {"kind": "ping", "from": "hermes-conrad"}


def test_session_emit_sync_handler_uses_asyncio_run(monkeypatch):
    """Sync handler should call ``asyncio.run`` and return the result."""
    from plugins.session_bridge import tools as t

    captured = {}

    def fake_run(coro):
        captured["coro"] = coro
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(t.asyncio, "run", fake_run)

    fake_js = MagicMock()
    fake_js.publish = AsyncMock(
        return_value=MagicMock(sequence=99, stream="SESSIONS")
    )
    fake_js.find_stream_name = AsyncMock(return_value=STREAM_NAME_FROM_NATS_CLIENT)
    fake_js.add_stream = AsyncMock()
    fake_js.pull_subscribe = AsyncMock()
    fake_js.stream_info = AsyncMock()

    fake_nc = MagicMock()
    fake_nc.jetstream = MagicMock(return_value=fake_js)
    fake_nc.close = AsyncMock()

    fake_nats_mod = MagicMock()
    fake_nats_mod.connect = AsyncMock(return_value=fake_nc)

    with patch.dict(sys.modules, {"nats": fake_nats_mod}):
        with patch.dict(
            os.environ,
            {"HERMES_NATS_URLS": "nats://127.0.0.1:4222", "HERMES_AGENT_ID": "hermes-conrad"},
            clear=True,
        ):
            result = t.handle_session_emit(
                subject="peer.hermes-vmner.inbox",
                payload={"kind": "ping"},
                timeout=5,
            )

    assert result == {
        "ok": True,
        "subject": "peer.hermes-vmner.inbox",
        "seq": 99,
        "stream": "SESSIONS",
    }
    assert captured["coro"] is not None


@pytest.mark.asyncio
async def test_session_observe_underlying_coroutine_handles_empty():
    """Same pattern as test_session_emit_via_underlying_coroutine_returns_seq
    but for the observe path.
    """
    from plugins.session_bridge import tools as t

    fake_sub = MagicMock()
    fake_sub.fetch = AsyncMock(side_effect=Exception("timeout"))
    fake_msg = MagicMock()
    fake_msg.ack = AsyncMock()

    fake_js = MagicMock()
    fake_js.find_stream_name = AsyncMock(return_value=STREAM_NAME_FROM_NATS_CLIENT)
    fake_js.add_stream = AsyncMock()
    fake_js.pull_subscribe = AsyncMock(return_value=fake_sub)
    fake_js.stream_info = AsyncMock()

    fake_nc = MagicMock()
    fake_nc.jetstream = MagicMock(return_value=fake_js)
    fake_nc.close = AsyncMock()

    fake_nats_mod = MagicMock()
    fake_nats_mod.connect = AsyncMock(return_value=fake_nc)

    with patch.dict(sys.modules, {"nats": fake_nats_mod}):
        with patch.dict(os.environ, {"HERMES_NATS_URLS": "nats://127.0.0.1:4222"}, clear=True):
            client = t._broker()
            await client.connect()
            result = await client.observe(
                subject="peer.hermes-vmner.inbox",
                mode="latest",
                timeout=0.1,
            )
            await client.close()

    assert result["message"] is None
    assert "consumer" in result
