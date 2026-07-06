"""GatewayRunner.enqueue_internal_session_event — the adapter-FIFO seam.

The helper is the ONLY sanctioned injection point for plugin-originated
synthetic turns (session-routing back-channel). It must:
  * route through ``BasePlatformAdapter.handle_message`` (Level-1 guard +
    ``_pending_messages`` FIFO), never ``_handle_message`` directly
  * force ``event.internal = True``
  * default ``event.source`` to the session's recorded origin
  * fail soft (return False, no raise) on unknown keys / missing adapters
"""

import asyncio
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionEntry, build_session_key
from tests.gateway.restart_test_helpers import (
    RestartTestAdapter,
    make_restart_source,
)


def _make_runner_with_entry(adapter, source, session_key):
    runner = object.__new__(GatewayRunner)
    runner._background_tasks = set()
    runner.adapters = {Platform.TELEGRAM: adapter}
    entry = SessionEntry(
        session_key=session_key,
        session_id="20260706_000000_deadbeef",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        origin=source,
        platform=Platform.TELEGRAM,
    )
    store = MagicMock()
    store.get_entry = lambda key: entry if key == session_key else None
    runner.session_store = store
    return runner


@pytest.mark.asyncio
async def test_unknown_session_key_returns_false():
    adapter = RestartTestAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    event = MessageEvent(text="hello", source=None)
    assert runner.enqueue_internal_session_event("nope:key", event) is False


@pytest.mark.asyncio
async def test_no_adapter_for_platform_returns_false():
    adapter = RestartTestAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)
    runner.adapters = {}  # platform gone (adapter disconnected)

    event = MessageEvent(text="hello", source=None)
    assert runner.enqueue_internal_session_event(session_key, event) is False


@pytest.mark.asyncio
async def test_idle_session_dispatches_through_adapter():
    adapter = RestartTestAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    handled = asyncio.Event()
    seen = {}

    async def _handler(event):
        seen["event"] = event
        handled.set()
        return None

    adapter.set_message_handler(_handler)

    event = MessageEvent(text="[back-channel from peer] hi", source=None)
    assert runner.enqueue_internal_session_event(session_key, event) is True
    await asyncio.wait_for(handled.wait(), timeout=5.0)

    assert seen["event"].internal is True
    assert seen["event"].source is source  # origin fallback applied


@pytest.mark.asyncio
async def test_busy_session_lands_in_pending_messages_fifo():
    adapter = RestartTestAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    release = asyncio.Event()

    async def _block(event):
        await release.wait()
        return None

    adapter.set_message_handler(_block)

    # Occupy the session with a platform message.
    first = MessageEvent(text="occupying turn", source=source, message_id="1")
    await adapter.handle_message(first)
    await asyncio.sleep(0)
    assert session_key in adapter._active_sessions

    # Inject the internal event while busy: must queue, not run.
    event = MessageEvent(text="[back-channel from peer] hi", source=None)
    assert runner.enqueue_internal_session_event(session_key, event) is True
    # Allow the spawned handle_message task to reach the busy branch.
    for _ in range(10):
        if session_key in adapter._pending_messages:
            break
        await asyncio.sleep(0.01)

    pending = adapter._pending_messages.get(session_key)
    assert pending is not None
    assert "[back-channel from peer] hi" in pending.text
    assert pending.internal is True

    release.set()
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_event_with_explicit_source_keeps_it():
    adapter = RestartTestAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    handled = asyncio.Event()
    seen = {}

    async def _handler(event):
        seen["event"] = event
        handled.set()
        return None

    adapter.set_message_handler(_handler)

    explicit = make_restart_source(chat_id="999999")
    event = MessageEvent(text="hi", source=explicit)
    assert runner.enqueue_internal_session_event(session_key, event) is True
    await asyncio.wait_for(handled.wait(), timeout=5.0)
    assert seen["event"].source is explicit
