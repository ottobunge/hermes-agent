"""GatewayRunner.publish_internal_notification — the platform side-channel seam.

Mirror of enqueue_internal_session_event, but for USER-visible system
notifications (back-channel traffic, lifecycle events) published by
gateway code. The contract:
  * resolves the session's adapter via the session store (plugins never
    touch adapters directly)
  * delegates to ``BasePlatformAdapter.publish_system_notification``
    with the session's recorded origin as ``source``
  * NEVER enters session history — the base default delivers via
    ``adapter.send`` (pure platform delivery, same path as cron notices)
  * fails soft (returns False, no raise) on unknown keys / missing
    adapters / adapter errors
"""

import asyncio
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from gateway.config import Platform
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


class RecordingAdapter(RestartTestAdapter):
    """Captures publish_system_notification calls (seam-level tests)."""

    def __init__(self):
        super().__init__()
        self.notifications = []

    async def publish_system_notification(
        self, session_key, text, kind="info", source=None
    ):
        self.notifications.append((session_key, text, kind, source))


@pytest.mark.asyncio
async def test_notification_delegates_to_adapter_with_origin_source():
    adapter = RecordingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    ok = await runner.publish_internal_notification(
        session_key, "🔄 back-channel from peer\nhi", kind="back_channel_in"
    )
    assert ok is True
    assert adapter.notifications == [
        (session_key, "🔄 back-channel from peer\nhi", "back_channel_in", source)
    ]


@pytest.mark.asyncio
async def test_kind_defaults_to_info():
    adapter = RecordingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    assert await runner.publish_internal_notification(session_key, "x") is True
    assert adapter.notifications[0][2] == "info"


@pytest.mark.asyncio
async def test_unknown_session_key_returns_false_no_raise():
    adapter = RecordingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    ok = await runner.publish_internal_notification("nope:key", "x")
    assert ok is False
    assert adapter.notifications == []


@pytest.mark.asyncio
async def test_no_adapter_for_platform_returns_false():
    adapter = RecordingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)
    runner.adapters = {}  # platform gone (adapter disconnected)

    assert await runner.publish_internal_notification(session_key, "x") is False


@pytest.mark.asyncio
async def test_session_lookup_error_returns_false():
    adapter = RecordingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)
    runner.session_store.get_entry = MagicMock(side_effect=RuntimeError("boom"))

    assert await runner.publish_internal_notification(session_key, "x") is False


@pytest.mark.asyncio
async def test_adapter_error_swallowed_returns_false():
    class ExplodingAdapter(RestartTestAdapter):
        async def publish_system_notification(self, *a, **kw):
            raise RuntimeError("platform on fire")

    adapter = ExplodingAdapter()
    source = make_restart_source()
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    assert await runner.publish_internal_notification(session_key, "x") is False


# ---------------------------------------------------------------------------
# BasePlatformAdapter.publish_system_notification — default implementation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_base_default_delivers_via_send_to_origin_chat():
    adapter = RestartTestAdapter()  # records send() calls
    source = make_restart_source(chat_id="424242")
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    ok = await runner.publish_internal_notification(
        session_key, "✅ back-channel established with peer", kind="lifecycle"
    )
    assert ok is True
    assert adapter.sent == ["✅ back-channel established with peer"]
    chat_id, _content, _metadata = adapter.sent_calls[0]
    assert chat_id == "424242"


@pytest.mark.asyncio
async def test_base_default_routes_thread_metadata():
    adapter = RestartTestAdapter()
    source = make_restart_source(chat_id="1", chat_type="group", thread_id="77")
    session_key = build_session_key(source)
    runner = _make_runner_with_entry(adapter, source, session_key)

    assert await runner.publish_internal_notification(session_key, "x") is True
    _chat_id, _content, metadata = adapter.sent_calls[0]
    assert metadata is not None and metadata.get("thread_id") == "77"


@pytest.mark.asyncio
async def test_base_default_without_source_logs_only():
    adapter = RestartTestAdapter()
    await adapter.publish_system_notification("some:key", "text", kind="info")
    assert adapter.sent == []  # nothing to route to — log-only, no raise


@pytest.mark.asyncio
async def test_base_default_send_failure_swallowed():
    class FailingSendAdapter(RestartTestAdapter):
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            raise RuntimeError("send exploded")

    adapter = FailingSendAdapter()
    source = make_restart_source()
    await adapter.publish_system_notification(
        "some:key", "text", kind="info", source=source
    )  # must not raise
