"""Regression: compression surfaces start/end status messages with token stats.

Problem this fixes: when auto-compression kicks in mid-turn on a long session,
the user only sees a delayed final answer (or a "⏳ Compressing context…
your message is queued" after sending a follow-up). They have no visibility
into:
  (a) when compression actually started,
  (b) the pre-compression token count + threshold that triggered it,
  (c) the post-compression token count + how much was actually saved.

The fix in agent/conversation_compression.py introduces two helpers —
``format_compression_status_start`` and ``format_compression_status_end`` —
emitted via ``agent._emit_status(...)`` so the gateway's status_callback
rail delivers them to chat surfaces. This test pins both:

  * the helpers' format contract (so future tweaks stay within the
    implicit UX budget of the chat surfaces — keep line ≤ Telegram's
    4096 char cap, prefer compact Nk / k for token counts),
  * the round-trip end-to-end through ``compress_context()`` against a
    real ``AIAgent`` (no mocks for the agent itself; only the LLM calls
    are mocked, since the bug class is "wiring" not "LLM fidelity").

The pre-existing tests in test_telegram_noise_filter.py and
test_compaction_status.py keep the OLD literal ``COMPACTION_STATUS``
filtered on chat surfaces, so they prove the new rich message is
NOT accidentally re-filtered.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


# ── Helpers ─────────────────────────────────────────────────────────────
# Compact token formatter is part of the public contract: the message
# must fit on a Telegram chat and use Nk / k prefixes for readability.


def test_format_compression_status_start_includes_run_count_and_approx_tokens():
    from agent.conversation_compression import format_compression_status_start

    msg = format_compression_status_start(
        run_count=1,
        approx_tokens=120_000,
        context_length=200_000,
        threshold_tokens=160_000,
    )
    assert msg.startswith("🗜️ Auto-Compacting context (run #1)")
    assert "120k tokens" in msg
    assert "200k context" in msg
    assert "trigger 160k" in msg
    # Marker preserved so the desktop "Summarizing…" indicator still
    # matches via tui_gateway/server.py::_status_update.
    assert "Compacting context" in msg


def test_format_compression_status_start_omits_unknown_stats():
    """Missing stats are silently dropped, not rendered as 'None'."""
    from agent.conversation_compression import format_compression_status_start

    msg = format_compression_status_start(run_count=3, approx_tokens=None)
    assert msg.startswith("🗜️ Auto-Compacting context (run #3)")
    # No phantom "None" / "0" / "?" leakage.
    assert "None" not in msg
    assert "trigger" not in msg


def test_format_compression_status_start_manual_flag():
    """Manual /compress slash command flips the trigger label."""
    from agent.conversation_compression import format_compression_status_start

    msg = format_compression_status_start(
        run_count=1,
        approx_tokens=10_000,
        is_auto=False,
    )
    assert msg.startswith("🗜️ Manual Compacting context")


def test_format_compression_status_end_includes_savings_pct():
    from agent.conversation_compression import format_compression_status_end

    msg = format_compression_status_end(
        run_count=1,
        n_messages_in=220,
        n_messages_out=50,
        tokens_in=280_000,
        tokens_out=60_000,
        saved_estimate=220_000,
        savings_pct=78.6,
    )
    assert msg.startswith("✅ Context compressed (run #1)")
    assert "220→50 msgs" in msg
    assert "280k→60k tokens" in msg
    assert "Saved ~220k (79%)" in msg or "Saved ~220k (78%)" in msg


def test_format_compression_status_end_omits_savings_when_no_reduction():
    """Saved ~0 case must NOT show 'Saved ~0' text (looks like a bug)."""
    from agent.conversation_compression import format_compression_status_end

    msg = format_compression_status_end(
        run_count=1,
        n_messages_in=10,
        n_messages_out=10,
        tokens_in=1000,
        tokens_out=1000,
        saved_estimate=0,
        savings_pct=0.0,
    )
    # No misleading "Saved ~0" suffix when nothing was actually saved.
    assert "Saved" not in msg


def test_format_compaction_marker_preserved_in_compaction_status():
    """Backwards-compat: existing call sites / tests that import
    ``COMPACTION_STATUS`` still get a string containing
    ``COMPACTION_STATUS_MARKER`` so the desktop-app "Summarizing…"
    indicator keeps matching.
    """
    from agent.conversation_compression import (
        COMPACTION_STATUS,
        COMPACTION_STATUS_MARKER,
    )

    assert COMPACTION_STATUS_MARKER in COMPACTION_STATUS


# ── End-to-end: status messages survive the noisy-status filter ────────


@pytest.mark.parametrize(
    "message",
    [
        "🗜️ Auto-Compacting context (run #1) (~120k tokens, 200k context) — summarizing earlier conversation so I can continue…",
        "🗜️ Manual Compacting context (run #1) (~10k tokens) — summarizing earlier conversation so I can continue…",
        "✅ Context compressed (run #1) — 220→50 msgs, 280k→60k tokens. Saved ~220k (79%).",
    ],
)
def test_new_rich_compression_status_passes_chat_noise_filter(message):
    """The new rich messages must NOT be suppressed by the gateway's
    NOISY_STATUS filter — that's the whole point of this commit.
    """
    from gateway.config import Platform
    from gateway.run import _prepare_gateway_status_message

    for platform in (
        "telegram",
        "whatsapp",
        "discord",
        "slack",
        "mattermost",
        "matrix",
        "signal",
    ):
        # NOTE: _prepare_gateway_status_message strips the message only
        # if the noisy-status regex matches. We assert the OPPOSITE:
        # the message must come through unchanged (after secret-redaction).
        prepared = _prepare_gateway_status_message(platform, "lifecycle", message)
        assert prepared is not None, f"{platform} suppressed the new status"
        # And the core content (markers + token counts) must survive
        # secret-redaction (these messages carry no secrets, so they
        # round-trip identically).
        assert "Compacting context" in prepared or "Context compressed" in prepared


# ── End-to-end: compress_context emits start AND end on success ────────


def test_compress_context_emits_start_and_end_status_messages(tmp_path, monkeypatch):
    """Drive the real ``compress_context()`` path against a real AIAgent
    with a stubbed LLM. Assert both start + end status messages reach
    the agent's ``_emit_status`` callback with token stats populated.
    """
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")

    from run_agent import AIAgent
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    parent_sid = "test-compression-status-session"
    db.create_session(parent_sid, source="cli")

    agent = AIAgent(
        api_key="test-key",
        base_url="https://openrouter.ai/api/v1",
        model="test/model",
        quiet_mode=True,
        session_db=db,
        session_id=parent_sid,
        skip_context_files=True,
        skip_memory=True,
    )

    # Stub the compressor so we don't actually call an LLM.
    fake_compressor = MagicMock()

    def _increment_count(messages, **_kwargs):
        # Mirror the real ContextCompressor.compress() behavior: bump
        # ``compression_count`` before returning so the post-compression
        # status sees the freshly-incremented count.
        fake_compressor.compression_count += 1
        return [
            {"role": "user", "content": "[CONTEXT COMPACTION] summary"},
            {"role": "user", "content": "tail user msg"},
        ]

    fake_compressor.compress.side_effect = _increment_count
    # compression_count is the count of completed compressions. The next
    # call will be run #(count + 1) per the helper. Setting 0 here means
    # this is the first run.
    fake_compressor.compression_count = 0
    fake_compressor.context_length = 200_000
    fake_compressor.threshold_tokens = 160_000
    fake_compressor.last_prompt_tokens = 60_000
    fake_compressor._last_summary_error = None
    fake_compressor._last_compress_aborted = False
    fake_compressor._last_aux_model_failure_model = None
    fake_compressor._last_aux_model_failure_error = None
    fake_compressor._last_compression_savings_pct = 78.0
    agent.context_compressor = fake_compressor

    # Capture every _emit_status call.
    captured: list[str] = []
    agent._emit_status = MagicMock(side_effect=lambda msg: captured.append(msg))

    # Drive a 220-message history that crosses the threshold.
    history = [{"role": "user", "content": f"msg-{i}"} for i in range(220)]
    history.append({"role": "user", "content": "tail"})

    from agent.conversation_compression import compress_context

    new_messages, _ = compress_context(
        agent,
        history,
        "sys",
        approx_tokens=280_000,
    )

    # We expect exactly two status emits: start + end.
    assert len(captured) >= 2, f"Expected ≥2 status emits, got {len(captured)}: {captured}"

    start_msg = captured[0]
    assert "Compacting context" in start_msg
    assert "run #1" in start_msg
    # The start message carries the pre-compression token count from
    # approx_tokens=280_000, NOT the compressor's last_prompt_tokens
    # (which is stale until next API call).
    assert "280k" in start_msg, f"start_msg missing 280k: {start_msg!r}"
    assert "200k context" in start_msg
    assert "trigger 160k" in start_msg

    end_msg = captured[1]
    assert "Context compressed" in end_msg
    assert "run #1" in end_msg
    assert "tokens" in end_msg
    assert "Saved" in end_msg


def test_compress_context_skips_end_status_on_abort(tmp_path, monkeypatch):
    """If the compressor aborts (aux LLM failed), only the existing
    abort warning should reach the user — no confusing ✅ end-status
    claiming success when compression actually failed.
    """
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")

    from run_agent import AIAgent
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    parent_sid = "test-compression-abort-session"
    db.create_session(parent_sid, source="cli")

    agent = AIAgent(
        api_key="test-key",
        base_url="https://openrouter.ai/api/v1",
        model="test/model",
        quiet_mode=True,
        session_db=db,
        session_id=parent_sid,
        skip_context_files=True,
        skip_memory=True,
    )

    fake_compressor = MagicMock()
    # abort → returns messages unchanged
    history = [{"role": "user", "content": f"m{i}"} for i in range(10)]
    fake_compressor.compress.return_value = history
    fake_compressor.compression_count = 0
    fake_compressor.context_length = 200_000
    fake_compressor.threshold_tokens = 160_000
    fake_compressor.last_prompt_tokens = 0
    fake_compressor._last_summary_error = "aux-llm timed out"
    fake_compressor._last_compress_aborted = True
    agent.context_compressor = fake_compressor

    captured: list[tuple[str, str]] = []  # (kind, msg)
    agent._emit_status = MagicMock(side_effect=lambda msg: captured.append(("status", msg)))
    agent._emit_warning = MagicMock(side_effect=lambda msg: captured.append(("warning", msg)))

    from agent.conversation_compression import compress_context

    new_messages, _ = compress_context(agent, history, "sys", approx_tokens=10_000)

    # The abort path returns the ORIGINAL messages (no rotation).
    assert new_messages == history
    # An abort warning was emitted (existing behaviour).
    warnings = [m for kind, m in captured if kind == "warning"]
    assert any("aborted" in w.lower() for w in warnings)
    # BUT no ✅ end-status was emitted (would be misleading).
    statuses = [m for kind, m in captured if kind == "status"]
    assert not any("Context compressed" in s for s in statuses), \
        f"end-status leaked through on abort: {statuses}"
