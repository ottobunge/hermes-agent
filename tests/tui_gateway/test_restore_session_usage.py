"""Tests for restoring usage counters on session resume."""

import pytest


class FakeAgent:
    def __init__(self):
        self.session_input_tokens = 0
        self.session_output_tokens = 0
        self.session_cache_read_tokens = 0
        self.session_cache_write_tokens = 0
        self.session_reasoning_tokens = 0
        self.session_total_tokens = 0
        self.session_api_calls = 0
        self.session_estimated_cost_usd = 0.0
        self.session_cost_status = "unknown"


def test_restore_session_usage_populates_agent_counters():
    from tui_gateway.server import _restore_session_usage

    agent = FakeAgent()

    _restore_session_usage(
        agent,
        {
            "input_tokens": 120,
            "output_tokens": 30,
            "cache_read_tokens": 8,
            "cache_write_tokens": 2,
            "reasoning_tokens": 15,
            "api_call_count": 7,
            "estimated_cost_usd": 0.042,
            "cost_status": "estimated",
        },
    )

    assert agent.session_input_tokens == 120
    assert agent.session_output_tokens == 30
    assert agent.session_cache_read_tokens == 8
    assert agent.session_cache_write_tokens == 2
    assert agent.session_reasoning_tokens == 15
    assert agent.session_total_tokens == 175
    assert agent.session_api_calls == 7
    assert agent.session_estimated_cost_usd == pytest.approx(0.042)
    assert agent.session_cost_status == "estimated"


def test_restore_session_usage_defaults_missing_values_to_zero():
    from tui_gateway.server import _restore_session_usage

    agent = FakeAgent()

    _restore_session_usage(agent, {"input_tokens": None, "output_tokens": 50})

    assert agent.session_input_tokens == 0
    assert agent.session_output_tokens == 50
    assert agent.session_total_tokens == 50
    assert agent.session_api_calls == 0
    assert agent.session_estimated_cost_usd == 0.0
    assert agent.session_cost_status == "unknown"


def test_get_usage_reflects_restored_counters():
    from tui_gateway.server import _get_usage, _restore_session_usage

    agent = FakeAgent()
    _restore_session_usage(
        agent,
        {
            "input_tokens": 5,
            "output_tokens": 4,
            "cache_read_tokens": 3,
            "cache_write_tokens": 2,
            "reasoning_tokens": 1,
            "api_call_count": 2,
        },
    )

    usage = _get_usage(agent)

    assert usage["input"] == 5
    assert usage["output"] == 4
    assert usage["cache_read"] == 3
    assert usage["cache_write"] == 2
    assert usage["reasoning"] == 1
    assert usage["total"] == 15
    assert usage["calls"] == 2


def test_get_usage_reflects_restored_cost():
    from tui_gateway.server import _get_usage, _restore_session_usage

    agent = FakeAgent()
    _restore_session_usage(
        agent,
        {
            "estimated_cost_usd": 0.125,
            "cost_status": "estimated",
        },
    )

    usage = _get_usage(agent)

    assert usage["cost_usd"] == pytest.approx(0.125)
    assert usage["cost_status"] == "estimated"
