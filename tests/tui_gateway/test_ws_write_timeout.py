"""Tests for WebSocket write timeout configuration."""


def test_ws_write_timeout_allows_slow_windows_scheduling():
    from tui_gateway import ws

    assert ws._WS_WRITE_TIMEOUT_S == 30.0
