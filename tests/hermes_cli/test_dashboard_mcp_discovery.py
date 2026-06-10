"""Tests for MCP discovery in Dashboard/Desktop server mode."""

from unittest.mock import patch


def _stub_uvicorn_run(monkeypatch):
    import uvicorn

    captured = {}

    def fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(uvicorn, "run", fake_run)
    return captured


def test_start_server_starts_background_mcp_discovery(monkeypatch):
    from hermes_cli import web_server

    _stub_uvicorn_run(monkeypatch)

    with patch("hermes_cli.mcp_startup.start_background_mcp_discovery") as discover:
        web_server.start_server(host="127.0.0.1", port=9119, open_browser=False, allow_public=False)

    discover.assert_called_once()
    assert discover.call_args.kwargs["thread_name"] == "dashboard-mcp-discovery"


def test_start_server_ignores_mcp_discovery_failures(monkeypatch):
    from hermes_cli import web_server

    _stub_uvicorn_run(monkeypatch)

    with patch("hermes_cli.mcp_startup.start_background_mcp_discovery", side_effect=RuntimeError("boom")):
        web_server.start_server(host="127.0.0.1", port=9119, open_browser=False, allow_public=False)


def test_wait_for_mcp_discovery_falls_back_to_shared_startup_thread():
    import tui_gateway.entry as entry

    saved = entry._mcp_discovery_thread
    try:
        entry._mcp_discovery_thread = None
        with patch("hermes_cli.mcp_startup.wait_for_mcp_discovery") as wait:
            entry.wait_for_mcp_discovery(timeout=0.5)
        wait.assert_called_once_with(timeout=0.5)
    finally:
        entry._mcp_discovery_thread = saved
