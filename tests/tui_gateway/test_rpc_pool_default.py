"""Tests for TUI RPC pool worker sizing."""


def test_rpc_pool_default_is_cpu_adaptive(monkeypatch):
    import tui_gateway.server as server

    monkeypatch.delenv("HERMES_TUI_RPC_POOL_WORKERS", raising=False)
    monkeypatch.setattr(server.os, "cpu_count", lambda: 16)

    assert server._resolve_rpc_pool_workers() == 32


def test_rpc_pool_env_override_can_exceed_default(monkeypatch):
    import tui_gateway.server as server

    monkeypatch.setenv("HERMES_TUI_RPC_POOL_WORKERS", "40")

    assert server._resolve_rpc_pool_workers() == 40


def test_rpc_pool_env_override_can_lower_default(monkeypatch):
    import tui_gateway.server as server

    monkeypatch.setenv("HERMES_TUI_RPC_POOL_WORKERS", "4")

    assert server._resolve_rpc_pool_workers() == 4


def test_rpc_pool_invalid_env_falls_back_to_default(monkeypatch):
    import tui_gateway.server as server

    monkeypatch.setenv("HERMES_TUI_RPC_POOL_WORKERS", "nope")
    monkeypatch.setattr(server.os, "cpu_count", lambda: 2)

    assert server._resolve_rpc_pool_workers() == 8


def test_rpc_pool_module_uses_resolved_worker_count():
    import tui_gateway.server as server

    assert server._rpc_pool_workers == server._resolve_rpc_pool_workers()
