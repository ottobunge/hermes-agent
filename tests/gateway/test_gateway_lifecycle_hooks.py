"""Gateway lifecycle plugin hooks (on_gateway_start / on_gateway_stop).

Covers the dispatch seam added for session-routing v0.3.0:
  * awaitable hook results are awaited on the gateway loop
  * a raising callback / awaitable never blocks startup or shutdown
  * both hook names are valid registrations
"""

import asyncio
from unittest.mock import patch

import pytest

from gateway.run import GatewayRunner
from hermes_cli.plugins import VALID_HOOKS, PluginManager


def _bare_runner() -> GatewayRunner:
    runner = object.__new__(GatewayRunner)
    return runner


class TestValidHooks:
    def test_gateway_lifecycle_hooks_are_valid(self):
        assert "on_gateway_start" in VALID_HOOKS
        assert "on_gateway_stop" in VALID_HOOKS


class TestInvokePluginGatewayLifecycleHook:
    @pytest.mark.asyncio
    async def test_awaitable_results_are_awaited(self):
        runner = _bare_runner()
        ran = asyncio.Event()

        async def _async_setup():
            ran.set()

        with patch(
            "hermes_cli.plugins.invoke_hook",
            return_value=[_async_setup()],
        ) as invoke_mock:
            await runner._invoke_plugin_gateway_lifecycle_hook("on_gateway_start")

        assert ran.is_set()
        invoke_mock.assert_called_once()
        assert invoke_mock.call_args.args == ("on_gateway_start",)
        assert invoke_mock.call_args.kwargs["gateway"] is runner

    @pytest.mark.asyncio
    async def test_raising_awaitable_does_not_propagate(self):
        runner = _bare_runner()
        survived = asyncio.Event()

        async def _boom():
            raise RuntimeError("plugin exploded")

        async def _ok():
            survived.set()

        with patch(
            "hermes_cli.plugins.invoke_hook",
            return_value=[_boom(), _ok()],
        ):
            # Must not raise, and must still await the healthy plugin.
            await runner._invoke_plugin_gateway_lifecycle_hook("on_gateway_stop")

        assert survived.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_failure_does_not_propagate(self):
        runner = _bare_runner()
        with patch(
            "hermes_cli.plugins.invoke_hook",
            side_effect=RuntimeError("registry corrupt"),
        ):
            await runner._invoke_plugin_gateway_lifecycle_hook("on_gateway_start")

    @pytest.mark.asyncio
    async def test_sync_results_are_ignored(self):
        runner = _bare_runner()
        with patch(
            "hermes_cli.plugins.invoke_hook",
            return_value=[{"some": "dict"}, "text"],
        ):
            await runner._invoke_plugin_gateway_lifecycle_hook("on_gateway_start")


class TestHookRegistrationRoundTrip:
    def test_manager_invokes_gateway_start_callbacks(self):
        mgr = PluginManager()
        seen = {}

        def _cb(gateway=None, **kwargs):
            seen["gateway"] = gateway
            return None

        mgr._hooks.setdefault("on_gateway_start", []).append(_cb)
        sentinel = object()
        results = mgr.invoke_hook("on_gateway_start", gateway=sentinel)
        assert seen["gateway"] is sentinel
        assert results == []

    def test_raising_sync_callback_is_swallowed(self):
        mgr = PluginManager()

        def _boom(**kwargs):
            raise RuntimeError("bad plugin")

        mgr._hooks.setdefault("on_gateway_stop", []).append(_boom)
        # invoke_hook wraps each callback; must not raise.
        assert mgr.invoke_hook("on_gateway_stop", gateway=object()) == []
