from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from gateway.config import Platform

from .adapter import WakewordAdapter, check_requirements, validate_config


class PluginContext(Protocol):
    def register_platform(self, **kwargs: Any) -> None: ...


async def _start_adapter(gateway: Any = None, **_kwargs: Any) -> None:
    """Start the gateway-owned adapter if core lifecycle has not done so."""
    adapter = getattr(gateway, "adapters", {}).get(Platform("wakeword"))
    if adapter is not None and not adapter.is_connected:
        await adapter.connect()


async def _stop_adapter(gateway: Any = None, **_kwargs: Any) -> None:
    """Stop the gateway-owned adapter before plugin shutdown completes."""
    adapter = getattr(gateway, "adapters", {}).get(Platform("wakeword"))
    if adapter is not None and adapter.is_connected:
        await adapter.disconnect()


def register(ctx: PluginContext) -> None:
    """Register the wakeword adapter factory and lifecycle delegates."""
    ctx.register_platform(
        name="wakeword", label="Wakeword",
        adapter_factory=lambda config: WakewordAdapter(config),
        check_fn=check_requirements, validate_config=validate_config,
        emoji="🎙", pii_safe=True,
        platform_hint=(
            "You are replying through a local voice wake-word device. "
            "Use concise, speech-friendly plain text."
        ),
    )
    register_hook = getattr(ctx, "register_hook", None)
    if register_hook is not None:
        register_hook("on_gateway_start", _start_adapter)
        register_hook("on_gateway_stop", _stop_adapter)
__all__ = ["register"]
