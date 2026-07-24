from __future__ import annotations

import logging
import stat
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol

from aiohttp import web

from plugins.wakeword_bridge.handler import BridgeContext, GatewayRunner
from plugins.wakeword_bridge.server import start_server

logger = logging.getLogger(__name__)
SECRET_PATH = Path.home() / ".config" / "hermes" / "wakeword_hmac"


class PluginContext(Protocol):
    def register_hook(
        self,
        name: str,
        callback: Callable[..., Awaitable[None]],
    ) -> None: ...


class WakewordRuntime:
    """Own the mutable aiohttp runner for the gateway process lifetime."""

    def __init__(self, secret: bytes) -> None:
        self._secret = secret
        self._runner: web.AppRunner | None = None

    async def on_gateway_start(
        self,
        gateway: GatewayRunner | None = None,
        **_kwargs: Any,
    ) -> None:
        """Start the bridge after the gateway has entered its event loop.

        ``**_kwargs`` absorbs infrastructural kwargs injected by
        ``PluginManager.invoke_hook`` (e.g. ``telemetry_schema_version``);
        without it Python raises TypeError before the coroutine is built
        and the gateway's per-callback try/except silently swallows the failure.
        """
        if gateway is None:
            logger.warning("wakeword_bridge.start skipped: gateway unavailable")
            return
        if self._runner is not None:
            return
        self._runner = await start_server(BridgeContext(self._secret, gateway))
        logger.info("wakeword_bridge.start host=127.0.0.1 port=8645")

    async def on_gateway_stop(
        self,
        gateway: GatewayRunner | None = None,
        **_kwargs: Any,
    ) -> None:
        """Release the bridge listener during graceful gateway shutdown."""
        del gateway
        if self._runner is None:
            return
        await self._runner.cleanup()
        self._runner = None
        logger.info("wakeword_bridge.stop")


def _load_secret() -> bytes | None:
    try:
        mode = stat.S_IMODE(SECRET_PATH.stat().st_mode)
        raw = SECRET_PATH.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        logger.warning(
            "wakeword_bridge.disabled: create %s with a 32-byte hex key",
            SECRET_PATH,
        )
        return None
    except OSError as exc:
        logger.warning("wakeword_bridge.disabled: cannot read secret: %s", exc)
        return None
    if mode != 0o600:
        logger.warning(
            "wakeword_bridge.disabled: secret mode must be 0600 (got %04o)",
            mode,
        )
        return None
    try:
        secret = bytes.fromhex(raw)
    except ValueError:
        logger.warning("wakeword_bridge.disabled: secret must be hexadecimal")
        return None
    if len(secret) != 32:
        logger.warning("wakeword_bridge.disabled: secret must decode to 32 bytes")
        return None
    return secret


def register(ctx: PluginContext) -> None:
    """Register gateway lifecycle hooks when a valid secret is configured."""
    secret = _load_secret()
    if secret is None:
        return
    runtime = WakewordRuntime(secret)
    ctx.register_hook("on_gateway_start", runtime.on_gateway_start)
    ctx.register_hook("on_gateway_stop", runtime.on_gateway_stop)
