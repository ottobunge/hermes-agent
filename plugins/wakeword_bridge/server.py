from __future__ import annotations

from typing import Final

from aiohttp import web

from plugins.wakeword_bridge.handler import BridgeContext, handle_inject

MAX_BODY_BYTES: Final = 8 * 1024 * 1024
HOST: Final = "127.0.0.1"
PORT: Final = 8645


async def start_server(context: BridgeContext) -> web.AppRunner:
    """Start the loopback wakeword endpoint in the gateway event loop."""
    app = web.Application(client_max_size=MAX_BODY_BYTES)
    app["ctx"] = context
    app.router.add_post("/wakeword/inject", handle_inject)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    try:
        await site.start()
    except OSError:
        await runner.cleanup()
        raise
    return runner
