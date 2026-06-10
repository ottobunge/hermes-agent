"""Tests for WebSocket write timeout configuration."""

import asyncio
import threading


def test_ws_write_timeout_allows_slow_windows_scheduling():
    from tui_gateway import ws

    assert ws._WS_WRITE_TIMEOUT_S == 30.0


def test_ws_write_from_worker_thread_does_not_wait_for_send():
    from tui_gateway.ws import WSTransport

    class SlowSocket:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def send_text(self, _line):
            self.started.set()
            await self.release.wait()

    async def run_case():
        loop = asyncio.get_running_loop()
        socket = SlowSocket()
        transport = WSTransport(socket, loop, peer="test")
        result = {}

        def worker():
            result["ok"] = transport.write({"ok": True})

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=0.5)

        assert not thread.is_alive()
        assert result == {"ok": True}
        await asyncio.wait_for(socket.started.wait(), timeout=1.0)
        socket.release.set()

    asyncio.run(run_case())
