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


def test_ws_write_rejects_unbounded_pending_backlog(monkeypatch):
    from tui_gateway import ws as ws_mod
    from tui_gateway.ws import WSTransport

    class NeverDoneFuture:
        def result(self, timeout=None):
            return None

    class DummyExecutor:
        def submit(self, *_args, **_kwargs):
            return NeverDoneFuture()

    class DummySocket:
        async def send_text(self, _line):
            return None

    async def run_case():
        loop = asyncio.get_running_loop()
        transport = WSTransport(DummySocket(), loop, peer="test")

        monkeypatch.setattr(ws_mod, "_WS_MAX_PENDING_WRITES", 1)
        monkeypatch.setattr(ws_mod, "_WRITE_EXECUTOR", DummyExecutor())
        def fake_schedule(coro, _loop):
            coro.close()
            return NeverDoneFuture()

        monkeypatch.setattr("agent.async_utils.safe_schedule_threadsafe", fake_schedule)

        results = []

        def worker():
            results.append(transport.write({"first": True}))
            results.append(transport.write({"second": True}))

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=1.0)

        assert not thread.is_alive()
        assert results == [True, False]
        assert transport._closed is True

    asyncio.run(run_case())
