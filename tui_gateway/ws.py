"""WebSocket transport for the tui_gateway JSON-RPC server.

Reuses :func:`tui_gateway.server.dispatch` verbatim so every RPC method, every
slash command, every approval/clarify/sudo flow, and every agent event flows
through the same handlers whether the client is Ink over stdio or an iOS /
web client over WebSocket.

Wire protocol
-------------
Identical to stdio: newline-delimited JSON-RPC in both directions. The server
emits a ``gateway.ready`` event immediately after connection accept, then
echoes responses/events for inbound requests. No framing differences.

Mounting
--------
    from fastapi import WebSocket
    from tui_gateway.ws import handle_ws

    @app.websocket("/api/ws")
    async def ws(ws: WebSocket):
        await handle_ws(ws)
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import threading
from typing import Any

from tui_gateway import server

_log = logging.getLogger(__name__)

_WRITE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="tui-ws-write",
)

# Max seconds the write executor will wait for the event loop to flush a
# frame before marking the transport dead.
_WS_WRITE_TIMEOUT_S = 30.0
_WS_MAX_PENDING_WRITES = 128
_WS_LOG_PAYLOAD_PREVIEW = 240

# Keep starlette optional at import time; handle_ws uses the real class when
# it's available and falls back to a generic Exception sentinel otherwise.
try:
    from starlette.websockets import WebSocketDisconnect as _WebSocketDisconnect
except ImportError:  # pragma: no cover - starlette is a required install path
    _WebSocketDisconnect = Exception  # type: ignore[assignment]


class WSTransport:
    """Per-connection WS transport.

    ``write`` is safe to call from any thread. From the event loop thread
    we use fire-and-forget. From worker threads we schedule ``_safe_send``
    on the event loop and offload the blocking ``fut.result()`` wait to a
    tiny write executor so RPC workers are not consumed by slow socket I/O.

    Callers that need to know when the bytes are on the wire should use
    :meth:`write_async` from the loop thread.
    """

    def __init__(
        self,
        ws: Any,
        loop: asyncio.AbstractEventLoop,
        *,
        peer: str = "unknown",
    ) -> None:
        self._ws: Any = ws
        self._loop: asyncio.AbstractEventLoop = loop
        self._peer: str = peer
        self._closed: bool = False
        self._pending_writes: int = 0
        self._pending_lock: threading.Lock = threading.Lock()

    def write(self, obj: dict[str, Any]) -> bool:
        if self._closed:
            return False

        line = json.dumps(obj, ensure_ascii=False)

        try:
            on_loop = asyncio.get_running_loop() is self._loop
        except RuntimeError:
            on_loop = False

        if on_loop:
            self._loop.create_task(self._safe_send(line))
            return True

        try:
            from agent.async_utils import safe_schedule_threadsafe

            if not self._reserve_pending_write():
                return False
            fut = safe_schedule_threadsafe(self._safe_send(line), self._loop)
            if fut is None:
                self._release_pending_write()
                self._closed = True
                return False
            _WRITE_EXECUTOR.submit(self._await_write, fut)
            return True
        except Exception as exc:
            self._release_pending_write()
            _log.warning(
                "ws schedule failed peer=%s error_type=%s error=%s",
                self._peer,
                type(exc).__name__,
                exc,
            )
            return False

    async def write_async(self, obj: dict[str, Any]) -> bool:
        """Send from the owning event loop. Awaits until the frame is on the wire."""
        if self._closed:
            return False
        await self._safe_send(json.dumps(obj, ensure_ascii=False))
        return not self._closed

    def _await_write(self, fut: concurrent.futures.Future[Any]) -> None:
        try:
            fut.result(timeout=_WS_WRITE_TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            self._closed = True
            _log.warning(
                "ws write timed out after %.0fs peer=%s (event loop stalled)",
                _WS_WRITE_TIMEOUT_S,
                self._peer,
            )
        except Exception:
            pass
        finally:
            self._release_pending_write()

    def _reserve_pending_write(self) -> bool:
        with self._pending_lock:
            if self._closed:
                return False
            if self._pending_writes >= _WS_MAX_PENDING_WRITES:
                self._closed = True
                _log.warning(
                    "ws write backlog exceeded limit=%d peer=%s",
                    _WS_MAX_PENDING_WRITES,
                    self._peer,
                )
                return False
            self._pending_writes += 1
            return True

    def _release_pending_write(self) -> None:
        with self._pending_lock:
            if self._pending_writes > 0:
                self._pending_writes -= 1

    async def _safe_send(self, line: str) -> None:
        try:
            await self._ws.send_text(line)
        except Exception as exc:
            self._closed = True
            _log.warning(
                "ws send failed peer=%s error_type=%s error=%s",
                self._peer,
                type(exc).__name__,
                exc,
            )

    def close(self) -> None:
        self._closed = True


def _ws_peer_label(ws: Any) -> str:
    """Return ``host:port`` when available, else a stable placeholder."""
    client = getattr(ws, "client", None)
    if client is None:
        return "unknown"
    host = getattr(client, "host", None) or "unknown"
    port = getattr(client, "port", None)
    return f"{host}:{port}" if port is not None else host


async def handle_ws(ws: Any) -> None:
    """Run one WebSocket session. Wire-compatible with ``tui_gateway.entry``."""
    peer = _ws_peer_label(ws)
    transport: WSTransport | None = None
    messages = 0
    parse_errors = 0
    dispatch_crashes = 0
    send_failures = 0
    disconnect_reason = "not_connected"

    try:
        await ws.accept()
        disconnect_reason = "connected"
        _log.info("ws accepted peer=%s", peer)

        transport = WSTransport(ws, asyncio.get_running_loop(), peer=peer)

        ready_ok = await transport.write_async(
            {
                "jsonrpc": "2.0",
                "method": "event",
                "params": {
                    "type": "gateway.ready",
                    "payload": {"skin": server.resolve_skin()},
                },
            }
        )
        if not ready_ok:
            disconnect_reason = "ready_send_failed"
            send_failures += 1
            _log.error("ws ready frame send failed peer=%s", peer)
            return

        while True:
            try:
                raw = await ws.receive_text()
            except _WebSocketDisconnect as exc:
                disconnect_reason = (
                    "client_disconnect("
                    f"code={getattr(exc, 'code', None)},"
                    f"reason={getattr(exc, 'reason', None)})"
                )
                break
            except Exception:
                disconnect_reason = "receive_failed"
                _log.exception("ws receive failed peer=%s", peer)
                break

            line = raw.strip()
            if not line:
                continue
            messages += 1

            try:
                req = json.loads(line)
            except json.JSONDecodeError as exc:
                parse_errors += 1
                _log.warning(
                    "ws parse error peer=%s index=%d error=%s payload=%r",
                    peer,
                    messages,
                    exc,
                    line[:_WS_LOG_PAYLOAD_PREVIEW],
                )
                ok = await transport.write_async(
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32700, "message": "parse error"},
                        "id": None,
                    }
                )
                if not ok:
                    disconnect_reason = "send_failed_after_parse_error"
                    send_failures += 1
                    _log.warning("ws parse-error reply send failed peer=%s", peer)
                    break
                continue

            # dispatch() may schedule long handlers on the pool; it returns
            # None in that case and the worker writes the response itself via
            # the transport we pass in (a separate thread, so transport.write
            # is the safe path there). For inline handlers it returns the
            # response dict, which we write here from the loop.
            req_id = req.get("id") if isinstance(req, dict) else None
            req_method = req.get("method") if isinstance(req, dict) else None
            try:
                resp = await asyncio.to_thread(server.dispatch, req, transport)
            except Exception:
                dispatch_crashes += 1
                _log.exception(
                    "ws dispatch crash peer=%s id=%s method=%s",
                    peer,
                    req_id,
                    req_method,
                )
                ok = await transport.write_async(
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32603, "message": "internal error"},
                        "id": req_id if req_id is not None else None,
                    }
                )
                if not ok:
                    disconnect_reason = "send_failed_after_dispatch_crash"
                    send_failures += 1
                    _log.warning(
                        "ws dispatch-crash reply send failed peer=%s id=%s method=%s",
                        peer,
                        req_id,
                        req_method,
                    )
                    break
                continue
            if resp is not None and not await transport.write_async(resp):
                disconnect_reason = "send_failed_after_response"
                send_failures += 1
                _log.warning(
                    "ws response send failed peer=%s id=%s method=%s",
                    peer,
                    req_id,
                    req_method,
                )
                break
    finally:
        detached_sessions = 0
        reaped_scheduled = 0
        if transport is not None:
            transport.close()

            # Detach the transport from any sessions it owned so later emits
            # fall back to stdio instead of crashing into a closed socket.
            #
            # In the dashboard's in-process gateway that stdio fallback has no
            # real reader, so a detached session would otherwise sit forever
            # holding its _SlashWorker subprocess open (one leaked python proc
            # per browser refresh — #38591 fallout). Schedule a grace-delayed
            # reap; a quick reconnect / session.resume re-binds a live
            # transport and cancels it (see _ws_session_is_orphaned).
            for _sid, sess in list(server._sessions.items()):
                if sess.get("transport") is transport:
                    sess["transport"] = server._stdio_transport
                    detached_sessions += 1
                    try:
                        server._schedule_ws_orphan_reap(_sid)
                        reaped_scheduled += 1
                    except Exception:
                        _log.exception(
                            "ws orphan-reap schedule failed peer=%s sid=%s",
                            peer,
                            _sid,
                        )
        try:
            await ws.close()
        except Exception as exc:
            _log.debug("ws close failed peer=%s error=%s", peer, exc)
        _log.info(
            "ws closed peer=%s reason=%s messages=%d parse_errors=%d "
            "dispatch_crashes=%d send_failures=%d detached_sessions=%d",
            peer,
            disconnect_reason,
            messages,
            parse_errors,
            dispatch_crashes,
            send_failures,
            detached_sessions,
        )
