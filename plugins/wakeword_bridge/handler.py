from __future__ import annotations

from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from typing import Protocol
from uuid import uuid4

from aiohttp import web

from gateway.platforms.base import (
    MessageEvent,
    MessageType,
    cache_audio_from_bytes,
)
from plugins.session_routing.address import AddressError, parse as parse_address
from plugins.wakeword_bridge.auth import verify_signature


class GatewayRunner(Protocol):
    def enqueue_internal_session_event(
        self,
        session_key: str,
        event: MessageEvent,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class BridgeContext:
    secret: bytes
    gateway_runner: GatewayRunner


@dataclass(frozen=True, slots=True)
class InjectionForm:
    audio: bytes
    target: str
    captured_at: int
    device_id: str
    correlation_id: str


def _parse_form(content_type: str, body: bytes) -> InjectionForm:
    envelope = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode(
            "ascii"
        )
        + body
    )
    message = BytesParser(policy=policy.default).parsebytes(envelope)
    if not message.is_multipart():
        raise web.HTTPBadRequest(reason="multipart/form-data required")

    fields: dict[str, str] = {}
    audio: bytes | None = None
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        payload = part.get_payload(decode=True) or b""
        if name == "audio":
            audio = payload
        elif isinstance(name, str):
            try:
                fields[name] = payload.decode(part.get_content_charset() or "utf-8")
            except UnicodeDecodeError as exc:
                raise web.HTTPBadRequest(reason=f"invalid {name} encoding") from exc

    target = fields.get("target", "").strip()
    if audio is None:
        raise web.HTTPBadRequest(reason="missing audio form field")
    if not target:
        raise web.HTTPBadRequest(reason="missing target form field")
    try:
        captured_at = int(fields.get("captured_at", "0"))
    except ValueError as exc:
        raise web.HTTPBadRequest(reason="captured_at must be an integer") from exc
    return InjectionForm(
        audio=audio,
        target=target,
        captured_at=captured_at,
        device_id=fields.get("device_id", "unknown") or "unknown",
        correlation_id=fields.get("correlation_id", "") or uuid4().hex,
    )


async def handle_inject(request: web.Request) -> web.Response:
    """Authenticate and enqueue one captured voice message."""
    signature = request.headers.get("X-Hermes-Wakeword-Key")
    timestamp_header = request.headers.get("X-Hermes-Wakeword-Ts")
    if not signature or not timestamp_header:
        raise web.HTTPUnauthorized(reason="missing wakeword authentication headers")
    try:
        timestamp = int(timestamp_header)
    except ValueError as exc:
        raise web.HTTPUnauthorized(reason="invalid wakeword timestamp") from exc

    body = await request.read()
    context: BridgeContext = request.app["ctx"]
    if not verify_signature(context.secret, body, timestamp, signature):
        raise web.HTTPUnauthorized(reason="hmac mismatch or stale timestamp")

    form = _parse_form(request.headers.get("Content-Type", ""), body)
    try:
        _gateway_id, session_key = parse_address(form.target)
    except AddressError as exc:
        raise web.HTTPBadRequest(reason=f"invalid target: {exc}") from exc

    audio_path = cache_audio_from_bytes(form.audio, ext=".ogg")
    event = MessageEvent(
        text="",
        message_type=MessageType.VOICE,
        media_urls=[audio_path],
        media_types=["audio/ogg"],
        internal=True,
        metadata={
            "origin": "wakeword_bridge",
            "device_id": form.device_id,
            "correlation_id": form.correlation_id,
            "captured_at": form.captured_at,
        },
    )
    if not context.gateway_runner.enqueue_internal_session_event(session_key, event):
        raise web.HTTPServiceUnavailable(
            reason=f"could not enqueue for session {session_key}"
        )
    return web.json_response(
        {
            "correlation_id": form.correlation_id,
            "session_key": session_key,
            "queued": True,
        }
    )
