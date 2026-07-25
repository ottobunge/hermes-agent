from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
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

from .auth import verify_signature


@dataclass(frozen=True, slots=True)
class InjectionForm:
    audio: bytes
    target: str
    captured_at: int
    device_id: str
    correlation_id: str


class EventAdapter(Protocol):
    def build_source(self, **kwargs): ...
    async def handle_message(self, event: MessageEvent) -> None: ...


@dataclass(frozen=True, slots=True)
class HandlerContext:
    secret: bytes
    adapter: EventAdapter


def device_chat_id(device_id: str) -> str:
    """Map an untrusted device label to a stable wakeword chat address."""
    raw = device_id.strip().lower()
    normalized = re.sub(r"[^a-z0-9-]+", "-", raw).strip("-")
    if normalized == raw and len(raw) <= 64:
        return f"wakeword-{raw or 'unknown'}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"wakeword-{normalized[:48] or 'device'}-{digest}"


def parse_form(content_type: str, body: bytes) -> InjectionForm:
    """Parse the daemon's multipart body without changing signed bytes."""
    headers = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n"
    envelope = headers.encode("ascii") + body
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
    if not audio:
        raise web.HTTPBadRequest(reason="missing audio form field")
    try:
        captured_at = int(fields.get("captured_at", "0"))
    except ValueError as exc:
        raise web.HTTPBadRequest(reason="captured_at must be an integer") from exc
    return InjectionForm(
        audio=audio,
        target=fields.get("target", ""),
        captured_at=captured_at,
        device_id=fields.get("device_id", "unknown") or "unknown",
        correlation_id=fields.get("correlation_id", "") or uuid4().hex,
    )


def build_event(adapter: EventAdapter, form: InjectionForm) -> MessageEvent:
    """Cache captured audio and construct the internal voice event."""
    chat_id = device_chat_id(form.device_id)
    source = adapter.build_source(
        chat_id=chat_id,
        chat_name=form.device_id,
        chat_type="dm",
        user_id=f"wakeword:{form.device_id}",
        user_name=form.device_id,
        message_id=form.correlation_id,
    )
    try:
        timestamp = (datetime.fromtimestamp(form.captured_at, tz=timezone.utc)
                     if form.captured_at > 0 else datetime.now(tz=timezone.utc))
    except (OSError, OverflowError, ValueError) as exc:
        raise web.HTTPBadRequest(reason="captured_at is out of range") from exc
    return MessageEvent(
        text="", message_type=MessageType.VOICE, source=source,
        message_id=form.correlation_id,
        media_urls=[cache_audio_from_bytes(form.audio, ext=".ogg")],
        media_types=["audio/ogg"],
        internal=True,
        metadata={
            "origin": "wakeword",
            "device": form.device_id,
            "correlation_id": form.correlation_id,
            "captured_at": form.captured_at,
        },
        timestamp=timestamp)


async def handle_inject(request: web.Request) -> web.Response:
    """Authenticate and dispatch one daemon capture through the adapter."""
    signature = request.headers.get("X-Hermes-Wakeword-Key", "")
    raw_timestamp = request.headers.get("X-Hermes-Wakeword-Ts", "")
    try:
        timestamp = int(raw_timestamp)
    except ValueError as exc:
        raise web.HTTPUnauthorized(reason="invalid wakeword timestamp") from exc
    body = await request.read()
    context: HandlerContext = request.app["ctx"]
    if not signature or not verify_signature(
        context.secret, body, timestamp, signature, max_age_seconds=30
    ):
        raise web.HTTPUnauthorized(reason="hmac mismatch or stale timestamp")
    form = parse_form(request.headers.get("Content-Type", ""), body)
    event = build_event(context.adapter, form)
    await context.adapter.handle_message(event)
    return web.json_response({
        "chat_id": event.source.chat_id,
        "correlation_id": event.metadata["correlation_id"], "queued": True,
    })


def create_app(context: HandlerContext) -> web.Application:
    """Build the loopback HTTP application."""
    app = web.Application(client_max_size=8 * 1024 * 1024)
    app["ctx"] = context
    app.router.add_post("/wakeword/inject", handle_inject)
    return app
