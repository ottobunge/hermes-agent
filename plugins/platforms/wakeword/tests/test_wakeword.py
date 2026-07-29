from __future__ import annotations

import asyncio  # noqa: ANYIO_OK - exercises the asyncio-based adapter contract.
import json
import time
from email import policy
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from aiohttp.test_utils import TestClient, TestServer

from gateway.config import Platform, PlatformConfig
from gateway.platforms import base
from gateway.platforms.base import MessageType, ProcessingOutcome

from plugins.platforms.wakeword.adapter import WakewordAdapter
from plugins.platforms.wakeword.auth import compute_signature, verify_signature
from plugins.platforms.wakeword.handler import (
    HandlerContext,
    InjectionForm,
    build_event,
    create_app,
    device_chat_id,
)
from plugins.platforms.wakeword.outbound import play_audio, send_text


class FakeSocket:
    def __init__(self) -> None:
        self.address = ""
        self.payload = b""

    def __enter__(self) -> FakeSocket:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def connect(self, address: str) -> None:
        self.address = address

    def settimeout(self, _seconds: float) -> None:
        return None
    def sendall(self, payload: bytes) -> None:
        self.payload = payload


def _multipart_body(
    captured_at: int,
    audio: bytes | None = b"OggS integration payload",
    device_id: str = "MiniPC Kitchen",
) -> tuple[str, bytes]:
    boundary = "wakeword-test-boundary"
    chunks = [
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"device_id\"\r\n\r\n{device_id}\r\n".encode(),
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"captured_at\"\r\n\r\n{captured_at}\r\n".encode(),
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"target\"\r\n\r\ngateway\r\n".encode(),
    ]
    if audio is not None:
        chunks.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"capture.ogg\"\r\nContent-Type: audio/ogg\r\n\r\n".encode()
            + audio
            + b"\r\n"
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    body = b"".join(chunks)
    content_type = f"multipart/form-data; boundary={boundary}"
    envelope = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
    assert BytesParser(policy=policy.default).parsebytes(envelope).is_multipart()
    return content_type, body


def test_hmac_round_trip_and_tamper_detection() -> None:
    secret = bytes.fromhex("11" * 32)
    body = b"test multipart body"
    timestamp = 1234567890
    nonce = "01" * 16
    signature = compute_signature(secret, body, timestamp, nonce)
    assert verify_signature(secret, body, timestamp, nonce, signature)
    assert not verify_signature(secret, body + b"x", timestamp, nonce, signature)


def test_hmac_freshness_is_enforced_when_requested() -> None:
    secret = bytes.fromhex("22" * 32)
    body = b"captured audio"
    timestamp = int(time.time()) - 60
    nonce = "02" * 16
    signature = compute_signature(secret, body, timestamp, nonce)
    assert not verify_signature(secret, body, timestamp, nonce, signature,
                                max_age_seconds=30)


def test_device_address_is_stable_and_safe() -> None:
    assert device_chat_id("MiniPC-Kitchen") == "wakeword-minipc-kitchen"
    assert device_chat_id("a b") != device_chat_id("a-b")

def test_build_event_caches_audio_and_sets_voice_source(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(base, "AUDIO_CACHE_DIR", tmp_path)
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))
    form = InjectionForm(audio=b"OggS payload", target="gw/ignored",
                         captured_at=1234567890, device_id="minipc-kitchen",
                         correlation_id="corr-1")
    event = build_event(adapter, form)
    assert event.message_type is MessageType.VOICE
    assert Path(event.media_urls[0]).read_bytes() == form.audio
    assert event.source.chat_id == "wakeword-minipc-kitchen"
    assert event.source.user_id == "wakeword:minipc-kitchen"
    assert event.internal is True
    assert event.metadata == {"origin": "wakeword", "device": "minipc-kitchen"}


def test_handle_inject_returns_202_with_valid_signature(
    tmp_path: Path, monkeypatch,
) -> None:
    secret = bytes.fromhex("33" * 32)
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))
    handle_message = AsyncMock()
    monkeypatch.setattr(adapter, "handle_message", handle_message)
    monkeypatch.setattr(base, "AUDIO_CACHE_DIR", tmp_path)

    async def exercise() -> None:
        timestamp = int(time.time())
        content_type, body = _multipart_body(timestamp)
        nonce = "03" * 16
        headers = {
            "Content-Type": content_type,
            "X-Hermes-Wakeword-Key": compute_signature(secret, body, timestamp, nonce),
            "X-Hermes-Wakeword-Ts": str(timestamp),
            "X-Hermes-Wakeword-Nonce": nonce,
        }
        async with TestClient(TestServer(create_app(HandlerContext(secret, adapter)))) as client:
            response = await client.post("/wakeword/inject", data=body, headers=headers)
            assert response.status == 202
            event = handle_message.await_args.args[0]
            assert Path(event.media_urls[0]).read_bytes() == b"OggS integration payload"

            replay = await client.post("/wakeword/inject", data=body, headers=headers)
            assert replay.status == 401

            stale_timestamp = timestamp - 31
            stale_headers = dict(headers)
            stale_headers.update({
                "X-Hermes-Wakeword-Key": compute_signature(
                    secret, body, stale_timestamp, nonce),
                "X-Hermes-Wakeword-Ts": str(stale_timestamp),
                "X-Hermes-Wakeword-Nonce": nonce,
            })
            stale = await client.post("/wakeword/inject", data=body, headers=stale_headers)
            assert stale.status == 401

            missing_key_headers = dict(headers)
            missing_key_headers.pop("X-Hermes-Wakeword-Key")
            missing_key_headers["X-Hermes-Wakeword-Nonce"] = "05" * 16
            missing_key = await client.post(
                "/wakeword/inject", data=body, headers=missing_key_headers)
            assert missing_key.status == 401

            missing_type, missing_body = _multipart_body(timestamp, audio=None)
            missing_nonce = "06" * 16
            missing_headers = {
                "Content-Type": missing_type,
                "X-Hermes-Wakeword-Key": compute_signature(
                    secret, missing_body, timestamp, missing_nonce),
                "X-Hermes-Wakeword-Ts": str(timestamp),
                "X-Hermes-Wakeword-Nonce": missing_nonce,
            }
            missing_audio = await client.post(
                "/wakeword/inject", data=missing_body, headers=missing_headers)
            assert missing_audio.status == 400

    asyncio.run(exercise())


def test_handle_inject_returns_401_on_tampered_body() -> None:
    secret = bytes.fromhex("44" * 32)
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))

    async def exercise() -> None:
        timestamp = int(time.time())
        content_type, body = _multipart_body(timestamp)
        nonce = "07" * 16
        headers = {
            "Content-Type": content_type,
            "X-Hermes-Wakeword-Key": compute_signature(secret, body, timestamp, nonce),
            "X-Hermes-Wakeword-Ts": str(timestamp),
            "X-Hermes-Wakeword-Nonce": nonce,
        }
        tampered = body[:-1] + bytes([body[-1] ^ 1])
        async with TestClient(TestServer(create_app(HandlerContext(secret, adapter)))) as client:
            response = await client.post("/wakeword/inject", data=tampered, headers=headers)
            assert response.status == 401

    asyncio.run(exercise())


def test_inbound_event_calls_handle_message_with_voice_event(
    tmp_path: Path, monkeypatch,
) -> None:
    secret = bytes.fromhex("55" * 32)
    device_id = "Kitchen / Main"
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))
    handle_message = AsyncMock()
    monkeypatch.setattr(adapter, "handle_message", handle_message)
    monkeypatch.setattr(base, "AUDIO_CACHE_DIR", tmp_path)

    async def exercise() -> None:
        timestamp = int(time.time())
        content_type, body = _multipart_body(timestamp, device_id=device_id)
        nonce = "08" * 16
        headers = {
            "Content-Type": content_type,
            "X-Hermes-Wakeword-Key": compute_signature(secret, body, timestamp, nonce),
            "X-Hermes-Wakeword-Ts": str(timestamp),
            "X-Hermes-Wakeword-Nonce": nonce,
        }
        async with TestClient(TestServer(create_app(HandlerContext(secret, adapter)))) as client:
            response = await client.post("/wakeword/inject", data=body, headers=headers)
            assert response.status == 202

    asyncio.run(exercise())
    handle_message.assert_awaited_once()
    event = handle_message.await_args.args[0]
    assert event.message_type is MessageType.VOICE
    assert len(event.media_urls) == 1
    assert event.internal is True
    assert event.metadata == {"origin": "wakeword", "device": device_id}
    assert event.source.chat_id == device_chat_id(device_id)


def test_should_auto_tts_for_chat_always_false() -> None:
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))
    assert adapter._should_auto_tts_for_chat("any-chat-id") is False
    assert adapter._should_auto_tts_for_chat(
        "any-chat-id", chat_type="group", metadata={"voice": True}) is False


def test_send_text_writes_one_json_line() -> None:
    fake_socket = FakeSocket()
    send_text(Path("/tmp/voice-reply.sock"), "wakeword-kitchen",
              "Dinner is ready", socket_factory=lambda: fake_socket)
    assert fake_socket.address == "/tmp/voice-reply.sock"
    assert json.loads(fake_socket.payload) == {
        "chat_id": "wakeword-kitchen", "text": "Dinner is ready"}
    assert fake_socket.payload.endswith(b"\n")


def test_play_audio_routes_to_pw_play_when_present() -> None:
    calls: list[tuple[str, ...]] = []
    def run(command: tuple[str, ...], **_kwargs: Any) -> SimpleNamespace:
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr=b"")
    play_audio("/tmp/reply.ogg", runner=run, which=lambda name: "/usr/bin/pw-play" if name == "pw-play" else None)
    assert calls == [(
        "pw-play", "--quiet", "--volume=0.8", "/tmp/reply.ogg"
    )]


def test_play_audio_falls_back_to_mpv_when_pw_play_missing() -> None:
    calls: list[tuple[str, ...]] = []
    def run(command: tuple[str, ...], **_kwargs: Any) -> SimpleNamespace:
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr=b"")
    play_audio("/tmp/reply.ogg", runner=run, which=lambda name: "/usr/bin/mpv" if name == "mpv" else None)
    assert calls == [(
        "mpv", "--no-video", "--no-terminal", "--quiet", "--volume=80",
        "/tmp/reply.ogg"
    )]


def test_play_audio_raises_when_no_player_present() -> None:
    def run(command: tuple[str, ...], **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stderr=b"")
    try:
        play_audio("/tmp/reply.ogg", runner=run, which=lambda _name: None)
    except OSError as exc:
        assert "neither was found" in str(exc)
    else:
        raise AssertionError("expected OSError when no player is on PATH")


def test_reply_chime_runs_only_after_success(monkeypatch) -> None:
    played: list[bool] = []
    monkeypatch.setattr("plugins.platforms.wakeword.adapter.play_reply_chime",
                        lambda: played.append(True))
    adapter = WakewordAdapter(PlatformConfig(enabled=True), Platform("wakeword"))
    asyncio.run(adapter.on_processing_complete(None, ProcessingOutcome.FAILURE))
    asyncio.run(adapter.on_processing_complete(None, ProcessingOutcome.SUCCESS))
    assert played == [True]
