from __future__ import annotations

import asyncio  # noqa: ANYIO_OK - exercises the asyncio-based adapter contract.
import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from gateway.config import Platform, PlatformConfig
from gateway.platforms import base
from gateway.platforms.base import MessageType, ProcessingOutcome

from plugins.platforms.wakeword.adapter import WakewordAdapter
from plugins.platforms.wakeword.auth import compute_signature, verify_signature
from plugins.platforms.wakeword.handler import InjectionForm, build_event, device_chat_id
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


def test_hmac_round_trip_and_tamper_detection() -> None:
    secret = bytes.fromhex("11" * 32)
    body = b"test multipart body"
    timestamp = 1234567890
    signature = compute_signature(secret, body, timestamp)
    assert verify_signature(secret, body, timestamp, signature)
    assert not verify_signature(secret, body + b"x", timestamp, signature)


def test_hmac_freshness_is_enforced_when_requested() -> None:
    secret = bytes.fromhex("22" * 32)
    body = b"captured audio"
    timestamp = int(time.time()) - 60
    signature = compute_signature(secret, body, timestamp)
    assert not verify_signature(secret, body, timestamp, signature,
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
    assert event.metadata["correlation_id"] == "corr-1"


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
