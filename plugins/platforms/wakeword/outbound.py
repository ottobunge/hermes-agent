from __future__ import annotations

import json
import shutil
import socket
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol


class UnixSocket(Protocol):
    def __enter__(self) -> UnixSocket: ...
    def __exit__(self, *args: Any) -> None: ...
    def settimeout(self, seconds: float) -> None: ...
    def connect(self, address: str) -> None: ...
    def sendall(self, data: bytes) -> None: ...


class CompletedProcess(Protocol):
    returncode: int
    stderr: bytes


def _socket_factory() -> socket.socket:
    return socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)


def send_text(
    socket_path: Path,
    chat_id: str,
    text: str,
    *,
    socket_factory: Callable[[], UnixSocket] = _socket_factory,
) -> None:
    """Write one newline-delimited JSON request to the TTS daemon."""
    message = {"chat_id": chat_id, "text": text}
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8") + b"\n"
    with socket_factory() as client:
        client.settimeout(10.0)
        client.connect(str(socket_path))
        client.sendall(payload)


_PLAY_CANDIDATES: tuple[tuple[str, ...], ...] = (
    # pw-play is the native PipeWire player — present on NixOS 25.05+
    # without extra deps. We try it first; fall back to mpv (also
    # usually in the user's PATH) only if pw-play is missing.
    ("pw-play", "--quiet", "--volume=0.8"),
    ("mpv", "--no-video", "--no-terminal", "--quiet", "--volume=80"),
)


def play_audio(
    audio_path: str,
    *,
    runner: Callable[..., CompletedProcess] = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> None:
    """Play gateway-generated audio on the host's default sink.

    Tries ``pw-play`` first (PipeWire's native player; absent from the
    gateway's runtimeDeps and from ``/run/current-system/sw/bin`` so it
    is typically missing). Falls back to ``mpv`` when ``pw-play`` is
    not on PATH. Both invocations block until the audio finishes or
    the 120s timeout fires.
    """
    if which("pw-play") is not None:
        candidates = (_PLAY_CANDIDATES[0],)
    elif which("mpv") is not None:
        candidates = (_PLAY_CANDIDATES[1],)
    else:
        raise OSError(
            "play_audio requires either `pw-play` or `mpv` on PATH; "
            "neither was found."
        )
    last_error: OSError | None = None
    for prefix in candidates:
        cmd = (*prefix, audio_path)
        result = runner(cmd, capture_output=True, timeout=120, check=False)
        if result.returncode == 0:
            return
        last_error = OSError(result.stderr.decode("utf-8", errors="replace").strip())
    assert last_error is not None  # for type-checkers; loop guarantees one error
    raise last_error
