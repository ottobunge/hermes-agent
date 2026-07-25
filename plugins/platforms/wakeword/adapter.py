from __future__ import annotations

import asyncio  # noqa: ANYIO_OK - BasePlatformAdapter and aiohttp use asyncio.
import logging
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4

from aiohttp import web

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    ProcessingOutcome,
    SendResult,
)

from .auth import load_secret
from .chime import play_reply_chime
from .handler import HandlerContext, create_app
from .outbound import play_audio, send_text

logger = logging.getLogger(__name__)

SECRET_PATH = Path.home() / ".config" / "hermes" / "wakeword_hmac"
SOCKET_PATH = Path.home() / ".cache" / "hermes" / "voice-reply.sock"


class WakewordAdapter(BasePlatformAdapter):
    """Loopback wake-word ingress with local speaker replies."""

    interactive_resume = False

    @property
    def authorization_is_upstream(self) -> bool:
        return True

    def __init__(
        self,
        config: PlatformConfig,
        platform: Platform | None = None,
    ) -> None:
        super().__init__(config=config, platform=platform or Platform("wakeword"))
        extra = config.extra or {}
        self._host = str(extra.get("host", "127.0.0.1"))
        self._port = int(extra.get("port", 8645))
        self._secret_path = Path(extra.get("secret_path", SECRET_PATH)).expanduser()
        self._socket_path = Path(extra.get("socket_path", SOCKET_PATH)).expanduser()
        self._runner: web.AppRunner | None = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Start the signed loopback injection endpoint."""
        del is_reconnect
        if self._runner is not None:
            return True
        secret = load_secret(self._secret_path)
        if secret is None:
            logger.warning(
                "[%s] wakeword secret missing, invalid, or not mode 0600: %s",
                self.name,
                self._secret_path,
            )
            return False
        context = HandlerContext(secret, self)
        runner = web.AppRunner(create_app(context), access_log=None)
        try:
            await runner.setup()
            await web.TCPSite(runner, self._host, self._port).start()
        except (OSError, asyncio.CancelledError):
            await runner.cleanup()
            raise
        self._runner = runner
        self._mark_connected()
        logger.info("[%s] Listening on http://%s:%d", self.name, self._host, self._port)
        return True

    async def disconnect(self) -> None:
        """Stop accepting wake-word captures."""
        runner, self._runner = self._runner, None
        try:
            if runner is not None:
                await runner.cleanup()
        finally:
            self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """Route reply text to the local edge-tts daemon."""
        del reply_to, metadata
        try:
            await asyncio.to_thread(send_text, self._socket_path, chat_id, content)
        except OSError as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        return SendResult(success=True, message_id=uuid4().hex[:12])

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: str | None = None,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> SendResult:
        """Play already-synthesized audio on the default local sink."""
        del chat_id, caption, reply_to, metadata, kwargs
        try:
            await asyncio.to_thread(play_audio, audio_path)
        except (OSError, subprocess.SubprocessError) as exc:
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=uuid4().hex[:12])

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Return metadata for one local wakeword device."""
        return {"name": chat_id.removeprefix("wakeword-"), "type": "dm"}

    def _should_auto_tts_for_chat(
        self,
        chat_id: str,
        *,
        chat_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Never auto-TTS wake-word replies via the gateway.

        Voice replies are delivered to the device via ``send_voice``
        (which plays the synthesized audio through ``pw-play``/``mpv``).
        Auto-TTS would re-synthesize the same text and play it again
        through the voice-reply daemon's separate path. Returning
        ``False`` here short-circuits the auto-TTS gate at
        ``BasePlatformAdapter._maybe_auto_tts_for_voice_input``.
        """
        del chat_id, chat_type, metadata
        return False

    async def on_processing_complete(
        self,
        event: MessageEvent,
        outcome: ProcessingOutcome,
    ) -> None:
        """Play the high reply cue after successful delivery."""
        del event
        if outcome is ProcessingOutcome.SUCCESS:
            await asyncio.to_thread(play_reply_chime)


def check_requirements(secret_path: Path | None = None) -> bool:
    """Return whether a secure shared secret is available.

    The secret path defaults to ``~/.config/hermes/wakeword_hmac`` but
    can be overridden for testing or non-standard layouts. Operators
    who set ``extra.secret_path`` in ``platforms.wakeword.extra`` get
    the right path here when ``hermes setup`` is run.
    """
    return load_secret(secret_path or SECRET_PATH) is not None


def validate_config(config: PlatformConfig) -> bool:
    """Validate optional host and port settings."""
    extra = config.extra or {}
    try:
        port = int(extra.get("port", 8645))
    except (TypeError, ValueError):
        return False
    host_is_loopback = str(extra.get("host", "127.0.0.1")) == "127.0.0.1"
    return host_is_loopback and 0 < port < 65536
