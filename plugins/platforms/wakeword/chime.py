from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
import wave
from array import array
from pathlib import Path


def _play_tones(frequencies: tuple[int, ...]) -> None:
    sample_rate = 16000
    samples = array("h")
    for index, frequency in enumerate(frequencies):
        duration = 0.20 if index == len(frequencies) - 1 else 0.15
        count = int(sample_rate * duration)
        for position in range(count):
            edge = min(position / 80, (count - position - 1) / 80, 1.0)
            phase = 2 * math.pi * frequency * position / sample_rate
            samples.append(int(27852 * edge * math.sin(phase)))
        if index < len(frequencies) - 1:
            samples.extend([0] * int(sample_rate * 0.05))
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
        path = Path(temp_file.name)
    try:
        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(samples.tobytes())
        player = shutil.which("pw-play") or shutil.which("mpv")
        if player is None:
            return
        command = (
            [player, "--rate", str(sample_rate), "--channels", "1", str(path)]
            if Path(player).name == "pw-play"
            else [player, "--no-video", "--quiet", str(path)]
        )
        subprocess.run(command, capture_output=True, timeout=5, check=False)
    finally:
        path.unlink(missing_ok=True)


def play_reply_chime() -> None:
    """Play a distinctly higher-pitched reply-complete cue.

    The capture chime lives in the daemon at
    ``packages/hermes-wake-word/default.nix`` (660/880/1100 Hz) and is
    played by the daemon itself after a successful capture. This
    plugin's chime is the *reply* cue (880/1175/1568 Hz ascending),
    intentionally higher so the user can distinguish "I heard you"
    from "I answered you".
    """
    _play_tones((880, 1175, 1568))
