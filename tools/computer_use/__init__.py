"""Computer use toolset — universal (any-model) desktop control.

Architecture
------------
Drives the user's desktop session through a swappable backend:

* macOS  — ``cua_backend.py`` (default). Talks MCP over stdio to
  ``cua-driver`` for SkyLight SPIs that post events without stealing
  cursor / focus / Space.
* Linux  — ``linux_backend.py``. Drives AT-SPI + grim + ydotool +
  wtype + wl-copy through a CLI pipeline (no PyGObject dependency).
* Any platform can plug in their own ``ComputerUseBackend``
  implementation behind ``HERMES_COMPUTER_USE_BACKEND``.

The selected backend never raises the user's window / focus / Space
("no-foreground contract"). The agent reads the accessibility tree
and posts synthesised input events through uinput (Linux) or
SkyLight private SPIs (macOS), so the user can co-work the same
machine.

The schema is a plain OpenAI function-calling shape that every
tool-capable model can drive. Vision models get the raw screenshot;
AX mode returns text + bounds for non-vision models.

Wiring
------
* ``tool.py``        — registers the ``computer_use`` tool via
                       ``tools.registry`` + handles backend selection.
* ``backend.py``     — abstract ``ComputerUseBackend`` interface.
* ``cua_backend.py`` — macOS implementation (default).
* ``linux_backend.py`` — Linux implementation (ydotool + grim).
* ``schema.py``      — shared schema + docstring for the generic
                       ``computer_use`` tool. Model-agnostic.
* ``vision_routing.py`` — decide whether to hand the captured PNG
                       to ``auxiliary.vision`` instead of the main
                       model.

The outer integration points (multimodal tool-result plumbing,
screenshot eviction in the Anthropic adapter, image-aware token
estimation, the COMPUTER_USE_GUIDANCE prompt block, approval hook,
and the skill) live alongside this package. See
``agent/anthropic_adapter.py`` and ``agent/prompt_builder.py`` for
the salvaged hunks from PR #4562.
"""

from __future__ import annotations

# Re-export the public surface so `from tools.computer_use import ...` works.
from tools.computer_use.tool import (  # noqa: F401
    handle_computer_use,
    set_approval_callback,
    check_computer_use_requirements,
    get_computer_use_schema,
)
