"""Linux backend for the `computer_use` toolset.

A drop-in implementation of :class:`ComputerUseBackend` for Linux / Unix systems
that drives the user's existing desktop via small, already-installed CLIs.

What it talks to
----------------
* **AT-SPI / a11y bus** — for the accessibility tree (apps, windows,
  interactable elements with bounds). Talks plain D-Bus via ``dbus-send``
  so we don't pull in ``pyatspi`` / ``PyGObject`` (those aren't in the
  hermes-agent runtime's nix store).
* **grim / slurp** — for screenshots, optional region picking.
* **ydotool** — for mouse + keyboard synthesised through ``uinput`` (goes
  through ``ydotoold`` which holds ``/dev/uinput``). Works on Wayland and
  X11 the same way.
* **wtype** — IME-aware Wayland text input (used when available;
  ydotool is the fallback).
* **wl-copy / wl-paste** — Wayland clipboard (the X11 equivalents
  ``xclip`` / ``xsel`` are tried if those are missing).

What this backend is **not**
----------------------------
* Not a focus-stealer. We never ``raise_window`` the target app — the
  goal is the same as cua-driver's "no-foreground contract": the agent
  reads input through the AT-SPI tree and posts events through uinput,
  the user's real focus stays where they left it.
* Not an X11 emulator. We do not pretend to be a Wayland compositor; we
  drive whichever compositor the user is logged into.
* Not a screencapture-portal bypass. On KDE Plasma, ``grim`` returns
  ``compositor doesn't support the screen capture protocol`` unless
  ``xdg-desktop-portal-kde`` is installed and configured; this backend
  just surfaces that error so the user sees it.

Limitations vs cua-driver
-------------------------
* SOM (set-of-mark) capture is not painted onto the PNG — we don't ship
  Pillow in the hermes-agent runtime to draw number overlays. The model
  still gets both the elements list (with bounds) and the screenshot;
  it just has to correlate them by eye the way it does for the AX
  ``mode="ax"`` path.
* No pixel-exact ``focus_app`` semantics — Linux doesn't expose a
  pid-scoped event posting API like macOS SkyLight. ``focus_app(name)``
  resolves to the first AT-SPI app whose name matches ``name``
  (case-insensitive substring); subsequent ``click`` / ``type_text`` are
  posted through ``ydotool`` to the focus the user already has, which
  generally lands in the right window because the user already had it
  focused.
* Hotkey semantics map to ``ydotool key`` keysym names (``ctrl``, not
  ``ctrl_l``). Modifier-only or single-modifier keys should work
  consistently.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tools.computer_use.backend import (
    ActionResult,
    CaptureResult,
    ComputerUseBackend,
    UIElement,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config & small utilities
# ---------------------------------------------------------------------------

# The set of Wayland / X11 input + screenshot tools we know how to use.
# Anything missing gets a clear "install X" hint at first failure rather
# than a stack trace.
_YDOTOOL = shutil.which("ydotool")
_WTYPE = shutil.which("wtype")
_GRIM = shutil.which("grim")
_SLURP = shutil.which("slurp")
_WL_COPY = shutil.which("wl-copy")
_WL_PASTE = shutil.which("wl-paste")
_DBUS_SEND = shutil.which("dbus-send")

# Captures are written here by grim; we then read them back to base64
# them into the multimodal response.
_SCREENSHOT_DIR = Path(
    os.environ.get(
        "HERMES_COMPUTER_USE_SCREENSHOT_DIR",
        str(Path(tempfile.gettempdir()) / "hermes-computer-use"),
    )
)

# Where the AT-SPI bus lives on this distro. The default works for the
# canonical NixOS + KDE Plasma + at-spi2-core layout; override via env
# if a host uses a different layout (e.g. ``$XDG_RUNTIME_DIR/at-spi/bus_0``
# is the freedesktop.org spec).
def _default_atspi_bus() -> str:
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    candidate = Path(runtime_dir) / "at-spi" / "bus_0"
    return os.environ.get("HERMES_COMPUTER_USE_ATSPI_BUS", str(candidate))


_ATSPI_BUS = _default_atspi_bus()


def _tool_hint(name: str) -> str:
    """Build an installation hint for missing CLI tools."""
    return (
        f"`{name}` not found on PATH. Install it or set "
        f"HERMES_COMPUTER_USE_{name.upper().replace('-', '_')}=/abs/path."
    )


def _run(cmd: List[str], *, timeout: float = 10.0, **kw) -> subprocess.CompletedProcess:
    """Run a subprocess, swallowing the typical ``FileNotFoundError`` race."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)
    except FileNotFoundError as e:
        raise RuntimeError(f"missing executable: {cmd[0]!r} ({e})") from e


def _image_dimensions_from_png(path: Path) -> Tuple[int, int]:
    """Tiny PNG/JPEG dimension sniffer (no Pillow dep)."""
    try:
        raw = path.read_bytes()
    except Exception:
        return 0, 0
    if raw.startswith(b"\x89PNG\r\n\x1a\n") and len(raw) >= 24:
        import struct
        w, h = struct.unpack(">II", raw[16:24])
        return int(w), int(h)
    if raw.startswith(b"\xff\xd8"):
        i = 2
        n = len(raw)
        while i + 9 < n:
            if raw[i] != 0xFF:
                i += 1
                continue
            marker = raw[i + 1]
            i += 2
            if marker in {0xD8, 0xD9} or 0xD0 <= marker <= 0xD7:
                continue
            if i + 2 > n:
                break
            seg_len = int.from_bytes(raw[i:i + 2], "big")
            if seg_len < 2 or i + seg_len > n:
                break
            if marker in {
                0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
                0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
            } and seg_len >= 7:
                h = int.from_bytes(raw[i + 3:i + 5], "big")
                w = int.from_bytes(raw[i + 5:i + 7], "big")
                if w and h:
                    return int(w), int(h)
            i += seg_len
    return 0, 0


# ---------------------------------------------------------------------------
# AT-SPI D-Bus helpers — pure subprocess, no PyGObject dependency
# ---------------------------------------------------------------------------

# Roles we consider "interactable". Anything else (pure structural
# containers, panes, layers) is filtered out so the model doesn't drown
# in noise. AT-SPI role names follow the freedesktop.org spec.
_INTERACTABLE_ROLES = frozenset({
    "push button", "toggle button", "menu item", "check box", "radio button",
    "combo box", "entry", "text", "password text", "spin button",
    "slider", "progress bar", "scroll bar", "incrementor", "decrementor",
    "list item", "list box", "tree item", "tree table", "table cell",
    "tab", "page tab", "page tab list", "link", "image", "icon",
    "document text", "document frame", "document web", "heading",
    "panel", "scroll pane", "split pane", "status bar", "tool bar",
    "menu bar", "menu", "popup menu", "dialog", "window", "frame",
    "filler", "application", "form", "grouping", "separator",
    "tool tip", "notification", "animation", "canvas", "chart",
    "math", "caption", "static", "description", "info bar",
    "level bar", "title bar", "accelerator label", "arrow", "calendar",
    "check menu item", "color chooser", "data table", "date editor",
    "desktop icon", "desktop frame", "dial", "directory pane",
    "drawing area", "file chooser", "font chooser", "glass pane",
    "html container", "input method window", "internal frame",
    "label", "layered pane", "list", "menu button", "option pane",
    "paragraph", "progress monitor", "radio menu item", "rating",
    "root pane", "row header", "section", "spin button",
    "status icon", "table", "table column header", "table row header",
    "tearoff menu item", "terminal", "toggle menu item", "tree",
    "unknown", "viewport", "header", "footer", "paragraph",
    "block container", "suggestion", "landmark", "marquee",
})


# AT-SPI2 architecture quirk: the accessibility roots only become
# enumerable when an AT client (screen-reader-style consumer) has
# registered an event listener on the registry daemon. Without an
# active registration the cache may be empty even when GUI apps are
# running. Pyatspi registers a listener on construction; we replicate
# that by calling ``Registry.RegisterEvent`` with a harmless event
# on first use, then running a tiny ``gdbus monitor`` in the background
# to listen for ``EventListenerRegistered`` signals.
_GDBUS: Optional[str] = shutil.which("gdbus")


def _ensure_atspi_client_registration() -> bool:
    """Register this process as an AT client so apps expose their tree.

    AT-SPI2 only lazily populates ``/org/a11y/atspi/accessible/<...>``
    when an AT client has signaled interest. Pyatspi / Orca do this by
    calling ``Registry.RegisterEvent`` for every event type they care
    about. We register one cheap sentinel event (``Document:LoadComplete``)
    — that's enough to flip the apps into "publishing" mode.

    Requires ``gdbus`` on PATH (in NixOS: ``pkgs.glib``).
    """
    if _GDBUS is None:
        logger.debug("gdbus not on PATH; AT-SPI enumeration may be empty")
        return False
    if not Path(_ATSPI_BUS).exists():
        return False
    try:
        proc = _run(
            [
                _GDBUS,
                "call",
                "--address", f"unix:path={_ATSPI_BUS}",
                "--dest", "org.a11y.atspi.Registry",
                "--object-path", "/org/a11y/atspi/registry",
                "--method", "org.a11y.atspi.Registry.RegisterEvent",
                "Document:LoadComplete",
                "[]",
                f":1.{os.getpid() % 1000000}",  # fake well-known bus name
            ],
            timeout=5,
        )
        return proc.returncode == 0
    except Exception as e:
        logger.debug("AT-SPI client registration failed: %s", e)
        return False


def _atspi_call(
    method_signature: str,
    path: str,
    destination: str,
    *args: str,
    timeout: float = 5.0,
) -> str:
    """Make a D-Bus method call via ``dbus-send``.

    Returns the raw stdout (which is the dbus-send print-reply output).
    Raises RuntimeError on missing daemon or call failure.

    Note: plain ``dbus-send`` cannot SASL-handshake into the AT-SPI
    bus on most distros (it gets ``AccessDenied`` from the auth
    helper). For the method calls that need to actually run on the
    a11y bus, prefer ``gdbus`` directly — see ``_gdbus_call``.
    """
    if _DBUS_SEND is None:
        raise RuntimeError(_tool_hint("dbus-send"))
    if not Path(_ATSPI_BUS).exists():
        raise RuntimeError(
            f"AT-SPI bus not found at {_ATSPI_BUS}. Is at-spi2-registryd running? "
            f"On KDE Plasma 6 the daemon auto-starts when the user logs in."
        )
    cmd = [
        _DBUS_SEND,
        "--address", f"unix:path={_ATSPI_BUS}",
        "--print-reply",
        "--dest", destination,
        path,
        method_signature,
        *args,
    ]
    proc = _run(cmd, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"dbus-send {method_signature!r} to {destination} failed "
            f"(rc={proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


def _gdbus_call(method: str, object_path: str, *args: str, interface: str = "") -> str:
    """Call a D-Bus method on the AT-SPI bus using ``gdbus``.

    Returns the raw stdout. ``gdbus`` does the SASL AUTH EXTERNAL
    handshake that plain ``dbus-send`` cannot, which is what the
    AT-SPI2 daemon requires.

    The ``interface`` argument is optional — most AT-SPI methods have
    unique enough names that ``gdbus`` resolves them. Pass it
    explicitly when ambiguous.
    """
    if _GDBUS is None:
        raise RuntimeError(
            "gdbus is required for AT-SPI method calls on most distros. "
            "Install it: NixOS `pkgs.glib`; Debian/Ubuntu `apt install libglib2.0-bin`."
        )
    cmd: List[str] = [
        _GDBUS, "call",
        "--address", f"unix:path={_ATSPI_BUS}",
        "--dest", "org.a11y.atspi.Registry",
        "--object-path", object_path,
        "--method", method,
    ]
    if interface:
        # gdbus takes --interface when present
        cmd += ["--", method.split(".", 1)[-1]]
    cmd += list(args)
    proc = _run(cmd, timeout=10)
    if proc.returncode != 0:
        raise RuntimeError(
            f"gdbus {method} failed rc={proc.returncode}: {proc.stderr.strip()}"
        )
    return proc.stdout


_AT_SPI_OBJECT_RE = re.compile(
    r'object path "(/org/a11y/atspi/accessible/[\w/]+)"', re.IGNORECASE
)
_STRING_PROP_RE = re.compile(r'^\s*string\s+"([^"]*)"', re.MULTILINE)
_STRING_LIST_RE = re.compile(r'^\s*string\s+"([^"]*)"', re.MULTILINE)
_GDBUS_BOXED_RE = re.compile(r"\(\s*(\S+?)\s+\)\s*$")


def _parse_object_paths(blob: str) -> List[str]:
    """Pull D-Bus ``object path`` lines out of a dbus-send reply."""
    return _AT_SPI_OBJECT_RE.findall(blob)


def _strip_gvariant_parens(value: str) -> str:
    """Strip ``gvariant`` boxed-string parens gdbus adds to atomics."""
    m = _GDBUS_BOXED_RE.match(value.strip())
    if m and m.group(1).startswith('"') and m.group(1).endswith('"'):
        return m.group(1)[1:-1]
    return value


def _gdbus_get_string(object_path: str, prop: str, *, interface: str = "org.a11y.atspi.Accessible") -> str:
    """Fetch a single string D-Bus property via gdbus."""
    out = _gdbus_call(
        f"org.freedesktop.DBus.Properties.Get",
        object_path,
        interface,
        prop,
    )
    # gdbus output looks like:
    #   (<'the string',)
    # or for arrays:
    #   (<@as 'foo'>,)
    # Extract the first quoted string we see.
    for line in out.splitlines():
        line = line.strip()
        if "<" in line and line.startswith("(") and line.endswith(","):
            inner = line[2:-2]  # strip "(<" and ">,")
            inner = _strip_gvariant_parens(inner)
            inner = inner.strip("'\"")
            return inner
    return ""


def _gdbus_get_children(object_path: str) -> List[str]:
    """Return AT-SPI object paths of immediate children via gdbus."""
    try:
        out = _gdbus_call(
            "org.a11y.atspi.Accessible.GetChildren",
            object_path,
        )
    except Exception as e:
        logger.debug("GetChildren on %s failed: %s", object_path, e)
        return []
    # gdbus emits an array of (objectpath,) tuples, e.g.:
    #   ([(objectpath '/org/a11y/...'), (objectpath '/org/a11y/...')],)
    # Extract each objectpath value once.
    paths: List[str] = []
    seen: set = set()
    for m in re.finditer(r"objectpath '([^']+)'", out):
        if m.group(1) not in seen:
            seen.add(m.group(1))
            paths.append(m.group(1))
    return paths


def _gdbus_get_extents(object_path: str) -> Tuple[int, int, int, int]:
    """Return (x, y, w, h) for an accessible via Component.GetExtents.

    Coord type 0 = screen coordinates (the kind the model needs to
    pass to ``ydotool mousetap``); 1 = window-relative.

    ``gdbus`` returns the four ints wrapped twice — once for the
    struct (a, b, c, d) and once for the out-parameter — so the
    actual wire format is ``((0, 0, 1024, 768),)``.
    """
    try:
        out = _gdbus_call(
            "org.a11y.atspi.Component.GetExtents",
            object_path,
            "0",  # Coord type 0 == SCREEN (gdbus args don't want a type sig)
        )
    except Exception as e:
        logger.debug("GetExtents on %s failed: %s", object_path, e)
        return (0, 0, 0, 0)
    # Match the doubled tuple.
    m = re.search(r"\(\((-?\d+),\s*(-?\d+),\s*(-?\d+),\s*(-?\d+)\),\)", out)
    if not m:
        m = re.search(r"\((-?\d+),\s*(-?\d+),\s*(-?\d+),\s*(-?\d+)\)", out)
    if m:
        return tuple(int(m.group(i)) for i in (1, 2, 3, 4))  # type: ignore[return-value]
    return (0, 0, 0, 0)


def _gdbus_get_process_id(object_path: str) -> int:
    """Return the ProcessId property of an accessible.

    Not every accessible exposes ProcessId — the desktop root only
    carries one if it's owned by a specific app. Returns 0 when
    unavailable.
    """
    try:
        out = _gdbus_call(
            "org.freedesktop.DBus.Properties.Get",
            object_path,
            "org.a11y.atspi.Accessible",
            "ProcessId",
        )
    except Exception:
        # "Property unavailable" is expected for AT-SPI trees; not
        # worth logging at warning level.
        return 0
    m = re.search(r"uint32\s+(\d+)", out)
    return int(m.group(1)) if m else 0


def _gdbus_get_name(object_path: str) -> str:
    """Return the Name property of an accessible.

    On gdbus wire-format AT-SPI comes back as a single-quoted string
    inside a tuple: ``(<'the name'>,)``.
    """
    try:
        out = _gdbus_call(
            "org.freedesktop.DBus.Properties.Get",
            object_path,
            "org.a11y.atspi.Accessible",
            "Name",
        )
    except Exception as e:
        logger.debug("Name on %s failed: %s", object_path, e)
        return ""
    m = re.search(r"'([^']*)'", out)
    return m.group(1) if m else ""


def _gdbus_get_role(object_path: str) -> str:
    """Return the role name of an accessible, e.g. ``push button``.

    The AT-SPI ``Role`` property is a struct of ``(name, localised_name,
    enum_value)``. Pyatspi exposes ``role.name``; on the D-Bus side
    AT-SPI2 also publishes a convenience method ``GetRoleName`` that
    returns just the canonical name as a string, which is what we
    need for the role-match table.
    """
    try:
        out = _gdbus_call(
            "org.a11y.atspi.Accessible.GetRoleName",
            object_path,
        )
    except Exception as e:
        logger.debug("GetRoleName on %s failed: %s", object_path, e)
        return ""
    # gdbus emits a single-quoted string inside a tuple, e.g. ('push button',)
    m = re.search(r"'([^']*)'", out)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# AT-SPI walk
# ---------------------------------------------------------------------------


def _atspi_accessible_children(bus_path: str) -> List[str]:
    """Return the list of bus paths of immediate children of an Accessible."""
    return _gdbus_get_children(bus_path)


def _atspi_app_list() -> List[Dict[str, Any]]:
    """Enumerate every AT-SPI root application on the bus.

    Each entry: {"path": "...", "name": "Konsole", "pid": 1234}.

    Method: walk ``/org/a11y/atspi/accessible/root`` (the registry's
    canonical root) and enumerate each direct child. Apps that have
    registered an accessibility root show up here after we've called
    :func:`_ensure_atspi_client_registration`.
    """
    try:
        # dbus-send works for the simple "Hello"-cleared introspection
        # of the registry root. The accessible root under it lives at
        # ``/org/a11y/atspi/accessible/root`` (older paths also valid).
        children_paths = _gdbus_get_children("/org/a11y/atspi/accessible/root")
        # If the canonical path is empty, fall back to introspecting
        # the lower-level cache (some distros expose apps there).
        if not children_paths:
            children_paths = _gdbus_get_children(
                "/org/a11y/atspi/accessible"
            )
        # On Qt/KDE Plasma, apps may also live at a per-app-prefix path;
        # but for the canonical "list apps" pass, the accessible/root
        # tree is enough.
    except Exception as e:
        logger.debug("AT-SPI app enumeration failed: %s", e)
        return []
    apps: List[Dict[str, Any]] = []
    for p in children_paths:
        try:
            name = _gdbus_get_name(p)
            pid = _gdbus_get_process_id(p)
        except Exception:
            continue
        apps.append({"path": p, "name": name or "(unnamed)", "pid": pid})
    return apps


def _atspi_walk(
    bus_path: str,
    *,
    depth: int = 0,
    max_depth: int = 24,
    limit: int = 400,
    app_name: str = "",
    out: Optional[List[UIElement]] = None,
) -> List[UIElement]:
    """Recursive walker that yields one UIElement per interactable node.

    Bounds come from the AT-SPI Component interface's ``GetExtents``.
    Returns up to ``limit`` elements. Stops descending once the limit
    is hit so we never trash session context.
    """
    if out is None:
        out = []
    if depth > max_depth or len(out) >= limit:
        return out

    role = ""
    name = ""
    try:
        role = _gdbus_get_role(bus_path)
        name = _gdbus_get_name(bus_path)
    except Exception as e:
        logger.debug("props for %s failed: %s", bus_path, e)
        return out

    bounds: Tuple[int, int, int, int] = (0, 0, 0, 0)
    try:
        bounds = _gdbus_get_extents(bus_path)
    except Exception as e:
        logger.debug("GetExtents on %s failed: %s", bus_path, e)

    role_norm = role.lower().strip() if role else ""
    if role_norm in _INTERACTABLE_ROLES and (bounds[2] > 0 and bounds[3] > 0):
        if name:
            label = name
        elif role:
            label = f"<{role}>"
        else:
            label = ""
        if not label:
            return out
        idx = len(out) + 1
        out.append(UIElement(
            index=idx,
            role=role,
            label=label,
            bounds=bounds,
            app=app_name,
            pid=0,
            window_id=0,
        ))

    if len(out) >= limit:
        return out

    for child in _atspi_accessible_children(bus_path):
        _atspi_walk(
            child,
            depth=depth + 1,
            max_depth=max_depth,
            limit=limit,
            app_name=app_name,
            out=out,
        )
        if len(out) >= limit:
            break
    return out


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class LinuxCliBackend(ComputerUseBackend):
    """Concrete ``ComputerUseBackend`` for Linux desktops.

    Discovery is lazy: ``start()`` confirms ``ydotool`` + AT-SPI bus exist
    and caches an AT-SPI app enumeration; ``is_available()`` returns True
    iff those succeed.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = False
        # Cache the last AT-SPI app enumeration so list_apps / focus_app
        # don't hammer the bus.
        self._apps_cache: List[Dict[str, Any]] = []
        self._apps_cache_at: float = 0.0
        # Last-targeted app — used by SOM mode so capture_after=True
        # re-captures the same window rather than the frontmost.
        self._last_app: str = ""
        self._last_pid: int = 0
        # Element cache populated by the most recent capture(); used
        # by click/drag/scroll/set_value to resolve element= indices.
        self._last_elements: List[UIElement] = []
        _SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Lifecycle ──────────────────────────────────────────────────
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if _YDOTOOL is None:
                raise RuntimeError(
                    "LinuxCliBackend requires ydotool on PATH. "
                    "On NixOS: programs.ydotool.enable = true; group = \"uinput\". "
                    "On other distros: `apt install ydotool` or `pacman -S ydotool`."
                )
            if _GRIM is None:
                raise RuntimeError(_tool_hint("grim"))
            # ydotoold must have /dev/uinput — that's a host config concern,
            # not ours. We just smoke-test that the socket exists.
            if not Path("/run/ydotoold/socket").exists():
                # Maybe the user configured a different socket path. Not fatal.
                logger.warning(
                    "/run/ydotoold/socket missing — ydotool CLI will likely "
                    "fail until ydotoold is started. Check `systemctl --user "
                    "status ydotool` on systemd hosts."
                )
            # AT-SPI client registration: AT-SPI2 only populates the cache
            # when a client has registered a listener. Register as one.
            if Path(_ATSPI_BUS).exists():
                registered = _ensure_atspi_client_registration()
                if registered:
                    # Give the registry a brief moment to refresh.
                    time.sleep(0.1)
                self._apps_cache = _atspi_app_list() if registered else []
            else:
                self._apps_cache = []
            self._apps_cache_at = time.time()
            self._started = True

    def stop(self) -> None:
        with self._lock:
            self._started = False
            self._apps_cache = []

    def is_available(self) -> bool:
        if sys.platform != "linux":
            return False
        if _YDOTOOL is None or _GRIM is None:
            return False
        # AT-SPI is required for AX mode / element-resolution, but
        # vision-only capture (screenshot + coordinate clicks) works
        # without it. We report available if either input AND a
        # screenshot tool is present — the API surface for "I can
        # take a screenshot and click" is enough for many tasks.
        return Path(_ATSPI_BUS).exists() or _GRIM is not None

    # ── Capture ────────────────────────────────────────────────────
    def capture(self, mode: str = "som", app: Optional[str] = None) -> CaptureResult:
        png_b64: Optional[str] = None
        width = height = 0
        png_bytes_len = 0
        png_path: Optional[Path] = None

        # Reset the element cache at the start so a stale lookup doesn't
        # satisfy the call when a fresh app is in focus.
        self._last_elements = []

        try:
            if mode != "ax":
                # Always take a screenshot for "vision" and "som" modes.
                # On KDE Plasma without xdg-desktop-portal-kde grim will fail;
                # surface the error to the caller so they know.
                png_path = _SCREENSHOT_DIR / f"capture-{int(time.time() * 1000)}.png"
                proc = _run([_GRIM, str(png_path)], timeout=15)
                if proc.returncode == 0 and png_path.exists():
                    width, height = _image_dimensions_from_png(png_path)
                    raw = png_path.read_bytes()
                    png_bytes_len = len(raw)
                    import base64
                    png_b64 = base64.b64encode(raw).decode()
                else:
                    logger.warning(
                        "grim exited rc=%s: %s",
                        proc.returncode,
                        proc.stderr.strip() or "<no stderr>",
                    )
        except Exception as e:
            logger.warning("screenshot failed: %s", e)

        # Refresh app cache periodically.
        if not self._apps_cache or (time.time() - self._apps_cache_at) > 5.0:
            try:
                self._apps_cache = _atspi_app_list()
                self._apps_cache_at = time.time()
            except Exception as e:
                logger.debug("AT-SPI app re-enumeration failed: %s", e)

        chosen = self._resolve_app(app)
        elements: List[UIElement] = []
        app_name = ""
        window_title = ""
        if chosen is not None:
            app_name = chosen["name"]
            window_title = chosen["name"]
            self._last_app = app_name
            self._last_pid = chosen["pid"]
            try:
                elements = _atspi_walk(
                    chosen["path"],
                    limit=400,
                    app_name=app_name,
                )
            except Exception as e:
                logger.warning("AT-SPI walk failed: %s", e)

        # Re-index elements based on the surviving list (1-based SOM).
        for i, e in enumerate(elements, start=1):
            e.index = i

        # Drop the temp PNG so we don't accumulate garbage.
        if png_path is not None:
            try:
                png_path.unlink()
            except Exception:
                pass

        # Stash for the click/drag/scroll element-resolver. Make the
        # user's "ax" mode carry the elements too — that path already
        # exists, this just narrows the gap to "vision" which is meant
        # to be a screen-only path anyway.
        self._last_elements = list(elements)

        return CaptureResult(
            mode=mode,
            width=width,
            height=height,
            png_b64=png_b64,
            elements=elements,
            app=app_name,
            window_title=window_title,
            png_bytes_len=png_bytes_len,
        )

    def _resolve_app(self, app: Optional[str]) -> Optional[Dict[str, Any]]:
        if not self._apps_cache:
            return None
        if app:
            sub = app.lower()
            for entry in self._apps_cache:
                if sub in entry["name"].lower():
                    return entry
        # Fall back to "last targeted" so capture_after= follow-ups
        # re-bind to the same app.
        if self._last_app:
            for entry in self._apps_cache:
                if entry["name"].lower() == self._last_app.lower():
                    return entry
        # Last resort: first entry.
        return self._apps_cache[0]

    # ── Pointer ────────────────────────────────────────────────────
    def click(
        self,
        *,
        element: Optional[int] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        button: str = "left",
        click_count: int = 1,
        modifiers: Optional[List[str]] = None,
    ) -> ActionResult:
        cx: Optional[int] = x
        cy: Optional[int] = y
        if element is not None:
            el = self._element_by_index(element)
            if el is None:
                return ActionResult(
                    ok=False, action="click",
                    message=(f"element #{element} not found — call "
                             "computer_use(action='capture') first"),
                )
            cx, cy = el.center()
            if cx is None or cy is None or el.bounds[2] == 0:
                return ActionResult(
                    ok=False, action="click",
                    message=(f"element #{element} ({el.role}) has no screen "
                             "bounds — cannot click by element index"),
                )

        if cx is None or cy is None:
            return ActionResult(
                ok=False, action="click",
                message="click requires element= or x/y",
            )

        cmd = [_YDOTOOL, "mousetap", "--"]
        # ydotool uses keysym button names: "left" / "right" / "middle"
        # (the same names as xdotool). Cursor goes from neutral to (x,y)
        # implicitly because mousetap is click-in-place.
        cmd += [button, str(cx), str(cy)]
        if click_count > 1:
            cmd += ["--repeat", str(click_count)]
        try:
            proc = _run(cmd, timeout=5)
        except Exception as e:
            return ActionResult(
                ok=False, action="click",
                message=f"ydotool mousetap failed: {e}",
            )
        if proc.returncode != 0:
            return ActionResult(
                ok=False, action="click",
                message=f"ydotool returned rc={proc.returncode}: {proc.stderr.strip()}",
            )
        return ActionResult(
            ok=True, action="click",
            message=f"clicked ({cx},{cy}) button={button}{(' x'+str(click_count)) if click_count>1 else ''}",
        )

    def drag(
        self,
        *,
        from_element: Optional[int] = None,
        to_element: Optional[int] = None,
        from_xy: Optional[Tuple[int, int]] = None,
        to_xy: Optional[Tuple[int, int]] = None,
        button: str = "left",
        modifiers: Optional[List[str]] = None,
    ) -> ActionResult:
        if from_element is not None:
            el = self._element_by_index(from_element)
            if el is None:
                return ActionResult(ok=False, action="drag",
                    message=f"from_element #{from_element} not found")
            fx, fy = el.center()
        elif from_xy:
            fx, fy = int(from_xy[0]), int(from_xy[1])
        else:
            return ActionResult(ok=False, action="drag",
                message="drag requires from_element or from_xy")

        if to_element is not None:
            el = self._element_by_index(to_element)
            if el is None:
                return ActionResult(ok=False, action="drag",
                    message=f"to_element #{to_element} not found")
            tx, ty = el.center()
        elif to_xy:
            tx, ty = int(to_xy[0]), int(to_xy[1])
        else:
            return ActionResult(ok=False, action="drag",
                message="drag requires to_element or to_xy")

        # ydotool's mousemove path: combine mousedown + several mousemove
        # steps + mouseup. ydotool doesn't have a one-shot drag primitive,
        # but the kernel driver doesn't care — dispatch the events.
        cmds: List[List[str]] = [
            [_YDOTOOL, "mousedown", "--", button, str(fx), str(fy)],
        ]
        # 10 intermediate steps is enough for casual drag-and-drop.
        steps = 10
        for i in range(1, steps):
            ix = fx + (tx - fx) * i // steps
            iy = fy + (ty - fy) * i // steps
            cmds.append([_YDOTOOL, "mousemove", "--", str(ix), str(iy)])
        cmds.append([_YDOTOOL, "mouseup", "--", button, str(tx), str(ty)])
        try:
            for cmd in cmds:
                proc = _run(cmd, timeout=5)
                if proc.returncode != 0:
                    return ActionResult(ok=False, action="drag",
                        message=f"ydotool step failed rc={proc.returncode}: {proc.stderr.strip()}")
        except Exception as e:
            return ActionResult(ok=False, action="drag", message=str(e))
        return ActionResult(ok=True, action="drag",
            message=f"dragged ({fx},{fy}) → ({tx},{ty})")

    def scroll(
        self,
        *,
        direction: str,
        amount: int = 3,
        element: Optional[int] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        modifiers: Optional[List[str]] = None,
    ) -> ActionResult:
        # Move pointer to (x,y) if either was supplied.
        if element is not None:
            el = self._element_by_index(element)
            if el is None:
                return ActionResult(ok=False, action="scroll",
                    message=f"element #{element} not found")
            x, y = el.center()
        if x is not None and y is not None:
            _run([_YDOTOOL, "mousemove", "--", str(x), str(y)], timeout=5)

        # ydotool's wheel keysyms: scroll == "wheel", value 1 (down),
        # -1 (up), 2 (right), -2 (left). See ydotool's keys.h.
        direction_map = {
            "down": "1", "up": "-1",
            "right": "2", "left": "-2",
        }
        value = direction_map.get(direction.lower())
        if value is None:
            return ActionResult(
                ok=False, action="scroll",
                message=f"bad direction {direction!r}; use up|down|left|right",
            )
        try:
            for _ in range(max(1, min(50, amount))):
                proc = _run(
                    [_YDOTOOL, "key", "wheel:" + value],
                    timeout=5,
                )
                if proc.returncode != 0:
                    return ActionResult(
                        ok=False, action="scroll",
                        message=f"ydotool key wheel:{value} rc={proc.returncode}",
                    )
        except Exception as e:
            return ActionResult(ok=False, action="scroll", message=str(e))
        return ActionResult(
            ok=True, action="scroll",
            message=f"scrolled {direction} x{amount}",
        )

    # ── Keyboard ──────────────────────────────────────────────────
    def type_text(self, text: str) -> ActionResult:
        if not text:
            return ActionResult(ok=True, action="type", message="empty text")
        # Prefer wtype — it's IME-aware and handles accelerators the
        # user's layout doesn't see directly. Fall back to ydotool type.
        if _WTYPE is not None:
            try:
                proc = _run([_WTYPE, "--", text], timeout=10)
                if proc.returncode == 0:
                    return ActionResult(
                        ok=True, action="type",
                        message=f"typed {len(text)} chars via wtype",
                    )
                logger.debug("wtype rc=%s: %s — falling back to ydotool",
                             proc.returncode, proc.stderr.strip())
            except Exception as e:
                logger.debug("wtype failed: %s — falling back to ydotool", e)
        # ydotool path: write text to a temp file and use `ydotool type -`.
        try:
            with tempfile.NamedTemporaryFile(
                "w", suffix=".txt", delete=False, encoding="utf-8"
            ) as fh:
                fh.write(text)
                tmp_path = fh.name
            try:
                proc = _run([_YDOTOOL, "type", "--file", tmp_path], timeout=20)
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            if proc.returncode != 0:
                return ActionResult(
                    ok=False, action="type",
                    message=f"ydotool type rc={proc.returncode}: {proc.stderr.strip()}",
                )
            return ActionResult(
                ok=True, action="type",
                message=f"typed {len(text)} chars via ydotool",
            )
        except Exception as e:
            return ActionResult(ok=False, action="type", message=str(e))

    def key(self, keys: str) -> ActionResult:
        # Convert "ctrl+s" / "alt+Tab" / "Return" into ydotool keysym form:
        # ydotool accepts individual keysyms separated by spaces, or
        # "KEY1:KEY2" for held-down combos (we use the spaced form).
        if not keys:
            return ActionResult(ok=False, action="key",
                message="empty keys string")
        parts = re.split(r"\s*\+\s*", keys.strip())
        normalized: List[str] = []
        for p in parts:
            tok = p.strip()
            if not tok:
                continue
            low = tok.lower()
            # Aliases that the model might pass.
            low = {
                "control": "ctrl",
                "command": "super",
                "cmd": "super",
                "option": "alt",
                "return": "enter",
                "esc": "escape",
                "space": "spacebar",
                "del": "delete",
            }.get(low, low)
            normalized.append(low)
        if not normalized:
            return ActionResult(ok=False, action="key",
                message=f"no keysym tokens parsed from {keys!r}")
        try:
            proc = _run(
                [_YDOTOOL, "key", "--clearmodifiers", *normalized],
                timeout=5,
            )
        except Exception as e:
            return ActionResult(ok=False, action="key", message=str(e))
        if proc.returncode != 0:
            return ActionResult(
                ok=False, action="key",
                message=f"ydotool key rc={proc.returncode}: {proc.stderr.strip()}",
            )
        return ActionResult(
            ok=True, action="key",
            message=f"sent keys {'+'.join(normalized)}",
        )

    # ── Introspection ──────────────────────────────────────────────
    def list_apps(self) -> List[Dict[str, Any]]:
        try:
            self._apps_cache = _atspi_app_list()
            self._apps_cache_at = time.time()
        except Exception as e:
            logger.debug("list_apps refresh: %s", e)
        return [
            {"name": a["name"], "pid": a["pid"]}
            for a in self._apps_cache
        ]

    def focus_app(self, app: str, raise_window: bool = False) -> ActionResult:
        # raise_window is intentionally ignored — same reasoning as cua:
        # stealing focus is exactly what this backend is designed to avoid.
        if not app:
            return ActionResult(ok=False, action="focus_app",
                message="focus_app requires `app`")
        # Refresh app cache before searching.
        try:
            self._apps_cache = _atspi_app_list()
            self._apps_cache_at = time.time()
        except Exception as e:
            return ActionResult(ok=False, action="focus_app",
                message=f"AT-SPI refresh failed: {e}")
        sub = app.lower()
        chosen = None
        for entry in self._apps_cache:
            if sub == entry["name"].lower():
                chosen = entry
                break
        if chosen is None:
            for entry in self._apps_cache:
                if sub in entry["name"].lower():
                    chosen = entry
                    break
        if chosen is None:
            return ActionResult(
                ok=False, action="focus_app",
                message=(f"no AT-SPI app matches {app!r}; available: "
                         + ", ".join(a["name"] for a in self._apps_cache[:8])),
            )
        self._last_app = chosen["name"]
        self._last_pid = chosen["pid"]
        return ActionResult(
            ok=True, action="focus_app",
            message=(f"targeted {chosen['name']} (pid {chosen['pid']}); "
                     "no raise, kernel input still routes to currently "
                     "focused window — verify with capture() that the "
                     "intended app is foreground."),
        )

    # ── Native-value mutation ──────────────────────────────────────
    def set_value(self, value: str, element: Optional[int] = None) -> ActionResult:
        # The simplest reliable cross-WM way to "set a value" is to:
        # 1) click on the element to focus it,
        # 2) select-all + delete + type the value.
        if element is None:
            return ActionResult(ok=False, action="set_value",
                message="set_value requires element=")
        el = self._element_by_index(element)
        if el is None:
            return ActionResult(ok=False, action="set_value",
                message=f"element #{element} not found")
        cx, cy = el.center()
        if cx is None or cy is None or el.bounds[2] == 0:
            return ActionResult(ok=False, action="set_value",
                message=(f"element #{element} ({el.role}) has no screen "
                         "bounds — cannot target by click"))
        # Click to focus.
        try:
            proc = _run(
                [_YDOTOOL, "mousetap", "--", "left", str(cx), str(cy)],
                timeout=5,
            )
            if proc.returncode != 0:
                return ActionResult(ok=False, action="set_value",
                    message=f"focus-click rc={proc.returncode}")
        except Exception as e:
            return ActionResult(ok=False, action="set_value", message=str(e))
        # Clear existing content. Ctrl+A then Delete is the safest
        # cross-platform combo; on macOS it's Cmd+A but we're on Linux.
        for combo in ("ctrl+a", "delete"):
            res = self.key(combo)
            if not res.ok:
                return ActionResult(ok=False, action="set_value",
                    message=f"failed during select-all/clear: {res.message}")
        # Type the new value.
        typed = self.type_text(value)
        if not typed.ok:
            return typed
        return ActionResult(
            ok=True, action="set_value",
            message=f"set value {value!r} on element #{element}",
        )

    def _element_by_index(self, idx: int) -> Optional[UIElement]:
        """Helper used by click/drag/scroll/set_value for element-resolution.

        Backed by an internal cache populated by the most recent
        ``capture()``. Resolution is by SOM index (1-based) within the
        current app's tree.
        """
        cache = getattr(self, "_last_elements", None)
        if not cache:
            return None
        for el in cache:
            if el.index == idx:
                return el
        return None

    def wait(self, seconds: float) -> ActionResult:
        return super().wait(seconds)
