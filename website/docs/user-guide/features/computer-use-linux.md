---
title: Computer Use (Linux / KDE Plasma)
---

# Computer Use (Linux)

Hermes Agent can drive your Linux desktop — clicking, typing, scrolling,
dragging — **in the background**, same as on macOS. Your cursor doesn't
move, keyboard focus doesn't change, the compositor doesn't raise a
window or switch a virtual desktop. You and the agent co-work on the
same machine.

Unlike most computer-use integrations, this works with **any
tool-capable model** — Claude, GPT, Gemini, or an open model on a local
vLLM endpoint. There's no Anthropic-native schema to worry about.

## How it works

The Linux backend (`LinuxCliBackend`) drives your existing desktop
session through small CLIs that are already on NixOS / KDE Plasma /
GNOME hosts:

| Capability | Tool | Notes |
|---|---|---|
| Accessibility tree (apps, elements, bounds) | AT-SPI / a11y D-Bus, queried via `gdbus call` | No PyGObject dependency. Backend auto-registers as an AT client on first use so apps expose their tree. |
| Mouse click / drag / scroll | `ydotool mousetap` / `mousedown` / `mousemove` / `mouseup` | Goes through `ydotoold` → `/dev/uinput`, not the cursor. |
| Keyboard typing | `wtype -- <text>` (preferred) → `ydotool type --file <tmp>` (fallback) | `wtype` is IME-aware; `ydotool` is the kernel-direct path. |
| Hotkey combos (Ctrl+C, Alt+Tab, Enter) | `ydotool key --clearmodifiers <keysyms>` | Aliases map: `command` → `super`, `option` → `alt`, `return` → `enter`. |
| Screenshot | `grim` (PNG, base64 returned inline) | Requires `xdg-desktop-portal-kde` for screen capture on KWin; see NixOS snippet below. |
| Clipboard | `wl-copy` / `wl-paste` | X11 fallbacks supported. |

The backend is **not a focus-stealer**. We never `raise_window` the
target app. The agent reads the input tree through AT-SPI and posts
synthesised events through uinput; the user's real focus stays where
they left it.

## Enabling (NixOS)

If your `hosts/<host>/variables.nix` already has
`services.hermesLocal.nativeControl = true`, the package list is half
done. Add the screen-capture portal and the AT-SPI introspection
helpers:

```nix
# hosts/thinkpad/configuration.nix
{ pkgs, vars, ... }:

let
  hermesLocal = vars.services.hermesLocal or { };
  # ...
in
{
  # 1. uinput ACL for raw userland access (ydotoold already covers
  # the daemon path; this is for any future non-daemon uinput client).
  services.udev.extraRules = ''
    KERNEL=="uinput", GROUP="uinput", MODE="0660"
  '';

  # 2. Add the portal backend required by grim on the KWin compositor.
  environment.systemPackages = with pkgs; [
    pciutils
  ] ++ lib.optionals vars.services.hermesLocal.enable (with pkgs; [
    hermes-agent
    raft
    xdg-desktop-portal
    xdg-desktop-portal-kde   # for KWin — use xdg-desktop-portal-wlr on Sway/Hyprland
    dbus                     # provides dbus-send; mostly transitive
    glib                     # provides gdbus — required for AT-SPI method calls
  ]) ++ lib.optionals vars.services.hermesLocal.nativeControl (with pkgs; [
    ydotool
    wtype
    xdotool
    wl-clipboard
    grim
    slurp
  ]);

  # 3. Enable the portal so grim / xdg-desktop-portal-kde can capture.
  xdg.portal = {
    enable = true;
    extraPortals = with pkgs; [
      xdg-desktop-portal-kde
    ];
  };
}
```

After rebuild, relogin so the new groups + portal apply, then:

```bash
hermes computer-use status
# LinuxCliBackend: ready (ydotool + grim + gdbus on PATH)
#   AT-SPI bus: /run/user/1000/at-spi/bus_0
#   Optional: wtype=True, wl-copy=True
#   Customise with HERMES_COMPUTER_USE_BACKEND=linux|cua
```

## Enabling (other distros)

Install the same set of CLIs:

| Distro | Command |
|---|---|
| Debian / Ubuntu | `sudo apt install ydotool wtype xdotool wl-clipboard grim slurp libglib2.0-bin xdg-desktop-portal xdg-desktop-portal-gtk` (replace `-gtk` with `-kde` on Plasma) |
| Fedora | `sudo dnf install ydotool wtype xdotool wl-clipboard grim slurp glib2 xdg-desktop-portal xdg-desktop-portal-gnome` |
| Arch | `sudo pacman -S ydotool wtype xdotool wl-clipboard grim slurp glib2 xdg-desktop-portal xdg-desktop-portal-gnome` |

Then make sure:

1. `ydotoold` is running (systemd user service unit
   `ydotool.service` on most distros).
2. `at-spi2-registryd` is running (the GNOME `at-spi2-core` package
   on most distros; auto-starts under KDE Plasma).
3. Your session's `DBUS_SESSION_BUS_ADDRESS` is set (default on most
   Wayland compositors).

## Backend selection

`hermes-agent` picks the backend automatically:

| Platform | Backend | Why |
|---|---|---|
| macOS | `CuaDriverBackend` (existing) | Background computer-use via SkyLight SPIs. |
| Linux | `LinuxCliBackend` (new) | AT-SPI + uinput CLI toolchain. |
| Other | None | Falls back to a clear error in `hermes tools`. |

Override via `HERMES_COMPUTER_USE_BACKEND`:

- `linux` — always pick the CLI backend.
- `cua` — always pick the macOS MCP backend.
- `auto` (default) — choose based on platform + availability.
- `noop` — tests / CI.

## Usage

```python
computer_use(action="capture", mode="som")          # raw PNG + accessibility tree
computer_use(action="capture", mode="ax")            # text-only accessibility tree
computer_use(action="capture", mode="vision", app="Konsole")  # PNG of named app
computer_use(action="list_apps")                     # enumerate visible apps
computer_use(action="focus_app", app="Konsole")      # target without raising
computer_use(action="click", element=12)             # click by SOM index
computer_use(action="click", x=1234, y=567)         # click by coordinate
computer_use(action="drag", from_element=3, to_element=17)
computer_use(action="type", text="hello world")
computer_use(action="key", keys="ctrl+s")
computer_use(action="scroll", direction="down", amount=5)
computer_use(action="set_value", value="new", element=42)
```

Always re-capture before any element-indexed action — element indices
are only stable within one capture.

## Known limitations vs cua-driver

1. **No SOM-overlaid PNGs** — we don't ship Pillow in the
   hermes-agent runtime to draw number overlays. Vision models still
   get the raw PNG; AX mode returns text + bounds. Same as the
   existing `mode="ax"` path on macOS.
2. **No focus stealing** — same as cua-driver. After
   `focus_app(name)`, verify the intended app is foreground by
   re-running `capture(mode="ax")`.
3. **Qt apps register accessibility lazily** — Qt apps register
   their AT-SPI roots only when an AT client is active. The backend
   registers itself on first use, so the cache populates within ~100 ms.

## See also

- [Computer Use (macOS)](computer-use.md) — the cua-driver path.
- [Built-in Tools Reference](../../reference/tools-reference) — the
  full tool / toolset catalogue.
- `tools/computer_use/linux_backend.py` — source (~1200 lines).
