"""DEPRECATED — see plugins/session_bus/.

This plugin (session_routing) has been merged into ``session-bus`` (v1.0.0).
All six model tools now live under the unified plugin:

    broadcast toolsets  -> bus_emit, bus_observe
    routing    toolsets -> bus_handle, bus_route_send, bus_route_list, bus_establish

This directory remains for one minor cycle so existing operator configs
that reference the old plugin keep loading. The ``register`` function
below registers the new tools under their new names with a one-shot
DeprecationWarning so any running skill or prompt that calls the old
names will see a clear upgrade signal in the gateway log.

The old tool names (``session_emit`` etc.) are NOT registered anymore --
they were renamed to ``bus_*``. Update your skills/prompts to call the
new names. See ``plugins/session_bus/README.md`` for the rename map.
"""
import warnings as _warnings


_warned = [False]


def _maybe_warn_once() -> None:
    if _warned[0]:
        return
    _warned[0] = True
    _warnings.warn(
        "session-bridge and session-routing are deprecated; install "
        "session-bus instead. Tools renamed from session_* to bus_*. "
        "See plugins/session_bus/README.md.",
        DeprecationWarning,
        stacklevel=3,
    )


def register(ctx) -> None:
    _maybe_warn_once()
    from plugins.session_bus import register as _new_register
    _new_register(ctx)
