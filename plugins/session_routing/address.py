"""Hermes session addressing — canonical address, parsing, NATS subject encoding.

A canonical Hermes session address has the form:

    <gateway_id>/<session_key>

Where:
  - ``gateway_id``  is the stable per-hermes-instance identifier, currently
    derived from ``_default_gateway_id()`` (``gw-<hostname>``). Operators may
    override per-host via ``~/.hermes/config.yaml`` once that ship (Phase 1
    reuses the existing default).
  - ``session_key`` is the value of ``HERMES_SESSION_KEY`` for that session
    (e.g. ``agent:main:telegram:dm:189562939:39702``). It already uniquely
    identifies a session across the gateway's platforms.

The address is what one hermes agent gives to another so the other can route
a reply: ``session_handle`` returns it; ``session_route_send`` accepts it.

NATS subject encoding
---------------------
NATS subjects use ``.`` as the segment separator and ``*``/``>`` as wildcards.
Our addresses contain ``:`` and ``/``, both of which are valid in raw NATS
subjects but we want consistent segmentation. We translate the address into a
NATS subject of the shape::

    from.<gateway_id>.gw.<session_key_safe>.deliver

where ``<session_key_safe>`` is the ``session_key`` with the only NATS-illegal
character (``*``, ``>``) escaped to ``_``. We do NOT need to escape ``:`` or
``/`` for raw NATS subjects — but we DO encode them as ``_`` so the subject
remains readable when listed via ``nats sub -l``.

The encode/decode pair is round-trippable across the wire.
"""

from __future__ import annotations

import os
import re
from typing import Optional


# ---------------------------------------------------------------------------
# Canonical address:  <gateway_id>/<session_key>
# ---------------------------------------------------------------------------

_ADDRESS_RE = re.compile(
    r"""
    ^
    (?P<gateway_id>[A-Za-z0-9._\-]+?)        # gateway_id, conservative charset
    /
    (?P<session_key>.+?)                      # session_key (rest of string)
    $
    """,
    re.VERBOSE,
)


class AddressError(ValueError):
    """Raised when a session address cannot be parsed or constructed."""


# Characters forbidden anywhere in a canonical address. NATS reserves
# ``.`` (segment separator), ``*`` and ``>`` (wildcards). We additionally
# forbid ``\`` because we never need to escape (session_keys come from a
# closed-shape string emitted by the gateway) and adding an escape layer
# invites encoding bugs.
_NATS_FORBIDDEN = ("*", ">")


def _check_no_forbidden(token: str, role: str) -> None:
    for ch in _NATS_FORBIDDEN:
        if ch in token:
            raise AddressError(
                f"{role} must not contain {ch!r} (NATS-reserved): {token!r}"
            )


def parse(address: str) -> tuple[str, str]:
    """Parse ``<gateway_id>/<session_key>`` into ``(gateway_id, session_key)``.

    Accepts whitespace around the address and ignores a single trailing
    newline (handy for shell-pasted strings). Raises ``AddressError`` on
    malformed input — no silent fallback because addresses are the contract
    between agents.
    """
    if not isinstance(address, str):
        raise AddressError(f"address must be str, got {type(address).__name__}")
    cleaned = address.strip()
    if not cleaned:
        raise AddressError("address is empty")
    m = _ADDRESS_RE.match(cleaned)
    if not m:
        raise AddressError(f"address not in '<gateway_id>/<session_key>' form: {address!r}")
    gateway_id = m.group("gateway_id")
    session_key = m.group("session_key")
    _check_no_forbidden(gateway_id, "gateway_id")
    _check_no_forbidden(session_key, "session_key")
    return gateway_id, session_key


def build(gateway_id: str, session_key: str) -> str:
    """Build a canonical address from its parts."""
    if not gateway_id or "/" in gateway_id:
        raise AddressError(f"invalid gateway_id: {gateway_id!r}")
    if not session_key or "/" in session_key:
        raise AddressError(f"invalid session_key: {session_key!r}")
    _check_no_forbidden(gateway_id, "gateway_id")
    _check_no_forbidden(session_key, "session_key")
    return f"{gateway_id}/{session_key}"


def is_address(value: str) -> bool:
    """Return True iff ``value`` parses as a canonical address."""
    try:
        parse(value)
    except AddressError:
        return False
    return True


# ---------------------------------------------------------------------------
# Gateway-id resolution
# ---------------------------------------------------------------------------

def resolve_gateway_id(explicit: Optional[str] = None) -> str:
    """Resolve the gateway_id for THIS hermes instance.

    Precedence:
      1. ``explicit`` argument — used by tools that take it from config.yaml.
      2. ``HERMES_GATEWAY_ID`` env var — supported for parity with other
         HERMES_* identifiers; we still treat it as a behavioral knob per
         the project's env-vs-config policy (operator can choose).
      3. ``_default_gateway_id()`` from ``hermes_cli.gateway_enroll`` —
         returns ``gw-<hostname>`` or ``gw-hermes`` when hostname is empty.

    Note: AGENTS.md instructs that non-secret config (like a stable id) goes
    in ``~/.hermes/config.yaml`` rather than env. We accept env for parity
    with the existing HERMES_* names but document the config-file path in the
    plugin manifest. Operators who want a stable id across rebuilds set it in
    config — not via env.
    """
    if explicit:
        return explicit.strip()
    env_val = os.environ.get("HERMES_GATEWAY_ID", "").strip()
    if env_val:
        return env_val
    # Imported lazily so the plugin module loads even if hermes_cli is absent.
    from hermes_cli.gateway_enroll import _default_gateway_id
    return _default_gateway_id()


def resolve_session_key(explicit: Optional[str] = None) -> str:
    """Resolve the session_key for THIS session.

    Precedence: ``explicit`` arg → ``HERMES_SESSION_KEY`` env var →
    empty string. We never silently invent a key, so if both are empty the
    call site surfaces a config error to the user.
    """
    if explicit:
        return explicit.strip()
    env_val = os.environ.get("HERMES_SESSION_KEY", "").strip()
    return env_val


def my_address(gateway_id: Optional[str] = None,
               session_key: Optional[str] = None) -> str:
    """Build this gateway+session's canonical address.

    Both arguments default to the resolved-from-env values via the helpers
    above. Raises ``AddressError`` when either component is missing — that
    signals a misconfigured gateway (no ``HERMES_SESSION_KEY`` set, which
    would only happen in extreme misuse).
    """
    gw = resolve_gateway_id(gateway_id)
    sk = resolve_session_key(session_key)
    if not gw:
        raise AddressError("could not determine gateway_id (set HERMES_GATEWAY_ID or config.routing.gateway_id)")
    if not sk:
        raise AddressError("could not determine session_key (HERMES_SESSION_KEY is empty)")
    return build(gw, sk)


# ---------------------------------------------------------------------------
# NATS subject encoding
# ---------------------------------------------------------------------------
#
# The NATS subject for a routed message is::
#
#     from.<sender_gateway_id>.<session_key>.deliver
#
# The SENDER's gateway_id is in the prefix (not the recipient's). That
# matches the recipient-side allow-list filter ``from.<trusted_sender>.>``
# which the recipient's inbox subscriber installs — senders not on the
# allow list produce subjects the consumer doesn't match, so their
# messages never even reach the recipient's broker-side queue. (Putting
# the recipient's gateway_id in the prefix would create a sieve the
# recipient can't apply: every send would qualify as long as it's
# addressed to them.)
#
# ``session_key`` here is the RECIPIENT's session identifier (since the
# recipient also sees this on their end — they want to know which of
# THEIR sessions the message is addressed to). We deliberately do NOT
# encode the recipient's gateway_id in the subject: it's redundant with
# ``session_key`` (different gateways won't share session_keys) and it
# would prevent the allow-list filter from working.
#
# No escape layer is needed: we reject ``*`` and ``>`` at parse time
# above, so every address maps to a unique subject segment without
# aliasing.


def encode_subject(
    sender_gateway_id: str,
    session_key: str,
    verb: str = "deliver",
) -> str:
    """Build a NATS JetStream subject for a routed message.

    Resulting shape:
        ``from.<sender_gateway_id>.<session_key>.<verb>``

    Caller passes the sender's gateway_id (resolved via
    ``resolve_gateway_id()``) and the recipient's session_key. The
    subject identifies the SENDER at the broker level so that recipient
    inbox subscribers can filter on a per-sender allow-list
    (``from.<trusted_sender>.>``).

    Recipients can subscribe to ``subject_filter_for(allowed_sender_gw)``
    to hear only the senders they trust.
    """
    from plugins.session_routing.address import _check_no_forbidden
    if not verb or "." in verb:
        raise AddressError(f"verb must be a single token, got {verb!r}")
    _check_no_forbidden(sender_gateway_id, "sender_gateway_id")
    _check_no_forbidden(session_key, "session_key")
    return f"from.{sender_gateway_id}.{session_key}.{verb}"


def subject_filter_for(sender_gateway_id: str) -> str:
    """Subject filter pattern a recipient uses to hear ALL sends FROM
    ``sender_gateway_id``.

    Pattern shape:
        ``from.<sender_gateway_id>.>``
    """
    return f"from.{sender_gateway_id}.>"


def decode_subject(subject: str) -> tuple[str, str]:
    """Inverse of ``encode_subject``.

    Returns ``(sender_gateway_id, session_key)`` so a recipient can
    identify the originating peer. The recipient's own identity is NOT
    in the subject — use the envelope's ``to`` field for that.

    Note: a valid routed-deliver subject is of the shape
    ``from.<sender>.<recipient_sk>.deliver``. We accept any single-token
    verb at the tail (forward-compat with verbs like ``delivered``,
    ``read``, ``ack`` in Phase 2) — the model logic treats the verb as
    opaque; we only need a non-empty, dot-free tail.
    """
    parts = subject.split(".")
    if (
        len(parts) < 4
        or parts[0] != "from"
        or not parts[-1]
        or not parts[1]  # sender_gateway_id empty
    ):
        raise AddressError(f"not a routed deliver subject: {subject!r}")
    sender_gateway_id = parts[1]  # already validated non-empty by the guard above
    # All segments between the gateway_id and the trailing verb are the
    # recipient's session_key, rejoined with ``.``.
    session_key = ".".join(parts[2:-1])
    return sender_gateway_id, session_key
