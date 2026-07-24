from __future__ import annotations

import time
from typing import Any

import pytest

from plugins.session_routing.address import AddressError, parse as parse_address
from plugins.wakeword_bridge.auth import compute_signature, verify_signature
from plugins.wakeword_bridge.handler import BridgeContext, GatewayRunner
from plugins.wakeword_bridge.__init__ import WakewordRuntime


def test_hmac_round_trip() -> None:
    # Given
    secret = bytes.fromhex("11" * 32)
    body = b"captured-ogg-audio"
    timestamp = int(time.time())

    # When
    signature = compute_signature(secret, body, timestamp)

    # Then
    assert verify_signature(secret, body, timestamp, signature)
    assert not verify_signature(secret, body + b"tampered", timestamp, signature)


def test_hmac_stale_timestamp() -> None:
    # Given
    secret = bytes.fromhex("22" * 32)
    body = b"captured-ogg-audio"
    timestamp = int(time.time()) - 60
    signature = compute_signature(secret, body, timestamp)

    # When
    verified = verify_signature(secret, body, timestamp, signature)

    # Then
    assert not verified


def test_address_parse_rejects_invalid() -> None:
    # Given
    valid = "gw-minipc/agent:main:telegram:dm:123:456"

    # When / Then
    with pytest.raises(AddressError):
        parse_address("garbage")
    assert parse_address(valid) == (
        "gw-minipc",
        "agent:main:telegram:dm:123:456",
    )


def test_hook_callbacks_absorb_infrastructure_kwargs() -> None:
    # Gateway's PluginManager.invoke_hook injects tooling kwargs such as
    # `telemetry_schema_version` into every hook call. Hook callbacks must
    # accept **_kwargs or Python raises TypeError before the coroutine is
    # created. The aiohttp server is *not* started in this test — we only
    # verify the method signature.

    # Given
    runtime = WakewordRuntime(secret=bytes.fromhex("33" * 32))
    fake_gateway = _FakeGateway()

    # When / Then: the kwargs that invoke_hook injects must not raise.
    # We can't await a real coroutine on a non-running event loop, so we
    # use the underlying unbound coroutine function via .close() to assert
    # no-args-call-parse error.
    import inspect

    sig_start = inspect.signature(runtime.on_gateway_start)
    assert "_kwargs" in sig_start.parameters, (
        "on_gateway_start must accept **_kwargs to absorb PluginManager.invoke_hook kwargs"
    )
    sig_stop = inspect.signature(runtime.on_gateway_stop)
    assert "_kwargs" in sig_stop.parameters, (
        "on_gateway_stop must accept **_kwargs to absorb PluginManager.invoke_hook kwargs"
    )


class _FakeGateway:
    """Stand-in for GatewayRunner — never has enqueue called in this test."""

    def enqueue_internal_session_event(self, session_key: str, event: Any) -> bool:
        return True
