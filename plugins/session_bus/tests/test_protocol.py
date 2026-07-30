"""Offline tests for the closed typed-message registry (protocol.py)."""

import pytest

from plugins.session_bus import handshake, protocol
from plugins.session_bus.protocol import (
    ERROR_CODE_UNKNOWN_TYPE,
    ERROR_CODE_VALIDATION,
    PROTOCOL_VERSION,
    REGISTERED_TYPES,
    ProtocolError,
    build_ack_delivery,
    build_message_error,
    build_message_text,
    is_app_namespaced,
    is_registered_type,
    validate_payload,
    validate_payload_strict,
)

CH = "20260706_174653_5ed43bcb:a1b2c3d4"
SID = "20260706_174653_5ed43bcb"


def _valid_text(**overrides):
    payload = build_message_text(
        channel_id=CH, session_id=SID, body="hello over back-channel"
    )
    payload.update(overrides)
    return payload


class TestRegistry:
    def test_registry_is_closed_and_complete(self):
        assert REGISTERED_TYPES == {
            "handshake.request",
            "handshake.ack",
            "handshake.established",
            "handshake.reject",
            "handshake.bye",
            "message.text",
            "message.error",
            "message.ack_delivery",
            "delegate.task",
            "delegate.result",
        }

    def test_is_registered_type(self):
        assert is_registered_type("message.text")
        assert not is_registered_type("message.evil")
        assert not is_registered_type(None)
        assert not is_registered_type(42)

    def test_app_namespace_convention(self):
        assert is_app_namespaced("app.kanban.task_update")
        assert is_app_namespaced("app.my-plugin.some.nested.type")
        assert not is_app_namespaced("kanban.task_update")
        assert not is_app_namespaced("app.")
        assert not is_app_namespaced("app.only_plugin")
        # Reserved but NOT registered in Phase 1 — still unknown.
        ok, code, _ = validate_payload(_valid_text(type="app.kanban.x"))
        assert not ok and code == ERROR_CODE_UNKNOWN_TYPE


class TestBuilders:
    def test_message_text_validates(self):
        ok, code, detail = validate_payload(_valid_text())
        assert ok, (code, detail)

    def test_message_error_validates(self):
        payload = build_message_error(
            channel_id=CH,
            session_id=SID,
            error_code="unknown_type",
            in_reply_to="bad-msg-id",
            detail="no such type",
        )
        ok, code, detail = validate_payload(payload)
        assert ok, (code, detail)
        assert payload["in_reply_to"] == "bad-msg-id"

    def test_ack_delivery_validates(self):
        payload = build_ack_delivery(
            channel_id=CH, session_id=SID, in_reply_to="orig-id"
        )
        ok, _, _ = validate_payload(payload)
        assert ok

    def test_handshake_frames_validate(self):
        request = handshake.build_request(
            channel_id=CH, session_id=SID, nonce=handshake.new_nonce()
        )
        ok, code, detail = validate_payload(request)
        assert ok, (code, detail)

        ack = handshake.build_ack(
            channel_id=CH,
            session_id=SID,
            nonce="n",
            in_reply_to="env-1",
        )
        assert validate_payload(ack)[0]

        established = handshake.build_established(
            channel_id=CH, session_id=SID, nonce="n", in_reply_to="env-2"
        )
        assert validate_payload(established)[0]

        reject = handshake.build_reject(
            channel_id=CH, session_id=SID, in_reply_to="env-1", reason="busy"
        )
        assert validate_payload(reject)[0]

        bye = handshake.build_bye(channel_id=CH, session_id=SID)
        assert validate_payload(bye)[0]


class TestValidationFailures:
    def test_unknown_type(self):
        ok, code, detail = validate_payload(_valid_text(type="message.evil"))
        assert not ok
        assert code == ERROR_CODE_UNKNOWN_TYPE
        assert "message.evil" in detail

    def test_non_dict_payload(self):
        ok, code, _ = validate_payload("just a string")
        assert not ok and code == ERROR_CODE_VALIDATION

    def test_missing_type(self):
        payload = _valid_text()
        del payload["type"]
        ok, code, _ = validate_payload(payload)
        assert not ok and code == ERROR_CODE_VALIDATION

    @pytest.mark.parametrize("field", ["channel_id", "session_id"])
    def test_missing_cross_cutting_field(self, field):
        payload = _valid_text(**{field: ""})
        ok, code, detail = validate_payload(payload)
        assert not ok and code == ERROR_CODE_VALIDATION
        assert field in detail

    def test_wrong_protocol_version(self):
        ok, code, detail = validate_payload(_valid_text(protocol_version=2))
        assert not ok and code == ERROR_CODE_VALIDATION
        assert "protocol_version" in detail

    def test_in_reply_to_must_be_string_or_null(self):
        ok, code, _ = validate_payload(_valid_text(in_reply_to=123))
        assert not ok and code == ERROR_CODE_VALIDATION
        assert validate_payload(_valid_text(in_reply_to=None))[0]

    def test_message_text_requires_body(self):
        payload = _valid_text()
        del payload["body"]
        ok, code, detail = validate_payload(payload)
        assert not ok and code == ERROR_CODE_VALIDATION
        assert "body" in detail

    def test_message_error_requires_error_code(self):
        payload = build_message_error(
            channel_id=CH, session_id=SID, error_code="x", in_reply_to=None
        )
        del payload["error_code"]
        ok, code, _ = validate_payload(payload)
        assert not ok and code == ERROR_CODE_VALIDATION

    def test_handshake_request_requires_nonce_and_capabilities(self):
        request = handshake.build_request(
            channel_id=CH, session_id=SID, nonce="n1"
        )
        del request["nonce"]
        ok, code, detail = validate_payload(request)
        assert not ok and "nonce" in detail

    def test_strict_raises_protocol_error(self):
        with pytest.raises(ProtocolError) as exc:
            validate_payload_strict(_valid_text(type="nope.nope"))
        assert exc.value.error_code == ERROR_CODE_UNKNOWN_TYPE
        # Valid payload passes silently.
        validate_payload_strict(_valid_text())

    def test_delegate_types_registered_but_flagged_unsupported(self):
        payload = _valid_text(type="delegate.task")
        del payload["body"]
        ok, _, _ = validate_payload(payload)
        assert ok  # schema-valid...
        assert "delegate.task" in protocol.UNSUPPORTED_TYPES  # ...but reserved
