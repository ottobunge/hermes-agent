"""Offline unit tests for the handshake state machine + frame builders."""

import pytest

from plugins.session_bus.handshake import (
    DEFAULT_CAPABILITIES,
    HandshakeChannel,
    HandshakeError,
    HandshakeState,
    PROTOCOL_VERSION,
    build_bye,
    build_reject,
    build_request,
    new_nonce,
    resolve_race,
)

CH_A = "20260706_174653_5ed43bcb:a1b2c3d4"
CH_B = "20260706_174909_fadd9fd3:0badcafe"
SID_A = "20260706_174653_5ed43bcb"
SID_B = "20260706_174909_fadd9fd3"
ADDR_A = "gw-thinkpad/agent:main:telegram:dm:1:1"
ADDR_B = "gw-agent-vm/agent:main:telegram:dm:2:2"


def _mk(role="initiator", **kwargs):
    defaults = dict(
        channel_id=CH_A if role == "initiator" else CH_B,
        session_id=SID_A if role == "initiator" else SID_B,
        my_address=ADDR_A if role == "initiator" else ADDR_B,
        peer_address=ADDR_B if role == "initiator" else ADDR_A,
    )
    defaults.update(kwargs)
    return HandshakeChannel(**defaults)


class TestFrameBuilders:
    def test_request_carries_cross_cutting_fields(self):
        frame = build_request(channel_id=CH_A, session_id=SID_A, nonce="n1")
        assert frame["type"] == "handshake.request"
        assert frame["channel_id"] == CH_A
        assert frame["session_id"] == SID_A
        assert frame["in_reply_to"] is None
        assert frame["protocol_version"] == PROTOCOL_VERSION
        assert frame["nonce"] == "n1"
        assert frame["capabilities"] == DEFAULT_CAPABILITIES

    def test_missing_channel_id_raises(self):
        with pytest.raises(HandshakeError):
            build_request(channel_id="", session_id=SID_A, nonce="n1")

    def test_missing_session_id_raises(self):
        with pytest.raises(HandshakeError):
            build_bye(channel_id=CH_A, session_id="")

    def test_reject_carries_reason_and_reply(self):
        frame = build_reject(
            channel_id=CH_B, session_id=SID_B, in_reply_to="m-1", reason="busy"
        )
        assert frame["reason"] == "busy"
        assert frame["in_reply_to"] == "m-1"


class TestHappyPath:
    def test_request_ack_established_round_trip(self):
        initiator = _mk("initiator")
        responder = _mk("responder")

        request = initiator.start(capabilities=["text"])
        assert initiator.state == HandshakeState.REQUEST_SENT
        assert request["capabilities"] == ["text"]

        ack = responder.on_request(
            request, request_envelope_msg_id="env-req-1", capabilities=["text"]
        )
        assert responder.state == HandshakeState.ACK_SENT
        assert ack["nonce"] == request["nonce"]
        assert ack["in_reply_to"] == "env-req-1"
        assert responder.peer_capabilities == ["text"]

        initiator.request_msg_id = "env-req-1"
        established = initiator.on_ack(ack)
        assert initiator.state == HandshakeState.ESTABLISHED
        assert initiator.peer_capabilities == ["text"]
        assert established["type"] == "handshake.established"
        assert established["nonce"] == request["nonce"]

        responder.on_established(established)
        assert responder.state == HandshakeState.ESTABLISHED

    def test_close_established_returns_bye_and_peer_converges(self):
        initiator, responder = self._established_pair()
        bye = initiator.close(reason="session ended")
        assert bye is not None and bye["type"] == "handshake.bye"
        assert initiator.state == HandshakeState.CLOSED

        responder.on_bye(bye)
        assert responder.state == HandshakeState.CLOSED

    def test_concurrent_close_is_idempotent(self):
        initiator, responder = self._established_pair()
        bye_a = initiator.close()
        bye_b = responder.close()
        # Both sides went CLOSED locally; each receives the peer's bye.
        initiator.on_bye(bye_b)
        responder.on_bye(bye_a)
        assert initiator.state == HandshakeState.CLOSED
        assert responder.state == HandshakeState.CLOSED
        # Double-close is a no-op, not an error.
        assert initiator.close() is None

    @staticmethod
    def _established_pair():
        initiator = _mk("initiator")
        responder = _mk("responder")
        request = initiator.start()
        ack = responder.on_request(request, request_envelope_msg_id="env-1")
        initiator.request_msg_id = "env-1"
        established = initiator.on_ack(ack)
        responder.on_established(established)
        return initiator, responder


class TestValidationFailures:
    def test_bad_ack_nonce_rejected_and_state_unchanged(self):
        initiator = _mk("initiator")
        request = initiator.start()
        forged = {
            "type": "handshake.ack",
            "channel_id": CH_B,
            "session_id": SID_B,
            "nonce": "not-the-nonce",
            "in_reply_to": None,
            "protocol_version": 1,
        }
        with pytest.raises(HandshakeError, match="nonce mismatch"):
            initiator.on_ack(forged)
        assert initiator.state == HandshakeState.REQUEST_SENT

    def test_ack_reply_correlation_enforced_when_known(self):
        initiator = _mk("initiator")
        request = initiator.start()
        initiator.request_msg_id = "env-real"
        stale_ack = {
            "type": "handshake.ack",
            "nonce": request["nonce"],
            "in_reply_to": "env-stale",
        }
        with pytest.raises(HandshakeError, match="in_reply_to mismatch"):
            initiator.on_ack(stale_ack)

    def test_request_without_nonce_rejected(self):
        responder = _mk("responder")
        with pytest.raises(HandshakeError, match="no nonce"):
            responder.on_request(
                {"type": "handshake.request"},
                request_envelope_msg_id="env-1",
            )

    def test_established_with_wrong_nonce_rejected(self):
        initiator = _mk("initiator")
        responder = _mk("responder")
        request = initiator.start()
        responder.on_request(request, request_envelope_msg_id="env-1")
        with pytest.raises(HandshakeError, match="nonce mismatch"):
            responder.on_established(
                {"type": "handshake.established", "nonce": "wrong"}
            )
        assert responder.state == HandshakeState.ACK_SENT

    def test_wrong_frame_type_raises(self):
        initiator = _mk("initiator")
        initiator.start()
        with pytest.raises(HandshakeError, match="expected handshake.ack"):
            initiator.on_ack({"type": "message.text"})

    def test_illegal_transition_raises(self):
        initiator = _mk("initiator")
        with pytest.raises(HandshakeError, match="illegal in state"):
            initiator.on_ack({"type": "handshake.ack", "nonce": "x"})
        with pytest.raises(HandshakeError, match="illegal in state"):
            initiator.start() and initiator.start()

    def test_reject_terminal(self):
        initiator = _mk("initiator")
        request = initiator.start()
        initiator.on_reject(
            build_reject(
                channel_id=CH_B,
                session_id=SID_B,
                in_reply_to="env-1",
                reason="channel_busy",
            )
        )
        assert initiator.state == HandshakeState.REJECTED
        assert initiator.reject_reason == "channel_busy"
        # Terminal: close() is a no-op, no bye escapes.
        assert initiator.close() is None
        assert initiator.state == HandshakeState.REJECTED


class TestTimeout:
    def test_pending_request_expires(self):
        initiator = _mk("initiator", timeout_seconds=30.0)
        initiator.start(now=1000.0)
        assert initiator.is_expired(now=1010.0) is False
        assert initiator.is_expired(now=1031.0) is True

    def test_settled_states_never_expire(self):
        initiator = _mk("initiator", timeout_seconds=0.0)
        assert initiator.is_expired(now=99999.0) is False  # IDLE
        request = initiator.start(now=0.0)
        responder = _mk("responder", timeout_seconds=30.0)
        ack = responder.on_request(
            request, request_envelope_msg_id="env-1", now=0.0
        )
        initiator.on_ack(ack)
        assert initiator.state == HandshakeState.ESTABLISHED
        assert initiator.is_expired(now=99999.0) is False


class TestRaceDeterminism:
    def test_exactly_one_side_wins(self):
        a = resolve_race(ADDR_A, ADDR_B)
        b = resolve_race(ADDR_B, ADDR_A)
        assert {a, b} == {"mine", "theirs"}

    def test_smaller_address_wins(self):
        assert resolve_race("gw-a/s1", "gw-b/s2") == "mine"
        assert resolve_race("gw-b/s2", "gw-a/s1") == "theirs"

    def test_identical_addresses_raise(self):
        with pytest.raises(HandshakeError):
            resolve_race(ADDR_A, ADDR_A)


class TestNonce:
    def test_nonces_are_unique_uuid4(self):
        seen = {new_nonce() for _ in range(100)}
        assert len(seen) == 100
        assert all(len(n) == 36 for n in seen)
