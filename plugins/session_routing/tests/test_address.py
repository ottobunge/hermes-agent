"""Tests for plugins.session_routing.address — offline, no broker needed."""

from __future__ import annotations

import os
import unittest

from plugins.session_routing.address import (
    AddressError,
    build,
    decode_subject,
    encode_subject,
    is_address,
    my_address,
    parse,
    resolve_gateway_id,
    resolve_session_key,
    subject_filter_for,
)


class ParseAndBuild(unittest.TestCase):
    def test_roundtrip_simple(self):
        s = "gw-thinkpad/agent:main:telegram:dm:189562939:39702"
        gw, sk = parse(s)
        self.assertEqual(gw, "gw-thinkpad")
        self.assertEqual(sk, "agent:main:telegram:dm:189562939:39702")
        self.assertEqual(build(gw, sk), s)

    def test_roundtrip_with_trailing_newline(self):
        s = "gw-agent-vm/agent:main:darwin:nix-darwin:otto@mbp:0\n"
        gw, sk = parse(s)
        self.assertEqual((gw, sk), ("gw-agent-vm", "agent:main:darwin:nix-darwin:otto@mbp:0"))

    def test_roundtrip_with_surrounding_whitespace(self):
        s = "  gw-thinkpad/agent:main:telegram:dm:189562939:39702  "
        self.assertEqual(parse(s), ("gw-thinkpad", "agent:main:telegram:dm:189562939:39702"))

    def test_is_address(self):
        self.assertTrue(is_address("gw-thinkpad/agent:main:telegram:dm:189562939:39702"))
        self.assertTrue(is_address("gw-x/y"))
        self.assertFalse(is_address(""))
        self.assertFalse(is_address("no-slash-here"))
        self.assertFalse(is_address("/leading-slash"))
        self.assertFalse(is_address("trailing-slash/"))

    def test_rejects_empty(self):
        for bad in ["", "  ", None]:
            with self.subTest(bad=bad):
                with self.assertRaises(AddressError):
                    parse(bad)  # type: ignore[arg-type]

    def test_rejects_no_slash(self):
        with self.assertRaises(AddressError):
            parse("gw-thinkpad-no-slash")

    def test_rejects_multiple_slashes_via_build(self):
        # build() rejects both gateway_id and session_key containing '/'
        with self.assertRaises(AddressError):
            build("gw/f", "session")
        with self.assertRaises(AddressError):
            build("gw", "session/with/slashes")
        with self.assertRaises(AddressError):
            build("", "session")

    def test_rejects_non_string(self):
        with self.assertRaises(AddressError):
            parse(123)  # type: ignore[arg-type]

    def test_session_key_with_at_sign(self):
        # The macOS session_key contains `otto@mbp` — make sure we don't break.
        s = "gw-agent-vm/agent:main:darwin:nix-darwin:otto@mbp:0"
        self.assertEqual(parse(s), ("gw-agent-vm", "agent:main:darwin:nix-darwin:otto@mbp:0"))


class EncodeSubject(unittest.TestCase):
    def test_encode_deliver(self):
        # New signature: encode_subject(sender_gw, session_key, verb)
        subj = encode_subject(
            sender_gateway_id="gw-sender",
            session_key="agent:main:telegram:dm:189562939:39702",
            verb="deliver",
        )
        self.assertEqual(
            subj,
            "from.gw-sender.agent:main:telegram:dm:189562939:39702.deliver",
        )

    def test_decode_roundtrip(self):
        sender_gw = "gw-sender"
        sk = "agent:main:telegram:dm:189562939:39702"
        subj = encode_subject(sender_gw, sk, verb="deliver")
        # decode_subject returns (sender_gw, recipient_session_key)
        out_sender, out_sk = decode_subject(subj)
        self.assertEqual(out_sender, sender_gw)
        self.assertEqual(out_sk, sk)

    def test_decode_rejects_non_deliver_or_malformed(self):
        for bad in [
            "system.foo",                 # not from.<gateway>
            "from.gw-thinkpad.deliver",   # missing session_key segment
            "prefix.from.gw-thinkpad.x.deliver",  # leading segment
            "",                           # empty
            "from..x.deliver",            # empty sender_gateway_id
            "from..deliver",              # only empty parts
        ]:
            with self.subTest(bad=bad):
                with self.assertRaises(AddressError):
                    decode_subject(bad)

    def test_verb_rejects_dot(self):
        with self.assertRaises(AddressError):
            encode_subject("gw-a", "session", verb="two.parts")

    def test_forbidden_chars_rejected(self):
        for bad in [
            "gw-think*pad",        # * in sender_gw
            "foo*bar",             # * in session_key
            "x>y",                 # > in session_key
            ">gw",                 # > in sender_gw
        ]:
            with self.subTest(bad=bad):
                with self.assertRaises(AddressError):
                    encode_subject(sender_gateway_id=bad.split("/")[0] if "/" in bad else bad[:5],
                                   session_key=bad,
                                   verb="deliver")
        # Same via build()
        with self.assertRaises(AddressError):
            build("gw*", "session")
        with self.assertRaises(AddressError):
            build("gw", "session>with-arrow")

    def test_subject_filter_for(self):
        self.assertEqual(subject_filter_for("gw-thinkpad"), "from.gw-thinkpad.>")


class Resolve(unittest.TestCase):
    def test_resolve_gateway_id_explicit(self):
        self.assertEqual(resolve_gateway_id(explicit="gw-custom"), "gw-custom")
        # Whitespace trimmed
        self.assertEqual(resolve_gateway_id(explicit="  gw-custom  "), "gw-custom")

    def test_resolve_gateway_id_from_env(self, monkeypatch=None):
        old = os.environ.get("HERMES_GATEWAY_ID")
        try:
            os.environ["HERMES_GATEWAY_ID"] = "gw-from-env"
            self.assertEqual(resolve_gateway_id(), "gw-from-env")
        finally:
            if old is None:
                os.environ.pop("HERMES_GATEWAY_ID", None)
            else:
                os.environ["HERMES_GATEWAY_ID"] = old

    def test_resolve_session_key_from_env(self):
        old = os.environ.get("HERMES_SESSION_KEY")
        try:
            os.environ["HERMES_SESSION_KEY"] = "agent:main:foo"
            self.assertEqual(resolve_session_key(), "agent:main:foo")
        finally:
            if old is None:
                os.environ.pop("HERMES_SESSION_KEY", None)
            else:
                os.environ["HERMES_SESSION_KEY"] = old

    def test_resolve_session_key_empty(self):
        old = os.environ.get("HERMES_SESSION_KEY")
        try:
            os.environ.pop("HERMES_SESSION_KEY", None)
            self.assertEqual(resolve_session_key(), "")
        finally:
            if old is not None:
                os.environ["HERMES_SESSION_KEY"] = old

    def test_resolve_session_key_explicit_wins(self):
        old = os.environ.get("HERMES_SESSION_KEY")
        try:
            os.environ["HERMES_SESSION_KEY"] = "from-env"
            self.assertEqual(resolve_session_key(explicit="from-arg"), "from-arg")
        finally:
            if old is None:
                os.environ.pop("HERMES_SESSION_KEY", None)
            else:
                os.environ["HERMES_SESSION_KEY"] = old

    def test_my_address_both_supplied(self):
        self.assertEqual(
            my_address(gateway_id="gw-thinkpad",
                       session_key="agent:main:telegram:dm:189562939:39702"),
            "gw-thinkpad/agent:main:telegram:dm:189562939:39702",
        )

    def test_my_address_raises_when_session_missing(self):
        old = os.environ.get("HERMES_SESSION_KEY")
        try:
            os.environ.pop("HERMES_SESSION_KEY", None)
            with self.assertRaises(AddressError):
                my_address(gateway_id="gw-thinkpad")
        finally:
            if old is not None:
                os.environ["HERMES_SESSION_KEY"] = old


if __name__ == "__main__":
    unittest.main()
