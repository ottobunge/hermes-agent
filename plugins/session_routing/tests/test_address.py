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
    subject_allow_pattern,
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
        addr = "gw-thinkpad/agent:main:telegram:dm:189562939:39702"
        subj = encode_subject(addr, verb="deliver")
        self.assertEqual(subj, "from.gw-thinkpad.agent:main:telegram:dm:189562939:39702.deliver")

    def test_decode_roundtrip(self):
        addr = "gw-thinkpad/agent:main:telegram:dm:189562939:39702"
        subj = encode_subject(addr, verb="deliver")
        gw, sk = decode_subject(subj)
        self.assertEqual((gw, sk), parse(addr))

    def test_decode_rejects_non_deliver(self):
        for bad in [
            "system.foo",
            "from.gw-thinkpad.system.foo",   # verb != deliver
            "from.gw-thinkpad.deliver",      # missing session_key segment
            "prefix.from.gw-thinkpad.x.deliver",  # leading segment
            "",
        ]:
            with self.subTest(bad=bad):
                with self.assertRaises(AddressError):
                    decode_subject(bad)

    def test_verb_rejects_dot(self):
        with self.assertRaises(AddressError):
            encode_subject("gw/a", verb="two.parts")

    def test_forbidden_chars_rejected(self):
        for bad in [
            "gw-think*pad/x",          # * in gateway_id
            "gw-thinkpad/x*y",         # * in session_key
            "gw-thinkpad/x>",          # > in session_key
            ">gw-thinkpad/x",          # > in gateway_id
        ]:
            with self.subTest(bad=bad):
                with self.assertRaises(AddressError):
                    parse(bad)
        # Same via build()
        with self.assertRaises(AddressError):
            build("gw*", "session")
        with self.assertRaises(AddressError):
            build("gw", "session>with-arrow")

    def test_allow_pattern(self):
        self.assertEqual(subject_allow_pattern("gw-thinkpad"), "from.gw-thinkpad.>")


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
