"""Offline tests for plugins.session_routing.allow (the broker-touching
parts exercised under test_integration.py; here we cover pure
functions: YAML parsing, union, dedup, stable sort).
"""

from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from plugins.session_routing.allow import (
    read_config_gateway_id,
    read_static_peers_from_yaml,
    union,
    _config_yaml_path,
)


class UnionTests(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(union(), [])
        self.assertEqual(union([], [], []), [])

    def test_dedup_preserves_first_appearance_order_until_sort(self):
        # union() returns sorted, so test that explicitly.
        result = union(["b", "a", "c"], ["a", "d"])
        self.assertEqual(result, ["a", "b", "c", "d"])

    def test_filters_blanks(self):
        result = union(["a", "", "b"], [None, "c"])  # type: ignore[list-item]
        self.assertEqual(result, ["a", "b", "c"])

    def test_stable_across_runs(self):
        a = union(["z", "a", "m"])
        b = union(["m", "z", "a"])
        self.assertEqual(a, b)


class YamlParsing(unittest.TestCase):
    def test_parses_block(self):
        text = textwrap.dedent("""
            # unrelated top-level
            some_other_thing: 42

            session_routing:
              peers:
                - gw-agent-vm
                - gw-other-bridge

            trailing:
              foo: bar
            """)
        self.assertEqual(
            read_static_peers_from_yaml(text),
            ["gw-agent-vm", "gw-other-bridge"],
        )

    def test_quoted_peers(self):
        text = textwrap.dedent("""
            session_routing:
              peers:
                - "gw-quoted"
                - 'gw-single'
            """)
        self.assertEqual(
            read_static_peers_from_yaml(text),
            ["gw-quoted", "gw-single"],
        )

    def test_block_absent(self):
        text = "unrelated:\n  foo: bar\n"
        self.assertEqual(read_static_peers_from_yaml(text), [])

    def test_peers_missing(self):
        text = "session_routing:\n  enabled: true\n"
        self.assertEqual(read_static_peers_from_yaml(text), [])

    def test_malformed_yaml_does_not_crash(self):
        # The regex is permissive — anything that doesn't match returns
        # empty rather than raising.
        text = "this is not yaml at all\n"
        self.assertEqual(read_static_peers_from_yaml(text), [])


class ConfigYamlPath(unittest.TestCase):
    def test_default_uses_hermes_home(self):
        with patch.dict(os.environ, {"HERMES_HOME": "/tmp/fake-hermes"}):
            self.assertEqual(_config_yaml_path(), Path("/tmp/fake-hermes/config.yaml"))

    def test_default_when_hermes_home_unset(self):
        env = os.environ.copy()
        env.pop("HERMES_HOME", None)
        with patch.dict(os.environ, env, clear=True):
            # The path should at least end with /config.yaml
            self.assertTrue(str(_config_yaml_path()).endswith(".hermes/config.yaml"))


class ReadConfigGatewayId(unittest.TestCase):
    """Tests for the ``session_routing.gateway_id`` config-file override."""

    def test_parses_unquoted(self):
        text = textwrap.dedent("""
            session_routing:
              gateway_id: gw-thinkpad
              peers:
                - gw-agent-vm
            """)
        self.assertEqual(read_config_gateway_id(config_yaml_text=text), "gw-thinkpad")

    def test_parses_double_quoted(self):
        text = textwrap.dedent("""
            session_routing:
              gateway_id: "gw-stable"
            """)
        self.assertEqual(read_config_gateway_id(config_yaml_text=text), "gw-stable")

    def test_parses_single_quoted(self):
        text = textwrap.dedent("""
            session_routing:
              gateway_id: 'gw-stable'
            """)
        self.assertEqual(read_config_gateway_id(config_yaml_text=text), "gw-stable")

    def test_parses_with_dashes_and_dots(self):
        text = textwrap.dedent("""
            session_routing:
              gateway_id: gw-thinkpad-v2.lab.local
            """)
        self.assertEqual(
            read_config_gateway_id(config_yaml_text=text),
            "gw-thinkpad-v2.lab.local",
        )

    def test_returns_none_when_block_absent(self):
        text = "unrelated:\n  foo: bar\n"
        self.assertIsNone(read_config_gateway_id(config_yaml_text=text))

    def test_returns_none_when_key_absent(self):
        # session_routing exists but gateway_id is not declared — peers-only.
        text = textwrap.dedent("""
            session_routing:
              peers:
                - gw-agent-vm
            """)
        self.assertIsNone(read_config_gateway_id(config_yaml_text=text))

    def test_returns_none_for_unsupported_characters(self):
        # The regex constrains to [A-Za-z0-9._-]; slashes/spaces should
        # not match (and would be rejected by AddressError anyway).
        text = textwrap.dedent("""
            session_routing:
              gateway_id: gw/with-slash
            """)
        self.assertIsNone(read_config_gateway_id(config_yaml_text=text))

    def test_does_not_pick_up_other_keys(self):
        # Only `session_routing.gateway_id` counts — a sibling key like
        # `agent.gateway_id` or a top-level `gateway_id` is ignored.
        text = textwrap.dedent("""
            gateway_id: gw-top-level
            agent:
              gateway_id: gw-agent-nested
            session_routing:
              peers:
                - gw-agent-vm
            """)
        self.assertIsNone(read_config_gateway_id(config_yaml_text=text))

    def test_does_not_crash_on_missing_file(self):
        # When HERMES_HOME points to a non-existent dir, return None —
        # don't raise.
        with patch.dict(os.environ, {"HERMES_HOME": "/tmp/does-not-exist-xyz-12345"}):
            self.assertIsNone(read_config_gateway_id())


if __name__ == "__main__":
    unittest.main()
