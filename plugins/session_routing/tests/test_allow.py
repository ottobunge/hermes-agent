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


if __name__ == "__main__":
    unittest.main()
