"""Tests for OpenRouter variant tag preservation in model switching.

Regression test for GitHub PR #6088 / Discord report: OpenRouter model IDs
with variant suffixes like ``:free``, ``:extended``, ``:fast`` were being
mangled by the colon-to-slash conversion in model_switch.py Step c.

The fix: Step c now skips colon→slash conversion when the model name already
contains a forward slash (i.e. is already in ``vendor/model`` format), since
the colon is a variant tag, not a vendor separator.
"""
import pytest
from unittest.mock import patch

from hermes_cli.model_switch import parse_model_flags_detailed, switch_model


# Shared mock context — skip network calls, credential resolution, catalog lookups
_MOCK_VALIDATION = {"accepted": True, "persist": True, "recognized": True, "message": None}


def _run_switch(raw_input: str, current_provider: str = "openrouter") -> str:
    """Run switch_model with mocked dependencies, return the resolved model name."""
    with patch("hermes_cli.model_switch.resolve_alias", return_value=None), \
         patch("hermes_cli.model_switch.list_provider_models", return_value=[]), \
         patch("hermes_cli.runtime_provider.resolve_runtime_provider",
               return_value={"api_key": "test", "base_url": "", "api_mode": "chat_completions"}), \
         patch("hermes_cli.models.validate_requested_model", return_value=_MOCK_VALIDATION), \
         patch("hermes_cli.model_switch.get_model_info", return_value=None), \
         patch("hermes_cli.model_switch.get_model_capabilities", return_value=None), \
         patch("hermes_cli.models.detect_provider_for_model", return_value=None):
        result = switch_model(
            raw_input=raw_input,
            current_provider=current_provider,
            current_model="anthropic/claude-sonnet-4.6",
        )
        assert result.success, f"switch_model failed: {result.error_message}"
        return result.new_model


class TestVariantTagPreservation:
    """OpenRouter variant tags (:free, :extended, :fast) must survive model switching."""

    @pytest.mark.parametrize("model,expected", [
        ("nvidia/nemotron-3-super-120b-a12b:free", "nvidia/nemotron-3-super-120b-a12b:free"),
        ("anthropic/claude-sonnet-4.6:extended", "anthropic/claude-sonnet-4.6:extended"),
        ("meta-llama/llama-4-maverick:fast", "meta-llama/llama-4-maverick:fast"),
    ])
    def test_slash_format_preserves_variant_tag(self, model, expected):
        """Models already in vendor/model:tag format must not have their tag mangled."""
        assert _run_switch(model) == expected

    def test_legacy_colon_format_converts_to_slash(self):
        """Legacy vendor:model (no slash) should still be converted to vendor/model."""
        result = _run_switch("nvidia:nemotron-3-super-120b-a12b")
        assert result == "nvidia/nemotron-3-super-120b-a12b"

    def test_legacy_colon_format_with_tag_converts_first_colon_only(self):
        """vendor:model:free (no slash) → vendor/model:free — first colon becomes slash."""
        result = _run_switch("nvidia:nemotron-3-super-120b-a12b:free")
        assert result == "nvidia/nemotron-3-super-120b-a12b:free"

    def test_bare_model_name_unaffected(self):
        """Bare model names without colons or slashes should work normally."""
        result = _run_switch("claude-sonnet-4.6")
        assert result == "anthropic/claude-sonnet-4.6"

    def test_already_correct_slug_no_tag(self):
        """Standard vendor/model slugs without tags pass through unchanged."""
        result = _run_switch("anthropic/claude-sonnet-4.6")
        assert result == "anthropic/claude-sonnet-4.6"


class TestForceModelSwitch:
    """Forced switches bypass stale provider model listings only when requested."""

    def test_parse_force_flag_before_or_after_model(self):
        before = parse_model_flags_detailed("--force zai/glm-5.2")
        after = parse_model_flags_detailed("zai/glm-5.2 --force --global")

        assert before.model_input == "zai/glm-5.2"
        assert before.is_force is True
        assert after.model_input == "zai/glm-5.2"
        assert after.is_global is True
        assert after.is_force is True

    def test_force_bypasses_provider_listing_rejection(self):
        rejected = {
            "accepted": False,
            "persist": False,
            "recognized": False,
            "message": "Model `glm-5.2` was not found in this provider's model listing.",
        }

        with patch("hermes_cli.model_switch.resolve_alias", return_value=None), \
             patch("hermes_cli.model_switch.list_provider_models", return_value=[]), \
             patch("hermes_cli.runtime_provider.resolve_runtime_provider",
                   return_value={"api_key": "test", "base_url": "https://openrouter.ai/api/v1", "api_mode": "chat_completions"}), \
             patch("hermes_cli.models.validate_requested_model", return_value=rejected), \
             patch("hermes_cli.model_switch.get_model_info", return_value=None), \
             patch("hermes_cli.model_switch.get_model_capabilities", return_value=None), \
             patch("hermes_cli.models.detect_provider_for_model", return_value=None):
            # Without --force, the rejected validation propagates.
            result_no_force = switch_model(
                raw_input="unknown/some-hidden-model",
                current_provider="openrouter",
                current_model="anthropic/claude-sonnet-4.6",
                force=False,
            )
            assert not result_no_force.success
            assert "not found" in (result_no_force.warning_message or result_no_force.error_message or "")

            # With --force, validation is skipped and the model is accepted.
            result_force = switch_model(
                raw_input="unknown/some-hidden-model",
                current_provider="openrouter",
                current_model="anthropic/claude-sonnet-4.6",
                force=True,
            )
            assert result_force.success, f"forced switch should bypass validation: {result_force.error_message}"
            assert result_force.new_model == "unknown/some-hidden-model"
            assert "Forced model switch" in (result_force.warning_message or "")
