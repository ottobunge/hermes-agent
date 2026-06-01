"""Regression tests for Mattermost approval prompt thread routing."""

import sys
import threading
import types
from collections import OrderedDict
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import SendResult
from gateway.session import SessionSource


class _ApprovalAgent:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.tools: list[Any] = []

    def run_conversation(
        self,
        user_message: Any,
        conversation_history: Optional[list[dict[str, Any]]] = None,
        task_id: Optional[str] = None,
        persist_user_message: Optional[str] = None,
    ) -> dict[str, Any]:
        from tools.approval import check_all_command_guards

        _ = (user_message, conversation_history, task_id, persist_user_message)
        _ = check_all_command_guards("rm -rf /tmp/hermes-routing-test", "local")
        return {
            "final_response": "done",
            "messages": [],
            "api_calls": 1,
            "completed": True,
        }


class _MattermostAdapter:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self._pending_messages: dict[str, Any] = {}

    def pause_typing_for_chat(self, chat_id: str) -> None:
        _ = chat_id

    def resume_typing_for_chat(self, chat_id: str) -> None:
        pass

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        self.sent.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": metadata,
            }
        )
        return SendResult(success=True, message_id="approval-post")

    async def edit_message(self, chat_id: str, message_id: str, content: str) -> SendResult:
        _ = (chat_id, message_id, content)
        return SendResult(success=False)

    def has_pending_interrupt(self, session_key: str) -> bool:
        _ = session_key
        return False

    def get_pending_message(self, session_key: str) -> None:
        _ = session_key
        return None


def _install_fake_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_run_agent = types.ModuleType("run_agent")
    setattr(fake_run_agent, "AIAgent", _ApprovalAgent)
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)


def _make_runner(adapter: _MattermostAdapter) -> gateway_run.GatewayRunner:
    runner = object.__new__(gateway_run.GatewayRunner)
    setattr(runner, "adapters", {Platform.MATTERMOST: adapter})
    setattr(runner, "_ephemeral_system_prompt", "")
    setattr(runner, "_prefill_messages", [])
    setattr(runner, "_reasoning_config", None)
    setattr(runner, "_service_tier", None)
    setattr(runner, "_provider_routing", {})
    setattr(runner, "_fallback_model", None)
    setattr(runner, "_running_agents", {})
    setattr(runner, "_pending_model_notes", {})
    setattr(runner, "_session_db", None)
    setattr(runner, "_agent_cache", OrderedDict())
    setattr(runner, "_agent_cache_lock", threading.Lock())
    setattr(runner, "_session_model_overrides", {})
    setattr(runner, "_pending_skills_reload_notes", {})
    setattr(runner, "_draining", False)
    setattr(runner, "hooks", SimpleNamespace(loaded_hooks=False, emit=AsyncMock()))
    setattr(runner, "config", SimpleNamespace(streaming=None))
    setattr(runner, "session_store", SimpleNamespace(_entries={}))
    setattr(runner, "_get_or_create_gateway_honcho", lambda session_key: (None, None))
    setattr(runner, "_is_session_run_current", lambda session_key, generation: True)
    setattr(runner, "_consume_pending_native_image_paths", lambda session_key: [])
    setattr(runner, "_update_runtime_status", lambda gateway_state=None, exit_reason=None: None)
    return runner


def _mattermost_source(thread_id: str) -> SessionSource:
    return SessionSource(
        platform=Platform.MATTERMOST,
        chat_id="channel-1",
        chat_type="channel",
        user_id="user-1",
        thread_id=thread_id,
    )


@pytest.mark.asyncio
async def test_mattermost_approval_prompt_uses_triggering_post_reply_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Approval prompts must stay in the originating Mattermost thread."""

    _install_fake_agent(monkeypatch)
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_AGENT_TIMEOUT", "0")
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_load_gateway_runtime_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "test-model")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {"provider": "test", "api_key": "***"},
    )

    import hermes_cli.tools_config as tools_config
    import tools.approval as approval_mod

    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda user_config, platform_key: {"core"})
    monkeypatch.setattr(approval_mod, "_get_approval_config", lambda: {"mode": "manual", "gateway_timeout": 0})

    adapter = _MattermostAdapter()
    runner = _make_runner(adapter)
    source = _mattermost_source(thread_id="root-post-1")

    result = await runner._run_agent(
        message="please run cleanup",
        context_prompt="",
        history=[],
        source=source,
        session_id="session-1",
        session_key="agent:main:mattermost:channel:channel-1:root-post-1",
        event_message_id="reply-post-9",
    )

    assert result["final_response"] == "done"
    assert adapter.sent
    approval_prompt = adapter.sent[0]
    assert "Dangerous command requires approval" in approval_prompt["content"]
    assert approval_prompt["chat_id"] == "channel-1"
    assert approval_prompt["reply_to"] == "reply-post-9"
    assert approval_prompt["metadata"] == {"thread_id": "root-post-1"}
