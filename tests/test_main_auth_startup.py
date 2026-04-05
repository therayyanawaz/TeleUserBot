from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

import main


class _MissingAuthManager:
    def __init__(self, *args, **kwargs):
        pass

    async def get_access_token(self):
        raise main.OAuthError(
            "Missing OAuth token in env. Set TG_USERBOT_AUTH_JSON or TG_USERBOT_AUTH_JSON_B64."
        )


class _ReadyAuthManager:
    def __init__(self, *args, **kwargs):
        pass

    async def get_access_token(self):
        return "token"


class _EnvBootstrapAuthManager:
    def __init__(self, *args, **kwargs):
        pass

    async def get_access_token(self):
        if str(os.getenv(main.ENV_AUTH_JSON_B64, "") or "").strip() == "fresh-b64":
            return "token"
        raise main.OAuthError(
            "Missing OAuth token in env. Set TG_USERBOT_AUTH_JSON or TG_USERBOT_AUTH_JSON_B64."
        )


class _StaleEnvAuthManager:
    def __init__(self, *args, **kwargs):
        pass

    async def get_access_token(self):
        if str(os.getenv(main.ENV_AUTH_JSON_B64, "") or "").strip() == "fresh-b64":
            return "token"
        raise main.OAuthError(
            "Stored env refresh token is stale because it was already rotated. "
            "Run `python auth.py login --env-file .env` and update your deployed secrets."
        )


def _reset_auth_state() -> None:
    main.auth_manager = None
    main.auth_startup_mode_configured = "auto"
    main.auth_startup_mode_effective = "auto"
    main.auth_ready = False
    main.auth_degraded = False
    main.auth_failure_reason = ""
    main.auth_features_disabled = []
    main.startup_auth_repair_status = "not_needed"
    main.startup_auth_repair_message = ""
    main._clear_digest_recovery_state()
    main._last_cli_status_signature = None
    main._last_cli_status_at = 0.0


@pytest.fixture(autouse=True)
def _reset_runtime_state(monkeypatch):
    _reset_auth_state()
    monkeypatch.setattr(main, "_print_cli_status", lambda *args, **kwargs: None)
    monkeypatch.delenv(main.ENV_AUTH_ENV_ONLY, raising=False)
    monkeypatch.delenv(main.ENV_AUTH_JSON, raising=False)
    monkeypatch.delenv(main.ENV_AUTH_JSON_B64, raising=False)
    yield
    _reset_auth_state()


@pytest.mark.asyncio
async def test_prepare_auth_runtime_strict_missing_auth_raises(monkeypatch):
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "strict", raising=False)
    monkeypatch.setattr(main, "AuthManager", _MissingAuthManager)

    with pytest.raises(RuntimeError) as exc:
        await main._prepare_auth_runtime()

    message = str(exc.value)
    assert "Missing OAuth token in env." in message
    assert "python auth.py bootstrap-env" in message
    assert main.auth_ready is False
    assert main.auth_degraded is False
    assert main.auth_startup_mode_effective == "strict"


@pytest.mark.asyncio
async def test_prepare_auth_runtime_degraded_missing_auth_continues(monkeypatch):
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "degraded", raising=False)
    monkeypatch.setattr(main, "AuthManager", _MissingAuthManager)

    await main._prepare_auth_runtime()

    assert main.auth_ready is False
    assert main.auth_degraded is True
    assert main.auth_startup_mode_effective == "degraded"
    assert "Missing OAuth token in env." in main.auth_failure_reason
    assert "query_mode" in main.auth_features_disabled
    assert "ocr_translation" in main.auth_features_disabled


@pytest.mark.asyncio
async def test_prepare_auth_runtime_auto_uses_interactive_policy(monkeypatch):
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "auto", raising=False)
    monkeypatch.setattr(main, "AuthManager", _ReadyAuthManager)

    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: True)
    await main._prepare_auth_runtime()
    assert main.auth_startup_mode_effective == "strict"
    assert main.auth_ready is True
    assert main.auth_degraded is False

    _reset_auth_state()
    monkeypatch.setattr(main, "AuthManager", _MissingAuthManager)
    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: False)
    await main._prepare_auth_runtime()
    assert main.auth_startup_mode_effective == "degraded"
    assert main.auth_degraded is True


def test_refresh_runtime_from_env_mutation_updates_env_and_resets_runtime(monkeypatch):
    reload_calls: list[str] = []

    def fake_reload(module):
        reload_calls.append(module.__name__)
        return module

    main.destination_peer = object()
    main.bot_destination_token = "token"
    main.bot_destination_chat_id = "chat"
    main.query_allowed_bot_user_id = 123
    main.query_bot_user_id_checked = True

    monkeypatch.setattr(main.importlib, "reload", fake_reload)

    main._refresh_runtime_from_env_mutation(
        {
            main.ENV_AUTH_ENV_ONLY: "true",
            main.ENV_AUTH_JSON: "",
            main.ENV_AUTH_JSON_B64: "fresh-b64",
        }
    )

    assert os.environ[main.ENV_AUTH_ENV_ONLY] == "true"
    assert os.environ[main.ENV_AUTH_JSON] == ""
    assert os.environ[main.ENV_AUTH_JSON_B64] == "fresh-b64"
    assert reload_calls == ["config"]
    assert main.destination_peer is None
    assert main.bot_destination_token is None
    assert main.bot_destination_chat_id is None
    assert main.query_allowed_bot_user_id is None
    assert main.query_bot_user_id_checked is False


@pytest.mark.asyncio
async def test_prepare_auth_runtime_for_startup_bootstraps_missing_env_auth(monkeypatch):
    bootstrap_calls = {"count": 0}

    async def fake_bootstrap_env_oauth_payload(logger=None):
        bootstrap_calls["count"] += 1
        return {
            "json": '{"access_token":"fresh"}',
            "b64": "fresh-b64",
            "account_id": "acct-123",
            "expires_at": "9999999999",
        }

    def fake_write_auth_payload_to_env_file(payload, env_path):
        return env_path

    def fake_refresh_runtime_from_env_mutation(updates):
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: True)
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "auto", raising=False)
    monkeypatch.setattr(main, "AuthManager", _EnvBootstrapAuthManager)
    monkeypatch.setattr(main, "bootstrap_env_oauth_payload", fake_bootstrap_env_oauth_payload)
    monkeypatch.setattr(main, "write_auth_payload_to_env_file", fake_write_auth_payload_to_env_file)
    monkeypatch.setattr(main, "_refresh_runtime_from_env_mutation", fake_refresh_runtime_from_env_mutation)

    await main._prepare_auth_runtime_for_startup()

    assert bootstrap_calls["count"] == 1
    assert os.environ[main.ENV_AUTH_ENV_ONLY] == "true"
    assert os.environ[main.ENV_AUTH_JSON_B64] == "fresh-b64"
    assert main.auth_ready is True
    assert main.auth_degraded is False
    assert main.startup_auth_repair_status == "repaired"
    assert "Saved OpenAI env auth" in main.startup_auth_repair_message


@pytest.mark.asyncio
async def test_prepare_auth_runtime_for_startup_repairs_stale_env_auth(monkeypatch):
    bootstrap_calls = {"count": 0}

    async def fake_bootstrap_env_oauth_payload(logger=None):
        bootstrap_calls["count"] += 1
        return {
            "json": '{"access_token":"fresh"}',
            "b64": "fresh-b64",
            "account_id": "acct-123",
            "expires_at": "9999999999",
        }

    def fake_write_auth_payload_to_env_file(payload, env_path):
        return env_path

    def fake_refresh_runtime_from_env_mutation(updates):
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    monkeypatch.setenv(main.ENV_AUTH_ENV_ONLY, "true")
    monkeypatch.setenv(main.ENV_AUTH_JSON_B64, "stale-b64")
    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: True)
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "auto", raising=False)
    monkeypatch.setattr(main, "AuthManager", _StaleEnvAuthManager)
    monkeypatch.setattr(main, "bootstrap_env_oauth_payload", fake_bootstrap_env_oauth_payload)
    monkeypatch.setattr(main, "write_auth_payload_to_env_file", fake_write_auth_payload_to_env_file)
    monkeypatch.setattr(main, "_refresh_runtime_from_env_mutation", fake_refresh_runtime_from_env_mutation)

    await main._prepare_auth_runtime_for_startup()

    assert bootstrap_calls["count"] == 1
    assert os.environ[main.ENV_AUTH_JSON_B64] == "fresh-b64"
    assert main.auth_ready is True
    assert main.startup_auth_repair_status == "repaired"


@pytest.mark.asyncio
async def test_prepare_auth_runtime_for_startup_fails_when_guided_bootstrap_fails(monkeypatch):
    async def fake_bootstrap_env_oauth_payload(logger=None):
        raise main.OAuthError("OAuth callback timed out.")

    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: True)
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "auto", raising=False)
    monkeypatch.setattr(main, "AuthManager", _EnvBootstrapAuthManager)
    monkeypatch.setattr(main, "bootstrap_env_oauth_payload", fake_bootstrap_env_oauth_payload)

    with pytest.raises(RuntimeError) as exc:
        await main._prepare_auth_runtime_for_startup()

    assert "guided auth bootstrap failed" in str(exc.value).lower()
    assert main.startup_auth_repair_status == "failed"


@pytest.mark.asyncio
async def test_prepare_auth_runtime_for_startup_preserves_noninteractive_behavior(monkeypatch):
    monkeypatch.setattr(main, "_is_interactive_runtime", lambda: False)
    monkeypatch.setattr(main.config, "OPENAI_AUTH_STARTUP_MODE", "strict", raising=False)
    monkeypatch.setattr(main, "AuthManager", _MissingAuthManager)

    with pytest.raises(RuntimeError):
        await main._prepare_auth_runtime_for_startup()

    assert main.startup_auth_repair_status == "skipped"


@pytest.mark.asyncio
async def test_handle_query_request_replies_with_degraded_message(monkeypatch):
    replies: list[str] = []

    async def fake_safe_reply_markdown(event_ref, text, **kwargs):
        replies.append(text)
        return object()

    main.auth_ready = False
    main.auth_degraded = True
    main.auth_failure_reason = "Missing OAuth token in env."
    monkeypatch.setattr(main, "_is_query_mode_enabled", lambda: True)
    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)

    await main._handle_query_request(
        event_ref=object(),
        text="What happened today?",
        sender_id=1,
        chat_id="chat",
        reply_to=None,
        prefer_bot_identity=False,
        bot_chat_id=None,
    )

    assert replies
    assert "temporarily unavailable" in replies[0].lower()
    assert "bootstrap-env" in replies[0]


@pytest.mark.asyncio
async def test_status_payload_and_digest_status_include_auth_health(monkeypatch):
    class _FakeEvent:
        def __init__(self):
            self.messages: list[str] = []

        async def reply(self, text, **kwargs):
            self.messages.append(text)

    main.startup_ready = True
    main.startup_error = ""
    main.startup_phase = "telegram_login"
    main.started_as_username = "tester"
    main.auth_startup_mode_configured = "auto"
    main.auth_startup_mode_effective = "degraded"
    main.auth_ready = False
    main.auth_degraded = True
    main.auth_failure_reason = "Missing OAuth token in env."
    main.auth_features_disabled = ["query_mode", "ocr_translation"]

    monkeypatch.setattr(main, "count_pending", lambda: 0)
    monkeypatch.setattr(main, "count_inflight", lambda: 0)
    monkeypatch.setattr(main, "get_last_digest_timestamp", lambda: None)
    monkeypatch.setattr(main, "load_inbound_job_counts", lambda: {"triage": 1})
    monkeypatch.setattr(main, "oldest_pending_inbound_job_age_seconds", lambda: 7.0)
    monkeypatch.setattr(main, "get_filter_decision_cache_stats", lambda: {"hits": 2.0, "misses": 1.0, "hit_rate": 0.67})
    monkeypatch.setattr(main, "count_ai_decision_cache_entries", lambda: 3)
    monkeypatch.setattr(main, "load_recent_inbound_job_failures", lambda limit=5: [])
    monkeypatch.setattr(main, "_pipeline_worker_targets", lambda: {"triage": 4, "ai": 2})
    monkeypatch.setattr(main, "_pipeline_stage_latency_snapshot", lambda: {"triage": 12.0})
    monkeypatch.setattr(main, "_is_digest_mode_enabled", lambda: True)
    monkeypatch.setattr(main, "_is_dupe_detection_enabled", lambda: True)
    monkeypatch.setattr(main, "_is_severity_routing_enabled", lambda: True)
    monkeypatch.setattr(main, "_is_query_mode_enabled", lambda: True)
    monkeypatch.setattr(main, "_is_query_runtime_available", lambda: False)
    monkeypatch.setattr(main, "_is_html_formatting_enabled", lambda: False)
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 3600)
    monkeypatch.setattr(main, "_digest_daily_times", lambda: [])
    monkeypatch.setattr(main, "_digest_queue_clear_interval_seconds", lambda: 0)
    monkeypatch.setattr(main, "_digest_queue_clear_scope", lambda: "disabled")
    monkeypatch.setattr(main, "_digest_daily_window_hours", lambda: 24)
    monkeypatch.setattr(main, "_digest_429_threshold_per_hour", lambda: 3)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: None)
    monkeypatch.setattr(main, "get_quota_health", lambda: {"status": "healthy", "recent_429_count": 0})
    monkeypatch.setattr(main, "dupe_detector", None)

    payload = main._web_status_payload()
    assert payload["query_mode_available"] is False
    assert payload["digest_recovery"]["state"] == "normal"
    assert payload["auth"]["mode_effective"] == "degraded"
    assert payload["auth"]["ready"] is False
    assert payload["auth"]["degraded"] is True
    assert payload["auth"]["features_disabled"] == ["query_mode", "ocr_translation"]
    assert payload["auth"]["startup_repair_status"] == "not_needed"

    event = _FakeEvent()
    await main._on_digest_status_command(event)
    assert event.messages
    assert "Auth Status" in event.messages[0]
    assert "degraded" in event.messages[0]
    assert "query_mode, ocr_translation" in event.messages[0]
    assert "Startup repair" in event.messages[0]
    assert "Recovery mode" in event.messages[0]


def test_pull_latest_repo_version_on_startup_detects_no_change(monkeypatch):
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="Already up to date.\n",
            stderr="",
        ),
    )

    assert main._pull_latest_repo_version_on_startup() is False


def test_pull_latest_repo_version_on_startup_detects_update(monkeypatch):
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="Updating 123..456 Fast-forward\n",
            stderr="",
        ),
    )

    assert main._pull_latest_repo_version_on_startup() is True
