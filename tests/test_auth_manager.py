from __future__ import annotations

import base64
import json

import httpx
import pytest

import auth


def _fake_access_token(account_id: str = "acct-123") -> str:
    payload = {
        auth.JWT_CLAIM_PATH: {
            "chatgpt_account_id": account_id,
        }
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).decode("utf-8").rstrip("=")
    return f"header.{encoded}.signature"


def _token_payload(*, access_token: str | None = None, refresh_token: str = "refresh-1", expires_at: int = 1_900_000_000) -> dict[str, object]:
    token = access_token or _fake_access_token()
    return {
        "access_token": token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "account_id": "acct-123",
    }


@pytest.mark.asyncio
async def test_env_only_requires_real_env_payload_even_when_cache_exists(tmp_path, monkeypatch):
    cache_path = tmp_path / "auth.env-cache.json"
    cache_path.write_text(json.dumps(_token_payload()), encoding="utf-8")

    monkeypatch.setattr(auth, "ENV_TOKEN_CACHE_PATH", cache_path)
    monkeypatch.setattr(auth, "TOKEN_PATH", tmp_path / "auth.json")
    monkeypatch.setenv(auth.ENV_AUTH_ENV_ONLY, "true")
    monkeypatch.delenv(auth.ENV_AUTH_JSON, raising=False)
    monkeypatch.delenv(auth.ENV_AUTH_JSON_B64, raising=False)

    manager = auth.AuthManager()
    status = manager.status()

    assert status["mode"] == "env-only"
    assert status["source"] == "none"
    assert status["has_access_token"] is False

    with pytest.raises(auth.OAuthError) as exc:
        await manager.get_auth_context()

    message = str(exc.value)
    assert "Missing OAuth token in env." in message
    assert "python auth.py login --env-file .env" in message


@pytest.mark.asyncio
async def test_env_only_allows_newer_env_cache_when_env_payload_exists(tmp_path, monkeypatch):
    cache_path = tmp_path / "auth.env-cache.json"
    cache_path.write_text(
        json.dumps(
            _token_payload(
                access_token=_fake_access_token("acct-cache"),
                refresh_token="refresh-cache",
                expires_at=1_900_000_500,
            )
        ),
        encoding="utf-8",
    )

    env_payload = _token_payload(
        access_token=_fake_access_token("acct-env"),
        refresh_token="refresh-env",
        expires_at=1_900_000_000,
    )

    monkeypatch.setattr(auth, "ENV_TOKEN_CACHE_PATH", cache_path)
    monkeypatch.setattr(auth, "TOKEN_PATH", tmp_path / "auth.json")
    monkeypatch.setenv(auth.ENV_AUTH_ENV_ONLY, "true")
    monkeypatch.setenv(auth.ENV_AUTH_JSON, json.dumps(env_payload))
    monkeypatch.delenv(auth.ENV_AUTH_JSON_B64, raising=False)

    manager = auth.AuthManager()
    status = manager.status()
    context = await manager.get_auth_context()

    assert status["source"] == "env-cache"
    assert context["account_id"] == "acct-123"
    assert manager._load_token_data()["refresh_token"] == "refresh-cache"


def test_auth_manager_loads_repo_dotenv_without_exported_shell_vars(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_payload = _token_payload()
    env_path.write_text(
        "\n".join(
            [
                f"{auth.ENV_AUTH_ENV_ONLY}=true",
                f'{auth.ENV_AUTH_JSON}={json.dumps(json.dumps(env_payload))}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(auth, "DEFAULT_ENV_PATH", env_path)
    monkeypatch.delenv(auth.ENV_AUTH_ENV_ONLY, raising=False)
    monkeypatch.delenv(auth.ENV_AUTH_JSON, raising=False)
    monkeypatch.delenv(auth.ENV_AUTH_JSON_B64, raising=False)

    auth._load_dotenv_defaults(env_path)
    manager = auth.AuthManager()
    status = manager.status()

    assert status["mode"] == "env-only"
    assert status["source"] == "env"
    assert status["has_access_token"] is True


@pytest.mark.asyncio
async def test_post_token_payload_retries_transport_errors(monkeypatch):
    attempts = {"count": 0}
    reset_calls: list[str] = []

    class _FakeClient:
        async def post(self, _url, data=None):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise httpx.TransportError("Server closed the connection: [Errno 104] Connection reset by peer")
            return httpx.Response(
                200,
                json={
                    "access_token": _fake_access_token(),
                    "refresh_token": "refresh-2",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "scope": "openid profile email offline_access",
                },
            )

    async def fake_get_auth_http_client():
        return _FakeClient()

    async def fake_reset_shared_http_client(name: str):
        reset_calls.append(name)

    monkeypatch.setattr(auth, "get_auth_http_client", fake_get_auth_http_client)
    monkeypatch.setattr(auth, "reset_shared_http_client", fake_reset_shared_http_client)

    token_data = await auth.AuthManager()._post_token_payload({"grant_type": "refresh_token"})

    assert attempts["count"] == 2
    assert reset_calls == ["auth"]
    assert token_data["refresh_token"] == "refresh-2"
