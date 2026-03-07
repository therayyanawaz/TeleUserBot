from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import auth


class AuthEnvCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        self.temp_path = Path(self._tempdir.name)
        self.env_cache_path = self.temp_path / "auth.env-cache.json"
        self.token_path = self.temp_path / "auth.json"

        self.env_patcher = mock.patch.dict(
            os.environ,
            {
                auth.ENV_AUTH_ENV_ONLY: "true",
                auth.ENV_AUTH_JSON: "",
                auth.ENV_AUTH_JSON_B64: "",
            },
            clear=False,
        )
        self.env_patcher.start()
        self.addCleanup(self.env_patcher.stop)

        self.cache_patcher = mock.patch.object(auth, "ENV_TOKEN_CACHE_PATH", self.env_cache_path)
        self.cache_patcher.start()
        self.addCleanup(self.cache_patcher.stop)

        self.token_patcher = mock.patch.object(auth, "TOKEN_PATH", self.token_path)
        self.token_patcher.start()
        self.addCleanup(self.token_patcher.stop)

    def test_env_only_save_writes_shadow_cache(self) -> None:
        manager = auth.AuthManager()
        token_data = {
            "access_token": "access-new",
            "refresh_token": "refresh-new",
            "expires_at": 2000,
            "account_id": "acct-1",
        }

        manager._save_token_data(token_data)

        self.assertTrue(self.env_cache_path.exists())
        saved = json.loads(self.env_cache_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["refresh_token"], "refresh-new")
        self.assertEqual(saved["access_token"], "access-new")

    def test_env_only_load_prefers_newer_shadow_cache(self) -> None:
        stale_env = {
            "access_token": "access-old",
            "refresh_token": "refresh-old",
            "expires_at": 1000,
            "account_id": "acct-1",
        }
        os.environ[auth.ENV_AUTH_JSON] = json.dumps(stale_env)

        fresh_cache = {
            "access_token": "access-fresh",
            "refresh_token": "refresh-fresh",
            "expires_at": 5000,
            "account_id": "acct-1",
        }
        auth._write_token_data_to_file(fresh_cache, self.env_cache_path)

        manager = auth.AuthManager()
        loaded = manager._load_token_data()

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["refresh_token"], "refresh-fresh")
        self.assertEqual(loaded["access_token"], "access-fresh")

    def test_stale_env_refresh_is_blocked_after_first_failure(self) -> None:
        stale_env = {
            "access_token": "access-old",
            "refresh_token": "refresh-old",
            "expires_at": 1000,
            "account_id": "acct-1",
        }
        os.environ[auth.ENV_AUTH_JSON] = json.dumps(stale_env)

        manager = auth.AuthManager()
        refresh_calls = {"count": 0}

        async def fake_refresh(_refresh_token: str) -> dict:
            refresh_calls["count"] += 1
            raise auth.OAuthError(
                "Stored env refresh token is stale because it was already rotated."
            )

        with mock.patch.object(manager, "_refresh_with_token", side_effect=fake_refresh):
            with self.assertRaises(auth.OAuthError):
                auth.asyncio.run(manager.refresh_auth_context())
            with self.assertRaises(auth.OAuthError):
                auth.asyncio.run(manager.refresh_auth_context())

        self.assertEqual(refresh_calls["count"], 1)


if __name__ == "__main__":
    unittest.main()
