"""Zero-config OpenAI OAuth (PKCE) token manager for Codex subscription backend."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import sys
import time
import urllib.parse
import webbrowser
import argparse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx


# Public OAuth client constants (same family as Codex CLI OAuth flow).
OPENAI_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
DEFAULT_OPENAI_ISSUER = "https://auth.openai.com"
OPENAI_ISSUER = (
    str(os.getenv("OPENAI_AUTH_BASE", DEFAULT_OPENAI_ISSUER) or DEFAULT_OPENAI_ISSUER)
    .strip()
    .rstrip("/")
)
REDIRECT_URI = "http://localhost:1455/auth/callback"
SCOPES = "openid profile email offline_access"
AUTH_ENDPOINT = str(
    os.getenv("OPENAI_AUTH_ENDPOINT", f"{OPENAI_ISSUER}/oauth/authorize")
    or f"{OPENAI_ISSUER}/oauth/authorize"
).strip()
TOKEN_ENDPOINT = str(
    os.getenv("OPENAI_TOKEN_ENDPOINT", f"{OPENAI_ISSUER}/oauth/token")
    or f"{OPENAI_ISSUER}/oauth/token"
).strip()
OAUTH_ORIGINATOR = "pi"
JWT_CLAIM_PATH = "https://api.openai.com/auth"

# Local runtime state storage (outside repo).
RUNTIME_DIR = Path.home() / ".tg_userbot"
TOKEN_PATH = RUNTIME_DIR / "auth.json"
DB_PATH = RUNTIME_DIR / "seen.db"
ERROR_LOG_PATH = RUNTIME_DIR / "errors.log"
ENV_AUTH_JSON = "TG_USERBOT_AUTH_JSON"
ENV_AUTH_JSON_B64 = "TG_USERBOT_AUTH_JSON_B64"
ENV_AUTH_ENV_ONLY = "OPENAI_AUTH_ENV_ONLY"

TOKEN_REFRESH_SKEW_SECONDS = 60
CALLBACK_TIMEOUT_SECONDS = 300


class OAuthError(RuntimeError):
    """Raised for OAuth flow and token lifecycle errors."""


@dataclass
class _CallbackResult:
    code: Optional[str]
    state: Optional[str]
    error: Optional[str]
    error_description: Optional[str]


class _CallbackServer(HTTPServer):
    callback_result: Optional[_CallbackResult] = None


def _base64url_no_padding(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - (len(value) % 4)) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _decode_jwt_payload(token: str) -> Dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = json.loads(_base64url_decode(parts[1]).decode("utf-8"))
        if isinstance(payload, dict):
            return payload
        return {}
    except Exception:
        return {}


def _extract_account_id_from_access_token(access_token: str) -> Optional[str]:
    payload = _decode_jwt_payload(access_token)
    auth_claim = payload.get(JWT_CLAIM_PATH)
    if not isinstance(auth_claim, dict):
        return None
    account_id = auth_claim.get("chatgpt_account_id")
    if isinstance(account_id, str) and account_id.strip():
        return account_id.strip()
    return None


def _normalize_unix_seconds(raw: Any) -> Optional[int]:
    try:
        value = int(raw)
    except Exception:
        return None
    # If milliseconds, convert to seconds.
    if value > 10_000_000_000:
        value = value // 1000
    if value <= 0:
        return None
    return value


def _generate_code_verifier() -> str:
    # RFC 7636: high-entropy cryptographic random string (32 bytes -> base64url).
    return _base64url_no_padding(os.urandom(32))


def _generate_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    return _base64url_no_padding(digest)


def _generate_state() -> str:
    # 16 random bytes in hex format.
    return secrets.token_hex(16)


def _build_callback_handler(expected_path: str):
    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != expected_path:
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Not found.")
                return

            query = urllib.parse.parse_qs(parsed.query)
            self.server.callback_result = _CallbackResult(  # type: ignore[attr-defined]
                code=query.get("code", [None])[0],
                state=query.get("state", [None])[0],
                error=query.get("error", [None])[0],
                error_description=query.get("error_description", [None])[0],
            )

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h3>OpenAI authentication complete.</h3>"
                b"<p>Return to terminal.</p></body></html>"
            )

    return CallbackHandler


def _wait_for_callback(timeout_seconds: int) -> _CallbackResult:
    parsed = urllib.parse.urlparse(REDIRECT_URI)
    host = parsed.hostname or "localhost"
    port = parsed.port or 1455
    path = parsed.path or "/"

    handler_cls = _build_callback_handler(path)
    server = _CallbackServer((host, port), handler_cls)
    server.timeout = 1
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            server.handle_request()
            if server.callback_result is not None:
                return server.callback_result
    finally:
        server.server_close()

    raise OAuthError("Timed out waiting for OAuth callback.")


def _callback_from_url(value: str) -> _CallbackResult:
    parsed = urllib.parse.urlparse((value or "").strip())
    query = urllib.parse.parse_qs(parsed.query)
    return _CallbackResult(
        code=query.get("code", [None])[0],
        state=query.get("state", [None])[0],
        error=query.get("error", [None])[0],
        error_description=query.get("error_description", [None])[0],
    )


def _serialize_token_for_env(token_data: Dict[str, Any]) -> Tuple[str, str]:
    payload = {
        "access_token": str(token_data.get("access_token") or ""),
        "refresh_token": str(token_data.get("refresh_token") or ""),
        "expires_at": int(_normalize_unix_seconds(token_data.get("expires_at")) or 0),
        "account_id": str(token_data.get("account_id") or ""),
        "scope": token_data.get("scope"),
        "token_type": token_data.get("token_type"),
    }
    json_blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    b64_blob = base64.b64encode(json_blob.encode("utf-8")).decode("utf-8")
    return json_blob, b64_blob


def ensure_runtime_dir() -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(RUNTIME_DIR, 0o700)
    except OSError:
        pass
    return RUNTIME_DIR


class AuthManager:
    """Handles token load, refresh, and full OAuth PKCE login."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._lock = asyncio.Lock()
        self._runtime_token_data: Optional[Dict[str, Any]] = None
        ensure_runtime_dir()

    def _is_env_only_mode(self) -> bool:
        raw = str(os.getenv(ENV_AUTH_ENV_ONLY, "true") or "").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    async def get_access_token(self) -> str:
        context = await self.get_auth_context()
        return context["access_token"]

    async def get_auth_context(self) -> Dict[str, str]:
        """
        Return auth context needed for Codex subscription backend calls.
        Keys: access_token, account_id
        """
        async with self._lock:
            token_data = self._load_token_data()
            if token_data is None:
                if self._is_env_only_mode():
                    raise OAuthError(
                        "Missing OAuth token in env. Set TG_USERBOT_AUTH_JSON "
                        "or TG_USERBOT_AUTH_JSON_B64."
                    )
                if not sys.stdin.isatty():
                    raise OAuthError(
                        "No OAuth token in env and interactive browser auth is unavailable "
                        "(non-interactive runtime). Bootstrap locally, then set "
                        "TG_USERBOT_AUTH_JSON_B64 in secrets."
                    )
                token_data = await self._run_full_oauth_flow()
            elif self._is_expiring(token_data):
                token_data = await self._refresh_or_reauth(token_data)
            elif not token_data.get("access_token"):
                token_data = await self._refresh_or_reauth(token_data)

            access_token = str(token_data.get("access_token") or "").strip()
            if not access_token:
                if self._is_env_only_mode():
                    raise OAuthError(
                        "OAuth access token missing in env payload. "
                        "Update TG_USERBOT_AUTH_JSON/B64."
                    )
                token_data = await self._run_full_oauth_flow()
                access_token = token_data["access_token"]

            account_id = str(token_data.get("account_id") or "").strip()
            if not account_id:
                inferred = _extract_account_id_from_access_token(access_token)
                if inferred:
                    token_data["account_id"] = inferred
                    account_id = inferred

            if not account_id:
                token_data = await self._refresh_or_reauth(token_data)
                access_token = str(token_data.get("access_token") or "").strip()
                account_id = str(token_data.get("account_id") or "").strip()

            if not access_token:
                raise OAuthError("Missing access_token after auth flow.")
            if not account_id:
                raise OAuthError(
                    "Missing ChatGPT account_id in OAuth token. "
                    "Re-authentication required."
                )

            self._save_token_data(token_data)
            return {"access_token": access_token, "account_id": account_id}

    async def refresh_token(self) -> str:
        context = await self.refresh_auth_context()
        return context["access_token"]

    async def refresh_auth_context(self) -> Dict[str, str]:
        """Force refresh using refresh_token; fallback to full OAuth if needed."""
        async with self._lock:
            token_data = self._load_token_data() or {}
            token_data = await self._refresh_or_reauth(token_data, allow_reauth=False)
            self._save_token_data(token_data)
            return self._token_data_to_context(token_data)

    async def reauthenticate(self) -> str:
        """Force full OAuth auth flow regardless of existing token state."""
        context = await self.reauthenticate_context()
        return context["access_token"]

    async def reauthenticate_context(self) -> Dict[str, str]:
        async with self._lock:
            if self._is_env_only_mode():
                raise OAuthError(
                    "Re-authenticate disabled in env-only mode. "
                    "Update TG_USERBOT_AUTH_JSON/B64 secret and restart."
                )
            token_data = await self._run_full_oauth_flow()
            self._save_token_data(token_data)
            return self._token_data_to_context(token_data)

    def _token_data_to_context(self, token_data: Dict[str, Any]) -> Dict[str, str]:
        access_token = str(token_data.get("access_token") or "").strip()
        account_id = str(token_data.get("account_id") or "").strip()
        if not account_id and access_token:
            inferred = _extract_account_id_from_access_token(access_token)
            if inferred:
                token_data["account_id"] = inferred
                account_id = inferred

        if not access_token:
            raise OAuthError("Token state missing access_token.")
        if not account_id:
            raise OAuthError("Token state missing account_id.")
        return {"access_token": access_token, "account_id": account_id}

    def _load_token_data(self) -> Optional[Dict[str, Any]]:
        if isinstance(self._runtime_token_data, dict):
            return dict(self._runtime_token_data)

        env_data = self._load_token_data_from_env()
        if env_data is not None:
            self._logger.info("Loaded OAuth token state from env secret.")
            return env_data
        return None

    def _load_token_data_from_env(self) -> Optional[Dict[str, Any]]:
        raw_json = str(os.getenv(ENV_AUTH_JSON, "") or "").strip()
        raw_b64 = str(os.getenv(ENV_AUTH_JSON_B64, "") or "").strip()

        if not raw_json and raw_b64:
            try:
                raw_json = _base64url_decode(raw_b64).decode("utf-8")
            except Exception:
                # Fallback to standard base64 form.
                try:
                    raw_json = base64.b64decode(raw_b64).decode("utf-8")
                except Exception:
                    self._logger.exception(
                        "Failed to decode %s environment variable.",
                        ENV_AUTH_JSON_B64,
                    )
                    return None

        if not raw_json:
            return None

        try:
            payload = json.loads(raw_json)
        except Exception:
            self._logger.exception("Failed to parse %s as JSON.", ENV_AUTH_JSON)
            return None

        if not isinstance(payload, dict):
            self._logger.error("OAuth env token payload must be a JSON object.")
            return None

        return self._normalize_loaded_token_data(payload)

    def _normalize_loaded_token_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Backward-compatible migrations from previous auth.json shapes.
        if not data.get("access_token") and isinstance(data.get("access"), str):
            data["access_token"] = data["access"]
        if not data.get("refresh_token") and isinstance(data.get("refresh"), str):
            data["refresh_token"] = data["refresh"]

        if not data.get("account_id"):
            if isinstance(data.get("accountId"), str):
                data["account_id"] = data["accountId"]
            elif isinstance(data.get("chatgpt_account_id"), str):
                data["account_id"] = data["chatgpt_account_id"]

        expires_at = _normalize_unix_seconds(data.get("expires_at"))
        if expires_at is None:
            expires_at = _normalize_unix_seconds(data.get("expires"))
        if expires_at is not None:
            data["expires_at"] = expires_at

        access_token = str(data.get("access_token") or "").strip()
        refresh_token = str(data.get("refresh_token") or "").strip()
        if not access_token and not refresh_token:
            return None

        if access_token and not data.get("account_id"):
            inferred = _extract_account_id_from_access_token(access_token)
            if inferred:
                data["account_id"] = inferred

        return data

    def _save_token_data(self, token_data: Dict[str, Any]) -> None:
        # Env-only mode: keep refreshed token state in memory only.
        # This avoids writing auth.json to disk in cloud runtimes.
        self._runtime_token_data = dict(token_data)

    def _is_expiring(self, token_data: Dict[str, Any]) -> bool:
        expires_at = _normalize_unix_seconds(token_data.get("expires_at"))
        if not expires_at:
            return True
        return expires_at < int(time.time()) + TOKEN_REFRESH_SKEW_SECONDS

    async def _refresh_or_reauth(
        self,
        token_data: Dict[str, Any],
        *,
        allow_reauth: bool = True,
    ) -> Dict[str, Any]:
        refresh_token = str(token_data.get("refresh_token") or "").strip()
        if not refresh_token:
            if allow_reauth:
                if self._is_env_only_mode():
                    raise OAuthError(
                        "No refresh_token available in env payload. "
                        "Update TG_USERBOT_AUTH_JSON/B64."
                    )
                self._logger.info("No refresh token found. Running full OAuth flow.")
                return await self._run_full_oauth_flow()
            raise OAuthError("No refresh token available.")

        try:
            refreshed = await self._refresh_with_token(refresh_token)
            if not refreshed.get("refresh_token"):
                refreshed["refresh_token"] = refresh_token

            if not refreshed.get("account_id") and refreshed.get("access_token"):
                inferred = _extract_account_id_from_access_token(
                    str(refreshed["access_token"])
                )
                if inferred:
                    refreshed["account_id"] = inferred
            return refreshed
        except Exception:
            if allow_reauth:
                if self._is_env_only_mode():
                    raise OAuthError(
                        "OAuth refresh failed in env-only mode. "
                        "Update TG_USERBOT_AUTH_JSON/B64 and restart."
                    )
                self._logger.exception("Refresh failed. Running full OAuth flow.")
                return await self._run_full_oauth_flow()
            self._logger.exception("Refresh failed.")
            raise

    async def _run_full_oauth_flow(self) -> Dict[str, Any]:
        if self._is_env_only_mode():
            raise OAuthError(
                "OAuth browser flow disabled in env-only mode. "
                "Provide TG_USERBOT_AUTH_JSON or TG_USERBOT_AUTH_JSON_B64."
            )
        code_verifier = _generate_code_verifier()
        code_challenge = _generate_code_challenge(code_verifier)
        state = _generate_state()

        params = {
            "client_id": OPENAI_CLIENT_ID,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPES,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "state": state,
            # Mirrors Codex login hints.
            "id_token_add_organizations": "true",
            "codex_cli_simplified_flow": "true",
            "originator": OAUTH_ORIGINATOR,
        }
        auth_url = f"{AUTH_ENDPOINT}?{urllib.parse.urlencode(params)}"
        self._logger.info("Opening browser for OpenAI OAuth login...")
        self._logger.info("If browser does not open, visit:\n%s", auth_url)
        webbrowser.open(auth_url, new=1, autoraise=True)
        callback: _CallbackResult
        try:
            callback = await asyncio.to_thread(_wait_for_callback, CALLBACK_TIMEOUT_SECONDS)
        except OAuthError:
            if not sys.stdin.isatty():
                raise OAuthError(
                    "OAuth callback timed out. This runtime cannot complete localhost redirect. "
                    "Run local bootstrap and set TG_USERBOT_AUTH_JSON_B64 in env."
                )
            self._logger.warning(
                "OAuth callback timed out. Paste the final callback URL from your browser "
                "(it should start with http://localhost:1455/auth/callback?...)."
            )
            pasted = input("Paste callback URL: ").strip()
            if not pasted:
                raise OAuthError("OAuth callback URL not provided.")
            callback = _callback_from_url(pasted)

        if callback.error:
            raise OAuthError(
                f"OAuth authorize error: {callback.error} "
                f"{callback.error_description or ''}".strip()
            )
        if callback.state != state:
            raise OAuthError("OAuth state mismatch.")
        if not callback.code:
            raise OAuthError("OAuth callback did not include authorization code.")

        token_data = await self._exchange_code_for_tokens(callback.code, code_verifier)
        if not token_data.get("account_id"):
            account_id = _extract_account_id_from_access_token(token_data["access_token"])
            if not account_id:
                raise OAuthError(
                    "Could not extract ChatGPT account ID from OAuth access token."
                )
            token_data["account_id"] = account_id
        return token_data

    async def _exchange_code_for_tokens(
        self, code: str, code_verifier: str
    ) -> Dict[str, Any]:
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": OPENAI_CLIENT_ID,
            "code_verifier": code_verifier,
        }
        return await self._post_token_payload(payload)

    async def _refresh_with_token(self, refresh_token: str) -> Dict[str, Any]:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": OPENAI_CLIENT_ID,
        }
        return await self._post_token_payload(payload)

    async def _post_token_payload(self, payload: Dict[str, str]) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.post(TOKEN_ENDPOINT, data=payload)

        if response.status_code >= 400:
            raise OAuthError(
                f"Token endpoint error {response.status_code}: {response.text}"
            )

        raw = response.json()
        access_token = str(raw.get("access_token") or "").strip()
        refresh_token = str(raw.get("refresh_token") or "").strip()
        if not access_token:
            raise OAuthError("Token response missing access_token.")

        expires_in = int(raw.get("expires_in", 3600))
        account_id = _extract_account_id_from_access_token(access_token)

        token_data: Dict[str, Any] = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "id_token": raw.get("id_token"),
            "token_type": raw.get("token_type"),
            "scope": raw.get("scope"),
            "expires_in": expires_in,
            "expires_at": int(time.time()) + expires_in,
            "account_id": account_id,
            "token_source": "codex_oauth_v1",
        }
        return token_data


async def bootstrap_env_oauth_payload(
    logger: Optional[logging.Logger] = None,
) -> Dict[str, str]:
    """
    Run interactive OAuth locally, then return env-ready JSON + base64 payload.
    Useful for headless/cloud runtimes where localhost callback cannot complete.
    """
    prev = os.getenv(ENV_AUTH_ENV_ONLY)
    os.environ[ENV_AUTH_ENV_ONLY] = "false"
    try:
        manager = AuthManager(logger=logger)
        token_data = await manager._run_full_oauth_flow()
        manager._save_token_data(token_data)
        json_blob, b64_blob = _serialize_token_for_env(token_data)
        return {
            "json": json_blob,
            "b64": b64_blob,
            "account_id": str(token_data.get("account_id") or ""),
            "expires_at": str(int(_normalize_unix_seconds(token_data.get("expires_at")) or 0)),
        }
    finally:
        if prev is None:
            os.environ.pop(ENV_AUTH_ENV_ONLY, None)
        else:
            os.environ[ENV_AUTH_ENV_ONLY] = prev


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OpenAI OAuth helpers for TeleUserBot",
    )
    sub = parser.add_subparsers(dest="command")

    bootstrap = sub.add_parser(
        "bootstrap-env",
        help="Run local OAuth and print TG_USERBOT_AUTH_JSON / TG_USERBOT_AUTH_JSON_B64",
    )
    bootstrap.add_argument(
        "--out",
        default="",
        help="Optional path to write env export snippet.",
    )
    return parser


def _run_cli() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    if args.command != "bootstrap-env":
        parser.print_help()
        return 0

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("auth-bootstrap")

    try:
        payload = asyncio.run(bootstrap_env_oauth_payload(logger=logger))
    except Exception as exc:
        print(f"Bootstrap failed: {exc}")
        return 1

    snippet = (
        f"TG_USERBOT_AUTH_JSON={json.dumps(payload['json'])}\n"
        f"TG_USERBOT_AUTH_JSON_B64={json.dumps(payload['b64'])}\n"
    )
    print("\n# Set these as secrets in Replit/server environment:\n")
    print(snippet)
    print(
        f"# Token account_id={payload['account_id']} expires_at={payload['expires_at']}"
    )

    out = str(getattr(args, "out", "") or "").strip()
    if out:
        path = Path(out).expanduser().resolve()
        path.write_text(snippet, encoding="utf-8")
        print(f"# Wrote env snippet to: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(_run_cli())
