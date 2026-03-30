"""Zero-config OpenAI OAuth (PKCE) token manager for Codex subscription backend."""

from __future__ import annotations

import asyncio
import ast
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

from shared_http import get_auth_http_client, reset_shared_http_client


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"


def _load_dotenv_defaults(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            try:
                value = str(ast.literal_eval(value))
            except Exception:
                value = value[1:-1]
        os.environ.setdefault(key, value)


_load_dotenv_defaults(DEFAULT_ENV_PATH)


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
ENV_TOKEN_CACHE_PATH = RUNTIME_DIR / "auth.env-cache.json"
DB_PATH = RUNTIME_DIR / "seen.db"
ERROR_LOG_PATH = RUNTIME_DIR / "errors.log"
ENV_AUTH_JSON = "TG_USERBOT_AUTH_JSON"
ENV_AUTH_JSON_B64 = "TG_USERBOT_AUTH_JSON_B64"
ENV_AUTH_ENV_ONLY = "OPENAI_AUTH_ENV_ONLY"

TOKEN_REFRESH_SKEW_SECONDS = 60
CALLBACK_TIMEOUT_SECONDS = 300
AUTH_FAILURE_COOLDOWN_SECONDS = 900
AUTH_TERMINAL_FAILURE_COOLDOWN_SECONDS = 3600


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


def normalize_oauth_error_message(exc: Exception) -> str:
    text = str(exc or "").strip()
    return text or exc.__class__.__name__


def _is_terminal_env_auth_error(message: str) -> bool:
    lowered = str(message or "").strip().lower()
    if not lowered:
        return False
    terminal_markers = (
        "refresh token is stale",
        "refresh token has already been used",
        "refresh_token_reused",
        "authentication token has been invalidated",
        "sign in again",
        "update tg_userbot_auth_json",
        "update your deployed secrets",
    )
    return any(marker in lowered for marker in terminal_markers)


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


def _env_snippet_from_payload(payload: Dict[str, str]) -> str:
    return (
        f"{ENV_AUTH_JSON}={json.dumps(payload['json'])}\n"
        f"{ENV_AUTH_JSON_B64}={json.dumps(payload['b64'])}\n"
    )


def _extract_env_key(line: str) -> Optional[str]:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key, _ = stripped.split("=", 1)
    key = key.strip()
    if not key:
        return None
    return key


def _render_env_line(key: str, value: str) -> str:
    if key == ENV_AUTH_ENV_ONLY:
        return f"{key}={value}"
    return f"{key}={json.dumps(value)}"


def write_auth_payload_to_env_file(payload: Dict[str, str], env_path: Path) -> Path:
    """Create/update .env with env-only auth keys from OAuth bootstrap payload."""
    env_path = env_path.expanduser().resolve()
    env_path.parent.mkdir(parents=True, exist_ok=True)

    if not env_path.exists():
        example_path = env_path.parent / ".env.example"
        if example_path.exists():
            env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            env_path.write_text("", encoding="utf-8")

    original_text = env_path.read_text(encoding="utf-8")
    original_lines = original_text.splitlines()
    has_trailing_newline = original_text.endswith("\n")

    updates = {
        ENV_AUTH_ENV_ONLY: "true",
        ENV_AUTH_JSON: "",
        ENV_AUTH_JSON_B64: str(payload.get("b64") or ""),
    }
    update_order = [ENV_AUTH_ENV_ONLY, ENV_AUTH_JSON, ENV_AUTH_JSON_B64]

    updated_lines = []
    seen_keys = set()
    for line in original_lines:
        key = _extract_env_key(line)
        if key in updates:
            updated_lines.append(_render_env_line(key, updates[key]))
            seen_keys.add(key)
            continue
        updated_lines.append(line)

    for key in update_order:
        if key not in seen_keys:
            updated_lines.append(_render_env_line(key, updates[key]))

    out_text = "\n".join(updated_lines)
    if has_trailing_newline or out_text:
        out_text += "\n"
    env_path.write_text(out_text, encoding="utf-8")
    return env_path


def clear_auth_payload_from_env_file(env_path: Path) -> Path:
    """Clear env-only OAuth secrets from a local .env file."""
    env_path = env_path.expanduser().resolve()
    if not env_path.exists():
        return env_path

    original_text = env_path.read_text(encoding="utf-8")
    original_lines = original_text.splitlines()
    has_trailing_newline = original_text.endswith("\n")

    updates = {
        ENV_AUTH_JSON: "",
        ENV_AUTH_JSON_B64: "",
    }

    updated_lines = []
    seen_keys = set()
    for line in original_lines:
        key = _extract_env_key(line)
        if key in updates:
            updated_lines.append(_render_env_line(key, updates[key]))
            seen_keys.add(key)
            continue
        updated_lines.append(line)

    for key, value in updates.items():
        if key not in seen_keys:
            updated_lines.append(_render_env_line(key, value))

    out_text = "\n".join(updated_lines)
    if has_trailing_newline or out_text:
        out_text += "\n"
    env_path.write_text(out_text, encoding="utf-8")
    return env_path


def ensure_runtime_dir() -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(RUNTIME_DIR, 0o700)
    except OSError:
        pass
    return RUNTIME_DIR


def _load_token_data_from_file(path: Path = TOKEN_PATH) -> Optional[Dict[str, Any]]:
    token_path = path.expanduser().resolve()
    if not token_path.exists():
        return None
    try:
        payload = json.loads(token_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logging.debug("Failed to load token file %s: %s", token_path, exc)
        return None
    except OSError as exc:
        logging.debug("Failed to read token file %s: %s", token_path, exc)
        return None
    if not isinstance(payload, dict):
        return None
    manager = AuthManager()
    return manager._normalize_loaded_token_data(payload)


def _write_token_data_to_file(token_data: Dict[str, Any], path: Path = TOKEN_PATH) -> Path:
    token_path = path.expanduser().resolve()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(token_data, indent=2, ensure_ascii=False)
    token_path.write_text(serialized + "\n", encoding="utf-8")
    try:
        os.chmod(token_path, 0o600)
    except OSError:
        pass
    return token_path


def _delete_token_file(path: Path = TOKEN_PATH) -> bool:
    token_path = path.expanduser().resolve()
    if not token_path.exists():
        return False
    token_path.unlink()
    return True


def _token_recency_score(token_data: Optional[Dict[str, Any]]) -> int:
    if not isinstance(token_data, dict):
        return 0
    expires_at = _normalize_unix_seconds(token_data.get("expires_at"))
    return int(expires_at or 0)


def _prefer_newer_token_data(
    *candidates: Tuple[str, Optional[Dict[str, Any]]],
) -> Tuple[str, Optional[Dict[str, Any]]]:
    best_source = "none"
    best_payload: Optional[Dict[str, Any]] = None
    best_score = -1
    for source, payload in candidates:
        if payload is None:
            continue
        score = _token_recency_score(payload)
        if score > best_score:
            best_source = source
            best_payload = payload
            best_score = score
    return best_source, best_payload


def _select_env_only_token_data(
    env_data: Optional[Dict[str, Any]],
    env_cache_data: Optional[Dict[str, Any]],
) -> Tuple[str, Optional[Dict[str, Any]]]:
    if env_data is None:
        return "none", None
    return _prefer_newer_token_data(
        ("env-cache", env_cache_data),
        ("env", env_data),
    )


class AuthManager:
    """Handles token load, refresh, and full OAuth PKCE login."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._lock = asyncio.Lock()
        self._runtime_token_data: Optional[Dict[str, Any]] = None
        self._auth_failure_message: Optional[str] = None
        self._auth_failure_until: int = 0
        ensure_runtime_dir()

    def _clear_auth_failure(self) -> None:
        self._auth_failure_message = None
        self._auth_failure_until = 0

    def _set_auth_failure(
        self,
        message: str,
        *,
        cooldown_seconds: int,
        log_level: int = logging.ERROR,
    ) -> None:
        now = int(time.time())
        active_until = now + max(30, int(cooldown_seconds))
        if self._auth_failure_message == message and self._auth_failure_until >= now:
            return
        self._auth_failure_message = message
        self._auth_failure_until = active_until
        self._logger.log(
            log_level,
            "OpenAI auth unavailable until %s: %s",
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(active_until)),
            message,
        )

    def _raise_if_auth_failed(self) -> None:
        now = int(time.time())
        if self._auth_failure_message and self._auth_failure_until > now:
            raise OAuthError(self._auth_failure_message)

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
            self._raise_if_auth_failed()
            token_data = self._load_token_data()
            if token_data is None:
                if self._is_env_only_mode():
                    raise OAuthError(
                        "Missing OAuth token in env. Set TG_USERBOT_AUTH_JSON "
                        "or TG_USERBOT_AUTH_JSON_B64, or run `python auth.py login --env-file .env`."
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
                        "Update TG_USERBOT_AUTH_JSON/B64 or run `python auth.py login --env-file .env`."
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
            self._raise_if_auth_failed()
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

    async def logout(self) -> Dict[str, bool]:
        async with self._lock:
            self._runtime_token_data = None
            self._clear_auth_failure()
            removed_file = _delete_token_file(TOKEN_PATH)
            removed_env_cache = _delete_token_file(ENV_TOKEN_CACHE_PATH)
            return {
                "cleared_memory": True,
                "removed_file": removed_file,
                "removed_env_cache": removed_env_cache,
            }

    def status(self) -> Dict[str, Any]:
        env_data = self._load_token_data_from_env()
        env_cache_data = _load_token_data_from_file(ENV_TOKEN_CACHE_PATH)
        file_data = None if self._is_env_only_mode() else _load_token_data_from_file(TOKEN_PATH)

        source = "none"
        token_data: Dict[str, Any] = {}
        if isinstance(self._runtime_token_data, dict):
            source = "memory"
            token_data = dict(self._runtime_token_data)
        elif self._is_env_only_mode():
            source, selected = _select_env_only_token_data(env_data, env_cache_data)
            token_data = dict(selected or {})
        elif file_data is not None:
            source = "file"
            token_data = file_data
        elif env_data is not None:
            source = "env"
            token_data = env_data

        expires_at = _normalize_unix_seconds(token_data.get("expires_at"))
        return {
            "mode": "env-only" if self._is_env_only_mode() else "local-file",
            "source": source,
            "account_id": str(token_data.get("account_id") or ""),
            "has_access_token": bool(str(token_data.get("access_token") or "").strip()),
            "has_refresh_token": bool(str(token_data.get("refresh_token") or "").strip()),
            "expires_at": expires_at,
            "token_file": str(TOKEN_PATH),
            "token_file_exists": TOKEN_PATH.exists(),
            "env_cache_file": str(ENV_TOKEN_CACHE_PATH),
            "env_cache_file_exists": ENV_TOKEN_CACHE_PATH.exists(),
        }

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
        if self._is_env_only_mode():
            env_cache_data = _load_token_data_from_file(ENV_TOKEN_CACHE_PATH)
            source, selected = _select_env_only_token_data(env_data, env_cache_data)
            if selected is not None:
                if source == "env-cache":
                    self._logger.info("Loaded OAuth token state from %s.", ENV_TOKEN_CACHE_PATH)
                else:
                    self._logger.info("Loaded OAuth token state from env secret.")
                return dict(selected)
            return None

        if env_data is not None:
            self._logger.info("Loaded OAuth token state from env secret.")
            return env_data
        if not self._is_env_only_mode():
            file_data = _load_token_data_from_file(TOKEN_PATH)
            if file_data is not None:
                self._logger.info("Loaded OAuth token state from %s.", TOKEN_PATH)
                return file_data
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
        self._runtime_token_data = dict(token_data)
        self._clear_auth_failure()
        if self._is_env_only_mode():
            # Persist refreshed env-mode tokens to a runtime shadow cache so the
            # next restart does not replay an already-consumed refresh token.
            _write_token_data_to_file(token_data, ENV_TOKEN_CACHE_PATH)
            return
        _write_token_data_to_file(token_data, TOKEN_PATH)

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
                        "Update TG_USERBOT_AUTH_JSON/B64 or run `python auth.py login --env-file .env`."
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
        except Exception as exc:
            message = normalize_oauth_error_message(exc)
            if self._is_env_only_mode():
                is_terminal = _is_terminal_env_auth_error(message)
                if allow_reauth:
                    env_message = (
                        message
                        if is_terminal
                        else (
                            "OAuth refresh failed in env-only mode. "
                            "Update TG_USERBOT_AUTH_JSON/B64 and restart, or run `python auth.py login --env-file .env`."
                        )
                    )
                    self._set_auth_failure(
                        env_message,
                        cooldown_seconds=(
                            AUTH_TERMINAL_FAILURE_COOLDOWN_SECONDS
                            if is_terminal
                            else AUTH_FAILURE_COOLDOWN_SECONDS
                        ),
                    )
                    raise OAuthError(env_message) from exc
                self._set_auth_failure(
                    message,
                    cooldown_seconds=(
                        AUTH_TERMINAL_FAILURE_COOLDOWN_SECONDS
                        if is_terminal
                        else AUTH_FAILURE_COOLDOWN_SECONDS
                    ),
                )
                raise OAuthError(message) from exc
            if allow_reauth:
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
        response: httpx.Response | None = None
        for attempt in range(3):
            http = await get_auth_http_client()
            try:
                response = await http.post(TOKEN_ENDPOINT, data=payload)
                break
            except httpx.TransportError as exc:
                await reset_shared_http_client("auth")
                if attempt < 2:
                    self._logger.warning(
                        "OpenAI OAuth transport error on attempt %s/3: %s",
                        attempt + 1,
                        exc,
                    )
                    await asyncio.sleep(0.35 * (attempt + 1))
                    continue
                raise OAuthError(
                    f"OpenAI OAuth connection failed after retries: {exc}"
                ) from exc

        if response is None:
            raise OAuthError("OpenAI OAuth connection failed before receiving a response.")

        if response.status_code >= 400:
            try:
                err_payload = response.json()
            except Exception:
                err_payload = None
            if isinstance(err_payload, dict):
                err = err_payload.get("error")
                if isinstance(err, dict):
                    err_code = str(err.get("code") or "").strip().lower()
                    err_message = str(err.get("message") or "").strip()
                    if err_code == "refresh_token_reused":
                        if self._is_env_only_mode():
                            raise OAuthError(
                                "Stored env refresh token is stale because it was already rotated. "
                                "Run `python auth.py login --env-file .env` and update your deployed secrets."
                            )
                        raise OAuthError(
                            "Stored refresh token is stale because it was already rotated. "
                            "Run `python auth.py login` to sign in again."
                        )
                    if err_message:
                        raise OAuthError(
                            f"Token endpoint error {response.status_code}: {err_message}"
                        )
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


async def login_local_oauth(
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Run browser OAuth and persist token state to ~/.tg_userbot/auth.json."""
    prev = os.getenv(ENV_AUTH_ENV_ONLY)
    os.environ[ENV_AUTH_ENV_ONLY] = "false"
    try:
        manager = AuthManager(logger=logger)
        token_data = await manager._run_full_oauth_flow()
        manager._save_token_data(token_data)
        return token_data
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

    setup = sub.add_parser(
        "setup-env",
        help="Run local OAuth and write/update .env automatically (all-in-one local setup).",
    )
    setup.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file to create/update. Default: .env in current directory.",
    )
    setup.add_argument(
        "--out",
        default="",
        help="Optional path to also write TG_USERBOT_AUTH_JSON/TG_USERBOT_AUTH_JSON_B64 snippet.",
    )
    setup.add_argument(
        "--print-secrets",
        action="store_true",
        help="Print full token secrets to stdout (sensitive).",
    )

    login = sub.add_parser(
        "login",
        help="Login to OpenAI OAuth either locally (~/.tg_userbot/auth.json) or by updating a .env file.",
    )
    login.add_argument(
        "--env-file",
        default="",
        help="If set, run env bootstrap and write/update this .env file instead of local auth.json.",
    )
    login.add_argument(
        "--print-secrets",
        action="store_true",
        help="When using --env-file, also print the raw env secrets to stdout (sensitive).",
    )

    logout = sub.add_parser(
        "logout",
        help="Clear saved OpenAI OAuth state.",
    )
    logout.add_argument(
        "--env-file",
        default="",
        help="Optional .env file to clear TG_USERBOT_AUTH_JSON / TG_USERBOT_AUTH_JSON_B64 from.",
    )

    sub.add_parser(
        "status",
        help="Show which OpenAI OAuth token source is active and whether it is usable.",
    )
    return parser


def _run_cli() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    if args.command not in {"bootstrap-env", "setup-env", "login", "logout", "status"}:
        parser.print_help()
        return 0

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logger = logging.getLogger("auth-bootstrap")

    if args.command == "status":
        manager = AuthManager(logger=logger)
        status = manager.status()
        expires_at = status.get("expires_at")
        print(f"mode={status['mode']}")
        print(f"source={status['source']}")
        print(f"account_id={status['account_id'] or 'unknown'}")
        print(f"has_access_token={status['has_access_token']}")
        print(f"has_refresh_token={status['has_refresh_token']}")
        print(f"token_file={status['token_file']}")
        print(f"token_file_exists={status['token_file_exists']}")
        print(f"env_cache_file={status['env_cache_file']}")
        print(f"env_cache_file_exists={status['env_cache_file_exists']}")
        if expires_at:
            print(f"expires_at={expires_at}")
        else:
            print("expires_at=unknown")
        return 0

    if args.command == "logout":
        manager = AuthManager(logger=logger)
        result = asyncio.run(manager.logout())
        env_file = str(getattr(args, "env_file", "") or "").strip()
        if env_file:
            path = clear_auth_payload_from_env_file(Path(env_file))
            print(f"# Cleared env auth secrets in: {path}")
        print(f"# Cleared in-memory runtime state: {result['cleared_memory']}")
        print(f"# Removed local token file: {result['removed_file']}")
        print(f"# Removed env shadow cache: {result['removed_env_cache']}")
        print("# If you stored auth in external secret managers, remove it there too.")
        return 0

    if args.command == "login":
        env_file = str(getattr(args, "env_file", "") or "").strip()
        if env_file:
            try:
                payload = asyncio.run(bootstrap_env_oauth_payload(logger=logger))
            except Exception as exc:
                print(f"Login failed: {exc}")
                return 1
            snippet = _env_snippet_from_payload(payload)
            env_path = write_auth_payload_to_env_file(payload, Path(env_file))
            print(f"# Updated env file: {env_path}")
            print(f"# {ENV_AUTH_ENV_ONLY}=true")
            print(f"# {ENV_AUTH_JSON}=\"\"")
            print(f"# {ENV_AUTH_JSON_B64}=SET (len={len(payload['b64'])})")
            print(
                f"# Token account_id={payload['account_id']} expires_at={payload['expires_at']}"
            )
            if bool(getattr(args, "print_secrets", False)):
                print("\n# Sensitive values (requested via --print-secrets):\n")
                print(snippet)
            return 0

        try:
            token_data = asyncio.run(login_local_oauth(logger=logger))
        except Exception as exc:
            print(f"Login failed: {exc}")
            return 1
        expires_at = int(_normalize_unix_seconds(token_data.get("expires_at")) or 0)
        print(f"# Saved local OAuth token: {TOKEN_PATH}")
        print(f"# account_id={token_data.get('account_id') or ''} expires_at={expires_at}")
        print(f"# Set {ENV_AUTH_ENV_ONLY}=false when running main.py to use local auth.json.")
        return 0

    try:
        payload = asyncio.run(bootstrap_env_oauth_payload(logger=logger))
    except Exception as exc:
        print(f"Bootstrap failed: {exc}")
        return 1

    snippet = _env_snippet_from_payload(payload)

    if args.command == "bootstrap-env":
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

    env_file = str(getattr(args, "env_file", "") or "").strip() or ".env"
    env_path = write_auth_payload_to_env_file(payload, Path(env_file))
    print(f"# Updated env file: {env_path}")
    print(f"# {ENV_AUTH_ENV_ONLY}=true")
    print(f"# {ENV_AUTH_JSON}=\"\"")
    print(f"# {ENV_AUTH_JSON_B64}=SET (len={len(payload['b64'])})")
    print(
        f"# Token account_id={payload['account_id']} expires_at={payload['expires_at']}"
    )

    out = str(getattr(args, "out", "") or "").strip()
    if out:
        path = Path(out).expanduser().resolve()
        path.write_text(snippet, encoding="utf-8")
        print(f"# Wrote env snippet to: {path}")

    if bool(getattr(args, "print_secrets", False)):
        print("\n# Sensitive values (requested via --print-secrets):\n")
        print(snippet)

    return 0


if __name__ == "__main__":
    raise SystemExit(_run_cli())
