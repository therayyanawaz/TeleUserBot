"""Telegram News Aggregator userbot entry point."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import re
import time
import urllib.parse
from collections import defaultdict, deque
from datetime import datetime
import importlib
from typing import Deque, Dict, List, Sequence, Tuple
import uuid

import httpx
from telethon import TelegramClient, events, functions, utils
from telethon.errors import (
    ChatForwardsRestrictedError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    UserAlreadyParticipantError,
)
from telethon.errors.rpcerrorlist import BotMethodInvalidError
from telethon.tl import types
from telethon.tl.custom.message import Message
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest

import config
from ai_filter import (
    classify_severity,
    create_digest_summary,
    generate_answer_from_context,
    get_quota_health,
    summarize_breaking_headline,
    summarize_or_skip,
)
from auth import AuthManager, ERROR_LOG_PATH, ensure_runtime_dir
from db import (
    ack_digest_batch,
    claim_digest_batch,
    count_inflight,
    count_pending,
    get_last_digest_timestamp,
    init_db,
    is_seen,
    mark_seen,
    mark_seen_many,
    restore_digest_batch,
    save_to_digest_queue,
    set_last_digest_timestamp,
)
from prompts import quiet_period_message
from utils import (
    LiveTelegramStreamer,
    DuplicateRuntime,
    GlobalDuplicateResult,
    HybridDuplicateEngine,
    build_dupe_fingerprint,
    build_telegram_message_link,
    configure_duplicate_runtime,
    estimate_tokens_rough,
    format_eta,
    format_ts,
    is_duplicate_and_handle,
    log_structured,
    normalize_space,
    parse_daily_times,
    parse_time_filter_from_query,
    search_recent_messages,
    seconds_until_next_daily_time,
    split_markdown_chunks,
)


ensure_runtime_dir()

LOGGER = logging.getLogger("tg_news_userbot")
LOGGER.setLevel(logging.INFO)

_error_handler = logging.FileHandler(ERROR_LOG_PATH)
_error_handler.setLevel(logging.ERROR)
_error_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
)
LOGGER.addHandler(_error_handler)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
LOGGER.addHandler(_console_handler)


client: TelegramClient | None = None
auth_manager: AuthManager | None = None
destination_peer = None
bot_destination_token: str | None = None
bot_destination_chat_id: str | None = None

# (channel_id, grouped_id) -> [messages]
album_buffers: Dict[Tuple[str, int], List[Message]] = defaultdict(list)
album_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
digest_scheduler_task: asyncio.Task | None = None
source_title_cache: Dict[str, str] = {}
digest_next_run_ts: float | None = None
digest_retry_backoff_seconds: int = 0
digest_loop_lock = asyncio.Lock()
dupe_detector: HybridDuplicateEngine | None = None
monitored_source_chat_ids: List[int] = []
query_last_request_ts: Dict[int, float] = {}
query_conversation_history: Dict[int, Deque[Dict[str, str]]] = defaultdict(
    lambda: deque(maxlen=20)
)
breaking_delivery_refs: Dict[str, Dict[str, object]] = {}
query_allowed_bot_user_id: int | None = None
query_bot_user_id_checked: bool = False

ALBUM_WAIT_SECONDS = 1.5
ENV_PATH = Path(__file__).with_name(".env")


def _is_missing_telegram_api_id() -> bool:
    return not isinstance(config.TELEGRAM_API_ID, int) or config.TELEGRAM_API_ID <= 0


def _is_missing_telegram_api_hash() -> bool:
    return not isinstance(config.TELEGRAM_API_HASH, str) or not config.TELEGRAM_API_HASH.strip()


def _is_missing_folder_invite_link() -> bool:
    link = getattr(config, "FOLDER_INVITE_LINK", "")
    return (
        not isinstance(link, str)
        or not link.strip()
        or "xxxxxxxxxx" in link
    )


def _is_placeholder_destination(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {
        "@mychannel",
        "mychannel",
        "@yourchannel",
        "yourchannel",
        "@destination",
        "destination",
    }


def _looks_like_bot_token(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{6,}:[A-Za-z0-9_-]{20,}", value.strip()))


def _bot_destination_token_from_config() -> str:
    token = str(getattr(config, "BOT_DESTINATION_TOKEN", "") or "").strip()
    if token:
        return token
    destination = str(getattr(config, "DESTINATION", "") or "").strip()
    if _looks_like_bot_token(destination):
        return destination
    return ""


def _is_bot_destination_mode() -> bool:
    return bool(_bot_destination_token_from_config())


def _is_missing_bot_destination_token() -> bool:
    token = _bot_destination_token_from_config()
    return not token or not _looks_like_bot_token(token)


def _is_missing_bot_destination_chat_id() -> bool:
    chat_id = getattr(config, "BOT_DESTINATION_CHAT_ID", "")
    return not str(chat_id).strip()


def _looks_like_addlist_link(value: str) -> bool:
    normalized = value.strip()
    if not normalized:
        return False
    if normalized.startswith("https://") or normalized.startswith("http://"):
        parsed = urllib.parse.urlparse(normalized)
        path = parsed.path.strip("/")
    else:
        cleaned = normalized
        if cleaned.startswith("t.me/"):
            cleaned = cleaned[len("t.me/") :]
        elif cleaned.startswith("telegram.me/"):
            cleaned = cleaned[len("telegram.me/") :]
        path = cleaned.strip("/")
    return path.startswith("addlist/")


def _normalize_addlist_link(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        return normalized
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    if normalized.startswith("t.me/") or normalized.startswith("telegram.me/"):
        return f"https://{normalized}"
    return normalized


def _manual_source_entries() -> List[str]:
    combined: List[str] = []
    for key in ("EXTRA_SOURCES", "SOURCES"):
        value = getattr(config, key, [])
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, str) and item.strip():
                combined.append(item.strip())

    # preserve order and dedupe
    seen = set()
    unique = []
    for item in combined:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def _has_manual_sources() -> bool:
    return len(_manual_source_entries()) > 0


def _is_missing_destination() -> bool:
    if _is_bot_destination_mode():
        return False
    return (
        not isinstance(config.DESTINATION, str)
        or not config.DESTINATION.strip()
        or _is_placeholder_destination(config.DESTINATION)
        or _looks_like_bot_token(config.DESTINATION)
        or _looks_like_addlist_link(config.DESTINATION)
    )


def _is_digest_mode_enabled() -> bool:
    raw = getattr(config, "DIGEST_MODE", True)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return bool(raw)


def _digest_interval_seconds() -> int:
    raw = getattr(config, "DIGEST_INTERVAL_MINUTES", 30)
    try:
        minutes = int(raw)
    except Exception:
        minutes = 30
    minutes = max(1, min(minutes, 24 * 60))
    return minutes * 60


def _digest_daily_times() -> List[Tuple[int, int]]:
    raw = getattr(config, "DIGEST_DAILY_TIMES", [])
    if not isinstance(raw, list):
        return []
    values = [str(x) for x in raw]
    return parse_daily_times(values)


def _digest_max_posts() -> int:
    raw = getattr(config, "DIGEST_MAX_POSTS", 80)
    try:
        value = int(raw)
    except Exception:
        value = 80
    return max(1, min(value, 500))


def _digest_send_delay_seconds() -> float:
    raw = getattr(config, "DIGEST_SEND_DELAY_SECONDS", 0.8)
    try:
        value = float(raw)
    except Exception:
        value = 0.8
    return max(0.0, min(value, 5.0))


def _digest_send_chunk_size() -> int:
    raw = getattr(config, "DIGEST_SEND_CHUNK_SIZE", 3600)
    try:
        value = int(raw)
    except Exception:
        value = 3600
    return max(1000, min(value, 3900))


def _digest_retry_base_seconds() -> int:
    raw = getattr(config, "DIGEST_RETRY_BASE_SECONDS", 30)
    try:
        value = int(raw)
    except Exception:
        value = 30
    return max(5, min(value, 600))


def _digest_retry_max_seconds() -> int:
    raw = getattr(config, "DIGEST_RETRY_MAX_SECONDS", 900)
    try:
        value = int(raw)
    except Exception:
        value = 900
    return max(30, min(value, 3600))


def _digest_429_threshold_per_hour() -> int:
    raw = getattr(config, "DIGEST_429_THRESHOLD_PER_HOUR", 3)
    try:
        value = int(raw)
    except Exception:
        value = 3
    return max(1, min(value, 50))


def _bool_flag(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _is_dupe_detection_enabled() -> bool:
    raw = getattr(config, "DUPE_ENABLED", getattr(config, "ENABLE_DUPE_DETECTION", True))
    return _bool_flag(raw, True)


def _dupe_threshold() -> float:
    raw = getattr(config, "DUPE_THRESHOLD", 0.87)
    try:
        value = float(raw)
    except Exception:
        value = 0.87
    return min(max(value, 0.5), 0.99)


def _dupe_cache_size() -> int:
    raw = getattr(config, "DUPE_CACHE_SIZE", 400)
    try:
        value = int(raw)
    except Exception:
        value = 400
    return max(50, min(value, 5000))


def _dupe_history_hours() -> int:
    raw = getattr(config, "DUPE_HISTORY_HOURS", 4)
    try:
        value = int(raw)
    except Exception:
        value = 4
    return max(1, min(value, 24))


def _dupe_merge_instead_of_skip() -> bool:
    raw = getattr(config, "DUPE_MERGE_INSTEAD_OF_SKIP", True)
    return _bool_flag(raw, True)


def _is_severity_routing_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_SEVERITY_ROUTING", True), True)


def _is_immediate_high_enabled() -> bool:
    return _bool_flag(getattr(config, "IMMEDIATE_HIGH", True), True)


def _include_source_tags() -> bool:
    return _bool_flag(getattr(config, "INCLUDE_SOURCE_TAGS", False), False)


def _is_query_mode_enabled() -> bool:
    return _bool_flag(getattr(config, "QUERY_MODE_ENABLED", True), True)


def _query_max_messages() -> int:
    raw = getattr(config, "QUERY_MAX_MESSAGES", 50)
    try:
        value = int(raw)
    except Exception:
        value = 50
    return max(20, min(value, 60))


def _query_default_hours_back() -> int:
    raw = getattr(config, "QUERY_DEFAULT_HOURS_BACK", 24)
    try:
        value = int(raw)
    except Exception:
        value = 24
    return max(1, min(value, 24 * 30))


async def _resolve_query_bot_user_id() -> int | None:
    global query_allowed_bot_user_id, query_bot_user_id_checked
    if query_bot_user_id_checked:
        return query_allowed_bot_user_id

    query_bot_user_id_checked = True
    token = _bot_destination_token_from_config()
    if not token or not _looks_like_bot_token(token):
        query_allowed_bot_user_id = None
        return None

    try:
        async with httpx.AsyncClient(timeout=15) as http:
            response = await http.get(f"https://api.telegram.org/bot{token}/getMe")
        if response.status_code != 200:
            query_allowed_bot_user_id = None
            return None

        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("ok"):
            query_allowed_bot_user_id = None
            return None

        result = payload.get("result")
        if not isinstance(result, dict):
            query_allowed_bot_user_id = None
            return None

        bot_id = int(result.get("id") or 0)
        query_allowed_bot_user_id = bot_id if bot_id > 0 else None
        return query_allowed_bot_user_id
    except Exception:
        LOGGER.debug("Unable to resolve own bot identity for query filtering.", exc_info=True)
        query_allowed_bot_user_id = None
        return None


def _is_streaming_enabled() -> bool:
    return _bool_flag(getattr(config, "STREAMING_ENABLED", True), True)


def _stream_edit_interval_ms() -> int:
    raw = getattr(config, "STREAM_EDIT_INTERVAL_MS", 400)
    try:
        value = int(raw)
    except Exception:
        value = 400
    return max(200, min(value, 2000))


def _stream_max_chars_per_edit() -> int:
    raw = getattr(config, "STREAM_MAX_CHARS_PER_EDIT", 120)
    try:
        value = int(raw)
    except Exception:
        value = 120
    return max(60, min(value, 800))


def _stream_typing_action_enabled() -> bool:
    return _bool_flag(getattr(config, "STREAM_TYPING_ACTION", True), True)


def _breaking_keywords() -> List[str]:
    raw = getattr(config, "BREAKING_NEWS_KEYWORDS", [])
    if not isinstance(raw, list):
        return []
    result: List[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        normalized = item.strip().lower()
        if normalized:
            result.append(normalized)
    return result


def _breaking_match_threshold() -> int:
    raw = getattr(config, "BREAKING_MATCH_THRESHOLD", 1)
    try:
        value = int(raw)
    except Exception:
        value = 1
    return max(1, min(value, 10))


def _contains_breaking_keyword(text: str) -> bool:
    keywords = _breaking_keywords()
    if not keywords:
        return False
    lowered = text.lower()
    hits = sum(1 for keyword in keywords if keyword in lowered)
    return hits >= _breaking_match_threshold()


def _digest_status_command() -> str:
    raw = str(getattr(config, "DIGEST_STATUS_COMMAND", "/digest_status") or "").strip()
    return raw or "/digest_status"


def _prompt_non_empty(prompt_text: str) -> str:
    while True:
        value = input(prompt_text).strip()
        if value:
            return value
        print("Value cannot be empty. Try again.")


def _prompt_positive_int(prompt_text: str) -> int:
    while True:
        raw = input(prompt_text).strip()
        try:
            val = int(raw)
        except ValueError:
            print("Please enter a valid integer.")
            continue
        if val > 0:
            return val
        print("Value must be greater than 0.")


def _prompt_phone_number() -> str:
    while True:
        phone = input("Telegram phone number (E.164, e.g. +15551234567): ").strip()
        if not phone:
            print("Phone number is required.")
            continue
        if ":" in phone:
            print("Bot token is not allowed. Enter your personal phone number.")
            continue
        if not re.fullmatch(r"\+?[0-9]{5,20}", phone):
            print("Invalid phone format. Use digits with optional leading '+'.")
            continue
        return phone


def _prompt_destination() -> str:
    while True:
        value = _prompt_non_empty(
            "DESTINATION (@channel, chat invite link, 'me', peer ID, or bot token): "
        )
        if _looks_like_bot_token(value):
            config.BOT_DESTINATION_TOKEN = value.strip()
            print(
                "Detected bot token. Switching to bot destination mode. "
                "You will be asked for BOT_DESTINATION_CHAT_ID."
            )
            return ""
        if _looks_like_addlist_link(value):
            normalized_folder_link = _normalize_addlist_link(value)
            if _is_missing_folder_invite_link():
                config.FOLDER_INVITE_LINK = normalized_folder_link
                _persist_config_updates({"FOLDER_INVITE_LINK": config.FOLDER_INVITE_LINK})
                print(
                    "Detected folder addlist link and saved it as FOLDER_INVITE_LINK in .env. "
                    "Now enter one destination chat/channel."
                )
            else:
                print(
                    "Destination cannot be a folder addlist link. "
                    "Use one destination chat/channel, e.g. @my_private_channel or me."
                )
            continue
        return value


def _prompt_bot_destination_token() -> str:
    while True:
        value = _prompt_non_empty("BOT_DESTINATION_TOKEN: ")
        if not _looks_like_bot_token(value):
            print("Invalid bot token format. Expected <digits>:<token>.")
            continue
        return value


def _prompt_bot_destination_chat_id() -> str:
    while True:
        value = _prompt_non_empty("BOT_DESTINATION_CHAT_ID (chat id or @channel): ")
        if _looks_like_bot_token(value):
            print("This looks like a token, not a chat ID. Enter chat ID or @channel.")
            continue
        return value


def _prompt_destination_mode() -> str:
    while True:
        raw = input(
            "Destination mode [1=chat/channel, 2=bot token+chat id] (default 1): "
        ).strip()
        if raw in {"", "1", "chat", "channel", "telethon"}:
            return "chat"
        if raw in {"2", "bot", "botapi"}:
            return "bot"
        print("Invalid choice. Enter 1 or 2.")


def _prompt_sources() -> List[str]:
    while True:
        raw = input(
            "Manual sources (comma-separated usernames/links, e.g. @ch1,https://t.me/+abc...): "
        ).strip()
        parts = [x.strip() for x in raw.split(",") if x.strip()]
        if parts:
            return parts
        print("Enter at least one source.")


def _persist_config_updates(updates: Dict[str, object]) -> None:
    if not updates:
        return

    def _serialize_env_value(value: object) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            return json.dumps([str(item) for item in value], ensure_ascii=False)
        return str(value)

    def _format_env_line(key: str, value: object) -> str:
        serialized = _serialize_env_value(value)
        if not serialized:
            return f'{key}=""'
        if re.fullmatch(r"[A-Za-z0-9_./:+@-]+", serialized):
            return f"{key}={serialized}"
        return f"{key}={json.dumps(serialized, ensure_ascii=False)}"

    existing: Dict[str, str] = {}
    if ENV_PATH.exists():
        for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key:
                existing[key] = value.strip()

    for key, value in updates.items():
        existing[key] = _format_env_line(key, value).split("=", 1)[1]

    lines = [
        "# Auto-generated by TeleUserBot prompts. Override with exported env vars if needed.",
    ]
    for key in sorted(existing.keys()):
        lines.append(f"{key}={existing[key]}")
    lines.append("")

    tmp_path = ENV_PATH.with_suffix(".tmp")
    tmp_path.write_text("\n".join(lines), encoding="utf-8")
    os.replace(tmp_path, ENV_PATH)


def _prompt_for_missing_config() -> None:
    updates_to_persist: Dict[str, object] = {}

    # Auto-recover from common mistake: bot token pasted into DESTINATION.
    raw_destination = getattr(config, "DESTINATION", "")
    if isinstance(raw_destination, str) and _looks_like_bot_token(raw_destination):
        if not str(getattr(config, "BOT_DESTINATION_TOKEN", "")).strip():
            config.BOT_DESTINATION_TOKEN = raw_destination.strip()
            updates_to_persist["BOT_DESTINATION_TOKEN"] = config.BOT_DESTINATION_TOKEN
        config.DESTINATION = ""
        updates_to_persist["DESTINATION"] = config.DESTINATION
        print(
            "Detected bot token in DESTINATION. "
            "Moved it to BOT_DESTINATION_TOKEN and switched to bot destination mode."
        )

    # Auto-recover from common mistake: folder addlist pasted into DESTINATION.
    if isinstance(getattr(config, "DESTINATION", None), str) and _looks_like_addlist_link(
        config.DESTINATION
    ):
        if _is_missing_folder_invite_link():
            config.FOLDER_INVITE_LINK = _normalize_addlist_link(config.DESTINATION)
            updates_to_persist["FOLDER_INVITE_LINK"] = config.FOLDER_INVITE_LINK
        config.DESTINATION = ""
        updates_to_persist["DESTINATION"] = config.DESTINATION
        print(
            "Detected addlist folder link in DESTINATION. "
            "Moved it to FOLDER_INVITE_LINK and cleared DESTINATION."
        )

    missing = []
    if _is_missing_telegram_api_id():
        missing.append("TELEGRAM_API_ID")
    if _is_missing_telegram_api_hash():
        missing.append("TELEGRAM_API_HASH")
    if _is_bot_destination_mode():
        if _is_missing_bot_destination_token():
            missing.append("BOT_DESTINATION_TOKEN")
        if _is_missing_bot_destination_chat_id():
            missing.append("BOT_DESTINATION_CHAT_ID")
    elif _is_missing_destination():
        missing.append("DESTINATION")

    if missing:
        print("\nConfig missing values:", ", ".join(missing))
        if _is_missing_telegram_api_id():
            config.TELEGRAM_API_ID = _prompt_positive_int("TELEGRAM_API_ID: ")
            updates_to_persist["TELEGRAM_API_ID"] = config.TELEGRAM_API_ID
        if _is_missing_telegram_api_hash():
            config.TELEGRAM_API_HASH = _prompt_non_empty("TELEGRAM_API_HASH: ")
            updates_to_persist["TELEGRAM_API_HASH"] = config.TELEGRAM_API_HASH
        if _is_bot_destination_mode() and _is_missing_bot_destination_token():
            config.BOT_DESTINATION_TOKEN = _prompt_bot_destination_token()
            updates_to_persist["BOT_DESTINATION_TOKEN"] = config.BOT_DESTINATION_TOKEN
        if _is_bot_destination_mode() and _is_missing_bot_destination_chat_id():
            config.BOT_DESTINATION_CHAT_ID = _prompt_bot_destination_chat_id()
            updates_to_persist["BOT_DESTINATION_CHAT_ID"] = config.BOT_DESTINATION_CHAT_ID
        if not _is_bot_destination_mode() and _is_missing_destination():
            mode = _prompt_destination_mode()
            if mode == "bot":
                config.BOT_DESTINATION_TOKEN = _prompt_bot_destination_token()
                config.BOT_DESTINATION_CHAT_ID = _prompt_bot_destination_chat_id()
                config.DESTINATION = ""
                updates_to_persist["BOT_DESTINATION_TOKEN"] = config.BOT_DESTINATION_TOKEN
                updates_to_persist["BOT_DESTINATION_CHAT_ID"] = (
                    config.BOT_DESTINATION_CHAT_ID
                )
                updates_to_persist["DESTINATION"] = config.DESTINATION
            else:
                config.DESTINATION = _prompt_destination()
                updates_to_persist["DESTINATION"] = config.DESTINATION
                if _is_bot_destination_mode():
                    updates_to_persist["BOT_DESTINATION_TOKEN"] = str(
                        getattr(config, "BOT_DESTINATION_TOKEN", "")
                    )
                    if _is_missing_bot_destination_chat_id():
                        config.BOT_DESTINATION_CHAT_ID = _prompt_bot_destination_chat_id()
                        updates_to_persist["BOT_DESTINATION_CHAT_ID"] = (
                            config.BOT_DESTINATION_CHAT_ID
                        )

    if _is_missing_folder_invite_link() and not _has_manual_sources():
        folder = input(
            "FOLDER_INVITE_LINK (leave empty to use manual sources only): "
        ).strip()
        if folder:
            config.FOLDER_INVITE_LINK = folder
            updates_to_persist["FOLDER_INVITE_LINK"] = config.FOLDER_INVITE_LINK
        else:
            config.EXTRA_SOURCES = _prompt_sources()
            updates_to_persist["EXTRA_SOURCES"] = config.EXTRA_SOURCES

    _persist_config_updates(updates_to_persist)


def _validate_config() -> None:
    if _is_missing_telegram_api_id():
        raise ValueError("Set TELEGRAM_API_ID in environment or .env")
    if _is_missing_telegram_api_hash():
        raise ValueError("Set TELEGRAM_API_HASH in environment or .env")
    if _is_bot_destination_mode():
        if _is_missing_bot_destination_token():
            raise ValueError("Set BOT_DESTINATION_TOKEN in environment or .env")
        if _is_missing_bot_destination_chat_id():
            raise ValueError("Set BOT_DESTINATION_CHAT_ID in environment or .env")
    elif _is_missing_destination():
        raise ValueError("Set DESTINATION in environment or .env")
    if _is_missing_folder_invite_link() and not _has_manual_sources():
        raise ValueError(
            "Set FOLDER_INVITE_LINK or EXTRA_SOURCES/SOURCES in environment or .env"
        )


def _require_client() -> TelegramClient:
    if client is None:
        raise RuntimeError("Telegram client not initialized.")
    return client


def _require_auth_manager() -> AuthManager:
    if auth_manager is None:
        raise RuntimeError("Auth manager not initialized.")
    return auth_manager


def _require_destination_peer():
    if destination_peer is None:
        raise RuntimeError("Destination peer not initialized.")
    return destination_peer


def _require_bot_destination_token() -> str:
    if not bot_destination_token:
        raise RuntimeError("Bot destination token not initialized.")
    return bot_destination_token


def _require_bot_destination_chat_id() -> str:
    if not bot_destination_chat_id:
        raise RuntimeError("Bot destination chat ID not initialized.")
    return bot_destination_chat_id


def _destination_uses_bot_api() -> bool:
    return bool(bot_destination_token and bot_destination_chat_id)


async def _bot_api_request(
    method: str,
    *,
    data: Dict[str, str] | None = None,
    files: Dict[str, tuple[str, bytes, str]] | None = None,
) -> object:
    token = _require_bot_destination_token()
    url = f"https://api.telegram.org/bot{token}/{method}"

    async with httpx.AsyncClient(timeout=60) as http:
        for _ in range(4):
            response = await http.post(url, data=data, files=files)
            payload = {}
            try:
                payload = response.json()
            except Exception:
                payload = {}

            if response.status_code == 429 or (
                isinstance(payload, dict) and payload.get("error_code") == 429
            ):
                retry_after = int(
                    (payload.get("parameters") or {}).get("retry_after", 2)  # type: ignore[union-attr]
                )
                await asyncio.sleep(retry_after + 1)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"Bot API {method} failed: HTTP {response.status_code} {response.text}"
                )

            if not isinstance(payload, dict):
                raise RuntimeError(f"Bot API {method} returned invalid response payload.")
            if not payload.get("ok"):
                raise RuntimeError(f"Bot API {method} error: {payload}")
            return payload.get("result")

    raise RuntimeError(f"Bot API {method} failed after retries.")


def _infer_chat_id_from_updates(updates: object) -> str | None:
    if not isinstance(updates, list):
        return None
    for update in reversed(updates):
        if not isinstance(update, dict):
            continue
        for key in (
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
            "my_chat_member",
            "chat_member",
        ):
            obj = update.get(key)
            if not isinstance(obj, dict):
                continue
            chat = obj.get("chat")
            if isinstance(chat, dict) and "id" in chat:
                return str(chat["id"])
    return None


def _media_type_for_bot(msg: Message) -> str:
    if msg.photo:
        return "photo"
    if msg.video:
        return "video"
    return "document"


def _filename_for_bot_media(msg: Message, index: int) -> str:
    if msg.photo:
        return f"photo_{msg.id or index}.jpg"
    if msg.video:
        return f"video_{msg.id or index}.mp4"
    if msg.document and getattr(msg.document, "attributes", None):
        for attr in msg.document.attributes:
            if isinstance(attr, types.DocumentAttributeFilename) and attr.file_name:
                return attr.file_name
    return f"file_{msg.id or index}.bin"


def _format_summary_text(source_title: str, summary: str) -> str:
    if _include_source_tags():
        return f"📰 **{source_title}**\n\n{summary.strip()}"
    return f"📰 **Update**\n\n{summary.strip()}"


def _format_source_label(source_title: str) -> str:
    if _include_source_tags():
        return f"📰 **{source_title}**"
    return "📰 **Update**"


def _to_bot_text(text: str | None, max_len: int | None = None) -> str | None:
    if text is None:
        return None
    cleaned = text.strip()
    if max_len is not None and len(cleaned) > max_len:
        return cleaned[: max_len - 3].rstrip() + "..."
    return cleaned


def _extract_addlist_hash(folder_invite_link: str) -> str:
    parsed = urllib.parse.urlparse(folder_invite_link.strip())
    path = parsed.path.strip("/")
    if path.startswith("addlist/"):
        invite_hash = path.split("/", 1)[1]
    else:
        invite_hash = path.split("/")[-1]
    if not invite_hash:
        raise ValueError("Invalid FOLDER_INVITE_LINK format.")
    return invite_hash


def _extract_private_invite_hash(source: str) -> str | None:
    normalized = source.strip()
    if normalized.startswith("https://") or normalized.startswith("http://"):
        parsed = urllib.parse.urlparse(normalized)
        path = parsed.path.strip("/")
    else:
        path = normalized.strip("/")

    if path.startswith("+"):
        return path[1:] or None
    if path.startswith("joinchat/"):
        return path.split("/", 1)[1] or None
    return None


def _extract_public_username(source: str) -> str | None:
    normalized = source.strip()
    if not normalized:
        return None
    if _looks_like_bot_token(normalized):
        return None

    if normalized.startswith("@"):
        return normalized

    if normalized.startswith("https://") or normalized.startswith("http://"):
        parsed = urllib.parse.urlparse(normalized)
        path = parsed.path.strip("/")
        if not path:
            return None
        if path.startswith("addlist/") or path.startswith("+") or path.startswith("joinchat/"):
            return None
        return path.split("/", 1)[0]

    if "/" not in normalized and " " not in normalized:
        return normalized
    return None


async def _call_with_floodwait(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as exc:
            wait_seconds = int(exc.seconds) + 1
            LOGGER.error("FloodWaitError: sleeping %s second(s).", wait_seconds)
            await asyncio.sleep(wait_seconds)


async def _source_title(msg: Message) -> str:
    chat = await msg.get_chat()
    title = getattr(chat, "title", None)
    if title:
        return title
    username = getattr(chat, "username", None)
    if username:
        return username
    return str(msg.chat_id)


async def _source_info(msg: Message) -> Tuple[str, str | None]:
    chat = await msg.get_chat()
    title = getattr(chat, "title", None)
    username = getattr(chat, "username", None)

    if isinstance(title, str) and title.strip():
        source = title.strip()
    elif isinstance(username, str) and username.strip():
        source = username.strip()
    else:
        source = str(msg.chat_id)

    link = build_telegram_message_link(
        username=username if isinstance(username, str) else None,
        chat_id=msg.chat_id,
        message_id=int(msg.id or 0),
    )
    return source, link


async def _source_title_from_channel_id(channel_id: str) -> str:
    cached = source_title_cache.get(channel_id)
    if cached:
        return cached

    tg = _require_client()
    candidates: List[object] = []
    if channel_id.lstrip("-").isdigit():
        candidates.append(int(channel_id))
    candidates.append(channel_id)

    title = channel_id
    for candidate in candidates:
        try:
            entity = await _call_with_floodwait(tg.get_entity, candidate)
            candidate_title = getattr(entity, "title", None)
            if isinstance(candidate_title, str) and candidate_title.strip():
                title = candidate_title.strip()
                break
            username = getattr(entity, "username", None)
            if isinstance(username, str) and username.strip():
                title = username.strip()
                break
        except Exception:
            continue

    source_title_cache[channel_id] = title
    return title


def _queue_for_digest(
    channel_id: str,
    message_id: int,
    raw_text: str,
    *,
    source_name: str | None = None,
    message_link: str | None = None,
) -> None:
    save_to_digest_queue(
        channel_id=channel_id,
        message_id=message_id,
        raw_text=raw_text,
        timestamp=int(time.time()),
        source_name=source_name,
        message_link=message_link,
    )


def _severity_emoji(level: str) -> str:
    normalized = (level or "").strip().lower()
    if normalized == "high":
        return "🔥"
    if normalized == "medium":
        return "⚠️"
    return "ℹ️"


def _format_breaking_text(source_title: str, headline: str) -> str:
    clean_headline = normalize_space(headline)
    if _include_source_tags():
        return f"🔥 **BREAKING • {source_title}**\n\n{clean_headline}"
    return f"🔥 **BREAKING**\n\n{clean_headline}"


def _cleanup_breaking_refs(now_ts: int | None = None) -> None:
    now = int(now_ts if now_ts is not None else time.time())
    cutoff = now - 1800
    stale = [key for key, value in breaking_delivery_refs.items() if int(value.get("ts", 0)) < cutoff]
    for key in stale:
        breaking_delivery_refs.pop(key, None)


def _register_breaking_delivery(
    *,
    text_hash: str,
    ref: object | None,
    base_text: str,
    primary_source: str,
) -> None:
    if not text_hash or ref is None:
        return
    now = int(time.time())
    _cleanup_breaking_refs(now)
    breaking_delivery_refs[text_hash] = {
        "ref": ref,
        "base_text": base_text.strip(),
        "primary_source": primary_source.strip(),
        "sources": {primary_source.strip()},
        "ts": now,
    }


async def _merge_duplicate_breaking(
    result: GlobalDuplicateResult,
    source_name: str,
) -> bool:
    if not _dupe_merge_instead_of_skip():
        return False
    if not result.matched_hash:
        return False

    now = int(time.time())
    _cleanup_breaking_refs(now)
    entry = breaking_delivery_refs.get(result.matched_hash)
    if not entry:
        return False

    base_text = str(entry.get("base_text") or "").strip()
    if not base_text:
        return False

    sources = entry.get("sources")
    if not isinstance(sources, set):
        sources = {str(entry.get("primary_source") or "").strip()}
    clean_source = source_name.strip()
    if not clean_source:
        clean_source = "another channel"
    if clean_source in sources:
        return True
    sources.add(clean_source)

    primary_source = str(entry.get("primary_source") or "").strip()
    extra_sources = [src for src in sorted(sources) if src and src != primary_source]
    if not extra_sources:
        entry["sources"] = sources
        entry["ts"] = now
        return True

    suffix = ", ".join(extra_sources[:6])
    merged_text = f"{base_text}\n\n_(also reported by {suffix})_"
    if len(merged_text) > 3900:
        merged_text = merged_text[:3897].rstrip() + "..."

    try:
        updated_ref = await _edit_sent_text(entry.get("ref"), merged_text)
    except Exception:
        LOGGER.debug("Failed to merge duplicate breaking edit.", exc_info=True)
        return False

    entry["ref"] = updated_ref
    entry["sources"] = sources
    entry["ts"] = now
    breaking_delivery_refs[result.matched_hash] = entry
    return True


async def _init_dupe_detector() -> None:
    global dupe_detector

    if not _is_dupe_detection_enabled():
        dupe_detector = None
        configure_duplicate_runtime(None)
        return

    try:
        detector = HybridDuplicateEngine(
            threshold=_dupe_threshold(),
            history_hours=_dupe_history_hours(),
            max_items=max(_dupe_cache_size() * 10, 2000),
            logger=LOGGER,
        )
        await asyncio.to_thread(
            detector.warm_start_from_db,
            warm_hours=min(2, _dupe_history_hours()),
        )
        await asyncio.to_thread(detector.purge_old_records)

        dupe_detector = detector
        configure_duplicate_runtime(
            DuplicateRuntime(
                engine=detector,
                logger=LOGGER,
                merge_instead_of_skip=_dupe_merge_instead_of_skip(),
                source_resolver=_source_title,
                merge_callback=_merge_duplicate_breaking,
            )
        )
        log_structured(
            LOGGER,
            "dupe_detector_ready",
            enabled=True,
            threshold=_dupe_threshold(),
            cache_size=detector.cache_size,
            history_hours=_dupe_history_hours(),
            warm_hours=min(2, _dupe_history_hours()),
            merge_instead_of_skip=_dupe_merge_instead_of_skip(),
            backend=detector.backend_name,
        )
    except Exception:
        dupe_detector = None
        configure_duplicate_runtime(None)
        LOGGER.exception("Failed to initialize near-duplicate detector. Disabling dedupe.")


async def _send_text(text: str) -> None:
    await _send_text_with_ref(text)


def _message_ref_id(ref: object) -> int | None:
    if isinstance(ref, Message):
        return int(ref.id or 0) or None
    if isinstance(ref, dict):
        try:
            value = int(ref.get("message_id", 0))
            return value or None
        except Exception:
            return None
    try:
        value = int(getattr(ref, "id", 0) or 0)
        return value or None
    except Exception:
        return None


async def _send_text_with_ref(text: str, reply_to: int | None = None) -> object:
    if _destination_uses_bot_api():
        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "text": _to_bot_text(text) or "",
            "disable_web_page_preview": "true",
            "parse_mode": "Markdown",
        }
        if reply_to:
            data["reply_to_message_id"] = str(reply_to)
        try:
            result = await _bot_api_request("sendMessage", data=data)
            return result
        except Exception:
            # Markdown can fail on malformed AI output; fallback to plain text.
            data.pop("parse_mode", None)
            return await _bot_api_request("sendMessage", data=data)

    tg = _require_client()
    sent = await _call_with_floodwait(
        tg.send_message,
        _require_destination_peer(),
        text,
        reply_to=reply_to,
        parse_mode="md",
        link_preview=False,
    )
    return sent


async def _edit_sent_text(ref: object, text: str) -> object:
    if _destination_uses_bot_api():
        message_id = _message_ref_id(ref)
        if not message_id:
            raise RuntimeError("Cannot edit Bot API message without message_id.")

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "message_id": str(message_id),
            "text": _to_bot_text(text) or "",
            "disable_web_page_preview": "true",
            "parse_mode": "Markdown",
        }
        try:
            result = await _bot_api_request("editMessageText", data=data)
            # Bot API can return `True` for some cases; keep original ref for continuity.
            if isinstance(result, dict):
                return result
            return ref
        except Exception:
            data.pop("parse_mode", None)
            result = await _bot_api_request("editMessageText", data=data)
            if isinstance(result, dict):
                return result
            return ref

    if not isinstance(ref, Message):
        raise RuntimeError("Telethon edit requires Message reference.")
    try:
        return await ref.edit(text, parse_mode="md", link_preview=False)
    except Exception:
        return await ref.edit(text, parse_mode=None, link_preview=False)


async def _send_single_media(msg: Message, caption: str | None) -> object:
    if _destination_uses_bot_api():
        raw = await msg.download_media(file=bytes)
        if raw is None:
            raise RuntimeError("Failed to download media for bot destination.")

        media_type = _media_type_for_bot(msg)
        method_map = {
            "photo": "sendPhoto",
            "video": "sendVideo",
            "document": "sendDocument",
        }
        upload_field = {
            "photo": "photo",
            "video": "video",
            "document": "document",
        }[media_type]
        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "caption": _to_bot_text(caption, max_len=1024) or "",
            "parse_mode": "Markdown",
        }
        files = {
            upload_field: (
                _filename_for_bot_media(msg, 0),
                raw,
                "application/octet-stream",
            )
        }
        try:
            return await _bot_api_request(method_map[media_type], data=data, files=files)
        except Exception:
            data.pop("parse_mode", None)
            return await _bot_api_request(method_map[media_type], data=data, files=files)

    tg = _require_client()
    try:
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            msg.media,
            caption=caption,
            parse_mode="md",
        )
    except ChatForwardsRestrictedError:
        raw = await msg.download_media(file=bytes)
        if raw is None:
            raise RuntimeError("Failed to download restricted media for re-send.")
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            raw,
            caption=caption,
            parse_mode="md",
        )


async def _send_album(messages: List[Message], caption: str | None) -> None:
    if _destination_uses_bot_api():
        if not messages:
            if caption:
                await _send_text(caption)
            return

        if len(messages) == 1:
            await _send_single_media(messages[0], caption)
            return

        media_entries: List[dict] = []
        files: Dict[str, tuple[str, bytes, str]] = {}
        downloaded_messages: List[Message] = []
        for idx, msg in enumerate(messages):
            raw = await msg.download_media(file=bytes)
            if raw is None:
                continue

            media_type = _media_type_for_bot(msg)
            field_name = f"file{idx}"
            downloaded_messages.append(msg)
            files[field_name] = (
                _filename_for_bot_media(msg, idx),
                raw,
                "application/octet-stream",
            )
            item = {"type": media_type, "media": f"attach://{field_name}"}
            if idx == 0 and caption:
                item["caption"] = _to_bot_text(caption, max_len=1024) or ""
            media_entries.append(item)

        if not media_entries:
            raise RuntimeError("Failed to download album media for bot destination.")

        if len(media_entries) == 1:
            only_msg = downloaded_messages[0]
            await _send_single_media(only_msg, caption)
            return

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "media": json.dumps(media_entries),
            "parse_mode": "Markdown",
        }
        try:
            await _bot_api_request("sendMediaGroup", data=data, files=files)
        except Exception:
            data.pop("parse_mode", None)
            await _bot_api_request("sendMediaGroup", data=data, files=files)
        return

    tg = _require_client()
    media_items = [m.media for m in messages if m.media]
    if not media_items:
        if caption:
            await _send_text(caption)
        return

    try:
        await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            media_items,
            caption=caption,
            parse_mode="md",
        )
    except ChatForwardsRestrictedError:
        downloaded = []
        for msg in messages:
            blob = await msg.download_media(file=bytes)
            if blob is not None:
                downloaded.append(blob)
        if not downloaded:
            raise RuntimeError("Failed to download album media for restricted chat.")
        await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            downloaded,
            caption=caption,
            parse_mode="md",
        )


async def _process_single_message(msg: Message) -> None:
    channel_id = str(msg.chat_id)
    if is_seen(channel_id, msg.id):
        return

    text = (msg.message or "").strip()
    source = await _source_title(msg)

    if msg.media and not text:
        await _send_single_media(msg, _format_source_label(source))
        mark_seen(channel_id, msg.id)
        return

    if msg.media and text:
        summary = await summarize_or_skip(text, _require_auth_manager())
        if summary is None:
            mark_seen(channel_id, msg.id)
            return
        await _send_single_media(msg, _format_summary_text(source, summary))
        mark_seen(channel_id, msg.id)
        return

    if not text:
        mark_seen(channel_id, msg.id)
        return

    summary = await summarize_or_skip(text, _require_auth_manager())
    if summary is None:
        mark_seen(channel_id, msg.id)
        return

    await _send_text(_format_summary_text(source, summary))
    mark_seen(channel_id, msg.id)


async def _process_album(messages: List[Message]) -> None:
    if not messages:
        return

    messages = sorted(messages, key=lambda m: m.id)
    channel_id = str(messages[0].chat_id)
    message_ids = [m.id for m in messages]

    if all(is_seen(channel_id, m_id) for m_id in message_ids):
        return

    source = await _source_title(messages[0])
    captions = [(m.message or "").strip() for m in messages if (m.message or "").strip()]
    combined_caption = "\n".join(captions).strip()

    if not combined_caption:
        await _send_album(messages, _format_source_label(source))
        mark_seen_many(channel_id, message_ids)
        return

    summary = await summarize_or_skip(combined_caption, _require_auth_manager())
    if summary is None:
        mark_seen_many(channel_id, message_ids)
        return

    await _send_album(messages, _format_summary_text(source, summary))
    mark_seen_many(channel_id, message_ids)


async def _queue_single_message_for_digest(msg: Message) -> None:
    channel_id = str(msg.chat_id)
    if is_seen(channel_id, msg.id):
        return

    text = (msg.message or "").strip()
    source, link = await _source_info(msg)

    # Keep the current behavior: media without caption should be forwarded immediately.
    if msg.media and not text:
        await _send_single_media(msg, _format_source_label(source))
        mark_seen(channel_id, msg.id)
        return

    if not text:
        mark_seen(channel_id, msg.id)
        return

    severity = "medium"
    if _is_severity_routing_enabled():
        severity = await classify_severity(text, _require_auth_manager())
    elif _contains_breaking_keyword(text):
        severity = "high"

    if severity == "high" and _is_immediate_high_enabled():
        _normalized, text_hash = build_dupe_fingerprint(text)
        headline = await summarize_breaking_headline(text, _require_auth_manager())
        if not headline:
            headline = normalize_space(text)
            if len(headline) > 140:
                headline = f"{headline[:137].rsplit(' ', 1)[0]}..."
        payload = _format_breaking_text(source, headline)
        if msg.media:
            await _send_single_media(msg, payload)
            sent_ref = None
        else:
            sent_ref = await _send_text_with_ref(payload)
            _register_breaking_delivery(
                text_hash=text_hash,
                ref=sent_ref,
                base_text=payload,
                primary_source=source,
            )
        mark_seen(channel_id, msg.id)
        log_structured(
            LOGGER,
            "breaking_sent_immediate",
            channel_id=channel_id,
            message_id=msg.id,
            source=source,
            severity=severity,
            dedupe_hash=text_hash,
        )
        return

    _queue_for_digest(
        channel_id,
        msg.id,
        text,
        source_name=source,
        message_link=link,
    )
    mark_seen(channel_id, msg.id)
    log_structured(
        LOGGER,
        "digest_item_queued",
        channel_id=channel_id,
        message_id=msg.id,
        source=source,
        severity=severity,
        severity_emoji=_severity_emoji(severity),
        has_link=bool(link),
        text_tokens=estimate_tokens_rough(text),
        pending=count_pending(),
    )


async def _queue_album_for_digest(messages: List[Message]) -> None:
    if not messages:
        return

    messages = sorted(messages, key=lambda m: m.id)
    channel_id = str(messages[0].chat_id)
    message_ids = [m.id for m in messages]

    if all(is_seen(channel_id, m_id) for m_id in message_ids):
        return

    source, link = await _source_info(messages[0])
    captions = [(m.message or "").strip() for m in messages if (m.message or "").strip()]
    combined_caption = "\n".join(captions).strip()

    # Keep current behavior: media-only albums are forwarded immediately with source label.
    if not combined_caption:
        await _send_album(messages, _format_source_label(source))
        mark_seen_many(channel_id, message_ids)
        return

    severity = "medium"
    if _is_severity_routing_enabled():
        severity = await classify_severity(combined_caption, _require_auth_manager())
    elif _contains_breaking_keyword(combined_caption):
        severity = "high"

    if severity == "high" and _is_immediate_high_enabled():
        headline = await summarize_breaking_headline(combined_caption, _require_auth_manager())
        if not headline:
            headline = normalize_space(combined_caption)
            if len(headline) > 140:
                headline = f"{headline[:137].rsplit(' ', 1)[0]}..."
        await _send_album(messages, _format_breaking_text(source, headline))
        mark_seen_many(channel_id, message_ids)
        log_structured(
            LOGGER,
            "breaking_album_sent_immediate",
            channel_id=channel_id,
            first_message_id=messages[0].id,
            album_size=len(messages),
            source=source,
            severity=severity,
        )
        return

    # Queue one digest item for the entire album (first message ID as queue key).
    _queue_for_digest(
        channel_id,
        messages[0].id,
        combined_caption,
        source_name=source,
        message_link=link,
    )
    mark_seen_many(channel_id, message_ids)
    log_structured(
        LOGGER,
        "digest_album_queued",
        channel_id=channel_id,
        first_message_id=messages[0].id,
        album_size=len(messages),
        source=source,
        severity=severity,
        severity_emoji=_severity_emoji(severity),
        has_link=bool(link),
        text_tokens=estimate_tokens_rough(combined_caption),
        pending=count_pending(),
    )


def _format_digest_message(
    digest_body: str,
    total_updates: int,
    sources: List[str],
) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = f"📰 **Daily Digest • {timestamp} • {total_updates} updates**"
    _ = sources
    return f"{header}\n\n{digest_body.strip()}"


def _effective_interval_seconds() -> int:
    base = _digest_interval_seconds()
    health = get_quota_health()
    threshold = _digest_429_threshold_per_hour()
    multiplier = 2 if int(health.get("recent_429_count", 0)) > threshold else 1
    return max(60, base * multiplier)


def _adaptive_batch_size() -> int:
    base = _digest_max_posts()
    health = get_quota_health()
    scale = float(health.get("batch_scale", 1.0))
    scaled = int(base * scale)
    return max(8, min(base, scaled))


async def _run_post_processors(text: str, context: Dict[str, object]) -> str:
    hooks = getattr(config, "DIGEST_POST_PROCESSORS", [])
    if not isinstance(hooks, list) or not hooks:
        return text

    current = text
    for path in hooks:
        try:
            target = str(path).strip()
            if not target or "." not in target:
                continue
            module_name, func_name = target.rsplit(".", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name, None)
            if func is None:
                continue
            maybe = func(current, context)
            if asyncio.iscoroutine(maybe):
                maybe = await maybe
            if isinstance(maybe, str) and maybe.strip():
                current = maybe.strip()
        except Exception:
            LOGGER.exception("Digest post-processor failed: %s", path)
    return current


async def _send_digest_message(text: str) -> None:
    chunks = split_markdown_chunks(text, max_chars=_digest_send_chunk_size())
    delay = _digest_send_delay_seconds()
    for idx, chunk in enumerate(chunks):
        await _send_text(chunk)
        if idx < len(chunks) - 1 and delay > 0:
            await asyncio.sleep(delay)


async def _flush_digest_queue_once() -> None:
    global digest_retry_backoff_seconds

    async with digest_loop_lock:
        batch_size = _adaptive_batch_size()
        batch_id, rows = claim_digest_batch(batch_size)
        if not rows:
            return

        pending_before = count_pending() + len(rows)
        source_names: List[str] = []
        posts: List[Dict[str, object]] = []
        for row in rows:
            source_name = str(row.get("source_name") or "").strip()
            channel_id = str(row["channel_id"])
            if not source_name:
                source_name = await _source_title_from_channel_id(channel_id)
            if source_name not in source_names:
                source_names.append(source_name)
            posts.append(
                {
                    "id": int(row["id"]),
                    "channel_id": channel_id,
                    "message_id": int(row["message_id"]),
                    "source_name": source_name,
                    "raw_text": str(row["raw_text"]),
                    "message_link": str(row.get("message_link") or ""),
                    "timestamp": int(row["timestamp"]),
                }
            )

        input_token_est = sum(estimate_tokens_rough(str(p["raw_text"])) for p in posts)
        log_structured(
            LOGGER,
            "digest_batch_claimed",
            pending_before=pending_before,
            claimed=len(posts),
            batch_size=batch_size,
            estimated_tokens=input_token_est,
            quota_health=get_quota_health(),
        )

        try:
            stream_stats = None
            if _is_streaming_enabled():
                streamer = LiveTelegramStreamer(
                    send_message=lambda text, reply_to: _send_text_with_ref(text, reply_to=reply_to),
                    edit_message=_edit_sent_text,
                    get_message_id=lambda ref: _message_ref_id(ref) or 0,
                    placeholder_text="Generating digest... ⏳",
                    edit_interval_ms=_stream_edit_interval_ms(),
                    max_chars_per_edit=_stream_max_chars_per_edit(),
                    typing_enabled=False,
                )
                await streamer.start()
                digest_body = await create_digest_summary(
                    posts,
                    _require_auth_manager(),
                    on_token=streamer.push,
                )
            else:
                digest_body = await create_digest_summary(posts, _require_auth_manager())

            if not digest_body.strip():
                digest_body = quiet_period_message(max(1, _digest_interval_seconds() // 60))

            digest_body = await _run_post_processors(
                digest_body,
                {
                    "batch_id": batch_id,
                    "posts": posts,
                    "pending_before": pending_before,
                },
            )

            digest_message = _format_digest_message(
                digest_body=digest_body,
                total_updates=len(posts),
                sources=source_names,
            )
            if _is_streaming_enabled():
                stream_stats = await streamer.finalize(digest_message)
            else:
                await _send_digest_message(digest_message)

            acked = ack_digest_batch(batch_id)
            set_last_digest_timestamp(int(time.time()))
            digest_retry_backoff_seconds = 0

            log_structured(
                LOGGER,
                "digest_sent",
                batch_id=batch_id,
                rows=len(posts),
                acked=acked,
                pending_after=count_pending(),
                response_chars=len(digest_body),
                stream_enabled=_is_streaming_enabled(),
                stream_edits=(stream_stats.edit_count if stream_stats else 0),
                stream_tokens_per_second=(
                    round(stream_stats.tokens_per_second, 3) if stream_stats else 0.0
                ),
            )
        except asyncio.CancelledError:
            restore_digest_batch(batch_id)
            raise
        except Exception as exc:
            restored = restore_digest_batch(batch_id)
            base = _digest_retry_base_seconds()
            max_backoff = _digest_retry_max_seconds()
            digest_retry_backoff_seconds = (
                base
                if digest_retry_backoff_seconds <= 0
                else min(max_backoff, digest_retry_backoff_seconds * 2)
            )
            log_structured(
                LOGGER,
                "digest_failed_restored",
                level=logging.ERROR,
                batch_id=batch_id,
                restored=restored,
                backoff_seconds=digest_retry_backoff_seconds,
                error=str(exc),
            )
            LOGGER.exception("Digest generation/sending failed.")


def _compute_next_scheduler_delay_seconds() -> int:
    if digest_retry_backoff_seconds > 0:
        return digest_retry_backoff_seconds

    daily_times = _digest_daily_times()
    if daily_times:
        return seconds_until_next_daily_time(daily_times)
    return _effective_interval_seconds()


async def run_digest_scheduler() -> None:
    global digest_next_run_ts

    log_structured(
        LOGGER,
        "digest_scheduler_start",
        mode="digest",
        pending=count_pending(),
        inflight=count_inflight(),
        interval_seconds=_digest_interval_seconds(),
        daily_times=[f"{h:02d}:{m:02d}" for h, m in _digest_daily_times()],
        quota_health=get_quota_health(),
    )

    try:
        while True:
            delay = _compute_next_scheduler_delay_seconds()
            digest_next_run_ts = time.time() + delay
            await asyncio.sleep(delay)
            await _flush_digest_queue_once()
    except asyncio.CancelledError:
        LOGGER.info("Digest scheduler stopped.")
        digest_next_run_ts = None
        return


async def _flush_album_after_wait(key: Tuple[str, int]) -> None:
    try:
        await asyncio.sleep(ALBUM_WAIT_SECONDS)
        items = album_buffers.pop(key, [])
        album_tasks.pop(key, None)
        if items:
            if _is_digest_mode_enabled():
                await _queue_album_for_digest(items)
            else:
                await _process_album(items)
    except asyncio.CancelledError:
        return
    except Exception:
        LOGGER.exception("Album processing failed for key=%s", key)


async def _resolve_entity_id(chat_obj) -> int | None:
    tg = _require_client()
    username = getattr(chat_obj, "username", None)
    if username:
        try:
            entity = await _call_with_floodwait(tg.get_entity, username)
            return utils.get_peer_id(entity)
        except Exception:
            pass

    chat_id = getattr(chat_obj, "id", None)
    if chat_id is not None:
        try:
            entity = await _call_with_floodwait(tg.get_entity, chat_id)
            return utils.get_peer_id(entity)
        except Exception:
            pass

    try:
        entity = await _call_with_floodwait(tg.get_entity, chat_obj)
        return utils.get_peer_id(entity)
    except Exception:
        return None


async def _interactive_user_login(tg: TelegramClient) -> None:
    """Authenticate this client as a personal user account only."""
    await tg.connect()
    if await tg.is_user_authorized():
        me = await _call_with_floodwait(tg.get_me)
        if getattr(me, "bot", False):
            raise RuntimeError(
                "Session is authenticated as bot. Remove session and login with phone number."
            )
        return

    phone = _prompt_phone_number()
    sent = await _call_with_floodwait(tg.send_code_request, phone)
    while True:
        code = _prompt_non_empty("Telegram login code: ")
        try:
            await _call_with_floodwait(
                tg.sign_in,
                phone=phone,
                code=code,
                phone_code_hash=sent.phone_code_hash,
            )
            break
        except SessionPasswordNeededError:
            password = _prompt_non_empty("Telegram 2FA password: ")
            await _call_with_floodwait(tg.sign_in, password=password)
            break
        except PhoneCodeInvalidError:
            print("Invalid login code. Try again.")
        except PhoneCodeExpiredError:
            print("Code expired. Requesting a new code...")
            sent = await _call_with_floodwait(tg.send_code_request, phone)

    me = await _call_with_floodwait(tg.get_me)
    if getattr(me, "bot", False):
        raise RuntimeError("Bot login detected. Login must be with personal phone number.")


async def _ensure_user_account_session() -> None:
    """Ensure current session is a user account, not a bot session."""
    global client

    tg = _require_client()
    await tg.connect()

    if not await tg.is_user_authorized():
        await _interactive_user_login(tg)
        return

    me = await _call_with_floodwait(tg.get_me)
    if not getattr(me, "bot", False):
        return

    LOGGER.warning(
        "Detected bot session in userbot.session. Resetting session for user login."
    )
    await _call_with_floodwait(tg.log_out)
    await tg.disconnect()

    client = TelegramClient("userbot", config.TELEGRAM_API_ID, config.TELEGRAM_API_HASH)
    await _interactive_user_login(client)
    me = await _call_with_floodwait(client.get_me)
    if getattr(me, "bot", False):
        raise RuntimeError(
            "Still logged in as bot. Delete userbot.session and sign in with phone number."
        )


async def _join_and_resolve_manual_source(source: str) -> int | None:
    tg = _require_client()

    private_hash = _extract_private_invite_hash(source)
    if private_hash:
        import_result = None
        try:
            import_result = await _call_with_floodwait(tg, ImportChatInviteRequest(hash=private_hash))
        except UserAlreadyParticipantError:
            pass
        except Exception:
            LOGGER.debug("ImportChatInviteRequest failed for source=%s", source, exc_info=True)

        if import_result is not None:
            for chat in getattr(import_result, "chats", []) or []:
                peer_id = await _resolve_entity_id(chat)
                if peer_id is not None:
                    return peer_id

        try:
            check = await _call_with_floodwait(tg, CheckChatInviteRequest(hash=private_hash))
            if isinstance(check, types.ChatInviteAlready):
                return utils.get_peer_id(check.chat)
        except Exception:
            pass

    username = _extract_public_username(source)
    if username:
        try:
            await _call_with_floodwait(tg, JoinChannelRequest(username))
        except UserAlreadyParticipantError:
            pass
        except Exception:
            LOGGER.debug("JoinChannelRequest failed for source=%s", source, exc_info=True)

        try:
            entity = await _call_with_floodwait(tg.get_entity, username)
            return utils.get_peer_id(entity)
        except Exception:
            LOGGER.debug("Failed resolving entity for source=%s", source, exc_info=True)
            return None

    return None


async def _resolve_destination_input_peer(raw_destination: str):
    tg = _require_client()
    value = raw_destination.strip()
    if not value:
        raise ValueError("Destination is empty.")
    if _looks_like_bot_token(value):
        raise ValueError("Destination cannot be a bot token.")
    if _looks_like_addlist_link(value):
        raise ValueError(
            "Destination cannot be a folder invite link (t.me/addlist/...). "
            "Use a single chat/channel destination."
        )

    private_hash = _extract_private_invite_hash(value)
    if private_hash:
        try:
            invite_result = await _call_with_floodwait(
                tg,
                ImportChatInviteRequest(hash=private_hash),
            )
            for chat in getattr(invite_result, "chats", []) or []:
                try:
                    return await _call_with_floodwait(tg.get_input_entity, chat)
                except Exception:
                    continue
        except UserAlreadyParticipantError:
            pass
        except Exception:
            LOGGER.debug("Destination private invite import failed.", exc_info=True)

        try:
            check = await _call_with_floodwait(tg, CheckChatInviteRequest(hash=private_hash))
            if isinstance(check, types.ChatInviteAlready):
                return await _call_with_floodwait(tg.get_input_entity, check.chat)
        except Exception:
            LOGGER.debug("Destination private invite resolve failed.", exc_info=True)

    candidates: List[object] = [value]
    username = _extract_public_username(value)
    if username:
        candidates.append(username)
        if isinstance(username, str) and not username.startswith("@"):
            candidates.append(f"@{username}")

    if value.lstrip("-").isdigit():
        candidates.append(int(value))

    seen = set()
    unique_candidates: List[object] = []
    for candidate in candidates:
        marker = repr(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        unique_candidates.append(candidate)

    for candidate in unique_candidates:
        try:
            return await _call_with_floodwait(tg.get_input_entity, candidate)
        except Exception:
            continue

    raise ValueError(f"Cannot resolve destination '{raw_destination}'.")


async def _ensure_destination_peer() -> None:
    global destination_peer, bot_destination_token, bot_destination_chat_id

    if _is_bot_destination_mode():
        while True:
            token = _bot_destination_token_from_config()
            chat_id = str(getattr(config, "BOT_DESTINATION_CHAT_ID", "") or "").strip()

            if not _looks_like_bot_token(token):
                config.BOT_DESTINATION_TOKEN = _prompt_bot_destination_token()
                _persist_config_updates({"BOT_DESTINATION_TOKEN": config.BOT_DESTINATION_TOKEN})
                continue

            if not chat_id:
                bot_destination_token = token
                try:
                    updates = await _bot_api_request("getUpdates", data={"limit": "20"})
                    inferred_chat_id = _infer_chat_id_from_updates(updates)
                except Exception:
                    inferred_chat_id = None

                if inferred_chat_id:
                    config.BOT_DESTINATION_CHAT_ID = inferred_chat_id
                    _persist_config_updates(
                        {"BOT_DESTINATION_CHAT_ID": config.BOT_DESTINATION_CHAT_ID}
                    )
                    LOGGER.info(
                        "Auto-detected BOT_DESTINATION_CHAT_ID=%s from getUpdates.",
                        inferred_chat_id,
                    )
                else:
                    print(
                        "BOT_DESTINATION_CHAT_ID not set and auto-detect failed. "
                        "Open chat with your bot, send /start, then enter chat ID."
                    )
                    config.BOT_DESTINATION_CHAT_ID = _prompt_bot_destination_chat_id()
                    _persist_config_updates(
                        {"BOT_DESTINATION_CHAT_ID": config.BOT_DESTINATION_CHAT_ID}
                    )
                continue

            bot_destination_token = token
            bot_destination_chat_id = chat_id
            destination_peer = None

            try:
                await _bot_api_request("getMe")
                await _bot_api_request(
                    "getChat",
                    data={"chat_id": _require_bot_destination_chat_id()},
                )
                LOGGER.info(
                    "Destination mode: Bot API (chat_id=%s).", _require_bot_destination_chat_id()
                )
                return
            except Exception as exc:
                LOGGER.error("Invalid bot destination config: %s", exc)
                print(
                    "Bot destination check failed. Ensure:\n"
                    "1) token is valid\n"
                    "2) BOT_DESTINATION_CHAT_ID is correct\n"
                    "3) you started the bot or added it to that chat/channel"
                )
                config.BOT_DESTINATION_TOKEN = _prompt_bot_destination_token()
                config.BOT_DESTINATION_CHAT_ID = _prompt_bot_destination_chat_id()
                _persist_config_updates(
                    {
                        "BOT_DESTINATION_TOKEN": config.BOT_DESTINATION_TOKEN,
                        "BOT_DESTINATION_CHAT_ID": config.BOT_DESTINATION_CHAT_ID,
                    }
                )
        return

    bot_destination_token = None
    bot_destination_chat_id = None
    while True:
        try:
            destination_peer = await _resolve_destination_input_peer(config.DESTINATION)
            return
        except Exception as exc:
            LOGGER.error("Invalid DESTINATION value '%s': %s", config.DESTINATION, exc)
            print(
                "Destination is invalid or inaccessible. "
                "Use @username, t.me link, private invite link, or peer ID."
            )
            config.DESTINATION = _prompt_destination()
            _persist_config_updates({"DESTINATION": config.DESTINATION})


async def setup_sources() -> List[int]:
    global monitored_source_chat_ids

    tg = _require_client()
    resolved_ids: List[int] = []

    folder_link = getattr(config, "FOLDER_INVITE_LINK", "").strip()
    if folder_link and not _is_missing_folder_invite_link():
        invite_hash = _extract_addlist_hash(folder_link)

        # Optional dry-run preview using messages.CheckChatInviteRequest.
        try:
            await _call_with_floodwait(tg, CheckChatInviteRequest(hash=invite_hash))
        except Exception:
            LOGGER.debug("CheckChatInviteRequest dry-run failed for addlist hash.", exc_info=True)

        preview_chats = []
        peers_for_join = []
        if hasattr(functions, "chatlists"):
            try:
                preview = await _call_with_floodwait(
                    tg,
                    functions.chatlists.CheckChatlistInviteRequest(slug=invite_hash),
                )
                preview_chats = list(getattr(preview, "chats", []) or [])
                if isinstance(preview, types.chatlists.ChatlistInvite):
                    peers_for_join = list(preview.peers)
                elif isinstance(preview, types.chatlists.ChatlistInviteAlready):
                    peers_for_join = list(preview.missing_peers)
            except BotMethodInvalidError:
                LOGGER.warning(
                    "Folder invite API unavailable for this session. "
                    "Continuing with manual sources."
                )
            except Exception:
                LOGGER.exception("Failed to inspect chat folder invite.")

        # Requested behavior: iterate chats and join each public/private source.
        for chat in preview_chats:
            username = getattr(chat, "username", None)
            try:
                if username:
                    await _call_with_floodwait(tg, JoinChannelRequest(username))
                else:
                    await _call_with_floodwait(tg, ImportChatInviteRequest(hash=invite_hash))
            except UserAlreadyParticipantError:
                pass
            except Exception:
                LOGGER.debug(
                    "Join failed for folder chat id=%s", getattr(chat, "id", None), exc_info=True
                )

        # Folder-native join path for missing peers.
        if peers_for_join and hasattr(functions, "chatlists"):
            input_peers = []
            for peer in peers_for_join:
                try:
                    input_peers.append(await _call_with_floodwait(tg.get_input_entity, peer))
                except Exception:
                    continue

            if input_peers:
                try:
                    await _call_with_floodwait(
                        tg,
                        functions.chatlists.JoinChatlistInviteRequest(
                            slug=invite_hash,
                            peers=input_peers,
                        ),
                    )
                except UserAlreadyParticipantError:
                    pass
                except BotMethodInvalidError:
                    LOGGER.warning(
                        "JoinChatlistInviteRequest unavailable for this session. "
                        "Continuing with already-available sources."
                    )
                except Exception:
                    LOGGER.debug("JoinChatlistInviteRequest failed.", exc_info=True)

        for chat in preview_chats:
            peer_id = await _resolve_entity_id(chat)
            if peer_id is not None and peer_id not in resolved_ids:
                resolved_ids.append(peer_id)

    # Additional manual sources (username/public/private links).
    for source in _manual_source_entries():
        peer_id = await _join_and_resolve_manual_source(source)
        if peer_id is not None and peer_id not in resolved_ids:
            resolved_ids.append(peer_id)

    config.SOURCES = resolved_ids
    monitored_source_chat_ids = list(resolved_ids)
    if folder_link and not _is_missing_folder_invite_link():
        print(f"✅ Listening to {len(resolved_ids)} channels from folder")
    return resolved_ids


def _digest_status_text() -> str:
    mode = "DIGEST" if _is_digest_mode_enabled() else "PER_POST"
    pending = count_pending()
    inflight = count_inflight()
    last_ts = get_last_digest_timestamp()
    next_eta = (
        format_eta(digest_next_run_ts - time.time()) if digest_next_run_ts is not None else "n/a"
    )
    quota = get_quota_health()
    threshold = _digest_429_threshold_per_hour()
    dupe_state = "on" if _is_dupe_detection_enabled() else "off"
    severity_state = "on" if _is_severity_routing_enabled() else "off"
    query_state = "on" if _is_query_mode_enabled() else "off"
    detector_backend = dupe_detector.backend_name if dupe_detector is not None else "disabled"

    return (
        f"🧠 **Digest Status**\n"
        f"- Mode: `{mode}`\n"
        f"- Pending queue: `{pending}`\n"
        f"- In-flight queue: `{inflight}`\n"
        f"- Last digest: `{format_ts(last_ts)}`\n"
        f"- Next run in: `{next_eta}`\n"
        f"- Interval base: `{_digest_interval_seconds() // 60}m`\n"
        f"- Dedupe: `{dupe_state}` ({detector_backend})\n"
        f"- Severity router: `{severity_state}`\n"
        f"- Query mode: `{query_state}`\n"
        f"- Quota health: `{quota.get('status', 'unknown')}`\n"
        f"- Recent 429 (1h): `{quota.get('recent_429_count', 0)}` / threshold `{threshold}`"
    )


async def _on_digest_status_command(event: events.NewMessage.Event) -> None:
    try:
        await event.reply(_digest_status_text(), parse_mode="md", link_preview=False)
    except Exception:
        LOGGER.exception("Failed sending /digest_status response.")


def _is_query_text_ignored(text: str) -> bool:
    if not text:
        return True
    if len(text) < 8:
        return True
    if text.startswith("/") or text.startswith("!"):
        return True
    return False


def _query_history_for_sender(sender_id: int) -> Deque[Dict[str, str]]:
    return query_conversation_history[sender_id]


def _append_query_history(sender_id: int, query_text: str, answer_text: str) -> None:
    history = _query_history_for_sender(sender_id)
    history.append({"role": "user", "content": normalize_space(query_text)})
    trimmed_answer = normalize_space(answer_text)
    if len(trimmed_answer) > 800:
        trimmed_answer = f"{trimmed_answer[:797].rsplit(' ', 1)[0]}..."
    history.append({"role": "assistant", "content": trimmed_answer})


async def _safe_reply_markdown(
    event: events.NewMessage.Event,
    text: str,
    *,
    edit_message: Message | None = None,
) -> Message | None:
    while True:
        try:
            if edit_message is not None:
                return await edit_message.edit(text, parse_mode="md", link_preview=False)
            return await event.reply(text, parse_mode="md", link_preview=False)
        except FloodWaitError as exc:
            wait_seconds = int(exc.seconds) + 1
            LOGGER.warning("FloodWait while replying to query: sleeping %ss", wait_seconds)
            await asyncio.sleep(wait_seconds)
        except Exception:
            try:
                if edit_message is not None:
                    return await edit_message.edit(text, parse_mode=None, link_preview=False)
                return await event.reply(text, parse_mode=None, link_preview=False)
            except FloodWaitError as exc:
                wait_seconds = int(exc.seconds) + 1
                LOGGER.warning("FloodWait while replying to query: sleeping %ss", wait_seconds)
                await asyncio.sleep(wait_seconds)
            except Exception:
                LOGGER.exception("Failed to send query response message.")
                return None


async def _send_query_typing(event: events.NewMessage.Event) -> None:
    if not _stream_typing_action_enabled():
        return
    tg = _require_client()
    try:
        peer = await event.get_input_chat()
        await _call_with_floodwait(
            tg,
            functions.messages.SetTypingRequest(
                peer=peer,
                action=types.SendMessageTypingAction(),
            ),
        )
    except Exception:
        LOGGER.debug("Query typing action failed.", exc_info=True)


async def _stream_query_answer(
    event: events.NewMessage.Event,
    *,
    progress_message: Message,
    query_text: str,
    results: List[Dict[str, object]],
    history: Sequence[Dict[str, str]],
) -> tuple[str, object]:
    async def _send_query_message(text: str, reply_to: int | None):
        return await event.reply(
            text,
            reply_to=reply_to,
            parse_mode="md",
            link_preview=False,
        )

    async def _edit_query_message(ref: object, text: str):
        edited = await _safe_reply_markdown(
            event,
            text,
            edit_message=ref if isinstance(ref, Message) else None,
        )
        return edited or ref

    streamer = LiveTelegramStreamer(
        send_message=_send_query_message,
        edit_message=_edit_query_message,
        get_message_id=lambda ref: _message_ref_id(ref) or 0,
        placeholder_text="Thinking... ⏳",
        edit_interval_ms=_stream_edit_interval_ms(),
        max_chars_per_edit=_stream_max_chars_per_edit(),
        typing_action_cb=lambda: _send_query_typing(event),
        typing_enabled=_stream_typing_action_enabled(),
    )
    await streamer.start(initial_ref=progress_message)

    answer = await generate_answer_from_context(
        query=query_text,
        context_messages=results,
        auth_manager=_require_auth_manager(),
        conversation_history=history,
        on_token=streamer.push,
    )
    if not answer.strip():
        answer = "No matching information found in recent updates."

    stats = await streamer.finalize(answer)
    return answer, stats


async def _on_query_message(event: events.NewMessage.Event) -> None:
    if not _is_query_mode_enabled():
        return

    msg = event.message
    if msg is None or not getattr(msg, "out", False):
        return

    # Fast text-level filter first to avoid expensive entity/network checks.
    text = normalize_space(str(msg.message or ""))
    if _is_query_text_ignored(text):
        return

    # Strict peer filter:
    # - only private user peers are considered query-safe
    # - immediately drop channels/groups/supergroups
    peer = getattr(msg, "peer_id", None)
    if not isinstance(peer, types.PeerUser):
        LOGGER.debug(
            "Query ignored: unsupported peer type=%s chat_id=%s",
            type(peer).__name__,
            getattr(msg, "chat_id", None),
        )
        return

    sender_id = int(getattr(msg, "sender_id", 0) or 0)
    target_user_id = int(getattr(peer, "user_id", 0) or 0)
    if sender_id <= 0 or target_user_id <= 0:
        LOGGER.debug(
            "Query ignored: invalid sender/target sender_id=%s target_user_id=%s",
            sender_id,
            target_user_id,
        )
        return

    # Allowed context #1: Saved Messages (self-chat).
    is_saved_messages = target_user_id == sender_id

    # Allowed context #2: private chat with user's own bot account (if configured).
    bot_user_id = await _resolve_query_bot_user_id()
    is_own_bot_pm = bot_user_id is not None and target_user_id == bot_user_id

    if not (is_saved_messages or is_own_bot_pm):
        LOGGER.debug(
            "Query ignored: private chat is not Saved Messages or own bot peer chat_id=%s peer_user_id=%s",
            getattr(msg, "chat_id", None),
            target_user_id,
        )
        return

    chat_id = str(getattr(msg, "chat_id", "") or "")

    now = time.time()
    last = query_last_request_ts.get(sender_id, 0.0)
    if now - last < 10:
        wait_seconds = int(max(1.0, 10 - (now - last)))
        await _safe_reply_markdown(
            event,
            f"Please wait {wait_seconds}s before your next query.",
        )
        return
    query_last_request_ts[sender_id] = now

    progress = await _safe_reply_markdown(event, "Searching your channels... ⏳")
    if progress is None:
        return

    try:
        parsed_hours, cleaned_query = parse_time_filter_from_query(
            text,
            _query_default_hours_back(),
        )

        results = await search_recent_messages(
            _require_client(),
            monitored_source_chat_ids,
            cleaned_query,
            max_messages=_query_max_messages(),
            default_hours_back=parsed_hours,
            logger=LOGGER,
        )

        await _safe_reply_markdown(
            event,
            f"Found {len(results)} messages -> analyzing... ⏳",
            edit_message=progress,
        )

        history = list(_query_history_for_sender(sender_id))
        if _is_streaming_enabled():
            answer, stream_stats = await _stream_query_answer(
                event,
                progress_message=progress,
                query_text=cleaned_query,
                results=results,
                history=history,
            )
        else:
            answer = await generate_answer_from_context(
                query=cleaned_query,
                context_messages=results,
                auth_manager=_require_auth_manager(),
                conversation_history=history,
            )
            if not answer.strip():
                answer = "No matching information found in recent updates."
            await _safe_reply_markdown(event, answer, edit_message=progress)
            stream_stats = None

        _append_query_history(sender_id, text, answer)
        log_structured(
            LOGGER,
            "query_answered",
            sender_id=sender_id,
            chat_id=chat_id,
            query_length=len(text),
            hours_back=parsed_hours,
            messages_found=len(results),
            response_chars=len(answer),
            stream_enabled=_is_streaming_enabled(),
            stream_edits=(stream_stats.edit_count if stream_stats else 0),
            stream_tokens_per_second=(
                round(stream_stats.tokens_per_second, 3) if stream_stats else 0.0
            ),
        )
    except Exception as exc:
        LOGGER.exception("Query handling failed.")
        await _safe_reply_markdown(
            event,
            "No matching information found in recent updates.",
            edit_message=progress,
        )
        log_structured(
            LOGGER,
            "query_failed",
            level=logging.ERROR,
            sender_id=sender_id,
            chat_id=chat_id,
            error=str(exc),
        )


async def _on_new_message(event: events.NewMessage.Event) -> None:
    msg = event.message
    channel_id = str(msg.chat_id)

    try:
        if is_seen(channel_id, msg.id):
            return

        if _is_dupe_detection_enabled():
            dupe_result = await is_duplicate_and_handle(event)
            if dupe_result.duplicate:
                mark_seen(channel_id, msg.id)
                if dupe_result.breaking_duplicate_recent:
                    log_structured(
                        LOGGER,
                        "breaking_duplicate_suppressed",
                        channel_id=channel_id,
                        message_id=msg.id,
                        score=round(float(dupe_result.final_score), 4),
                        matched_channel_id=dupe_result.matched_channel_id,
                        matched_message_id=dupe_result.matched_message_id,
                        merged=bool(dupe_result.merged),
                    )
                return

        if msg.grouped_id:
            key = (channel_id, int(msg.grouped_id))
            album_buffers[key].append(msg)
            task = album_tasks.get(key)
            if task and not task.done():
                task.cancel()
            album_tasks[key] = asyncio.create_task(_flush_album_after_wait(key))
            return

        if _is_digest_mode_enabled():
            await _queue_single_message_for_digest(msg)
        else:
            await _process_single_message(msg)
    except Exception:
        LOGGER.exception(
            "Failed processing message channel_id=%s message_id=%s",
            channel_id,
            msg.id,
        )


async def _shutdown_client() -> None:
    global digest_scheduler_task
    configure_duplicate_runtime(None)

    if digest_scheduler_task and not digest_scheduler_task.done():
        digest_scheduler_task.cancel()
        await asyncio.gather(digest_scheduler_task, return_exceptions=True)
    digest_scheduler_task = None

    tg = client
    if tg is None:
        return

    pending = [task for task in album_tasks.values() if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    album_tasks.clear()
    album_buffers.clear()

    try:
        if tg.is_connected():
            await tg.disconnect()
    except Exception:
        LOGGER.exception("Failed during client shutdown.")


def _startup_health_check() -> None:
    mode = "DIGEST" if _is_digest_mode_enabled() else "PER_POST"
    pending = count_pending()
    inflight = count_inflight()
    last_ts = get_last_digest_timestamp()
    log_structured(
        LOGGER,
        "startup_health",
        mode=mode,
        dupe_detection=_is_dupe_detection_enabled(),
        severity_routing=_is_severity_routing_enabled(),
        query_mode=_is_query_mode_enabled(),
        pending=pending,
        inflight=inflight,
        last_digest_ts=last_ts,
        last_digest_at=format_ts(last_ts),
        quota_health=get_quota_health(),
    )
    LOGGER.info(
        "Startup health: mode=%s pending=%s inflight=%s last_digest=%s dupe=%s severity=%s query=%s",
        mode,
        pending,
        inflight,
        format_ts(last_ts),
        _is_dupe_detection_enabled(),
        _is_severity_routing_enabled(),
        _is_query_mode_enabled(),
    )


async def main() -> None:
    global client, auth_manager, digest_scheduler_task

    _prompt_for_missing_config()
    _validate_config()
    init_db()
    await _init_dupe_detector()
    _startup_health_check()

    auth_manager = AuthManager(logger=LOGGER)
    await auth_manager.get_access_token()

    client = TelegramClient("userbot", config.TELEGRAM_API_ID, config.TELEGRAM_API_HASH)
    await _ensure_user_account_session()
    await _ensure_destination_peer()

    resolved_sources = await setup_sources()
    while not resolved_sources:
        print(
            "No channels resolved from folder. Add manual sources "
            "(username/public/private invite links)."
        )
        new_sources = _prompt_sources()
        current = getattr(config, "EXTRA_SOURCES", [])
        if not isinstance(current, list):
            current = []
        current.extend(new_sources)
        # Preserve order while deduplicating newly added manual sources.
        deduped: List[str] = []
        seen = set()
        for item in current:
            normalized = str(item).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        config.EXTRA_SOURCES = deduped
        _persist_config_updates({"EXTRA_SOURCES": config.EXTRA_SOURCES})
        resolved_sources = await setup_sources()

    client.add_event_handler(_on_new_message, events.NewMessage(chats=resolved_sources))
    status_pattern = rf"^{re.escape(_digest_status_command())}(?:\\s+.*)?$"
    client.add_event_handler(
        _on_digest_status_command,
        events.NewMessage(outgoing=True, pattern=status_pattern),
    )
    if _is_query_mode_enabled():
        client.add_event_handler(
            _on_query_message,
            events.NewMessage(outgoing=True),
        )

    me = await client.get_me()
    LOGGER.info("Userbot started as @%s", getattr(me, "username", "unknown"))
    if _is_missing_folder_invite_link():
        LOGGER.info("Listening to %s manually configured channels.", len(resolved_sources))
    if _is_digest_mode_enabled():
        digest_scheduler_task = asyncio.create_task(
            run_digest_scheduler(),
            name="digest-scheduler",
        )

    try:
        await client.run_until_disconnected()
    except (asyncio.CancelledError, KeyboardInterrupt):
        LOGGER.info("Shutdown requested. Stopping userbot...")
    finally:
        await _shutdown_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Userbot stopped.")
