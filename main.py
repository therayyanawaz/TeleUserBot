"""Telegram News Aggregator userbot entry point."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from types import SimpleNamespace
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
    MessageNotModifiedError,
    PhoneNumberInvalidError,
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
    create_digest_summary,
    generate_answer_from_context,
    get_quota_health,
    summarize_breaking_headline,
    summarize_vital_rational_view,
    summarize_or_skip,
    translate_ocr_text_to_english,
)
from auth import AuthManager, ERROR_LOG_PATH, ensure_runtime_dir
from db import (
    ack_digest_batch,
    claim_digest_batch,
    clear_digest_queue_scoped,
    count_inflight,
    count_pending,
    get_last_digest_timestamp,
    init_db,
    is_seen,
    load_source_delivery_ref,
    load_archive_since,
    load_queue_since,
    mark_seen,
    mark_seen_many,
    prune_archive_older_than,
    purge_source_delivery_refs,
    restore_digest_batch,
    save_source_delivery_ref,
    save_to_digest_archive,
    save_to_digest_queue,
    set_last_digest_timestamp,
)
from prompts import quiet_period_message
from severity_classifier import classify_message_severity
from utils import (
    apply_premium_emoji_html,
    build_alert_header,
    build_media_signature_digest,
    check_and_store_media_duplicate,
    compute_visual_media_hash,
    LiveTelegramStreamer,
    DuplicateRuntime,
    GlobalDuplicateResult,
    HybridDuplicateEngine,
    build_dupe_fingerprint,
    build_query_plan,
    build_telegram_message_link,
    configure_duplicate_runtime,
    estimate_tokens_rough,
    format_eta,
    format_ts,
    is_duplicate_and_handle,
    is_broad_news_query,
    load_custom_emoji_map,
    log_structured,
    normalize_space,
    parse_daily_times,
    parse_time_filter_from_query,
    expand_query_terms,
    extract_first_video_frame_bytes,
    extract_query_keywords,
    extract_query_numbers,
    extract_ocr_text_from_image_bytes,
    sanitize_telegram_html,
    search_recent_news_web,
    search_recent_messages,
    seconds_until_next_daily_time,
    split_html_chunks,
    strip_telegram_html,
)
from web_server import WebStatusServer

try:
    import fcntl
except Exception:  # pragma: no cover - non-Unix fallback
    fcntl = None  # type: ignore[assignment]


def _supports_color() -> bool:
    forced = str(os.getenv("CLI_COLOR", "") or "").strip().lower()
    if forced in {"1", "true", "yes", "on"}:
        return True
    if forced in {"0", "false", "no", "off"}:
        return False
    if os.getenv("NO_COLOR"):
        return False
    if not sys.stderr.isatty():
        return False
    return str(os.getenv("TERM", "") or "").strip().lower() not in {"", "dumb"}


_COLOR_ON = _supports_color()
_C_RESET = "\033[0m"
_C_BOLD = "\033[1m"
_C_DIM = "\033[2m"
_C_CYAN = "\033[36m"
_C_GREEN = "\033[32m"
_C_YELLOW = "\033[33m"
_C_RED = "\033[31m"


def _c(text: str, color: str, *, bold: bool = False, dim: bool = False) -> str:
    if not _COLOR_ON:
        return text
    style = ""
    if bold:
        style += _C_BOLD
    if dim:
        style += _C_DIM
    return f"{style}{color}{text}{_C_RESET}"


def _print_cli_banner() -> None:
    if not sys.stdin.isatty():
        return
    line = "═" * 64
    print(_c(line, _C_CYAN, bold=True))
    print(_c("Telegram News Intelligence Userbot", _C_CYAN, bold=True))
    print(_c("Single entrypoint: python main.py", _C_CYAN, dim=True))
    print(_c(line, _C_CYAN, bold=True))


def _print_cli_status(symbol: str, text: str, *, level: str = "info") -> None:
    if not sys.stdin.isatty():
        return
    color = _C_CYAN
    if level == "ok":
        color = _C_GREEN
    elif level == "warn":
        color = _C_YELLOW
    elif level == "error":
        color = _C_RED
    print(f"{_c(symbol, color, bold=True)} {text}")


class _ColorConsoleFormatter(logging.Formatter):
    _LEVEL_COLORS = {
        "DEBUG": _C_DIM,
        "INFO": _C_CYAN,
        "WARNING": _C_YELLOW,
        "ERROR": _C_RED,
        "CRITICAL": _C_RED,
    }

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        if not _COLOR_ON:
            return text
        color = self._LEVEL_COLORS.get(record.levelname, "")
        if not color:
            return text
        return f"{color}{text}{_C_RESET}"


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
_console_handler.setFormatter(_ColorConsoleFormatter("%(asctime)s | %(levelname)s | %(message)s"))
LOGGER.addHandler(_console_handler)


client: TelegramClient | None = None
auth_manager: AuthManager | None = None
destination_peer = None
bot_destination_token: str | None = None
bot_destination_chat_id: str | None = None
premium_emoji_map: Dict[str, str] = {}

# (channel_id, grouped_id) -> [messages]
album_buffers: Dict[Tuple[str, int], List[Message]] = defaultdict(list)
album_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
digest_scheduler_task: asyncio.Task | None = None
daily_digest_scheduler_task: asyncio.Task | None = None
queue_clear_scheduler_task: asyncio.Task | None = None
query_bot_poll_task: asyncio.Task | None = None
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
query_bot_updates_offset: int = 0
query_bot_poll_started_at: int = 0
breaking_delivery_refs: Dict[str, Dict[str, object]] = {}
breaking_topic_threads: List[Dict[str, object]] = []
breaking_topic_lock = asyncio.Lock()
query_allowed_bot_user_id: int | None = None
query_bot_user_id_checked: bool = False
instance_lock_handle = None
web_status_server: WebStatusServer | None = None
started_as_username: str = "unknown"
started_as_user_id: int = 0
startup_phase: str = "booting"
startup_ready: bool = False
startup_error: str = ""

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


def _is_interactive_runtime() -> bool:
    return sys.stdin.isatty()


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
    raw = getattr(config, "DIGEST_INTERVAL_MINUTES", 60)
    try:
        minutes = int(raw)
    except Exception:
        minutes = 60
    minutes = max(1, min(minutes, 24 * 60))
    return minutes * 60


def _digest_daily_times() -> List[Tuple[int, int]]:
    raw = getattr(config, "DIGEST_DAILY_TIMES", ["00:00"])
    if not isinstance(raw, list):
        return []
    values = [str(x) for x in raw]
    return parse_daily_times(values)


def _digest_queue_clear_interval_seconds() -> int:
    # Queue clear scheduler is intentionally disabled to preserve full intake flow.
    # Digest queue should only be drained by hourly digest claiming.
    return 0


def _digest_queue_clear_include_inflight() -> bool:
    return _bool_flag(getattr(config, "DIGEST_QUEUE_CLEAR_INCLUDE_INFLIGHT", True), True)


def _digest_queue_clear_scope() -> str:
    """
    Queue clear scope:
    - inflight: clear only stuck claimed rows (safe default)
    - pending: clear only unsent rows
    - all: clear everything
    """
    raw = str(getattr(config, "DIGEST_QUEUE_CLEAR_SCOPE", "") or "").strip().lower()
    if raw in {"inflight", "pending", "all"}:
        return raw
    # Backward-compat with older include_inflight flag behavior.
    # Historical setting true used to mean "all", false meant "pending".
    legacy_include = _digest_queue_clear_include_inflight()
    return "all" if legacy_include else "pending"


def _digest_daily_window_hours() -> int:
    raw = getattr(config, "DIGEST_DAILY_WINDOW_HOURS", 24)
    try:
        value = int(raw)
    except Exception:
        value = 24
    return max(1, min(value, 168))


def _digest_daily_max_posts() -> int:
    raw = getattr(config, "DIGEST_DAILY_MAX_POSTS", 300)
    try:
        value = int(raw)
    except Exception:
        value = 300
    return max(10, min(value, 2000))


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


def _dupe_use_sentence_transformers() -> bool:
    raw = getattr(config, "DUPE_USE_SENTENCE_TRANSFORMERS", False)
    return _bool_flag(raw, False)


def _is_severity_routing_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_SEVERITY_ROUTING", True), True)


def _is_immediate_high_enabled() -> bool:
    return _bool_flag(getattr(config, "IMMEDIATE_HIGH", True), True)


def _include_source_tags() -> bool:
    return _bool_flag(getattr(config, "INCLUDE_SOURCE_TAGS", False), False)


def _is_html_formatting_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_HTML_FORMATTING", True), True)


def _is_premium_emoji_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_PREMIUM_EMOJI", True), True)


def _premium_emoji_map_path() -> str:
    raw = str(getattr(config, "PREMIUM_EMOJI_MAP_FILE", "nezami_emoji_map.json") or "").strip()
    return raw or "nezami_emoji_map.json"


def _load_premium_emoji_map() -> None:
    global premium_emoji_map
    if not _is_premium_emoji_enabled():
        premium_emoji_map = {}
        return
    premium_emoji_map = load_custom_emoji_map(_premium_emoji_map_path(), logger=LOGGER)


def _render_outbound_text(
    text: str | None,
    *,
    allow_premium_tags: bool = False,
) -> str | None:
    if text is None:
        return None
    value = str(text)
    if _is_html_formatting_enabled():
        value = sanitize_telegram_html(value)
        if allow_premium_tags and premium_emoji_map:
            value = apply_premium_emoji_html(value, premium_emoji_map)
    return value


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


def _is_query_web_fallback_enabled() -> bool:
    return _bool_flag(getattr(config, "QUERY_WEB_FALLBACK_ENABLED", True), True)


def _query_web_min_telegram_results() -> int:
    raw = getattr(config, "QUERY_WEB_MIN_TELEGRAM_RESULTS", 3)
    try:
        value = int(raw)
    except Exception:
        value = 3
    return max(0, min(value, 20))


def _query_web_max_results() -> int:
    raw = getattr(config, "QUERY_WEB_MAX_RESULTS", 12)
    try:
        value = int(raw)
    except Exception:
        value = 12
    return max(3, min(value, 40))


def _query_web_max_hours_back() -> int:
    raw = getattr(config, "QUERY_WEB_MAX_HOURS_BACK", 24)
    try:
        value = int(raw)
    except Exception:
        value = 24
    return max(1, min(value, 72))


def _query_web_require_recent() -> bool:
    return _bool_flag(getattr(config, "QUERY_WEB_REQUIRE_RECENT", True), True)


def _query_web_require_min_sources() -> int:
    raw = getattr(config, "QUERY_WEB_REQUIRE_MIN_SOURCES", 2)
    try:
        value = int(raw)
    except Exception:
        value = 2
    return max(1, min(value, 10))


def _query_web_allowed_domains() -> list[str]:
    raw = getattr(config, "QUERY_WEB_ALLOWED_DOMAINS", [])
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        domain = normalize_space(str(item or "")).lower().lstrip(".")
        if domain:
            out.append(domain)
    return out


def _is_high_risk_news_query(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    high_risk_terms = (
        "leader",
        "supreme leader",
        "president",
        "prime minister",
        "successor",
        "succession",
        "deceased",
        "died",
        "dead",
        "killed",
        "assassinated",
        "resigned",
        "coup",
        "overthrown",
        "nuclear plant",
        "nuclear site",
    )
    return any(term in lowered for term in high_risk_terms)


def _query_prefers_web_crosscheck(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    if extract_query_numbers(query):
        return True
    if is_broad_news_query(query) and bool(extract_query_keywords(query)):
        return True
    markers = (
        "who died",
        "who was killed",
        "who was injured",
        "casualties",
        "fatalities",
        "death toll",
        "how many killed",
        "how many injured",
    )
    return any(marker in lowered for marker in markers)


def _query_allowed_peer_ids() -> set[int]:
    raw = getattr(config, "QUERY_ALLOWED_PEER_IDS", [])
    if not isinstance(raw, list):
        return set()
    allowed: set[int] = set()
    for item in raw:
        try:
            value = int(str(item).strip())
        except Exception:
            continue
        if value > 0:
            allowed.add(value)
    return allowed


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


def _is_web_server_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_WEB_SERVER", True), True)


def _web_server_host() -> str:
    raw = str(getattr(config, "WEB_SERVER_HOST", "0.0.0.0") or "").strip()
    return raw or "0.0.0.0"


def _web_server_port() -> int:
    raw = getattr(config, "WEB_SERVER_PORT", 8080)
    try:
        value = int(raw)
    except Exception:
        value = 8080
    return max(1, min(value, 65535))


def _should_hold_on_startup_error() -> bool:
    # In non-interactive deployment (e.g., Replit autoscale), keep process alive
    # with health/status endpoints so platform health checks don't flap.
    raw = str(
        os.getenv(
            "HOLD_ON_STARTUP_ERROR",
            "true" if not sys.stdin.isatty() else "false",
        )
        or ""
    ).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _breaking_keywords() -> List[str]:
    default_keywords = [
        "breaking",
        "urgent",
        "just now",
        "alert",
        "explosion",
        "strike",
        "airstrike",
        "missile",
        "attack",
        "air raid",
        "drone strike",
        "casualties",
        "killed",
        "intercepted",
    ]
    raw = getattr(config, "BREAKING_NEWS_KEYWORDS", [])
    result: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, str):
                continue
            normalized = item.strip().lower()
            if normalized and normalized not in result:
                result.append(normalized)
    for keyword in default_keywords:
        if keyword not in result:
            result.append(keyword)
    return result


def _breaking_match_threshold() -> int:
    raw = getattr(config, "BREAKING_MATCH_THRESHOLD", 1)
    try:
        value = int(raw)
    except Exception:
        value = 1
    return max(1, min(value, 10))


def _is_breaking_topic_threads_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_BREAKING_TOPIC_THREADS", True), True)


def _breaking_topic_window_seconds() -> int:
    raw = getattr(config, "BREAKING_TOPIC_WINDOW_MINUTES", 180)
    try:
        value = int(raw)
    except Exception:
        value = 180
    return max(10 * 60, min(value * 60, 24 * 60 * 60))


def _breaking_topic_min_overlap() -> int:
    raw = getattr(config, "BREAKING_TOPIC_MIN_OVERLAP", 2)
    try:
        value = int(raw)
    except Exception:
        value = 2
    return max(1, min(value, 8))


def _breaking_topic_min_ratio() -> float:
    raw = getattr(config, "BREAKING_TOPIC_MIN_RATIO", 0.55)
    try:
        value = float(raw)
    except Exception:
        value = 0.55
    return min(max(value, 0.1), 1.0)


def _breaking_topic_fuzzy_ratio() -> float:
    raw = getattr(config, "BREAKING_TOPIC_FUZZY_RATIO", 0.72)
    try:
        value = float(raw)
    except Exception:
        value = 0.72
    return min(max(value, 0.3), 1.0)


def _breaking_topic_continuity_prefix() -> str:
    raw = str(
        getattr(
            config,
            "BREAKING_TOPIC_CONTINUITY_PREFIX",
            "",
        )
        or ""
    ).strip()
    return raw


def _humanized_vital_opinion_enabled() -> bool:
    return _bool_flag(getattr(config, "HUMANIZED_VITAL_OPINION_ENABLED", True), True)


def _humanized_vital_opinion_probability() -> float:
    raw = getattr(config, "HUMANIZED_VITAL_OPINION_PROBABILITY", 0.35)
    try:
        value = float(raw)
    except Exception:
        value = 0.35
    return min(max(value, 0.0), 1.0)


def _should_attach_vital_opinion(text: str) -> bool:
    if not _humanized_vital_opinion_enabled():
        return False
    probability = _humanized_vital_opinion_probability()
    if probability <= 0:
        return False
    if probability >= 1:
        return True
    normalized = normalize_space(text).lower()
    if not normalized:
        return False
    digest = hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < probability


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
    # Deployment/runtime without TTY must not block on interactive prompts.
    if not _is_interactive_runtime():
        return

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
    issues: List[str] = []

    if _is_missing_telegram_api_id():
        issues.append("Set TELEGRAM_API_ID in environment or .env")
    if _is_missing_telegram_api_hash():
        issues.append("Set TELEGRAM_API_HASH in environment or .env")

    if _is_bot_destination_mode():
        token = _bot_destination_token_from_config()
        chat_id = str(getattr(config, "BOT_DESTINATION_CHAT_ID", "") or "").strip()
        if not _looks_like_bot_token(token):
            issues.append("BOT_DESTINATION_TOKEN is missing or invalid")
        if not chat_id:
            issues.append("Set BOT_DESTINATION_CHAT_ID in environment or .env")
        elif _looks_like_bot_token(chat_id):
            issues.append("BOT_DESTINATION_CHAT_ID looks like a bot token; set real chat id")
        elif _looks_like_addlist_link(chat_id):
            issues.append("BOT_DESTINATION_CHAT_ID cannot be a folder addlist link")
    else:
        destination = str(getattr(config, "DESTINATION", "") or "").strip()
        if _is_missing_destination():
            issues.append("Set DESTINATION in environment or .env")
        elif _looks_like_addlist_link(destination):
            issues.append("DESTINATION cannot be a folder addlist link")
        elif _looks_like_bot_token(destination):
            issues.append("DESTINATION cannot be a bot token")

    if _is_missing_folder_invite_link() and not _has_manual_sources():
        issues.append("Set FOLDER_INVITE_LINK or EXTRA_SOURCES/SOURCES")
    else:
        folder_link = str(getattr(config, "FOLDER_INVITE_LINK", "") or "").strip()
        if folder_link and not _is_missing_folder_invite_link():
            try:
                _extract_addlist_hash(_normalize_addlist_link(folder_link))
            except Exception:
                issues.append("FOLDER_INVITE_LINK is invalid; expected t.me/addlist/<hash>")

        for source in _manual_source_entries():
            value = str(source or "").strip()
            if not value:
                continue
            if _looks_like_bot_token(value):
                issues.append(f"Manual source '{value}' looks like bot token")
            elif _looks_like_addlist_link(value):
                issues.append(
                    f"Manual source '{value}' is addlist link; use FOLDER_INVITE_LINK instead"
                )

    if issues:
        bullet = "\n".join(f"- {item}" for item in issues)
        raise ValueError(f"Configuration validation failed:\n{bullet}")

    if _is_bot_destination_mode() and str(getattr(config, "DESTINATION", "") or "").strip():
        LOGGER.warning(
            "Both BOT destination and DESTINATION are set. "
            "Bot mode will be used and DESTINATION ignored."
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


def _bot_api_timeout() -> httpx.Timeout:
    # Media uploads need a much larger write timeout than text-only requests.
    return httpx.Timeout(connect=20.0, read=90.0, write=180.0, pool=20.0)


async def _bot_api_request(
    method: str,
    *,
    data: Dict[str, str] | None = None,
    files: Dict[str, tuple[str, bytes, str]] | None = None,
) -> object:
    token = _require_bot_destination_token()
    url = f"https://api.telegram.org/bot{token}/{method}"

    async with httpx.AsyncClient(timeout=_bot_api_timeout()) as http:
        for attempt in range(4):
            try:
                response = await http.post(url, data=data, files=files)
            except (
                httpx.WriteTimeout,
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.RemoteProtocolError,
                httpx.TransportError,
            ) as exc:
                if attempt >= 3:
                    raise
                wait_seconds = min(8, 2 ** attempt)
                LOGGER.warning(
                    "Bot API %s transport failure (%s). Retrying in %ss.",
                    method,
                    exc.__class__.__name__,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                continue

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


async def _query_bot_api_request(
    method: str,
    *,
    data: Dict[str, str] | None = None,
) -> object:
    """
    Bot API calls used by query assistant replies in own-bot PM context.
    Uses configured BOT_DESTINATION_TOKEN but allows dynamic chat_id per request.
    """
    token = _bot_destination_token_from_config()
    if not _looks_like_bot_token(token):
        raise RuntimeError("BOT_DESTINATION_TOKEN is missing/invalid for bot query replies.")

    url = f"https://api.telegram.org/bot{token}/{method}"
    async with httpx.AsyncClient(timeout=60) as http:
        for _ in range(4):
            response = await http.post(url, data=data)
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


async def _query_bot_send_message(data: Dict[str, str]) -> object:
    """
    Send query reply via Bot API.
    If Telegram rejects reply_to_message_id for this peer, retry once without it.
    """
    try:
        return await _query_bot_api_request("sendMessage", data=data)
    except RuntimeError as exc:
        error_text = str(exc).lower()
        if "message to be replied not found" in error_text and "reply_to_message_id" in data:
            retry_data = dict(data)
            retry_data.pop("reply_to_message_id", None)
            LOGGER.debug(
                "Bot query reply target not found; retrying sendMessage without reply_to_message_id."
            )
            return await _query_bot_api_request("sendMessage", data=retry_data)
        raise


def _is_query_bot_webhook_conflict(error: Exception) -> bool:
    lowered = str(error or "").lower()
    return "http 409" in lowered and "webhook" in lowered


async def _ensure_query_bot_polling_mode() -> bool:
    """
    Ensure the query bot can use getUpdates by clearing any active webhook.
    """
    try:
        info = await _query_bot_api_request("getWebhookInfo", data={})
    except Exception:
        LOGGER.debug("Failed to inspect bot webhook state for query polling.", exc_info=True)
        return False

    webhook_url = ""
    pending_updates = 0
    if isinstance(info, dict):
        webhook_url = normalize_space(str(info.get("url") or ""))
        try:
            pending_updates = int(info.get("pending_update_count") or 0)
        except Exception:
            pending_updates = 0

    if not webhook_url:
        return True

    LOGGER.warning(
        "Active webhook detected for query bot (%s, pending=%s). "
        "Deleting webhook so bot PM query replies can use polling.",
        webhook_url,
        pending_updates,
    )
    await _query_bot_api_request(
        "deleteWebhook",
        data={"drop_pending_updates": "true"},
    )
    await asyncio.sleep(0.5)
    return True


async def _prime_query_bot_update_offset() -> None:
    """
    Drop stale pending query updates from previous runs and move polling to the tail.
    """
    global query_bot_updates_offset

    try:
        updates = await _query_bot_api_request(
            "getUpdates",
            data={
                "timeout": "0",
                "limit": "1",
                "offset": "-1",
            },
        )
    except Exception:
        LOGGER.debug("Failed priming bot query update offset.", exc_info=True)
        return

    if not isinstance(updates, list):
        return

    max_seen = query_bot_updates_offset
    for update in updates:
        if not isinstance(update, dict):
            continue
        update_id = int(update.get("update_id", 0) or 0)
        if update_id > 0:
            max_seen = max(max_seen, update_id + 1)
    query_bot_updates_offset = max(query_bot_updates_offset, max_seen)


async def _resolve_query_bot_reply_target(
    *,
    chat_id: int,
    sender_id: int,
    text: str,
    max_attempts: int = 8,
    sleep_seconds: float = 0.45,
) -> int | None:
    """
    Resolve the bot-side message_id for a user query inside the bot PM.

    The outgoing MTProto message id seen by the userbot is not guaranteed to
    match the Bot API message id visible to the bot. To thread bot replies
    correctly, inspect recent Bot API updates and match the user's query text.
    """
    global query_bot_updates_offset

    normalized_text = normalize_space(text)
    if not normalized_text:
        return None

    now_ts = int(time.time())

    for attempt in range(max(1, int(max_attempts))):
        data = {
            "timeout": "0",
            "limit": "50",
        }
        if query_bot_updates_offset > 0:
            data["offset"] = str(query_bot_updates_offset)

        try:
            updates = await _query_bot_api_request("getUpdates", data=data)
        except Exception:
            LOGGER.debug("Failed to resolve bot-side reply target via getUpdates.", exc_info=True)
            return None

        max_seen_offset = query_bot_updates_offset
        matched_message_id: int | None = None
        fallback_message_id: int | None = None
        fallback_message_ts: int = 0

        if isinstance(updates, list):
            for update in reversed(updates):
                if not isinstance(update, dict):
                    continue
                update_id = int(update.get("update_id", 0) or 0)
                if update_id > 0:
                    max_seen_offset = max(max_seen_offset, update_id + 1)

                payload = update.get("message") or update.get("edited_message")
                if not isinstance(payload, dict):
                    continue

                payload_chat = payload.get("chat")
                if not isinstance(payload_chat, dict):
                    continue
                if int(payload_chat.get("id", 0) or 0) != int(chat_id):
                    continue

                payload_from = payload.get("from")
                if not isinstance(payload_from, dict):
                    continue
                if int(payload_from.get("id", 0) or 0) != int(sender_id):
                    continue

                payload_date = int(payload.get("date", 0) or 0)
                if payload_date > 0 and abs(now_ts - payload_date) > 180:
                    continue

                candidate = int(payload.get("message_id", 0) or 0)
                if candidate > 0 and payload_date >= fallback_message_ts:
                    fallback_message_id = candidate
                    fallback_message_ts = payload_date

                if candidate <= 0:
                    continue

                payload_text_raw = payload.get("text") or payload.get("caption") or ""
                payload_text = normalize_space(str(payload_text_raw or ""))
                if payload_text == normalized_text:
                    matched_message_id = candidate
                    break

        query_bot_updates_offset = max(query_bot_updates_offset, max_seen_offset)
        if matched_message_id:
            return matched_message_id
        if fallback_message_id:
            return fallback_message_id

        if attempt + 1 < max_attempts:
            await asyncio.sleep(max(0.05, float(sleep_seconds)))

    return None


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
    safe_source = sanitize_telegram_html(source_title)
    safe_summary = sanitize_telegram_html(summary)
    if _include_source_tags():
        return f"<b>📰 {safe_source}</b><br><br>{safe_summary}"
    return f"<b>📰 Update</b><br><br>{safe_summary}"


def _format_source_label(source_title: str) -> str:
    safe_source = sanitize_telegram_html(source_title)
    if _include_source_tags():
        return f"<b>📰 {safe_source}</b>"
    return "<b>📰 Update</b>"


def _to_bot_text(text: str | None, max_len: int | None = None) -> str | None:
    if text is None:
        return None
    cleaned = text.strip()
    if max_len is not None and len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 3].rstrip() + "..."
    return (
        _render_outbound_text(cleaned, allow_premium_tags=True)
        if _is_html_formatting_enabled()
        else cleaned
    )


def _to_plain_text(text: str | None, max_len: int | None = None) -> str | None:
    if text is None:
        return None
    cleaned = text.strip()
    if _is_html_formatting_enabled():
        cleaned = strip_telegram_html(cleaned)
    if max_len is not None and len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 3].rstrip() + "..."
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


def _is_retryable_network_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionResetError, TimeoutError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, OSError):
        text = str(exc).lower()
        retry_markers = (
            "connection reset by peer",
            "broken pipe",
            "timed out",
            "temporary failure",
            "network is unreachable",
            "connection aborted",
        )
        return any(marker in text for marker in retry_markers)
    return False


async def _call_with_floodwait(func, *args, **kwargs):
    network_attempt = 0
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as exc:
            wait_seconds = int(exc.seconds) + 1
            LOGGER.error("FloodWaitError: sleeping %s second(s).", wait_seconds)
            await asyncio.sleep(wait_seconds)
        except Exception as exc:
            if _is_retryable_network_error(exc) and network_attempt < 1:
                network_attempt += 1
                wait_seconds = 2 * network_attempt
                LOGGER.warning(
                    "Retryable network error in %s (%s). Retrying in %ss.",
                    getattr(func, "__name__", repr(func)),
                    exc.__class__.__name__,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                continue
            raise


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


def _media_text_ocr_enabled() -> bool:
    return bool(getattr(config, "MEDIA_TEXT_OCR_ENABLED", True))


def _media_text_ocr_video_enabled() -> bool:
    return bool(getattr(config, "MEDIA_TEXT_OCR_VIDEO_ENABLED", True))


def _media_text_ocr_min_chars() -> int:
    try:
        value = int(getattr(config, "MEDIA_TEXT_OCR_MIN_CHARS", 12))
    except Exception:
        value = 12
    return max(8, min(value, 200))


def _media_text_ocr_max_chars() -> int:
    try:
        value = int(getattr(config, "MEDIA_TEXT_OCR_MAX_CHARS", 1600))
    except Exception:
        value = 1600
    return max(200, min(value, 4000))


def _media_text_ocr_video_max_bytes() -> int:
    try:
        value_mb = int(getattr(config, "MEDIA_TEXT_OCR_VIDEO_MAX_MB", 25))
    except Exception:
        value_mb = 25
    value_mb = max(1, min(value_mb, 200))
    return value_mb * 1024 * 1024


def _media_text_ocr_langs() -> str:
    raw = str(getattr(config, "MEDIA_TEXT_OCR_LANGS", "eng+ara+fas+urd+rus") or "").strip()
    return raw or "eng+ara+fas+urd+rus"


def _message_mime_type(msg: Message) -> str:
    value = getattr(getattr(msg, "file", None), "mime_type", None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    value = getattr(getattr(msg, "document", None), "mime_type", None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return ""


def _media_dupe_history_hours() -> int:
    try:
        value = int(getattr(config, "DUPE_HISTORY_HOURS", 4))
    except Exception:
        value = 4
    return max(1, min(value, 24))


def _message_is_image_ocr_candidate(msg: Message) -> bool:
    if not getattr(msg, "media", None):
        return False
    if msg.photo:
        return True
    mime_type = _message_mime_type(msg)
    return mime_type.startswith("image/")


def _message_is_video_ocr_candidate(msg: Message) -> bool:
    if not getattr(msg, "media", None):
        return False
    if msg.video:
        return True
    mime_type = _message_mime_type(msg)
    return mime_type.startswith("video/")


def _message_media_kind(msg: Message) -> str:
    if msg.photo or _message_is_image_ocr_candidate(msg):
        return "image"
    if msg.video or _message_is_video_ocr_candidate(msg):
        return "video"
    if getattr(msg, "document", None):
        return "document"
    return "media"


async def _download_media_signature_bytes(msg: Message) -> bytes:
    for thumb in (0, 1, -1):
        try:
            blob = await msg.download_media(file=bytes, thumb=thumb)
        except TypeError:
            break
        except Exception:
            continue
        if isinstance(blob, (bytes, bytearray)) and len(blob) >= 64:
            return bytes(blob)

    try:
        size = int(getattr(getattr(msg, "file", None), "size", 0) or 0)
    except Exception:
        size = 0
    if size and size > (2 * 1024 * 1024):
        return b""
    try:
        blob = await msg.download_media(file=bytes)
    except Exception:
        return b""
    if isinstance(blob, (bytes, bytearray)):
        return bytes(blob)
    return b""


async def _message_media_signature(msg: Message) -> tuple[str, str]:
    if not getattr(msg, "media", None):
        return "", ""

    media_kind = _message_media_kind(msg)
    parts: list[str] = [media_kind]
    media_blob = await _download_media_signature_bytes(msg)

    visual_blob = b""
    if media_kind == "image":
        visual_blob = media_blob
    elif media_kind == "video" and media_blob:
        with contextlib.suppress(Exception):
            visual_blob = await asyncio.to_thread(extract_first_video_frame_bytes, media_blob)

    visual_hash = ""
    if visual_blob:
        visual_hash = await asyncio.to_thread(compute_visual_media_hash, visual_blob)
    if visual_hash:
        return f"visual:{media_kind}:{visual_hash}", media_kind

    file_obj = getattr(msg, "file", None)
    for attr in ("id", "size", "mime_type", "name"):
        value = getattr(file_obj, attr, None)
        if value:
            parts.append(f"{attr}:{value}")

    photo = getattr(msg, "photo", None)
    if photo is not None:
        for attr in ("id", "access_hash"):
            value = getattr(photo, attr, None)
            if value:
                parts.append(f"photo_{attr}:{value}")

    document = getattr(msg, "document", None)
    if document is not None:
        for attr in ("id", "access_hash", "mime_type", "size"):
            value = getattr(document, attr, None)
            if value:
                parts.append(f"doc_{attr}:{value}")

    if media_blob:
        parts.append(f"thumb_sha1:{hashlib.sha1(media_blob).hexdigest()}")

    return build_media_signature_digest(parts), media_kind


async def _check_single_media_duplicate(msg: Message) -> bool:
    media_hash, media_kind = await _message_media_signature(msg)
    if not media_hash:
        return False
    raw_text = normalize_space(str(getattr(msg, "message", "") or ""))
    result = await asyncio.to_thread(
        check_and_store_media_duplicate,
        channel_id=str(msg.chat_id),
        message_id=int(msg.id or 0),
        media_hash=media_hash,
        raw_text=raw_text,
        media_kind=media_kind,
        timestamp=int(time.time()),
        history_hours=_media_dupe_history_hours(),
    )
    if not result.duplicate:
        return False
    log_structured(
        LOGGER,
        "media_duplicate_suppressed",
        channel_id=str(msg.chat_id),
        message_id=int(msg.id or 0),
        media_kind=media_kind,
        media_hash=media_hash,
        score=round(float(result.match_score), 4),
        matched_channel_id=result.matched_channel_id,
        matched_message_id=result.matched_message_id,
    )
    return True


async def _check_album_media_duplicate(messages: List[Message]) -> bool:
    if not messages:
        return False
    signatures: list[str] = []
    for msg in messages:
        media_hash, _media_kind = await _message_media_signature(msg)
        if media_hash:
            signatures.append(media_hash)
    album_hash = build_media_signature_digest(signatures)
    if not album_hash:
        return False
    raw_text = normalize_space(
        " ".join(str(getattr(msg, "message", "") or "") for msg in messages)
    )
    first = messages[0]
    result = await asyncio.to_thread(
        check_and_store_media_duplicate,
        channel_id=str(first.chat_id),
        message_id=int(first.id or 0),
        media_hash=album_hash,
        raw_text=raw_text,
        media_kind="album",
        timestamp=int(time.time()),
        history_hours=_media_dupe_history_hours(),
    )
    if not result.duplicate:
        return False
    log_structured(
        LOGGER,
        "album_media_duplicate_suppressed",
        channel_id=str(first.chat_id),
        message_id=int(first.id or 0),
        media_kind="album",
        media_hash=album_hash,
        score=round(float(result.match_score), 4),
        matched_channel_id=result.matched_channel_id,
        matched_message_id=result.matched_message_id,
        album_size=len(messages),
    )
    return True


def _normalize_media_ocr_text(text: str) -> str:
    cleaned = normalize_space(text)
    if not cleaned:
        return ""
    if len(cleaned) < _media_text_ocr_min_chars():
        return ""
    limit = _media_text_ocr_max_chars()
    if len(cleaned) > limit:
        cleaned = f"{cleaned[: limit - 3].rsplit(' ', 1)[0]}..."
    return cleaned


async def _extract_media_ocr_translation(msg: Message) -> str | None:
    if not _media_text_ocr_enabled():
        return None
    if not getattr(msg, "media", None):
        return None

    ocr_text = ""

    if _message_is_image_ocr_candidate(msg):
        blob = await msg.download_media(file=bytes)
        if not blob:
            return None
        ocr_text = await asyncio.to_thread(
            extract_ocr_text_from_image_bytes,
            blob,
            max_chars=_media_text_ocr_max_chars(),
            languages=_media_text_ocr_langs(),
        )
    elif _message_is_video_ocr_candidate(msg):
        if not _media_text_ocr_video_enabled():
            return None
        media_size = int(getattr(getattr(msg, "file", None), "size", 0) or 0)
        if media_size and media_size > _media_text_ocr_video_max_bytes():
            return None
        blob = await msg.download_media(file=bytes)
        if not blob:
            return None
        frame_blob = await asyncio.to_thread(extract_first_video_frame_bytes, blob)
        if not frame_blob:
            return None
        ocr_text = await asyncio.to_thread(
            extract_ocr_text_from_image_bytes,
            frame_blob,
            max_chars=_media_text_ocr_max_chars(),
            languages=_media_text_ocr_langs(),
        )
    else:
        return None

    normalized = _normalize_media_ocr_text(ocr_text)
    if not normalized:
        return None

    translated = await translate_ocr_text_to_english(normalized, _require_auth_manager())
    if not translated:
        return None
    return sanitize_telegram_html(translated)


async def _caption_for_media_without_text(msg: Message) -> str | None:
    """
    Media-only posts get caption text only when OCR finds non-English text that
    can be translated. No source-label fallback, no visual descriptions.
    """
    return await _extract_media_ocr_translation(msg)


async def _caption_for_album_without_text(messages: List[Message]) -> str | None:
    if not messages or not _media_text_ocr_enabled():
        return None

    snippets: List[str] = []
    seen_plain: set[str] = set()
    for msg in messages:
        translated = await _extract_media_ocr_translation(msg)
        if not translated:
            continue
        plain = normalize_space(strip_telegram_html(translated)).lower()
        if not plain or plain in seen_plain:
            continue
        seen_plain.add(plain)
        snippets.append(translated)
        if len(snippets) >= 3:
            break

    if not snippets:
        return None

    combined = "\n\n".join(snippets)
    limit = min(_media_text_ocr_max_chars(), 1000)
    plain = strip_telegram_html(combined)
    if len(plain) <= limit:
        return combined
    truncated = plain[: limit - 3].rsplit(" ", 1)[0].rstrip()
    return sanitize_telegram_html(f"{truncated}...")


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


def _archive_for_query_search(
    channel_id: str,
    message_id: int,
    raw_text: str,
    *,
    source_name: str | None = None,
    message_link: str | None = None,
) -> None:
    save_to_digest_archive(
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


def _reply_to_message_id(msg: Message) -> int:
    reply = getattr(msg, "reply_to", None)
    value = int(getattr(reply, "reply_to_msg_id", 0) or 0)
    return value if value > 0 else 0


def _source_reply_map_history_hours() -> int:
    return 24 * 7


def _message_text_content(msg: Message | None) -> str:
    if msg is None:
        return ""
    return normalize_space((getattr(msg, "message", None) or "").strip())


async def _load_reply_message(msg: Message) -> Message | None:
    if not _reply_to_message_id(msg):
        return None
    try:
        reply = await msg.get_reply_message()
    except Exception:
        LOGGER.debug("Failed to load replied source message.", exc_info=True)
        return None
    return reply if isinstance(reply, Message) else None


def _truncate_context_line(text: str, limit: int = 240) -> str:
    cleaned = normalize_space(text)
    if len(cleaned) <= limit:
        return cleaned
    truncated = cleaned[: limit - 3].rsplit(" ", 1)[0].rstrip()
    return f"{truncated}..."


def _clean_followup_context_line(text: str) -> str:
    cleaned = normalize_space(text)
    if not cleaned:
        return ""

    cleaned = re.sub(
        r"\baccording to [^.!,;:]+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\bbased on [^.!,;:]+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\bcirculating on telegram\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\breportedly\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" ,;:-")
    return normalize_space(cleaned)


def _format_followup_media_caption(source_title: str, context_line: str) -> str | None:
    cleaned = _clean_followup_context_line(context_line)
    cleaned = _truncate_context_line(cleaned, limit=260)
    if not cleaned:
        return None
    source_html = sanitize_telegram_html((source_title or "").strip() or "Source")
    context_html = sanitize_telegram_html(cleaned)
    return f"<b>{source_html}</b><br><br><i>Follow-up visuals:</i> {context_html}"


async def _build_reply_context_caption(
    source_title: str,
    reply_message: Message | None,
) -> tuple[str | None, str | None]:
    reply_text = _message_text_content(reply_message)
    if not reply_text:
        return None, None

    headline = ""
    try:
        headline = await summarize_breaking_headline(reply_text, _require_auth_manager())
    except Exception:
        LOGGER.debug("Reply-context headline generation failed.", exc_info=True)
        headline = ""

    context_line = normalize_space(headline or reply_text)
    if not context_line:
        return None, None
    return _format_followup_media_caption(source_title, context_line), reply_text


async def _resolve_source_reply_target(
    msg: Message,
    *,
    fallback_topic_seed: str | None = None,
) -> int | None:
    reply_message = await _load_reply_message(msg)
    if reply_message is not None:
        mapped = load_source_delivery_ref(
            channel_id=str(reply_message.chat_id),
            source_message_id=int(reply_message.id or 0),
        )
        if mapped:
            return mapped

        reply_text = _message_text_content(reply_message)
        if reply_text and not fallback_topic_seed:
            fallback_topic_seed = reply_text

    if fallback_topic_seed:
        return await _resolve_breaking_topic_reply(fallback_topic_seed)
    return None


def _register_source_delivery_refs(
    *,
    channel_id: str,
    source_message_ids: Sequence[int],
    sent_ref: object | None,
) -> None:
    destination_message_id = _message_ref_id(sent_ref)
    if not destination_message_id:
        return
    with contextlib.suppress(Exception):
        purge_source_delivery_refs(history_hours=_source_reply_map_history_hours())
    for source_message_id in {int(item) for item in source_message_ids if int(item or 0) > 0}:
        save_source_delivery_ref(
            channel_id=channel_id,
            source_message_id=source_message_id,
            destination_message_id=destination_message_id,
        )


def _classify_severity_with_breakdown(
    *,
    text: str,
    source: str,
    channel_id: str,
    message_id: int,
    has_media: bool,
    has_link: bool,
    reply_to: int,
    album_size: int = 1,
) -> tuple[str, float, dict]:
    payload = {
        "text": text,
        "source": source,
        "channel_id": channel_id,
        "message_id": message_id,
        "has_media": bool(has_media),
        "has_link": bool(has_link),
        "reply_to": int(reply_to or 0),
        "timestamp": int(time.time()),
        "text_tokens": estimate_tokens_rough(text),
        # Integrates existing runtime setting into deterministic score.
        "humanized_vital_probability": _humanized_vital_opinion_probability(),
    }
    try:
        severity, score, breakdown = classify_message_severity(payload)
    except Exception as exc:
        severity, score = "medium", 0.0
        breakdown = {
            "error": f"severity_classifier_failed: {exc}",
            "fallback": "medium",
        }

    log_structured(
        LOGGER,
        "severity_classified",
        channel_id=channel_id,
        message_id=message_id,
        source=source,
        severity=severity,
        score=score,
        album_size=album_size,
        has_media=bool(has_media),
        has_link=bool(has_link),
        reply_to=int(reply_to or 0),
        breakdown=breakdown,
    )
    return severity, score, breakdown


def _format_breaking_text(source_title: str, headline: str, rational_view: str | None = None) -> str:
    def _clean_rational_view(value: str) -> str:
        text = normalize_space(str(value or ""))
        if not text:
            return ""
        text = text.rstrip(".")
        text = re.sub(r"\.\.\.+$", "", text).strip()
        words = text.split()
        if words:
            tail = words[-1].strip(".,;:!?").lower()
            if tail in {
                "and",
                "or",
                "but",
                "because",
                "while",
                "with",
                "without",
                "if",
                "as",
                "to",
                "of",
                "for",
                "in",
                "on",
                "at",
                "from",
                "by",
                "amid",
                "pending",
            }:
                words.pop()
                text = " ".join(words).strip()
        if not text:
            return ""
        if not text.lower().startswith("why it matters:"):
            text = f"Why it matters: {text}"
        text = text.rstrip(".,;:!?")
        return f"{text}."

    clean_headline = normalize_space(headline)
    safe_headline = sanitize_telegram_html(clean_headline)
    def _normalize_structured_block(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"^\s*why it matters:\s*", "", text, flags=re.IGNORECASE)
        text = text.replace("\r", "\n")
        text = re.sub(r"\n{2,}", "\n", text)
        # Split compressed bullets into separate lines.
        if "\n" not in text and "<br>" not in text.lower():
            text = re.sub(r"\s+•\s*", "<br>• ", text)
            text = re.sub(
                r"\s+(What:|Where:|When:|Location:|Status:)",
                r"<br>• \1",
                text,
                flags=re.IGNORECASE,
            )
        return sanitize_telegram_html(text)

    rational_part = ""
    if rational_view:
        lowered = str(rational_view or "").lower()
        is_structured = (
            "<b>what:" in lowered
            or "•" in lowered
            or "<br>" in lowered
            or "\n" in str(rational_view or "")
        )
        if is_structured:
            block = _normalize_structured_block(str(rational_view))
            if block:
                rational_part = f"<br><br>{block}"
        else:
            cleaned_rational = _clean_rational_view(rational_view)
            if cleaned_rational:
                rational_part = f"<br><br><i>{sanitize_telegram_html(cleaned_rational)}</i>"
    header = build_alert_header(
        clean_headline,
        severity="high",
        source_title=source_title,
        include_source=_include_source_tags(),
    )
    return f"{header}<br><br>{safe_headline}{rational_part}"


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


_BREAKING_TOPIC_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "with",
    "after",
    "before",
    "near",
    "over",
    "under",
    "says",
    "said",
    "reports",
    "report",
    "breaking",
    "urgent",
    "update",
    "developing",
    "live",
    "news",
}


def _breaking_topic_tokens(text: str) -> set[str]:
    normalized, _ = build_dupe_fingerprint(text)
    if not normalized:
        return set()
    tokens = set(re.findall(r"[a-z0-9]{3,}", normalized))
    return {token for token in tokens if token not in _BREAKING_TOPIC_STOPWORDS}


def _breaking_topic_normalized(text: str) -> str:
    normalized, _ = build_dupe_fingerprint(text)
    return normalized


def _breaking_topic_similarity(
    left_tokens: set[str],
    right_tokens: set[str],
) -> tuple[int, float]:
    if not left_tokens or not right_tokens:
        return 0, 0.0
    overlap = len(left_tokens & right_tokens)
    if overlap <= 0:
        return 0, 0.0
    ratio = overlap / max(1, min(len(left_tokens), len(right_tokens)))
    return overlap, ratio


def _breaking_topic_fuzzy_similarity(left_norm: str, right_norm: str) -> float:
    if not left_norm or not right_norm:
        return 0.0
    from difflib import SequenceMatcher

    return float(SequenceMatcher(None, left_norm, right_norm).ratio())


def _cleanup_breaking_topic_threads_locked(now_ts: int | None = None) -> None:
    now = int(now_ts if now_ts is not None else time.time())
    cutoff = now - _breaking_topic_window_seconds()
    breaking_topic_threads[:] = [
        entry
        for entry in breaking_topic_threads
        if int(entry.get("ts", 0)) >= cutoff and int(entry.get("message_id", 0)) > 0
    ]
    max_items = 300
    if len(breaking_topic_threads) > max_items:
        breaking_topic_threads.sort(key=lambda item: int(item.get("ts", 0)), reverse=True)
        del breaking_topic_threads[max_items:]


async def _resolve_breaking_topic_reply(topic_seed: str) -> int | None:
    if not _is_breaking_topic_threads_enabled():
        return None
    tokens = _breaking_topic_tokens(topic_seed)
    current_norm = _breaking_topic_normalized(topic_seed)
    if not current_norm:
        return None
    if len(tokens) < _breaking_topic_min_overlap():
        return None

    now = int(time.time())
    best_message_id: int | None = None
    best_overlap = 0
    best_ratio = 0.0
    best_fuzzy = 0.0
    best_ts = 0
    min_overlap = _breaking_topic_min_overlap()
    min_ratio = _breaking_topic_min_ratio()
    min_fuzzy = _breaking_topic_fuzzy_ratio()

    async with breaking_topic_lock:
        _cleanup_breaking_topic_threads_locked(now)
        for entry in breaking_topic_threads:
            entry_tokens = entry.get("tokens")
            if not isinstance(entry_tokens, set):
                continue
            overlap, ratio = _breaking_topic_similarity(tokens, entry_tokens)
            fuzzy = _breaking_topic_fuzzy_similarity(
                current_norm,
                str(entry.get("norm", "") or ""),
            )
            passes = (overlap >= min_overlap and ratio >= min_ratio) or (fuzzy >= min_fuzzy)
            if not passes:
                continue
            entry_ts = int(entry.get("ts", 0))
            if overlap > best_overlap:
                best_overlap = overlap
                best_ratio = ratio
                best_fuzzy = fuzzy
                best_ts = entry_ts
                best_message_id = int(entry.get("message_id", 0)) or None
            elif overlap == best_overlap and ratio > best_ratio:
                best_ratio = ratio
                best_fuzzy = fuzzy
                best_ts = entry_ts
                best_message_id = int(entry.get("message_id", 0)) or None
            elif overlap == best_overlap and abs(ratio - best_ratio) < 1e-6 and fuzzy > best_fuzzy:
                best_fuzzy = fuzzy
                best_ts = entry_ts
                best_message_id = int(entry.get("message_id", 0)) or best_message_id
            elif (
                overlap == best_overlap
                and abs(ratio - best_ratio) < 1e-6
                and abs(fuzzy - best_fuzzy) < 1e-6
            ):
                # Prefer more recent thread if score ties.
                if entry_ts > best_ts:
                    best_ts = entry_ts
                    best_message_id = int(entry.get("message_id", 0)) or best_message_id

    return best_message_id


async def _register_breaking_topic_thread(
    *,
    topic_seed: str,
    sent_ref: object | None,
) -> None:
    if not _is_breaking_topic_threads_enabled():
        return
    message_id = _message_ref_id(sent_ref)
    if not message_id:
        return
    tokens = _breaking_topic_tokens(topic_seed)
    norm = _breaking_topic_normalized(topic_seed)
    if not norm:
        return
    if len(tokens) < _breaking_topic_min_overlap():
        return

    now = int(time.time())
    async with breaking_topic_lock:
        _cleanup_breaking_topic_threads_locked(now)
        for entry in breaking_topic_threads:
            if int(entry.get("message_id", 0)) == int(message_id):
                entry["tokens"] = tokens
                entry["norm"] = norm
                entry["ts"] = now
                return
        breaking_topic_threads.append(
            {
                "message_id": int(message_id),
                "tokens": tokens,
                "norm": norm,
                "ts": now,
            }
        )


def _prepend_breaking_continuity(payload: str) -> str:
    prefix = sanitize_telegram_html(_breaking_topic_continuity_prefix())
    if not prefix:
        return payload
    return f"<i>{prefix}</i><br><br>{payload}"


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
    merged_text = f"{base_text}<br><br><i>(also reported by {sanitize_telegram_html(suffix)})</i>"
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
            use_sentence_transformers=_dupe_use_sentence_transformers(),
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
            use_sentence_transformers=_dupe_use_sentence_transformers(),
            backend=detector.backend_name,
        )
    except Exception:
        dupe_detector = None
        configure_duplicate_runtime(None)
        LOGGER.exception("Failed to initialize near-duplicate detector. Disabling dedupe.")


async def _send_text(text: str) -> None:
    await _send_text_with_ref(text)


def _message_ref_id(ref: object) -> int | None:
    if isinstance(ref, (list, tuple)):
        for item in ref:
            value = _message_ref_id(item)
            if value:
                return value
        return None
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


def _acquire_instance_lock():
    """
    Ensure only one process uses the Telethon session DB at a time.
    """
    lock_path = ensure_runtime_dir() / "userbot.lock"
    handle = open(lock_path, "a+", encoding="utf-8")
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        owner = ""
        try:
            handle.seek(0)
            owner = handle.read().strip()
        except Exception:
            owner = ""
        handle.close()
        owner_msg = f" (lock owner PID: {owner})" if owner else ""
        raise RuntimeError(
            "Another TeleUserBot instance is already running. "
            f"Stop it before starting a new one{owner_msg}."
        )
    except Exception:
        handle.close()
        raise

    try:
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        os.fsync(handle.fileno())
    except Exception:
        # Lock is held; metadata write failure is non-fatal.
        pass

    return handle


def _release_instance_lock(handle) -> None:
    if handle is None:
        return
    try:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    with contextlib.suppress(Exception):
        handle.close()


def _is_session_db_locked_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "database is locked" in str(exc).lower()


def _is_reply_target_missing_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "message to be replied not found" in text and "reply" in text


async def _send_text_with_ref(text: str, reply_to: int | None = None) -> object:
    if _destination_uses_bot_api():
        payload_text = (
            _render_outbound_text(text, allow_premium_tags=True)
            if _is_html_formatting_enabled()
            else text
        )
        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "text": _to_bot_text(payload_text) or "",
            "disable_web_page_preview": "true",
            "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
        }
        if reply_to:
            data["reply_to_message_id"] = str(reply_to)
        try:
            result = await _bot_api_request("sendMessage", data=data)
            return result
        except Exception as exc:
            if reply_to and _is_reply_target_missing_error(exc):
                LOGGER.debug("Reply target missing for sendMessage; retrying without reply_to.")
                retry_data = dict(data)
                retry_data.pop("reply_to_message_id", None)
                return await _bot_api_request("sendMessage", data=retry_data)
            # If HTML parse fails, strip tags and retry as plain text.
            data.pop("parse_mode", None)
            fallback_text = (
                strip_telegram_html(payload_text) if _is_html_formatting_enabled() else payload_text
            )
            data["text"] = _to_plain_text(fallback_text) or ""
            return await _bot_api_request("sendMessage", data=data)

    tg = _require_client()
    payload_text = (
        _render_outbound_text(text, allow_premium_tags=False)
        if _is_html_formatting_enabled()
        else text
    )
    sent = await _call_with_floodwait(
        tg.send_message,
        _require_destination_peer(),
        payload_text,
        reply_to=reply_to,
        parse_mode="html" if _is_html_formatting_enabled() else "md",
        link_preview=False,
    )
    return sent


async def _edit_sent_text(ref: object, text: str) -> object:
    if _destination_uses_bot_api():
        payload_text = (
            _render_outbound_text(text, allow_premium_tags=True)
            if _is_html_formatting_enabled()
            else text
        )
        message_id = _message_ref_id(ref)
        if not message_id:
            raise RuntimeError("Cannot edit Bot API message without message_id.")

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "message_id": str(message_id),
            "text": _to_bot_text(payload_text) or "",
            "disable_web_page_preview": "true",
            "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
        }
        try:
            result = await _bot_api_request("editMessageText", data=data)
            # Bot API can return `True` for some cases; keep original ref for continuity.
            if isinstance(result, dict):
                return result
            return ref
        except Exception:
            data.pop("parse_mode", None)
            fallback_text = (
                strip_telegram_html(payload_text) if _is_html_formatting_enabled() else payload_text
            )
            data["text"] = _to_plain_text(fallback_text) or ""
            result = await _bot_api_request("editMessageText", data=data)
            if isinstance(result, dict):
                return result
            return ref

    if not isinstance(ref, Message):
        raise RuntimeError("Telethon edit requires Message reference.")
    payload_text = (
        _render_outbound_text(text, allow_premium_tags=False)
        if _is_html_formatting_enabled()
        else text
    )
    try:
        return await ref.edit(
            payload_text,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            link_preview=False,
        )
    except Exception:
        fallback_text = (
            strip_telegram_html(payload_text) if _is_html_formatting_enabled() else payload_text
        )
        return await ref.edit(fallback_text, parse_mode=None, link_preview=False)


async def _send_single_media(
    msg: Message,
    caption: str | None,
    *,
    reply_to: int | None = None,
) -> object:
    if _destination_uses_bot_api():
        safe_caption = (
            _render_outbound_text(caption or "", allow_premium_tags=True)
            if (caption and _is_html_formatting_enabled())
            else caption
        )
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
            "caption": _to_bot_text(safe_caption, max_len=1024) or "",
            "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
        }
        if reply_to:
            data["reply_to_message_id"] = str(reply_to)
        files = {
            upload_field: (
                _filename_for_bot_media(msg, 0),
                raw,
                "application/octet-stream",
            )
        }
        try:
            return await _bot_api_request(method_map[media_type], data=data, files=files)
        except Exception as exc:
            if reply_to and _is_reply_target_missing_error(exc):
                LOGGER.debug("Reply target missing for media send; retrying without reply_to.")
                retry_data = dict(data)
                retry_data.pop("reply_to_message_id", None)
                return await _bot_api_request(method_map[media_type], data=retry_data, files=files)
            data.pop("parse_mode", None)
            if safe_caption:
                fallback_caption = (
                    strip_telegram_html(safe_caption)
                    if _is_html_formatting_enabled()
                    else safe_caption
                )
                data["caption"] = _to_plain_text(fallback_caption, max_len=1024) or ""
            return await _bot_api_request(method_map[media_type], data=data, files=files)

    tg = _require_client()
    safe_caption = (
        _render_outbound_text(caption or "", allow_premium_tags=False)
        if (caption and _is_html_formatting_enabled())
        else caption
    )
    try:
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            msg.media,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )
    except ChatForwardsRestrictedError:
        raw = await msg.download_media(file=bytes)
        if raw is None:
            raise RuntimeError("Failed to download restricted media for re-send.")
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            raw,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )


async def _send_album(
    messages: List[Message],
    caption: str | None,
    *,
    reply_to: int | None = None,
) -> object | None:
    if _destination_uses_bot_api():
        safe_caption = (
            _render_outbound_text(caption or "", allow_premium_tags=True)
            if (caption and _is_html_formatting_enabled())
            else caption
        )
        if not messages:
            if safe_caption:
                return await _send_text_with_ref(safe_caption, reply_to=reply_to)
            return

        if len(messages) == 1:
            return await _send_single_media(messages[0], safe_caption, reply_to=reply_to)

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
            if idx == 0 and safe_caption:
                item["caption"] = _to_bot_text(safe_caption, max_len=1024) or ""
                item["parse_mode"] = "HTML" if _is_html_formatting_enabled() else "Markdown"
            media_entries.append(item)

        if not media_entries:
            raise RuntimeError("Failed to download album media for bot destination.")

        if len(media_entries) == 1:
            only_msg = downloaded_messages[0]
            return await _send_single_media(only_msg, safe_caption, reply_to=reply_to)

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "media": json.dumps(media_entries),
        }
        if reply_to:
            data["reply_to_message_id"] = str(reply_to)
        try:
            return await _bot_api_request("sendMediaGroup", data=data, files=files)
        except Exception as exc:
            if reply_to and _is_reply_target_missing_error(exc):
                LOGGER.debug("Reply target missing for media group send; retrying without reply_to.")
                retry_data = dict(data)
                retry_data.pop("reply_to_message_id", None)
                return await _bot_api_request("sendMediaGroup", data=retry_data, files=files)
            if safe_caption and media_entries:
                fallback_caption = (
                    strip_telegram_html(safe_caption)
                    if _is_html_formatting_enabled()
                    else safe_caption
                )
                media_entries[0]["caption"] = _to_plain_text(
                    fallback_caption,
                    max_len=1024,
                ) or ""
                media_entries[0].pop("parse_mode", None)
                data["media"] = json.dumps(media_entries)
            return await _bot_api_request("sendMediaGroup", data=data, files=files)
        return

    tg = _require_client()
    safe_caption = (
        _render_outbound_text(caption or "", allow_premium_tags=False)
        if (caption and _is_html_formatting_enabled())
        else caption
    )
    media_items = [m.media for m in messages if m.media]
    if not media_items:
        if safe_caption:
            return await _send_text_with_ref(safe_caption, reply_to=reply_to)
        return

    try:
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            media_items,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )
    except ChatForwardsRestrictedError:
        downloaded = []
        for msg in messages:
            blob = await msg.download_media(file=bytes)
            if blob is not None:
                downloaded.append(blob)
        if not downloaded:
            raise RuntimeError("Failed to download album media for restricted chat.")
        return await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            downloaded,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )


async def _process_single_message(msg: Message) -> None:
    channel_id = str(msg.chat_id)
    if is_seen(channel_id, msg.id):
        return

    text = (msg.message or "").strip()
    source, link = await _source_info(msg)

    if msg.media and not text:
        reply_message = await _load_reply_message(msg)
        reply_to = await _resolve_source_reply_target(msg)
        media_caption = await _caption_for_media_without_text(msg)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        sent_ref = await _send_single_media(msg, media_caption, reply_to=reply_to)
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=[msg.id],
            sent_ref=sent_ref,
        )
        archive_text = strip_telegram_html(media_caption) if media_caption else reply_context_text
        if archive_text:
            _archive_for_query_search(
                channel_id,
                msg.id,
                archive_text,
                source_name=source,
                message_link=link,
            )
        mark_seen(channel_id, msg.id)
        return

    if msg.media and text:
        summary = await summarize_or_skip(text, _require_auth_manager())
        if summary is None:
            mark_seen(channel_id, msg.id)
            return
        reply_to = await _resolve_source_reply_target(msg)
        sent_ref = await _send_single_media(
            msg,
            _format_summary_text(source, summary),
            reply_to=reply_to,
        )
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=[msg.id],
            sent_ref=sent_ref,
        )
        mark_seen(channel_id, msg.id)
        return

    if not text:
        mark_seen(channel_id, msg.id)
        return

    summary = await summarize_or_skip(text, _require_auth_manager())
    if summary is None:
        mark_seen(channel_id, msg.id)
        return

    reply_to = await _resolve_source_reply_target(msg)
    sent_ref = await _send_text_with_ref(_format_summary_text(source, summary), reply_to=reply_to)
    _register_source_delivery_refs(
        channel_id=channel_id,
        source_message_ids=[msg.id],
        sent_ref=sent_ref,
    )
    mark_seen(channel_id, msg.id)


async def _process_album(messages: List[Message]) -> None:
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

    if not combined_caption:
        reply_message = await _load_reply_message(messages[0])
        reply_to = await _resolve_source_reply_target(messages[0])
        media_caption = await _caption_for_album_without_text(messages)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        sent_ref = await _send_album(messages, media_caption, reply_to=reply_to)
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=sent_ref,
        )
        archive_text = strip_telegram_html(media_caption) if media_caption else reply_context_text
        if archive_text:
            _archive_for_query_search(
                channel_id,
                messages[0].id,
                archive_text,
                source_name=source,
                message_link=link,
            )
        mark_seen_many(channel_id, message_ids)
        return

    summary = await summarize_or_skip(combined_caption, _require_auth_manager())
    if summary is None:
        mark_seen_many(channel_id, message_ids)
        return

    reply_to = await _resolve_source_reply_target(messages[0])
    sent_ref = await _send_album(
        messages,
        _format_summary_text(source, summary),
        reply_to=reply_to,
    )
    _register_source_delivery_refs(
        channel_id=channel_id,
        source_message_ids=message_ids,
        sent_ref=sent_ref,
    )
    mark_seen_many(channel_id, message_ids)


async def _queue_single_message_for_digest(msg: Message) -> None:
    channel_id = str(msg.chat_id)
    if is_seen(channel_id, msg.id):
        return

    text = (msg.message or "").strip()
    source, link = await _source_info(msg)

    if msg.media and not text:
        reply_message = await _load_reply_message(msg)
        reply_to = await _resolve_source_reply_target(msg)
        media_caption = await _caption_for_media_without_text(msg)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        sent_ref = await _send_single_media(msg, media_caption, reply_to=reply_to)
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=[msg.id],
            sent_ref=sent_ref,
        )
        archive_text = strip_telegram_html(media_caption) if media_caption else reply_context_text
        if archive_text:
            _archive_for_query_search(
                channel_id,
                msg.id,
                archive_text,
                source_name=source,
                message_link=link,
            )
        mark_seen(channel_id, msg.id)
        return

    if not text:
        mark_seen(channel_id, msg.id)
        return

    severity = "medium"
    severity_score = 0.0
    severity_breakdown: dict = {}
    if _is_severity_routing_enabled():
        severity, severity_score, severity_breakdown = _classify_severity_with_breakdown(
            text=text,
            source=source,
            channel_id=channel_id,
            message_id=msg.id,
            has_media=bool(msg.media),
            has_link=bool(link),
            reply_to=_reply_to_message_id(msg),
        )
    elif _contains_breaking_keyword(text):
        severity = "high"

    if severity != "high" and _contains_breaking_keyword(text):
        severity = "high"
        severity_score = max(float(severity_score), 0.82)
        severity_breakdown = dict(severity_breakdown or {})
        severity_breakdown["keyword_override"] = True

    if severity == "high" and _is_immediate_high_enabled():
        _normalized, text_hash = build_dupe_fingerprint(text)
        headline = await summarize_breaking_headline(text, _require_auth_manager())
        if not headline:
            headline = normalize_space(text)
            if len(headline) > 420:
                headline = f"{headline[:417].rsplit(' ', 1)[0]}..."
        rational_view: str | None = None
        if _should_attach_vital_opinion(text):
            rational_view = await summarize_vital_rational_view(text, _require_auth_manager())
        payload = _format_breaking_text(source, headline, rational_view)
        topic_seed = f"{headline}\n{text}"
        reply_to = await _resolve_source_reply_target(msg, fallback_topic_seed=topic_seed)
        if msg.media:
            sent_ref = await _send_single_media(msg, payload, reply_to=reply_to)
        else:
            sent_ref = await _send_text_with_ref(payload, reply_to=reply_to)
            _register_breaking_delivery(
                text_hash=text_hash,
                ref=sent_ref,
                base_text=payload,
                primary_source=source,
            )
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=[msg.id],
            sent_ref=sent_ref,
        )
        await _register_breaking_topic_thread(topic_seed=topic_seed, sent_ref=sent_ref)
        _archive_for_query_search(
            channel_id,
            msg.id,
            text,
            source_name=source,
            message_link=link,
        )
        mark_seen(channel_id, msg.id)
        log_structured(
            LOGGER,
            "breaking_sent_immediate",
            channel_id=channel_id,
            message_id=msg.id,
            source=source,
            severity=severity,
            severity_score=severity_score,
            rational_view=bool(rational_view),
            dedupe_hash=text_hash,
            reply_to=reply_to or 0,
            severity_breakdown=severity_breakdown,
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
        severity_score=severity_score,
        severity_emoji=_severity_emoji(severity),
        has_link=bool(link),
        text_tokens=estimate_tokens_rough(text),
        pending=count_pending(),
        severity_breakdown=severity_breakdown,
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

    if not combined_caption:
        reply_message = await _load_reply_message(messages[0])
        reply_to = await _resolve_source_reply_target(messages[0])
        media_caption = await _caption_for_album_without_text(messages)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        sent_ref = await _send_album(messages, media_caption, reply_to=reply_to)
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=sent_ref,
        )
        archive_text = strip_telegram_html(media_caption) if media_caption else reply_context_text
        if archive_text:
            _archive_for_query_search(
                channel_id,
                messages[0].id,
                archive_text,
                source_name=source,
                message_link=link,
            )
        mark_seen_many(channel_id, message_ids)
        return

    severity = "medium"
    severity_score = 0.0
    severity_breakdown: dict = {}
    if _is_severity_routing_enabled():
        severity, severity_score, severity_breakdown = _classify_severity_with_breakdown(
            text=combined_caption,
            source=source,
            channel_id=channel_id,
            message_id=messages[0].id,
            has_media=True,
            has_link=bool(link),
            reply_to=_reply_to_message_id(messages[0]),
            album_size=len(messages),
        )
    elif _contains_breaking_keyword(combined_caption):
        severity = "high"

    if severity != "high" and _contains_breaking_keyword(combined_caption):
        severity = "high"
        severity_score = max(float(severity_score), 0.82)
        severity_breakdown = dict(severity_breakdown or {})
        severity_breakdown["keyword_override"] = True

    if severity == "high" and _is_immediate_high_enabled():
        headline = await summarize_breaking_headline(combined_caption, _require_auth_manager())
        if not headline:
            headline = normalize_space(combined_caption)
            if len(headline) > 420:
                headline = f"{headline[:417].rsplit(' ', 1)[0]}..."
        rational_view: str | None = None
        if _should_attach_vital_opinion(combined_caption):
            rational_view = await summarize_vital_rational_view(
                combined_caption,
                _require_auth_manager(),
            )
        payload = _format_breaking_text(source, headline, rational_view)
        topic_seed = f"{headline}\n{combined_caption}"
        reply_to = await _resolve_source_reply_target(messages[0], fallback_topic_seed=topic_seed)
        sent_ref = await _send_album(messages, payload, reply_to=reply_to)
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=sent_ref,
        )
        await _register_breaking_topic_thread(topic_seed=topic_seed, sent_ref=sent_ref)
        _archive_for_query_search(
            channel_id,
            messages[0].id,
            combined_caption,
            source_name=source,
            message_link=link,
        )
        mark_seen_many(channel_id, message_ids)
        log_structured(
            LOGGER,
            "breaking_album_sent_immediate",
            channel_id=channel_id,
            first_message_id=messages[0].id,
            album_size=len(messages),
            source=source,
            severity=severity,
            severity_score=severity_score,
            rational_view=bool(rational_view),
            reply_to=reply_to or 0,
            severity_breakdown=severity_breakdown,
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
        severity_score=severity_score,
        severity_emoji=_severity_emoji(severity),
        has_link=bool(link),
        text_tokens=estimate_tokens_rough(combined_caption),
        pending=count_pending(),
        severity_breakdown=severity_breakdown,
    )


def _format_digest_message(
    digest_body: str,
    total_updates: int,
    sources: List[str],
    *,
    title: str = "Digest",
) -> str:
    def _count_digest_headlines(body: str) -> int:
        plain = normalize_space(strip_telegram_html(body))
        if not plain:
            return 0
        lowered = plain.lower()
        if "no major developments" in lowered or "quiet period" in lowered:
            return 0

        count = 0
        for raw_line in body.replace("<br><br>", "\n").replace("<br>", "\n").splitlines():
            line = normalize_space(strip_telegram_html(raw_line))
            if not line:
                continue
            if re.match(r"^[•-]\s+", line):
                count += 1
                continue
            if re.match(r"^[\U0001F300-\U0001FAFF\u2600-\u27BF]", line):
                count += 1
                continue
        return count

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    label = sanitize_telegram_html(title.strip() or "Digest")
    headline_count = _count_digest_headlines(digest_body)
    if headline_count > 0:
        header = (
            f"<b>📰 {label} • {timestamp} • "
            f"{headline_count} headlines from {total_updates} updates</b>"
        )
    else:
        header = f"<b>📰 {label} • {timestamp} • {total_updates} updates reviewed</b>"
    _ = sources
    return sanitize_telegram_html(f"{header}<br><br>{digest_body.strip()}")


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
    chunks = split_html_chunks(text, max_chars=_digest_send_chunk_size())
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
                    html_mode=_is_html_formatting_enabled(),
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
                title=f"Hourly Digest (last {max(1, _digest_interval_seconds() // 60)}m)",
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


async def _send_daily_archive_digest_once() -> None:
    async with digest_loop_lock:
        window_hours = _digest_daily_window_hours()
        interval_minutes = max(1, window_hours * 60)
        now_ts = int(time.time())
        since_ts = max(0, now_ts - window_hours * 3600)
        max_rows = _digest_daily_max_posts()
        rows = load_archive_since(since_ts, max_rows)

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

        log_structured(
            LOGGER,
            "daily_digest_batch_loaded",
            window_hours=window_hours,
            selected=len(posts),
            max_rows=max_rows,
        )

        digest_body = ""
        stream_stats = None
        try:
            if _is_streaming_enabled():
                streamer = LiveTelegramStreamer(
                    send_message=lambda text, reply_to: _send_text_with_ref(text, reply_to=reply_to),
                    edit_message=_edit_sent_text,
                    get_message_id=lambda ref: _message_ref_id(ref) or 0,
                    placeholder_text="Generating 24h digest... ⏳",
                    edit_interval_ms=_stream_edit_interval_ms(),
                    max_chars_per_edit=_stream_max_chars_per_edit(),
                    typing_enabled=False,
                    html_mode=_is_html_formatting_enabled(),
                )
                await streamer.start()
                digest_body = await create_digest_summary(
                    posts,
                    _require_auth_manager(),
                    interval_minutes=interval_minutes,
                    on_token=streamer.push,
                )
            else:
                digest_body = await create_digest_summary(
                    posts,
                    _require_auth_manager(),
                    interval_minutes=interval_minutes,
                )

            if not digest_body.strip():
                digest_body = quiet_period_message(interval_minutes)

            digest_message = _format_digest_message(
                digest_body=digest_body,
                total_updates=len(posts),
                sources=source_names,
                title=f"24h Digest (last {window_hours}h)",
            )
            if _is_streaming_enabled():
                stream_stats = await streamer.finalize(digest_message)
            else:
                await _send_digest_message(digest_message)

            set_last_digest_timestamp(int(time.time()))
            log_structured(
                LOGGER,
                "daily_digest_sent",
                window_hours=window_hours,
                rows=len(posts),
                response_chars=len(digest_body),
                stream_enabled=_is_streaming_enabled(),
                stream_edits=(stream_stats.edit_count if stream_stats else 0),
                stream_tokens_per_second=(
                    round(stream_stats.tokens_per_second, 3) if stream_stats else 0.0
                ),
            )
        except Exception:
            LOGGER.exception("Daily archive digest generation/sending failed.")
        finally:
            # Keep rolling history only; 3x window retention provides re-run safety.
            retention_hours = max(72, window_hours * 3)
            pruned = prune_archive_older_than(now_ts - retention_hours * 3600)
            if pruned:
                log_structured(
                    LOGGER,
                    "digest_archive_pruned",
                    pruned=pruned,
                    retention_hours=retention_hours,
                )


def _compute_next_scheduler_delay_seconds() -> int:
    if digest_retry_backoff_seconds > 0:
        return digest_retry_backoff_seconds
    return _effective_interval_seconds()


async def run_digest_scheduler() -> None:
    global digest_next_run_ts

    log_structured(
        LOGGER,
        "digest_scheduler_start",
        mode="hourly_digest",
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


async def run_daily_digest_scheduler() -> None:
    daily_times = _digest_daily_times()
    if not daily_times:
        return

    log_structured(
        LOGGER,
        "daily_digest_scheduler_start",
        daily_times=[f"{h:02d}:{m:02d}" for h, m in daily_times],
        window_hours=_digest_daily_window_hours(),
    )
    try:
        while True:
            delay = seconds_until_next_daily_time(daily_times)
            await asyncio.sleep(delay)
            await _send_daily_archive_digest_once()
    except asyncio.CancelledError:
        LOGGER.info("Daily digest scheduler stopped.")
        return


async def run_digest_queue_clear_scheduler() -> None:
    interval = _digest_queue_clear_interval_seconds()
    if interval <= 0:
        log_structured(
            LOGGER,
            "digest_queue_clear_scheduler_disabled",
            interval_seconds=interval,
            reason="disabled_in_config",
        )
        return
    scope = _digest_queue_clear_scope()

    log_structured(
        LOGGER,
        "digest_queue_clear_scheduler_start",
        interval_seconds=interval,
        scope=scope,
    )
    try:
        while True:
            await asyncio.sleep(interval)
            async with digest_loop_lock:
                deleted = clear_digest_queue_scoped(scope)
            log_structured(
                LOGGER,
                "digest_queue_cleared",
                deleted=deleted,
                scope=scope,
                pending_after=count_pending(),
                inflight_after=count_inflight(),
            )
    except asyncio.CancelledError:
        LOGGER.info("Digest queue clear scheduler stopped.")
        return


async def _flush_album_after_wait(key: Tuple[str, int]) -> None:
    try:
        await asyncio.sleep(ALBUM_WAIT_SECONDS)
        items = album_buffers.pop(key, [])
        album_tasks.pop(key, None)
        if items:
            items = await _refresh_album_messages(items)
            if await _check_album_media_duplicate(items):
                mark_seen_many(str(items[0].chat_id), [int(m.id or 0) for m in items])
                return
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


async def _refresh_album_messages(messages: List[Message]) -> List[Message]:
    """
    Rehydrate buffered album fragments from Telegram before processing.

    NewMessage album fragments can arrive without the final caption-bearing state.
    Refetching by IDs gives the authoritative server copy and prevents silent
    caption loss on grouped media.
    """
    if not messages:
        return messages

    ordered = sorted(messages, key=lambda m: int(m.id or 0))
    chat_ref = ordered[0].chat_id
    ids = [int(m.id or 0) for m in ordered if int(m.id or 0) > 0]
    if not ids:
        return ordered

    tg = _require_client()
    try:
        refreshed = await _call_with_floodwait(tg.get_messages, chat_ref, ids=ids)
    except Exception:
        LOGGER.debug("Failed to refresh album messages; using buffered fragments.", exc_info=True)
        return ordered

    refreshed_list: List[Message]
    if isinstance(refreshed, Message):
        refreshed_list = [refreshed]
    elif isinstance(refreshed, list):
        refreshed_list = [item for item in refreshed if isinstance(item, Message)]
    else:
        refreshed_list = []

    if not refreshed_list:
        return ordered

    refreshed_by_id = {int(item.id or 0): item for item in refreshed_list if int(item.id or 0) > 0}
    resolved: List[Message] = []
    for original in ordered:
        resolved.append(refreshed_by_id.get(int(original.id or 0), original))
    return sorted(resolved, key=lambda m: int(m.id or 0))


async def _connect_client_with_lock_guard(
    tg: TelegramClient,
    *,
    retries: int = 3,
    retry_delay_seconds: float = 1.5,
) -> None:
    """
    Connect Telethon client with friendly handling for locked SQLite session DB.
    """
    attempts = max(1, int(retries))
    for attempt in range(attempts):
        try:
            await tg.connect()
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "database is locked" not in message:
                raise
            if attempt + 1 < attempts:
                await asyncio.sleep(retry_delay_seconds * (attempt + 1))
                continue
            raise RuntimeError(
                "Telethon session database is locked. "
                "Another bot instance is likely running. "
                "Stop the other process, then run again."
            ) from exc


async def _interactive_user_login(tg: TelegramClient) -> None:
    """Authenticate this client as a personal user account only."""
    if not _is_interactive_runtime():
        raise RuntimeError(
            "Telegram login requires interactive terminal. "
            "Create userbot.session locally, then deploy."
        )
    try:
        await _connect_client_with_lock_guard(tg)
        if await tg.is_user_authorized():
            me = await _call_with_floodwait(tg.get_me)
            if getattr(me, "bot", False):
                raise RuntimeError(
                    "Session is authenticated as bot. Remove session and login with phone number."
                )
            return

        while True:
            phone = _prompt_phone_number()
            try:
                sent = await _call_with_floodwait(tg.send_code_request, phone)
                break
            except PhoneNumberInvalidError:
                print(
                    "Telegram rejected that phone number. "
                    "Use full E.164 format with country code, e.g. +15551234567."
                )
                continue
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
    except sqlite3.OperationalError as exc:
        if _is_session_db_locked_error(exc):
            raise RuntimeError(
                "Telethon session database is locked. "
                "Another bot instance is likely running. "
                "Stop the other process, then run again."
            ) from exc
        raise


async def _ensure_user_account_session() -> None:
    """Ensure current session is a user account, not a bot session."""
    global client

    tg = _require_client()
    try:
        await _connect_client_with_lock_guard(tg)

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
    except sqlite3.OperationalError as exc:
        if _is_session_db_locked_error(exc):
            raise RuntimeError(
                "Telethon session database is locked. "
                "Another bot instance is likely running. "
                "Stop the other process, then run again."
            ) from exc
        raise


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
        interactive = _is_interactive_runtime()
        while True:
            token = _bot_destination_token_from_config()
            chat_id = str(getattr(config, "BOT_DESTINATION_CHAT_ID", "") or "").strip()

            if not _looks_like_bot_token(token):
                if not interactive:
                    raise RuntimeError(
                        "BOT_DESTINATION_TOKEN is invalid in non-interactive runtime."
                    )
                config.BOT_DESTINATION_TOKEN = _prompt_bot_destination_token()
                _persist_config_updates({"BOT_DESTINATION_TOKEN": config.BOT_DESTINATION_TOKEN})
                continue

            if not chat_id:
                if not interactive:
                    raise RuntimeError(
                        "BOT_DESTINATION_CHAT_ID is missing in non-interactive runtime."
                    )
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
                if not interactive:
                    raise RuntimeError(
                        "Bot destination validation failed in non-interactive runtime. "
                        "Check BOT_DESTINATION_TOKEN/BOT_DESTINATION_CHAT_ID and bot access."
                    ) from exc
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
    interactive = _is_interactive_runtime()
    while True:
        try:
            destination_peer = await _resolve_destination_input_peer(config.DESTINATION)
            return
        except Exception as exc:
            LOGGER.error("Invalid DESTINATION value '%s': %s", config.DESTINATION, exc)
            if not interactive:
                raise RuntimeError(
                    f"Cannot resolve DESTINATION '{config.DESTINATION}' in non-interactive runtime."
                ) from exc
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


def _web_status_payload() -> Dict[str, object]:
    now = time.time()
    next_in = (
        max(0, int(digest_next_run_ts - now))
        if digest_next_run_ts is not None
        else None
    )
    try:
        last_ts = get_last_digest_timestamp()
    except Exception:
        last_ts = None
    try:
        pending = count_pending()
    except Exception:
        pending = 0
    try:
        inflight = count_inflight()
    except Exception:
        inflight = 0
    return {
        "ok": not bool(startup_error),
        "ready": bool(startup_ready),
        "phase": startup_phase,
        "error": startup_error or None,
        "service": "NetworkSlutter",
        "mode": "DIGEST" if _is_digest_mode_enabled() else "PER_POST",
        "started_as": started_as_username,
        "timestamp": int(now),
        "pending_queue": pending,
        "inflight_queue": inflight,
        "next_digest_in_seconds": next_in,
        "last_digest_at": format_ts(last_ts),
        "sources_monitored": len(monitored_source_chat_ids),
        "dupe_detection": _is_dupe_detection_enabled(),
        "severity_routing": _is_severity_routing_enabled(),
        "query_mode": _is_query_mode_enabled(),
        "html_formatting": _is_html_formatting_enabled(),
    }


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
    daily_labels = [f"{h:02d}:{m:02d}" for h, m in _digest_daily_times()]
    daily_display = ", ".join(daily_labels) if daily_labels else "off"
    queue_clear_interval_seconds = _digest_queue_clear_interval_seconds()
    queue_clear_every = (
        f"{queue_clear_interval_seconds // 60}m" if queue_clear_interval_seconds > 0 else "off"
    )
    queue_clear_scope = _digest_queue_clear_scope() if queue_clear_interval_seconds > 0 else "disabled"
    daily_window_hours = _digest_daily_window_hours()

    return (
        "<b>🧠 Digest Status</b><br>"
        f"• Mode: <code>{mode}</code><br>"
        f"• Pending queue: <code>{pending}</code><br>"
        f"• In-flight queue: <code>{inflight}</code><br>"
        f"• Last digest: <code>{format_ts(last_ts)}</code><br>"
        f"• Next run in: <code>{next_eta}</code><br>"
        f"• Interval base: <code>{_digest_interval_seconds() // 60}m</code><br>"
        f"• Daily digest times: <code>{sanitize_telegram_html(daily_display)}</code><br>"
        f"• Daily window: <code>{daily_window_hours}h</code><br>"
        f"• Queue clear every: <code>{sanitize_telegram_html(str(queue_clear_every))}</code><br>"
        f"• Queue clear scope: <code>{sanitize_telegram_html(queue_clear_scope)}</code><br>"
        f"• Dedupe: <code>{dupe_state}</code> ({sanitize_telegram_html(detector_backend)})<br>"
        f"• Severity router: <code>{severity_state}</code><br>"
        f"• Query mode: <code>{query_state}</code><br>"
        f"• HTML formatting: <code>{'on' if _is_html_formatting_enabled() else 'off'}</code><br>"
        f"• Quota health: <code>{quota.get('status', 'unknown')}</code><br>"
        f"• Recent 429 (1h): <code>{quota.get('recent_429_count', 0)}</code> / "
        f"threshold <code>{threshold}</code>"
    )


async def _on_digest_status_command(event: events.NewMessage.Event) -> None:
    try:
        status_text = _digest_status_text()
        if _is_html_formatting_enabled():
            status_text = _render_outbound_text(status_text, allow_premium_tags=False) or status_text
        await event.reply(
            status_text,
            parse_mode="html" if _is_html_formatting_enabled() else None,
            link_preview=False,
        )
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


def _filter_stored_query_rows(
    rows: Sequence[Dict[str, object]],
    *,
    query_text: str,
    broad_query: bool,
) -> list[Dict[str, object]]:
    expanded_terms = {
        normalize_space(term).lower()
        for term in expand_query_terms(query_text)
        if normalize_space(term)
    }
    query_numbers = set(extract_query_numbers(query_text))

    if not broad_query and not expanded_terms and not query_numbers:
        return []

    out: list[Dict[str, object]] = []
    for row in rows:
        text = normalize_space(str(row.get("raw_text") or ""))
        if not text:
            continue

        lowered_text = text.lower()
        if not broad_query:
            keyword_hit = any(term in lowered_text for term in expanded_terms)
            number_hit = any(number in lowered_text for number in query_numbers)
            if not keyword_hit and not number_hit:
                continue

        timestamp = int(row.get("timestamp") or 0)
        date_label = (
            datetime.fromtimestamp(timestamp).astimezone().isoformat()
            if timestamp > 0
            else ""
        )
        out.append(
            {
                "chat_id": str(row.get("channel_id") or ""),
                "message_id": int(row.get("message_id") or 0),
                "timestamp": timestamp,
                "date": date_label,
                "source": normalize_space(str(row.get("source_name") or "")) or "Archive",
                "text": text,
                "link": normalize_space(str(row.get("message_link") or "")),
            }
        )
    return out


def _load_queue_query_context(
    *,
    query_text: str,
    hours_back: int,
    limit: int,
    broad_query: bool,
) -> list[Dict[str, object]]:
    since_ts = max(0, int(time.time()) - max(1, int(hours_back)) * 3600)
    rows = load_queue_since(since_ts, max(1, int(limit)))
    if not rows:
        return []
    return _filter_stored_query_rows(rows, query_text=query_text, broad_query=broad_query)


def _load_archive_query_context(
    *,
    query_text: str,
    hours_back: int,
    limit: int,
    broad_query: bool,
) -> list[Dict[str, object]]:
    """
    Pull recent archived updates into query context.

    Broad digest-style prompts use archive history as the primary evidence pool.
    Subject-specific prompts only keep archive rows that mention the extracted
    query keywords.
    """
    since_ts = max(0, int(time.time()) - max(1, int(hours_back)) * 3600)
    rows = load_archive_since(since_ts, max(1, int(limit)))
    if not rows:
        return []
    return _filter_stored_query_rows(rows, query_text=query_text, broad_query=broad_query)


def _wrap_query_digest_answer(answer: str, *, hours_back: int) -> str:
    cleaned = normalize_space(answer)
    if not cleaned:
        return answer
    if "no relevant information found" in strip_telegram_html(cleaned).lower():
        return answer
    if "no major developments right now" in strip_telegram_html(cleaned).lower():
        return answer
    title = f"<b>{max(1, int(hours_back))}-hour digest</b>"
    return f"{title}<br><br>{answer.strip()}"


def _strip_query_answer_citations(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return value
    value = re.sub(r'\s*<a href="[^"]+">[^<]*</a>', "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*<i>\[[^<>\]]+\]</i>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*\[[^\[\]<>]{2,60}\]", "", value)
    value = re.sub(r"\bRead more\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"(?:<br>\s*){3,}", "<br><br>", value, flags=re.IGNORECASE)
    value = re.sub(r"[ \t]{2,}", " ", value)
    return sanitize_telegram_html(value.strip())


def _merge_query_context(
    telegram_rows: Sequence[Dict[str, object]],
    web_rows: Sequence[Dict[str, object]],
    *,
    limit: int,
) -> list[Dict[str, object]]:
    merged: dict[str, Dict[str, object]] = {}

    def _sig(item: Dict[str, object]) -> str:
        link = normalize_space(str(item.get("link") or ""))
        if link:
            return f"link:{link.lower()}"
        text = normalize_space(str(item.get("text") or ""))
        if text:
            return f"text:{hashlib.sha1(text.lower().encode('utf-8')).hexdigest()}"
        source = normalize_space(str(item.get("source") or ""))
        date = normalize_space(str(item.get("date") or ""))
        return f"meta:{source}|{date}"

    for row in list(telegram_rows) + list(web_rows):
        key = _sig(row)
        if not key:
            continue
        existing = merged.get(key)
        if existing is None:
            merged[key] = dict(row)
            continue
        # Prefer Telegram evidence when both map to same signature.
        existing_is_web = bool(existing.get("is_web"))
        row_is_web = bool(row.get("is_web"))
        if existing_is_web and not row_is_web:
            merged[key] = dict(row)

    ordered = sorted(
        merged.values(),
        key=lambda item: int(item.get("timestamp", 0) or 0),
        reverse=True,
    )
    return ordered[: max(1, int(limit))]


def _format_evidence_split_section(
    telegram_rows: Sequence[Dict[str, object]],
    web_rows: Sequence[Dict[str, object]],
    *,
    max_items: int = 4,
) -> str:
    if not telegram_rows and not web_rows:
        return ""

    limit = max(1, int(max_items))
    lines: list[str] = ["<b>Evidence Split</b>", "<b>Telegram Providers</b>"]

    telegram_seen: set[str] = set()
    telegram_lines = 0
    for item in telegram_rows:
        source_raw = normalize_space(str(item.get("source") or "Telegram"))
        source_key = source_raw.lower()
        if source_key in telegram_seen:
            continue
        telegram_seen.add(source_key)

        text = normalize_space(str(item.get("text") or ""))
        if len(text) > 95:
            text = f"{text[:92].rsplit(' ', 1)[0]}..."
        text_safe = sanitize_telegram_html(text or "Matched update")
        source_safe = sanitize_telegram_html(source_raw)
        link = normalize_space(str(item.get("link") or ""))

        if link.startswith("http"):
            lines.append(f'• <i>{source_safe}</i>: <a href="{link}">{text_safe}</a>')
        else:
            lines.append(f"• <i>{source_safe}</i>: {text_safe}")
        telegram_lines += 1
        if telegram_lines >= limit:
            break
    if telegram_lines == 0:
        lines.append("• <i>No recent Telegram matches.</i>")

    lines.append("<b>Web Providers</b>")
    web_seen: set[str] = set()
    web_lines = 0
    for item in web_rows:
        source_raw = normalize_space(str(item.get("source") or "Web"))
        source_key = source_raw.lower()
        if source_key in web_seen:
            continue
        web_seen.add(source_key)

        text = normalize_space(str(item.get("text") or ""))
        if len(text) > 95:
            text = f"{text[:92].rsplit(' ', 1)[0]}..."
        text_safe = sanitize_telegram_html(text or "Matched report")
        source_safe = sanitize_telegram_html(source_raw)
        link = normalize_space(str(item.get("link") or ""))

        if link.startswith("http"):
            lines.append(f'• <i>{source_safe}</i>: <a href="{link}">{text_safe}</a>')
        else:
            lines.append(f"• <i>{source_safe}</i>: {text_safe}")
        web_lines += 1
        if web_lines >= limit:
            break
    if web_lines == 0:
        lines.append("• <i>No recent trusted web matches in the selected time window.</i>")

    return "<br>".join(lines)


async def _safe_reply_markdown(
    event: events.NewMessage.Event,
    text: str,
    *,
    edit_message: Message | Dict[str, object] | None = None,
    reply_to: int | None = None,
    prefer_bot_identity: bool = False,
    bot_chat_id: int | None = None,
) -> Message | Dict[str, object] | None:
    payload_text = (
        _render_outbound_text(text, allow_premium_tags=False)
        if _is_html_formatting_enabled()
        else text
    )
    if not str(payload_text or "").strip():
        payload_text = (
            "<b>🟢 No relevant information found.</b>"
            if _is_html_formatting_enabled()
            else "No relevant information found."
        )
    while True:
        try:
            if edit_message is not None:
                if prefer_bot_identity and isinstance(edit_message, dict):
                    message_id = int(edit_message.get("message_id", 0) or 0)
                    if message_id <= 0 or not bot_chat_id:
                        raise RuntimeError("Cannot edit bot query message without valid ids.")
                    data = {
                        "chat_id": str(bot_chat_id),
                        "message_id": str(message_id),
                        "text": _to_bot_text(payload_text) or "",
                        "disable_web_page_preview": "true",
                        "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
                    }
                    result = await _query_bot_api_request("editMessageText", data=data)
                    return result if isinstance(result, dict) else edit_message
                if isinstance(edit_message, Message):
                    return await edit_message.edit(
                        payload_text,
                        parse_mode="html" if _is_html_formatting_enabled() else "md",
                        link_preview=False,
                    )
                raise RuntimeError("Unsupported edit_message type for query response.")

            if prefer_bot_identity:
                if not bot_chat_id:
                    raise RuntimeError("Missing bot_chat_id for bot query reply.")
                data = {
                    "chat_id": str(bot_chat_id),
                    "text": _to_bot_text(payload_text) or "",
                    "disable_web_page_preview": "true",
                    "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
                }
                if reply_to:
                    data["reply_to_message_id"] = str(reply_to)
                result = await _query_bot_send_message(data)
                return result if isinstance(result, dict) else None

            tg = _require_client()
            # Query UX:
            # - Saved Messages: user-account response (can reply_to the query)
            # - Own bot PM: handled above via Bot API for real bot identity
            return await _call_with_floodwait(
                tg.send_message,
                event.chat_id,
                payload_text,
                reply_to=reply_to,
                parse_mode="html" if _is_html_formatting_enabled() else "md",
                link_preview=False,
            )
        except MessageNotModifiedError:
            return edit_message
        except FloodWaitError as exc:
            wait_seconds = int(exc.seconds) + 1
            LOGGER.warning("FloodWait while replying to query: sleeping %ss", wait_seconds)
            await asyncio.sleep(wait_seconds)
        except Exception:
            try:
                fallback_text = (
                    strip_telegram_html(payload_text)
                    if _is_html_formatting_enabled()
                    else text
                )
                fallback_text = (fallback_text or "").strip()
                if not fallback_text:
                    fallback_text = "No relevant information found."
                # Force valid UTF-8 safe payload before plain-text send/edit.
                fallback_text = fallback_text.encode("utf-8", errors="replace").decode(
                    "utf-8",
                    errors="replace",
                )
                if edit_message is not None:
                    if prefer_bot_identity and isinstance(edit_message, dict):
                        message_id = int(edit_message.get("message_id", 0) or 0)
                        if message_id <= 0 or not bot_chat_id:
                            raise RuntimeError("Cannot edit bot query message without valid ids.")
                        data = {
                            "chat_id": str(bot_chat_id),
                            "message_id": str(message_id),
                            "text": _to_plain_text(fallback_text) or "",
                            "disable_web_page_preview": "true",
                        }
                        result = await _query_bot_api_request("editMessageText", data=data)
                        return result if isinstance(result, dict) else edit_message
                    if isinstance(edit_message, Message):
                        return await edit_message.edit(
                            fallback_text,
                            parse_mode=None,
                            link_preview=False,
                        )
                    raise RuntimeError("Unsupported edit_message type for query response.")

                if prefer_bot_identity:
                    if not bot_chat_id:
                        raise RuntimeError("Missing bot_chat_id for bot query reply.")
                    data = {
                        "chat_id": str(bot_chat_id),
                        "text": _to_plain_text(fallback_text) or "",
                        "disable_web_page_preview": "true",
                    }
                    if reply_to:
                        data["reply_to_message_id"] = str(reply_to)
                    result = await _query_bot_send_message(data)
                    return result if isinstance(result, dict) else None

                tg = _require_client()
                return await _call_with_floodwait(
                    tg.send_message,
                    event.chat_id,
                    fallback_text,
                    reply_to=reply_to,
                    parse_mode=None,
                    link_preview=False,
                )
            except MessageNotModifiedError:
                return edit_message
            except FloodWaitError as exc:
                wait_seconds = int(exc.seconds) + 1
                LOGGER.warning("FloodWait while replying to query: sleeping %ss", wait_seconds)
                await asyncio.sleep(wait_seconds)
            except Exception:
                LOGGER.exception("Failed to send query response message.")
                return None


async def _stream_query_answer(
    event: events.NewMessage.Event,
    *,
    progress_message: Message | Dict[str, object],
    query_text: str,
    results: List[Dict[str, object]],
    history: Sequence[Dict[str, str]],
    digest_mode: bool = False,
    digest_hours_back: int | None = None,
    prefer_bot_identity: bool = False,
    bot_chat_id: int | None = None,
    root_reply_to: int | None = None,
    final_suffix_html: str = "",
) -> tuple[str, object]:
    async def _send_query_message(text: str, reply_to: int | None):
        effective_reply_to = reply_to if reply_to else root_reply_to
        return await _safe_reply_markdown(
            event,
            text,
            reply_to=effective_reply_to,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )

    async def _edit_query_message(ref: object, text: str):
        edited = await _safe_reply_markdown(
            event,
            text,
            edit_message=ref if isinstance(ref, (Message, dict)) else None,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )
        return edited or ref

    streamer = LiveTelegramStreamer(
        send_message=_send_query_message,
        edit_message=_edit_query_message,
        get_message_id=lambda ref: _message_ref_id(ref) or 0,
        placeholder_text="Thinking... ⏳",
        edit_interval_ms=_stream_edit_interval_ms(),
        max_chars_per_edit=_stream_max_chars_per_edit(),
        typing_enabled=False,
        html_mode=_is_html_formatting_enabled(),
    )
    await streamer.start(initial_ref=progress_message)

    if digest_mode:
        answer = await create_digest_summary(
            list(results),
            auth_manager=_require_auth_manager(),
            interval_minutes=max(1, int((digest_hours_back or 1) * 60)),
            on_token=streamer.push,
        )
        answer = _wrap_query_digest_answer(answer, hours_back=digest_hours_back or 1)
    else:
        answer = await generate_answer_from_context(
            query=query_text,
            context_messages=results,
            auth_manager=_require_auth_manager(),
            conversation_history=history,
            on_token=streamer.push,
        )
    if not answer.strip():
        answer = "No matching information found in recent updates."
    answer = _strip_query_answer_citations(answer)
    if final_suffix_html.strip() and "no relevant information found" not in strip_telegram_html(answer).lower():
        answer = f"{answer.strip()}<br><br>{final_suffix_html.strip()}"

    stats = await streamer.finalize(answer)
    return answer, stats


async def _handle_query_request(
    *,
    event_ref: object,
    text: str,
    sender_id: int,
    chat_id: str,
    reply_to: int | None,
    prefer_bot_identity: bool,
    bot_chat_id: int | None,
) -> None:
    now = time.time()
    last = query_last_request_ts.get(sender_id, 0.0)
    if now - last < 10:
        wait_seconds = int(max(1.0, 10 - (now - last)))
        await _safe_reply_markdown(
            event_ref,  # type: ignore[arg-type]
            f"Please wait {wait_seconds}s before your next query.",
            reply_to=reply_to,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )
        return
    query_last_request_ts[sender_id] = now

    progress = await _safe_reply_markdown(
        event_ref,  # type: ignore[arg-type]
        (
            "Searching channels + trusted web... ⏳"
            if _is_query_web_fallback_enabled()
            else "Searching your channels... ⏳"
        ),
        reply_to=reply_to,
        prefer_bot_identity=prefer_bot_identity,
        bot_chat_id=bot_chat_id,
    )
    if progress is None:
        return

    try:
        plan = build_query_plan(text, default_hours=_query_default_hours_back())
        parsed_hours = plan.hours_back
        cleaned_query = plan.cleaned_query
        effective_query = plan.original_query or text
        broad_query = plan.broad_query
        query_keywords = list(plan.keywords)
        query_numbers = list(plan.numbers)
        high_risk_query = _is_high_risk_news_query(effective_query)
        context_limit = _query_max_messages() * (2 if broad_query or len(query_keywords) <= 2 else 1)

        preload_web_search = bool(
            _is_query_web_fallback_enabled()
            and (broad_query or bool(query_keywords) or bool(query_numbers))
            and (
                _query_prefers_web_crosscheck(effective_query)
                or high_risk_query
                or broad_query
            )
        )

        telegram_task = asyncio.create_task(
            search_recent_messages(
                _require_client(),
                monitored_source_chat_ids,
                cleaned_query,
                max_messages=context_limit,
                default_hours_back=parsed_hours,
                logger=LOGGER,
            )
        )
        web_task: asyncio.Task[list[dict[str, object]]] | None = None
        if preload_web_search:
            web_task = asyncio.create_task(
                search_recent_news_web(
                    cleaned_query,
                    hours_back=min(parsed_hours, _query_web_max_hours_back()),
                    max_results=_query_web_max_results(),
                    allowed_domains=_query_web_allowed_domains(),
                    require_recent=_query_web_require_recent(),
                    logger=LOGGER,
                )
            )

        telegram_results = await telegram_task

        queue_results: list[dict[str, object]] = []
        archive_results: list[dict[str, object]] = []
        if broad_query or len(telegram_results) < _query_web_min_telegram_results():
            queue_results = _load_queue_query_context(
                query_text=effective_query,
                hours_back=parsed_hours,
                limit=max(context_limit * 3, 80),
                broad_query=broad_query,
            )
            if queue_results:
                telegram_results = _merge_query_context(
                    telegram_results,
                    queue_results,
                    limit=max(context_limit, 40),
                )

        if broad_query or len(telegram_results) < _query_web_min_telegram_results():
            archive_results = _load_archive_query_context(
                query_text=effective_query,
                hours_back=parsed_hours,
                limit=max(context_limit * 3, 80),
                broad_query=broad_query,
            )
            if archive_results:
                telegram_results = _merge_query_context(
                    telegram_results,
                    archive_results,
                    limit=max(context_limit, 40),
                )

        web_results: list[dict[str, object]] = []
        web_fallback_used = False
        ran_web_search = bool(preload_web_search)

        should_run_web_search = bool(
            _is_query_web_fallback_enabled()
            and (broad_query or bool(query_keywords) or bool(query_numbers))
            and (
                _query_prefers_web_crosscheck(effective_query)
                or
                high_risk_query
                or broad_query
                or len(telegram_results) < _query_web_min_telegram_results()
            )
        )

        if should_run_web_search and web_task is None:
            ran_web_search = True
            await _safe_reply_markdown(
                event_ref,  # type: ignore[arg-type]
                (
                    "Cross-checking channels with trusted web news... ⏳"
                    if not high_risk_query
                    else "High-risk query detected. Cross-checking trusted web news... ⏳"
                ),
                edit_message=progress,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
            )
            web_results = await search_recent_news_web(
                cleaned_query,
                hours_back=min(parsed_hours, _query_web_max_hours_back()),
                max_results=_query_web_max_results(),
                allowed_domains=_query_web_allowed_domains(),
                require_recent=_query_web_require_recent(),
                logger=LOGGER,
            )
        elif web_task is not None:
            web_results = await web_task

        if should_run_web_search and web_results:
            required_sources = _query_web_require_min_sources()
            if web_results and required_sources > 1:
                source_hosts = {
                    normalize_space(str(item.get("source") or "")).lower()
                    for item in web_results
                    if normalize_space(str(item.get("source") or ""))
                }
                if len(source_hosts) < required_sources:
                    LOGGER.info(
                        "Web fallback rejected for query due to low source diversity (%s < %s).",
                        len(source_hosts),
                        required_sources,
                    )
                    web_results = []
        web_fallback_used = bool(web_results)

        results = _merge_query_context(
            telegram_results,
            web_results,
            limit=max(context_limit, _query_max_messages()),
        )

        await _safe_reply_markdown(
            event_ref,  # type: ignore[arg-type]
            (
                f"Found {len(telegram_results)} evidence items"
                + (f" + {len(web_results)} web reports" if web_results else "")
                + " -> analyzing... ⏳"
            ),
            edit_message=progress,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )

        history = list(_query_history_for_sender(sender_id))
        evidence_split_section = ""

        if _is_streaming_enabled():
            answer, stream_stats = await _stream_query_answer(
                event_ref,  # type: ignore[arg-type]
                progress_message=progress,
                query_text=effective_query,
                results=results,
                history=history,
                digest_mode=broad_query,
                digest_hours_back=parsed_hours,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
                root_reply_to=reply_to,
                final_suffix_html=evidence_split_section,
            )
        else:
            if broad_query:
                answer = await create_digest_summary(
                    list(results),
                    auth_manager=_require_auth_manager(),
                    interval_minutes=max(1, parsed_hours * 60),
                )
                answer = _wrap_query_digest_answer(answer, hours_back=parsed_hours)
            else:
                answer = await generate_answer_from_context(
                    query=effective_query,
                    context_messages=results,
                    auth_manager=_require_auth_manager(),
                    conversation_history=history,
                )
            if not answer.strip():
                answer = "No matching information found in recent updates."
            answer = _strip_query_answer_citations(answer)
            if (
                evidence_split_section
                and "no relevant information found" not in strip_telegram_html(answer).lower()
            ):
                answer = f"{answer.strip()}<br><br>{evidence_split_section}"
            await _safe_reply_markdown(
                event_ref,  # type: ignore[arg-type]
                answer,
                edit_message=progress,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
            )
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
            telegram_messages_found=len(telegram_results),
            queue_messages_found=len(queue_results),
            archive_messages_found=len(archive_results),
            web_messages_found=len(web_results),
            web_fallback_used=web_fallback_used,
            web_search_ran=ran_web_search,
            broad_query=broad_query,
            high_risk_query=high_risk_query,
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
            event_ref,  # type: ignore[arg-type]
            "No matching information found in recent updates.",
            edit_message=progress,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )
        log_structured(
            LOGGER,
            "query_failed",
            level=logging.ERROR,
            sender_id=sender_id,
            chat_id=chat_id,
            error=str(exc),
        )


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

    # Optional hard allowlist of peer IDs.
    allowed_peer_ids = _query_allowed_peer_ids()
    if allowed_peer_ids and (target_user_id not in allowed_peer_ids) and not is_saved_messages:
        LOGGER.debug(
            "Query ignored: target peer not in QUERY_ALLOWED_PEER_IDS chat_id=%s peer_user_id=%s",
            getattr(msg, "chat_id", None),
            target_user_id,
        )
        return

    if not (is_saved_messages or is_own_bot_pm):
        LOGGER.debug(
            "Query ignored: private chat is not Saved Messages or own bot peer chat_id=%s peer_user_id=%s",
            getattr(msg, "chat_id", None),
            target_user_id,
        )
        return

    # Bot-PM queries are handled from the actual Bot API update loop so the bot
    # has the exact incoming message_id and can reply deterministically.
    if is_own_bot_pm and not is_saved_messages:
        LOGGER.debug(
            "Query delegated to Bot API loop for bot PM chat_id=%s peer_user_id=%s",
            getattr(msg, "chat_id", None),
            target_user_id,
        )
        return

    await _handle_query_request(
        event_ref=event,
        text=text,
        sender_id=sender_id,
        chat_id=str(getattr(msg, "chat_id", "") or ""),
        reply_to=int(msg.id or 0) or None,
        prefer_bot_identity=False,
        bot_chat_id=None,
    )


async def run_query_bot_poll_loop() -> None:
    """
    Poll Bot API updates for the owner's PM with their bot.

    This is the reliable path for bot-PM queries because it gives the exact
    incoming bot-side message_id, so replies can thread properly.
    """
    global query_bot_updates_offset, query_bot_poll_started_at

    token = _bot_destination_token_from_config()
    if not _looks_like_bot_token(token):
        return

    owner_user_id = int(started_as_user_id or 0)
    if owner_user_id <= 0:
        return

    if await _resolve_query_bot_user_id() is None:
        return

    query_bot_poll_started_at = int(time.time())

    try:
        await _ensure_query_bot_polling_mode()
        await _prime_query_bot_update_offset()
    except Exception:
        LOGGER.exception("Failed preparing bot query polling mode.")
        await asyncio.sleep(2.0)

    while True:
        try:
            data = {
                "timeout": "30",
                "limit": "50",
            }
            if query_bot_updates_offset > 0:
                data["offset"] = str(query_bot_updates_offset)

            updates = await _query_bot_api_request("getUpdates", data=data)
            if not isinstance(updates, list):
                await asyncio.sleep(1.0)
                continue

            max_seen_offset = query_bot_updates_offset
            for update in updates:
                if not isinstance(update, dict):
                    continue
                update_id = int(update.get("update_id", 0) or 0)
                if update_id > 0:
                    max_seen_offset = max(max_seen_offset, update_id + 1)

                payload = update.get("message") or update.get("edited_message")
                if not isinstance(payload, dict):
                    continue

                payload_date = int(payload.get("date", 0) or 0)
                if (
                    query_bot_poll_started_at > 0
                    and payload_date > 0
                    and payload_date < (query_bot_poll_started_at - 5)
                ):
                    continue

                chat = payload.get("chat")
                if not isinstance(chat, dict):
                    continue
                if str(chat.get("type") or "") != "private":
                    continue

                chat_id = int(chat.get("id", 0) or 0)
                sender = payload.get("from")
                if not isinstance(sender, dict):
                    continue
                sender_id = int(sender.get("id", 0) or 0)
                if sender_id != owner_user_id:
                    continue
                if chat_id != owner_user_id:
                    continue

                text = normalize_space(
                    str(payload.get("text") or payload.get("caption") or "")
                )
                if _is_query_text_ignored(text):
                    continue

                message_id = int(payload.get("message_id", 0) or 0)
                if message_id <= 0:
                    continue

                await _handle_query_request(
                    event_ref=SimpleNamespace(chat_id=chat_id),
                    text=text,
                    sender_id=sender_id,
                    chat_id=str(chat_id),
                    reply_to=message_id,
                    prefer_bot_identity=True,
                    bot_chat_id=chat_id,
                )

            query_bot_updates_offset = max(query_bot_updates_offset, max_seen_offset)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_query_bot_webhook_conflict(exc):
                LOGGER.warning(
                    "Bot query polling hit active webhook conflict. Retrying after deleting webhook."
                )
                try:
                    await _ensure_query_bot_polling_mode()
                except Exception:
                    LOGGER.exception("Failed deleting bot webhook after polling conflict.")
                    await asyncio.sleep(3.0)
                continue
            LOGGER.exception("Bot query polling loop failed.")
            await asyncio.sleep(2.0)


async def _on_new_message(event: events.NewMessage.Event) -> None:
    msg = event.message
    channel_id = str(msg.chat_id)

    try:
        if is_seen(channel_id, msg.id):
            return

        if msg.media and not msg.grouped_id:
            if await _check_single_media_duplicate(msg):
                mark_seen(channel_id, msg.id)
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
    global digest_scheduler_task, daily_digest_scheduler_task, queue_clear_scheduler_task
    global query_bot_poll_task
    global web_status_server
    configure_duplicate_runtime(None)
    breaking_delivery_refs.clear()
    breaking_topic_threads.clear()

    if digest_scheduler_task and not digest_scheduler_task.done():
        digest_scheduler_task.cancel()
        await asyncio.gather(digest_scheduler_task, return_exceptions=True)
    digest_scheduler_task = None

    if daily_digest_scheduler_task and not daily_digest_scheduler_task.done():
        daily_digest_scheduler_task.cancel()
        await asyncio.gather(daily_digest_scheduler_task, return_exceptions=True)
    daily_digest_scheduler_task = None

    if queue_clear_scheduler_task and not queue_clear_scheduler_task.done():
        queue_clear_scheduler_task.cancel()
        await asyncio.gather(queue_clear_scheduler_task, return_exceptions=True)
    queue_clear_scheduler_task = None

    if query_bot_poll_task and not query_bot_poll_task.done():
        query_bot_poll_task.cancel()
        await asyncio.gather(query_bot_poll_task, return_exceptions=True)
    query_bot_poll_task = None

    if web_status_server is not None:
        try:
            web_status_server.stop()
        except Exception:
            LOGGER.exception("Failed stopping web status server.")
        web_status_server = None

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
    except Exception as exc:
        if _is_session_db_locked_error(exc):
            LOGGER.warning(
                "Skipped session flush on shutdown because session DB is locked by another process."
            )
            return
        LOGGER.exception("Failed during client shutdown.")


def _startup_health_check() -> None:
    mode = "DIGEST" if _is_digest_mode_enabled() else "PER_POST"
    pending = count_pending()
    inflight = count_inflight()
    last_ts = get_last_digest_timestamp()
    queue_clear_interval_seconds = _digest_queue_clear_interval_seconds()
    queue_clear_minutes = (
        (queue_clear_interval_seconds // 60) if queue_clear_interval_seconds > 0 else 0
    )
    queue_clear_display = (
        f"{queue_clear_minutes}m" if queue_clear_interval_seconds > 0 else "off"
    )
    queue_clear_scope = _digest_queue_clear_scope() if queue_clear_interval_seconds > 0 else "disabled"
    log_structured(
        LOGGER,
        "startup_health",
        mode=mode,
        dupe_detection=_is_dupe_detection_enabled(),
        dupe_use_sentence_transformers=_dupe_use_sentence_transformers(),
        severity_routing=_is_severity_routing_enabled(),
        query_mode=_is_query_mode_enabled(),
        query_web_fallback=_is_query_web_fallback_enabled(),
        html_formatting=_is_html_formatting_enabled(),
        premium_emoji=_is_premium_emoji_enabled(),
        premium_emoji_count=len(premium_emoji_map),
        humanized_vital_opinion=_humanized_vital_opinion_enabled(),
        humanized_vital_probability=_humanized_vital_opinion_probability(),
        query_allowed_peer_count=len(_query_allowed_peer_ids()),
        breaking_topic_threads=_is_breaking_topic_threads_enabled(),
        digest_interval_minutes=_digest_interval_seconds() // 60,
        digest_daily_times=[f"{h:02d}:{m:02d}" for h, m in _digest_daily_times()],
        digest_daily_window_hours=_digest_daily_window_hours(),
        digest_queue_clear_minutes=queue_clear_minutes,
        digest_queue_clear_enabled=bool(queue_clear_interval_seconds > 0),
        digest_queue_clear_inflight=_digest_queue_clear_include_inflight(),
        digest_queue_clear_scope=queue_clear_scope,
        web_server_enabled=_is_web_server_enabled(),
        web_server_host=_web_server_host(),
        web_server_port=_web_server_port(),
        pending=pending,
        inflight=inflight,
        last_digest_ts=last_ts,
        last_digest_at=format_ts(last_ts),
        quota_health=get_quota_health(),
    )
    LOGGER.info(
        "Startup health: mode=%s pending=%s inflight=%s last_digest=%s dupe=%s severity=%s query=%s query_web=%s html=%s premium_emoji=%s map=%s humanized=%s prob=%.2f topic_threads=%s interval=%sm daily=%s queue_clear=%s scope=%s web=%s@%s:%s",
        mode,
        pending,
        inflight,
        format_ts(last_ts),
        _is_dupe_detection_enabled(),
        _is_severity_routing_enabled(),
        _is_query_mode_enabled(),
        _is_query_web_fallback_enabled(),
        _is_html_formatting_enabled(),
        _is_premium_emoji_enabled(),
        len(premium_emoji_map),
        _humanized_vital_opinion_enabled(),
        _humanized_vital_opinion_probability(),
        _is_breaking_topic_threads_enabled(),
        _digest_interval_seconds() // 60,
        ",".join(f"{h:02d}:{m:02d}" for h, m in _digest_daily_times()) or "off",
        queue_clear_display,
        queue_clear_scope,
        _is_web_server_enabled(),
        _web_server_host(),
        _web_server_port(),
    )


async def main() -> None:
    global client, auth_manager, digest_scheduler_task
    global daily_digest_scheduler_task, queue_clear_scheduler_task, query_bot_poll_task
    global instance_lock_handle, web_status_server, started_as_username, started_as_user_id
    global startup_phase, startup_ready, startup_error

    try:
        _print_cli_banner()
        if _is_web_server_enabled():
            try:
                web_status_server = WebStatusServer(
                    host=_web_server_host(),
                    port=_web_server_port(),
                    get_status=_web_status_payload,
                    logger=LOGGER,
                )
                web_status_server.start()
            except Exception:
                LOGGER.exception("Failed starting web status server.")
                web_status_server = None

        startup_phase = "config"
        _print_cli_status("•", "Validating configuration...", level="info")
        try:
            _prompt_for_missing_config()
            _validate_config()
            _print_cli_status("✓", "Configuration validated", level="ok")
        except (RuntimeError, ValueError) as exc:
            startup_error = str(exc)
            startup_ready = False
            startup_phase = "error"
            LOGGER.error("%s", exc)
            print(exc)
            _print_cli_status("✗", "Configuration invalid. Fix values and re-run.", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after config validation error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            return

        startup_phase = "lock"
        _print_cli_status("•", "Checking single-instance lock...", level="info")
        try:
            instance_lock_handle = _acquire_instance_lock()
            _print_cli_status("✓", "Instance lock acquired", level="ok")
        except RuntimeError as exc:
            startup_error = str(exc)
            startup_ready = False
            startup_phase = "error"
            LOGGER.error("%s", exc)
            print(exc)
            _print_cli_status("✗", "Another instance is already running", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after startup lock error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            return

        try:
            startup_phase = "initializing"
            _load_premium_emoji_map()
            init_db()
            await _init_dupe_detector()
            _startup_health_check()
            _print_cli_status("✓", "Database and runtime state ready", level="ok")

            startup_phase = "auth"
            _print_cli_status("•", "Preparing OpenAI auth context...", level="info")
            auth_manager = AuthManager(logger=LOGGER)
            await auth_manager.get_access_token()
            _print_cli_status("✓", "OpenAI auth ready", level="ok")

            startup_phase = "telegram_login"
            _print_cli_status("•", "Connecting Telegram user session...", level="info")
            client = TelegramClient("userbot", config.TELEGRAM_API_ID, config.TELEGRAM_API_HASH)
            await _ensure_user_account_session()
            _print_cli_status("✓", "Telegram session authorized", level="ok")
            startup_phase = "destination_setup"
            _print_cli_status("•", "Validating destination...", level="info")
            await _ensure_destination_peer()
            _print_cli_status("✓", "Destination ready", level="ok")

            startup_phase = "source_setup"
            _print_cli_status("•", "Resolving source channels...", level="info")
            resolved_sources = await setup_sources()
            while not resolved_sources:
                if not _is_interactive_runtime():
                    raise RuntimeError(
                        "No source channels resolved. Set valid FOLDER_INVITE_LINK "
                        "or EXTRA_SOURCES in environment."
                    )
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
            _print_cli_status("✓", f"Sources ready ({len(resolved_sources)} total)", level="ok")

            startup_phase = "handlers"
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
            started_as_username = str(getattr(me, "username", "") or "unknown")
            started_as_user_id = int(getattr(me, "id", 0) or 0)
            LOGGER.info("Userbot started as @%s", started_as_username)
            _print_cli_status(
                "🚀",
                f"Running as @{started_as_username} — monitoring {len(resolved_sources)} source(s)",
                level="ok",
            )
            if _is_query_mode_enabled() and _looks_like_bot_token(_bot_destination_token_from_config()):
                query_bot_poll_task = asyncio.create_task(
                    run_query_bot_poll_loop(),
                    name="query-bot-poll",
                )
            if _is_missing_folder_invite_link():
                LOGGER.info("Listening to %s manually configured channels.", len(resolved_sources))
            if _is_digest_mode_enabled():
                digest_scheduler_task = asyncio.create_task(
                    run_digest_scheduler(),
                    name="digest-scheduler",
                )
                if _digest_daily_times():
                    daily_digest_scheduler_task = asyncio.create_task(
                        run_daily_digest_scheduler(),
                        name="daily-digest-scheduler",
                    )
                if _digest_queue_clear_interval_seconds() > 0:
                    queue_clear_scheduler_task = asyncio.create_task(
                        run_digest_queue_clear_scheduler(),
                        name="digest-queue-clear-scheduler",
                    )
                    _print_cli_status(
                        "✓",
                        "Schedulers started (hourly + daily + queue clear)",
                        level="ok",
                    )
                else:
                    queue_clear_scheduler_task = None
                    _print_cli_status(
                        "✓",
                        "Schedulers started (hourly + daily; queue clear disabled)",
                        level="ok",
                    )

            startup_phase = "running"
            startup_ready = True
            startup_error = ""
            try:
                await client.run_until_disconnected()
            except (asyncio.CancelledError, KeyboardInterrupt):
                LOGGER.info("Shutdown requested. Stopping userbot...")
                _print_cli_status("•", "Shutdown requested, stopping...", level="warn")
        except (RuntimeError, ValueError) as exc:
            startup_error = str(exc)
            startup_ready = False
            startup_phase = "error"
            LOGGER.error("%s", exc)
            print(exc)
            _print_cli_status("✗", "Startup failed", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after startup error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
        except Exception as exc:
            startup_error = str(exc)
            startup_ready = False
            startup_phase = "error"
            LOGGER.exception("Unhandled startup error.")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after unhandled startup error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            raise
        finally:
            await _shutdown_client()
    finally:
        startup_phase = "stopped"
        startup_ready = False
        _release_instance_lock(instance_lock_handle)
        instance_lock_handle = None


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Userbot stopped.")
