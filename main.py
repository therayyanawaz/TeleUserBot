"""Telegram News Aggregator userbot entry point."""

from __future__ import annotations

import asyncio
import contextlib
from difflib import SequenceMatcher
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
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
    create_digest_summary_result,
    extract_digest_narrative_parts,
    decide_filter_action,
    extract_feed_summary_parts,
    generate_answer_from_context,
    get_filter_decision_cache_stats,
    get_quota_health,
    normalize_feed_summary_html,
    resolve_breaking_style_mode,
    resolve_vital_rational_view_for_delivery,
    summarize_breaking_headline,
    summarize_vital_rational_view,
    summarize_or_skip,
    translate_ocr_text_to_english,
)
from prompts import QUERY_NO_MATCH_TEXT, digest_output_style
from auth import (
    ENV_AUTH_ENV_ONLY,
    ENV_AUTH_JSON,
    ENV_AUTH_JSON_B64,
    AuthManager,
    ERROR_LOG_PATH,
    OAuthError,
    bootstrap_env_oauth_payload,
    ensure_runtime_dir,
    normalize_oauth_error_message,
    write_auth_payload_to_env_file,
)
from breaking_story import (
    BreakingStoryCandidate,
    ContextEvidence,
    build_breaking_story_candidate,
    compute_breaking_story_cluster_key,
    derive_context_evidence,
    deserialize_breaking_story_facts,
    resolve_breaking_story_cluster,
    serialize_breaking_story_facts,
)
from db import (
    ack_digest_window,
    advance_inbound_job,
    count_active_breaking_story_clusters,
    claim_inbound_jobs,
    claim_digest_window,
    clear_digest_queue_scoped,
    complete_inbound_job,
    count_ai_decision_cache_entries,
    count_inflight,
    count_pending,
    enqueue_inbound_job,
    get_last_digest_timestamp,
    get_meta,
    init_db,
    is_seen,
    load_active_digest_window_claim,
    load_archive_window_page,
    load_batch_rows_page,
    load_inbound_job_counts,
    load_recent_inbound_job_failures,
    load_breaking_story_decision_counts,
    load_active_breaking_story_clusters,
    load_breaking_story_events,
    load_source_delivery_ref,
    load_archive_since,
    load_queue_since,
    mark_seen,
    mark_seen_many,
    oldest_pending_inbound_job_age_seconds,
    peek_oldest_pending_digest_timestamp,
    prune_archive_older_than,
    purge_ai_decision_cache,
    purge_source_delivery_refs,
    reset_in_progress_inbound_jobs,
    retry_or_dead_letter_inbound_job,
    restore_digest_window,
    save_breaking_story_cluster,
    save_breaking_story_event,
    save_source_delivery_ref,
    save_to_digest_archive,
    save_to_digest_queue,
    set_last_digest_timestamp,
    set_meta,
    count_archive_window_rows,
    purge_breaking_story_history,
)
from news_taxonomy import (
    get_ontology_health_snapshot,
    load_news_taxonomy,
    match_breaking_category,
    match_news_category,
)
from news_signals import detect_story_signals, looks_like_live_event_update, should_downgrade_explainer_urgency
from prompts import quiet_period_message
from runtime_presenter import RuntimeActivityBufferHandler, RuntimeActivityFormatter
from severity_classifier import classify_message_severity, severity_score_floor
from utils import (
    apply_premium_emoji_html,
    build_alert_header,
    build_media_signature_digest,
    check_and_store_media_duplicate,
    choose_alert_label,
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
    split_markdown_chunks,
    strip_telegram_html,
)
from web_server import WebStatusServer
from shared_http import close_shared_http_clients, get_bot_http_client

try:
    import fcntl
except Exception:  # pragma: no cover - non-Unix fallback
    fcntl = None  # type: ignore[assignment]

try:
    import resource
except Exception:  # pragma: no cover - non-Unix fallback
    resource = None  # type: ignore[assignment]


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
_last_cli_status_signature: tuple[str, str] | None = None
_last_cli_status_at: float = 0.0


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
    global _last_cli_status_signature, _last_cli_status_at

    if not sys.stdin.isatty():
        return
    normalized_text = str(text or "").strip()
    if normalized_text == "OpenAI auth ready" and (not auth_ready or auth_degraded):
        return
    now = time.time()
    signature = (level, normalized_text)
    if _last_cli_status_signature == signature and (now - _last_cli_status_at) < 0.5:
        return
    _last_cli_status_signature = signature
    _last_cli_status_at = now
    color = _C_CYAN
    if level == "ok":
        color = _C_GREEN
    elif level == "warn":
        color = _C_YELLOW
    elif level == "error":
        color = _C_RED
    print(f"{_c(symbol, color, bold=True)} {text}")


ensure_runtime_dir()

RUNTIME_LOG_PATH = ensure_runtime_dir() / "runtime.log"
_LOG_ROTATION_MAX_BYTES = 10 * 1024 * 1024
_LOG_ROTATION_BACKUP_COUNT = 5

LOGGER = logging.getLogger("tg_news_userbot")
LOGGER.setLevel(logging.INFO)

_runtime_logging_configured = False
_runtime_root_previous_level: int | None = None
_runtime_logger_previous_level: int | None = None
_runtime_logger_previous_propagate: bool | None = None
_runtime_file_handler: logging.Handler | None = None
_error_handler: logging.Handler | None = None
_console_handler: logging.Handler | None = None
_runtime_activity_buffer_handler: RuntimeActivityBufferHandler | None = None

_LOG_BOT_TOKEN_RE = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")
_LOG_CALLBACK_CODE_RE = re.compile(r"([?&]code=)[^&\s]+")
_LOG_BEARER_RE = re.compile(r"(?i)\b(Bearer)\s+[A-Za-z0-9._\-+/=]+")
_LOG_AUTH_HEADER_RE = re.compile(
    r"(?i)(authorization['\"]?\s*[:=]\s*['\"]?bearer\s+)[^\s'\",]+"
)
_LOG_ENV_ASSIGNMENT_RE = re.compile(
    r"(?i)\b((?:TG_USERBOT_AUTH_JSON(?:_B64)?|BOT_DESTINATION_TOKEN)\s*=\s*)(\"[^\"]*\"|'[^']*'|[^\s]+)"
)
_LOG_JSON_SECRET_RE = re.compile(
    r'(?i)("?(?:access_token|refresh_token|id_token|TG_USERBOT_AUTH_JSON|'
    r'TG_USERBOT_AUTH_JSON_B64|BOT_DESTINATION_TOKEN)"?\s*:\s*")[^"]*(")'
)


def _sanitize_log_text(text: str) -> str:
    cleaned = str(text or "")
    cleaned = _LOG_CALLBACK_CODE_RE.sub(r"\1[REDACTED]", cleaned)
    cleaned = _LOG_AUTH_HEADER_RE.sub(r"\1[REDACTED]", cleaned)
    cleaned = _LOG_BEARER_RE.sub(r"\1 [REDACTED]", cleaned)
    cleaned = _LOG_ENV_ASSIGNMENT_RE.sub(r"\1[REDACTED]", cleaned)
    cleaned = _LOG_JSON_SECRET_RE.sub(r"\1[REDACTED]\2", cleaned)
    cleaned = _LOG_BOT_TOKEN_RE.sub("[REDACTED_BOT_TOKEN]", cleaned)
    return cleaned


def _delete_runtime_log_files(*paths: Path) -> None:
    for path in paths:
        base_path = path.expanduser().resolve()
        base_path.parent.mkdir(parents=True, exist_ok=True)
        for candidate in base_path.parent.glob(f"{base_path.name}*"):
            if candidate.name == base_path.name or candidate.name.startswith(f"{base_path.name}."):
                with contextlib.suppress(Exception):
                    candidate.unlink()


def _reset_runtime_logging() -> None:
    global _runtime_logging_configured, _runtime_root_previous_level
    global _runtime_logger_previous_level, _runtime_logger_previous_propagate
    global _runtime_file_handler, _error_handler, _console_handler
    global _runtime_activity_buffer_handler

    root_logger = logging.getLogger()
    for handler in (_runtime_file_handler, _error_handler, _runtime_activity_buffer_handler):
        if handler is None:
            continue
        if handler in root_logger.handlers:
            root_logger.removeHandler(handler)
        with contextlib.suppress(Exception):
            handler.close()

    if _console_handler is not None and _console_handler in LOGGER.handlers:
        LOGGER.removeHandler(_console_handler)
        with contextlib.suppress(Exception):
            _console_handler.close()

    if _runtime_root_previous_level is not None:
        root_logger.setLevel(_runtime_root_previous_level)
    if _runtime_logger_previous_level is not None:
        LOGGER.setLevel(_runtime_logger_previous_level)
    if _runtime_logger_previous_propagate is not None:
        LOGGER.propagate = _runtime_logger_previous_propagate

    _runtime_file_handler = None
    _error_handler = None
    _console_handler = None
    _runtime_activity_buffer_handler = None
    _runtime_root_previous_level = None
    _runtime_logger_previous_level = None
    _runtime_logger_previous_propagate = None
    _runtime_logging_configured = False


def _recent_runtime_activity(limit: int = 24):
    if _runtime_activity_buffer_handler is None:
        return []
    return _runtime_activity_buffer_handler.snapshot(limit=limit)


def _configure_runtime_logging() -> None:
    global _runtime_logging_configured, _runtime_root_previous_level
    global _runtime_logger_previous_level, _runtime_logger_previous_propagate
    global _runtime_file_handler, _error_handler, _console_handler
    global _runtime_activity_buffer_handler

    if _runtime_logging_configured:
        return

    ensure_runtime_dir()
    _delete_runtime_log_files(RUNTIME_LOG_PATH, ERROR_LOG_PATH)

    file_formatter = RuntimeActivityFormatter(surface="file", sanitize=_sanitize_log_text)
    console_formatter = RuntimeActivityFormatter(
        surface="console",
        sanitize=_sanitize_log_text,
        color=_COLOR_ON,
    )
    root_logger = logging.getLogger()
    _runtime_root_previous_level = root_logger.level
    _runtime_logger_previous_level = LOGGER.level
    _runtime_logger_previous_propagate = LOGGER.propagate

    _runtime_file_handler = RotatingFileHandler(
        RUNTIME_LOG_PATH,
        maxBytes=_LOG_ROTATION_MAX_BYTES,
        backupCount=_LOG_ROTATION_BACKUP_COUNT,
        encoding="utf-8",
    )
    _runtime_file_handler.setLevel(logging.DEBUG)
    _runtime_file_handler.setFormatter(file_formatter)

    _error_handler = RotatingFileHandler(
        ERROR_LOG_PATH,
        maxBytes=_LOG_ROTATION_MAX_BYTES,
        backupCount=_LOG_ROTATION_BACKUP_COUNT,
        encoding="utf-8",
    )
    _error_handler.setLevel(logging.ERROR)
    _error_handler.setFormatter(file_formatter)

    _console_handler = logging.StreamHandler()
    _console_handler.setLevel(logging.INFO)
    _console_handler.setFormatter(console_formatter)

    _runtime_activity_buffer_handler = RuntimeActivityBufferHandler(
        capacity=120,
        sanitize=_sanitize_log_text,
    )
    _runtime_activity_buffer_handler.setLevel(logging.INFO)

    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(_runtime_file_handler)
    root_logger.addHandler(_error_handler)
    root_logger.addHandler(_runtime_activity_buffer_handler)

    LOGGER.setLevel(logging.DEBUG)
    LOGGER.propagate = True
    LOGGER.addHandler(_console_handler)
    _runtime_logging_configured = True
    _log_memory_snapshot("logger_bootstrap_complete", phase=startup_phase)


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
source_alias_cache: Dict[str, set[str]] = defaultdict(set)
digest_next_run_ts: float | None = None
digest_retry_backoff_seconds: int = 0
digest_loop_lock = asyncio.Lock()
ROLLING_DIGEST_LAST_COMPLETED_META_KEY = "rolling_digest_last_completed_window_end_ts"
dupe_detector: HybridDuplicateEngine | None = None
monitored_source_chat_ids: List[int] = []
query_last_request_ts: Dict[int, float] = {}
query_conversation_history: Dict[int, Deque[Dict[str, str]]] = defaultdict(
    lambda: deque(maxlen=20)
)
query_bot_updates_offset: int = 0
query_bot_poll_started_at: int = 0
breaking_delivery_refs: Dict[str, Dict[str, object]] = {}
breaking_story_ref_cache: Dict[str, object] = {}
breaking_topic_threads: List[Dict[str, object]] = []
breaking_topic_lock = asyncio.Lock()
delivery_context_stats: Dict[str, int] = defaultdict(int)
query_allowed_bot_user_id: int | None = None
query_bot_user_id_checked: bool = False
instance_lock_handle = None
web_status_server: WebStatusServer | None = None
started_as_username: str = "unknown"
started_as_user_id: int = 0
startup_phase: str = "booting"
startup_ready: bool = False
startup_error: str = ""
auth_startup_mode_configured: str = "auto"
auth_startup_mode_effective: str = "auto"
auth_ready: bool = False
auth_degraded: bool = False
auth_failure_reason: str = ""
auth_features_disabled: List[str] = []
startup_auth_repair_status: str = "not_needed"
startup_auth_repair_message: str = ""
pipeline_worker_tasks: List[asyncio.Task] = []
pipeline_sentence_transformer_warm_task: asyncio.Task | None = None
pipeline_query_web_semaphore: asyncio.Semaphore | None = None
pipeline_media_semaphore: asyncio.Semaphore | None = None
pipeline_stage_latency_seconds: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=200))
pipeline_stage_runs: Dict[str, int] = defaultdict(int)
runtime_memory_warning_logged: bool = False

INBOUND_STAGE_TRIAGE = "triage"
INBOUND_STAGE_OCR = "ocr"
INBOUND_STAGE_AI_DECISION = "ai_decision"
INBOUND_STAGE_DELIVERY = "delivery_or_queue"
INBOUND_STAGE_ARCHIVE = "archive"

ALBUM_WAIT_SECONDS = 1.5
ENV_PATH = Path(__file__).with_name(".env")
_UNKNOWN_MEMORY_BUDGET = object()
runtime_memory_budget_bytes_cache: int | None | object = _UNKNOWN_MEMORY_BUDGET


def _linux_memory_budget_bytes() -> int | None:
    if not sys.platform.startswith("linux"):
        return None

    budget_candidates: List[int] = []

    meminfo_path = Path("/proc/meminfo")
    with contextlib.suppress(Exception):
        for line in meminfo_path.read_text(encoding="utf-8").splitlines():
            if not line.startswith("MemTotal:"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                budget_candidates.append(int(parts[1]) * 1024)
            break

    for raw_path in (
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ):
        path = Path(raw_path)
        if not path.exists():
            continue
        with contextlib.suppress(Exception):
            raw_value = path.read_text(encoding="utf-8").strip().lower()
            if not raw_value or raw_value == "max":
                continue
            value = int(raw_value)
            if value > 0 and value < (1 << 60):
                budget_candidates.append(value)

    if not budget_candidates:
        return None
    return max(0, min(budget_candidates))


def _runtime_memory_budget_bytes() -> int | None:
    global runtime_memory_budget_bytes_cache

    if runtime_memory_budget_bytes_cache is _UNKNOWN_MEMORY_BUDGET:
        runtime_memory_budget_bytes_cache = _linux_memory_budget_bytes()
    if runtime_memory_budget_bytes_cache is None:
        return None
    if isinstance(runtime_memory_budget_bytes_cache, int):
        return runtime_memory_budget_bytes_cache
    return None


def _parse_linux_process_status_memory(status_text: str) -> Dict[str, int]:
    metrics: Dict[str, int] = {}
    for raw_line in str(status_text or "").splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        parts = value.strip().split()
        if not parts:
            continue
        try:
            numeric = int(parts[0])
        except Exception:
            continue
        unit = parts[1].lower() if len(parts) >= 2 else "bytes"
        size_bytes = numeric * 1024 if unit == "kb" else numeric
        normalized_key = key.strip().lower()
        if normalized_key == "vmrss":
            metrics["memory_current_bytes"] = size_bytes
        elif normalized_key == "vmhwm":
            metrics["memory_peak_bytes"] = size_bytes
        elif normalized_key == "vmswap":
            metrics["memory_swap_bytes"] = size_bytes
    return metrics


def _process_memory_snapshot() -> Dict[str, object]:
    snapshot: Dict[str, object] = {
        "memory_source": "unknown",
        "memory_current_bytes": 0,
        "memory_peak_bytes": 0,
        "memory_swap_bytes": 0,
    }

    if sys.platform.startswith("linux"):
        status_path = Path("/proc/self/status")
        with contextlib.suppress(Exception):
            parsed = _parse_linux_process_status_memory(status_path.read_text(encoding="utf-8"))
            if parsed:
                snapshot.update(parsed)
                snapshot["memory_source"] = "proc_status"
                snapshot["memory_budget_bytes"] = int(_runtime_memory_budget_bytes() or 0)
                return snapshot

    if resource is not None:
        with contextlib.suppress(Exception):
            usage = resource.getrusage(resource.RUSAGE_SELF)
            peak_value = int(getattr(usage, "ru_maxrss", 0) or 0)
            if peak_value > 0:
                if sys.platform == "darwin":
                    peak_bytes = peak_value
                else:
                    peak_bytes = peak_value * 1024
                snapshot["memory_peak_bytes"] = peak_bytes
                if not snapshot.get("memory_current_bytes"):
                    snapshot["memory_current_bytes"] = peak_bytes
                snapshot["memory_source"] = "resource_rusage"

    snapshot["memory_budget_bytes"] = int(_runtime_memory_budget_bytes() or 0)
    return snapshot


def _log_memory_snapshot(reason: str, *, phase: str | None = None, **fields: object) -> None:
    snapshot = _process_memory_snapshot()
    payload = {
        **snapshot,
        "memory_reason": normalize_space(reason) or "unspecified",
        "startup_phase": normalize_space(phase or startup_phase or "") or "unknown",
        **fields,
    }
    log_structured(LOGGER, "memory_snapshot", level=logging.DEBUG, **payload)


def _set_startup_phase(phase: str, *, reason: str = "") -> None:
    global startup_phase
    startup_phase = normalize_space(phase) or "unknown"
    _log_memory_snapshot(
        "startup_phase_changed",
        phase=startup_phase,
        phase_reason=normalize_space(reason) or startup_phase,
    )


def _is_low_memory_runtime() -> bool:
    budget = _runtime_memory_budget_bytes()
    if budget is None:
        return False
    # If < 2.5GB RAM, we consider it low memory for a Python/FFmpeg app with no swap.
    return budget <= (2560 * 1024 * 1024)


def _log_low_memory_runtime_once(reason: str) -> None:
    global runtime_memory_warning_logged, pipeline_media_semaphore

    if runtime_memory_warning_logged or not _is_low_memory_runtime():
        return
    runtime_memory_warning_logged = True
    budget = _runtime_memory_budget_bytes()
    mib = int((budget or 0) / (1024 * 1024))
    
    # Auto-throttle media concurrency if budget is tight.
    if budget <= (1024 * 1024 * 1024):
        concurrency = 1
    else:
        concurrency = 1
    
    if pipeline_media_semaphore and pipeline_media_semaphore._value > concurrency:
        pipeline_media_semaphore = asyncio.Semaphore(concurrency)

    LOGGER.warning(
        "Low-memory runtime detected (%s MiB budget). Applying conservative memory guards: %s",
        mib,
        reason,
    )
    _log_memory_snapshot(
        "low_memory_guard_enabled",
        low_memory_reason=normalize_space(reason),
        memory_budget_bytes=int(budget or 0),
    )


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


def _auth_startup_mode_from_config() -> str:
    raw = str(getattr(config, "OPENAI_AUTH_STARTUP_MODE", "auto") or "").strip().lower()
    if raw in {"strict", "degraded", "auto"}:
        return raw
    return "auto"


def _resolve_auth_startup_mode(configured: str | None = None) -> str:
    selected = (configured or _auth_startup_mode_from_config()).strip().lower()
    if selected == "strict":
        return "strict"
    if selected == "degraded":
        return "degraded"
    return "strict" if _is_interactive_runtime() else "degraded"


def _auth_env_only_mode() -> bool:
    raw = str(os.getenv("OPENAI_AUTH_ENV_ONLY", "true") or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _auth_disabled_features_for_runtime() -> List[str]:
    if auth_ready and not auth_degraded:
        return []
    return ["query_mode", "ocr_translation", "vital_rational_view"]


def _trim_runtime_reason(text: str, *, limit: int = 220) -> str:
    cleaned = normalize_space(text)
    if len(cleaned) <= limit:
        return cleaned
    shortened = cleaned[: limit - 3].rsplit(" ", 1)[0].rstrip()
    return f"{shortened}..."


def _auth_fix_commands() -> List[str]:
    if _auth_env_only_mode():
        return [
            "python auth.py bootstrap-env",
            "python auth.py setup-env",
        ]
    return ["python auth.py login"]


def _auth_fix_guidance_text() -> str:
    commands = _auth_fix_commands()
    if _auth_env_only_mode():
        return (
            "Generate env auth locally with "
            f"{' or '.join(commands)}. "
            "Then set TG_USERBOT_AUTH_JSON or TG_USERBOT_AUTH_JSON_B64 and restart."
        )
    return f"Run {commands[0]} and restart."


def _set_auth_runtime_state(
    *,
    configured_mode: str,
    effective_mode: str,
    ready: bool,
    degraded: bool,
    failure_reason: str = "",
    features_disabled: Sequence[str] | None = None,
) -> None:
    global auth_startup_mode_configured, auth_startup_mode_effective
    global auth_ready, auth_degraded, auth_failure_reason, auth_features_disabled

    auth_startup_mode_configured = configured_mode
    auth_startup_mode_effective = effective_mode
    auth_ready = bool(ready)
    auth_degraded = bool(degraded)
    auth_failure_reason = normalize_space(failure_reason)
    disabled = [normalize_space(item) for item in (features_disabled or []) if normalize_space(item)]
    auth_features_disabled = disabled


def _set_startup_auth_repair_state(*, status: str, message: str = "") -> None:
    global startup_auth_repair_status, startup_auth_repair_message

    startup_auth_repair_status = normalize_space(status) or "not_needed"
    startup_auth_repair_message = normalize_space(message)


def _extract_auth_startup_reason(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return "OpenAI auth unavailable."
    first_line = cleaned.splitlines()[0].strip()
    return normalize_space(first_line or cleaned)


def _is_interactive_auth_repair_candidate(reason: str) -> bool:
    lowered = _extract_auth_startup_reason(reason).lower()
    if not lowered:
        return False
    markers = (
        "missing oauth token in env",
        "oauth access token missing in env payload",
        "no refresh_token available in env payload",
        "oauth refresh failed in env-only mode",
        "stored env refresh token is stale",
        "refresh token is stale",
        "refresh token has already been used",
        "refresh_token_reused",
        "authentication token has been invalidated",
        "re-authenticate disabled in env-only mode",
        "update tg_userbot_auth_json",
        "update tg_userbot_auth_json/b64",
    )
    return any(marker in lowered for marker in markers)


def _startup_auth_requires_guided_bootstrap() -> bool:
    if not _is_interactive_runtime():
        return False
    try:
        status = AuthManager(logger=LOGGER).status()
    except Exception:
        return False
    return not bool(status.get("has_access_token"))


def _refresh_runtime_from_env_mutation(updates: Dict[str, str | None]) -> None:
    global config, destination_peer, bot_destination_token, bot_destination_chat_id
    global query_allowed_bot_user_id, query_bot_user_id_checked

    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)

    config = importlib.reload(config)
    destination_peer = None
    bot_destination_token = None
    bot_destination_chat_id = None
    query_allowed_bot_user_id = None
    query_bot_user_id_checked = False


async def _repair_auth_startup_env(reason: str) -> None:
    primary_reason = _extract_auth_startup_reason(reason)
    _set_startup_auth_repair_state(status="failed", message=primary_reason)
    LOGGER.warning(
        "OpenAI auth unavailable at startup; attempting guided env bootstrap: %s",
        primary_reason,
    )
    _print_cli_status(
        "!",
        "OpenAI auth missing or stale; starting guided browser login and saving to .env",
        level="warn",
    )

    try:
        payload = await bootstrap_env_oauth_payload(logger=LOGGER)
        env_path = write_auth_payload_to_env_file(payload, ENV_PATH)
        _refresh_runtime_from_env_mutation(
            {
                ENV_AUTH_ENV_ONLY: "true",
                ENV_AUTH_JSON: "",
                ENV_AUTH_JSON_B64: str(payload.get("b64") or ""),
            }
        )
    except Exception as exc:
        repaired_reason = normalize_oauth_error_message(exc)
        _set_startup_auth_repair_state(status="failed", message=repaired_reason)
        raise RuntimeError(
            f"OpenAI guided auth bootstrap failed: {repaired_reason}\n"
            "Run python auth.py setup-env and retry startup."
        ) from exc

    account_id = normalize_space(str(payload.get("account_id") or ""))
    repaired_message = f"Saved OpenAI env auth to {env_path.name}"
    if account_id:
        repaired_message = f"{repaired_message} (account_id={account_id})"
    _set_startup_auth_repair_state(status="repaired", message=repaired_message)
    _print_cli_status(
        "+",
        "OpenAI auth saved to .env; retrying startup auth validation",
        level="ok",
    )


async def _prepare_auth_runtime_for_startup() -> None:
    _set_startup_auth_repair_state(status="not_needed", message="")
    attempted_repair = False

    if _startup_auth_requires_guided_bootstrap():
        await _repair_auth_startup_env("OpenAI auth is not configured for startup.")
        attempted_repair = True
    else:
        try:
            await _prepare_auth_runtime()
        except RuntimeError as exc:
            reason = _extract_auth_startup_reason(str(exc))
            if not _is_interactive_runtime() or not _is_interactive_auth_repair_candidate(reason):
                _set_startup_auth_repair_state(status="skipped", message=reason)
                raise
            await _repair_auth_startup_env(reason)
            attempted_repair = True
        else:
            if auth_ready:
                return
            if not _is_interactive_runtime():
                if auth_failure_reason:
                    _set_startup_auth_repair_state(
                        status="skipped",
                        message=_extract_auth_startup_reason(auth_failure_reason),
                    )
                return
            if not auth_failure_reason or not _is_interactive_auth_repair_candidate(auth_failure_reason):
                if auth_failure_reason:
                    _set_startup_auth_repair_state(
                        status="skipped",
                        message=_extract_auth_startup_reason(auth_failure_reason),
                    )
                return
            await _repair_auth_startup_env(auth_failure_reason)
            attempted_repair = True

    if not attempted_repair:
        return

    try:
        await _prepare_auth_runtime()
    except RuntimeError as exc:
        reason = _extract_auth_startup_reason(str(exc))
        _set_startup_auth_repair_state(status="failed", message=reason)
        raise RuntimeError(
            f"{reason}\nGuided browser auth saved env credentials, but startup still could not validate them."
        ) from exc

    if not auth_ready:
        reason = _extract_auth_startup_reason(auth_failure_reason or "OpenAI auth unavailable.")
        _set_startup_auth_repair_state(status="failed", message=reason)
        raise RuntimeError(
            f"{reason}\nGuided browser auth completed, but startup still could not validate OpenAI auth."
        )

def _query_mode_temporarily_unavailable_text() -> str:
    reason = sanitize_telegram_html(_trim_runtime_reason(auth_failure_reason or "OpenAI auth unavailable."))
    command_text = " | ".join(sanitize_telegram_html(item) for item in _auth_fix_commands())
    return (
        "<b>Query mode temporarily unavailable</b><br>"
        "The bot is running in degraded auth mode, so AI-backed private queries are disabled.<br>"
        f"Reason: <code>{reason}</code><br>"
        f"Fix: <code>{command_text}</code>, then restart."
    )


def _auth_status_summary() -> str:
    if auth_ready and not auth_degraded:
        return "ready"
    if auth_degraded:
        return "degraded"
    return "unavailable"


def _is_query_runtime_available() -> bool:
    return _is_query_mode_enabled() and auth_ready and not auth_degraded


def _format_strict_auth_startup_error(reason: str) -> str:
    guidance = _auth_fix_guidance_text()
    if guidance:
        return f"{reason}\n{guidance}"
    return reason


async def _prepare_auth_runtime() -> None:
    global auth_manager

    configured_mode = _auth_startup_mode_from_config()
    effective_mode = _resolve_auth_startup_mode(configured_mode)
    auth_manager = AuthManager(logger=LOGGER)
    _set_auth_runtime_state(
        configured_mode=configured_mode,
        effective_mode=effective_mode,
        ready=False,
        degraded=False,
        failure_reason="",
        features_disabled=[],
    )

    try:
        await auth_manager.get_access_token()
    except OAuthError as exc:
        reason = normalize_oauth_error_message(exc)
        features_disabled = _auth_disabled_features_for_runtime()
        if effective_mode == "degraded":
            _set_auth_runtime_state(
                configured_mode=configured_mode,
                effective_mode=effective_mode,
                ready=False,
                degraded=True,
                failure_reason=reason,
                features_disabled=features_disabled,
            )
            LOGGER.warning("OpenAI auth unavailable; continuing in degraded mode: %s", reason)
            LOGGER.warning("%s", _auth_fix_guidance_text())
            return

        _set_auth_runtime_state(
            configured_mode=configured_mode,
            effective_mode=effective_mode,
            ready=False,
            degraded=False,
            failure_reason=reason,
            features_disabled=features_disabled,
        )
        raise RuntimeError(_format_strict_auth_startup_error(reason)) from exc

    _set_auth_runtime_state(
        configured_mode=configured_mode,
        effective_mode=effective_mode,
        ready=True,
        degraded=False,
        failure_reason="",
        features_disabled=[],
    )


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
    raw = getattr(config, "DIGEST_DAILY_TIMES", ["00:00"])
    if not isinstance(raw, list):
        return []
    values = [str(x) for x in raw]
    return parse_daily_times(values)


def _digest_queue_clear_interval_seconds() -> int:
    # Queue clear scheduler is intentionally disabled to preserve full intake flow.
    # Digest queue should only be drained by rolling digest window claiming.
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
    if _is_digest_mode_enabled():
        return False
    return _bool_flag(getattr(config, "IMMEDIATE_HIGH", True), True)


def _include_source_tags() -> bool:
    return _bool_flag(getattr(config, "INCLUDE_SOURCE_TAGS", False), False)


def _resolve_outbound_post_layout() -> str:
    raw = normalize_space(str(getattr(config, "OUTBOUND_POST_LAYOUT", "editorial_card") or "")).lower()
    if raw == "legacy":
        return "legacy"
    return "editorial_card"


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
    raw = getattr(config, "QUERY_WEB_MAX_HOURS_BACK", 24 * 7)
    try:
        value = int(raw)
    except Exception:
        value = 24 * 7
    return max(1, min(value, 24 * 7))


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


def _query_count_label(count: int, singular: str, plural: str | None = None) -> str:
    noun = singular if count == 1 else (plural or f"{singular}s")
    return f"{count} {noun}"


def _query_search_status(include_web: bool) -> str:
    if include_web:
        return "Searching your channels and trusted web sources... ⏳"
    return "Searching your channels for recent coverage... ⏳"


def _query_crosscheck_status(*, high_risk: bool) -> str:
    if high_risk:
        return "High-risk query detected. Verifying with trusted web sources... ⏳"
    return "Checking trusted web sources to confirm the latest details... ⏳"


def _query_analysis_status(telegram_count: int, web_count: int) -> str:
    parts = [_query_count_label(telegram_count, "Telegram update")]
    if web_count > 0:
        parts.append(_query_count_label(web_count, "trusted web report"))
    return f"{' and '.join(parts)} gathered. Building your answer now... ⏳"


def _query_writing_status() -> str:
    return "Writing a clear answer from the strongest evidence... ⏳"


def _query_expand_status() -> str:
    return "Need more context. Widening the Telegram scan to the last 7 days... ⏳"


def _query_expanded_window_hours() -> int:
    return 24 * 7


def _query_window_strategy(*, requested_hours: int, explicit_time_filter: bool) -> tuple[int, int]:
    primary_hours = max(1, int(requested_hours))
    if explicit_time_filter:
        return primary_hours, primary_hours
    return min(primary_hours, 24), max(primary_hours, _query_expanded_window_hours())


def _query_needs_expanded_window(
    query: str,
    results: Sequence[Dict[str, object]],
    *,
    broad_query: bool,
) -> bool:
    if len(results) < _query_web_min_telegram_results():
        return True
    if broad_query and len(results) < max(4, _query_web_min_telegram_results() + 1):
        return True

    terms = {
        normalize_space(term).lower()
        for term in expand_query_terms(query)
        if len(normalize_space(term)) >= 3
    }
    if not terms:
        return False

    matched_rows = 0
    for row in list(results)[:6]:
        text = normalize_space(str(row.get("text") or "")).lower()
        if not text:
            continue
        if any(term in text for term in terms):
            matched_rows += 1
    return matched_rows == 0


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
        http = await get_bot_http_client(httpx.Timeout(15.0))
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
    raw = getattr(config, "BREAKING_NEWS_KEYWORDS", [])
    result: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, str):
                continue
            normalized = item.strip().lower()
            if normalized and normalized not in result:
                result.append(normalized)
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


def _is_breaking_story_clusters_enabled() -> bool:
    return _bool_flag(getattr(config, "ENABLE_BREAKING_STORY_CLUSTERS", True), True)


def _breaking_story_window_seconds() -> int:
    raw = getattr(config, "BREAKING_STORY_WINDOW_MINUTES", 180)
    try:
        value = int(raw)
    except Exception:
        value = 180
    return max(30 * 60, min(value * 60, 24 * 60 * 60))


def _breaking_story_burst_seconds() -> int:
    raw = getattr(config, "BREAKING_STORY_BURST_SECONDS", 60)
    try:
        value = int(raw)
    except Exception:
        value = 60
    return max(15, min(value, 15 * 60))


def _humanized_vital_opinion_enabled() -> bool:
    return _bool_flag(getattr(config, "HUMANIZED_VITAL_OPINION_ENABLED", True), True)


def _humanized_vital_opinion_probability() -> float:
    raw = getattr(config, "HUMANIZED_VITAL_OPINION_PROBABILITY", 0.35)
    try:
        value = float(raw)
    except Exception:
        value = 0.35
    return min(max(value, 0.0), 1.0)


def _should_attach_vital_opinion(text: str, recent_context: Sequence[str] | None = None) -> bool:
    if not _humanized_vital_opinion_enabled():
        return False
    if resolve_breaking_style_mode() == "unhinged":
        return bool(recent_context)
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
    normalized = normalize_space(text)
    if not normalized:
        return False
    signals = detect_story_signals(normalized)
    if bool(signals.get("downgrade_explainer")) or not bool(signals.get("live_event_update")):
        return False
    if bool(signals.get("breaking_eligible")) or match_breaking_category(normalized) is not None:
        return True
    keywords = _breaking_keywords()
    if not keywords:
        return False
    lowered = normalized.lower()
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

    http = await get_bot_http_client(_bot_api_timeout())
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
    http = await get_bot_http_client(httpx.Timeout(60.0))
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


def _format_summary_text_legacy(source_title: str, summary: str) -> str:
    category_label = "Update"
    headline, context = extract_feed_summary_parts(str(summary or ""), str(summary or ""))
    clean_headline = _clean_editorial_headline(
        headline or str(summary or ""),
        source_title=source_title,
        category_label=category_label,
        severity="medium",
    )
    if not clean_headline:
        return ""
    clean_context = _clean_editorial_context(
        context,
        headline=clean_headline,
        source_title=source_title,
        category_label=category_label,
        severity="medium",
    )
    safe_source = sanitize_telegram_html(source_title)
    safe_summary = sanitize_telegram_html(clean_headline)
    if clean_context:
        safe_summary = f"{safe_summary}<br><br>Why it matters: {sanitize_telegram_html(clean_context)}"
    if _include_source_tags():
        return f"<b>📰 {safe_source}</b><br><br>{safe_summary}"
    return f"<b>📰 Update</b><br><br>{safe_summary}"


def _plain_text_from_html_fragment(text: str) -> str:
    if not text:
        return ""
    normalized = re.sub(r"(?i)<br\s*/?>", "\n", str(text))
    return normalize_space(strip_telegram_html(normalized))


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


def _caption_lines_from_fragment(text: str | None) -> List[str]:
    raw = re.sub(r"(?i)<br\s*/?>", "\n", str(text or ""))
    lines: List[str] = []
    for part in re.split(r"\n+", raw):
        cleaned = normalize_space(strip_telegram_html(part))
        if cleaned:
            lines.append(cleaned)
    return lines


def _render_caption_lines_html(lines: Sequence[str], *, bold_first: bool = False) -> str:
    rendered: List[str] = []
    for idx, line in enumerate(lines):
        safe_line = sanitize_telegram_html(normalize_space(line))
        if not safe_line:
            continue
        if idx == 0 and bold_first:
            rendered.append(f"<b>{safe_line}</b>")
        else:
            rendered.append(safe_line)
    return "<br><br>".join(rendered)


def _render_caption_lines_plain(lines: Sequence[str]) -> str:
    rendered = [normalize_space(line) for line in lines if normalize_space(line)]
    return "\n\n".join(rendered)


def _hard_wrap_caption_text(text: str, *, limit: int) -> List[str]:
    cleaned = normalize_space(text)
    if not cleaned:
        return []
    if len(cleaned) <= limit:
        return [cleaned]

    chunks: List[str] = []
    remaining = cleaned
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        window = remaining[:limit].rstrip()
        cut = max(
            window.rfind(". "),
            window.rfind("; "),
            window.rfind(", "),
            window.rfind(" - "),
            window.rfind(" — "),
        )
        if cut < int(limit * 0.55):
            cut = window.rfind(" ")
        if cut < int(limit * 0.55):
            cut = len(window)
        piece = normalize_space(window[:cut].rstrip(" ,;:-/"))
        words = piece.split()
        while words and words[-1].strip(".,;:!?").lower() in _CAPTION_INCOMPLETE_TAIL_WORDS:
            words.pop()
        piece = normalize_space(" ".join(words)) or normalize_space(window[:cut])
        if not piece:
            break
        chunks.append(piece)
        remaining = normalize_space(remaining[cut:].lstrip(" ,;:-/"))
    return chunks


def _split_caption_line_segments(text: str, *, limit: int) -> List[str]:
    cleaned = normalize_space(text)
    if not cleaned:
        return []
    if len(cleaned) <= limit:
        return [cleaned]

    sentences = [
        normalize_space(part)
        for part in re.split(r"(?<=[.!?;])\s+", cleaned)
        if normalize_space(part)
    ]
    if len(sentences) <= 1:
        return _hard_wrap_caption_text(cleaned, limit=limit)

    segments: List[str] = []
    current = ""
    for sentence in sentences:
        candidate = f"{current} {sentence}".strip() if current else sentence
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            segments.append(current)
            current = ""
        if len(sentence) <= limit:
            current = sentence
            continue
        segments.extend(_hard_wrap_caption_text(sentence, limit=limit))
    if current:
        segments.append(current)
    return segments or _hard_wrap_caption_text(cleaned, limit=limit)


def _fit_caption_segments(
    segments: Sequence[str],
    *,
    limit: int,
    use_html: bool,
) -> tuple[List[str], List[str]]:
    if not segments:
        return [], []
    renderer = _render_caption_lines_html if use_html else _render_caption_lines_plain
    fitted: List[str] = []
    for idx, segment in enumerate(segments):
        candidate = fitted + [segment]
        if fitted and len(renderer(candidate)) > limit:
            return fitted, list(segments[idx:])
        fitted = candidate
    return fitted, []


def _render_caption_chunk_batch(
    segments: Sequence[str],
    *,
    use_html: bool,
) -> List[str]:
    if not segments:
        return []
    renderer = _render_caption_lines_html if use_html else _render_caption_lines_plain
    chunks: List[str] = []
    remaining = list(segments)
    while remaining:
        fitted, overflow = _fit_caption_segments(
            remaining,
            limit=_MEDIA_CAPTION_MAX_CHARS,
            use_html=use_html,
        )
        if not fitted:
            fitted = [remaining[0]]
            overflow = remaining[1:]
        rendered = renderer(fitted)
        if rendered:
            chunks.append(rendered)
        remaining = overflow
    return chunks


def _prepare_media_caption_chunks(
    caption: str | None,
    *,
    allow_premium_tags: bool,
) -> tuple[str | None, List[str]]:
    if not caption:
        return None, []

    lines = _caption_lines_from_fragment(caption)
    if not lines:
        return None, []

    use_html = _is_html_formatting_enabled()
    header = lines[0] if re.fullmatch(r"〔.+〕", lines[0]) else ""
    body_lines = lines[1:] if header else lines
    body_segments: List[str] = []
    body_segment_limit = max(140, _MEDIA_CAPTION_MAX_CHARS - 120)
    for line in body_lines:
        body_segments.extend(_split_caption_line_segments(line, limit=body_segment_limit))

    if use_html:
        header_raw = _render_caption_lines_html([header], bold_first=True) if header else ""
        renderer = _render_caption_lines_html
    else:
        header_raw = _render_caption_lines_plain([header]) if header else ""
        renderer = _render_caption_lines_plain

    separator_len = len("<br><br>") if use_html else len("\n\n")
    if header_raw and body_segments:
        first_limit = max(160, _MEDIA_CAPTION_MAX_CHARS - len(header_raw) - separator_len)
    else:
        first_limit = _MEDIA_CAPTION_MAX_CHARS

    first_body_segments, remaining_segments = _fit_caption_segments(
        body_segments,
        limit=first_limit,
        use_html=use_html,
    )
    first_body_raw = renderer(first_body_segments) if first_body_segments else ""
    if header_raw and first_body_raw:
        separator = "<br><br>" if use_html else "\n\n"
        first_raw = f"{header_raw}{separator}{first_body_raw}"
    else:
        first_raw = header_raw or first_body_raw
    overflow_raw = _render_caption_chunk_batch(remaining_segments, use_html=use_html)

    first = _render_outbound_text(first_raw, allow_premium_tags=allow_premium_tags) if first_raw else None
    overflow = [
        rendered
        for chunk in overflow_raw
        if chunk and (rendered := _render_outbound_text(chunk, allow_premium_tags=allow_premium_tags))
    ]
    return first, overflow


async def _send_media_caption_overflow(
    overflow_chunks: Sequence[str],
    *,
    sent_ref: object | None,
) -> None:
    if not overflow_chunks:
        return
    reply_to = _message_ref_id(_primary_message_ref(sent_ref))
    for chunk in overflow_chunks:
        await _send_text_with_ref(chunk, reply_to=reply_to)


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

    source_title_cache[str(msg.chat_id)] = source
    _register_source_aliases(str(msg.chat_id), source, title, username)

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
            username = getattr(entity, "username", None)
            _register_source_aliases(channel_id, candidate_title, username)
            if isinstance(candidate_title, str) and candidate_title.strip():
                title = candidate_title.strip()
                break
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
    enabled = bool(getattr(config, "MEDIA_TEXT_OCR_VIDEO_ENABLED", True))
    if enabled and _is_low_memory_runtime():
        _log_low_memory_runtime_once("video OCR disabled to reduce peak memory use")
        return False
    return enabled


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

    async with (pipeline_media_semaphore or contextlib.nullcontext()):
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


def _format_ocr_translation_caption(translated_text: str) -> str | None:
    cleaned = normalize_space(strip_telegram_html(translated_text))
    if not cleaned:
        return None
    capped = _truncate_caption_fragment_complete(
        cleaned,
        limit=min(_media_text_ocr_max_chars(), 700),
    )
    if not capped or not _caption_fragment_is_usable(capped):
        return None
    return f"Translate: {sanitize_telegram_html(capped)}"


async def _extract_media_ocr_translation(msg: Message) -> str | None:
    if not _media_text_ocr_enabled():
        return None
    if not getattr(msg, "media", None):
        return None
    if not auth_ready:
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
    return _format_ocr_translation_caption(translated)


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


def _high_severity_score_floor() -> float:
    return float(severity_score_floor("high"))


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


_EDITORIAL_CONTEXT_BANNED_FRAGMENTS = (
    "regional stability",
    "worth watching",
    "situation remains tense",
    "civilian danger",
    "what explains",
    "reasons for",
    "ways to address",
)
_EDITORIAL_PROMO_PREFIX_RE = re.compile(
    r"^(?:flash update|live update|news update|breaking|alert|update)\s*[:\-–—]+\s*",
    flags=re.IGNORECASE,
)
_EDITORIAL_FLAG_PREFIX_RE = re.compile(r"^(?:[\U0001F1E6-\U0001F1FF]{2}\s*)+")
_EDITORIAL_EMOJI_PREFIX_RE = re.compile(r"^(?:[\U0001F300-\U0001FAFF\u2600-\u26FF\u2700-\u27BF\uFE0F]+\s*)+")
_EDITORIAL_HEADLINE_MAX_STANDARD = 160
_EDITORIAL_HEADLINE_MAX_BREAKING = 180
_EDITORIAL_CONTEXT_MAX_STANDARD = 200
_EDITORIAL_CONTEXT_MAX_BREAKING = 220
_CAPTION_HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{2,}\b")
_CAPTION_TELEGRAM_LINK_RE = re.compile(
    r"(?i)\b(?:https?://)?(?:t\.me|telegram\.me)/[A-Za-z0-9_+./-]+"
)
_CAPTION_PROMO_TO_END_RE = re.compile(
    r"(?i)\b(?:our channel|subscribe|follow\s+us|join(?: us| our channel)?|"
    r"watch here|watch live|livestream|live stream|full stream|full video|"
    r"watch the livestream|watch the full livestream)\b.*$"
)
_CAPTION_FOLLOW_PROMO_ONLY_RE = re.compile(
    r"(?i)^\s*follow(?=\s*(?:$|\||discussion\b|boost the channel\b|our channel\b|"
    r"subscribe\b|join\b|watch\b|:|[-–—](?=\s|$))).*$"
)
_CAPTION_SOURCE_CLAUSE_RE = re.compile(
    r"(?i)\b(?:our channel|source|channel)\s*[:\-–—|]+\s*$"
)
_CAPTION_INCOMPLETE_TAIL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "because",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "via",
    "with",
}
_CAPTION_GEO_PREFIX_ALLOWLIST = {
    "beirut",
    "damascus",
    "egypt",
    "eu",
    "gaza",
    "haifa",
    "iran",
    "iraq",
    "israel",
    "jordan",
    "lebanon",
    "russia",
    "syria",
    "tehran",
    "uae",
    "u.s.",
    "uk",
    "ukraine",
    "us",
    "usa",
    "yemen",
}
_CAPTION_SOURCE_PREFIX_CONNECTORS = {
    "al",
    "and",
    "de",
    "el",
    "en",
    "la",
    "of",
    "the",
}
_MEDIA_CAPTION_MAX_CHARS = 980


def _plain_category_label(label: str) -> str:
    plain = normalize_space(strip_telegram_html(str(label or "")))
    plain = re.sub(r"[\U0001F300-\U0001FAFF\u2600-\u26FF\u2700-\u27BF\uFE0F]", " ", plain)
    plain = plain.strip("[](){}〔〕|:- ")
    return normalize_space(plain)


def _looks_like_caption_source_prefix(prefix: str) -> bool:
    cleaned = normalize_space(prefix).strip(" .")
    lowered = cleaned.lower()
    if not cleaned or lowered in _CAPTION_GEO_PREFIX_ALLOWLIST:
        return False
    if lowered.startswith(("why it matters", "what", "where", "when", "status", "location")):
        return False
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9&'._/-]*", cleaned)
    if not tokens or len(tokens) > 5:
        return False
    has_signal = False
    for token in tokens:
        bare = token.strip(".")
        lower = bare.lower()
        if lower in _CAPTION_SOURCE_PREFIX_CONNECTORS:
            continue
        if bare.isdigit():
            has_signal = True
            continue
        if bare.isupper() and len(bare) >= 2:
            has_signal = True
            continue
        if bare[:1].isupper():
            has_signal = True
            continue
        return False
    return has_signal


def _source_alias_variants(value: object) -> set[str]:
    raw = normalize_space(strip_telegram_html(str(value or "")))
    if not raw or raw.lstrip("-").isdigit():
        return set()

    variants = {raw}
    bare = raw.lstrip("@").strip()
    if bare:
        variants.add(bare)
        if re.fullmatch(r"[A-Za-z0-9_]{2,}", bare):
            variants.add(f"@{bare}")
    return {normalize_space(item) for item in variants if normalize_space(item)}


def _register_source_aliases(channel_id: str, *values: object) -> None:
    key = normalize_space(str(channel_id or ""))
    if not key:
        return
    aliases = source_alias_cache[key]
    for value in values:
        aliases.update(_source_alias_variants(value))


def _collect_monitored_source_aliases(source_title: str = "") -> list[str]:
    aliases: set[str] = set()
    for cached_title in source_title_cache.values():
        aliases.update(_source_alias_variants(cached_title))
    for cached_aliases in source_alias_cache.values():
        aliases.update({normalize_space(item) for item in cached_aliases if normalize_space(item)})
    aliases.update(_source_alias_variants(source_title))
    for entry in _manual_source_entries():
        aliases.update(_source_alias_variants(entry))
        username = _extract_public_username(entry)
        if username:
            aliases.update(_source_alias_variants(username))
    aliases.discard("")
    return sorted(aliases, key=len, reverse=True)


_KNOWN_SOURCE_ATTRIBUTION_VERB_RE = (
    r"(?:reported|reports|said|says|claim(?:ed|s)?|indicate(?:d|s)?|warn(?:ed|ing|s)?|"
    r"posted|posts|wrote|writes)"
)


def _capitalize_sentence_start(text: str) -> str:
    cleaned = normalize_space(text)
    if not cleaned:
        return ""
    if cleaned[:1].islower():
        return cleaned[:1].upper() + cleaned[1:]
    return cleaned


def _is_known_source_alias_line(text: str, *, source_title: str = "") -> bool:
    plain = normalize_space(strip_telegram_html(str(text or "")))
    if not plain:
        return False
    known = {
        normalize_space(strip_telegram_html(alias)).lower()
        for alias in _collect_monitored_source_aliases(source_title)
        if normalize_space(strip_telegram_html(alias))
    }
    return plain.lower() in known


def _strip_known_source_aliases(text: str, *, source_title: str = "") -> str:
    cleaned = str(text or "")
    for alias in _collect_monitored_source_aliases(source_title):
        bare = alias.lstrip("@")
        if not bare:
            continue
        if alias.startswith("@"):
            cleaned = re.sub(
                rf"(?<!\w){re.escape(alias)}(?!\w)",
                "",
                cleaned,
                flags=re.IGNORECASE,
            )
        cleaned = re.sub(
            rf"(?i)^\s*@?{re.escape(bare)}\s*$",
            "",
            cleaned,
        )
        cleaned = re.sub(
            rf"(?i)^\s*@?{re.escape(bare)}\s*(?:[:\-–—|]+\s*|,\s*)(?P<rest>.+)$",
            lambda match: _capitalize_sentence_start(match.group("rest")),
            cleaned,
        )
    return cleaned


def _rewrite_known_source_alias_attribution(text: str, *, source_title: str = "") -> str:
    cleaned = str(text or "")

    def _generic_reports_line(rest: str) -> str:
        candidate = normalize_space(strip_telegram_html(rest).strip(" ,;:-|"))
        candidate = re.sub(r"(?i)^that\s+", "", candidate)
        candidate = re.sub(r"(?i)^reportedly\b[:,]?\s*", "", candidate)
        candidate = normalize_space(candidate.strip(" ,;:-|"))
        if not candidate:
            return ""
        if re.match(
            r"(?i)^(?:initial|preliminary|early|unconfirmed)\s+reports?\s+(?:indicate|suggest|point to)\b",
            candidate,
        ):
            return _capitalize_sentence_start(candidate)
        if re.match(r"(?i)^reports?\s+(?:indicate|suggest|point to)\b", candidate):
            return _capitalize_sentence_start(candidate)
        return f"Reports indicate {candidate}"

    for alias in _collect_monitored_source_aliases(source_title):
        bare = alias.lstrip("@")
        if not bare:
            continue
        escaped = re.escape(bare)
        cleaned = re.sub(
            rf"(?i)^\s*(?:according to|via|from)\s+@?{escaped}\b[:,]?\s*(?P<rest>.+)$",
            lambda match: _generic_reports_line(match.group("rest")),
            cleaned,
        )
        cleaned = re.sub(
            rf"(?i)^\s*@?{escaped}\s+{_KNOWN_SOURCE_ATTRIBUTION_VERB_RE}\s+(?:that\s+)?(?P<rest>.+)$",
            lambda match: _generic_reports_line(match.group("rest")),
            cleaned,
        )
        cleaned = re.sub(
            rf"(?i)(?P<prefix>^|[.;:!?]\s*)according to\s+@?{escaped}\b[:,]?\s*",
            lambda match: match.group("prefix"),
            cleaned,
        )
    return cleaned


def _strip_caption_promo_noise(text: str, *, source_title: str = "") -> str:
    cleaned = str(text or "")
    for _ in range(4):
        previous = cleaned
        cleaned = _CAPTION_TELEGRAM_LINK_RE.sub("", cleaned)
        cleaned = _strip_known_source_aliases(cleaned, source_title=source_title)
        cleaned = _CAPTION_HANDLE_RE.sub("", cleaned)
        cleaned = _CAPTION_PROMO_TO_END_RE.sub("", cleaned)
        cleaned = _CAPTION_FOLLOW_PROMO_ONLY_RE.sub("", cleaned)
        cleaned = _CAPTION_SOURCE_CLAUSE_RE.sub("", cleaned)
        prefix_match = re.match(
            r"^(?P<prefix>[A-Za-z][A-Za-z0-9&'._ /-]{1,50})(?P<sep>\s*[:\-–—|]+\s*)(?P<rest>.+)$",
            cleaned,
        )
        if prefix_match and _looks_like_caption_source_prefix(prefix_match.group("prefix")):
            separator = normalize_space(prefix_match.group("sep"))
            rest = normalize_space(prefix_match.group("rest"))
            if not (separator == "-" and re.match(r"^[a-z][A-Za-z0-9-]*\b", rest)):
                cleaned = rest
        cleaned = re.sub(r"(?i)\b(?:via|from)\s*[:\-–—|]+\s*$", "", cleaned)
        cleaned = normalize_space(cleaned.strip(" ,;:-|/[](){}"))
        if cleaned == previous:
            break
    return cleaned


def _normalize_caption_fragment(
    text: str,
    *,
    source_title: str = "",
    category_label: str = "",
) -> str:
    cleaned = _strip_editorial_prefixes(
        text,
        source_title=source_title,
        category_label=category_label,
    )
    cleaned = cleaned.lstrip("/\\| ").strip()
    cleaned = _strip_caption_promo_noise(cleaned, source_title=source_title)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"[|•]+", " ", cleaned)
    cleaned = normalize_space(cleaned.strip(" ,;:-|/[](){}"))
    return cleaned


def _looks_like_incomplete_source_fragment(text: str) -> bool:
    cleaned = normalize_space(text)
    if not cleaned:
        return True

    match = re.match(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9&'._ -]{1,40})\s*:\s*(?P<rest>.+)$",
        cleaned,
    )
    if not match:
        return False

    prefix = normalize_space(match.group("prefix")).lower().strip(" .")
    rest = normalize_space(match.group("rest"))
    rest_tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9.'/-]*", rest)
    if not rest:
        return True
    if len(rest_tokens) <= 2 and prefix not in _CAPTION_GEO_PREFIX_ALLOWLIST:
        return True
    return False


def _caption_fragment_is_usable(text: str) -> bool:
    cleaned = normalize_space(text)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if cleaned.endswith((':', '/', '-', '|', '•')):
        return False
    if cleaned.startswith(("@", "/", "\\")):
        return False
    if any(fragment in lowered for fragment in _EDITORIAL_CONTEXT_BANNED_FRAGMENTS):
        return False
    if any(
        lowered.startswith(marker)
        for marker in (
            "our channel",
            "subscribe",
            "follow",
            "join",
            "watch here",
            "watch live",
            "livestream",
            "live stream",
            "thread",
        )
    ):
        return False
    if _looks_like_incomplete_source_fragment(cleaned):
        return False
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9.'/-]*", cleaned)
    if not words:
        return False
    if words[-1].strip(".,;:!?").lower() in _CAPTION_INCOMPLETE_TAIL_WORDS:
        return False
    return True


def _truncate_caption_fragment_complete(text: str, *, limit: int) -> str:
    cleaned = normalize_space(text)
    if len(cleaned) <= limit:
        return cleaned

    window = cleaned[:limit].rstrip()
    cut = max(
        window.rfind(". "),
        window.rfind("; "),
        window.rfind(", "),
        window.rfind(" - "),
        window.rfind(" — "),
    )
    if cut < int(limit * 0.55):
        cut = window.rfind(" ")
    if cut < int(limit * 0.55):
        cut = len(window)

    truncated = normalize_space(window[:cut].rstrip(" ,;:-/"))
    words = truncated.split()
    while words and words[-1].strip(".,;:!?").lower() in _CAPTION_INCOMPLETE_TAIL_WORDS:
        words.pop()
    return normalize_space(" ".join(words))


def _strip_editorial_prefixes(
    text: str,
    *,
    source_title: str = "",
    category_label: str = "",
) -> str:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    source_plain = normalize_space(strip_telegram_html(source_title))
    category_plain = _plain_category_label(category_label)
    for _ in range(4):
        previous = cleaned
        cleaned = _EDITORIAL_FLAG_PREFIX_RE.sub("", cleaned)
        cleaned = _EDITORIAL_EMOJI_PREFIX_RE.sub("", cleaned)
        cleaned = _EDITORIAL_PROMO_PREFIX_RE.sub("", cleaned)
        cleaned = re.sub(r"^\s*why it matters\s*[:\-–—]+\s*", "", cleaned, flags=re.IGNORECASE)
        if source_plain:
            cleaned = re.sub(
                rf"^\s*{re.escape(source_plain)}\s*[:\-–—|]+\s*",
                "",
                cleaned,
                flags=re.IGNORECASE,
            )
        if category_plain:
            cleaned = re.sub(
                rf"^\s*{re.escape(category_plain)}\s*[:\-–—|]+\s*",
                "",
                cleaned,
                flags=re.IGNORECASE,
            )
            cleaned = re.sub(
                rf"^\s*{re.escape(category_plain)}\s+",
                "",
                cleaned,
                flags=re.IGNORECASE,
            )
        cleaned = cleaned.strip(" -–—:|•")
        cleaned = normalize_space(cleaned)
        if cleaned == previous:
            break
    return cleaned


def _clean_editorial_headline(
    value: str,
    *,
    source_title: str,
    category_label: str,
    severity: str,
) -> str:
    cleaned = _normalize_caption_fragment(
        value,
        source_title=source_title,
        category_label=category_label,
    )
    cleaned = cleaned.rstrip(".")
    if cleaned.endswith("?"):
        cleaned = cleaned.rstrip("?").rstrip()
    if not _caption_fragment_is_usable(cleaned):
        return ""
    headline_limit = (
        _EDITORIAL_HEADLINE_MAX_BREAKING
        if normalize_space(severity).lower() == "high"
        else _EDITORIAL_HEADLINE_MAX_STANDARD
    )
    truncated = _truncate_caption_fragment_complete(cleaned, limit=headline_limit)
    return truncated if _caption_fragment_is_usable(truncated) else ""


def _clean_editorial_context(
    value: str,
    *,
    headline: str,
    source_title: str,
    category_label: str,
    severity: str,
) -> str:
    text = str(value or "").replace("\r", "\n")
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    lines: list[str] = []
    for raw_line in re.split(r"\n+", text):
        cleaned = _normalize_caption_fragment(
            raw_line,
            source_title=source_title,
            category_label=category_label,
        )
        if not _caption_fragment_is_usable(cleaned):
            continue
        if SequenceMatcher(None, cleaned.lower(), headline.lower()).ratio() >= 0.72:
            continue
        lines.append(cleaned)
    if not lines:
        return ""
    context = " ".join(lines)
    context = normalize_space(context).rstrip(".")
    if not context:
        return ""
    context_limit = (
        _EDITORIAL_CONTEXT_MAX_BREAKING
        if normalize_space(severity).lower() == "high"
        else _EDITORIAL_CONTEXT_MAX_STANDARD
    )
    truncated = _truncate_caption_fragment_complete(context, limit=context_limit)
    return truncated if _caption_fragment_is_usable(truncated) else ""


def _record_delivery_context_stat(key: str) -> None:
    delivery_context_stats[str(key or "").strip()] += 1


def _delivery_context_stats_snapshot() -> Dict[str, int]:
    keys = (
        "context_generated",
        "context_omitted_no_anchor",
        "context_omitted_no_delta",
        "context_rejected_generic",
    )
    return {key: int(delivery_context_stats.get(key, 0)) for key in keys}


def _resolve_story_candidate_taxonomy(text: str) -> str:
    match = match_news_category(text) or match_breaking_category(text)
    if match is None:
        return ""
    return normalize_space(str(getattr(match, "category_key", "") or ""))


def _build_story_context_candidate(
    *,
    text: str,
    headline: str = "",
    topic_key: str = "",
    taxonomy_key: str = "",
    source: str = "",
    timestamp: int | None = None,
) -> BreakingStoryCandidate:
    resolved_text = normalize_space(text or headline)
    resolved_headline = normalize_space(headline or resolved_text)
    resolved_taxonomy = normalize_space(taxonomy_key or "")
    if not resolved_taxonomy:
        resolved_taxonomy = _resolve_story_candidate_taxonomy("\n".join(part for part in (resolved_headline, resolved_text) if part))
    return build_breaking_story_candidate(
        text=resolved_text,
        headline=resolved_headline,
        topic_key=topic_key,
        source=source,
        timestamp=timestamp,
        taxonomy_key=resolved_taxonomy,
    )


def _best_archive_context_evidence(
    candidate: BreakingStoryCandidate,
    *,
    hours: int = 6,
    limit: int = 160,
) -> tuple[ContextEvidence | None, str]:
    now = int(time.time())
    since_ts = max(0, now - max(1, hours) * 3600)
    best: ContextEvidence | None = None
    best_score = 0.0
    best_ts = 0
    saw_related = False
    for row in load_archive_since(since_ts, limit):
        raw_text = normalize_space(str(row.get("raw_text") or ""))
        if not raw_text:
            continue
        ts = int(row.get("timestamp") or 0)
        if raw_text == candidate.text or (ts and abs(candidate.timestamp - ts) <= 1 and raw_text == candidate.headline):
            continue
        anchor = _build_story_context_candidate(
            text=raw_text,
            headline=raw_text,
            source=str(row.get("source_name") or ""),
            timestamp=ts or None,
        )
        evidence, score, reason = derive_context_evidence(
            candidate,
            anchor,
            age_label=_format_story_bridge_age(max(0, now - max(0, ts))),
        )
        if reason != "no_anchor":
            saw_related = True
        if evidence is None:
            continue
        if score > best_score or (abs(score - best_score) < 0.001 and ts > best_ts):
            best = evidence
            best_score = score
            best_ts = ts
    if best is not None:
        return best, "context_generated"
    return None, ("context_omitted_no_delta" if saw_related else "context_omitted_no_anchor")


def _best_cluster_context_evidence(
    candidate: BreakingStoryCandidate,
    cluster_id: str,
) -> tuple[ContextEvidence | None, str]:
    if not cluster_id:
        return None, "context_omitted_no_anchor"
    now = int(time.time())
    best: ContextEvidence | None = None
    best_score = 0.0
    best_ts = 0
    saw_related = False
    events = load_breaking_story_events(cluster_id=cluster_id, limit=8)
    for event in reversed(events):
        if str(event.get("delta_kind") or "") == "duplicate_echo":
            continue
        display_text = normalize_space(str(event.get("display_text") or event.get("normalized_text") or ""))
        if not display_text:
            continue
        ts = int(event.get("created_ts") or 0)
        facts = deserialize_breaking_story_facts(event.get("facts_json"))
        anchor = BreakingStoryCandidate(
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            headline=display_text,
            text=display_text,
            source="",
            timestamp=ts or candidate.timestamp,
            facts=facts,
        )
        evidence, score, reason = derive_context_evidence(
            candidate,
            anchor,
            age_label=_format_story_bridge_age(max(0, now - max(0, ts))),
        )
        if reason != "no_anchor":
            saw_related = True
        if evidence is None:
            continue
        if score > best_score or (abs(score - best_score) < 0.001 and ts > best_ts):
            best = evidence
            best_score = score
            best_ts = ts
    if best is not None:
        return best, "context_generated"
    return None, ("context_omitted_no_delta" if saw_related else "context_omitted_no_anchor")


async def _resolve_dynamic_delivery_context(
    *,
    current_text: str,
    headline: str,
    candidate_context: str = "",
    topic_key: str = "",
    taxonomy_key: str = "",
    source_title: str = "",
    cluster_id: str = "",
) -> str:
    candidate = _build_story_context_candidate(
        text=current_text,
        headline=headline,
        topic_key=topic_key,
        taxonomy_key=taxonomy_key,
        source=source_title,
    )
    if not candidate.text:
        return ""

    if cluster_id:
        evidence, miss_key = _best_cluster_context_evidence(candidate, cluster_id)
    else:
        evidence, miss_key = _best_archive_context_evidence(candidate)
    if evidence is None:
        _record_delivery_context_stat(miss_key)
        return ""

    cleaned_candidate = resolve_vital_rational_view_for_delivery(
        candidate.text,
        candidate_context,
        None,
        evidence=evidence,
    )
    if cleaned_candidate:
        _record_delivery_context_stat("context_generated")
        return cleaned_candidate

    if candidate_context:
        _record_delivery_context_stat("context_rejected_generic")
    if not auth_ready:
        return ""

    try:
        generated = await summarize_vital_rational_view(
            candidate.text,
            _require_auth_manager(),
            evidence=evidence,
        )
    except Exception:
        LOGGER.debug("Dynamic context generation failed.", exc_info=True)
        return ""
    if generated:
        _record_delivery_context_stat("context_generated")
        return generated
    return ""


def _render_editorial_post(
    *,
    category_label: str,
    headline: str,
    context: str = "",
) -> str:
    safe_category = sanitize_telegram_html(normalize_space(category_label))
    safe_headline = sanitize_telegram_html(normalize_space(headline))
    if not context:
        return f"<b>{safe_category}</b><br><br>{safe_headline}"
    safe_context = sanitize_telegram_html(normalize_space(context))
    return (
        f"<b>{safe_category}</b><br><br>{safe_headline}"
        f"<br><br>Why it matters: {safe_context}"
    )


def _format_summary_text(
    source_title: str,
    summary: str,
    *,
    raw_text: str | None = None,
    severity: str = "medium",
    context_override: str | None = None,
) -> str:
    raw_basis = normalize_space(str(raw_text or ""))
    safe_summary = str(summary or "").strip()
    normalized_summary = normalize_feed_summary_html(safe_summary, raw_basis) if raw_basis else safe_summary
    if _resolve_outbound_post_layout() == "legacy":
        if raw_basis and not normalized_summary and not should_downgrade_explainer_urgency(raw_basis):
            normalized_summary = sanitize_telegram_html(_truncate_context_line(raw_basis, limit=320))
        return _format_summary_text_legacy(source_title, normalized_summary)

    headline, context = extract_feed_summary_parts(normalized_summary or safe_summary, raw_basis or safe_summary)
    category_label = choose_alert_label(raw_basis or headline, severity=severity)
    clean_headline = _clean_editorial_headline(
        headline or raw_basis or safe_summary,
        source_title=source_title,
        category_label=category_label,
        severity=severity,
    )
    if not clean_headline:
        clean_headline = _clean_editorial_headline(
            raw_basis or safe_summary,
            source_title=source_title,
            category_label=category_label,
            severity=severity,
        )
    if not clean_headline:
        return ""
    clean_context = _clean_editorial_context(
        context_override if context_override is not None else context,
        headline=clean_headline,
        source_title=source_title,
        category_label=category_label,
        severity=severity,
    )
    return _render_editorial_post(
        category_label=category_label,
        headline=clean_headline,
        context=clean_context,
    )


def _clean_followup_context_line(text: str) -> str:
    cleaned = _normalize_caption_fragment(text)
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
    cleaned = _truncate_caption_fragment_complete(normalize_space(cleaned), limit=260)
    return cleaned if _caption_fragment_is_usable(cleaned) else ""


def _format_followup_media_caption(source_title: str, context_line: str) -> str | None:
    cleaned = _clean_followup_context_line(
        _strip_known_source_aliases(context_line, source_title=source_title)
    )
    if not cleaned:
        return None
    context_html = sanitize_telegram_html(cleaned)
    return f"Follow-up visuals: {context_html}"


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


def _format_breaking_text_legacy(source_title: str, headline: str, rational_view: str | None = None) -> str:
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

    category_label = choose_alert_label(headline, severity="high")
    clean_headline = _clean_editorial_headline(
        headline,
        source_title=source_title,
        category_label=category_label,
        severity="high",
    )
    if not clean_headline:
        return ""
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

    rational_body = ""
    rational_is_structured = False
    if rational_view:
        lowered = str(rational_view or "").lower()
        rational_is_structured = (
            "<b>what:" in lowered
            or "•" in lowered
            or "<br>" in lowered
            or "\n" in str(rational_view or "")
        )
        if rational_is_structured:
            rational_body = _normalize_structured_block(str(rational_view))
        else:
            cleaned_rational = _clean_editorial_context(
                _clean_rational_view(rational_view),
                headline=clean_headline,
                source_title=source_title,
                category_label=category_label,
                severity="high",
            )
            if cleaned_rational:
                rational_body = sanitize_telegram_html(cleaned_rational)
    header = build_alert_header(
        clean_headline,
        severity="high",
        source_title=source_title,
        include_source=_include_source_tags(),
    )
    if resolve_breaking_style_mode() == "unhinged":
        if not rational_body:
            return f"{header} {safe_headline}".strip()
        return f"{header} {safe_headline}<br>{rational_body}".strip()
    rational_part = ""
    if rational_body:
        if rational_is_structured:
            rational_part = f"<br><br>{rational_body}"
        else:
            plain_rational = rational_body.rstrip()
            if plain_rational and plain_rational[-1] not in ".!?":
                plain_rational = f"{plain_rational}."
            rational_part = f"<br><br>Why it matters: {plain_rational}"
    return f"{header}<br><br>{safe_headline}{rational_part}"


def _format_breaking_text(source_title: str, headline: str, rational_view: str | None = None) -> str:
    if _resolve_outbound_post_layout() == "legacy":
        return _format_breaking_text_legacy(source_title, headline, rational_view)

    category_label = choose_alert_label(headline, severity="high")
    clean_headline = _clean_editorial_headline(
        headline,
        source_title=source_title,
        category_label=category_label,
        severity="high",
    )
    if not clean_headline:
        return ""
    clean_context = _clean_editorial_context(
        rational_view or "",
        headline=clean_headline,
        source_title=source_title,
        category_label=category_label,
        severity="high",
    )
    return _render_editorial_post(
        category_label=category_label,
        headline=clean_headline,
        context=clean_context,
    )


def _cleanup_breaking_refs(now_ts: int | None = None) -> None:
    now = int(now_ts if now_ts is not None else time.time())
    cutoff = now - 1800
    stale = [key for key, value in breaking_delivery_refs.items() if int(value.get("ts", 0)) < cutoff]
    for key in stale:
        breaking_delivery_refs.pop(key, None)


def _compact_sent_ref(ref: object) -> object | None:
    serialized = _serialize_sent_ref(ref)
    if serialized:
        compact = _deserialize_sent_ref(serialized)
        if compact is not None:
            return compact
    return _primary_message_ref(ref)


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
        "ref": _compact_sent_ref(ref),
        "base_text": base_text.strip(),
        "primary_source": primary_source.strip(),
        "sources": {primary_source.strip()},
        "ts": now,
    }


def _breaking_story_history_cutoff_ts(now_ts: int | None = None) -> int:
    now = int(now_ts if now_ts is not None else time.time())
    return now - max(_breaking_story_window_seconds() * 4, 24 * 60 * 60)


def _load_active_breaking_story_clusters_snapshot(now_ts: int | None = None) -> List[Dict[str, object]]:
    if not _is_breaking_story_clusters_enabled():
        return []
    now = int(now_ts if now_ts is not None else time.time())
    with contextlib.suppress(Exception):
        purge_breaking_story_history(older_than_ts=_breaking_story_history_cutoff_ts(now))
    with contextlib.suppress(Exception):
        return load_active_breaking_story_clusters(
            since_ts=now - _breaking_story_window_seconds(),
            limit=250,
        )
    return []


async def _resolve_breaking_story_root_ref(cluster: Dict[str, object]) -> object | None:
    cluster_id = str(cluster.get("cluster_id") or "")
    root_message_id = int(cluster.get("root_message_id") or 0)
    if not cluster_id or root_message_id <= 0:
        return None

    cached = breaking_story_ref_cache.get(cluster_id)
    if _message_ref_id(cached) == root_message_id:
        return cached

    stored_ref = _deserialize_sent_ref(str(cluster.get("root_sent_ref") or ""))
    has_media = bool(isinstance(stored_ref, dict) and stored_ref.get("has_media"))
    ref = await _load_destination_message_ref(root_message_id, has_media=has_media)
    if ref is not None:
        breaking_story_ref_cache[cluster_id] = _compact_sent_ref(ref)
    return ref


async def _cluster_story_bridge(
    candidate_text: str,
    headline: str,
    cluster_id: str,
    *,
    topic_key: str = "",
    taxonomy_key: str = "",
    source_title: str = "",
    candidate_context: str = "",
) -> str:
    return await _resolve_dynamic_delivery_context(
        current_text=candidate_text,
        headline=headline,
        candidate_context=candidate_context,
        topic_key=topic_key,
        taxonomy_key=taxonomy_key,
        source_title=source_title,
        cluster_id=cluster_id,
    )


def _persist_breaking_story_cluster(
    *,
    cluster_id: str,
    cluster_key: str,
    topic_key: str,
    taxonomy_key: str,
    root_message_id: int,
    root_ref: object | None,
    current_headline: str,
    candidate: BreakingStoryCandidate,
    opened_ts: int,
    updated_ts: int,
    last_delivery_ts: int,
    update_count: int,
    stored_root_sent_ref: str = "",
    current_facts_json_override: str | None = None,
) -> None:
    serialized_ref = _serialize_sent_ref(root_ref) or str(stored_root_sent_ref or "")
    save_breaking_story_cluster(
        cluster_id=cluster_id,
        cluster_key=cluster_key,
        topic_key=topic_key,
        taxonomy_key=taxonomy_key,
        root_message_id=root_message_id,
        root_sent_ref=serialized_ref,
        current_headline=current_headline,
        current_facts_json=(
            str(current_facts_json_override)
            if current_facts_json_override is not None
            else serialize_breaking_story_facts(candidate.facts)
        ),
        opened_ts=opened_ts,
        updated_ts=updated_ts,
        last_delivery_ts=last_delivery_ts,
        update_count=update_count,
        status="active",
    )
    if root_ref is not None and cluster_id:
        breaking_story_ref_cache[cluster_id] = _compact_sent_ref(root_ref)


async def _apply_breaking_story_cluster_policy(
    *,
    messages: Sequence[Message],
    source: str,
    candidate_text: str,
    headline: str,
    topic_key: str = "",
    story_bridge: str | None = None,
) -> Dict[str, object]:
    primary = messages[0]
    message_ids = [int(item.id or 0) for item in messages if int(item.id or 0) > 0]
    channel_id = str(primary.chat_id)
    now = int(time.time())
    candidate = build_breaking_story_candidate(
        text=candidate_text,
        headline=headline,
        topic_key=topic_key,
        source=source,
        timestamp=now,
    )

    resolution = resolve_breaking_story_cluster(
        candidate,
        _load_active_breaking_story_clusters_snapshot(now),
        now_ts=now,
        burst_seconds=_breaking_story_burst_seconds(),
    ) if _is_breaking_story_clusters_enabled() else None

    if resolution is not None and resolution.mismatch_reason:
        log_structured(
            LOGGER,
            "story_cluster_mismatch",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            reason=resolution.mismatch_reason,
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
        )

    cluster = dict(resolution.cluster) if resolution and resolution.cluster else None
    decision = resolution.decision if resolution is not None else "new_story"
    effective_bridge = story_bridge or ""
    if decision == "material_update" and cluster is not None:
        effective_bridge = await _cluster_story_bridge(
            candidate_text,
            headline,
            str(cluster.get("cluster_id") or ""),
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            source_title=source,
            candidate_context=effective_bridge,
        )
    elif decision == "new_story" or cluster is None:
        effective_bridge = await _resolve_dynamic_delivery_context(
            current_text=candidate_text,
            headline=headline,
            candidate_context=effective_bridge,
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            source_title=source,
        )
    elif decision in {"duplicate_echo", "minor_refinement"}:
        effective_bridge = ""

    payload_text = _format_breaking_text(source, headline, effective_bridge or None)
    if not payload_text:
        return {
            "final_action": "skip",
            "delivery_message_id": 0,
            "reply_to": 0,
            "cluster_id": "",
            "cluster_decision": "caption_redacted_empty",
            "sent_ref": None,
            "payload_text": "",
        }
    topic_seed = f"{headline}\n{candidate_text}"

    if decision == "new_story" or cluster is None:
        reply_to = await _resolve_source_reply_target(primary, fallback_topic_seed=topic_seed)
        if any(bool(item.media) for item in messages):
            sent_ref = (
                await _send_album(list(messages), payload_text, reply_to=reply_to)
                if len(messages) > 1
                else await _send_single_media(primary, payload_text, reply_to=reply_to)
            )
        else:
            sent_ref = await _send_text_with_ref(payload_text, reply_to=reply_to)
        root_ref = _primary_message_ref(sent_ref)
        root_message_id = int(_message_ref_id(root_ref) or 0)
        cluster_id = uuid.uuid4().hex
        _persist_breaking_story_cluster(
            cluster_id=cluster_id,
            cluster_key=compute_breaking_story_cluster_key(candidate),
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            root_message_id=root_message_id,
            root_ref=root_ref,
            current_headline=headline,
            candidate=candidate,
            opened_ts=now,
            updated_ts=now,
            last_delivery_ts=now,
            update_count=0,
        )
        save_breaking_story_event(
            cluster_id=cluster_id,
            source_channel_id=channel_id,
            source_message_id=int(primary.id or 0),
            delivery_message_id=root_message_id,
            normalized_text=candidate.facts.normalized_text,
            display_text=headline,
            facts_json=serialize_breaking_story_facts(candidate.facts),
            delta_kind="new_story",
            created_ts=now,
        )
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=root_ref,
        )
        _register_breaking_delivery(
            text_hash=candidate.facts.text_hash,
            ref=root_ref,
            base_text=payload_text,
            primary_source=source,
        )
        await _register_breaking_topic_thread(topic_seed=topic_seed, sent_ref=root_ref)
        log_structured(
            LOGGER,
            "story_cluster_created",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            cluster_id=cluster_id,
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            root_message_id=root_message_id,
        )
        return {
            "final_action": "breaking_delivery",
            "delivery_message_id": root_message_id,
            "reply_to": int(reply_to or 0),
            "cluster_id": cluster_id,
            "cluster_decision": "new_story",
            "sent_ref": sent_ref,
            "payload_text": payload_text,
        }

    cluster_id = str(cluster.get("cluster_id") or "")
    root_message_id = int(cluster.get("root_message_id") or 0)
    cluster_key = str(cluster.get("cluster_key") or "")
    opened_ts = int(cluster.get("opened_ts") or now)
    update_count = int(cluster.get("update_count") or 0)
    root_ref = await _resolve_breaking_story_root_ref(cluster)

    if decision == "minor_refinement" and bool(resolution and resolution.should_edit_root) and root_ref is not None:
        updated_ref = await _edit_sent_text(root_ref, payload_text)
        root_message_id = int(_message_ref_id(updated_ref) or root_message_id)
        _persist_breaking_story_cluster(
            cluster_id=cluster_id,
            cluster_key=cluster_key,
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            root_message_id=root_message_id,
            root_ref=updated_ref,
            current_headline=headline,
            candidate=candidate,
            opened_ts=opened_ts,
            updated_ts=now,
            last_delivery_ts=now,
            update_count=update_count,
            stored_root_sent_ref=str(cluster.get("root_sent_ref") or ""),
        )
        save_breaking_story_event(
            cluster_id=cluster_id,
            source_channel_id=channel_id,
            source_message_id=int(primary.id or 0),
            delivery_message_id=root_message_id,
            normalized_text=candidate.facts.normalized_text,
            display_text=headline,
            facts_json=serialize_breaking_story_facts(candidate.facts),
            delta_kind="minor_refinement",
            created_ts=now,
        )
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=updated_ref,
        )
        _register_breaking_delivery(
            text_hash=candidate.facts.text_hash,
            ref=updated_ref,
            base_text=payload_text,
            primary_source=source,
        )
        log_structured(
            LOGGER,
            "story_cluster_edited",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            cluster_id=cluster_id,
            root_message_id=root_message_id,
        )
        return {
            "final_action": "breaking_root_edited",
            "delivery_message_id": root_message_id,
            "reply_to": root_message_id,
            "cluster_id": cluster_id,
            "cluster_decision": "minor_refinement",
            "sent_ref": updated_ref,
            "payload_text": payload_text,
        }

    if decision == "material_update":
        reply_to = root_message_id or None
        if any(bool(item.media) for item in messages):
            sent_ref = (
                await _send_album(list(messages), payload_text, reply_to=reply_to)
                if len(messages) > 1
                else await _send_single_media(primary, payload_text, reply_to=reply_to)
            )
        else:
            sent_ref = await _send_text_with_ref(payload_text, reply_to=reply_to)
        sent_message_id = int(_message_ref_id(sent_ref) or 0)
        _persist_breaking_story_cluster(
            cluster_id=cluster_id,
            cluster_key=cluster_key,
            topic_key=candidate.topic_key,
            taxonomy_key=candidate.taxonomy_key,
            root_message_id=root_message_id,
            root_ref=root_ref,
            current_headline=headline,
            candidate=candidate,
            opened_ts=opened_ts,
            updated_ts=now,
            last_delivery_ts=now,
            update_count=update_count + 1,
            stored_root_sent_ref=str(cluster.get("root_sent_ref") or ""),
        )
        save_breaking_story_event(
            cluster_id=cluster_id,
            source_channel_id=channel_id,
            source_message_id=int(primary.id or 0),
            delivery_message_id=sent_message_id,
            normalized_text=candidate.facts.normalized_text,
            display_text=headline,
            facts_json=serialize_breaking_story_facts(candidate.facts),
            delta_kind="material_update",
            created_ts=now,
        )
        _register_source_delivery_refs(
            channel_id=channel_id,
            source_message_ids=message_ids,
            sent_ref=sent_ref,
        )
        _register_breaking_delivery(
            text_hash=candidate.facts.text_hash,
            ref=sent_ref,
            base_text=payload_text,
            primary_source=source,
        )
        await _register_breaking_topic_thread(topic_seed=topic_seed, sent_ref=sent_ref)
        log_structured(
            LOGGER,
            "story_cluster_threaded",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            cluster_id=cluster_id,
            root_message_id=root_message_id,
            delivery_message_id=sent_message_id,
        )
        return {
            "final_action": "breaking_delivery_threaded",
            "delivery_message_id": sent_message_id,
            "reply_to": root_message_id,
            "cluster_id": cluster_id,
            "cluster_decision": "material_update",
            "sent_ref": sent_ref,
            "payload_text": payload_text,
        }

    _persist_breaking_story_cluster(
        cluster_id=cluster_id,
        cluster_key=cluster_key,
        topic_key=str(cluster.get("topic_key") or candidate.topic_key),
        taxonomy_key=str(cluster.get("taxonomy_key") or candidate.taxonomy_key),
        root_message_id=root_message_id,
        root_ref=root_ref,
        current_headline=str(cluster.get("current_headline") or headline),
        candidate=candidate,
        opened_ts=opened_ts,
        updated_ts=now,
        last_delivery_ts=int(cluster.get("last_delivery_ts") or opened_ts),
        update_count=update_count,
        stored_root_sent_ref=str(cluster.get("root_sent_ref") or ""),
        current_facts_json_override=str(cluster.get("current_facts_json") or "{}"),
    )
    save_breaking_story_event(
        cluster_id=cluster_id,
        source_channel_id=channel_id,
        source_message_id=int(primary.id or 0),
        delivery_message_id=root_message_id,
        normalized_text=candidate.facts.normalized_text,
        display_text=headline,
        facts_json=serialize_breaking_story_facts(candidate.facts),
        delta_kind="duplicate_echo",
        created_ts=now,
    )
    _register_source_delivery_refs(
        channel_id=channel_id,
        source_message_ids=message_ids,
        sent_ref={"message_id": root_message_id},
    )
    log_structured(
        LOGGER,
        "story_cluster_suppressed",
        channel_id=channel_id,
        message_id=int(primary.id or 0),
        cluster_id=cluster_id,
        root_message_id=root_message_id,
        decision=decision,
    )
    return {
        "final_action": "breaking_suppressed",
        "delivery_message_id": root_message_id,
        "reply_to": root_message_id,
        "cluster_id": cluster_id,
        "cluster_decision": decision,
        "sent_ref": None,
        "payload_text": payload_text,
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


def _format_story_bridge_age(age_seconds: int) -> str:
    seconds = max(0, int(age_seconds))
    if seconds < 120:
        return "moments ago"
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        return f"{minutes}m ago"
    hours = max(1, seconds // 3600)
    return f"{hours}h ago"


def _compact_story_bridge_text(text: str, *, max_words: int = 18) -> str:
    clean = normalize_space(strip_telegram_html(text))
    if not clean:
        return ""
    clean = re.sub(r"\s+", " ", clean).strip()
    sentence_match = re.search(r"(.{18,260}?[.!?])(?:\s|$)", clean)
    if sentence_match:
        clean = normalize_space(sentence_match.group(1))
    words = clean.split()
    if len(words) > max_words:
        clean = " ".join(words[:max_words]).rstrip(".,;:!?") + "..."
    return clean


def _recent_story_bridge_context(text: str, *, hours: int = 6, limit: int = 3) -> List[str]:
    now = int(time.time())
    since_ts = max(0, now - max(1, hours) * 3600)
    rows = load_archive_since(since_ts, 160)
    current_tokens = _breaking_topic_tokens(text)
    current_norm = _breaking_topic_normalized(text)
    if not current_tokens or not current_norm:
        return []

    ranked: list[tuple[float, int, str]] = []
    seen_norms: set[str] = set()
    for row in rows:
        raw_text = str(row.get("raw_text") or "").strip()
        if not raw_text:
            continue
        candidate_norm = _breaking_topic_normalized(raw_text)
        if not candidate_norm or candidate_norm == current_norm or candidate_norm in seen_norms:
            continue
        candidate_tokens = _breaking_topic_tokens(raw_text)
        overlap, ratio = _breaking_topic_similarity(current_tokens, candidate_tokens)
        fuzzy = _breaking_topic_fuzzy_similarity(current_norm, candidate_norm)
        if overlap < 2 and ratio < 0.34 and fuzzy < 0.55:
            continue
        ts = int(row.get("timestamp") or 0)
        age = max(0, now - ts)
        recency_bonus = max(0.0, 1.0 - min(age, hours * 3600) / float(hours * 3600 or 1)) * 0.2
        score = overlap * 0.18 + ratio * 0.42 + fuzzy * 0.25 + recency_bonus
        compact = _compact_story_bridge_text(raw_text)
        if not compact:
            continue
        ranked.append((score, ts, f"{_format_story_bridge_age(age)}: {compact}"))
        seen_norms.add(candidate_norm)

    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in ranked[: max(1, limit)]]


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
    entry["ref"] = _compact_sent_ref(updated_ref)
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
            ensure_backends=not _dupe_use_sentence_transformers(),
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


async def _maybe_start_duplicate_backend_warmup() -> None:
    global pipeline_sentence_transformer_warm_task
    if pipeline_sentence_transformer_warm_task and not pipeline_sentence_transformer_warm_task.done():
        return
    if dupe_detector is None or not _dupe_use_sentence_transformers():
        return

    async def _warm_duplicate_backend() -> None:
        try:
            await asyncio.to_thread(dupe_detector.warm_backends)
            LOGGER.info("Sentence-transformer duplicate backend warmed in background.")
        except Exception:
            LOGGER.exception("Background sentence-transformer warmup failed.")

    pipeline_sentence_transformer_warm_task = asyncio.create_task(
        _warm_duplicate_backend(),
        name="dupe-backend-warmup",
    )


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


def _primary_message_ref(ref: object) -> object | None:
    if isinstance(ref, (list, tuple)):
        for item in ref:
            if item is not None:
                return item
        return None
    return ref


def _message_ref_has_media(ref: object) -> bool:
    target = _primary_message_ref(ref)
    if isinstance(target, Message):
        return bool(target.media)
    if isinstance(target, dict):
        return any(
            key in target
            for key in (
                "animation",
                "audio",
                "document",
                "photo",
                "sticker",
                "video",
                "video_note",
                "voice",
            )
        )
    return False


def _serialize_sent_ref(ref: object) -> str:
    target = _primary_message_ref(ref)
    message_id = _message_ref_id(target)
    if not message_id:
        return ""
    payload = {
        "message_id": int(message_id),
        "has_media": bool(_message_ref_has_media(target)),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _deserialize_sent_ref(value: str) -> object | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return None
    message_id = int(payload.get("message_id") or 0)
    if message_id <= 0:
        return None
    return {
        "message_id": message_id,
        "has_media": bool(payload.get("has_media")),
    }


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
        target_ref = _primary_message_ref(ref)
        message_id = _message_ref_id(target_ref)
        if not message_id:
            raise RuntimeError("Cannot edit Bot API message without message_id.")
        is_media_ref = _message_ref_has_media(target_ref)

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "message_id": str(message_id),
            "disable_web_page_preview": "true",
            "parse_mode": "HTML" if _is_html_formatting_enabled() else "Markdown",
        }
        method = "editMessageText"
        if is_media_ref:
            data["caption"] = _to_bot_text(payload_text) or ""
            method = "editMessageCaption"
            data.pop("disable_web_page_preview", None)
        else:
            data["text"] = _to_bot_text(payload_text) or ""
        try:
            result = await _bot_api_request(method, data=data)
            # Bot API can return `True` for some cases; keep original ref for continuity.
            if isinstance(result, dict):
                return result
            return ref
        except Exception:
            data.pop("parse_mode", None)
            fallback_text = (
                strip_telegram_html(payload_text) if _is_html_formatting_enabled() else payload_text
            )
            if is_media_ref:
                data["caption"] = _to_plain_text(fallback_text) or ""
            else:
                data["text"] = _to_plain_text(fallback_text) or ""
            result = await _bot_api_request(method, data=data)
            if isinstance(result, dict):
                return result
            return ref

    target_ref = _primary_message_ref(ref)
    if not isinstance(target_ref, Message):
        message_id = _message_ref_id(target_ref)
        if not message_id:
            raise RuntimeError("Telethon edit requires a message reference or message_id.")
        fetched_ref = await _load_destination_message_ref(
            message_id,
            has_media=_message_ref_has_media(target_ref),
        )
        fetched_target = _primary_message_ref(fetched_ref)
        if not isinstance(fetched_target, Message):
            raise RuntimeError(f"Failed to reload destination message {message_id} for edit.")
        target_ref = fetched_target
    payload_text = (
        _render_outbound_text(text, allow_premium_tags=False)
        if _is_html_formatting_enabled()
        else text
    )
    try:
        return await target_ref.edit(
            payload_text,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            link_preview=False,
        )
    except Exception:
        fallback_text = (
            strip_telegram_html(payload_text) if _is_html_formatting_enabled() else payload_text
        )
        return await target_ref.edit(fallback_text, parse_mode=None, link_preview=False)


async def _load_destination_message_ref(message_id: int, *, has_media: bool = False) -> object | None:
    resolved_id = int(message_id or 0)
    if resolved_id <= 0:
        return None
    if _destination_uses_bot_api():
        return {"message_id": resolved_id, "has_media": bool(has_media)}

    tg = _require_client()
    fetched = await _call_with_floodwait(
        tg.get_messages,
        _require_destination_peer(),
        ids=resolved_id,
    )
    if isinstance(fetched, Message):
        return fetched
    if isinstance(fetched, list):
        for item in fetched:
            if isinstance(item, Message):
                return item
    return None


async def _send_single_media(
    msg: Message,
    caption: str | None,
    *,
    reply_to: int | None = None,
) -> object:
    if _destination_uses_bot_api():
        safe_caption, overflow_chunks = _prepare_media_caption_chunks(
            caption,
            allow_premium_tags=True,
        )

        file_size = getattr(msg.file, "size", 0) if msg.file else 0
        # Bot API limit is 50MB. We use 48MB as a safe margin.
        if file_size > 48 * 1024 * 1024:
            LOGGER.warning(
                "Media too large for Bot API (%s MiB). Falling back to text-only.",
                int(file_size / (1024 * 1024)),
            )
            return await _send_text_with_ref(caption)

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
            sent_ref = await _bot_api_request(method_map[media_type], data=data, files=files)
        except Exception as exc:
            if "413" in str(exc) or "Request Entity Too Large" in str(exc):
                LOGGER.warning("Bot API rejected large media (413). Falling back to text-only.")
                return await _send_text_with_ref(caption)

            if reply_to and _is_reply_target_missing_error(exc):
                LOGGER.debug("Reply target missing for media send; retrying without reply_to.")
                retry_data = dict(data)
                retry_data.pop("reply_to_message_id", None)
                sent_ref = await _bot_api_request(method_map[media_type], data=retry_data, files=files)
                await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
                return sent_ref
            data.pop("parse_mode", None)
            if safe_caption:
                fallback_caption = (
                    strip_telegram_html(safe_caption)
                    if _is_html_formatting_enabled()
                    else safe_caption
                )
                data["caption"] = _to_plain_text(fallback_caption, max_len=1024) or ""
            sent_ref = await _bot_api_request(method_map[media_type], data=data, files=files)
            await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
            return sent_ref

        await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
        return sent_ref

    tg = _require_client()
    safe_caption, overflow_chunks = _prepare_media_caption_chunks(
        caption,
        allow_premium_tags=False,
    )
    try:
        sent_ref = await _call_with_floodwait(
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
        sent_ref = await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            raw,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )
    await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
    return sent_ref


async def _send_album(
    messages: List[Message],
    caption: str | None,
    *,
    reply_to: int | None = None,
) -> object | None:
    if _destination_uses_bot_api():
        if len(messages) == 1:
            return await _send_single_media(messages[0], caption, reply_to=reply_to)
        safe_caption, overflow_chunks = _prepare_media_caption_chunks(
            caption,
            allow_premium_tags=True,
        )
        if not messages:
            if safe_caption:
                sent_ref = await _send_text_with_ref(safe_caption, reply_to=reply_to)
                await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
                return sent_ref
            return

        media_entries: List[dict] = []
        files: Dict[str, tuple[str, bytes, str]] = {}
        downloaded_messages: List[Message] = []
        total_size = 0
        
        async with (pipeline_media_semaphore or contextlib.nullcontext()):
            for idx, msg in enumerate(messages):
                msg_size = getattr(msg.file, "size", 0) if msg.file else 0
                if total_size + msg_size > 48 * 1024 * 1024:
                    LOGGER.warning("Album total size exceeds Bot API limit. Skipping further files.")
                    break

                raw = await msg.download_media(file=bytes)
                if raw is None:
                    continue

                total_size += len(raw)
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
            LOGGER.warning("No media could be downloaded for album. Falling back to text-only.")
            return await _send_text_with_ref(caption)

        if len(media_entries) == 1:
            only_msg = downloaded_messages[0]
            return await _send_single_media(only_msg, caption, reply_to=reply_to)

        data = {
            "chat_id": _require_bot_destination_chat_id(),
            "media": json.dumps(media_entries),
        }
        if reply_to:
            data["reply_to_message_id"] = str(reply_to)
        try:
            sent_ref = await _bot_api_request("sendMediaGroup", data=data, files=files)
        except Exception as exc:
            if "413" in str(exc) or "Request Entity Too Large" in str(exc):
                LOGGER.warning("Bot API rejected media group (413). Falling back to text-only.")
                return await _send_text_with_ref(caption)

            if reply_to and _is_reply_target_missing_error(exc):
                LOGGER.debug("Reply target missing for media group send; retrying without reply_to.")
                retry_data = dict(data)
                retry_data.pop("reply_to_message_id", None)
                sent_ref = await _bot_api_request("sendMediaGroup", data=retry_data, files=files)
                await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
                return sent_ref
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
            sent_ref = await _bot_api_request("sendMediaGroup", data=data, files=files)
            await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
            return sent_ref
        await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
        return sent_ref

    tg = _require_client()
    safe_caption, overflow_chunks = _prepare_media_caption_chunks(
        caption,
        allow_premium_tags=False,
    )
    media_items = [m.media for m in messages if m.media]
    if not media_items:
        if safe_caption:
            sent_ref = await _send_text_with_ref(safe_caption, reply_to=reply_to)
            await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
            return sent_ref
        return

    try:
        sent_ref = await _call_with_floodwait(
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
        sent_ref = await _call_with_floodwait(
            tg.send_file,
            _require_destination_peer(),
            downloaded,
            caption=safe_caption,
            parse_mode="html" if _is_html_formatting_enabled() else "md",
            reply_to=reply_to,
        )
    await _send_media_caption_overflow(overflow_chunks, sent_ref=sent_ref)
    return sent_ref


async def _process_single_message(msg: Message) -> None:
    channel_id = str(msg.chat_id)
    if is_seen(channel_id, msg.id):
        return

    text = (msg.message or "").strip()
    source, link = await _source_info(msg)

    if msg.media and not text:
        reply_message = await _load_reply_message(msg)
        media_caption = await _caption_for_media_without_text(msg)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        archive_text = normalize_space(strip_telegram_html(media_caption) if media_caption else reply_context_text)
        if archive_text:
            _queue_for_digest(
                channel_id,
                msg.id,
                archive_text,
                source_name=source,
                message_link=link,
            )
            _archive_for_query_search(
                channel_id,
                msg.id,
                archive_text,
                source_name=source,
                message_link=link,
            )
            log_structured(
                LOGGER,
                "digest_media_queued",
                channel_id=channel_id,
                message_id=msg.id,
                source=source,
                pending=count_pending(),
                has_media=True,
            )
        mark_seen(channel_id, msg.id)
        return

    if msg.media and text:
        summary = await summarize_or_skip(text, _require_auth_manager())
        if summary is None:
            mark_seen(channel_id, msg.id)
            return
        reply_to = await _resolve_source_reply_target(msg)
        summary_headline, summary_context = extract_feed_summary_parts(summary, text)
        resolved_context = await _resolve_dynamic_delivery_context(
            current_text=text,
            headline=summary_headline,
            candidate_context=summary_context,
            source_title=source,
        )
        sent_ref = await _send_single_media(
            msg,
            _format_summary_text(source, summary, raw_text=text, context_override=resolved_context),
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
    summary_headline, summary_context = extract_feed_summary_parts(summary, text)
    resolved_context = await _resolve_dynamic_delivery_context(
        current_text=text,
        headline=summary_headline,
        candidate_context=summary_context,
        source_title=source,
    )
    sent_ref = await _send_text_with_ref(
        _format_summary_text(source, summary, raw_text=text, context_override=resolved_context),
        reply_to=reply_to,
    )
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
        media_caption = await _caption_for_album_without_text(messages)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        archive_text = normalize_space(strip_telegram_html(media_caption) if media_caption else reply_context_text)
        if archive_text:
            _queue_for_digest(
                channel_id,
                messages[0].id,
                archive_text,
                source_name=source,
                message_link=link,
            )
            _archive_for_query_search(
                channel_id,
                messages[0].id,
                archive_text,
                source_name=source,
                message_link=link,
            )
            log_structured(
                LOGGER,
                "digest_album_media_queued",
                channel_id=channel_id,
                first_message_id=messages[0].id,
                album_size=len(messages),
                source=source,
                pending=count_pending(),
                has_media=True,
            )
        mark_seen_many(channel_id, message_ids)
        return

    summary = await summarize_or_skip(combined_caption, _require_auth_manager())
    if summary is None:
        mark_seen_many(channel_id, message_ids)
        return

    reply_to = await _resolve_source_reply_target(messages[0])
    summary_headline, summary_context = extract_feed_summary_parts(summary, combined_caption)
    resolved_context = await _resolve_dynamic_delivery_context(
        current_text=combined_caption,
        headline=summary_headline,
        candidate_context=summary_context,
        source_title=source,
    )
    sent_ref = await _send_album(
        messages,
        _format_summary_text(
            source,
            summary,
            raw_text=combined_caption,
            context_override=resolved_context,
        ),
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
        media_caption = await _caption_for_media_without_text(msg)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        digest_text = normalize_space(
            _plain_text_from_html_fragment(media_caption) or reply_context_text or ""
        )
        if not digest_text:
            mark_seen(channel_id, msg.id)
            return
        severity = "medium"
        severity_score = 0.0
        severity_breakdown: dict = {}
        if _is_severity_routing_enabled():
            severity, severity_score, severity_breakdown = _classify_severity_with_breakdown(
                text=digest_text,
                source=source,
                channel_id=channel_id,
                message_id=msg.id,
                has_media=True,
                has_link=bool(link),
                reply_to=_reply_to_message_id(msg),
            )
        elif _contains_breaking_keyword(digest_text):
            severity = "high"
        _queue_for_digest(
            channel_id,
            msg.id,
            digest_text,
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
            has_media=True,
            text_tokens=estimate_tokens_rough(digest_text),
            pending=count_pending(),
            severity_breakdown=severity_breakdown,
        )
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

    if (
        severity != "high"
        and _contains_breaking_keyword(text)
        and not should_downgrade_explainer_urgency(text)
        and looks_like_live_event_update(text)
    ):
        severity = "high"
        severity_score = max(float(severity_score), _high_severity_score_floor())
        severity_breakdown = dict(severity_breakdown or {})
        severity_breakdown["keyword_override"] = True

    if severity == "high" and _is_immediate_high_enabled():
        _normalized, text_hash = build_dupe_fingerprint(text)
        headline = await summarize_breaking_headline(text, _require_auth_manager())
        if not headline:
            mark_seen(channel_id, msg.id)
            return
        cluster_result = await _apply_breaking_story_cluster_policy(
            messages=[msg],
            source=source,
            candidate_text=text,
            headline=headline,
        )
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
            dedupe_hash=text_hash,
            final_action=str(cluster_result.get("final_action") or ""),
            cluster_decision=str(cluster_result.get("cluster_decision") or ""),
            reply_to=int(cluster_result.get("reply_to") or 0),
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
        media_caption = await _caption_for_album_without_text(messages)
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption
        digest_text = normalize_space(
            _plain_text_from_html_fragment(media_caption) or reply_context_text or ""
        )
        if not digest_text:
            mark_seen_many(channel_id, message_ids)
            return
        severity = "medium"
        severity_score = 0.0
        severity_breakdown: dict = {}
        if _is_severity_routing_enabled():
            severity, severity_score, severity_breakdown = _classify_severity_with_breakdown(
                text=digest_text,
                source=source,
                channel_id=channel_id,
                message_id=messages[0].id,
                has_media=True,
                has_link=bool(link),
                reply_to=_reply_to_message_id(messages[0]),
                album_size=len(messages),
            )
        elif _contains_breaking_keyword(digest_text):
            severity = "high"
        _queue_for_digest(
            channel_id,
            messages[0].id,
            digest_text,
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
            has_media=True,
            text_tokens=estimate_tokens_rough(digest_text),
            pending=count_pending(),
            severity_breakdown=severity_breakdown,
        )
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

    if (
        severity != "high"
        and _contains_breaking_keyword(combined_caption)
        and not should_downgrade_explainer_urgency(combined_caption)
        and looks_like_live_event_update(combined_caption)
    ):
        severity = "high"
        severity_score = max(float(severity_score), _high_severity_score_floor())
        severity_breakdown = dict(severity_breakdown or {})
        severity_breakdown["keyword_override"] = True

    if severity == "high" and _is_immediate_high_enabled():
        headline = await summarize_breaking_headline(combined_caption, _require_auth_manager())
        if not headline:
            mark_seen_many(channel_id, message_ids)
            return
        cluster_result = await _apply_breaking_story_cluster_policy(
            messages=messages,
            source=source,
            candidate_text=combined_caption,
            headline=headline,
        )
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
            final_action=str(cluster_result.get("final_action") or ""),
            cluster_decision=str(cluster_result.get("cluster_decision") or ""),
            reply_to=int(cluster_result.get("reply_to") or 0),
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


def _pipeline_worker_targets() -> Dict[str, int]:
    targets = {
        INBOUND_STAGE_TRIAGE: max(1, int(getattr(config, "PIPELINE_TRIAGE_WORKERS", 4) or 4)),
        INBOUND_STAGE_AI_DECISION: max(1, int(getattr(config, "PIPELINE_AI_WORKERS", 2) or 2)),
        INBOUND_STAGE_DELIVERY: max(1, int(getattr(config, "PIPELINE_DELIVERY_WORKERS", 2) or 2)),
        INBOUND_STAGE_OCR: max(1, int(getattr(config, "PIPELINE_OCR_WORKERS", 1) or 1)),
        "query_web": max(1, int(getattr(config, "PIPELINE_QUERY_WEB_WORKERS", 1) or 1)),
    }
    if _is_low_memory_runtime():
        budget = _runtime_memory_budget_bytes() or 0
        triage_cap = 1 if budget <= (1024 * 1024 * 1024) else 2
        targets[INBOUND_STAGE_TRIAGE] = min(targets[INBOUND_STAGE_TRIAGE], triage_cap)
        targets[INBOUND_STAGE_AI_DECISION] = min(targets[INBOUND_STAGE_AI_DECISION], 1)
        targets[INBOUND_STAGE_DELIVERY] = min(targets[INBOUND_STAGE_DELIVERY], 1)
        targets[INBOUND_STAGE_OCR] = min(targets[INBOUND_STAGE_OCR], 1)
        targets["query_web"] = min(targets["query_web"], 1)
        _log_low_memory_runtime_once("pipeline worker fan-out reduced for constrained Linux host")
    return targets


def _pipeline_job_max_retries() -> int:
    return max(1, int(getattr(config, "PIPELINE_JOB_MAX_RETRIES", 4) or 4))


def _pipeline_retry_delay_seconds(retry_count: int) -> int:
    base = max(1, int(getattr(config, "PIPELINE_RETRY_BASE_SECONDS", 2) or 2))
    exponent = max(0, int(retry_count))
    return min(300, base * (2 ** exponent))


def _record_pipeline_stage_latency(stage: str, elapsed_seconds: float) -> None:
    pipeline_stage_runs[stage] += 1
    pipeline_stage_latency_seconds[stage].append(max(0.0, float(elapsed_seconds)))


def _pipeline_stage_latency_snapshot() -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for stage in (
        INBOUND_STAGE_TRIAGE,
        INBOUND_STAGE_OCR,
        INBOUND_STAGE_AI_DECISION,
        INBOUND_STAGE_DELIVERY,
        INBOUND_STAGE_ARCHIVE,
    ):
        values = list(pipeline_stage_latency_seconds.get(stage, ()))
        if not values:
            out[stage] = {
                "count": float(pipeline_stage_runs.get(stage, 0)),
                "avg_ms": 0.0,
                "p95_ms": 0.0,
            }
            continue
        ordered = sorted(values)
        p95_index = max(0, min(len(ordered) - 1, int(math.ceil(len(ordered) * 0.95)) - 1))
        out[stage] = {
            "count": float(pipeline_stage_runs.get(stage, 0)),
            "avg_ms": round((sum(values) / len(values)) * 1000.0, 2),
            "p95_ms": round(ordered[p95_index] * 1000.0, 2),
        }
    return out


def _pipeline_payload_json(payload: Dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _pipeline_job_message_ids(payload: Dict[str, object]) -> List[int]:
    raw_ids = payload.get("message_ids")
    if not isinstance(raw_ids, list):
        return []
    return [int(item) for item in raw_ids if int(item or 0) > 0]


def _pipeline_job_primary_message_id(payload: Dict[str, object]) -> int:
    message_ids = _pipeline_job_message_ids(payload)
    if message_ids:
        return int(message_ids[0])
    return int(payload.get("primary_message_id") or 0)


def _pipeline_job_key(*, channel_id: str, message_ids: Sequence[int], grouped_id: int = 0) -> str:
    cleaned_ids = [int(item) for item in message_ids if int(item or 0) > 0]
    if grouped_id > 0:
        return f"album:{channel_id}:{grouped_id}"
    if len(cleaned_ids) <= 1:
        return f"single:{channel_id}:{cleaned_ids[0] if cleaned_ids else 0}"
    return f"multi:{channel_id}:{cleaned_ids[0]}:{cleaned_ids[-1]}"


def _pipeline_priority_for_severity(severity: str | None) -> int:
    normalized = (severity or "").strip().lower()
    if normalized == "high":
        return 90
    if normalized == "medium":
        return 60
    return 50


def _build_inbound_payload(
    messages: Sequence[Message],
    *,
    kind: str,
    grouped_id: int = 0,
) -> Dict[str, object]:
    ordered = sorted(messages, key=lambda item: int(item.id or 0))
    primary = ordered[0]
    message_ids = [int(item.id or 0) for item in ordered if int(item.id or 0) > 0]
    return {
        "kind": kind,
        "channel_id": str(primary.chat_id),
        "primary_message_id": int(primary.id or 0),
        "message_ids": message_ids,
        "grouped_id": int(grouped_id or 0),
        "queued_at": int(time.time()),
    }


def _mark_payload_seen(payload: Dict[str, object]) -> None:
    channel_id = str(payload.get("channel_id") or "")
    message_ids = _pipeline_job_message_ids(payload)
    if not channel_id or not message_ids:
        return
    if len(message_ids) == 1:
        mark_seen(channel_id, message_ids[0])
    else:
        mark_seen_many(channel_id, message_ids)


async def _enqueue_inbound_messages(
    messages: Sequence[Message],
    *,
    kind: str,
    grouped_id: int = 0,
) -> bool:
    if not messages:
        return False
    payload = _build_inbound_payload(messages, kind=kind, grouped_id=grouped_id)
    message_ids = _pipeline_job_message_ids(payload)
    inserted = enqueue_inbound_job(
        job_key=_pipeline_job_key(
            channel_id=str(payload.get("channel_id") or ""),
            message_ids=message_ids,
            grouped_id=int(payload.get("grouped_id") or 0),
        ),
        stage=INBOUND_STAGE_TRIAGE,
        channel_id=str(payload.get("channel_id") or ""),
        message_id=_pipeline_job_primary_message_id(payload),
        payload_json=_pipeline_payload_json(payload),
        priority=50,
        max_retries=_pipeline_job_max_retries(),
    )
    if inserted:
        log_structured(
            LOGGER,
            "inbound_job_enqueued",
            stage=INBOUND_STAGE_TRIAGE,
            channel_id=str(payload.get("channel_id") or ""),
            message_id=_pipeline_job_primary_message_id(payload),
            kind=kind,
            grouped_id=int(payload.get("grouped_id") or 0),
            item_count=len(message_ids),
        )
    return inserted


def _load_inbound_payload(job: Dict[str, object]) -> Dict[str, object]:
    raw = str(job.get("payload_json") or "{}")
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


async def _fetch_messages_for_payload(payload: Dict[str, object]) -> List[Message]:
    channel_id = str(payload.get("channel_id") or "").strip()
    message_ids = _pipeline_job_message_ids(payload)
    if not channel_id or not message_ids:
        return []

    chat_ref: object = channel_id
    if channel_id.lstrip("-").isdigit():
        with contextlib.suppress(Exception):
            chat_ref = int(channel_id)

    tg = _require_client()
    try:
        fetched = await _call_with_floodwait(tg.get_messages, chat_ref, ids=message_ids)
    except Exception:
        LOGGER.debug("Failed to rehydrate inbound job messages.", exc_info=True)
        return []

    if isinstance(fetched, Message):
        fetched_items = [fetched]
    elif isinstance(fetched, list):
        fetched_items = [item for item in fetched if isinstance(item, Message)]
    else:
        fetched_items = []

    if not fetched_items:
        return []

    by_id = {int(item.id or 0): item for item in fetched_items if int(item.id or 0) > 0}
    resolved = [by_id[item_id] for item_id in message_ids if item_id in by_id]
    return sorted(resolved, key=lambda item: int(item.id or 0))


async def _advance_job_to_archive(job: Dict[str, object], payload: Dict[str, object]) -> None:
    advance_inbound_job(
        int(job["id"]),
        next_stage=INBOUND_STAGE_ARCHIVE,
        payload_json=_pipeline_payload_json(payload),
        priority=50,
    )


async def _handle_triage_inbound_job(job: Dict[str, object]) -> None:
    payload = _load_inbound_payload(job)
    messages = await _fetch_messages_for_payload(payload)
    if not messages:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "message_missing"
        await _advance_job_to_archive(job, payload)
        return

    primary = messages[0]
    source, link = await _source_info(primary)
    combined_text = normalize_space(
        "\n".join((item.message or "").strip() for item in messages if (item.message or "").strip())
    )
    has_media = any(bool(item.media) for item in messages)
    message_ids = [int(item.id or 0) for item in messages if int(item.id or 0) > 0]

    severity = "medium"
    severity_score = 0.0
    severity_breakdown: dict = {}
    if combined_text:
        if _is_severity_routing_enabled():
            severity, severity_score, severity_breakdown = _classify_severity_with_breakdown(
                text=combined_text,
                source=source,
                channel_id=str(primary.chat_id),
                message_id=int(primary.id or 0),
                has_media=has_media,
                has_link=bool(link),
                reply_to=_reply_to_message_id(primary),
                album_size=len(messages),
            )
        elif _contains_breaking_keyword(combined_text):
            severity = "high"

        if (
            severity != "high"
            and _contains_breaking_keyword(combined_text)
            and not should_downgrade_explainer_urgency(combined_text)
            and looks_like_live_event_update(combined_text)
        ):
            severity = "high"
            severity_score = max(float(severity_score), _high_severity_score_floor())
            severity_breakdown = dict(severity_breakdown or {})
            severity_breakdown["keyword_override"] = True

    payload.update(
        {
            "channel_id": str(primary.chat_id),
            "primary_message_id": int(primary.id or 0),
            "message_ids": message_ids,
            "source": source,
            "message_link": link or "",
            "combined_text": combined_text,
            "candidate_text": combined_text,
            "archive_text": combined_text,
            "has_text": bool(combined_text),
            "has_media": has_media,
            "severity": severity,
            "severity_score": float(severity_score),
            "severity_breakdown": severity_breakdown,
        }
    )

    if not combined_text and not has_media:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "empty_message"
        await _advance_job_to_archive(job, payload)
        return

    next_stage = INBOUND_STAGE_AI_DECISION if combined_text else INBOUND_STAGE_DELIVERY
    if not combined_text and has_media and _media_text_ocr_enabled():
        next_stage = INBOUND_STAGE_OCR

    advance_inbound_job(
        int(job["id"]),
        next_stage=next_stage,
        payload_json=_pipeline_payload_json(payload),
        priority=_pipeline_priority_for_severity(severity),
    )


async def _handle_ocr_inbound_job(job: Dict[str, object]) -> None:
    payload = _load_inbound_payload(job)
    messages = await _fetch_messages_for_payload(payload)
    if not messages:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "message_missing"
        await _advance_job_to_archive(job, payload)
        return

    caption_html = ""
    if str(payload.get("kind") or "single") == "album":
        caption_html = str(await _caption_for_album_without_text(messages) or "")
    else:
        caption_html = str(await _caption_for_media_without_text(messages[0]) or "")

    plain_text = _plain_text_from_html_fragment(caption_html)
    payload["ocr_caption_html"] = caption_html
    payload["ocr_plain_text"] = plain_text
    if plain_text and not str(payload.get("candidate_text") or "").strip():
        payload["candidate_text"] = plain_text
        payload["archive_text"] = plain_text

    next_stage = INBOUND_STAGE_AI_DECISION if str(payload.get("candidate_text") or "").strip() else INBOUND_STAGE_DELIVERY
    advance_inbound_job(
        int(job["id"]),
        next_stage=next_stage,
        payload_json=_pipeline_payload_json(payload),
        priority=_pipeline_priority_for_severity(str(payload.get("severity") or "medium")),
    )


async def _handle_ai_inbound_job(job: Dict[str, object]) -> None:
    payload = _load_inbound_payload(job)
    candidate_text = normalize_space(str(payload.get("candidate_text") or payload.get("combined_text") or ""))
    if not candidate_text:
        advance_inbound_job(
            int(job["id"]),
            next_stage=INBOUND_STAGE_DELIVERY,
            payload_json=_pipeline_payload_json(payload),
            priority=_pipeline_priority_for_severity(str(payload.get("severity") or "medium")),
        )
        return

    decision = await decide_filter_action(candidate_text, _require_auth_manager())
    payload["triage_severity"] = str(payload.get("severity") or "medium")
    payload["severity"] = decision.severity
    if decision.severity == "high":
        decision.story_bridge_html = ""
    payload["filter_decision"] = {
        "action": decision.action,
        "severity": decision.severity,
        "summary_html": decision.summary_html,
        "headline_html": decision.headline_html,
        "story_bridge_html": decision.story_bridge_html,
        "confidence": decision.confidence,
        "reason_code": decision.reason_code,
        "topic_key": decision.topic_key,
        "needs_ocr_translation": decision.needs_ocr_translation,
        "copy_origin": decision.copy_origin,
        "routing_origin": decision.routing_origin,
        "fallback_reason": decision.fallback_reason,
        "ai_attempt_count": decision.ai_attempt_count,
        "ai_quality_retry_used": decision.ai_quality_retry_used,
        "cached": decision.cached,
    }
    log_structured(
        LOGGER,
        "ai_filter_decision_ready",
        message_id=_pipeline_job_primary_message_id(payload),
        severity=decision.severity,
        action=decision.action,
        copy_origin=decision.copy_origin,
        routing_origin=decision.routing_origin,
        fallback_reason=decision.fallback_reason,
        ai_attempt_count=decision.ai_attempt_count,
        ai_quality_retry_used=decision.ai_quality_retry_used,
        cached=decision.cached,
    )
    advance_inbound_job(
        int(job["id"]),
        next_stage=INBOUND_STAGE_DELIVERY,
        payload_json=_pipeline_payload_json(payload),
        priority=_pipeline_priority_for_severity(str(payload.get("severity") or "medium")),
    )


async def _handle_delivery_inbound_job(job: Dict[str, object]) -> None:
    payload = _load_inbound_payload(job)
    messages = await _fetch_messages_for_payload(payload)
    if not messages:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "message_missing"
        await _advance_job_to_archive(job, payload)
        return

    primary = messages[0]
    kind = str(payload.get("kind") or "single")
    channel_id = str(payload.get("channel_id") or primary.chat_id)
    message_ids = [int(item.id or 0) for item in messages if int(item.id or 0) > 0]
    source = str(payload.get("source") or "")
    link = normalize_space(str(payload.get("message_link") or ""))
    has_media = bool(payload.get("has_media"))
    severity = str(payload.get("severity") or "medium")
    candidate_text = normalize_space(str(payload.get("candidate_text") or payload.get("combined_text") or ""))
    decision = payload.get("filter_decision")
    if not isinstance(decision, dict):
        decision = {}
    action = normalize_space(str(decision.get("action") or ("deliver" if candidate_text else ""))).lower()

    if action == "skip":
        payload["final_action"] = "skip"
        payload["skip_reason"] = str(decision.get("reason_code") or "ai_skip")
        await _advance_job_to_archive(job, payload)
        return

    if not candidate_text and has_media:
        reply_message = await _load_reply_message(primary)
        media_caption = str(payload.get("ocr_caption_html") or "")
        reply_caption, reply_context_text = await _build_reply_context_caption(source, reply_message)
        if not media_caption:
            media_caption = reply_caption or ""
        digest_text = (
            _plain_text_from_html_fragment(media_caption)
            or normalize_space(reply_context_text or "")
            or normalize_space(str(payload.get("archive_text") or ""))
        )
        if digest_text:
            _queue_for_digest(
                channel_id,
                int(primary.id or 0),
                digest_text,
                source_name=source,
                message_link=link or None,
            )
            payload["archive_text"] = digest_text
            payload["final_action"] = "digest_queued"
            log_structured(
                LOGGER,
                "digest_pipeline_media_queued",
                channel_id=channel_id,
                message_id=int(primary.id or 0),
                severity=severity,
                source=source,
                kind=kind,
                pending=count_pending(),
                copy_origin=str(decision.get("copy_origin") or ""),
                routing_origin=str(decision.get("routing_origin") or ""),
                fallback_reason=str(decision.get("fallback_reason") or ""),
                ai_attempt_count=int(decision.get("ai_attempt_count") or 1),
                ai_quality_retry_used=bool(decision.get("ai_quality_retry_used")),
            )
        else:
            payload["final_action"] = "skip"
            payload["skip_reason"] = "media_digest_text_missing"
        await _advance_job_to_archive(job, payload)
        return

    if severity == "high" and action == "deliver" and _is_immediate_high_enabled() and candidate_text:
        headline = _plain_text_from_html_fragment(str(decision.get("headline_html") or ""))
        if not headline:
            summary_headline, _ = extract_feed_summary_parts(
                str(decision.get("summary_html") or ""),
                candidate_text,
            )
            headline = normalize_space(summary_headline)
        if not headline:
            payload["final_action"] = "skip"
            payload["skip_reason"] = "missing_breaking_headline"
            payload["archive_text"] = candidate_text
            await _advance_job_to_archive(job, payload)
            return
        if len(headline) > 420:
            headline = f"{headline[:417].rsplit(' ', 1)[0]}..."
        story_bridge = _plain_text_from_html_fragment(str(decision.get("story_bridge_html") or ""))
        cluster_result = await _apply_breaking_story_cluster_policy(
            messages=messages,
            source=source,
            candidate_text=candidate_text,
            headline=headline,
            topic_key=str(decision.get("topic_key") or ""),
            story_bridge=story_bridge or None,
        )
        payload["delivery_message_id"] = int(cluster_result.get("delivery_message_id") or 0)
        payload["archive_text"] = candidate_text
        payload["final_action"] = str(cluster_result.get("final_action") or "breaking_delivery")
        payload["story_cluster_decision"] = str(cluster_result.get("cluster_decision") or "")
        payload["story_cluster_id"] = str(cluster_result.get("cluster_id") or "")
        log_structured(
            LOGGER,
            "breaking_pipeline_sent",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            source=source,
            severity=severity,
            severity_score=float(payload.get("severity_score") or 0.0),
            reply_to=int(cluster_result.get("reply_to") or 0),
            kind=kind,
            final_action=str(cluster_result.get("final_action") or ""),
            cluster_decision=str(cluster_result.get("cluster_decision") or ""),
            copy_origin=str(decision.get("copy_origin") or ""),
            routing_origin=str(decision.get("routing_origin") or ""),
            fallback_reason=str(decision.get("fallback_reason") or ""),
            ai_attempt_count=int(decision.get("ai_attempt_count") or 1),
            ai_quality_retry_used=bool(decision.get("ai_quality_retry_used")),
        )
        await _advance_job_to_archive(job, payload)
        return

    if _is_digest_mode_enabled() and candidate_text:
        _queue_for_digest(
            channel_id,
            int(primary.id or 0),
            candidate_text,
            source_name=source,
            message_link=link or None,
        )
        payload["final_action"] = "digest_queued"
        payload["archive_text"] = candidate_text
        log_structured(
            LOGGER,
            "digest_pipeline_queued",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            severity=severity,
            source=source,
            kind=kind,
            pending=count_pending(),
            digest_mode=True,
            copy_origin=str(decision.get("copy_origin") or ""),
            routing_origin=str(decision.get("routing_origin") or ""),
            fallback_reason=str(decision.get("fallback_reason") or ""),
            ai_attempt_count=int(decision.get("ai_attempt_count") or 1),
            ai_quality_retry_used=bool(decision.get("ai_quality_retry_used")),
        )
        await _advance_job_to_archive(job, payload)
        return

    if action == "digest" and candidate_text:
        _queue_for_digest(
            channel_id,
            int(primary.id or 0),
            candidate_text,
            source_name=source,
            message_link=link or None,
        )
        payload["final_action"] = "digest_queued"
        payload["archive_text"] = candidate_text
        log_structured(
            LOGGER,
            "digest_pipeline_queued",
            channel_id=channel_id,
            message_id=int(primary.id or 0),
            severity=severity,
            source=source,
            kind=kind,
            pending=count_pending(),
            digest_mode=False,
            copy_origin=str(decision.get("copy_origin") or ""),
            routing_origin=str(decision.get("routing_origin") or ""),
            fallback_reason=str(decision.get("fallback_reason") or ""),
            ai_attempt_count=int(decision.get("ai_attempt_count") or 1),
            ai_quality_retry_used=bool(decision.get("ai_quality_retry_used")),
        )
        await _advance_job_to_archive(job, payload)
        return

    summary_html = str(decision.get("summary_html") or "").strip()
    if not summary_html:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "decision_missing_summary"
        payload["archive_text"] = candidate_text
        await _advance_job_to_archive(job, payload)
        return
    reply_to = await _resolve_source_reply_target(primary)
    summary_headline, summary_context = extract_feed_summary_parts(summary_html, candidate_text)
    resolved_context = await _resolve_dynamic_delivery_context(
        current_text=candidate_text,
        headline=summary_headline,
        candidate_context=summary_context,
        topic_key=str(decision.get("topic_key") or ""),
        source_title=source,
    )
    formatted_summary = _format_summary_text(
        source,
        summary_html,
        raw_text=candidate_text,
        severity=severity,
        context_override=resolved_context,
    )
    if not formatted_summary:
        payload["final_action"] = "skip"
        payload["skip_reason"] = "caption_redacted_empty"
        payload["archive_text"] = candidate_text
        await _advance_job_to_archive(job, payload)
        return
    if has_media:
        if kind == "album":
            sent_ref = await _send_album(messages, formatted_summary, reply_to=reply_to)
        else:
            sent_ref = await _send_single_media(primary, formatted_summary, reply_to=reply_to)
    else:
        sent_ref = await _send_text_with_ref(formatted_summary, reply_to=reply_to)

    _register_source_delivery_refs(
        channel_id=channel_id,
        source_message_ids=message_ids,
        sent_ref=sent_ref,
    )
    payload["delivery_message_id"] = int(_message_ref_id(sent_ref) or 0)
    payload["archive_text"] = candidate_text
    payload["final_action"] = "delivered"
    await _advance_job_to_archive(job, payload)


async def _handle_archive_inbound_job(job: Dict[str, object]) -> None:
    payload = _load_inbound_payload(job)
    final_action = str(payload.get("final_action") or "").strip().lower()
    archive_text = normalize_space(str(payload.get("archive_text") or ""))
    channel_id = str(payload.get("channel_id") or "")
    primary_message_id = _pipeline_job_primary_message_id(payload)
    source_name = normalize_space(str(payload.get("source") or ""))
    message_link = normalize_space(str(payload.get("message_link") or ""))

    if final_action in {
        "digest_queued",
        "breaking_delivery",
        "breaking_delivery_threaded",
        "breaking_root_edited",
        "breaking_suppressed",
        "delivered",
        "media_passthrough",
    } and archive_text:
        _archive_for_query_search(
            channel_id,
            primary_message_id,
            archive_text,
            source_name=source_name or None,
            message_link=message_link or None,
        )

    _mark_payload_seen(payload)
    complete_inbound_job(int(job["id"]), payload_json=_pipeline_payload_json(payload))


async def _handle_inbound_job(stage: str, job: Dict[str, object]) -> None:
    if stage == INBOUND_STAGE_TRIAGE:
        await _handle_triage_inbound_job(job)
        return
    if stage == INBOUND_STAGE_OCR:
        await _handle_ocr_inbound_job(job)
        return
    if stage == INBOUND_STAGE_AI_DECISION:
        await _handle_ai_inbound_job(job)
        return
    if stage == INBOUND_STAGE_DELIVERY:
        await _handle_delivery_inbound_job(job)
        return
    if stage == INBOUND_STAGE_ARCHIVE:
        await _handle_archive_inbound_job(job)
        return
    raise RuntimeError(f"Unsupported inbound job stage: {stage}")


async def _run_inbound_stage_worker(stage: str, worker_index: int) -> None:
    worker_id = f"{stage}-{worker_index}-{uuid.uuid4().hex[:8]}"
    while True:
        jobs = claim_inbound_jobs(stage, 1, worker_id=worker_id)
        if not jobs:
            await asyncio.sleep(0.25)
            continue

        job = jobs[0]
        started = time.perf_counter()
        try:
            await _handle_inbound_job(stage, job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception(
                "Inbound pipeline stage failed stage=%s job_id=%s job_key=%s",
                stage,
                job.get("id"),
                job.get("job_key"),
            )
            retry_or_dead_letter_inbound_job(
                int(job["id"]),
                error_text=str(exc)[:1000],
                retry_delay_seconds=_pipeline_retry_delay_seconds(int(job.get("retry_count") or 0)),
            )
        finally:
            _record_pipeline_stage_latency(stage, time.perf_counter() - started)


async def _start_pipeline_workers() -> None:
    global pipeline_query_web_semaphore, pipeline_media_semaphore
    if pipeline_worker_tasks:
        return

    targets = _pipeline_worker_targets()
    pipeline_query_web_semaphore = asyncio.Semaphore(max(1, int(targets.get("query_web", 1) or 1)))
    pipeline_media_semaphore = asyncio.Semaphore(max(1, int(getattr(config, "PIPELINE_MEDIA_CONCURRENCY", 2) or 2)))
    reset_in_progress_inbound_jobs(older_than_seconds=0)
    purge_ai_decision_cache(
        older_than_ts=int(time.time()) - (max(1, int(getattr(config, "AI_DECISION_CACHE_HOURS", 72) or 72)) * 3600)
    )

    for stage in (
        INBOUND_STAGE_TRIAGE,
        INBOUND_STAGE_OCR,
        INBOUND_STAGE_AI_DECISION,
        INBOUND_STAGE_DELIVERY,
        INBOUND_STAGE_ARCHIVE,
    ):
        for index in range(max(1, int(targets.get(stage, 1) or 1))):
                pipeline_worker_tasks.append(
                    asyncio.create_task(
                        _run_inbound_stage_worker(stage, index + 1),
                        name=f"inbound-{stage}-{index + 1}",
                    )
                )
    _log_memory_snapshot(
        "pipeline_workers_started",
        pipeline_worker_count=len(pipeline_worker_tasks),
        pipeline_worker_targets=targets,
        pipeline_media_concurrency=int(getattr(config, "PIPELINE_MEDIA_CONCURRENCY", 2) or 2),
    )


async def _stop_pipeline_workers() -> None:
    global pipeline_query_web_semaphore
    pending = [task for task in pipeline_worker_tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    pipeline_worker_tasks.clear()
    pipeline_query_web_semaphore = None
    reset_in_progress_inbound_jobs(older_than_seconds=0)
    _log_memory_snapshot(
        "pipeline_workers_stopped",
        pipeline_worker_count=0,
        cancelled_worker_count=len(pending),
    )


def _format_digest_message(
    digest_body: str,
    total_updates: int,
    sources: List[str],
    *,
    title: str = "Digest",
    interval_minutes: int | None = None,
) -> str:
    timestamp = datetime.now().astimezone().strftime("%H:%M %Z").strip()
    label = sanitize_telegram_html(title.strip() or "Digest")
    header = f"<b>📰 {label}</b>"
    style = digest_output_style(interval_minutes or 24 * 60)
    stat_label = "headlines tracked" if style == "headline_rail" else "updates reviewed"
    metadata = sanitize_telegram_html(f"{timestamp} • {total_updates} {stat_label}")
    _ = sources
    return sanitize_telegram_html(f"{header}<br>{metadata}<br><br>{digest_body.strip()}")


def _digest_support_line_limit() -> int:
    raw = getattr(config, "DIGEST_MAX_LINES", 12)
    try:
        value = int(raw)
    except Exception:
        value = 12
    return max(3, min(value, 12))


def _render_digest_body_sections(
    headline: str,
    story: str,
    highlights: Sequence[str],
    also_moving: Sequence[str],
) -> str:
    clean_headline = normalize_space(strip_telegram_html(headline))
    clean_story = normalize_space(strip_telegram_html(story))
    clean_highlights = [
        normalize_space(strip_telegram_html(item))
        for item in highlights
        if normalize_space(strip_telegram_html(item))
    ]
    clean_also = [
        normalize_space(strip_telegram_html(item))
        for item in also_moving
        if normalize_space(strip_telegram_html(item))
    ]
    if clean_headline and not clean_story and clean_highlights:
        blocks = [
            "<br>".join(
                [
                    f"<b>{sanitize_telegram_html(clean_headline)}</b>",
                    *[f"• {sanitize_telegram_html(item)}" for item in clean_highlights],
                ]
            )
        ]
        if clean_also:
            blocks.append(
                "<br>".join(
                    [
                        "<i>Also moving</i>",
                        *[f"• {sanitize_telegram_html(item)}" for item in clean_also],
                    ]
                )
            )
        return sanitize_telegram_html("<br><br>".join(blocks))
    if not clean_headline or not clean_story:
        return sanitize_telegram_html(
            normalize_space(strip_telegram_html(story or headline or " ".join(clean_highlights)))
        )

    blocks = [
        "<br>".join(
            [
                f"<b>{sanitize_telegram_html(clean_headline)}</b>",
                sanitize_telegram_html(clean_story),
                *[f"• {sanitize_telegram_html(item)}" for item in clean_highlights],
            ]
        )
    ]
    if clean_also:
        blocks.append(
            "<br>".join(
                [
                    "<i>Also moving</i>",
                    *[f"• {sanitize_telegram_html(item)}" for item in clean_also],
                ]
            )
        )
    return sanitize_telegram_html("<br><br>".join(blocks))


def _split_digest_story_sentences(text: str, *, limit: int) -> List[str]:
    cleaned = normalize_space(strip_telegram_html(text))
    if not cleaned:
        return []
    if len(cleaned) <= limit:
        return [cleaned]

    sentences = [
        normalize_space(part)
        for part in re.split(r"(?<=[.!?])\s+", cleaned)
        if normalize_space(part)
    ]
    if not sentences:
        return [cleaned]

    chunks: List[str] = []
    current = ""
    for sentence in sentences:
        candidate = f"{current} {sentence}".strip() if current else sentence
        if current and len(candidate) > limit:
            chunks.append(current)
            current = sentence
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks or [cleaned]


def _split_digest_body_blocks(digest_body: str, *, max_chars: int) -> List[str]:
    cleaned = normalize_space(str(digest_body or ""))
    if not cleaned:
        return []
    headline, story, highlights, also_moving = extract_digest_narrative_parts(
        digest_body,
        max_lines=_digest_support_line_limit(),
    )
    if not headline:
        return [sanitize_telegram_html(cleaned)]
    if not story and highlights:
        remaining_highlights = list(highlights)
        remaining_also = list(also_moving)
        support_limit = _digest_support_line_limit()
        chunks: List[str] = []

        while remaining_highlights or remaining_also:
            part_highlights: List[str] = []
            part_also: List[str] = []

            while remaining_highlights and len(part_highlights) + len(part_also) < support_limit:
                candidate = _render_digest_body_sections(
                    headline,
                    "",
                    [*part_highlights, remaining_highlights[0]],
                    part_also,
                )
                if len(candidate) > max_chars and part_highlights:
                    break
                part_highlights.append(remaining_highlights.pop(0))

            while remaining_also and len(part_highlights) + len(part_also) < support_limit:
                candidate = _render_digest_body_sections(
                    headline,
                    "",
                    part_highlights,
                    [*part_also, remaining_also[0]],
                )
                if len(candidate) > max_chars and (part_also or part_highlights):
                    break
                part_also.append(remaining_also.pop(0))

            if not part_highlights and not part_also:
                if remaining_highlights:
                    part_highlights.append(remaining_highlights.pop(0))
                elif remaining_also:
                    part_also.append(remaining_also.pop(0))

            rendered = _render_digest_body_sections(
                headline,
                "",
                part_highlights,
                part_also,
            )
            if normalize_space(strip_telegram_html(rendered)):
                chunks.append(rendered)

        return chunks or [_render_digest_body_sections(headline, "", highlights, also_moving)]

    story_chunks = _split_digest_story_sentences(
        story,
        limit=max(140, max_chars - len(normalize_space(headline)) - 60),
    )
    if not story_chunks:
        story_chunks = [story]

    chunks: List[str] = []
    for story_chunk in story_chunks[:-1]:
        rendered = _render_digest_body_sections(headline, story_chunk, [], [])
        if normalize_space(strip_telegram_html(rendered)):
            chunks.append(rendered)

    active_story = story_chunks[-1]
    remaining_highlights = list(highlights)
    remaining_also = list(also_moving)
    support_limit = _digest_support_line_limit()
    base_body_len = len(_render_digest_body_sections(headline, active_story, [], []))

    if not remaining_highlights and not remaining_also:
        rendered = _render_digest_body_sections(headline, active_story, [], [])
        return [rendered] if normalize_space(strip_telegram_html(rendered)) else []

    while remaining_highlights or remaining_also:
        part_highlights: List[str] = []
        part_also: List[str] = []

        while remaining_highlights and len(part_highlights) + len(part_also) < support_limit:
            candidate = _render_digest_body_sections(
                headline,
                active_story,
                [*part_highlights, remaining_highlights[0]],
                part_also,
            )
            if len(candidate) > max_chars and (part_highlights or base_body_len <= max_chars):
                break
            part_highlights.append(remaining_highlights.pop(0))

        while remaining_also and len(part_highlights) + len(part_also) < support_limit:
            candidate = _render_digest_body_sections(
                headline,
                active_story,
                part_highlights,
                [*part_also, remaining_also[0]],
            )
            if len(candidate) > max_chars and (part_also or part_highlights or base_body_len <= max_chars):
                break
            part_also.append(remaining_also.pop(0))

        if not part_highlights and not part_also:
            if remaining_highlights:
                part_highlights.append(remaining_highlights.pop(0))
            elif remaining_also:
                part_also.append(remaining_also.pop(0))

        rendered = _render_digest_body_sections(
            headline,
            active_story,
            part_highlights,
            part_also,
        )
        if normalize_space(strip_telegram_html(rendered)):
            chunks.append(rendered)

    return chunks or [_render_digest_body_sections(headline, active_story, [], [])]


def _get_last_completed_digest_window_end() -> int | None:
    raw = normalize_space(str(get_meta(ROLLING_DIGEST_LAST_COMPLETED_META_KEY) or ""))
    if not raw:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    return value if value > 0 else None


def _align_digest_window_end(timestamp: int, *, interval_seconds: int | None = None) -> int:
    resolved_interval = max(60, int(interval_seconds or _digest_interval_seconds()))
    ts = int(max(0, timestamp))
    if ts <= 0:
        return resolved_interval
    return ((ts + resolved_interval - 1) // resolved_interval) * resolved_interval


def _last_closed_digest_window_end(now_ts: int | None = None) -> int:
    resolved_now = int(time.time() if now_ts is None else now_ts)
    interval_seconds = max(60, _digest_interval_seconds())
    return (resolved_now // interval_seconds) * interval_seconds


def _next_digest_window_end_to_process(now_ts: int | None = None) -> int | None:
    interval_seconds = max(60, _digest_interval_seconds())
    last_closed_window_end = _last_closed_digest_window_end(now_ts)
    last_completed_window_end = _get_last_completed_digest_window_end()
    if last_completed_window_end is not None:
        next_window_end = last_completed_window_end + interval_seconds
        if next_window_end <= last_closed_window_end:
            return next_window_end
        return None

    oldest_pending_ts = peek_oldest_pending_digest_timestamp()
    if oldest_pending_ts is not None:
        next_window_end = _align_digest_window_end(oldest_pending_ts, interval_seconds=interval_seconds)
        if next_window_end <= last_closed_window_end:
            return next_window_end

    return last_closed_window_end if last_closed_window_end > 0 else None


def _next_digest_boundary_timestamp(now_ts: int | None = None) -> int:
    resolved_now = int(time.time() if now_ts is None else now_ts)
    interval_seconds = max(60, _digest_interval_seconds())
    current_boundary = (resolved_now // interval_seconds) * interval_seconds
    return current_boundary + interval_seconds


def _digest_input_token_budget() -> int:
    raw_max = getattr(config, "DIGEST_MAX_TOKENS", 18000)
    raw_context = getattr(config, "CODEX_MODEL_CONTEXT_TOKENS", 200000)
    raw_fraction = getattr(config, "DIGEST_CONTEXT_FRACTION", 0.75)
    try:
        max_tokens = int(raw_max)
    except Exception:
        max_tokens = 18000
    try:
        context_tokens = int(raw_context)
    except Exception:
        context_tokens = 200000
    try:
        fraction = float(raw_fraction)
    except Exception:
        fraction = 0.75
    fraction = min(max(fraction, 0.1), 0.9)
    hard_cap = max(4000, int(context_tokens * fraction))
    token_budget = max(2000, min(max_tokens, hard_cap))
    return max(1200, token_budget - 2200)


def _digest_window_page_size() -> int:
    raw = getattr(config, "DIGEST_WINDOW_PAGE_SIZE", 200)
    try:
        value = int(raw)
    except Exception:
        value = 200
    return max(25, min(value, 1000))


def _estimate_digest_row_tokens(row: Dict[str, object]) -> int:
    text = normalize_space(str(row.get("raw_text") or ""))
    if not text:
        return 0
    return max(48, estimate_tokens_rough(text) + 32)


def _iter_digest_row_groups(
    row_loader,
    *,
    token_budget: int,
    page_size: int,
):
    after_id = 0
    current_rows: List[Dict[str, object]] = []
    current_tokens = 0
    while True:
        rows = row_loader(after_id=after_id, limit=page_size)
        if not rows:
            break
        after_id = int(rows[-1].get("id") or after_id)
        for row in rows:
            row_tokens = _estimate_digest_row_tokens(row)
            if row_tokens <= 0:
                continue
            if current_rows and current_tokens + row_tokens > token_budget:
                yield current_rows
                current_rows = []
                current_tokens = 0
            current_rows.append(row)
            current_tokens += min(row_tokens, token_budget)
        if after_id <= 0:
            break
    if current_rows:
        yield current_rows


async def _hydrate_digest_posts(rows: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    posts: List[Dict[str, object]] = []
    for row in rows:
        channel_id = str(row.get("channel_id") or "")
        source_name = normalize_space(str(row.get("source_name") or ""))
        if not source_name and channel_id:
            source_name = await _source_title_from_channel_id(channel_id)
        posts.append(
            {
                "id": int(row.get("id") or 0),
                "channel_id": channel_id,
                "message_id": int(row.get("message_id") or 0),
                "source_name": source_name,
                "raw_text": str(row.get("raw_text") or ""),
                "message_link": str(row.get("message_link") or ""),
                "timestamp": int(row.get("timestamp") or 0),
            }
        )
    return posts


def _digest_window_label(window_start_ts: int, window_end_ts: int) -> str:
    start_label = datetime.fromtimestamp(max(0, window_start_ts)).strftime("%H:%M")
    end_label = datetime.fromtimestamp(max(0, window_end_ts)).strftime("%H:%M")
    return f"{start_label}-{end_label}"


def _rolling_digest_title(window_start_ts: int, window_end_ts: int, *, part_index: int, part_count: int) -> str:
    base = f"30-Minute Digest ({_digest_window_label(window_start_ts, window_end_ts)})"
    if part_count > 1 and part_index > 1:
        return f"{base} • Part {part_index}/{part_count}"
    return base


def _daily_digest_title(window_hours: int, *, part_index: int, part_count: int) -> str:
    base = f"24h Digest (last {window_hours}h)"
    if part_count > 1 and part_index > 1:
        return f"{base} • Part {part_index}/{part_count}"
    return base


async def _send_digest_message_sequence(messages: Sequence[str]) -> object | None:
    first_ref = None
    delay = _digest_send_delay_seconds()
    for idx, message in enumerate(messages):
        ref = await _send_digest_message(message)
        if first_ref is None:
            first_ref = ref
        if idx < len(messages) - 1 and delay > 0:
            await asyncio.sleep(delay)
    return first_ref


def _effective_interval_seconds() -> int:
    return _digest_interval_seconds()


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


def _digest_pin_hourly_enabled() -> bool:
    return _bool_flag(getattr(config, "DIGEST_PIN_HOURLY", False), False)


def _digest_pin_daily_enabled() -> bool:
    return _bool_flag(getattr(config, "DIGEST_PIN_DAILY", False), False)


def _digest_pin_meta_key(kind: str) -> str:
    normalized = (kind or "").strip().lower()
    return f"digest_pin::{normalized}::message_id"


async def _unpin_digest_message_id(message_id: int) -> None:
    if message_id <= 0:
        return
    if _destination_uses_bot_api():
        await _bot_api_request(
            "unpinChatMessage",
            data={
                "chat_id": _require_bot_destination_chat_id(),
                "message_id": str(message_id),
            },
        )
        return

    tg = _require_client()
    await _call_with_floodwait(
        tg.unpin_message,
        _require_destination_peer(),
        message=message_id,
    )


async def _pin_digest_message_ref(ref: object | None, *, kind: str) -> None:
    enabled = _digest_pin_hourly_enabled() if kind == "hourly" else _digest_pin_daily_enabled()
    if not enabled:
        return

    message_id = _message_ref_id(ref) or 0
    if message_id <= 0:
        return

    meta_key = _digest_pin_meta_key(kind)
    previous_raw = get_meta(meta_key) or ""
    try:
        previous_id = int(previous_raw) if previous_raw else 0
    except Exception:
        previous_id = 0

    try:
        if previous_id > 0 and previous_id != message_id:
            with contextlib.suppress(Exception):
                await _unpin_digest_message_id(previous_id)

        if _destination_uses_bot_api():
            await _bot_api_request(
                "pinChatMessage",
                data={
                    "chat_id": _require_bot_destination_chat_id(),
                    "message_id": str(message_id),
                    "disable_notification": "true",
                },
            )
        else:
            tg = _require_client()
            await _call_with_floodwait(
                tg.pin_message,
                _require_destination_peer(),
                message=message_id,
                notify=False,
            )
        set_meta(meta_key, str(message_id))
    except Exception:
        LOGGER.exception("Failed rotating %s digest pin.", kind)


async def _send_digest_message(text: str) -> object | None:
    chunks = split_html_chunks(text, max_chars=_digest_send_chunk_size())
    delay = _digest_send_delay_seconds()
    first_ref = None
    for idx, chunk in enumerate(chunks):
        ref = await _send_text_with_ref(chunk)
        if first_ref is None:
            first_ref = ref
        if idx < len(chunks) - 1 and delay > 0:
            await asyncio.sleep(delay)
    return first_ref


async def _build_window_digest_messages(
    row_loader,
    *,
    total_updates: int,
    interval_minutes: int,
    title_builder,
    context: Dict[str, object],
) -> Tuple[List[str], Dict[str, object]]:
    token_budget = _digest_input_token_budget()
    page_size = _digest_window_page_size()
    logical_part_count = sum(
        1
        for _ in _iter_digest_row_groups(
            row_loader,
            token_budget=token_budget,
            page_size=page_size,
        )
    )

    if logical_part_count <= 0:
        quiet_body = quiet_period_message(interval_minutes)
        quiet_body = await _run_post_processors(
            quiet_body,
            {
                **context,
                "posts": [],
                "part_index": 1,
                "part_count": 1,
                "copy_origin": "fallback",
                "fallback_reason": "",
            },
        )
        return [
            _format_digest_message(
                digest_body=quiet_body,
                total_updates=total_updates,
                sources=[],
                title=title_builder(1, 1),
                interval_minutes=interval_minutes,
            )
        ], {
            "part_count": 1,
            "quiet": True,
            "response_chars": len(quiet_body),
            "ai_parts": 0,
            "fallback_parts": 1,
            "major_block_count": 0,
            "timeline_item_count": 0,
            "noise_stripped_count": 0,
            "translation_applied_count": 0,
            "citation_stripped_count": 0,
            "duplicate_collapsed_count": 0,
        }

    rendered_bodies: List[str] = []
    total_chars = 0
    ai_parts = 0
    fallback_parts = 0
    major_block_count = 0
    timeline_item_count = 0
    noise_stripped_count = 0
    translation_applied_count = 0
    citation_stripped_count = 0
    duplicate_collapsed_count = 0
    max_body_chars = max(1200, _digest_send_chunk_size() - 220)

    for part_index, row_group in enumerate(
        _iter_digest_row_groups(
            row_loader,
            token_budget=token_budget,
            page_size=page_size,
        ),
        start=1,
    ):
        posts = await _hydrate_digest_posts(row_group)
        result = await create_digest_summary_result(
            posts,
            _require_auth_manager(),
            interval_minutes=interval_minutes,
        )
        digest_body = result.html.strip() or quiet_period_message(interval_minutes)
        digest_body = await _run_post_processors(
            digest_body,
            {
                **context,
                "posts": posts,
                "part_index": part_index,
                "part_count": logical_part_count,
                "copy_origin": result.copy_origin,
                "fallback_reason": result.fallback_reason,
            },
        )
        rendered_bodies.extend(
            _split_digest_body_blocks(
                digest_body,
                max_chars=max_body_chars,
            )
        )
        total_chars += len(digest_body)
        if result.copy_origin == "ai":
            ai_parts += 1
        else:
            fallback_parts += 1
        major_block_count += int(result.major_block_count or 0)
        timeline_item_count += int(result.timeline_item_count or 0)
        noise_stripped_count += int(result.noise_stripped_count or 0)
        translation_applied_count += int(result.translation_applied_count or 0)
        citation_stripped_count += int(result.citation_stripped_count or 0)
        duplicate_collapsed_count += int(result.duplicate_collapsed_count or 0)

    if not rendered_bodies:
        rendered_bodies = [quiet_period_message(interval_minutes)]

    part_count = len(rendered_bodies)
    messages: List[str] = []
    for part_index, digest_body in enumerate(rendered_bodies, start=1):
        messages.append(
            _format_digest_message(
                digest_body=digest_body,
                total_updates=total_updates,
                sources=[],
                title=title_builder(part_index, part_count),
                interval_minutes=interval_minutes,
            )
        )

    return messages, {
        "part_count": part_count,
        "quiet": False,
        "response_chars": total_chars,
        "ai_parts": ai_parts,
        "fallback_parts": fallback_parts,
        "major_block_count": major_block_count,
        "timeline_item_count": timeline_item_count,
        "noise_stripped_count": noise_stripped_count,
        "translation_applied_count": translation_applied_count,
        "citation_stripped_count": citation_stripped_count,
        "duplicate_collapsed_count": duplicate_collapsed_count,
    }


async def _flush_digest_queue_once() -> bool:
    global digest_retry_backoff_seconds

    async with digest_loop_lock:
        active_batch_id, active_window_end_ts, active_rows = load_active_digest_window_claim()
        batch_id = active_batch_id
        window_end_ts = int(active_window_end_ts or 0)
        claimed_rows = int(active_rows or 0)

        if batch_id and window_end_ts <= 0:
            restore_digest_window(batch_id)
            batch_id = ""
            claimed_rows = 0

        if not batch_id:
            due_window_end_ts = _next_digest_window_end_to_process()
            if due_window_end_ts is None:
                return False
            window_end_ts = int(due_window_end_ts)
            batch_id, claimed_rows = claim_digest_window(window_end_ts)

        interval_minutes = max(1, _digest_interval_seconds() // 60)
        window_start_ts = max(0, window_end_ts - _digest_interval_seconds())
        pending_before = count_pending() + claimed_rows
        log_structured(
            LOGGER,
            "digest_window_claimed",
            batch_id=batch_id,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            claimed=claimed_rows,
            pending_before=pending_before,
            quota_health=get_quota_health(),
        )

        try:
            row_loader = lambda *, after_id=0, limit=200: load_batch_rows_page(
                batch_id,
                after_id=after_id,
                limit=limit,
            )
            messages, stats = await _build_window_digest_messages(
                row_loader,
                total_updates=claimed_rows,
                interval_minutes=interval_minutes,
                title_builder=lambda part_index, part_count: _rolling_digest_title(
                    window_start_ts,
                    window_end_ts,
                    part_index=part_index,
                    part_count=part_count,
                ),
                context={
                    "batch_id": batch_id,
                    "window_start_ts": window_start_ts,
                    "window_end_ts": window_end_ts,
                    "pending_before": pending_before,
                    "surface": "rolling_digest",
                },
            )
            sent_ref = await _send_digest_message_sequence(messages)
            await _pin_digest_message_ref(sent_ref, kind="hourly")

            acked = ack_digest_window(batch_id, window_end_ts=window_end_ts)
            set_last_digest_timestamp(int(time.time()))
            digest_retry_backoff_seconds = 0

            log_structured(
                LOGGER,
                "digest_sent",
                batch_id=batch_id,
                window_start_ts=window_start_ts,
                window_end_ts=window_end_ts,
                rows=claimed_rows,
                acked=acked,
                parts=int(stats.get("part_count") or 1),
                quiet=bool(stats.get("quiet")),
                pending_after=count_pending(),
                response_chars=int(stats.get("response_chars") or 0),
                ai_parts=int(stats.get("ai_parts") or 0),
                fallback_parts=int(stats.get("fallback_parts") or 0),
                major_block_count=int(stats.get("major_block_count") or 0),
                timeline_item_count=int(stats.get("timeline_item_count") or 0),
                noise_stripped_count=int(stats.get("noise_stripped_count") or 0),
                translation_applied_count=int(stats.get("translation_applied_count") or 0),
                citation_stripped_count=int(stats.get("citation_stripped_count") or 0),
                duplicate_collapsed_count=int(stats.get("duplicate_collapsed_count") or 0),
                digest_mode="strict_digest_only",
            )
            return True
        except asyncio.CancelledError:
            restore_digest_window(batch_id)
            raise
        except Exception as exc:
            restored = restore_digest_window(batch_id)
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
                window_end_ts=window_end_ts,
                restored=restored,
                backoff_seconds=digest_retry_backoff_seconds,
                error=str(exc),
            )
            LOGGER.exception("Digest generation/sending failed.")
            return False


async def _send_daily_archive_digest_once() -> None:
    async with digest_loop_lock:
        window_hours = _digest_daily_window_hours()
        interval_minutes = max(1, window_hours * 60)
        now_ts = int(time.time())
        since_ts = max(0, now_ts - window_hours * 3600)
        total_rows = count_archive_window_rows(since_ts, now_ts)

        log_structured(
            LOGGER,
            "daily_digest_batch_loaded",
            window_hours=window_hours,
            selected=total_rows,
            mode="full_window_archive",
        )

        try:
            row_loader = lambda *, after_id=0, limit=200: load_archive_window_page(
                since_ts,
                now_ts,
                after_id=after_id,
                limit=limit,
            )
            messages, stats = await _build_window_digest_messages(
                row_loader,
                total_updates=total_rows,
                interval_minutes=interval_minutes,
                title_builder=lambda part_index, part_count: _daily_digest_title(
                    window_hours,
                    part_index=part_index,
                    part_count=part_count,
                ),
                context={
                    "window_start_ts": since_ts,
                    "window_end_ts": now_ts,
                    "surface": "daily_digest",
                },
            )
            sent_ref = await _send_digest_message_sequence(messages)
            await _pin_digest_message_ref(sent_ref, kind="daily")

            set_last_digest_timestamp(int(time.time()))
            log_structured(
                LOGGER,
                "daily_digest_sent",
                window_hours=window_hours,
                rows=total_rows,
                parts=int(stats.get("part_count") or 1),
                quiet=bool(stats.get("quiet")),
                response_chars=int(stats.get("response_chars") or 0),
                ai_parts=int(stats.get("ai_parts") or 0),
                fallback_parts=int(stats.get("fallback_parts") or 0),
                major_block_count=int(stats.get("major_block_count") or 0),
                timeline_item_count=int(stats.get("timeline_item_count") or 0),
                noise_stripped_count=int(stats.get("noise_stripped_count") or 0),
                translation_applied_count=int(stats.get("translation_applied_count") or 0),
                citation_stripped_count=int(stats.get("citation_stripped_count") or 0),
                duplicate_collapsed_count=int(stats.get("duplicate_collapsed_count") or 0),
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
    due_window_end_ts = _next_digest_window_end_to_process()
    if due_window_end_ts is not None:
        return 0
    next_boundary_ts = _next_digest_boundary_timestamp()
    return max(1, next_boundary_ts - int(time.time()))


async def run_digest_scheduler() -> None:
    global digest_next_run_ts

    log_structured(
        LOGGER,
        "digest_scheduler_start",
        mode="strict_digest_only_30m",
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
            if delay > 0:
                await asyncio.sleep(delay)
            processed = await _flush_digest_queue_once()
            if delay <= 0 and not processed:
                await asyncio.sleep(1)
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
            inserted = await _enqueue_inbound_messages(
                items,
                kind="album",
                grouped_id=int(key[1] or 0),
            )
            if not inserted:
                LOGGER.debug(
                    "Skipped duplicate inbound album enqueue channel_id=%s grouped_id=%s",
                    key[0],
                    key[1],
                )
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
        except (ValueError, TypeError):
            pass
        except Exception as exc:
            LOGGER.debug("Failed resolving peer by chat id %s: %s", chat_id, exc, exc_info=True)

    try:
        entity = await _call_with_floodwait(tg.get_entity, chat_obj)
        return utils.get_peer_id(entity)
    except (ValueError, TypeError):
        return None
    except Exception as exc:
        LOGGER.debug("Failed resolving peer by entity: %s", exc, exc_info=True)
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
                _register_source_aliases(
                    str(peer_id),
                    getattr(chat, "title", None),
                    getattr(chat, "username", None),
                )

    # Additional manual sources (username/public/private links).
    for source in _manual_source_entries():
        _register_source_aliases(source, source, _extract_public_username(source))
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
    try:
        inbound_counts = load_inbound_job_counts()
    except Exception:
        inbound_counts = {}
    try:
        inbound_oldest = oldest_pending_inbound_job_age_seconds()
    except Exception:
        inbound_oldest = None
    try:
        ai_cache_stats = get_filter_decision_cache_stats()
    except Exception:
        ai_cache_stats = {"hits": 0.0, "misses": 0.0, "hit_rate": 0.0}
    try:
        ontology_health = get_ontology_health_snapshot()
    except Exception:
        ontology_health = {
            "version": 0,
            "compiled_category_count": 0,
            "fallback_label_rate": 0.0,
            "unmatched_live_event_rate": 0.0,
            "top_unmatched_event_tokens": [],
        }
    try:
        ai_cache_entries = count_ai_decision_cache_entries()
    except Exception:
        ai_cache_entries = 0
    try:
        recent_failures = load_recent_inbound_job_failures(limit=5)
    except Exception:
        recent_failures = []
    try:
        active_story_clusters = count_active_breaking_story_clusters(
            since_ts=int(now) - _breaking_story_window_seconds()
        )
    except Exception:
        active_story_clusters = 0
    try:
        recent_story_decisions = load_breaking_story_decision_counts(
            since_ts=int(now) - 3600
        )
    except Exception:
        recent_story_decisions = {}
    context_stats = _delivery_context_stats_snapshot()
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
        "breaking_story_clusters": {
            "enabled": _is_breaking_story_clusters_enabled(),
            "active": active_story_clusters,
            "recent_decisions_1h": recent_story_decisions,
        },
        "ontology": ontology_health,
        "delivery_context": context_stats,
        "query_mode": _is_query_mode_enabled(),
        "query_mode_available": _is_query_runtime_available(),
        "html_formatting": _is_html_formatting_enabled(),
        "breaking_sla_seconds": int(getattr(config, "BREAKING_SLA_SECONDS", 15) or 15),
        "auth": {
            "mode_configured": auth_startup_mode_configured,
            "mode_effective": auth_startup_mode_effective,
            "ready": auth_ready,
            "degraded": auth_degraded,
            "status": _auth_status_summary(),
            "failure_reason": auth_failure_reason or None,
            "features_disabled": list(auth_features_disabled),
            "fix_commands": _auth_fix_commands(),
            "startup_repair_status": startup_auth_repair_status,
            "startup_repair_message": startup_auth_repair_message or None,
        },
        "pipeline_workers": _pipeline_worker_targets(),
        "pipeline_queue_depth": inbound_counts,
        "pipeline_oldest_pending_seconds": inbound_oldest,
        "pipeline_stage_latencies_ms": _pipeline_stage_latency_snapshot(),
        "pipeline_recent_failures": recent_failures,
        "ai_decision_cache": {
            "entries": ai_cache_entries,
            "hits": ai_cache_stats.get("hits", 0.0),
            "misses": ai_cache_stats.get("misses", 0.0),
            "hit_rate": ai_cache_stats.get("hit_rate", 0.0),
        },
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
    auth_status = _auth_status_summary()
    auth_reason = sanitize_telegram_html(_trim_runtime_reason(auth_failure_reason or ""))
    disabled_features = ", ".join(auth_features_disabled) if auth_features_disabled else "none"
    context_stats = _delivery_context_stats_snapshot()
    query_state = "on" if _is_query_mode_enabled() else "off"
    if _is_query_mode_enabled() and not _is_query_runtime_available():
        query_state = "degraded"

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
        f"• Dynamic context: <code>{context_stats.get('context_generated', 0)}</code> shown, "
        f"<code>{context_stats.get('context_omitted_no_anchor', 0)}</code> no anchor, "
        f"<code>{context_stats.get('context_omitted_no_delta', 0)}</code> no delta, "
        f"<code>{context_stats.get('context_rejected_generic', 0)}</code> rejected<br>"
        f"• Quota health: <code>{quota.get('status', 'unknown')}</code><br>"
        f"• Recent 429 (1h): <code>{quota.get('recent_429_count', 0)}</code> / "
        f"threshold <code>{threshold}</code>"
    )


def _digest_auth_status_suffix() -> str:
    auth_status = sanitize_telegram_html(_auth_status_summary())
    disabled_features = ", ".join(auth_features_disabled) if auth_features_disabled else "none"
    reason = sanitize_telegram_html(_trim_runtime_reason(auth_failure_reason or ""))
    repair_status = sanitize_telegram_html(startup_auth_repair_status)
    repair_message = sanitize_telegram_html(_trim_runtime_reason(startup_auth_repair_message or ""))
    query_state = "on" if _is_query_runtime_available() else ("degraded" if _is_query_mode_enabled() else "off")

    lines = [
        "",
        "<b>Auth Status</b>",
        (
            f"Configured/effective: <code>{sanitize_telegram_html(auth_startup_mode_configured)} -> "
            f"{sanitize_telegram_html(auth_startup_mode_effective)}</code>"
        ),
        f"State: <code>{auth_status}</code>",
        f"Startup repair: <code>{repair_status}</code>",
        f"Query mode: <code>{sanitize_telegram_html(query_state)}</code>",
        f"Disabled features: <code>{sanitize_telegram_html(disabled_features)}</code>",
    ]
    if repair_message:
        lines.append(f"Repair detail: <code>{repair_message}</code>")
    if reason:
        lines.append(f"Reason: <code>{reason}</code>")
    return "<br>".join(lines)


async def _on_digest_status_command(event: events.NewMessage.Event) -> None:
    try:
        status_text = f"{_digest_status_text()}{_digest_auth_status_suffix()}"
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
    raw_lines = re.split(r"(?i)<br\s*/?>", value)
    cleaned_lines: list[str] = []

    for raw_line in raw_lines:
        line = raw_line
        if not normalize_space(strip_telegram_html(line)):
            continue
        for _ in range(4):
            previous = line
            line = re.sub(r'\s*<a href="[^"]+">[^<]*</a>', "", line, flags=re.IGNORECASE)
            line = re.sub(r"\s*<i>\[[^<>\]]+\]</i>", "", line, flags=re.IGNORECASE)
            line = re.sub(r"\s*\[[^\[\]<>]{2,60}\]", "", line)
            line = re.sub(r"\bRead more\b", "", line, flags=re.IGNORECASE)
            line = _CAPTION_TELEGRAM_LINK_RE.sub("", line)
            line = _rewrite_known_source_alias_attribution(line)
            line = _strip_known_source_aliases(line)
            line = _CAPTION_HANDLE_RE.sub("", line)
            line = re.sub(r"(?i)^\s*(?:fwd from|forwarded from)\b[^<]{0,120}", "", line)
            line = re.sub(
                r"(?i)\b(?:original msg|subscribe|follow\s+us|join(?: us| our channel)?)\b.*$",
                "",
                line,
            )
            line = _CAPTION_FOLLOW_PROMO_ONLY_RE.sub("", line)
            line = re.sub(
                r"(?i)\b(?:channel|channel username|username)\s*[:\-–—|]+\s*",
                "",
                line,
            )
            line = re.sub(r"\s+([,.;:!?])", r"\1", line)
            line = re.sub(r"<(b|i|u|s|code|pre|blockquote|tg-spoiler)>\s*</\1>", "", line, flags=re.IGNORECASE)
            line = normalize_space(line.strip(" ,;:-|/[](){}"))
            if line == previous:
                break
        plain_line = normalize_space(strip_telegram_html(line).strip(" ,;:-|/[](){}"))
        if not plain_line:
            continue
        if _is_known_source_alias_line(line):
            continue
        cleaned_lines.append(line)

    value = "<br>".join(cleaned_lines)
    value = re.sub(r"(?:<br>\s*){3,}", "<br><br>", value, flags=re.IGNORECASE)
    value = re.sub(r"[ \t]{2,}", " ", value)
    return sanitize_telegram_html(value.strip())


_QUERY_REPLY_MAX_CHARS = 3600


def _normalize_query_final_answer(text: str) -> str:
    cleaned = (str(text or "")).rstrip()
    cleaned = re.sub(r"\s+\.\.\.$", "", cleaned).strip()
    if _is_html_formatting_enabled():
        cleaned = sanitize_telegram_html(cleaned)
    if cleaned:
        return cleaned
    return (
        "<b>🟢 No relevant information found.</b>"
        if _is_html_formatting_enabled()
        else "No relevant information found."
    )


def _is_query_bullet_block(text: str) -> bool:
    plain = normalize_space(strip_telegram_html(str(text or "")))
    if not plain:
        return False
    return bool(re.match(r"^(?:[•▪◦●‣-]\s+|\d+\.\s+)", plain))


def _render_query_block(text: str, *, html_mode: bool) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    if html_mode:
        return sanitize_telegram_html(value)
    raw = re.sub(r"(?i)<br\s*/?>", "\n", value)
    return raw.strip()


def _split_query_bullet_block(
    text: str,
    *,
    max_chars: int,
    html_mode: bool,
) -> List[str]:
    plain = normalize_space(strip_telegram_html(str(text or "")))
    if not plain:
        return []

    match = re.match(r"^(?P<prefix>(?:[•▪◦●‣-]|\d+\.)\s+(?:\S+\s+)?)?(?P<body>.+)$", plain)
    prefix = normalize_space(str(match.group("prefix") or "")) if match else ""
    body = normalize_space(str(match.group("body") or plain)) if match else plain
    prefix_with_space = f"{prefix} " if prefix else ""
    body_limit = max(60, max_chars - len(prefix_with_space) - 16)

    sentences = [
        normalize_space(part)
        for part in re.split(r"(?<=[.!?;])\s+", body)
        if normalize_space(part)
    ]

    if len(sentences) <= 1:
        pieces = split_markdown_chunks(body, max_chars=body_limit)
        return [f"{prefix_with_space}{piece}".strip() for piece in pieces if piece.strip()]

    segments: List[str] = []
    current = ""
    for sentence in sentences:
        candidate = f"{current} {sentence}".strip() if current else sentence
        rendered_candidate = _render_query_block(
            f"{prefix_with_space}{candidate}".strip(),
            html_mode=html_mode,
        )
        if current and len(rendered_candidate) > max_chars:
            segments.append(current)
            current = ""
            if len(_render_query_block(f"{prefix_with_space}{sentence}".strip(), html_mode=html_mode)) > max_chars:
                pieces = split_markdown_chunks(sentence, max_chars=body_limit)
                segments.extend(piece for piece in pieces if piece.strip())
            else:
                current = sentence
            continue
        if len(rendered_candidate) > max_chars:
            pieces = split_markdown_chunks(sentence, max_chars=body_limit)
            segments.extend(piece for piece in pieces if piece.strip())
            continue
        current = candidate

    if current:
        segments.append(current)

    return [f"{prefix_with_space}{segment}".strip() for segment in segments if segment.strip()]


def _join_query_rendered_blocks(
    blocks: Sequence[Tuple[str, bool]],
    *,
    html_mode: bool,
) -> str:
    pieces: List[str] = []
    prev_is_bullet: bool | None = None
    single_sep = "<br>" if html_mode else "\n"
    double_sep = "<br><br>" if html_mode else "\n\n"

    for text, is_bullet in blocks:
        cleaned = str(text or "").strip()
        if not cleaned:
            continue
        if pieces:
            pieces.append(single_sep if (prev_is_bullet or is_bullet) else double_sep)
        pieces.append(cleaned)
        prev_is_bullet = is_bullet
    return "".join(pieces).strip()


def _prepare_query_answer_chunks(text: str, *, max_chars: int | None = None) -> List[str]:
    resolved_max = max(160, min(int(max_chars or _QUERY_REPLY_MAX_CHARS), 3900))
    html_mode = _is_html_formatting_enabled()
    cleaned = _normalize_query_final_answer(text)
    raw = re.sub(r"(?i)<br\s*/?>", "\n", cleaned).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return [cleaned]

    blocks: List[str] = []
    current: List[str] = []
    for line in raw.split("\n"):
        stripped = line.strip()
        if not stripped:
            if current:
                blocks.append("\n".join(current))
                current = []
            continue
        if _is_query_bullet_block(stripped):
            if current:
                blocks.append("\n".join(current))
                current = []
            blocks.append(stripped)
            continue
        current.append(stripped)
    if current:
        blocks.append("\n".join(current))
    if not blocks:
        return [cleaned]

    rendered_blocks: List[Tuple[str, bool]] = []
    for block in blocks:
        is_bullet = _is_query_bullet_block(block)
        rendered = _render_query_block(block, html_mode=html_mode)
        if not rendered:
            continue
        if len(rendered) <= resolved_max:
            rendered_blocks.append((rendered, is_bullet))
            continue
        if is_bullet:
            split_blocks = _split_query_bullet_block(block, max_chars=resolved_max, html_mode=html_mode)
            for split_block in split_blocks:
                rendered_split = _render_query_block(split_block, html_mode=html_mode)
                if rendered_split and len(rendered_split) <= resolved_max:
                    rendered_blocks.append((rendered_split, True))
                elif rendered_split:
                    fallback_parts = (
                        split_html_chunks(rendered_split, max_chars=resolved_max)
                        if html_mode
                        else split_markdown_chunks(rendered_split, max_chars=resolved_max)
                    )
                    rendered_blocks.extend(
                        (part, True)
                        for part in fallback_parts
                        if str(part or "").strip()
                    )
            continue

        fallback_parts = (
            split_html_chunks(rendered, max_chars=resolved_max)
            if html_mode
            else split_markdown_chunks(rendered, max_chars=resolved_max)
        )
        rendered_blocks.extend(
            (part, is_bullet)
            for part in fallback_parts
            if str(part or "").strip()
        )

    if not rendered_blocks:
        return [cleaned]

    chunks: List[str] = []
    current_blocks: List[Tuple[str, bool]] = []
    for block in rendered_blocks:
        candidate_blocks = current_blocks + [block]
        candidate_text = _join_query_rendered_blocks(candidate_blocks, html_mode=html_mode)
        if current_blocks and len(candidate_text) > resolved_max:
            chunks.append(_join_query_rendered_blocks(current_blocks, html_mode=html_mode))
            current_blocks = [block]
        else:
            current_blocks = candidate_blocks
    if current_blocks:
        chunks.append(_join_query_rendered_blocks(current_blocks, html_mode=html_mode))

    return [chunk for chunk in chunks if str(chunk or "").strip()] or [cleaned]


async def _deliver_query_final_answer(
    event: events.NewMessage.Event,
    *,
    progress_message: Message | Dict[str, object],
    final_text: str,
    prefer_bot_identity: bool = False,
    bot_chat_id: int | None = None,
    root_reply_to: int | None = None,
    streamer: LiveTelegramStreamer | None = None,
    max_chars: int | None = None,
) -> object:
    chunks = _prepare_query_answer_chunks(final_text, max_chars=max_chars)
    if streamer is not None:
        return await streamer.finalize(final_text, chunks=chunks)

    first_ref = await _safe_reply_markdown(
        event,
        chunks[0],
        edit_message=progress_message,
        prefer_bot_identity=prefer_bot_identity,
        bot_chat_id=bot_chat_id,
    )
    reply_to = _message_ref_id(first_ref if first_ref is not None else progress_message) or root_reply_to
    for extra in chunks[1:]:
        sent_ref = await _safe_reply_markdown(
            event,
            extra,
            reply_to=reply_to,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )
        reply_to = _message_ref_id(sent_ref) or reply_to

    normalized_final = _normalize_query_final_answer(final_text)
    plain_final = strip_telegram_html(normalized_final) if _is_html_formatting_enabled() else normalized_final
    estimated_tokens = estimate_tokens_rough(plain_final)
    return SimpleNamespace(
        total_chars=len(plain_final),
        estimated_tokens=estimated_tokens,
        duration_seconds=0.0,
        tokens_per_second=0.0,
        edit_count=1,
        message_count=len(chunks),
    )


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
        placeholder_text=_query_writing_status(),
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
        answer = QUERY_NO_MATCH_TEXT
    answer = _strip_query_answer_citations(answer)
    if final_suffix_html.strip() and "no relevant information found" not in strip_telegram_html(answer).lower():
        answer = f"{answer.strip()}<br><br>{final_suffix_html.strip()}"

    stats = await _deliver_query_final_answer(
        event,
        progress_message=progress_message,
        final_text=answer,
        prefer_bot_identity=prefer_bot_identity,
        bot_chat_id=bot_chat_id,
        root_reply_to=root_reply_to,
        streamer=streamer,
    )
    return answer, stats

async def _search_recent_news_web_bounded(*args, **kwargs) -> list[dict[str, object]]:
    if pipeline_query_web_semaphore is None:
        return await search_recent_news_web(*args, **kwargs)
    async with pipeline_query_web_semaphore:
        return await search_recent_news_web(*args, **kwargs)


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
    if not _is_query_runtime_available():
        await _safe_reply_markdown(
            event_ref,  # type: ignore[arg-type]
            _query_mode_temporarily_unavailable_text(),
            reply_to=reply_to,
            prefer_bot_identity=prefer_bot_identity,
            bot_chat_id=bot_chat_id,
        )
        return

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

    plan = build_query_plan(text, default_hours=_query_default_hours_back())
    parsed_hours = plan.hours_back
    primary_hours, expanded_hours = _query_window_strategy(
        requested_hours=parsed_hours,
        explicit_time_filter=bool(getattr(plan, "explicit_time_filter", False)),
    )

    progress = await _safe_reply_markdown(
        event_ref,  # type: ignore[arg-type]
        _query_search_status(_is_query_web_fallback_enabled()),
        reply_to=reply_to,
        prefer_bot_identity=prefer_bot_identity,
        bot_chat_id=bot_chat_id,
    )
    if progress is None:
        return

    try:
        cleaned_query = plan.cleaned_query
        effective_query = plan.original_query or text
        broad_query = plan.broad_query
        query_keywords = list(plan.keywords)
        query_numbers = list(plan.numbers)
        high_risk_query = _is_high_risk_news_query(effective_query)
        context_limit = _query_max_messages() * (2 if broad_query or len(query_keywords) <= 2 else 1)
        active_hours = primary_hours
        expanded_window_used = False

        telegram_task = asyncio.create_task(
            search_recent_messages(
                _require_client(),
                monitored_source_chat_ids,
                cleaned_query,
                max_messages=context_limit,
                default_hours_back=primary_hours,
                logger=LOGGER,
            )
        )

        telegram_results = await telegram_task

        if (
            expanded_hours > primary_hours
            and _query_needs_expanded_window(
                effective_query,
                telegram_results,
                broad_query=broad_query,
            )
        ):
            await _safe_reply_markdown(
                event_ref,  # type: ignore[arg-type]
                _query_expand_status(),
                edit_message=progress,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
            )
            expanded_results = await search_recent_messages(
                _require_client(),
                monitored_source_chat_ids,
                cleaned_query,
                max_messages=context_limit,
                default_hours_back=expanded_hours,
                logger=LOGGER,
            )
            telegram_results = _merge_query_context(
                telegram_results,
                expanded_results,
                limit=max(context_limit, 40),
            )
            active_hours = expanded_hours
            expanded_window_used = True

        queue_results: list[dict[str, object]] = []
        archive_results: list[dict[str, object]] = []
        if broad_query or len(telegram_results) < _query_web_min_telegram_results():
            queue_results = _load_queue_query_context(
                query_text=effective_query,
                hours_back=active_hours,
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
                hours_back=active_hours,
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
        ran_web_search = False

        should_run_web_search = bool(_is_query_web_fallback_enabled())

        if should_run_web_search:
            ran_web_search = True
            await _safe_reply_markdown(
                event_ref,  # type: ignore[arg-type]
                _query_crosscheck_status(high_risk=high_risk_query),
                edit_message=progress,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
            )
            web_results = await _search_recent_news_web_bounded(
                cleaned_query,
                hours_back=active_hours,
                max_results=_query_web_max_results(),
                allowed_domains=_query_web_allowed_domains(),
                require_recent=_query_web_require_recent(),
                logger=LOGGER,
            )

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
            _query_analysis_status(len(telegram_results), len(web_results)),
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
                digest_hours_back=active_hours,
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
                    interval_minutes=max(1, active_hours * 60),
                )
                answer = _wrap_query_digest_answer(answer, hours_back=active_hours)
            else:
                answer = await generate_answer_from_context(
                    query=effective_query,
                    context_messages=results,
                    auth_manager=_require_auth_manager(),
                    conversation_history=history,
                )
            if not answer.strip():
                answer = QUERY_NO_MATCH_TEXT
            answer = _strip_query_answer_citations(answer)
            if (
                evidence_split_section
                and "no relevant information found" not in strip_telegram_html(answer).lower()
            ):
                answer = f"{answer.strip()}<br><br>{evidence_split_section}"
            stream_stats = await _deliver_query_final_answer(
                event_ref,  # type: ignore[arg-type]
                progress_message=progress,
                final_text=answer,
                prefer_bot_identity=prefer_bot_identity,
                bot_chat_id=bot_chat_id,
                root_reply_to=reply_to,
            )

        _append_query_history(sender_id, text, answer)
        log_structured(
            LOGGER,
            "query_answered",
            sender_id=sender_id,
            chat_id=chat_id,
            query_length=len(text),
            hours_back=active_hours,
            requested_hours_back=parsed_hours,
            messages_found=len(results),
            telegram_messages_found=len(telegram_results),
            queue_messages_found=len(queue_results),
            archive_messages_found=len(archive_results),
            web_messages_found=len(web_results),
            web_fallback_used=web_fallback_used,
            web_search_ran=ran_web_search,
            expanded_window_used=expanded_window_used,
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
            QUERY_NO_MATCH_TEXT,
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
            # Limit concurrent media duplicate checks to prevent OOM on constrained hosts.
            async with (pipeline_media_semaphore or contextlib.nullcontext()):
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

        inserted = await _enqueue_inbound_messages([msg], kind="single")
        if not inserted:
            LOGGER.debug(
                "Skipped duplicate inbound enqueue channel_id=%s message_id=%s",
                channel_id,
                msg.id,
            )
    except Exception:
        LOGGER.exception(
            "Failed processing message channel_id=%s message_id=%s",
            channel_id,
            msg.id,
        )


async def _shutdown_client() -> None:
    global digest_scheduler_task, daily_digest_scheduler_task, queue_clear_scheduler_task
    global query_bot_poll_task
    global web_status_server, pipeline_sentence_transformer_warm_task
    _log_memory_snapshot("shutdown_started", phase=startup_phase)
    configure_duplicate_runtime(None)
    breaking_delivery_refs.clear()
    breaking_story_ref_cache.clear()
    breaking_topic_threads.clear()

    await _stop_pipeline_workers()

    if pipeline_sentence_transformer_warm_task and not pipeline_sentence_transformer_warm_task.done():
        pipeline_sentence_transformer_warm_task.cancel()
        await asyncio.gather(pipeline_sentence_transformer_warm_task, return_exceptions=True)
    pipeline_sentence_transformer_warm_task = None

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
        await close_shared_http_clients()
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
    finally:
        await close_shared_http_clients()
        _log_memory_snapshot("shutdown_completed", phase=startup_phase)


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
        digest_pin_hourly=_digest_pin_hourly_enabled(),
        digest_pin_daily=_digest_pin_daily_enabled(),
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
        "Startup health: mode=%s pending=%s inflight=%s last_digest=%s dupe=%s severity=%s query=%s query_web=%s html=%s premium_emoji=%s map=%s humanized=%s prob=%.2f topic_threads=%s interval=%sm daily=%s queue_clear=%s scope=%s pin_hourly=%s pin_daily=%s web=%s@%s:%s",
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
        _digest_pin_hourly_enabled(),
        _digest_pin_daily_enabled(),
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
        _configure_runtime_logging()
        startup_ready = False
        startup_error = ""
        _set_startup_phase("booting", reason="startup_entered")
        _print_cli_banner()
        if _is_web_server_enabled():
            try:
                web_status_server = WebStatusServer(
                    host=_web_server_host(),
                    port=_web_server_port(),
                    get_status=_web_status_payload,
                    get_recent_events=_recent_runtime_activity,
                    logger=LOGGER,
                )
                web_status_server.start()
                _log_memory_snapshot(
                    "web_status_server_started",
                    phase=startup_phase,
                    web_server_host=_web_server_host(),
                    web_server_port=_web_server_port(),
                )
            except Exception:
                LOGGER.exception("Failed starting web status server.")
                web_status_server = None

        _set_startup_phase("config", reason="config_validation_started")
        _print_cli_status("•", "Validating configuration...", level="info")
        try:
            _prompt_for_missing_config()
            _validate_config()
            load_news_taxonomy(force_reload=True)
            _log_memory_snapshot("config_validated", phase=startup_phase)
            _print_cli_status("✓", "Configuration validated", level="ok")
        except (RuntimeError, ValueError) as exc:
            startup_error = str(exc)
            startup_ready = False
            _set_startup_phase("error", reason="config_validation_failed")
            _log_memory_snapshot(
                "startup_failure",
                phase=startup_phase,
                failure_reason=startup_error,
                failure_stage="config",
            )
            LOGGER.error("%s", exc)
            _print_cli_status("✗", "Configuration invalid. Fix values and re-run.", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after config validation error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            return

        _set_startup_phase("lock", reason="instance_lock_started")
        _print_cli_status("•", "Checking single-instance lock...", level="info")
        try:
            instance_lock_handle = _acquire_instance_lock()
            _log_memory_snapshot("instance_lock_acquired", phase=startup_phase)
            _print_cli_status("✓", "Instance lock acquired", level="ok")
        except RuntimeError as exc:
            startup_error = str(exc)
            startup_ready = False
            _set_startup_phase("error", reason="instance_lock_failed")
            _log_memory_snapshot(
                "startup_failure",
                phase=startup_phase,
                failure_reason=startup_error,
                failure_stage="lock",
            )
            LOGGER.error("%s", exc)
            _print_cli_status("✗", "Another instance is already running", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after startup lock error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            return

        try:
            _set_startup_phase("initializing", reason="runtime_initialization_started")
            _load_premium_emoji_map()
            _log_memory_snapshot(
                "premium_emoji_map_loaded",
                phase=startup_phase,
                premium_emoji_count=len(premium_emoji_map),
            )
            init_db()
            _log_memory_snapshot("database_initialized", phase=startup_phase)
            await _init_dupe_detector()
            _log_memory_snapshot("dupe_detector_initialized", phase=startup_phase)
            _startup_health_check()
            _log_memory_snapshot("runtime_state_ready", phase=startup_phase)
            _print_cli_status("✓", "Database and runtime state ready", level="ok")

            _set_startup_phase("auth", reason="auth_preparation_started")
            _print_cli_status("•", "Preparing OpenAI auth context...", level="info")
            await _prepare_auth_runtime_for_startup()
            _log_memory_snapshot(
                "auth_runtime_prepared",
                phase=startup_phase,
                auth_ready=auth_ready,
                auth_degraded=auth_degraded,
                auth_mode=auth_startup_mode_effective,
            )
            log_structured(
                LOGGER,
                "auth_startup_state",
                mode_configured=auth_startup_mode_configured,
                mode_effective=auth_startup_mode_effective,
                ready=auth_ready,
                degraded=auth_degraded,
                failure_reason=auth_failure_reason or None,
                features_disabled=list(auth_features_disabled),
                startup_repair_status=startup_auth_repair_status,
                startup_repair_message=startup_auth_repair_message or None,
            )
            if auth_ready and not auth_degraded:
                _print_cli_status("+", "OpenAI auth ready", level="ok")
            else:
                summary = _trim_runtime_reason(auth_failure_reason or "OpenAI auth unavailable.", limit=96)
                _print_cli_status(
                    "!",
                    f"OpenAI auth degraded; disabled: {', '.join(auth_features_disabled) or 'none'} ({summary})",
                    level="warn",
                )

            _set_startup_phase("telegram_login", reason="telegram_session_connect_started")
            _print_cli_status("•", "Connecting Telegram user session...", level="info")
            client = TelegramClient("userbot", config.TELEGRAM_API_ID, config.TELEGRAM_API_HASH)
            await _ensure_user_account_session()
            _log_memory_snapshot("telegram_session_ready", phase=startup_phase)
            _print_cli_status("✓", "Telegram session authorized", level="ok")
            _set_startup_phase("destination_setup", reason="destination_validation_started")
            _print_cli_status("•", "Validating destination...", level="info")
            await _ensure_destination_peer()
            _log_memory_snapshot("destination_ready", phase=startup_phase)
            _print_cli_status("✓", "Destination ready", level="ok")

            _set_startup_phase("source_setup", reason="source_resolution_started")
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
            _log_memory_snapshot(
                "source_resolution_completed",
                phase=startup_phase,
                resolved_source_count=len(resolved_sources),
            )
            _print_cli_status("✓", f"Sources ready ({len(resolved_sources)} total)", level="ok")

            _set_startup_phase("pipeline", reason="pipeline_worker_start_started")
            await _start_pipeline_workers()
            _print_cli_status("✓", "Inbound pipeline workers started", level="ok")

            _set_startup_phase("handlers", reason="handler_registration_started")
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
            _log_memory_snapshot(
                "handlers_registered",
                phase=startup_phase,
                query_mode_enabled=_is_query_mode_enabled(),
                resolved_source_count=len(resolved_sources),
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
                        "Schedulers started (30m rolling + daily + queue clear)",
                        level="ok",
                    )
                else:
                    queue_clear_scheduler_task = None
                    _print_cli_status(
                        "✓",
                        "Schedulers started (30m rolling + daily; queue clear disabled)",
                        level="ok",
                    )

            _set_startup_phase("running", reason="startup_runtime_ready")
            startup_ready = True
            startup_error = ""
            _log_memory_snapshot(
                "runtime_ready",
                phase=startup_phase,
                resolved_source_count=len(resolved_sources),
                started_as_user_id=started_as_user_id,
                digest_mode_enabled=_is_digest_mode_enabled(),
                query_mode_enabled=_is_query_mode_enabled(),
            )
            await _maybe_start_duplicate_backend_warmup()
            try:
                await client.run_until_disconnected()
            except (asyncio.CancelledError, KeyboardInterrupt):
                LOGGER.info("Shutdown requested. Stopping userbot...")
                _print_cli_status("•", "Shutdown requested, stopping...", level="warn")
        except (RuntimeError, ValueError) as exc:
            startup_error = str(exc)
            startup_ready = False
            _set_startup_phase("error", reason="startup_runtime_failed")
            _log_memory_snapshot(
                "startup_failure",
                phase=startup_phase,
                failure_reason=startup_error,
                failure_stage="runtime",
            )
            LOGGER.error("%s", exc)
            _print_cli_status("✗", "Startup failed", level="error")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after startup error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
        except Exception as exc:
            startup_error = str(exc)
            startup_ready = False
            _set_startup_phase("error", reason="startup_runtime_unhandled_exception")
            _log_memory_snapshot(
                "startup_failure",
                phase=startup_phase,
                failure_reason=startup_error,
                failure_stage="runtime_unhandled",
            )
            LOGGER.exception("Unhandled startup error.")
            if _is_web_server_enabled() and _should_hold_on_startup_error():
                LOGGER.warning("Holding process alive after unhandled startup error for health visibility.")
                while True:
                    await asyncio.sleep(3600)
            raise
        finally:
            await _shutdown_client()
    finally:
        _set_startup_phase("stopped", reason="startup_runtime_stopped")
        startup_ready = False
        _release_instance_lock(instance_lock_handle)
        instance_lock_handle = None


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Userbot stopped.")
