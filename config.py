"""Runtime configuration loaded from environment variables and local .env."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
from typing import Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader without third-party dependencies."""
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
                parsed = ast.literal_eval(value)
                value = str(parsed)
            except Exception:
                value = value[1:-1]

        # Environment variables provided by shell/process take precedence.
        os.environ.setdefault(key, value)


def _env_str(key: str, default: str = "") -> str:
    return str(os.getenv(key, default) or "").strip()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return bool(default)
    lowered = str(raw).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _normalize_list(values: Iterable[object]) -> List[str]:
    out: List[str] = []
    for item in values:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _env_list(key: str, default: list[str] | None = None) -> list[str]:
    raw = os.getenv(key)
    if raw is None:
        return list(default or [])

    text = str(raw).strip()
    if not text:
        return []

    # Accept JSON list first.
    try:
        parsed_json = json.loads(text)
        if isinstance(parsed_json, list):
            return _normalize_list(parsed_json)
    except Exception:
        pass

    # Accept Python list representation.
    try:
        parsed_py = ast.literal_eval(text)
        if isinstance(parsed_py, list):
            return _normalize_list(parsed_py)
    except Exception:
        pass

    # Fallback: comma-separated values.
    return _normalize_list(part for part in text.split(","))


_load_dotenv(ENV_PATH)


# Telegram API credentials from https://my.telegram.org
TELEGRAM_API_ID = _env_int("TELEGRAM_API_ID", 0)  # type: int
TELEGRAM_API_HASH = _env_str("TELEGRAM_API_HASH", "")  # type: str

# Shared Telegram folder invite link (addlist).
FOLDER_INVITE_LINK = _env_str("FOLDER_INVITE_LINK", "")  # type: str

# Auto-populated at startup from folder invite resolution.
SOURCES = _env_list("SOURCES", [])  # type: list

# Optional extra sources (username/public/private links).
EXTRA_SOURCES = _env_list("EXTRA_SOURCES", [])  # type: list

# Telethon destination (used when bot destination mode is disabled).
DESTINATION = _env_str("DESTINATION", "")  # type: str

# Bot API destination mode (optional).
BOT_DESTINATION_TOKEN = _env_str("BOT_DESTINATION_TOKEN", "")  # type: str
BOT_DESTINATION_CHAT_ID = _env_str("BOT_DESTINATION_CHAT_ID", "")  # type: str

# Codex OAuth subscription backend settings.
CODEX_BASE_URL = _env_str("CODEX_BASE_URL", "https://chatgpt.com/backend-api")  # type: str
CODEX_MODEL = _env_str("CODEX_MODEL", "gpt-5.3-codex")  # type: str
CODEX_ORIGINATOR = _env_str("CODEX_ORIGINATOR", "pi")  # type: str
OPENAI_AUTH_STARTUP_MODE = _env_str("OPENAI_AUTH_STARTUP_MODE", "auto").lower()  # type: str

# -----------------------------------------------------------------------------
# Digest Mode Core
# -----------------------------------------------------------------------------
DIGEST_MODE = _env_bool("DIGEST_MODE", True)  # type: bool
DIGEST_INTERVAL_MINUTES = _env_int("DIGEST_INTERVAL_MINUTES", 60)  # type: int
DIGEST_DAILY_TIMES = _env_list("DIGEST_DAILY_TIMES", ["00:00"])  # type: list[str]
# 0 disables automatic queue clearing (recommended to preserve full flow).
DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES = _env_int("DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES", 0)  # type: int
DIGEST_QUEUE_CLEAR_INCLUDE_INFLIGHT = _env_bool("DIGEST_QUEUE_CLEAR_INCLUDE_INFLIGHT", True)  # type: bool
# Deprecated (runtime queue clear scheduler is disabled to preserve full flow).
DIGEST_QUEUE_CLEAR_SCOPE = _env_str("DIGEST_QUEUE_CLEAR_SCOPE", "inflight").lower()  # type: str
# Daily digest window in hours (midnight digest summarizes this trailing window).
DIGEST_DAILY_WINDOW_HOURS = _env_int("DIGEST_DAILY_WINDOW_HOURS", 24)  # type: int
DIGEST_DAILY_MAX_POSTS = _env_int("DIGEST_DAILY_MAX_POSTS", 300)  # type: int
DIGEST_MAX_POSTS = _env_int("DIGEST_MAX_POSTS", 80)  # type: int
DIGEST_MAX_TOKENS = _env_int("DIGEST_MAX_TOKENS", 18000)  # type: int
CODEX_MODEL_CONTEXT_TOKENS = _env_int("CODEX_MODEL_CONTEXT_TOKENS", 200000)  # type: int
DIGEST_CONTEXT_FRACTION = _env_float("DIGEST_CONTEXT_FRACTION", 0.75)  # type: float
DIGEST_MIN_POST_LENGTH = _env_int("DIGEST_MIN_POST_LENGTH", 12)  # type: int
DIGEST_MAX_LINES = _env_int("DIGEST_MAX_LINES", 12)  # type: int
DIGEST_PREFER_JSON_OUTPUT = _env_bool("DIGEST_PREFER_JSON_OUTPUT", True)  # type: bool
OUTPUT_LANGUAGE = _env_str("OUTPUT_LANGUAGE", "English")  # type: str

# Semantic echo suppression at intake.
ENABLE_DUPE_DETECTION = _env_bool("ENABLE_DUPE_DETECTION", True)  # type: bool
DUPE_ENABLED = _env_bool("DUPE_ENABLED", ENABLE_DUPE_DETECTION)  # type: bool
DUPE_THRESHOLD = _env_float("DUPE_THRESHOLD", 0.87)  # type: float
DUPE_CACHE_SIZE = _env_int("DUPE_CACHE_SIZE", 400)  # type: int
DUPE_HISTORY_HOURS = _env_int("DUPE_HISTORY_HOURS", 4)  # type: int
DUPE_MERGE_INSTEAD_OF_SKIP = _env_bool("DUPE_MERGE_INSTEAD_OF_SKIP", True)  # type: bool
# Keep this false for no-HF operation (recommended for lightweight/stable deploys).
DUPE_USE_SENTENCE_TRANSFORMERS = _env_bool("DUPE_USE_SENTENCE_TRANSFORMERS", False)  # type: bool

# Severity router for real-time high-priority events.
ENABLE_SEVERITY_ROUTING = _env_bool("ENABLE_SEVERITY_ROUTING", True)  # type: bool
IMMEDIATE_HIGH = _env_bool("IMMEDIATE_HIGH", True)  # type: bool

# Include importance indicators (★★★/★★/★) in output style.
DIGEST_IMPORTANCE_SCORING = _env_bool("DIGEST_IMPORTANCE_SCORING", True)  # type: bool

# Allow AI to include HTML read-more links when message links are available.
DIGEST_INCLUDE_READ_MORE_LINKS = _env_bool("DIGEST_INCLUDE_READ_MORE_LINKS", False)  # type: bool

# Include source/channel tags in outbound messages.
INCLUDE_SOURCE_TAGS = _env_bool("INCLUDE_SOURCE_TAGS", False)  # type: bool
# Outbound Telegram message presentation layout.
OUTBOUND_POST_LAYOUT = _env_str("OUTBOUND_POST_LAYOUT", "editorial_card").lower()  # type: str

# -----------------------------------------------------------------------------
# Media OCR Translation
# -----------------------------------------------------------------------------
# Best-effort OCR for media-only posts. The bot only uses OCR text when it can
# translate non-English text into English. It does not generate visual
# descriptions or invented captions.
MEDIA_TEXT_OCR_ENABLED = _env_bool("MEDIA_TEXT_OCR_ENABLED", True)  # type: bool
MEDIA_TEXT_OCR_VIDEO_ENABLED = _env_bool("MEDIA_TEXT_OCR_VIDEO_ENABLED", True)  # type: bool
MEDIA_TEXT_OCR_MIN_CHARS = _env_int("MEDIA_TEXT_OCR_MIN_CHARS", 12)  # type: int
MEDIA_TEXT_OCR_MAX_CHARS = _env_int("MEDIA_TEXT_OCR_MAX_CHARS", 1600)  # type: int
MEDIA_TEXT_OCR_VIDEO_MAX_MB = _env_int("MEDIA_TEXT_OCR_VIDEO_MAX_MB", 25)  # type: int
MEDIA_TEXT_OCR_LANGS = _env_str("MEDIA_TEXT_OCR_LANGS", "eng+ara+fas+urd+rus")  # type: str

# -----------------------------------------------------------------------------
# Breaking News Fast Path
# -----------------------------------------------------------------------------
BREAKING_NEWS_KEYWORDS = _env_list(
    "BREAKING_NEWS_KEYWORDS",
    ["breaking", "urgent", "just now"],
)  # type: list[str]
BREAKING_MATCH_THRESHOLD = _env_int("BREAKING_MATCH_THRESHOLD", 1)  # type: int
# Human-like follow-up threading for repeated breaking topics.
ENABLE_BREAKING_TOPIC_THREADS = _env_bool("ENABLE_BREAKING_TOPIC_THREADS", True)  # type: bool
BREAKING_TOPIC_WINDOW_MINUTES = _env_int("BREAKING_TOPIC_WINDOW_MINUTES", 180)  # type: int
BREAKING_TOPIC_MIN_OVERLAP = _env_int("BREAKING_TOPIC_MIN_OVERLAP", 2)  # type: int
BREAKING_TOPIC_MIN_RATIO = _env_float("BREAKING_TOPIC_MIN_RATIO", 0.55)  # type: float
# Fallback fuzzy ratio for paraphrased same-topic follow-ups.
BREAKING_TOPIC_FUZZY_RATIO = _env_float("BREAKING_TOPIC_FUZZY_RATIO", 0.72)  # type: float
BREAKING_TOPIC_CONTINUITY_PREFIX = _env_str(
    "BREAKING_TOPIC_CONTINUITY_PREFIX",
    "",
)  # type: str
BREAKING_STYLE_MODE = _env_str("BREAKING_STYLE_MODE", "unhinged").lower()  # type: str
# Optional humanized rational viewpoint for vital/high-severity alerts.
HUMANIZED_VITAL_OPINION_ENABLED = _env_bool("HUMANIZED_VITAL_OPINION_ENABLED", True)  # type: bool
HUMANIZED_VITAL_OPINION_PROBABILITY = _env_float("HUMANIZED_VITAL_OPINION_PROBABILITY", 0.35)  # type: float
HUMANIZED_VITAL_OPINION_MAX_WORDS = _env_int("HUMANIZED_VITAL_OPINION_MAX_WORDS", 20)  # type: int

# -----------------------------------------------------------------------------
# Reliability / Delivery / Retry
# -----------------------------------------------------------------------------
DIGEST_SEND_DELAY_SECONDS = _env_float("DIGEST_SEND_DELAY_SECONDS", 0.8)  # type: float
DIGEST_SEND_CHUNK_SIZE = _env_int("DIGEST_SEND_CHUNK_SIZE", 3600)  # type: int
DIGEST_PIN_HOURLY = _env_bool("DIGEST_PIN_HOURLY", False)  # type: bool
DIGEST_PIN_DAILY = _env_bool("DIGEST_PIN_DAILY", False)  # type: bool
DIGEST_RETRY_BASE_SECONDS = _env_int("DIGEST_RETRY_BASE_SECONDS", 30)  # type: int
DIGEST_RETRY_MAX_SECONDS = _env_int("DIGEST_RETRY_MAX_SECONDS", 900)  # type: int
DIGEST_429_THRESHOLD_PER_HOUR = _env_int("DIGEST_429_THRESHOLD_PER_HOUR", 3)  # type: int
BREAKING_SLA_SECONDS = _env_int("BREAKING_SLA_SECONDS", 15)  # type: int

# -----------------------------------------------------------------------------
# Intake Pipeline
# -----------------------------------------------------------------------------
PIPELINE_TRIAGE_WORKERS = _env_int("PIPELINE_TRIAGE_WORKERS", 4)  # type: int
PIPELINE_AI_WORKERS = _env_int("PIPELINE_AI_WORKERS", 2)  # type: int
PIPELINE_DELIVERY_WORKERS = _env_int("PIPELINE_DELIVERY_WORKERS", 2)  # type: int
PIPELINE_OCR_WORKERS = _env_int("PIPELINE_OCR_WORKERS", 1)  # type: int
PIPELINE_QUERY_WEB_WORKERS = _env_int("PIPELINE_QUERY_WEB_WORKERS", 1)  # type: int
PIPELINE_JOB_MAX_RETRIES = _env_int("PIPELINE_JOB_MAX_RETRIES", 4)  # type: int
PIPELINE_RETRY_BASE_SECONDS = _env_int("PIPELINE_RETRY_BASE_SECONDS", 2)  # type: int
AI_DECISION_CACHE_HOURS = _env_int("AI_DECISION_CACHE_HOURS", 72)  # type: int

# -----------------------------------------------------------------------------
# Commands / Extensibility
# -----------------------------------------------------------------------------
DIGEST_STATUS_COMMAND = _env_str("DIGEST_STATUS_COMMAND", "/digest_status")  # type: str
DIGEST_POST_PROCESSORS = _env_list("DIGEST_POST_PROCESSORS", [])  # type: list[str]

# -----------------------------------------------------------------------------
# Query Assistant Mode
# -----------------------------------------------------------------------------
QUERY_MODE_ENABLED = _env_bool("QUERY_MODE_ENABLED", True)  # type: bool
QUERY_MAX_MESSAGES = _env_int("QUERY_MAX_MESSAGES", 50)  # type: int
QUERY_DEFAULT_HOURS_BACK = _env_int("QUERY_DEFAULT_HOURS_BACK", 24)  # type: int
# Optional web fallback when Telegram channel evidence is weak.
QUERY_WEB_FALLBACK_ENABLED = _env_bool("QUERY_WEB_FALLBACK_ENABLED", True)  # type: bool
# Trigger web fallback when Telegram matches are below this count.
QUERY_WEB_MIN_TELEGRAM_RESULTS = _env_int("QUERY_WEB_MIN_TELEGRAM_RESULTS", 3)  # type: int
# Maximum web evidence items to attach in query context.
QUERY_WEB_MAX_RESULTS = _env_int("QUERY_WEB_MAX_RESULTS", 12)  # type: int
# Maximum age window for web evidence (hours).
QUERY_WEB_MAX_HOURS_BACK = _env_int("QUERY_WEB_MAX_HOURS_BACK", 24)  # type: int
# Enforce recency filter on web evidence timestamps.
QUERY_WEB_REQUIRE_RECENT = _env_bool("QUERY_WEB_REQUIRE_RECENT", True)  # type: bool
# Require at least this many unique web sources before using fallback evidence.
QUERY_WEB_REQUIRE_MIN_SOURCES = _env_int("QUERY_WEB_REQUIRE_MIN_SOURCES", 2)  # type: int
# Optional trusted news domains allowlist (empty = accept all news RSS domains).
QUERY_WEB_ALLOWED_DOMAINS = _env_list(
    "QUERY_WEB_ALLOWED_DOMAINS",
    [
        "reuters.com",
        "apnews.com",
        "bbc.com",
        "aljazeera.com",
        "cnn.com",
        "nytimes.com",
        "washingtonpost.com",
        "bloomberg.com",
        "ft.com",
        "theguardian.com",
        "dw.com",
        "france24.com",
        "aa.com.tr",
        "npr.org",
    ],
)  # type: list[str]
# Optional chat allowlist for query interface (chat IDs as integers/strings).
# If empty, query mode only accepts outgoing private chats (e.g., Saved Messages).
QUERY_ALLOWED_CHAT_IDS = _env_list("QUERY_ALLOWED_CHAT_IDS", [])  # type: list[str]
# Optional explicit query peer allowlist (user IDs as ints/strings). If set, query
# is allowed only when target peer ID is listed or when chat is Saved Messages.
QUERY_ALLOWED_PEER_IDS = _env_list("QUERY_ALLOWED_PEER_IDS", [])  # type: list[str]

# -----------------------------------------------------------------------------
# Streaming UX
# -----------------------------------------------------------------------------
STREAMING_ENABLED = _env_bool("STREAMING_ENABLED", True)  # type: bool
STREAM_EDIT_INTERVAL_MS = _env_int("STREAM_EDIT_INTERVAL_MS", 400)  # type: int
STREAM_MAX_CHARS_PER_EDIT = _env_int("STREAM_MAX_CHARS_PER_EDIT", 120)  # type: int

# -----------------------------------------------------------------------------
# Web Server (Replit/UptimeRobot keepalive + future website integration)
# -----------------------------------------------------------------------------
ENABLE_WEB_SERVER = _env_bool("ENABLE_WEB_SERVER", True)  # type: bool
WEB_SERVER_HOST = _env_str("WEB_SERVER_HOST", "0.0.0.0")  # type: str
WEB_SERVER_PORT = _env_int("WEB_SERVER_PORT", 8080)  # type: int

# -----------------------------------------------------------------------------
# Message Rendering
# -----------------------------------------------------------------------------
ENABLE_HTML_FORMATTING = _env_bool("ENABLE_HTML_FORMATTING", True)  # type: bool
# Enable branded premium emoji replacement via tg-emoji tags.
ENABLE_PREMIUM_EMOJI = _env_bool("ENABLE_PREMIUM_EMOJI", True)  # type: bool
# JSON file containing emoji -> custom_emoji_id map for Nezami pack.
PREMIUM_EMOJI_MAP_FILE = _env_str("PREMIUM_EMOJI_MAP_FILE", "nezami_emoji_map.json")  # type: str
