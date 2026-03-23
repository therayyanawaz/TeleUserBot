"""AI filter and digest generation via Codex OAuth subscription backend."""

from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import logging
import re
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import httpx

import config
from auth import AuthManager
from db import ai_decision_cache_get, ai_decision_cache_set
from news_taxonomy import match_news_category, normalize_taxonomy_text
from news_signals import detect_story_signals, looks_like_live_event_update, should_downgrade_explainer_urgency
from prompts import (
    HUMAN_NEWSROOM_VOICE,
    QUERY_NO_MATCH_TEXT,
    build_digest_input_block,
    build_digest_system_prompt,
    build_query_system_prompt,
    quiet_period_message,
)
from shared_http import get_codex_http_client
from utils import estimate_tokens_rough as _estimate_tokens_rough
from utils import (
    expand_query_terms,
    extract_query_keywords,
    extract_query_numbers,
    is_broad_news_query,
    normalize_space,
    sanitize_telegram_html,
    strip_telegram_html,
)


CACHE_MAX_ITEMS = 2048
DEFAULT_CODEX_MODEL = "gpt-5.3-codex"
DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api"
DEFAULT_CODEX_ORIGINATOR = "pi"
DEFAULT_DIGEST_MAX_POSTS = 80
DEFAULT_DIGEST_MAX_TOKENS = 18000
FILTER_DECISION_PROMPT_VERSION = "v5"
VITAL_VIEW_PROMPT_VERSION = "v2"

_DIGIT_TOKEN_RE = re.compile(r"\b\d+(?:[.,:/-]\d+)*\b")
_LATIN_TOKEN_RE = re.compile(r"[a-z][a-z0-9'-]{1,}", flags=re.IGNORECASE)
_CAPITALIZED_TOKEN_RE = re.compile(r"\b[A-Z][a-z]{2,}(?:['-][A-Za-z]{2,})?\b")
_NUMBER_WORD_MAP = {
    "zero": "0",
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "ten": "10",
    "eleven": "11",
    "twelve": "12",
    "thirteen": "13",
    "fourteen": "14",
    "fifteen": "15",
    "sixteen": "16",
    "seventeen": "17",
    "eighteen": "18",
    "nineteen": "19",
    "twenty": "20",
    "thirty": "30",
    "forty": "40",
    "fifty": "50",
    "sixty": "60",
    "seventy": "70",
    "eighty": "80",
    "ninety": "90",
    "hundred": "100",
    "thousand": "1000",
}
_NUMBER_WORD_RE = re.compile(
    r"\b(" + "|".join(sorted((re.escape(key) for key in _NUMBER_WORD_MAP), key=len, reverse=True)) + r")\b",
    flags=re.IGNORECASE,
)
_HEDGE_MARKERS = (
    "reportedly",
    "reported",
    "reports",
    "according to",
    "claims",
    "claimed",
    "claims of",
    "appears",
    "appear to",
    "apparently",
    "allegedly",
    "alleged",
    "unconfirmed",
    "suggests",
    "suggested",
    "possible",
    "possibly",
    "suspected",
)
_HEADLINE_HEDGE_MARKERS = (
    "reportedly",
    "appears",
    "apparently",
    "allegedly",
    "unconfirmed",
    "suspected",
    "likely",
    "said to",
)
_ACTOR_TOKENS = {
    "army",
    "forces",
    "military",
    "police",
    "officials",
    "government",
    "israel",
    "israeli",
    "iran",
    "iranian",
    "hamas",
    "hezbollah",
    "houthi",
    "houthis",
    "idf",
    "irgc",
    "air force",
}
_EDITORIAL_ESCALATION_TOKENS = {
    "massacre",
    "slaughter",
    "bloodbath",
    "carnage",
    "collapse",
    "humiliation",
    "chaos",
    "fiasco",
    "shitshow",
    "meltdown",
    "rout",
    "panic",
}
_CAPITALIZED_STOPWORDS = {
    "A",
    "An",
    "The",
    "This",
    "That",
    "These",
    "Those",
    "Officials",
    "Forces",
    "Troops",
    "Strike",
    "Strikes",
    "Attack",
    "Attacks",
    "Blast",
    "Blasts",
    "Missile",
    "Missiles",
    "Airstrike",
    "Airstrikes",
}
_FEED_CONTEXT_BANNED_FRAGMENTS = (
    "regional stability",
    "worth watching",
    "situation remains tense",
    "civilian danger",
    "what explains",
    "reasons for",
    "ways to address",
    "analysis",
    "opinion",
    "thread",
    "explainer",
)
_FEED_LINE_PREFIX_RE = re.compile(
    r"^(?:breaking|alert|live update|update|analysis|opinion|thread|explainer)\s*[:\-–—]+\s*",
    flags=re.IGNORECASE,
)
_FEED_QUESTION_RE = re.compile(
    r"^(?:why|how|what explains|what caused|can|could|should|would|will)\b",
    flags=re.IGNORECASE,
)

LOGGER = logging.getLogger("tg_news_userbot.ai_filter")
_SUMMARY_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_SEVERITY_CACHE: "OrderedDict[str, str]" = OrderedDict()
_HEADLINE_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_VITAL_VIEW_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_OCR_TRANSLATION_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_QUOTA_WARNING_LOGGED = False
_FILTER_DECISION_CACHE_HITS = 0
_FILTER_DECISION_CACHE_MISSES = 0

# Quota health tracking: capture recent 429/rate-limit events.
_QUOTA_429_EVENTS: deque[int] = deque(maxlen=512)


class _CodexAuthError(RuntimeError):
    """Raised when backend auth fails (401/403)."""


class _CodexRateLimitError(RuntimeError):
    """Raised when backend signals usage/limit exhaustion."""


class _CodexApiError(RuntimeError):
    """Raised for non-auth Codex backend errors."""


@dataclass
class FilterDecision:
    action: Literal["skip", "deliver", "digest"]
    severity: Literal["high", "medium", "low"]
    summary_html: str
    headline_html: str
    story_bridge_html: str
    confidence: float
    reason_code: str
    topic_key: str
    needs_ocr_translation: bool
    cached: bool = False


def estimate_tokens_rough(text: str) -> int:
    """Public helper requested by scheduler/health layers."""
    return _estimate_tokens_rough(text)


def _record_quota_429() -> None:
    now = int(time.time())
    _QUOTA_429_EVENTS.append(now)


def _prune_quota_events() -> None:
    now = int(time.time())
    cutoff = now - 3600
    while _QUOTA_429_EVENTS and _QUOTA_429_EVENTS[0] < cutoff:
        _QUOTA_429_EVENTS.popleft()


def get_quota_health() -> Dict[str, Any]:
    """Simple heuristic for adaptive batching/scheduling."""
    _prune_quota_events()
    count_429_1h = len(_QUOTA_429_EVENTS)

    threshold = int(max(1, int(getattr(config, "DIGEST_429_THRESHOLD_PER_HOUR", 3))))
    interval_multiplier = 2 if count_429_1h > threshold else 1

    if count_429_1h > threshold:
        batch_scale = 0.5
        status = "degraded"
    elif count_429_1h > 0:
        batch_scale = 0.75
        status = "warm"
    else:
        batch_scale = 1.0
        status = "healthy"

    return {
        "status": status,
        "recent_429_count": count_429_1h,
        "interval_multiplier": interval_multiplier,
        "batch_scale": batch_scale,
    }


def get_filter_decision_cache_stats() -> Dict[str, float]:
    total = _FILTER_DECISION_CACHE_HITS + _FILTER_DECISION_CACHE_MISSES
    hit_rate = (float(_FILTER_DECISION_CACHE_HITS) / float(total)) if total > 0 else 0.0
    return {
        "hits": float(_FILTER_DECISION_CACHE_HITS),
        "misses": float(_FILTER_DECISION_CACHE_MISSES),
        "hit_rate": hit_rate,
    }


def _cache_get(key: str) -> Optional[Optional[str]]:
    if key not in _SUMMARY_CACHE:
        return None
    _SUMMARY_CACHE.move_to_end(key)
    return _SUMMARY_CACHE[key]


def _cache_set(key: str, value: Optional[str]) -> None:
    _SUMMARY_CACHE[key] = value
    _SUMMARY_CACHE.move_to_end(key)
    while len(_SUMMARY_CACHE) > CACHE_MAX_ITEMS:
        _SUMMARY_CACHE.popitem(last=False)


def _cache_key(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _severity_cache_get(key: str) -> Optional[str]:
    if key not in _SEVERITY_CACHE:
        return None
    _SEVERITY_CACHE.move_to_end(key)
    return _SEVERITY_CACHE[key]


def _severity_cache_set(key: str, value: str) -> None:
    _SEVERITY_CACHE[key] = value
    _SEVERITY_CACHE.move_to_end(key)
    while len(_SEVERITY_CACHE) > CACHE_MAX_ITEMS:
        _SEVERITY_CACHE.popitem(last=False)


def _headline_cache_get(key: str) -> Optional[Optional[str]]:
    if key not in _HEADLINE_CACHE:
        return None
    _HEADLINE_CACHE.move_to_end(key)
    return _HEADLINE_CACHE[key]


def _headline_cache_set(key: str, value: Optional[str]) -> None:
    _HEADLINE_CACHE[key] = value
    _HEADLINE_CACHE.move_to_end(key)
    while len(_HEADLINE_CACHE) > CACHE_MAX_ITEMS:
        _HEADLINE_CACHE.popitem(last=False)


def _vital_view_cache_get(key: str) -> Optional[Optional[str]]:
    if key not in _VITAL_VIEW_CACHE:
        return None
    _VITAL_VIEW_CACHE.move_to_end(key)
    return _VITAL_VIEW_CACHE[key]


def _vital_view_cache_set(key: str, value: Optional[str]) -> None:
    _VITAL_VIEW_CACHE[key] = value
    _VITAL_VIEW_CACHE.move_to_end(key)
    while len(_VITAL_VIEW_CACHE) > CACHE_MAX_ITEMS:
        _VITAL_VIEW_CACHE.popitem(last=False)


def _ocr_translation_cache_get(key: str) -> Optional[Optional[str]]:
    if key not in _OCR_TRANSLATION_CACHE:
        return None
    _OCR_TRANSLATION_CACHE.move_to_end(key)
    return _OCR_TRANSLATION_CACHE[key]


def _ocr_translation_cache_set(key: str, value: Optional[str]) -> None:
    _OCR_TRANSLATION_CACHE[key] = value
    _OCR_TRANSLATION_CACHE.move_to_end(key)
    while len(_OCR_TRANSLATION_CACHE) > CACHE_MAX_ITEMS:
        _OCR_TRANSLATION_CACHE.popitem(last=False)


def _likely_noise(text: str) -> bool:
    lower = text.lower()
    spam_terms = (
        "airdrop",
        "giveaway",
        "promo code",
        "referral",
        "join now",
        "paid group",
        "vip",
        "dm for",
        "casino",
        "bet",
        "discount",
        "advertisement",
    )
    term_hits = sum(1 for term in spam_terms if term in lower)
    url_hits = lower.count("http://") + lower.count("https://") + lower.count("t.me/")
    hashtag_hits = len(re.findall(r"#[a-z0-9_]+", lower))
    return term_hits >= 2 or (term_hits >= 1 and url_hits >= 1) or hashtag_hits >= 12


def _truncate_feed_line(text: str, *, limit: int) -> str:
    cleaned = normalize_space(text)
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3].rsplit(' ', 1)[0]}..."


def _feed_summary_segments(*texts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in texts:
        if not value:
            continue
        plain = re.sub(r"(?i)<br\s*/?>", "\n", str(value))
        plain = normalize_space(strip_telegram_html(plain))
        if not plain:
            continue
        for part in re.split(r"\n+|(?<=[.!?])\s+", plain):
            cleaned = normalize_space(part.strip(" \t-•"))
            cleaned = _FEED_LINE_PREFIX_RE.sub("", cleaned).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
    return out


def _is_bad_feed_headline(line: str) -> bool:
    cleaned = normalize_space(line)
    lowered = cleaned.lower()
    signals = detect_story_signals(cleaned)
    if len(cleaned) < 24:
        return True
    if cleaned.endswith("?") or _FEED_QUESTION_RE.search(cleaned):
        return True
    if bool(signals.get("downgrade_explainer")) and not bool(signals.get("live_event_update")):
        return True
    if any(fragment in lowered for fragment in _FEED_CONTEXT_BANNED_FRAGMENTS):
        return True
    if cleaned.count("...") or cleaned.endswith("..."):
        return True
    return False


def _is_weak_feed_context(line: str, headline: str) -> bool:
    cleaned = normalize_space(line)
    lowered = cleaned.lower()
    if not cleaned:
        return True
    if cleaned.endswith("?") or _FEED_QUESTION_RE.search(cleaned):
        return True
    if any(fragment in lowered for fragment in _FEED_CONTEXT_BANNED_FRAGMENTS):
        return True
    if SequenceMatcher(None, cleaned.lower(), headline.lower()).ratio() >= 0.72:
        return True
    if should_downgrade_explainer_urgency(cleaned) and not looks_like_live_event_update(cleaned):
        return True
    return False


def _feed_segment_taxonomy_score(segment: str, category_match: Any | None) -> int:
    if category_match is None:
        return 0
    normalized = normalize_taxonomy_text(segment)
    if not normalized:
        return 0

    score = 0
    for phrase in category_match.matched_primary:
        if re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", normalized):
            score += 3
    for phrase in category_match.matched_aliases:
        if re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", normalized):
            score += 2
    return score


def _build_feed_summary_html(raw_text: str, summary_seed: str = "") -> str:
    signals = detect_story_signals(raw_text)
    category_match = match_news_category(raw_text)
    segments = _feed_summary_segments(summary_seed, raw_text)
    if not segments:
        return ""

    acceptable = [segment for segment in segments if not _is_bad_feed_headline(segment)]
    acceptable.sort(
        key=lambda segment: (
            1 if looks_like_live_event_update(segment) else 0,
            _feed_segment_taxonomy_score(segment, category_match),
            -len(segment),
        ),
        reverse=True,
    )
    preferred = [segment for segment in acceptable if looks_like_live_event_update(segment)]

    headline = preferred[0] if preferred else (acceptable[0] if acceptable else "")
    if not headline:
        return ""
    if bool(signals.get("downgrade_explainer")) and not looks_like_live_event_update(headline):
        return ""

    headline = _truncate_feed_line(headline.rstrip(".!?"), limit=150)
    headline_html = f"<b>{sanitize_telegram_html(headline)}</b>"

    context = ""
    for segment in acceptable:
        if segment == headline:
            continue
        if _is_weak_feed_context(segment, headline):
            continue
        context = _truncate_feed_line(segment.rstrip(".!?"), limit=170)
        break

    if not context:
        return headline_html
    return f"{headline_html}<br>{sanitize_telegram_html(context)}"


def normalize_feed_summary_html(summary_html: str, raw_text: str) -> str:
    normalized = _build_feed_summary_html(raw_text, summary_html)
    if normalized:
        return normalized
    return ""


def _fallback_summary(text: str) -> Optional[str]:
    if _likely_noise(text):
        return None
    summary = _build_feed_summary_html(text)
    return summary or None


def _severity_from_text_heuristic(text: str) -> Literal["high", "medium", "low"]:
    lowered = text.lower()
    signals = detect_story_signals(text)
    category_match = match_news_category(text)

    if category_match is not None:
        bias = category_match.severity_bias
        if bias == "high":
            if bool(signals.get("downgrade_explainer")):
                return "medium"
            return "high"
        if bias == "medium":
            return "medium"
        if len(text.strip()) < 24 or _likely_noise(text):
            return "low"
        return "medium"

    high_terms = (
        "explosion",
        "missile",
        "strike",
        "killed",
        "dead",
        "casualties",
        "airstrike",
        "attack",
        "evacuation",
        "war",
        "mobilization",
        "state of emergency",
        "martial law",
        "earthquake",
        "flood",
        "wildfire",
        "blast",
        "drone strike",
    )
    medium_terms = (
        "sanction",
        "outage",
        "cyberattack",
        "ban",
        "shutdown",
        "airspace",
        "rates",
        "inflation",
        "policy",
        "military exercise",
        "warning",
        "evacuate",
    )

    high_hits = sum(1 for term in high_terms if term in lowered)
    medium_hits = sum(1 for term in medium_terms if term in lowered)

    if high_hits >= 1:
        if bool(signals.get("downgrade_explainer")):
            return "medium"
        return "high"
    if medium_hits >= 1:
        return "medium"
    if len(text.strip()) < 24 or _likely_noise(text):
        return "low"
    return "medium"


def _fallback_headline(text: str) -> Optional[str]:
    cleaned = normalize_space(text)
    if not cleaned:
        return None
    summary_html = _build_feed_summary_html(cleaned)
    if summary_html:
        plain_summary = re.sub(r"(?i)<br\s*/?>.*$", "", summary_html)
        headline = normalize_space(strip_telegram_html(plain_summary))
        if headline:
            return headline
    sentence_match = re.search(r"(.{24,700}?[.!?])(?:\s|$)", cleaned)
    if sentence_match:
        first = normalize_space(sentence_match.group(1))
    else:
        first = cleaned
    if len(first) > 420:
        first = f"{first[:417].rsplit(' ', 1)[0]}..."
    return first


def _resolve_codex_model() -> str:
    model = str(getattr(config, "CODEX_MODEL", DEFAULT_CODEX_MODEL) or "").strip()
    return model or DEFAULT_CODEX_MODEL


def _resolve_codex_originator() -> str:
    originator = str(getattr(config, "CODEX_ORIGINATOR", DEFAULT_CODEX_ORIGINATOR) or "").strip()
    return originator or DEFAULT_CODEX_ORIGINATOR


def _resolve_codex_url() -> str:
    base = str(getattr(config, "CODEX_BASE_URL", DEFAULT_CODEX_BASE_URL) or "").strip()
    if not base:
        base = DEFAULT_CODEX_BASE_URL
    normalized = base.rstrip("/")
    if normalized.endswith("/codex/responses"):
        return normalized
    if normalized.endswith("/codex"):
        return f"{normalized}/responses"
    return f"{normalized}/codex/responses"


def _resolve_output_language() -> str:
    language = normalize_space(str(getattr(config, "OUTPUT_LANGUAGE", "English") or ""))
    return language or "English"


def resolve_breaking_style_mode() -> Literal["unhinged", "classic"]:
    raw = normalize_space(str(getattr(config, "BREAKING_STYLE_MODE", "unhinged") or "")).lower()
    if raw == "classic":
        return "classic"
    return "unhinged"


def _breaking_style_is_unhinged() -> bool:
    return resolve_breaking_style_mode() == "unhinged"


def _extract_digit_tokens(text: str) -> set[str]:
    return {token.strip() for token in _DIGIT_TOKEN_RE.findall(str(text or "")) if token.strip()}


def _extract_numeric_tokens(text: str) -> set[str]:
    normalized = normalize_space(str(text or ""))
    tokens = {token.strip() for token in _DIGIT_TOKEN_RE.findall(normalized) if token.strip()}
    for token in _NUMBER_WORD_RE.findall(normalized):
        canonical = _NUMBER_WORD_MAP.get(str(token).lower())
        if canonical:
            tokens.add(canonical)
    return tokens


def _extract_latin_tokens(text: str) -> set[str]:
    return {
        token.lower()
        for token in _LATIN_TOKEN_RE.findall(normalize_space(str(text or "")))
        if len(token) >= 2
    }


def _source_has_latin_grounding(text: str) -> bool:
    return len(_extract_latin_tokens(text)) >= 4


def _text_has_hedge_markers(text: str, *, headline_mode: bool = False) -> bool:
    lowered = normalize_space(str(text or "")).lower()
    if not lowered:
        return False
    markers = _HEADLINE_HEDGE_MARKERS if headline_mode else _HEDGE_MARKERS
    return any(marker in lowered for marker in markers)


def _extract_candidate_named_tokens(text: str) -> set[str]:
    out: set[str] = set()
    for token in _CAPITALIZED_TOKEN_RE.findall(normalize_space(str(text or ""))):
        if token in _CAPITALIZED_STOPWORDS:
            continue
        out.add(token.lower())
    return out


def _safe_breaking_headline_fallback(text: str) -> str:
    fallback = _cleanup_headline(_fallback_headline(text) or "")
    if fallback:
        return fallback
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    if len(cleaned) > 220:
        cleaned = f"{cleaned[:217].rsplit(' ', 1)[0]}..."
    return cleaned


def _breaking_headline_is_grounded(source_text: str, candidate: str) -> bool:
    source_clean = normalize_space(strip_telegram_html(str(source_text or "")))
    candidate_clean = normalize_space(strip_telegram_html(str(candidate or "")))
    if not source_clean or not candidate_clean:
        return False

    source_numbers = _extract_numeric_tokens(source_clean)
    candidate_numbers = _extract_numeric_tokens(candidate_clean)
    if candidate_numbers and not candidate_numbers.issubset(source_numbers):
        return False

    if _text_has_hedge_markers(source_clean) and not _text_has_hedge_markers(
        candidate_clean,
        headline_mode=True,
    ):
        return False

    source_tokens = _extract_latin_tokens(source_clean)
    candidate_tokens = _extract_latin_tokens(candidate_clean)

    if _source_has_latin_grounding(source_clean):
        unsupported_actors = {
            token for token in candidate_tokens if token in _ACTOR_TOKENS and token not in source_tokens
        }
        if unsupported_actors:
            return False

        unsupported_named_tokens = _extract_candidate_named_tokens(candidate_clean) - source_tokens
        if unsupported_named_tokens:
            return False

    unsupported_editorial = {
        token
        for token in candidate_tokens
        if token in _EDITORIAL_ESCALATION_TOKENS and token not in source_tokens
    }
    if unsupported_editorial:
        return False

    return True


def resolve_breaking_headline_for_delivery(source_text: str, candidate: str | None) -> str:
    fallback = _safe_breaking_headline_fallback(source_text)
    cleaned_candidate = _cleanup_headline(str(candidate or ""))
    if not cleaned_candidate:
        return fallback
    if not _breaking_style_is_unhinged():
        return cleaned_candidate
    if _breaking_headline_is_grounded(source_text, cleaned_candidate):
        return cleaned_candidate
    return fallback


def _include_source_tags() -> bool:
    raw = getattr(config, "INCLUDE_SOURCE_TAGS", False)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return False


def _streaming_enabled() -> bool:
    raw = getattr(config, "STREAMING_ENABLED", True)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(raw)


def _summary_system_prompt() -> str:
    language = _resolve_output_language()
    return (
        "You are a strict news filter working like a human newsroom editor.\n"
        f"Always write output in {language}.\n"
        f"If the input is in another language, translate it into {language} while summarizing.\n"
        f"{HUMAN_NEWSROOM_VOICE}\n"
        "Output must be Telegram HTML only.\n"
        "Allowed tags: <b>, <i>, <u>, <s>, <tg-spoiler>, <code>, <pre>, <blockquote>, <a href>, <br>.\n"
        "If this message is real news or useful info, return a compact feed card: line 1 is a rewritten factual headline, line 2 is optional and must be one short contextual sentence.\n"
        "Do not copy article titles, teaser paragraphs, rhetorical questions, or explainer framing.\n"
        "Lead with the actual development, not with why/how framing.\n"
        "If the second line would be obvious, generic, or padded, omit it.\n"
        "If it is spam, ads, promotional, or noise, reply only: SKIP"
    )


def _filter_decision_system_prompt() -> str:
    language = _resolve_output_language()
    prompt = (
        "You are a strict newsroom intake classifier for Telegram news monitoring.\n"
        f"Always write HTML fields in {language}. Translate if needed.\n"
        f"{HUMAN_NEWSROOM_VOICE}\n"
        "Return ONLY one JSON object with these keys:\n"
        "action, severity, summary_html, headline_html, story_bridge_html, confidence, reason_code, topic_key, needs_ocr_translation\n"
        "action must be one of: skip, deliver, digest\n"
        "severity must be one of: high, medium, low\n"
        "summary_html must be Telegram-safe HTML using only <b>, <i>, <u>, <s>, <tg-spoiler>, <code>, <pre>, <blockquote>, <a href>, <br>\n"
        "headline_html must be a concise one-line Telegram-safe HTML headline or empty string\n"
        "story_bridge_html must be a concise Telegram-safe HTML contextual bridge or empty string\n"
        "confidence must be a number from 0 to 1\n"
        "reason_code must be a short snake_case code\n"
        "topic_key must be a short normalized topic label\n"
        "needs_ocr_translation must be true or false\n"
        "Use skip for spam, ads, promos, noise, or clearly irrelevant updates.\n"
        "Use deliver for urgent/breaking items that should be sent immediately.\n"
        "Use digest for meaningful non-breaking items worth keeping.\n"
        "If the text is mainly an analysis, explainer, opinion, recap, thread, or question-led piece without a concrete new event or official development, use digest or skip, not deliver.\n"
        "For delivered non-breaking items, summary_html must be a compact feed card: line 1 is a rewritten factual headline and line 2 is optional one short contextual sentence.\n"
        "Do not copy article titles, teaser paragraphs, rhetorical questions, or why/how/what explains framing into summary_html.\n"
        "The first line must lead with the actual development, not the source framing.\n"
        "Omit the second line if it would be fluff, generic stakes, or obvious common sense.\n"
        "Do not include markdown fences, comments, or extra text."
    )
    if _breaking_style_is_unhinged():
        prompt += (
            "\nIf severity is high, headline_html must be exactly one Telegram-safe line in an uncensored, hard-hitting, commentary-heavy live-news voice."
            "\nIf severity is high, keep every factual element directly grounded in the source text."
            "\nIf severity is high and the source is uncertain or hedged, headline_html must keep that uncertainty explicit."
            "\nIf severity is high, do not invent names, numbers, locations, actors, motives, or certainty."
            "\nIf severity is high, story_bridge_html must be an empty string unless recent related updates are explicitly provided."
            "\nIf recent related updates are provided, story_bridge_html may be one or two short lines or a compact mini context block."
            "\nIf you do provide story_bridge_html, it must start with Why it matters:, cite concrete prior-thread details, and avoid generic stakes or obvious consequences."
        )
    return prompt


def _severity_system_prompt() -> str:
    return (
        "Classify urgency for an incoming news post.\n"
        "Return ONLY one lowercase word: high OR medium OR low.\n"
        "Use high for explosions, deaths, major attacks, official declarations, "
        "war escalation, large-scale disasters, state emergency actions.\n"
        "Use medium for meaningful but non-critical updates.\n"
        "Use low for minor/noisy/context-only updates.\n"
        "No punctuation, no extra words."
    )


def _breaking_headline_prompt() -> str:
    language = _resolve_output_language()
    if _breaking_style_is_unhinged():
        return (
            "You write savage one-line breaking-news alerts for Telegram.\n"
            f"Output language must be {language}. Translate if needed.\n"
            "Return exactly one complete sentence on one line.\n"
            "Use an uncensored, hard-hitting, commentary-heavy voice.\n"
            "Profanity is allowed. Sharp editorial phrasing is allowed.\n"
            "But every factual element must be directly grounded in the source text.\n"
            "Do not invent names, numbers, casualties, locations, actors, motives, or certainty.\n"
            "If the source is hedged or uncertain, your line must keep that uncertainty explicit.\n"
            "Lead with the most important fact and make the wording hit hard.\n"
            "Target 10-24 words.\n"
            "Never end with ellipsis.\n"
            "No prefix, no source tag, no markdown, no extra explanation, no slurs."
        )
    return (
        "You write sharp live-news one-liners for Telegram alerts.\n"
        f"Output language must be {language}. Translate if needed.\n"
        "Return exactly one complete sentence with facts only.\n"
        "Sound like a strong human live-news presenter: direct, concrete, active voice.\n"
        "Use natural spoken cadence without sounding casual or sloppy.\n"
        "Use the most important fact first.\n"
        "Do not use generic framing like Breaking, Update, reports say, or situation update unless uncertainty is the key fact.\n"
        "Weak: Situation update after overnight military activity.\n"
        "Strong: Air defenses lit up over Amman after overnight interceptions.\n"
        "Target 12-28 words.\n"
        "Never end with ellipsis.\n"
        "Do not cut off mid-thought.\n"
        "No prefix, no markdown, no source tag, no explanation, no hype."
    )


def _vital_rational_view_prompt() -> str:
    language = _resolve_output_language()
    try:
        max_words = int(getattr(config, "HUMANIZED_VITAL_OPINION_MAX_WORDS", 20))
    except Exception:
        max_words = 20
    max_words = max(10, min(max_words, 32))
    if _breaking_style_is_unhinged():
        return (
            "You write contextual bridge lines for urgent Telegram breaking alerts.\n"
            f"Output language must be {language}. Translate if needed.\n"
            "Start the first line with: Why it matters:\n"
            f"Usually return one line. You may use two short lines or a compact mini context block only when one line cannot explain the connection.\n"
            f"Keep the full output under {max(18, min(max_words * 2, 40))} words.\n"
            "Your only job is to connect the current update to the ongoing story using the recent related updates.\n"
            "Make the bridge answer: how does this update change the story the reader was already following?\n"
            "Reference concrete prior-thread details such as the earlier location, target, asset, exchange, report, or official confirmation.\n"
            "Do not give generic stakes, obvious consequences, or empty escalation language.\n"
            "Good: Why it matters: Forty minutes ago the alerts were centered on Haifa; this now pushes the same exchange toward Acre.\n"
            "Good: Why it matters: Earlier reports focused on interceptions over Amman.\nThis now suggests the same exchange is reaching deeper into Jordanian airspace.\n"
            "Bad: Why it matters: This raises regional stability concerns.\n"
            "Bad: Why it matters: This shows the situation is still serious.\n"
            "If there is no concrete, non-obvious bridge to the recent updates, reply exactly: SKIP\n"
            "Do not speculate. Do not invent causality. No hashtags. No markdown."
        )
    return (
        "You write one-line connective context for urgent Telegram alerts.\n"
        f"Output language must be {language}. Translate if needed.\n"
        f"Return exactly one sentence (max {max_words} words), neutral and precise.\n"
        "Start with: Why it matters: \n"
        "Do not explain generic consequences or obvious common sense.\n"
        "Your only job is to connect the current update to the recent story arc when that connection is real.\n"
        "Use the provided recent related updates as your bridge material.\n"
        "Answer the reader's implicit question: what changed now compared with what was already happening?\n"
        "Good pattern: Why it matters: This comes hours after earlier strikes near Tehran, so the pressure now appears to be reaching deeper into the capital.\n"
        "Good pattern: Why it matters: Earlier Gulf alerts centered on Bahrain, and this pushes the same tension into Abu Dhabi.\n"
        "Bad: Why it matters: This raises regional stability concerns.\n"
        "Bad: Why it matters: Civilian danger is increasing.\n"
        "If there is no non-obvious bridge to the recent updates, reply exactly: SKIP\n"
        "Do not speculate. Do not take sides. No hashtags. No markdown."
    )


def _ocr_translation_prompt() -> str:
    return (
        "You receive OCR text extracted from an image or video frame.\n"
        "Your job is translation only.\n"
        "Rules:\n"
        "- If the OCR text is already English, reply exactly: SKIP\n"
        "- If the OCR text is unreadable, fragmented, or too noisy to trust, reply exactly: SKIP\n"
        "- If the OCR text is not English, translate it faithfully into clear English\n"
        "- Preserve names, numbers, times, place names, and warning language\n"
        "- Do not summarize\n"
        "- Do not describe the image or video\n"
        "- Do not add commentary, source labels, headings, bullets, or markdown\n"
        "- Output plain translated text only"
    )


def _build_codex_payload(
    text: str,
    instructions: str,
    *,
    verbosity: str = "medium",
    stream: bool = True,
    image_data_urls: Sequence[str] | None = None,
) -> dict:
    content: list[dict[str, object]] = [{"type": "input_text", "text": text}]
    if image_data_urls:
        for data_url in image_data_urls:
            value = str(data_url or "").strip()
            if not value:
                continue
            content.append(
                {
                    "type": "input_image",
                    "image_url": value,
                }
            )

    payload: dict[str, object] = {
        "model": _resolve_codex_model(),
        "store": False,
        "stream": bool(stream),
        "instructions": instructions,
        "input": [
            {
                "role": "user",
                "content": content,
            }
        ],
        "text": {"verbosity": verbosity},
        "include": ["reasoning.encrypted_content"],
    }
    return payload


def _extract_response_output_text(response_payload: dict) -> str:
    outputs = response_payload.get("output")
    if not isinstance(outputs, list):
        return ""

    chunks: list[str] = []
    for item in outputs:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") in {"output_text", "refusal"}:
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    chunks.append(text)

    return "".join(chunks).strip()


def _extract_completed_text(event: dict) -> str:
    response = event.get("response")
    if isinstance(response, dict):
        return _extract_response_output_text(response)
    # Some payloads can arrive as direct response body.
    if isinstance(event, dict) and event.get("output") is not None:
        return _extract_response_output_text(event)
    return ""


def _raise_codex_http_error(response: httpx.Response) -> None:
    message = response.text or response.reason_phrase or "Request failed"
    code = ""

    try:
        payload = response.json()
        err = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(err, dict):
            code = str(err.get("code") or err.get("type") or "").strip().lower()
            message = str(err.get("message") or message).strip()
    except Exception:
        pass

    if response.status_code in (401, 403):
        raise _CodexAuthError(message or f"HTTP {response.status_code}")

    if response.status_code == 429 or code in {
        "usage_limit_reached",
        "usage_not_included",
        "rate_limit_exceeded",
    }:
        _record_quota_429()
        raise _CodexRateLimitError(message or "ChatGPT usage limit reached.")

    raise _CodexApiError(message or f"Codex backend error: HTTP {response.status_code}")


@dataclass
class CodexStreamMetrics:
    total_chars: int = 0
    delta_events: int = 0
    started_at: float = 0.0
    ended_at: float = 0.0

    @property
    def elapsed(self) -> float:
        if self.ended_at <= self.started_at:
            return 0.0
        return self.ended_at - self.started_at

    @property
    def tokens_per_second(self) -> float:
        elapsed = self.elapsed
        if elapsed <= 0:
            return 0.0
        return float(estimate_tokens_rough("x" * self.total_chars)) / elapsed


class _StreamingCodexResponse:
    def __init__(
        self,
        *,
        cleaned: str,
        auth_context: Dict[str, str],
        instructions: str,
        verbosity: str,
        image_data_urls: Sequence[str] | None = None,
    ) -> None:
        self._cleaned = cleaned
        self._auth_context = auth_context
        self._instructions = instructions
        self._verbosity = verbosity
        self._image_data_urls = list(image_data_urls or [])

        self._stream_ctx = None
        self._response: httpx.Response | None = None

        self.full_text = ""
        self.metrics = CodexStreamMetrics()
        self._iterated = False

    async def __aenter__(self) -> "_StreamingCodexResponse":
        access_token = self._auth_context["access_token"]
        account_id = self._auth_context["account_id"]
        headers = {
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id,
            "OpenAI-Beta": "responses=experimental",
            "originator": _resolve_codex_originator(),
            "accept": "text/event-stream",
            "content-type": "application/json",
            "user-agent": "tg-news-userbot/1.0",
        }
        payload = _build_codex_payload(
            self._cleaned,
            instructions=self._instructions,
            verbosity=self._verbosity,
            stream=True,
            image_data_urls=self._image_data_urls,
        )

        client = await get_codex_http_client()
        self._stream_ctx = client.stream(
            "POST",
            _resolve_codex_url(),
            headers=headers,
            json=payload,
        )
        self._response = await self._stream_ctx.__aenter__()
        if self._response.status_code >= 400:
            body = await self._response.aread()
            error_response = httpx.Response(
                status_code=self._response.status_code,
                headers=self._response.headers,
                content=body,
                request=self._response.request,
            )
            await self.__aexit__(None, None, None)
            _raise_codex_http_error(error_response)

        self.metrics.started_at = time.time()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.metrics.ended_at = time.time()
        if self._stream_ctx is not None:
            await self._stream_ctx.__aexit__(exc_type, exc, tb)
            self._stream_ctx = None
        self._response = None

    async def __aiter__(self) -> AsyncIterator[str]:
        if self._iterated:
            return
        self._iterated = True
        if self._response is None:
            return

        data_lines: list[str] = []
        completed_text = ""

        async for line in self._response.aiter_lines():
            if line == "":
                if not data_lines:
                    continue
                event_data = "\n".join(data_lines).strip()
                data_lines = []
                if not event_data or event_data == "[DONE]":
                    continue

                try:
                    event = json.loads(event_data)
                except json.JSONDecodeError:
                    continue

                event_type = str(event.get("type") or "")
                if event_type in {"response.output_text.delta", "response.refusal.delta"}:
                    delta = event.get("delta")
                    if isinstance(delta, str) and delta:
                        self.full_text += delta
                        self.metrics.total_chars += len(delta)
                        self.metrics.delta_events += 1
                        yield delta
                    continue

                if event_type == "response.failed":
                    err = event.get("response", {}).get("error", {})
                    message = ""
                    if isinstance(err, dict):
                        message = str(err.get("message") or "")
                    raise _CodexApiError(message or "Codex response failed.")

                if event_type == "error":
                    message = str(event.get("message") or event.get("code") or "").strip()
                    raise _CodexApiError(message or "Codex backend returned error event.")

                if event_type in {"response.completed", "response.done"}:
                    maybe = _extract_completed_text(event)
                    if maybe:
                        completed_text = maybe
                continue

            if line.startswith("data:"):
                data_lines.append(line[5:].strip())

        if not self.full_text and completed_text:
            self.full_text = completed_text
            self.metrics.total_chars = len(completed_text)


def streaming_codex_response(
    cleaned: str,
    auth_context: Dict[str, str],
    instructions: str,
    *,
    verbosity: str = "medium",
    image_data_urls: Sequence[str] | None = None,
) -> _StreamingCodexResponse:
    return _StreamingCodexResponse(
        cleaned=cleaned,
        auth_context=auth_context,
        instructions=instructions,
        verbosity=verbosity,
        image_data_urls=image_data_urls,
    )


def _extract_response_text_from_json(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    if payload.get("response") and isinstance(payload.get("response"), dict):
        return _extract_response_output_text(payload["response"])
    return _extract_response_output_text(payload)


async def _call_codex_non_stream(
    cleaned: str,
    auth_context: Dict[str, str],
    instructions: str,
    *,
    verbosity: str = "medium",
    image_data_urls: Sequence[str] | None = None,
) -> str:
    access_token = auth_context["access_token"]
    account_id = auth_context["account_id"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "chatgpt-account-id": account_id,
        "OpenAI-Beta": "responses=experimental",
        "originator": _resolve_codex_originator(),
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "tg-news-userbot/1.0",
    }
    payload = _build_codex_payload(
        cleaned,
        instructions=instructions,
        verbosity=verbosity,
        stream=False,
        image_data_urls=image_data_urls,
    )
    http = await get_codex_http_client()
    response = await http.post(_resolve_codex_url(), headers=headers, json=payload)
    if response.status_code >= 400:
        _raise_codex_http_error(response)
    try:
        data = response.json()
    except Exception:
        return ""
    return _extract_response_text_from_json(data).strip()


async def _call_codex(
    cleaned: str,
    auth_context: Dict[str, str],
    instructions: str,
    *,
    verbosity: str = "medium",
    on_token: Callable[[str], Awaitable[None]] | None = None,
    image_data_urls: Sequence[str] | None = None,
) -> str:
    if _streaming_enabled():
        try:
            fragments: list[str] = []
            async with streaming_codex_response(
                cleaned,
                auth_context,
                instructions,
                verbosity=verbosity,
                image_data_urls=image_data_urls,
            ) as stream:
                async for delta in stream:
                    fragments.append(delta)
                    if on_token is not None:
                        await on_token(delta)

                parsed = "".join(fragments).strip() or stream.full_text.strip()
                if parsed:
                    return parsed
        except (_CodexAuthError, _CodexRateLimitError):
            raise
        except Exception as exc:
            LOGGER.debug("Streaming path failed, falling back to non-stream: %s", exc)

    return await _call_codex_non_stream(
        cleaned,
        auth_context,
        instructions,
        verbosity=verbosity,
        image_data_urls=image_data_urls,
    )


async def codex_chat_completion(
    text: str,
    auth_context: Dict[str, str],
    instructions: str,
) -> str:
    """Compatibility wrapper for generic Codex responses call."""
    return await _call_codex(text, auth_context, instructions=instructions)


def _resolve_digest_max_posts() -> int:
    raw = getattr(config, "DIGEST_MAX_POSTS", DEFAULT_DIGEST_MAX_POSTS)
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_DIGEST_MAX_POSTS
    return max(1, min(value, 500))


def _resolve_digest_max_lines() -> int:
    raw = getattr(config, "DIGEST_MAX_LINES", 12)
    try:
        value = int(raw)
    except Exception:
        value = 12
    return max(3, min(value, 12))


def _resolve_digest_token_budget() -> int:
    raw_max = getattr(config, "DIGEST_MAX_TOKENS", DEFAULT_DIGEST_MAX_TOKENS)
    raw_context = getattr(config, "CODEX_MODEL_CONTEXT_TOKENS", 200000)
    raw_fraction = getattr(config, "DIGEST_CONTEXT_FRACTION", 0.75)

    try:
        max_tokens = int(raw_max)
    except Exception:
        max_tokens = DEFAULT_DIGEST_MAX_TOKENS

    try:
        context_tokens = int(raw_context)
    except Exception:
        context_tokens = 200000

    try:
        fraction = float(raw_fraction)
    except Exception:
        fraction = 0.75

    # Hard cap: never exceed 75% of nominal context.
    fraction = min(max(fraction, 0.2), 0.75)
    safe_limit = int(context_tokens * fraction)

    max_tokens = max(1000, max_tokens)
    return min(max_tokens, max(1000, safe_limit))


def _extract_json_object_block(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""

    # JSON fenced code block
    m = re.search(r"```json\s*(\{.*?\})\s*```", cleaned, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # first object-like block
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        return cleaned[start : end + 1].strip()
    return ""


def _try_parse_digest_json(text: str) -> Dict[str, Any] | None:
    block = _extract_json_object_block(text)
    if not block:
        return None
    try:
        payload = json.loads(block)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _resolve_ai_decision_cache_hours() -> int:
    raw = getattr(config, "AI_DECISION_CACHE_HOURS", 72)
    try:
        value = int(raw)
    except Exception:
        value = 72
    return max(1, min(value, 24 * 30))


def _sanitize_reason_code(value: Any) -> str:
    cleaned = normalize_space(str(value or "")).lower()
    cleaned = re.sub(r"[^a-z0-9_]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned[:48] or "unclassified"


def _sanitize_topic_key(value: Any, fallback_text: str) -> str:
    cleaned = normalize_space(str(value or "")).lower()
    cleaned = re.sub(r"[^a-z0-9\s/_-]+", "", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned).strip(" _-")
    if cleaned:
        return cleaned[:80]
    tokens = re.findall(r"[a-z0-9]{3,}", normalize_space(fallback_text).lower())
    return "_".join(tokens[:6])[:80] or "general_update"


def _fallback_filter_decision(text: str) -> FilterDecision:
    cleaned = normalize_space(text)
    if not cleaned or _likely_noise(cleaned):
        return FilterDecision(
            action="skip",
            severity="low",
            summary_html="",
            headline_html="",
            story_bridge_html="",
            confidence=0.2,
            reason_code="likely_noise",
            topic_key=_sanitize_topic_key("", cleaned),
            needs_ocr_translation=False,
        )

    summary = _fallback_summary(cleaned) or ""
    severity = _severity_from_text_heuristic(cleaned)
    if should_downgrade_explainer_urgency(cleaned):
        severity = "medium" if summary else "low"
        action: Literal["skip", "deliver", "digest"] = "digest" if summary else "skip"
    else:
        action = "deliver" if severity == "high" else "digest"
    headline = _fallback_headline(cleaned) or ""
    return FilterDecision(
        action=action,
        severity=severity,
        summary_html=summary,
        headline_html=headline,
        story_bridge_html="",
        confidence=0.55 if summary else 0.35,
        reason_code="local_fallback",
        topic_key=_sanitize_topic_key("", cleaned),
        needs_ocr_translation=False,
    )


def _normalize_filter_action(value: Any, severity: str) -> Literal["skip", "deliver", "digest"]:
    cleaned = normalize_space(str(value or "")).lower()
    if cleaned in {"skip", "drop", "ignore"}:
        return "skip"
    if cleaned in {"deliver", "send", "breaking", "immediate"}:
        return "deliver"
    if cleaned in {"digest", "queue", "keep"}:
        return "digest"
    return "deliver" if severity == "high" else "digest"


def _validate_filter_decision(
    payload: Dict[str, Any],
    raw_text: str,
    *,
    recent_context: Sequence[str] | None = None,
) -> FilterDecision:
    fallback = _fallback_filter_decision(raw_text)
    story_signals = detect_story_signals(raw_text)

    severity = _normalize_severity_label(str(payload.get("severity") or "")) or fallback.severity
    action = _normalize_filter_action(payload.get("action"), severity)
    confidence_raw = payload.get("confidence", fallback.confidence)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = fallback.confidence
    confidence = max(0.0, min(confidence, 1.0))

    raw_summary_html = sanitize_telegram_html(str(payload.get("summary_html") or "")).strip()
    headline_html = sanitize_telegram_html(str(payload.get("headline_html") or "")).strip()
    story_bridge_html = sanitize_telegram_html(str(payload.get("story_bridge_html") or "")).strip()
    summary_html = normalize_feed_summary_html(raw_summary_html, raw_text)

    if action != "skip" and not summary_html:
        summary_html = fallback.summary_html
    if bool(story_signals.get("downgrade_explainer")) and (
        action == "deliver" or severity == "high"
    ):
        severity = "medium" if summary_html else "low"
        action = "digest" if summary_html else "skip"
        confidence = min(confidence, 0.75)
    if severity == "high":
        headline_html = resolve_breaking_headline_for_delivery(
            raw_text,
            headline_html or fallback.headline_html,
        )
        if _breaking_style_is_unhinged():
            story_bridge_html = resolve_vital_rational_view_for_delivery(
                raw_text,
                story_bridge_html,
                recent_context,
            )

    needs_ocr_translation = bool(payload.get("needs_ocr_translation", False))
    reason_code = _sanitize_reason_code(payload.get("reason_code"))
    if bool(story_signals.get("downgrade_explainer")) and action != "deliver":
        reason_code = "explainer_digest"
    topic_key = _sanitize_topic_key(payload.get("topic_key"), raw_text)

    if action == "skip":
        summary_html = ""
        headline_html = ""
        story_bridge_html = ""

    return FilterDecision(
        action=action,
        severity=severity,
        summary_html=summary_html,
        headline_html=headline_html,
        story_bridge_html=story_bridge_html,
        confidence=confidence,
        reason_code=reason_code,
        topic_key=topic_key,
        needs_ocr_translation=needs_ocr_translation,
    )


def _importance_stars(value: Any) -> str:
    try:
        v = int(value)
    except Exception:
        v = 1
    if v >= 3:
        return "★★★"
    if v == 2:
        return "★★"
    return "★"


def _severity_emoji(value: Any) -> str:
    normalized = normalize_space(str(value or "")).lower()
    if normalized in {"high", "critical", "3", "★★★"}:
        return "🔥"
    if normalized in {"medium", "2", "★★"}:
        return "⚠️"
    if normalized in {"low", "1", "★"}:
        return "ℹ️"
    # numeric fallback
    try:
        score = int(value)
    except Exception:
        score = 1
    if score >= 3:
        return "🔥"
    if score == 2:
        return "⚠️"
    return "ℹ️"


def _json_digest_to_html(payload: Dict[str, Any], *, interval_minutes: int, max_lines: int) -> str:
    quiet = bool(payload.get("quiet"))
    if quiet:
        return quiet_period_message(interval_minutes)

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return quiet_period_message(interval_minutes)

    # Sort by severity/importance descending if available.
    def _score(item: Any) -> int:
        if not isinstance(item, dict):
            return 0
        severity = str(item.get("severity") or "").lower()
        if severity == "high":
            return 3
        if severity == "medium":
            return 2
        if severity == "low":
            return 1
        try:
            return int(item.get("importance", 1))
        except Exception:
            return 1

    ordered = sorted([it for it in items if isinstance(it, dict)], key=_score, reverse=True)

    lines: List[str] = []
    for item in ordered:
        emoji = normalize_space(str(item.get("emoji") or "")) or _severity_emoji(
            item.get("severity", item.get("importance"))
        )
        if emoji not in {"🔥", "⚠️", "ℹ️"}:
            emoji = _severity_emoji(item.get("severity", item.get("importance")))

        headline = normalize_space(
            str(item.get("headline") or item.get("title") or item.get("summary") or "")
        )
        if not headline:
            continue
        if len(headline) > 140:
            headline = f"{headline[:137].rsplit(' ', 1)[0]}..."

        line = f"{emoji} {headline}".strip()
        lines.append(line)

        if len(lines) >= max_lines:
            break

    if not lines:
        return quiet_period_message(interval_minutes)

    return sanitize_telegram_html("<br>".join(lines[:max_lines]))


def _strip_digest_citations(text: str) -> str:
    cleaned = str(text or "")
    if not cleaned:
        return ""
    cleaned = re.sub(r'<a\s+href="[^"]*">\s*Read more\s*</a>', "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<a\s+href="[^"]*">.*?</a>', "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bRead more\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\[[^\]]+\]", "", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\s+\n", "\n", cleaned)
    cleaned = re.sub(r"(?:<br>\s*){3,}", "<br><br>", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _digest_needs_english_rewrite(text: str, output_language: str) -> bool:
    if normalize_space(output_language).lower() != "english":
        return False
    plain = strip_telegram_html(text)
    if not plain:
        return False

    non_ascii_letters = sum(1 for ch in plain if ch.isalpha() and ord(ch) > 127)
    arabic_script = bool(re.search(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", plain))
    cyrillic_script = bool(re.search(r"[\u0400-\u04FF]", plain))
    return arabic_script or cyrillic_script or non_ascii_letters >= 4


def _digest_english_rewrite_prompt() -> str:
    return (
        "Rewrite this Telegram HTML digest into clean English only.\n"
        "Rules:\n"
        "- Keep the same headline-only digest structure\n"
        "- Preserve severity emojis and line breaks\n"
        "- Make the English sound human, readable, and alive\n"
        "- Avoid repetitive sentence shapes and stiff wording\n"
        "- Translate every non-English word or phrase into English\n"
        "- Remove citations, source names, bracket tags, outlet names, links, and any 'Read more' text\n"
        "- Do not add new facts\n"
        "- Output Telegram HTML only"
    )


async def _normalize_digest_output(
    text: str,
    auth_manager: AuthManager,
    *,
    interval_minutes: int,
    max_lines: int,
) -> str:
    cleaned = _html_digest_cleanup(
        _strip_digest_citations(text),
        interval_minutes=interval_minutes,
        max_lines=max_lines,
    )
    if not _digest_needs_english_rewrite(cleaned, _resolve_output_language()):
        return cleaned

    try:
        auth_context = await auth_manager.get_auth_context()
        rewritten = await _call_codex(
            cleaned,
            auth_context,
            instructions=_digest_english_rewrite_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            rewritten = await _call_codex(
                cleaned,
                auth_context,
                instructions=_digest_english_rewrite_prompt(),
                verbosity="low",
            )
        except Exception:
            return cleaned
    except Exception:
        return cleaned

    normalized = _html_digest_cleanup(
        _strip_digest_citations(rewritten),
        interval_minutes=interval_minutes,
        max_lines=max_lines,
    )
    return normalized or cleaned


def _html_digest_cleanup(text: str, *, interval_minutes: int, max_lines: int) -> str:
    cleaned = _strip_digest_citations((text or "").strip())
    if not cleaned:
        return quiet_period_message(interval_minutes)

    if cleaned.upper().startswith("SKIP"):
        return quiet_period_message(interval_minutes)

    quiet = quiet_period_message(interval_minutes)
    if normalize_space(cleaned) == quiet:
        return quiet

    text_lines = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
    lines_raw = [line.rstrip() for line in text_lines.splitlines() if line.strip()]
    lines: List[str] = []
    for raw in lines_raw:
        line = re.sub(r"</?[^>]+>", "", raw).strip()
        line = re.sub(r"^[-*•]\s*", "", line)
        line = line.strip()
        if not line:
            continue
        if not line.startswith(("🔥", "⚠️", "ℹ️")):
            # Force compact headline line format when model drifts.
            severity = _severity_from_text_heuristic(line)
            emoji = "🔥" if severity == "high" else ("⚠️" if severity == "medium" else "ℹ️")
            line = f"{emoji} {line}"
        line = re.sub(
            r"^(🔥|⚠️|ℹ️)\s+(?:breaking|alert|live update|update|situation update)\s*[:\-–—]+\s*",
            r"\1 ",
            line,
            flags=re.IGNORECASE,
        )
        if len(line) > 220:
            line = f"{line[:217].rsplit(' ', 1)[0]}..."
        line = re.sub(r"\s\[[^\]]+\](?!\()", "", line).strip()
        lines.append(line)
    if not lines:
        return quiet_period_message(interval_minutes)

    return sanitize_telegram_html("<br>".join(lines[:max_lines]))


def _post_source(post: Dict[str, object]) -> str:
    for key in ("source_name", "source", "source_title", "channel_id"):
        value = normalize_space(str(post.get(key) or ""))
        if value:
            return value
    return "Unknown"


def _post_timestamp(post: Dict[str, object]) -> int:
    try:
        value = int(post.get("timestamp") or 0)
    except Exception:
        value = 0
    return value if value > 0 else 0


def _post_text(post: Dict[str, object]) -> str:
    return normalize_space(str(post.get("raw_text") or post.get("text") or ""))


def _post_link(post: Dict[str, object]) -> str:
    value = normalize_space(str(post.get("message_link") or post.get("link") or ""))
    return value if value.startswith("http") else ""


def _build_digest_context(
    posts: Sequence[Dict[str, object]],
    *,
    token_budget: int,
    max_posts: int,
) -> Tuple[List[str], int, int]:
    min_len = int(max(1, int(getattr(config, "DIGEST_MIN_POST_LENGTH", 12))))

    deduped: List[Dict[str, object]] = []
    seen = set()
    for post in posts:
        text = _post_text(post)
        if len(text) < min_len:
            continue
        if _likely_noise(text):
            continue
        fingerprint = _cache_key(text.lower())
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(post)

    lines: List[str] = []
    used_tokens = 0
    total_input = len(posts)

    for idx, post in enumerate(deduped[:max_posts], start=1):
        source = _post_source(post)
        text = _post_text(post)
        if len(text) > 1200:
            text = f"{text[:1197].rsplit(' ', 1)[0]}..."

        ts = _post_timestamp(post)
        ts_label = (
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if ts
            else "unknown-time"
        )

        link = _post_link(post)
        link_part = f" | link={link}" if link else ""
        line = f"{idx}. [{source}] ({ts_label}) {text}{link_part}"

        line_tokens = estimate_tokens_rough(line) + 8
        if lines and (used_tokens + line_tokens > token_budget):
            break
        if not lines and line_tokens > token_budget:
            # Guarantee at least one line by truncating aggressively.
            short = text[: max(120, token_budget * 4)]
            line = f"{idx}. [{source}] ({ts_label}) {short}..."
            line_tokens = estimate_tokens_rough(line)

        lines.append(line)
        used_tokens += line_tokens

    return lines, len(lines), total_input


def _rule_based_fallback_digest(
    posts: Sequence[Dict[str, object]],
    *,
    max_items: int = 10,
) -> str:
    top: List[str] = []
    seen = set()
    include_links = False
    include_sources = False

    for post in posts:
        text = _post_text(post)
        if not text or _likely_noise(text):
            continue

        fingerprint = _cache_key(text.lower())
        if fingerprint in seen:
            continue
        seen.add(fingerprint)

        source = _post_source(post)
        title = text.split(". ")[0].strip()
        title = re.sub(
            r"^(?:breaking|alert|live update|update|urgent)\s*[:\-–—]+\s*",
            "",
            title,
            flags=re.IGNORECASE,
        )
        if len(title) > 120:
            title = f"{title[:117].rsplit(' ', 1)[0]}..."

        severity = _severity_from_text_heuristic(text)
        emoji = "🔥" if severity == "high" else ("⚠️" if severity == "medium" else "ℹ️")
        line = f"{emoji} {title}"
        if include_sources:
            line += f" <i>[{source}]</i>"
        link = _post_link(post)
        if include_links and link:
            line += f' <a href="{link}">Read more</a>'

        top.append(line)
        if len(top) >= max_items:
            break

    if not top:
        return ""
    return sanitize_telegram_html("<br>".join(top))


def _tiny_local_digest(posts: Sequence[Dict[str, object]], *, max_items: int = 6) -> str:
    """
    Lightweight local second-stage fallback.
    This is intentionally tiny/fast and purely deterministic.
    """
    lines: List[str] = []
    include_sources = False
    for post in posts:
        text = _post_text(post)
        if not text:
            continue
        source = _post_source(post)
        short = text if len(text) <= 120 else f"{text[:117].rsplit(' ', 1)[0]}..."
        severity = _severity_from_text_heuristic(text)
        emoji = "🔥" if severity == "high" else ("⚠️" if severity == "medium" else "ℹ️")
        line = f"{emoji} {short}"
        if include_sources:
            line += f" <i>[{source}]</i>"
        lines.append(line)
        if len(lines) >= max_items:
            break
    return sanitize_telegram_html("<br>".join(lines)) if lines else ""


def _minimal_digest(posts: Sequence[Dict[str, object]], *, max_items: int = 4) -> str:
    snippets: List[str] = []
    for post in posts:
        text = _post_text(post)
        if not text:
            continue
        short = text if len(text) <= 110 else f"{text[:107].rsplit(' ', 1)[0]}..."
        emoji = "🔥" if _severity_from_text_heuristic(text) == "high" else "ℹ️"
        snippets.append(f"{emoji} {short}")
        if len(snippets) >= max_items:
            break
    if not snippets:
        return ""
    return sanitize_telegram_html("<br>".join(snippets))


def local_fallback_digest(posts: Sequence[Dict[str, object]], *, interval_minutes: int) -> str:
    if not posts:
        return quiet_period_message(interval_minutes)

    # Fallback chain:
    # 1) rule-based editorial bullets
    # 2) tiny local deterministic compression
    # 3) minimal short bullet list
    rule_based = _rule_based_fallback_digest(posts)
    if rule_based:
        return rule_based

    tiny = _tiny_local_digest(posts)
    if tiny:
        return tiny

    minimal = _minimal_digest(posts)
    if minimal:
        return minimal

    return quiet_period_message(interval_minutes)


def _query_requests_detail(query: str) -> bool:
    lowered = query.lower()
    hints = (
        "in detail",
        "detailed",
        "full report",
        "deep dive",
        "full analysis",
        "comprehensive",
    )
    return any(hint in lowered for hint in hints)


def _query_is_high_risk(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    terms = (
        "leader",
        "supreme leader",
        "president",
        "prime minister",
        "successor",
        "succession",
        "assassinated",
        "resigned",
        "coup",
        "overthrown",
        "nuclear plant",
        "nuclear site",
    )
    return any(term in lowered for term in terms)


def _query_is_identity_question(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    markers = (
        "who died",
        "who was killed",
        "who got killed",
        "who was injured",
        "who got injured",
        "names of the dead",
        "names of the victims",
        "victims names",
        "which people died",
        "identify the dead",
    )
    return any(marker in lowered for marker in markers)


def _query_is_casualty_question(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    terms = (
        "killed",
        "dead",
        "died",
        "injured",
        "wounded",
        "casualties",
        "fatalities",
        "victims",
    )
    return any(term in lowered for term in terms)


def _query_requires_multi_source_confirmation(query: str) -> bool:
    lowered = normalize_space(query).lower()
    if not lowered:
        return False
    markers = (
        "supreme leader",
        "new leader",
        "successor",
        "succession",
        "prime minister",
        "president",
        "assassinated",
        "nuclear plant",
        "nuclear site",
        "reactor",
    )
    return any(marker in lowered for marker in markers)


def _query_distinct_sources(context_messages: Sequence[Dict[str, object]]) -> int:
    sources = {
        normalize_space(str(item.get("source") or "")).lower()
        for item in context_messages
        if normalize_space(str(item.get("source") or ""))
    }
    return len(sources)


def _answer_has_definitive_high_risk_claim(answer: str) -> bool:
    lowered = normalize_space(re.sub(r"</?[^>]+>", " ", answer or "")).lower()
    if not lowered:
        return False
    markers = (
        "is dead",
        "is deceased",
        "has died",
        "was killed",
        "new leader is",
        "has been appointed",
        "confirmed dead",
        "confirmed killed",
    )
    return any(marker in lowered for marker in markers)


def _extract_text_numbers(text: str) -> set[str]:
    return {token for token in re.findall(r"\b\d{2,6}\b", normalize_space(text))}


def _query_text_tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]{3,}", normalize_space(text).lower())}


def _query_text_has_identity_markers(text: str) -> bool:
    lowered = normalize_space(text).lower()
    if not lowered:
        return False
    markers = (
        "identified",
        "named",
        "victim",
        "victims",
        "children",
        "women",
        "journalist",
        "commander",
        "official",
        "funeral",
        "martyr",
        "martyrs",
        "obituary",
        "name list",
    )
    return any(marker in lowered for marker in markers)


def _query_text_has_casualty_markers(text: str) -> bool:
    lowered = normalize_space(text).lower()
    if not lowered:
        return False
    markers = (
        "killed",
        "dead",
        "died",
        "injured",
        "wounded",
        "casualties",
        "fatalities",
        "victims",
        "death toll",
    )
    return any(marker in lowered for marker in markers)


def _query_source_reliability(item: Dict[str, object]) -> float:
    source = normalize_space(str(item.get("source") or "")).lower()
    text = normalize_space(str(item.get("text") or "")).lower()
    is_web = bool(item.get("is_web"))

    top_tier_web = (
        "reuters",
        "associated press",
        "ap news",
        "bbc",
        "al jazeera",
        "dw",
        "bloomberg",
        "financial times",
        "ft",
        "npr",
        "washington post",
        "new york times",
        "france24",
        "the guardian",
    )
    mid_tier_web = (
        "cnn",
        "cbs",
        "abc",
        "fox",
        "axios",
        "times of israel",
        "haaretz",
        "investing.com",
    )
    official_markers = (
        "ministry",
        "spokesperson",
        "official",
        "army",
        "idf",
        "irgc",
        "ukmto",
        "centcom",
    )

    score = 0.0
    if is_web:
        if any(marker in source for marker in top_tier_web):
            score += 1.8
        elif any(marker in source for marker in mid_tier_web):
            score += 1.1
        else:
            score += 0.35
    else:
        if any(marker in source for marker in official_markers) or any(marker in text for marker in official_markers):
            score += 0.9
        elif "alert" in source or "report" in source or "news" in source:
            score += 0.45
        else:
            score += 0.2

    if "facebook" in source or "x.com" in source or "twitter" in source:
        score -= 0.25
    return score


def _score_query_context_row(
    query: str,
    item: Dict[str, object],
    *,
    now_ts: int,
) -> float:
    question = normalize_space(query)
    if not question:
        return 0.0

    text = normalize_space(str(item.get("text") or ""))
    if not text:
        return 0.0

    lowered_question = question.lower()
    lowered_text = text.lower()
    query_keywords = extract_query_keywords(question)
    query_keyword_set = set(query_keywords)
    expanded_terms = {
        normalize_space(term).lower()
        for term in expand_query_terms(question)
        if normalize_space(term)
    }
    text_tokens = _query_text_tokens(text)
    query_numbers = set(extract_query_numbers(question))
    text_numbers = _extract_text_numbers(text)

    keyword_hits = len(query_keyword_set & text_tokens)
    keyword_score = (
        (keyword_hits / max(1, len(query_keyword_set))) * 3.5
        if query_keyword_set
        else 0.0
    )
    alias_hits = sum(1 for term in expanded_terms if term and term in lowered_text)
    alias_score = (
        (alias_hits / max(1, len(expanded_terms))) * 4.0
        if expanded_terms
        else 0.0
    )

    number_hits = len(query_numbers & text_numbers)
    number_score = float(number_hits) * 2.5
    if query_numbers and number_hits == len(query_numbers):
        number_score += 2.0

    direct_phrase_score = 0.0
    if lowered_question and lowered_question in lowered_text:
        direct_phrase_score += 2.0
    similarity_score = SequenceMatcher(None, lowered_question[:320], lowered_text[:900]).ratio() * 1.8

    intent_score = 0.0
    if _query_is_identity_question(question) and _query_text_has_identity_markers(text):
        intent_score += 1.4
    if _query_is_casualty_question(question) and _query_text_has_casualty_markers(text):
        intent_score += 1.2

    source = normalize_space(str(item.get("source") or ""))
    source_score = 0.15 if source else 0.0
    if str(item.get("link") or "").startswith("http"):
        source_score += 0.2
    source_score += _query_source_reliability(item)

    timestamp = int(item.get("timestamp") or 0)
    recency_score = 0.0
    if timestamp > 0 and now_ts > 0:
        age_hours = max(0.0, (now_ts - timestamp) / 3600.0)
        recency_score = max(0.0, 1.0 - min(age_hours / 48.0, 1.0))

    return (
        keyword_score
        + alias_score
        + number_score
        + direct_phrase_score
        + similarity_score
        + intent_score
        + source_score
        + recency_score
    )


def _scored_query_context(
    query: str,
    context_messages: Sequence[Dict[str, object]],
) -> list[tuple[float, Dict[str, object]]]:
    now_ts = int(time.time())
    scored: list[tuple[float, Dict[str, object]]] = []
    for item in context_messages:
        score = _score_query_context_row(query, item, now_ts=now_ts)
        scored.append((score, item))
    scored.sort(
        key=lambda pair: (
            pair[0],
            int(pair[1].get("timestamp", 0) or 0),
        ),
        reverse=True,
    )
    return scored


def _query_confidence_allows_answer(
    query: str,
    scored_rows: Sequence[tuple[float, Dict[str, object]]],
) -> bool:
    if not scored_rows:
        return False

    if is_broad_news_query(query):
        return len(scored_rows) >= 2

    top_score = float(scored_rows[0][0])
    distinct_sources = _query_distinct_sources([row for _, row in scored_rows[:6]])
    exact_number_need = bool(extract_query_numbers(query))
    identity_query = _query_is_identity_question(query)
    top_text = normalize_space(str(scored_rows[0][1].get("text") or ""))

    if exact_number_need:
        top_numbers = _extract_text_numbers(top_text)
        if not (set(extract_query_numbers(query)) & top_numbers) and top_score < 4.5:
            return False

    if identity_query:
        return top_score >= 4.2 or distinct_sources >= 2

    return top_score >= 2.6 or distinct_sources >= 2


def _rank_query_context(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    *,
    limit: int = 18,
) -> list[Dict[str, object]]:
    scored = _scored_query_context(query, context_messages)

    out: list[Dict[str, object]] = []
    per_source: dict[str, int] = {}
    for score, item in scored:
        if score <= 0:
            continue
        source = normalize_space(str(item.get("source") or "unknown")).lower()
        if per_source.get(source, 0) >= 3:
            continue
        per_source[source] = per_source.get(source, 0) + 1
        out.append(item)
        if len(out) >= max(6, int(limit)):
            break

    return out if out else list(context_messages[: max(6, int(limit))])


def _query_context_token_budget() -> int:
    raw = getattr(config, "DIGEST_MAX_TOKENS", 18000)
    try:
        value = int(raw)
    except Exception:
        value = 18000
    return max(4000, min(value, 18000))


def _build_query_context_lines(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    *,
    token_budget: int,
) -> tuple[list[str], int]:
    lines: list[str] = []
    used = 0
    seen = set()
    ranked_messages = _rank_query_context(query, context_messages, limit=24)

    for idx, item in enumerate(ranked_messages, start=1):
        text = normalize_space(str(item.get("text") or ""))
        if not text:
            continue
        signature = _cache_key(text.lower())
        if signature in seen:
            continue
        seen.add(signature)

        source = normalize_space(str(item.get("source") or "unknown"))
        date_label = normalize_space(str(item.get("date") or "unknown-time"))
        link = normalize_space(str(item.get("link") or ""))
        source_reliability = round(_query_source_reliability(item), 2)
        source_kind = "web" if bool(item.get("is_web")) else "telegram"
        if len(text) > 900:
            text = f"{text[:897].rsplit(' ', 1)[0]}..."

        line = (
            f'{idx}. source_name="{source}" | source_kind="{source_kind}" '
            f'| source_reliability={source_reliability} | date="{date_label}" | text="{text}"'
        )
        if link.startswith("http"):
            line += f' | url="{link}"'

        line_tokens = estimate_tokens_rough(line) + 10
        if lines and used + line_tokens > token_budget:
            break
        lines.append(line)
        used += line_tokens

    return lines, used


def _fallback_query_answer(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    *,
    detailed: bool,
) -> str:
    if not context_messages:
        return QUERY_NO_MATCH_TEXT

    selected = _rank_query_context(query, context_messages, limit=(8 if detailed else 4))
    selected = selected[: (6 if detailed else 4)]
    lines = []
    intro_title = "What stands out right now"
    normalized_query = normalize_space(query).rstrip("?")
    keywords = extract_query_keywords(query)
    if _query_is_identity_question(query):
        intro_title = "Best available answer"
    elif _query_is_casualty_question(query):
        intro_title = "What the evidence shows"
    elif is_broad_news_query(query):
        if len(keywords) == 1:
            intro_title = f"Latest on {keywords[0].title()}"
        elif len(keywords) >= 2:
            intro_title = f"Latest on {keywords[0].title()} and {keywords[1].title()}"
    elif normalized_query and len(normalized_query) <= 72:
        intro_title = normalized_query[:1].upper() + normalized_query[1:]
    for item in selected:
        text = normalize_space(str(item.get("text") or ""))
        if not text:
            continue
        text = re.sub(
            r"^(?:breaking|alert|live update|update)\s*[:\-–—]+\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )
        if len(text) > 180:
            text = f"{text[:177].rsplit(' ', 1)[0]}..."
        prefix = "🔥" if _severity_from_text_heuristic(text) == "high" else "⚠️"
        line = f"• {prefix} {text}"
        lines.append(line)

    if not lines:
        return QUERY_NO_MATCH_TEXT
    return sanitize_telegram_html(f"<b>{intro_title}</b><br>" + "<br>".join(lines))


async def decide_filter_action(text: str, auth_manager: AuthManager) -> FilterDecision:
    global _FILTER_DECISION_CACHE_HITS, _FILTER_DECISION_CACHE_MISSES

    cleaned = normalize_space(text)
    if len(cleaned) < 10:
        return _fallback_filter_decision(cleaned)

    normalized_hash = _cache_key(cleaned)
    model = _resolve_codex_model()
    cached_json = ai_decision_cache_get(
        normalized_hash=normalized_hash,
        prompt_version=FILTER_DECISION_PROMPT_VERSION,
        model=model,
        max_age_hours=_resolve_ai_decision_cache_hours(),
    )
    if cached_json:
        cached_payload = _try_parse_digest_json(cached_json)
        if cached_payload is not None:
            decision = _validate_filter_decision(cached_payload, cleaned)
            decision.cached = True
            _FILTER_DECISION_CACHE_HITS += 1
            return decision

    _FILTER_DECISION_CACHE_MISSES += 1

    if _likely_noise(cleaned):
        decision = _fallback_filter_decision(cleaned)
        ai_decision_cache_set(
            normalized_hash=normalized_hash,
            prompt_version=FILTER_DECISION_PROMPT_VERSION,
            model=model,
            decision_json=json.dumps(decision.__dict__, separators=(",", ":")),
        )
        return decision

    compact = cleaned if len(cleaned) <= 2400 else f"{cleaned[:2397].rsplit(' ', 1)[0]}..."

    try:
        auth_context = await auth_manager.get_auth_context()
        raw = await _call_codex(
            compact,
            auth_context,
            instructions=_filter_decision_system_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            raw = await _call_codex(
                compact,
                auth_context,
                instructions=_filter_decision_system_prompt(),
                verbosity="low",
            )
        except Exception:
            return _fallback_filter_decision(cleaned)
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        return _fallback_filter_decision(cleaned)
    except Exception:
        return _fallback_filter_decision(cleaned)

    payload = _try_parse_digest_json(raw)
    decision = _validate_filter_decision(payload or {}, cleaned)
    ai_decision_cache_set(
        normalized_hash=normalized_hash,
        prompt_version=FILTER_DECISION_PROMPT_VERSION,
        model=model,
        decision_json=json.dumps(decision.__dict__, separators=(",", ":")),
    )
    return decision


async def summarize_or_skip(text: str, auth_manager: AuthManager) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned or len(cleaned) < 10:
        return None

    key = _cache_key(cleaned)
    cached = _cache_get(key)
    if cached is not None or key in _SUMMARY_CACHE:
        return cached

    if _likely_noise(cleaned):
        _cache_set(key, None)
        return None

    try:
        decision = await decide_filter_action(cleaned, auth_manager)
    except Exception:
        fallback = _fallback_summary(cleaned)
        _cache_set(key, fallback)
        return fallback

    if decision.action == "skip" or not decision.summary_html:
        _cache_set(key, None)
        return None

    _cache_set(key, decision.summary_html)
    return decision.summary_html


def _normalize_severity_label(text: str) -> Literal["high", "medium", "low"] | None:
    cleaned = normalize_space(text).lower()
    if not cleaned:
        return None

    tokens = re.findall(r"[a-z]+", cleaned)
    for token in tokens:
        if token in {"high", "medium", "low"}:
            return token  # type: ignore[return-value]
        if token in {"critical", "urgent", "severe"}:
            return "high"
        if token in {"moderate"}:
            return "medium"
        if token in {"minor"}:
            return "low"
    return None


async def classify_severity(
    text: str,
    auth_manager: AuthManager,
) -> Literal["high", "medium", "low"]:
    cleaned = text.strip()
    if not cleaned:
        return "low"

    key = _cache_key(f"severity::{cleaned}")
    cached = _severity_cache_get(key)
    if cached in {"high", "medium", "low"}:
        return cached  # type: ignore[return-value]

    heuristic = _severity_from_text_heuristic(cleaned)
    if _likely_noise(cleaned):
        _severity_cache_set(key, "low")
        return "low"

    compact = cleaned if len(cleaned) <= 1800 else f"{cleaned[:1797].rsplit(' ', 1)[0]}..."

    try:
        auth_context = await auth_manager.get_auth_context()
        raw = await _call_codex(
            compact,
            auth_context,
            instructions=_severity_system_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            raw = await _call_codex(
                compact,
                auth_context,
                instructions=_severity_system_prompt(),
                verbosity="low",
            )
        except Exception:
            _severity_cache_set(key, heuristic)
            return heuristic
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        _severity_cache_set(key, heuristic)
        return heuristic
    except Exception:
        _severity_cache_set(key, heuristic)
        return heuristic

    parsed = _normalize_severity_label(raw)
    if parsed is None:
        parsed = heuristic
    _severity_cache_set(key, parsed)
    return parsed


def _cleanup_headline(raw: str) -> str:
    def _looks_incomplete(value: str) -> bool:
        trimmed = normalize_space(value)
        if not trimmed:
            return True
        if trimmed.endswith(("...", "…", "(", "[", "{", ":", ";", "-", "–", "—", "/")):
            return True
        if trimmed.count("(") > trimmed.count(")"):
            return True
        tail = trimmed.split()[-1].strip(".,;:!?").lower()
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
            "over",
            "under",
            "into",
            "toward",
            "towards",
            "linked",
        }:
            return True
        return False

    text = normalize_space(raw)
    if not text:
        return ""
    text = re.sub(r"^[*`#>\-\s]+", "", text).strip()
    text = re.sub(
        r"^(?:breaking|alert|live update|update|urgent)\s*[:\-–—]+\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\[/?[A-Za-z0-9_ -]+\]", "", text).strip()
    if "\n" in text:
        text = text.splitlines()[0].strip()
    if _looks_incomplete(text):
        return ""
    if len(text) > 420:
        sentence_match = re.search(r"(.{24,700}?[.!?])(?:\s|$)", text)
        if sentence_match:
            text = normalize_space(sentence_match.group(1))
        else:
            text = f"{text[:417].rsplit(' ', 1)[0]}..."
    return text


def _cleanup_vital_view(raw: str) -> str:
    def _trim_tail_fragment(value: str) -> str:
        connectors = {
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
            "after",
            "before",
        }
        text_value = normalize_space(value)
        if not text_value:
            return text_value
        words = text_value.split()
        while words:
            tail = words[-1].strip(".,;:!?").lower()
            if tail in connectors:
                words.pop()
                continue
            break
        return " ".join(words).strip()

    def _finalize_sentence(value: str) -> str:
        text_value = normalize_space(value).rstrip(".,;:!?")
        if not text_value:
            return ""
        return f"{text_value}."

    raw_text = str(raw or "")
    if not raw_text.strip():
        return ""
    if raw_text.strip().upper() == "SKIP":
        return ""
    raw_text = re.sub(r"<br\s*/?>", "\n", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"</?[^>]+>", "", raw_text).strip()
    try:
        max_words = int(getattr(config, "HUMANIZED_VITAL_OPINION_MAX_WORDS", 20))
    except Exception:
        max_words = 20
    max_words = max(10, min(max_words, 32))
    if _breaking_style_is_unhinged():
        total_word_budget = max(16, min(max_words * 2, 40))
        lines: list[str] = []
        for line in raw_text.splitlines():
            cleaned_line = normalize_space(re.sub(r"^[*`#>\-\s]+", "", line))
            cleaned_line = re.sub(r"^\s*why it matters:\s*", "", cleaned_line, flags=re.IGNORECASE)
            cleaned_line = _trim_tail_fragment(cleaned_line)
            cleaned_line = cleaned_line.rstrip(".,;:!?")
            if cleaned_line:
                lines.append(cleaned_line)
        if not lines:
            return ""
        lines[0] = f"Why it matters: {lines[0]}"
        output_lines: list[str] = []
        used_words = 0
        for line in lines[:3]:
            words = line.split()
            remaining = total_word_budget - used_words
            if remaining <= 0:
                break
            if len(words) > remaining:
                line = " ".join(words[:remaining]).rstrip(".,;:!?")
            finalized = _finalize_sentence(line)
            if not finalized:
                continue
            output_lines.append(finalized)
            used_words += len(finalized.split())
        return "\n".join(output_lines)

    text = normalize_space(raw_text)
    if not text:
        return ""
    if "\n" in text:
        text = text.splitlines()[0].strip()
    if not text:
        return ""
    if not text.lower().startswith("why it matters:"):
        text = f"Why it matters: {text}"
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words])
    text = _trim_tail_fragment(text)
    if text.lower().startswith("why it matters:"):
        detail = normalize_space(text.split(":", 1)[1] if ":" in text else "")
        if not detail:
            return ""
    return _finalize_sentence(text)


_VITAL_GENERIC_DETAILS = {
    "this may affect regional stability; verify updates across multiple reliable sources",
    "this may affect regional stability",
    "civilian danger is increasing",
    "this keeps escalation risk high",
    "this raises immediate civilian-safety risks",
    "this can strain critical infrastructure",
    "the same story is still unfolding",
}
_VITAL_GENERIC_FRAGMENTS = (
    "verify updates across multiple reliable sources",
    "regional stability",
    "civilian-safety risks",
    "civilian danger",
    "critical infrastructure",
    "the same story is still unfolding",
    "worth watching",
    "raises escalation risk",
    "shows the situation is still serious",
    "big escalation",
    "serious escalation",
)
_VITAL_UNSUPPORTED_IMPLICATION_FRAGMENTS = (
    "all-out war",
    "full-scale war",
    "regime collapse",
    "everything is now collapsing",
    "proves",
    "confirms a wider war",
)
_VITAL_CONTINUITY_MARKERS = (
    "earlier",
    "before",
    "after",
    "follows",
    "followed",
    "this now",
    "same thread",
    "same exchange",
    "same operation",
    "confirms",
    "confirm",
    "reaching",
    "reached",
    "centered on",
    "pushes",
    "pulls",
    "widens",
    "spreads",
    "next turn",
    "moved from",
    "shifted from",
)
_VITAL_CONFIRMATION_MARKERS = (
    "confirmed",
    "confirms",
    "confirmation",
    "official",
    "officials",
    "statement",
    "spokesperson",
    "ministry",
)
_VITAL_RESPONSE_MARKERS = (
    "retaliation",
    "retaliatory",
    "response",
    "responded",
    "responds",
    "in response",
    "after earlier",
    "following earlier",
)
_VITAL_RELATION_STOPWORDS = {
    "after",
    "alert",
    "alerts",
    "around",
    "earlier",
    "late",
    "local",
    "near",
    "night",
    "official",
    "officials",
    "overnight",
    "report",
    "reports",
    "said",
    "says",
    "statement",
    "update",
}
_VITAL_EVENT_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("strike", ("airstrike", "airstrikes", "strike", "strikes", "raid", "raids", "bombardment", "targeted", "hit", "hits")),
    ("interception", ("interception", "interceptions", "intercepted", "intercepts", "air defenses", "air defence", "shot down", "downed")),
    ("launch", ("rocket", "rockets", "missile", "missiles", "drone", "drones", "launch", "launched")),
    ("explosion", ("explosion", "explosions", "blast", "blasts", "detonation")),
    ("statement", ("statement", "announced", "announces", "confirmed", "confirms", "official", "officials", "spokesperson", "ministry")),
    ("warning", ("warning", "warnings", "alert", "alerts", "sirens", "evacuation")),
)
_VITAL_EVENT_LABELS = {
    "strike": "strikes",
    "interception": "interceptions",
    "launch": "launches",
    "explosion": "explosions",
    "statement": "official confirmation",
    "warning": "alerts",
}

_VITAL_ASSET_PATTERNS: tuple[str, ...] = (
    "THAAD battery",
    "THAAD radar",
    "THAAD",
    "radar",
    "airspace",
    "air defenses",
    "air defence",
    "naval base",
    "air base",
    "airbase",
    "airport",
    "port",
    "embassy",
    "reactor",
    "nuclear plant",
    "power plant",
    "oil field",
    "gas field",
    "industrial area",
    "industrial zone",
    "hospital",
    "school",
    "camp",
    "vessel",
    "ship",
    "tanker",
)

_VITAL_PRIORITY_LOCATIONS: tuple[str, ...] = (
    "Strait of Hormuz",
    "Gulf of Oman",
    "Bab al-Mandeb",
    "Red Sea",
    "Persian Gulf",
    "Gulf of Aden",
)

_VITAL_LOCATION_RE = re.compile(
    r"\b(?:in|near|over|around|at|off|outside|inside|north of|south of|east of|west of)\s+"
    r"(?:the\s+)?"
    r"((?:(?:north|south|east|west|northern|southern|eastern|western|central)\s+)?"
    r"[A-Z][A-Za-z0-9'/-]*(?:\s+[A-Z][A-Za-z0-9'/-]*){0,4})"
)
_VITAL_NAMED_ASSET_RE = re.compile(
    r"((?:[A-Z][A-Za-z0-9'/-]*|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9'/-]*|[A-Z]{2,})){0,3}\s+"
    r"(?:industrial area|industrial zone|naval base|air base|airbase|airport|port|embassy|"
    r"oil field|gas field|power plant|hospital|school|camp|battery|radar))"
)


def _is_bad_location_candidate(value: str) -> bool:
    lowered = normalize_space(value).lower()
    if not lowered:
        return True
    actor_markers = (
        "the idf",
        "idf",
        "the army",
        "army",
        "government",
        "ministry",
        "officials",
        "police",
        "military",
        "spokesperson",
    )
    return any(marker == lowered or lowered.startswith(f"{marker} ") for marker in actor_markers)


def _usable_vital_location(value: str) -> str:
    candidate = normalize_space(value)
    if len(candidate) < 3:
        return ""
    if candidate.lower() in {"a", "an", "the"}:
        return ""
    if _is_bad_location_candidate(candidate):
        return ""
    return candidate


def _usable_vital_focus(value: str) -> str:
    candidate = normalize_space(value)
    if len(candidate) < 3:
        return ""
    if candidate.lower() in {"a", "an", "the"}:
        return ""
    return candidate


def _extract_vital_location(text: str) -> str:
    clean = normalize_space(strip_telegram_html(text))
    if not clean:
        return ""
    lowered = clean.lower()
    for location in _VITAL_PRIORITY_LOCATIONS:
        if location.lower() in lowered:
            return location
    location_match = _VITAL_LOCATION_RE.search(clean)
    if location_match:
        return normalize_space(location_match.group(1))

    lead_match = re.match(r"([A-Z][A-Za-z0-9'/-]*(?:\s+[A-Z][A-Za-z0-9'/-]*){0,3})", clean)
    if lead_match:
        candidate = normalize_space(lead_match.group(1))
        if not _is_bad_location_candidate(candidate):
            return candidate
    return ""


def _extract_vital_focus(text: str) -> str:
    clean = normalize_space(strip_telegram_html(text))
    if not clean:
        return ""

    named_asset = _VITAL_NAMED_ASSET_RE.search(clean)
    if named_asset:
        return normalize_space(named_asset.group(1))

    for pattern in _VITAL_ASSET_PATTERNS:
        match = re.search(rf"\b{re.escape(pattern)}\b", clean, flags=re.IGNORECASE)
        if match:
            return clean[match.start() : match.end()]

    return _extract_vital_location(clean)


def _extract_vital_event_label(text: str) -> str:
    lowered = normalize_space(strip_telegram_html(text)).lower()
    if not lowered:
        return ""
    for label, patterns in _VITAL_EVENT_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return label
    return ""


def _split_story_bridge_context_entry(text: str) -> tuple[str, str]:
    clean = normalize_space(strip_telegram_html(text))
    if not clean:
        return "", ""
    match = re.match(r"^(moments ago|\d+[mh] ago)\s*:\s*(.+)$", clean, flags=re.IGNORECASE)
    if match:
        return normalize_space(match.group(1)), normalize_space(match.group(2))
    return "", clean


def _story_bridge_time_prefix(age_label: str) -> str:
    clean = normalize_space(age_label)
    if not clean:
        return "Earlier, "
    return f"{clean}, "


def _extract_story_bridge_context_markers(text: str) -> set[str]:
    clean = normalize_space(strip_telegram_html(text))
    markers: set[str] = set()
    location = _usable_vital_location(_extract_vital_location(clean))
    focus = _usable_vital_focus(_extract_vital_focus(clean))
    event = _extract_vital_event_label(clean)
    if location:
        markers.add(location.lower())
    if focus:
        markers.add(focus.lower())
    if event:
        markers.add(_VITAL_EVENT_LABELS.get(event, event).lower())
    return markers


def _extract_story_bridge_shared_tokens(current_text: str, anchor_text: str) -> set[str]:
    current_tokens = _extract_latin_tokens(current_text)
    anchor_tokens = _extract_latin_tokens(anchor_text)
    return {
        token
        for token in (current_tokens & anchor_tokens)
        if len(token) >= 4 and token not in _VITAL_RELATION_STOPWORDS
    }


def _derive_story_bridge_relation(current_text: str, anchor_text: str) -> tuple[str | None, dict[str, str]]:
    current_clean = normalize_space(strip_telegram_html(current_text))
    anchor_clean = normalize_space(strip_telegram_html(anchor_text))
    if not current_clean or not anchor_clean:
        return None, {}

    current_location = _usable_vital_location(_extract_vital_location(current_clean))
    anchor_location = _usable_vital_location(_extract_vital_location(anchor_clean))
    current_focus = _usable_vital_focus(_extract_vital_focus(current_clean))
    anchor_focus = _usable_vital_focus(_extract_vital_focus(anchor_clean))
    current_event = _extract_vital_event_label(current_clean)
    anchor_event = _extract_vital_event_label(anchor_clean)
    current_lower = current_clean.lower()
    anchor_lower = anchor_clean.lower()
    shared_tokens = _extract_story_bridge_shared_tokens(current_clean, anchor_clean)
    details = {
        "current_location": current_location,
        "anchor_location": anchor_location,
        "current_focus": current_focus,
        "anchor_focus": anchor_focus,
        "current_event": current_event,
        "anchor_event": anchor_event,
    }

    if any(marker in current_lower for marker in _VITAL_CONFIRMATION_MARKERS) and _text_has_hedge_markers(anchor_lower):
        return "confirmation", details
    if (
        current_location
        and anchor_location
        and current_location.lower() != anchor_location.lower()
        and (shared_tokens or (current_event and anchor_event))
    ):
        return "spread", details
    if (
        current_focus
        and anchor_focus
        and current_focus.lower() != anchor_focus.lower()
        and (
            shared_tokens
            or (current_event and anchor_event)
            or (current_location and anchor_location and current_location.lower() == anchor_location.lower())
        )
    ):
        return "focus_shift", details
    if any(marker in current_lower for marker in _VITAL_RESPONSE_MARKERS) and (anchor_event or anchor_focus or anchor_location):
        return "response_chain", details
    if current_event and anchor_event and current_event != anchor_event:
        return "phase_shift", details
    if current_location and anchor_location and current_location.lower() == anchor_location.lower() and (shared_tokens or current_event or anchor_event):
        return "continuation", details
    if current_focus and anchor_focus and current_focus.lower() == anchor_focus.lower() and (shared_tokens or current_event or anchor_event):
        return "continuation", details
    return None, details


def _render_story_bridge_relation(kind: str, details: dict[str, str], age_label: str) -> str:
    prefix = _story_bridge_time_prefix(age_label)
    current_location = normalize_space(details.get("current_location"))
    anchor_location = normalize_space(details.get("anchor_location"))
    current_focus = normalize_space(details.get("current_focus"))
    anchor_focus = normalize_space(details.get("anchor_focus"))
    current_event = normalize_space(_VITAL_EVENT_LABELS.get(details.get("current_event", ""), details.get("current_event", "")))
    anchor_event = normalize_space(_VITAL_EVENT_LABELS.get(details.get("anchor_event", ""), details.get("anchor_event", "")))

    if kind == "confirmation":
        anchor_target = anchor_location or anchor_focus or anchor_event
        if anchor_target:
            return f"Why it matters: {prefix}this appears to confirm the earlier report around {anchor_target}."
        return ""
    if kind == "spread" and current_location and anchor_location:
        return (
            f"Why it matters: {prefix}the pressure was centered on {anchor_location}; "
            f"this now pushes the same thread into {current_location}."
        )
    if kind == "focus_shift" and current_focus and anchor_focus:
        return (
            f"Why it matters: {prefix}alerts were centered on {anchor_focus}; "
            f"this now pulls {current_focus} into the same thread."
        )
    if kind == "response_chain":
        anchor_target = anchor_focus or anchor_location or anchor_event
        if anchor_target:
            return f"Why it matters: {prefix}this looks like the next turn after earlier pressure around {anchor_target}."
        return ""
    if kind == "phase_shift":
        if anchor_event and current_event and anchor_location and current_location and anchor_location.lower() != current_location.lower():
            return (
                f"Why it matters: {prefix}the story has moved from {anchor_event} around {anchor_location} "
                f"to {current_event} around {current_location}."
            )
        if anchor_event and current_event:
            anchor_target = anchor_location or anchor_focus
            current_target = current_location or current_focus
            if anchor_target and current_target and anchor_target.lower() != current_target.lower():
                return (
                    f"Why it matters: {prefix}the story has shifted from {anchor_event} around {anchor_target} "
                    f"to {current_event} around {current_target}."
                )
            return f"Why it matters: {prefix}the story has moved from {anchor_event} into {current_event}."
    if kind == "continuation":
        if current_location:
            return f"Why it matters: {prefix}the same exchange is still centered on {current_location}."
        if current_focus:
            return f"Why it matters: {prefix}{current_focus} is still at the center of the same thread."
    return ""


def _vital_view_is_too_generic(text: str) -> bool:
    lowered = normalize_space(strip_telegram_html(text)).lower()
    if not lowered:
        return True
    detail = lowered
    if detail.startswith("why it matters:"):
        detail = normalize_space(detail.split(":", 1)[1])
    if not detail:
        return True
    if detail in _VITAL_GENERIC_DETAILS:
        return True
    return any(fragment in detail for fragment in _VITAL_GENERIC_FRAGMENTS)


def _recent_context_bridge_input(current_text: str, recent_context: Sequence[str] | None) -> str:
    current = normalize_space(strip_telegram_html(current_text))
    lines = [f"Current update: {current}"]
    cleaned_context: list[str] = []
    for item in recent_context or ():
        value = normalize_space(strip_telegram_html(str(item)))
        if not value:
            continue
        cleaned_context.append(value)
    if cleaned_context:
        lines.append("Recent related updates:")
        lines.extend(f"- {item}" for item in cleaned_context[:3])
    return "\n".join(lines)


def _vital_view_is_contextual(
    text: str,
    current_text: str,
    recent_context: Sequence[str] | None,
) -> bool:
    lowered = normalize_space(strip_telegram_html(text)).lower()
    if not lowered:
        return False
    if _vital_view_is_too_generic(lowered):
        return False
    if any(fragment in lowered for fragment in _VITAL_UNSUPPORTED_IMPLICATION_FRAGMENTS):
        return False
    if not any(marker in lowered for marker in _VITAL_CONTINUITY_MARKERS):
        return False

    current_clean = normalize_space(strip_telegram_html(current_text)).lower()
    if current_clean and SequenceMatcher(None, lowered, current_clean).ratio() >= 0.9:
        return False

    context_markers: set[str] = set()
    for item in recent_context or ():
        _age, body = _split_story_bridge_context_entry(str(item))
        context_markers.update(_extract_story_bridge_context_markers(body))
    if not context_markers:
        return False
    return any(marker in lowered for marker in context_markers)


def _fallback_story_bridge(current_text: str, recent_context: Sequence[str] | None) -> str | None:
    for item in recent_context or ():
        age_label, body = _split_story_bridge_context_entry(str(item))
        if not body:
            continue
        relation, details = _derive_story_bridge_relation(current_text, body)
        if not relation:
            continue
        fallback = _render_story_bridge_relation(relation, details, age_label)
        if fallback and _vital_view_is_contextual(fallback, current_text, recent_context):
            return fallback
    return None


def resolve_vital_rational_view_for_delivery(
    current_text: str,
    candidate: str | None,
    recent_context: Sequence[str] | None,
) -> str:
    if not recent_context:
        return ""
    cleaned_candidate = _cleanup_vital_view(str(candidate or ""))
    if cleaned_candidate and _vital_view_is_contextual(cleaned_candidate, current_text, recent_context):
        return cleaned_candidate
    fallback = _cleanup_vital_view(_fallback_story_bridge(current_text, recent_context) or "")
    if fallback and _vital_view_is_contextual(fallback, current_text, recent_context):
        return fallback
    return ""


async def summarize_breaking_headline(
    text: str,
    auth_manager: AuthManager,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned:
        return None

    key = _cache_key(f"headline::{resolve_breaking_style_mode()}::{cleaned}")
    cached = _headline_cache_get(key)
    if cached is not None or key in _HEADLINE_CACHE:
        return cached

    compact = cleaned if len(cleaned) <= 1800 else f"{cleaned[:1797].rsplit(' ', 1)[0]}..."

    try:
        auth_context = await auth_manager.get_auth_context()
        raw = await _call_codex(
            compact,
            auth_context,
            instructions=_breaking_headline_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            raw = await _call_codex(
                compact,
                auth_context,
                instructions=_breaking_headline_prompt(),
                verbosity="low",
            )
        except Exception:
            fallback = resolve_breaking_headline_for_delivery(cleaned, None)
            _headline_cache_set(key, fallback)
            return fallback
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        fallback = resolve_breaking_headline_for_delivery(cleaned, None)
        _headline_cache_set(key, fallback)
        return fallback
    except Exception:
        fallback = resolve_breaking_headline_for_delivery(cleaned, None)
        _headline_cache_set(key, fallback)
        return fallback

    headline = resolve_breaking_headline_for_delivery(cleaned, raw)
    if headline:
        _headline_cache_set(key, headline)
        return headline

    fallback = resolve_breaking_headline_for_delivery(cleaned, None)
    _headline_cache_set(key, fallback)
    return fallback


async def summarize_vital_rational_view(
    text: str,
    auth_manager: AuthManager,
    *,
    recent_context: Sequence[str] | None = None,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned:
        return None

    cleaned_context = [
        normalize_space(strip_telegram_html(str(item)))
        for item in (recent_context or ())
        if normalize_space(strip_telegram_html(str(item)))
    ]
    if not cleaned_context:
        return None

    context_key = "||".join(cleaned_context[:3])
    key = _cache_key(
        f"vital_view::{VITAL_VIEW_PROMPT_VERSION}::{resolve_breaking_style_mode()}::{cleaned}::{context_key}"
    )
    cached = _vital_view_cache_get(key)
    if cached is not None or key in _VITAL_VIEW_CACHE:
        return cached

    compact = _recent_context_bridge_input(cleaned, cleaned_context)
    fallback = resolve_vital_rational_view_for_delivery(cleaned, None, cleaned_context)

    try:
        auth_context = await auth_manager.get_auth_context()
        raw = await _call_codex(
            compact,
            auth_context,
            instructions=_vital_rational_view_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            raw = await _call_codex(
                compact,
                auth_context,
                instructions=_vital_rational_view_prompt(),
                verbosity="low",
            )
        except Exception:
            _vital_view_cache_set(key, fallback)
            return fallback
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        _vital_view_cache_set(key, fallback)
        return fallback
    except Exception:
        _vital_view_cache_set(key, fallback)
        return fallback

    view = resolve_vital_rational_view_for_delivery(cleaned, raw, cleaned_context)
    if view:
        _vital_view_cache_set(key, view)
        return view

    _vital_view_cache_set(key, fallback)
    return fallback


async def translate_ocr_text_to_english(
    text: str,
    auth_manager: AuthManager,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned or len(cleaned) < 8:
        return None

    key = _cache_key(f"ocr_translate::{cleaned}")
    cached = _ocr_translation_cache_get(key)
    if cached is not None or key in _OCR_TRANSLATION_CACHE:
        return cached

    compact = cleaned if len(cleaned) <= 2400 else f"{cleaned[:2397].rsplit(' ', 1)[0]}..."

    try:
        auth_context = await auth_manager.get_auth_context()
        raw = await _call_codex(
            compact,
            auth_context,
            instructions=_ocr_translation_prompt(),
            verbosity="low",
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            raw = await _call_codex(
                compact,
                auth_context,
                instructions=_ocr_translation_prompt(),
                verbosity="low",
            )
        except Exception:
            _ocr_translation_cache_set(key, None)
            return None
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        _ocr_translation_cache_set(key, None)
        return None
    except Exception:
        _ocr_translation_cache_set(key, None)
        return None

    translated = normalize_space(raw)
    if not translated or translated.upper().startswith("SKIP"):
        _ocr_translation_cache_set(key, None)
        return None

    safe = sanitize_telegram_html(translated)
    _ocr_translation_cache_set(key, safe or None)
    return safe or None


async def create_digest_summary(
    posts: List[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    interval_minutes: int | None = None,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    """
    Build one editorial digest from queued posts.

    Returns Telegram-HTML digest body or the exact quiet-period sentence.
    """
    if interval_minutes is None:
        interval_minutes = int(max(1, int(getattr(config, "DIGEST_INTERVAL_MINUTES", 30))))
    else:
        interval_minutes = int(max(1, interval_minutes))
    quiet_text = quiet_period_message(interval_minutes)
    if not posts:
        return quiet_text

    manager = auth_manager or AuthManager(logger=LOGGER)
    max_posts = _resolve_digest_max_posts()
    max_lines = _resolve_digest_max_lines()

    token_budget = _resolve_digest_token_budget()
    # Reserve room for instructions + response generation.
    context_budget = max(1000, token_budget - 2200)

    lines, included_count, total_input = _build_digest_context(
        posts,
        token_budget=context_budget,
        max_posts=max_posts,
    )
    if not lines or included_count == 0:
        return quiet_text

    prefer_json = bool(getattr(config, "DIGEST_PREFER_JSON_OUTPUT", True))
    # JSON mode streams poorly for live UX; force markdown when streaming edits are enabled.
    if on_token is not None:
        prefer_json = False
    importance_scoring = bool(getattr(config, "DIGEST_IMPORTANCE_SCORING", True))
    include_links = False
    output_language = _resolve_output_language()

    user_payload = (
        "Batch metadata:\n"
        f"- Total queued posts: {total_input}\n"
        f"- Included after dedupe/noise/token budget: {included_count}\n"
        f"- Estimated input tokens: {sum(estimate_tokens_rough(x) for x in lines)}\n\n"
        + build_digest_input_block(lines)
    )

    try:
        auth_context = await manager.get_auth_context()
    except Exception:
        return local_fallback_digest(posts, interval_minutes=interval_minutes)

    prompt = build_digest_system_prompt(
        interval_minutes=interval_minutes,
        json_mode=prefer_json,
        importance_scoring=importance_scoring,
        include_links=include_links,
        output_language=output_language,
        include_source_tags=False,
    )

    try:
        content = await _call_codex(
            user_payload,
            auth_context,
            instructions=prompt,
            on_token=on_token,
        )
    except _CodexAuthError:
        try:
            auth_context = await manager.refresh_auth_context()
            content = await _call_codex(
                user_payload,
                auth_context,
                instructions=prompt,
                on_token=on_token,
            )
        except Exception:
            return local_fallback_digest(posts, interval_minutes=interval_minutes)
    except _CodexRateLimitError as exc:
        global _QUOTA_WARNING_LOGGED
        if not _QUOTA_WARNING_LOGGED:
            LOGGER.error(
                "Codex subscription limit reached (429) during digest generation. "
                "Using local fallback digest. %s",
                exc,
            )
            _QUOTA_WARNING_LOGGED = True
        return local_fallback_digest(posts, interval_minutes=interval_minutes)
    except (httpx.HTTPError, _CodexApiError, ValueError) as exc:
        LOGGER.error("Codex digest call failed, using local fallback digest: %s", exc)
        return local_fallback_digest(posts, interval_minutes=interval_minutes)
    except Exception:
        return local_fallback_digest(posts, interval_minutes=interval_minutes)

    if not content.strip():
        return local_fallback_digest(posts, interval_minutes=interval_minutes)

    if prefer_json:
        parsed = _try_parse_digest_json(content)
        if parsed is not None:
            return await _normalize_digest_output(
                _json_digest_to_html(
                    parsed,
                    interval_minutes=interval_minutes,
                    max_lines=max_lines,
                ),
                manager,
                interval_minutes=interval_minutes,
                max_lines=max_lines,
            )
        # Fallback to explicit markdown retry if JSON parse failed.
        markdown_prompt = build_digest_system_prompt(
            interval_minutes=interval_minutes,
            json_mode=False,
            importance_scoring=importance_scoring,
            include_links=include_links,
            output_language=output_language,
            include_source_tags=False,
        )
        try:
            content = await _call_codex(
                user_payload,
                auth_context,
                instructions=markdown_prompt,
                on_token=on_token,
            )
        except Exception:
            return local_fallback_digest(posts, interval_minutes=interval_minutes)

    return await _normalize_digest_output(
        content,
        manager,
        interval_minutes=interval_minutes,
        max_lines=max_lines,
    )


async def generate_answer_from_context(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    conversation_history: Sequence[Dict[str, str]] | None = None,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    """
    Generate a conversational query answer from recent Telegram context messages.
    """
    question = normalize_space(query)
    if not question:
        return QUERY_NO_MATCH_TEXT

    if not context_messages:
        return QUERY_NO_MATCH_TEXT

    manager = auth_manager or AuthManager(logger=LOGGER)
    detailed = _query_requests_detail(question)
    high_risk_query = _query_is_high_risk(question)
    strict_confirmation_query = _query_requires_multi_source_confirmation(question)
    scored_context = _scored_query_context(question, context_messages)
    ranked_context = _rank_query_context(question, context_messages, limit=24)
    distinct_sources = _query_distinct_sources(ranked_context[:10] or context_messages)
    output_language = _resolve_output_language()

    # Strict guardrail for high-risk queries (leadership/death/succession):
    # if evidence diversity is weak, do not produce potentially false claims.
    if strict_confirmation_query and distinct_sources < 2:
        return QUERY_NO_MATCH_TEXT
    if not _query_confidence_allows_answer(question, scored_context[:12]):
        return QUERY_NO_MATCH_TEXT

    # Reserve room for prompt + answer generation.
    context_budget = max(2000, _query_context_token_budget() - 2600)
    context_lines, used_tokens = _build_query_context_lines(
        question,
        ranked_context,
        token_budget=context_budget,
    )
    if not context_lines:
        return QUERY_NO_MATCH_TEXT

    history_lines: list[str] = []
    if conversation_history:
        for turn in list(conversation_history)[-10:]:
            role = normalize_space(str(turn.get("role") or ""))
            content = normalize_space(str(turn.get("content") or ""))
            if role and content:
                if len(content) > 280:
                    content = f"{content[:277].rsplit(' ', 1)[0]}..."
                history_lines.append(f"- {role}: {content}")

    payload_parts = [
        f"User query:\n{question}",
        "Context metadata:",
        f"- Context messages provided: {len(context_messages)}",
        f"- Context messages ranked for relevance: {len(ranked_context)}",
        f"- Context lines included: {len(context_lines)}",
        f"- Estimated context tokens: {used_tokens}",
        f"- Top relevance score: {round(float(scored_context[0][0]), 3) if scored_context else 0.0}",
        f"- High-risk query: {'yes' if high_risk_query else 'no'}",
        f"- Strict multi-source confirmation required: {'yes' if strict_confirmation_query else 'no'}",
        f"- Identity-style query: {'yes' if _query_is_identity_question(question) else 'no'}",
        f"- Casualty-style query: {'yes' if _query_is_casualty_question(question) else 'no'}",
        f"- Distinct evidence sources: {distinct_sources}",
    ]
    if history_lines:
        payload_parts.extend(["", "Conversation history (recent):", *history_lines])
    payload_parts.extend(["", "Evidence messages:", *context_lines])
    user_payload = "\n".join(payload_parts)

    prompt = build_query_system_prompt(
        output_language=output_language,
        detailed=detailed,
    )

    try:
        auth_context = await manager.get_auth_context()
        content = await _call_codex(
            user_payload,
            auth_context,
            instructions=prompt,
            verbosity="low" if not detailed else "medium",
            on_token=on_token,
        )
    except _CodexAuthError:
        try:
            auth_context = await manager.refresh_auth_context()
            content = await _call_codex(
                user_payload,
                auth_context,
                instructions=prompt,
                verbosity="low" if not detailed else "medium",
                on_token=on_token,
            )
        except Exception:
            return _fallback_query_answer(question, ranked_context, detailed=detailed)
    except _CodexRateLimitError:
        return _fallback_query_answer(question, ranked_context, detailed=detailed)
    except (httpx.HTTPError, _CodexApiError, ValueError):
        return _fallback_query_answer(question, ranked_context, detailed=detailed)
    except Exception:
        return _fallback_query_answer(question, ranked_context, detailed=detailed)

    cleaned = normalize_space(content)
    if not cleaned:
        return _fallback_query_answer(question, ranked_context, detailed=detailed)

    plain_cleaned = re.sub(r"</?[^>]+>", "", cleaned).lower()
    if QUERY_NO_MATCH_TEXT.lower() in cleaned.lower() or "no relevant information found" in plain_cleaned:
        return QUERY_NO_MATCH_TEXT

    if strict_confirmation_query and distinct_sources < 2 and _answer_has_definitive_high_risk_claim(cleaned):
        return QUERY_NO_MATCH_TEXT

    safe = sanitize_telegram_html(content.strip())
    if not safe:
        return QUERY_NO_MATCH_TEXT
    return safe
