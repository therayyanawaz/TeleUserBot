"""AI filter and digest generation via Codex OAuth subscription backend."""

from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import logging
import re
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterable, List, Literal, Optional, Sequence, Tuple

import httpx

import config
from auth import AuthManager
from breaking_story import ContextEvidence
from db import ai_decision_cache_get, ai_decision_cache_set
from news_taxonomy import match_news_category, normalize_taxonomy_text
from news_signals import detect_story_signals, looks_like_live_event_update, should_downgrade_explainer_urgency
from prompts import (
    HUMAN_NEWSROOM_VOICE,
    QUERY_NO_MATCH_TEXT,
    build_digest_input_block,
    build_digest_system_prompt,
    build_query_system_prompt,
    digest_output_style,
    quiet_period_message,
)
from shared_http import get_codex_http_client, reset_shared_http_client
from utils import estimate_tokens_rough as _estimate_tokens_rough
from utils import (
    expand_query_terms,
    extract_query_keywords,
    extract_query_numbers,
    is_broad_news_query,
    log_structured,
    normalize_space,
    query_prefers_direct_answer,
    sanitize_telegram_html,
    strip_telegram_html,
)


CACHE_MAX_ITEMS = 2048
DEFAULT_CODEX_MODEL = "gpt-5.1-codex-mini"
DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api"
DEFAULT_CODEX_ORIGINATOR = "pi"
DEFAULT_DIGEST_MAX_POSTS = 80
DEFAULT_DIGEST_MAX_TOKENS = 18000
FILTER_DECISION_PROMPT_VERSION = "v6"
VITAL_VIEW_PROMPT_VERSION = "v3"

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
    "explainer",
    "our channel",
    "subscribe",
    "follow",
    "join",
    "livestream",
    "live stream",
    "watch here",
)
_FEED_LINE_PREFIX_RE = re.compile(
    r"^(?:breaking|alert|live update|update|analysis|opinion|thread|explainer)\s*[:\-–—]+\s*",
    flags=re.IGNORECASE,
)
_FEED_QUESTION_RE = re.compile(
    r"^(?:why|how|what explains|what caused|can|could|should|would|will)\b",
    flags=re.IGNORECASE,
)
_FEED_HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{2,}\b")
_FEED_TELEGRAM_LINK_RE = re.compile(
    r"(?i)\b(?:https?://)?(?:t\.me|telegram\.me)/[A-Za-z0-9_+./-]+"
)
_FEED_PROMO_TO_END_RE = re.compile(
    r"(?i)\b(?:our channel|subscribe|follow\s+us|join(?: us| our channel)?|"
    r"watch here|watch live|livestream|live stream)\b.*$"
)
_FEED_FOLLOW_PROMO_ONLY_RE = re.compile(
    r"(?i)^\s*follow(?=\s*(?:$|\||discussion\b|boost the channel\b|our channel\b|"
    r"subscribe\b|join\b|watch\b|:|[-–—](?=\s|$))).*$"
)
_FEED_INCOMPLETE_TAIL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "because",
    "but",
    "by",
    "for",
    "from",
    "if",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "than",
    "then",
    "to",
    "via",
    "while",
    "with",
}
_FEED_DANGLING_TAIL_WORDS = {
    "he",
    "her",
    "his",
    "it",
    "its",
    "my",
    "our",
    "she",
    "that",
    "their",
    "theirs",
    "them",
    "they",
    "this",
    "those",
    "these",
    "whose",
    "your",
    "yours",
}
_FEED_GEO_PREFIX_ALLOWLIST = {
    "beirut",
    "damascus",
    "gaza",
    "haifa",
    "iran",
    "iraq",
    "israel",
    "lebanon",
    "syria",
    "tehran",
    "u.s.",
    "us",
    "usa",
    "yemen",
}
_FEED_SOURCE_PREFIX_CONNECTORS = {
    "al",
    "and",
    "de",
    "el",
    "en",
    "la",
    "of",
    "the",
}

LOGGER = logging.getLogger("tg_news_userbot.ai_filter")
_SUMMARY_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_SEVERITY_CACHE: "OrderedDict[str, str]" = OrderedDict()
_HEADLINE_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_VITAL_VIEW_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
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
    copy_origin: Literal["ai", "fallback"] = "ai"
    routing_origin: Literal["ai", "system_override", "fallback"] = "ai"
    fallback_reason: str = ""
    ai_attempt_count: int = 1
    ai_quality_retry_used: bool = False
    cached: bool = False


@dataclass
class GeneratedTextResult:
    html: str
    copy_origin: Literal["ai", "fallback"]
    fallback_reason: str = ""
    ai_attempt_count: int = 1
    ai_quality_retry_used: bool = False
    major_block_count: int = 0
    timeline_item_count: int = 0
    noise_stripped_count: int = 0
    translation_applied_count: int = 0
    citation_stripped_count: int = 0
    duplicate_collapsed_count: int = 0
    narrative_headline: str = ""
    narrative_story: str = ""
    narrative_highlights: Tuple[str, ...] = ()
    narrative_also_moving: Tuple[str, ...] = ()


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


def _looks_like_generated_source_prefix(prefix: str) -> bool:
    cleaned = normalize_space(prefix).strip(" .")
    lowered = cleaned.lower()
    if not cleaned or lowered in _FEED_GEO_PREFIX_ALLOWLIST:
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
        if lower in _FEED_SOURCE_PREFIX_CONNECTORS:
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


def _clean_generated_delivery_segment(line: str) -> str:
    cleaned = normalize_space(strip_telegram_html(str(line or "")))
    if not cleaned:
        return ""
    cleaned = _FEED_LINE_PREFIX_RE.sub("", cleaned).strip()
    cleaned = cleaned.lstrip("/\\| ").strip()
    cleaned = _FEED_TELEGRAM_LINK_RE.sub("", cleaned)
    cleaned = _FEED_HANDLE_RE.sub("", cleaned)
    cleaned = _FEED_PROMO_TO_END_RE.sub("", cleaned)
    cleaned = _FEED_FOLLOW_PROMO_ONLY_RE.sub("", cleaned)
    cleaned = re.sub(r"(?i)\b(?:channel|source)\s*[:\-–—|]+\s*$", "", cleaned)
    prefix_match = re.match(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9&'._ /-]{1,50})(?P<sep>\s*[:\-–—|]+\s*)(?P<rest>.+)$",
        cleaned,
    )
    if prefix_match and _looks_like_generated_source_prefix(prefix_match.group("prefix")):
        separator = normalize_space(prefix_match.group("sep"))
        rest = normalize_space(prefix_match.group("rest"))
        if not (separator == "-" and re.match(r"^[a-z][A-Za-z0-9-]*\b", rest)):
            cleaned = rest
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    return normalize_space(cleaned.strip(" ,;:-|/[](){}"))


def _feed_segment_is_incomplete(line: str) -> bool:
    cleaned = normalize_space(line)
    if not cleaned:
        return True
    if cleaned.endswith(('...', '…')):
        return True
    if cleaned.endswith((':', '/', '-', '|', '•')):
        return True
    if cleaned.startswith(("@", "/", "\\")):
        return True
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9.'/-]*", cleaned)
    if not words:
        return True
    tail_word = words[-1].strip(".,;:!?").lower()
    if tail_word in _FEED_INCOMPLETE_TAIL_WORDS:
        return True
    if tail_word in {"its", "their", "his", "her", "our", "your", "my", "whose"}:
        return True
    match = re.match(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9&'._ -]{1,40})\s*:\s*(?P<rest>.+)$",
        cleaned,
    )
    if match:
        prefix = normalize_space(match.group("prefix")).lower().strip(" .")
        rest_words = re.findall(r"[A-Za-z0-9][A-Za-z0-9.'/-]*", normalize_space(match.group("rest")))
        if len(rest_words) <= 2 and prefix not in _FEED_GEO_PREFIX_ALLOWLIST:
            return True
    return False


def _clean_generated_delivery_html(text: str, *, max_lines: int) -> str:
    out: list[str] = []
    raw = re.sub(r"(?i)<br\s*/?>", "\n", str(text or ""))
    for part in re.split(r"\n+", raw):
        cleaned = _clean_generated_delivery_segment(part)
        if not cleaned or _feed_segment_is_incomplete(cleaned):
            continue
        out.append(sanitize_telegram_html(cleaned))
        if len(out) >= max_lines:
            break
    return "<br>".join(out)


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
            cleaned = _clean_generated_delivery_segment(part.strip(" \t-•"))
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
    if _digest_is_low_value_quote_or_rant(cleaned):
        return True
    if cleaned.endswith("?") or _FEED_QUESTION_RE.search(cleaned):
        return True
    if bool(signals.get("downgrade_explainer")) and not bool(signals.get("live_event_update")):
        return True
    if any(fragment in lowered for fragment in _FEED_CONTEXT_BANNED_FRAGMENTS):
        return True
    if cleaned.count("...") or cleaned.endswith("..."):
        return True
    if _feed_segment_is_incomplete(cleaned):
        return True
    return False


def _is_weak_feed_context(line: str, headline: str) -> bool:
    cleaned = normalize_space(line)
    lowered = cleaned.lower()
    if not cleaned:
        return True
    if _digest_is_low_value_quote_or_rant(cleaned):
        return True
    if cleaned.endswith("?") or _FEED_QUESTION_RE.search(cleaned):
        return True
    if any(fragment in lowered for fragment in _FEED_CONTEXT_BANNED_FRAGMENTS):
        return True
    if SequenceMatcher(None, cleaned.lower(), headline.lower()).ratio() >= 0.72:
        return True
    if should_downgrade_explainer_urgency(cleaned) and not looks_like_live_event_update(cleaned):
        return True
    if _feed_segment_is_incomplete(cleaned):
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


def _normalize_generated_feed_summary_html(summary_html: str, raw_text: str) -> str:
    signals = detect_story_signals(raw_text)
    segments = _feed_summary_segments(summary_html)
    if not segments:
        return ""

    acceptable = [segment for segment in segments if not _is_bad_feed_headline(segment)]
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


def extract_feed_summary_parts(summary_html: str, raw_text: str) -> tuple[str, str]:
    normalized = normalize_feed_summary_html(summary_html, raw_text)
    plain = re.sub(r"(?i)<br\s*/?>", "\n", normalized or "")
    lines = [
        normalize_space(strip_telegram_html(line))
        for line in re.split(r"\n+", plain)
        if normalize_space(strip_telegram_html(line))
    ]
    headline = lines[0] if lines else normalize_space(strip_telegram_html(summary_html or ""))
    if not headline:
        headline = _fallback_headline(raw_text) or normalize_space(raw_text)
    context = " ".join(lines[1:]).strip()
    return headline, context


def _fallback_summary(text: str) -> Optional[str]:
    if _likely_noise(text):
        return None
    summary = _build_feed_summary_html(text)
    return summary or None


def _filter_decision_quality_issue(decision: FilterDecision, raw_text: str) -> str:
    if decision.action == "skip":
        return ""

    summary_lines = _summary_plain_lines(decision.summary_html)
    if not summary_lines:
        return "empty_output"

    headline_issue = _news_copy_quality_issue(summary_lines[0], raw_text)
    if headline_issue:
        return headline_issue

    if len(summary_lines) > 1:
        context_issue = _news_copy_quality_issue(summary_lines[1], raw_text, allow_short=True)
        if context_issue and context_issue not in {"headline_too_thin"}:
            return context_issue
        if SequenceMatcher(None, summary_lines[0].lower(), summary_lines[1].lower()).ratio() >= 0.78:
            return "repetitive_copy"

    if decision.severity == "high":
        headline = normalize_space(strip_telegram_html(decision.headline_html))
        headline_issue = _news_copy_quality_issue(headline, raw_text)
        if headline_issue:
            return headline_issue
        if not resolve_breaking_headline_for_delivery(raw_text, headline, allow_fallback=False):
            return "ungrounded_copy"

    return ""


def _severity_from_text_heuristic(text: str) -> Literal["high", "medium", "low"]:
    signals = detect_story_signals(text)
    category_match = match_news_category(text)
    ontology = signals.get("ontology") or {}
    frame = ontology.get("event_frame") or {}
    signal_breakdown = ontology.get("signal_breakdown") or {}
    live_event_score = float(signal_breakdown.get("live_event_score") or 0.0)

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

    if bool(signals.get("live_event_update")) and (
        live_event_score >= 1.6
        or (
            frame.get("actions")
            and (frame.get("recency_hits") or frame.get("official_hits"))
        )
    ):
        if bool(signals.get("downgrade_explainer")):
            return "medium"
        return "high"
    if bool(signals.get("concrete_event")) or frame.get("targets") or frame.get("official_hits"):
        return "medium"
    if len(text.strip()) < 24 or _likely_noise(text):
        return "low"
    return "medium"


def _fallback_headline(text: str) -> Optional[str]:
    cleaned = normalize_space(text)
    if not cleaned:
        return None

    def _headline_strength_score(value: str) -> tuple[int, int, int, int]:
        lowered = value.lower()
        actor_hits = len(re.findall(r"\b(?:iran|israel|idf|irgc|hezbollah|hamas|us|u\.s\.|ukraine|russia|army|government|ministry|officials?)\b", lowered))
        action_hits = len(re.findall(r"\b(?:hit|hits|struck|strike|strikes|launched|launches|killed|kills|destroyed|destroys|suspended|halts|intercepted|downed|fired|fires|attacked|attack|captured|ordered|announced|confirmed)\b", lowered))
        specificity_hits = len(re.findall(r"\b[A-Z][a-z]{2,}\b|\b\d+\b", value))
        penalty = 1 if any(pattern in lowered for pattern in _WEAK_COPY_PATTERNS) else 0
        return (actor_hits + action_hits + specificity_hits - penalty, action_hits, specificity_hits, -penalty)

    candidates = [
        normalize_space(part)
        for part in re.split(r"(?<=[.!?])\s+", cleaned)
        if normalize_space(part)
    ]
    if not candidates:
        candidates = [cleaned]

    summary_html = _build_feed_summary_html(cleaned)
    if summary_html:
        plain_summary = re.sub(r"(?i)<br\s*/?>.*$", "", summary_html)
        headline = normalize_space(strip_telegram_html(plain_summary))
        if headline and _headline_strength_score(headline) >= _headline_strength_score(max(candidates[:4], key=_headline_strength_score)):
            return headline

    best = max(candidates[:4], key=_headline_strength_score)
    if len(best) > 420:
        best = f"{best[:417].rsplit(' ', 1)[0]}..."
    return best


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


def resolve_breaking_headline_for_delivery(
    source_text: str,
    candidate: str | None,
    *,
    allow_fallback: bool = True,
) -> str:
    fallback = _safe_breaking_headline_fallback(source_text)
    cleaned_candidate = _cleanup_headline(str(candidate or ""))
    if not cleaned_candidate:
        return fallback if allow_fallback else ""
    if not _breaking_style_is_unhinged():
        return cleaned_candidate
    if _breaking_headline_is_grounded(source_text, cleaned_candidate):
        return cleaned_candidate
    return fallback if allow_fallback else ""


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
        "When the source contains a clear actor, action, location, object, number, or official attribution, carry those specifics into the rewrite.\n"
        "Do not copy article titles, teaser paragraphs, rhetorical questions, or explainer framing.\n"
        "Reject vague leads like situation update, incident reported, developments continue, or tensions rise unless the source itself contains nothing more specific.\n"
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
        "action, severity, summary_html, headline_html, story_bridge_html, confidence, reason_code, topic_key\n"
        "action must be one of: skip, deliver, digest\n"
        "severity must be one of: high, medium, low\n"
        "summary_html must be Telegram-safe HTML using only <b>, <i>, <u>, <s>, <tg-spoiler>, <code>, <pre>, <blockquote>, <a href>, <br>\n"
        "headline_html must be a concise one-line Telegram-safe HTML headline or empty string\n"
        "story_bridge_html must be a concise Telegram-safe HTML contextual bridge or empty string\n"
        "Never include channel names, channel usernames, source handles, t.me links, outlet/source prefixes, or self-promo text like our channel / subscribe / follow in any field.\n"
        "If cleanup would leave only promo, source attribution, or an incomplete fragment, output an empty string instead of that fragment.\n"
        "confidence must be a number from 0 to 1\n"
        "reason_code must be a short snake_case code\n"
        "topic_key must be a short normalized topic label\n"
        "Use skip for spam, ads, promos, noise, or clearly irrelevant updates.\n"
        "Use deliver for urgent/breaking items that should be sent immediately.\n"
        "Use digest for meaningful non-breaking items worth keeping.\n"
        "If the text is mainly an analysis, explainer, opinion, recap, thread, or question-led piece without a concrete new event or official development, use digest or skip, not deliver.\n"
        "For delivered non-breaking items, summary_html must be a compact feed card: line 1 is a rewritten factual headline and line 2 is optional one short contextual sentence.\n"
        "If the source contains a clear actor, action, location, object, number, or official body, include those specifics in the rewrite.\n"
        "Do not copy article titles, teaser paragraphs, rhetorical questions, or why/how/what explains framing into summary_html.\n"
        "Reject vague leads like situation update, incident reported, developments continue, explosions shake [country], or tensions rise unless the source itself is equally vague.\n"
        "Do not let line 2 merely restate line 1 in different words.\n"
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
            "You write one compact evidence-based context line for Telegram news alerts.\n"
            f"Output language must be {language}. Translate if needed.\n"
            "Start with: Why it matters:\n"
            f"Default to one sentence. You may use two short sentences only when one sentence cannot clearly carry both the earlier anchor and the new delta. Keep the full output under {max(18, min(max_words * 2, 40))} words.\n"
            "You will be given a current update, one prior anchor, an anchor detail, and one new delta.\n"
            "Your only job is to explain the concrete change between the anchor and the current update.\n"
            "You must mention both the earlier anchor detail and the new delta detail.\n"
            "Do not give generic stakes, obvious consequences, empty escalation language, or vague continuity phrases.\n"
            "Good: Why it matters: Earlier reports put the strikes around Haifa; this update places the same exchange in Acre.\n"
            "Good: Why it matters: Earlier reports said nine were wounded in Bnei Brak; this update raises that to 12 and adds Magen David Adom confirmation.\n"
            "Bad: Why it matters: This raises regional stability concerns.\n"
            "Bad: Why it matters: The same story is still unfolding.\n"
            "If the anchor detail or new delta is weak, missing, or unsupported, reply exactly: SKIP\n"
            "Do not speculate. Do not invent causality. No hashtags. No markdown."
        )
    return (
        "You write one compact evidence-based context line for Telegram news alerts.\n"
        f"Output language must be {language}. Translate if needed.\n"
        f"Return exactly one sentence (max {max_words} words), neutral and precise.\n"
        "Start with: Why it matters:\n"
        "You will be given a current update, one prior anchor, an anchor detail, and one new delta.\n"
        "Your only job is to explain the concrete change from the earlier anchor to the current update.\n"
        "You must mention both the earlier anchor detail and the new delta detail.\n"
        "Do not explain generic consequences or obvious common sense.\n"
        "Good: Why it matters: Earlier reports centered on Bahrain; this update places the same disruption in Abu Dhabi.\n"
        "Good: Why it matters: Earlier reports put casualties at nine in Bnei Brak; this update raises that to 12.\n"
        "Bad: Why it matters: This raises regional stability concerns.\n"
        "Bad: Why it matters: Civilian danger is increasing.\n"
        "If the anchor detail or new delta is weak, missing, or unsupported, reply exactly: SKIP\n"
        "Do not speculate. Do not take sides. No hashtags. No markdown."
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
    response: httpx.Response | None = None
    for attempt in range(3):
        http = await get_codex_http_client()
        try:
            response = await http.post(_resolve_codex_url(), headers=headers, json=payload)
            break
        except httpx.TransportError as exc:
            await reset_shared_http_client("codex")
            if attempt < 2:
                LOGGER.warning(
                    "Codex non-stream transport error on attempt %s/3: %s",
                    attempt + 1,
                    exc,
                )
                await asyncio.sleep(0.35 * (attempt + 1))
                continue
            raise _CodexApiError(f"Codex request failed after retries: {exc}") from exc
    if response is None:
        raise _CodexApiError("Codex request failed before receiving a response.")
    if response.status_code >= 400:
        _raise_codex_http_error(response)
    try:
        data = response.json()
    except (json.JSONDecodeError, ValueError) as exc:
        LOGGER.debug("Failed to parse Codex JSON response: %s", exc)
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


_DIGEST_STORY_MAX_CHARS = 420


def _digest_also_moving_cap(max_lines: int | None = None) -> int:
    resolved = _resolve_digest_max_lines() if max_lines is None else max(1, int(max_lines))
    return max(1, min(3, max(1, resolved // 3)))


def _cap_headline_rail_support(
    highlights: Sequence[str],
    also_moving: Sequence[str],
    *,
    max_lines: int,
) -> tuple[List[str], List[str]]:
    total_cap = max(1, int(max_lines))
    capped_highlights = list(highlights[:total_cap])
    remaining = max(0, total_cap - len(capped_highlights))
    capped_also = list(also_moving[: min(_digest_also_moving_cap(max_lines), remaining)])
    return capped_highlights, capped_also


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


_ALLOWED_FILTER_FALLBACK_REASONS = {
    "auth_error",
    "rate_limited",
    "timeout",
    "transport_error",
    "api_error",
    "invalid_payload",
    "empty_output",
    "quality_rejected_after_retry",
}
_WEAK_COPY_PATTERNS = (
    "situation update",
    "incident reported",
    "incident in",
    "latest developments",
    "major developments",
    "reports say",
    "report says",
    "according to reports",
    "there are reports",
    "there appears to be",
    "appears to be",
    "heightened tension",
    "heightened tensions",
    "ongoing developments",
)
_COPY_SPECIFICITY_STOPWORDS = {
    "about",
    "after",
    "amid",
    "another",
    "around",
    "being",
    "between",
    "confirmed",
    "continues",
    "earlier",
    "from",
    "have",
    "into",
    "latest",
    "major",
    "more",
    "near",
    "news",
    "north",
    "officials",
    "over",
    "reported",
    "reports",
    "said",
    "several",
    "situation",
    "south",
    "their",
    "there",
    "this",
    "update",
    "warned",
    "with",
}


def _normalize_filter_fallback_reason(value: Any) -> str:
    cleaned = normalize_space(str(value or "")).lower()
    cleaned = re.sub(r"[^a-z0-9_]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if cleaned in _ALLOWED_FILTER_FALLBACK_REASONS:
        return cleaned
    return ""


def _fallback_reason_from_exc(exc: BaseException) -> str:
    if isinstance(exc, _CodexAuthError):
        return "auth_error"
    if isinstance(exc, _CodexRateLimitError):
        return "rate_limited"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.TransportError):
        return "transport_error"
    if isinstance(exc, _CodexApiError):
        return "api_error"
    if isinstance(exc, (json.JSONDecodeError, ValueError, TypeError)):
        return "invalid_payload"
    return "api_error"


def _copy_specificity_tokens(text: str) -> set[str]:
    return {
        token.lower()
        for token in _LATIN_TOKEN_RE.findall(normalize_space(str(text or "")))
        if len(token) >= 3 and token.lower() not in _COPY_SPECIFICITY_STOPWORDS
    }


def _summary_plain_lines(summary_html: str) -> list[str]:
    plain = re.sub(r"(?i)<br\s*/?>", "\n", str(summary_html or ""))
    return [
        normalize_space(strip_telegram_html(line))
        for line in re.split(r"\n+", plain)
        if normalize_space(strip_telegram_html(line))
    ]


def _generated_copy_has_grounding(candidate: str, source_text: str) -> bool:
    candidate_clean = normalize_space(strip_telegram_html(candidate))
    source_clean = normalize_space(strip_telegram_html(source_text))
    if not candidate_clean or not source_clean:
        return False

    source_named = _extract_candidate_named_tokens(source_clean)
    candidate_named = _extract_candidate_named_tokens(candidate_clean)
    if source_named and candidate_named and not (candidate_named & source_named):
        return False

    source_numbers = _extract_numeric_tokens(source_clean)
    candidate_numbers = _extract_numeric_tokens(candidate_clean)
    if source_numbers and candidate_numbers and not (candidate_numbers & source_numbers):
        return False

    source_tokens = _copy_specificity_tokens(source_clean)
    candidate_tokens = _copy_specificity_tokens(candidate_clean)
    if len(source_tokens) >= 6 and len(source_tokens & candidate_tokens) < 2:
        return False
    return True


def _news_copy_quality_issue(candidate: str, source_text: str, *, allow_short: bool = False) -> str:
    cleaned = normalize_space(strip_telegram_html(candidate))
    lowered = cleaned.lower()
    if not cleaned:
        return "empty_output"
    if _feed_segment_is_incomplete(cleaned):
        return "incomplete_copy"
    if any(pattern in lowered for pattern in _WEAK_COPY_PATTERNS):
        return "vague_copy"
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9.'/-]*", cleaned)
    if not allow_short and len(words) < 4:
        return "headline_too_thin"
    if cleaned.endswith(("...", "…")):
        return "incomplete_copy"
    if not _generated_copy_has_grounding(cleaned, source_text):
        return "ungrounded_copy"
    return ""


def _filter_retry_feedback(issue: str) -> str:
    issue = normalize_space(issue).lower()
    if issue in {"invalid_payload", "empty_output"}:
        return (
            "Your last answer was unusable. Return exactly one valid JSON object with every required key, "
            "and make every output field complete."
        )
    if issue == "citation_leak":
        return (
            "Your last answer still named a source. Remove every outlet, channel, username, handle, or source-style phrase "
            "and rewrite any needed uncertainty as generic wording such as 'initial reports indicate'."
        )
    if issue == "duplication":
        return (
            "Your last answer repeated the same fact. Collapse duplicates and make each line add a distinct factual update."
        )
    if issue == "weak_scene_setter":
        return (
            "Your opening line was too generic. Rewrite the scene setter as one sharp, concrete sentence that frames the window."
        )
    if issue == "missing_story":
        return (
            "Your digest is missing the short story paragraph. Add one compact paragraph that covers the whole window before any bullets."
        )
    if issue == "oversized_also_moving":
        return (
            "Your Also moving rail is too long. Keep it to only a few overflow items and move the main facts into the story or highlights."
        )
    if issue == "headline_too_thin":
        return (
            "Your last copy was too thin. Rewrite it with a concrete actor, action, and location when present in the source."
        )
    if issue in {"continuation_line", "dependent_line"}:
        return (
            "Your last rail line depended on another line. Rewrite it as a standalone headline with the named actor or drop it."
        )
    if issue in {"question_line", "banter_line", "soft_news", "history_fragment"}:
        return (
            "Your last rail line was not suitable hard-news headline copy. Drop jokes, questions, soft features, and history scraps."
        )
    if issue == "ungrounded_copy":
        return (
            "Your last copy drifted away from the source. Keep the wording sharp, but preserve the concrete names, locations, "
            "numbers, and actors that appear in the source."
        )
    return (
        "Your last copy was too vague or incomplete. Rewrite it in a sharper newsroom voice with concrete facts, "
        "complete sentences, and no generic filler."
    )


def _decision_retry_prompt(base_prompt: str, issue: str) -> str:
    return f"{base_prompt}\n\nRevision feedback:\n- {_filter_retry_feedback(issue)}"


async def _call_codex_with_auth_repair(
    cleaned: str,
    auth_manager: AuthManager,
    instructions: str,
    *,
    verbosity: str = "medium",
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    try:
        auth_context = await auth_manager.get_auth_context()
        return await _call_codex(
            cleaned,
            auth_context,
            instructions=instructions,
            verbosity=verbosity,
            on_token=on_token,
        )
    except _CodexAuthError as exc:
        auth_context = await auth_manager.refresh_auth_context()
        try:
            return await _call_codex(
                cleaned,
                auth_context,
                instructions=instructions,
                verbosity=verbosity,
                on_token=on_token,
            )
        except Exception as refresh_exc:
            raise refresh_exc from exc


def _fallback_filter_decision(
    text: str,
    *,
    fallback_reason: str = "",
    ai_attempt_count: int = 1,
    ai_quality_retry_used: bool = False,
) -> FilterDecision:
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
            copy_origin="fallback",
            routing_origin="fallback",
            fallback_reason=_normalize_filter_fallback_reason(fallback_reason),
            ai_attempt_count=max(1, ai_attempt_count),
            ai_quality_retry_used=ai_quality_retry_used,
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
        copy_origin="fallback",
        routing_origin="fallback",
        fallback_reason=_normalize_filter_fallback_reason(fallback_reason),
        ai_attempt_count=max(1, ai_attempt_count),
        ai_quality_retry_used=ai_quality_retry_used,
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
    context_evidence: ContextEvidence | None = None,
    copy_origin: Literal["ai", "fallback"] = "ai",
    routing_origin: Literal["ai", "system_override", "fallback"] = "ai",
    fallback_reason: str = "",
    ai_attempt_count: int = 1,
    ai_quality_retry_used: bool = False,
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
    raw_summary_html = _clean_generated_delivery_html(raw_summary_html, max_lines=2)
    headline_html = _clean_generated_delivery_html(headline_html, max_lines=1)
    story_bridge_html = _clean_generated_delivery_html(story_bridge_html, max_lines=2)
    summary_html = _normalize_generated_feed_summary_html(raw_summary_html, raw_text)

    if bool(story_signals.get("downgrade_explainer")) and (
        action == "deliver" or severity == "high"
    ):
        severity = "medium" if summary_html else "low"
        action = "digest" if summary_html else "skip"
        confidence = min(confidence, 0.75)
        routing_origin = "system_override"
    if severity == "high":
        headline_html = resolve_breaking_headline_for_delivery(
            raw_text,
            headline_html,
            allow_fallback=False,
        )
        if _breaking_style_is_unhinged():
            story_bridge_html = resolve_vital_rational_view_for_delivery(
                raw_text,
                story_bridge_html,
                recent_context,
                evidence=context_evidence,
            )

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
        copy_origin=copy_origin,
        routing_origin=routing_origin,
        fallback_reason=_normalize_filter_fallback_reason(fallback_reason),
        ai_attempt_count=max(1, ai_attempt_count),
        ai_quality_retry_used=ai_quality_retry_used,
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


_DIGEST_PROMO_FRAGMENT_RE = re.compile(
    r"(?i)\b(?:follow us|discussion|boost the channel|our channel|subscribe|join(?: us| our channel)?|"
    r"watch here|watch live|livestream|live stream|share|paylas|paylaş|smaylik|abon[eə]\s+olun|global eye|şərh[_\s-]*yaz)\b.*$"
)
_DIGEST_HASHTAG_RE = re.compile(r"(?<!\w)#[\w\-/]+")
_DIGEST_PUNCT_ONLY_RE = re.compile(r"^[\W_]+$")
_DIGEST_LEADING_NOISE_RE = re.compile(r"^[^0-9A-Za-z\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\u0400-\u04FF\"'“‘]+")
_DIGEST_GENERIC_UNCERTAINTY_RE = re.compile(
    r"(?i)^(?:initial|preliminary|early|unconfirmed)\s+reports?\s+(?:indicate|suggest|point to)\b"
)
_DIGEST_SOURCE_PREFIX_RE = re.compile(
    r"(?i)^(?:enemy media|hebrew sources?|hebrew media|israeli media|news from the zionist enemy|"
    r"follow-up from [^:]{1,80}|source|outlet|channel)\s*[:\-–—|]+\s*"
)
_DIGEST_ALSO_MOVING_RE = re.compile(r"(?i)^also moving$")
_DIGEST_CITATION_WORD_RE = re.compile(
    r"(?i)\b(?:enemy(?:'s)? media|hebrew(?:-language)? sources?|hebrew media|israeli media|"
    r"local media|foreign media|follow-up from|news agency|media reports?)\b"
)
_DIGEST_ACCORDING_TO_CITATION_RE = re.compile(
    r"(?i)\baccording to\s+[^.,;:]{0,80}\b(?:media|sources?|outlet|channel|network|agency|press|radio|tv)\b[:,]?\s*"
)
_DIGEST_SOURCE_REPORT_RE = re.compile(
    r"(?i)^(?P<prefix>(?:enemy(?:'s)? media|hebrew(?:-language)? sources?|hebrew media|israeli media|"
    r"local media|foreign media|news from the zionist enemy|follow-up from [^:]{1,80}|reports? in [^:]{1,80}|"
    r"[^:]{1,80}\b(?:media|sources?|outlet|channel|network|agency|press|radio|tv)\b))"
    r"(?:\s*[:\-–—|,]\s*|\s+)"
    r"(?:(?:reported|reports|say|says|said|claim|claims|claimed|indicate|indicates|indicated|warned|warning)\s+(?:that\s+)?)?"
    r"(?P<rest>.+)$"
)
_DIGEST_REPORTS_OF_RE = re.compile(
    r"(?i)^(?P<label>(?:preliminary|initial|early|unconfirmed)(?:\s+[a-z-]+)?\s+reports?|there\s+(?:are|were)\s+reports?|reports?)\s+of\s+(?P<rest>.+)$"
)
_DIGEST_WEAK_SCENE_SETTER_RE = re.compile(
    r"(?i)^(?:the window stayed active|the main development in this window was|the half-hour was dominated by)\b"
)
_DIGEST_CONTEXT_PREFIX_RE = re.compile(r"(?i)^context\s*[-:]\s*")
_DIGEST_LEADING_MARKER_RE = re.compile(r"^(?:[•●▪■◆◦▸🔴🟠🟡🟢🔵🟣⚫⚪🟥🟧🟨🟩🟦🟪⬛⬜]+\s*)+")
_DIGEST_INLINE_BULLET_RE = re.compile(r"\s+[•●▪■◆◦▸]+\s+")
_DIGEST_INLINE_ALERT_RE = re.compile(r"\s*[🔴🟠🟡🟢🔵🟣⚫⚪🟥🟧🟨🟩🟦🟪⬛⬜]+\s*")
_DIGEST_SPECIFIC_ACTION_RE = re.compile(
    r"(?i)\b(?:arrested|attack|attacked|captures?|captured|confirmed|destroyed|explosions?|fell|fired|hit|hits|intercepted|killed|launched|missile|raided|reports? of|seized|smuggler|strike|strikes|targeted)\b"
)
_DIGEST_FIRST_PERSON_RE = re.compile(r"(?i)\b(?:i|i'm|i am|i've|i will|we|we're|we are|we've|our|my)\b")
_DIGEST_SIGNATURE_LINE_RE = re.compile(r"(?i)^(?:president\s+[a-z]{2,24}|[a-z]{1,4})$")
_DIGEST_QUOTE_FRAGMENT_RE = re.compile(r"[\"“”'‘’]")
_DIGEST_QUOTE_PREFIX_RE = re.compile(
    r"(?i)^(?:[^:]{3,80}:\s*)?[\"“'‘].+|^[^:]{3,120}:\s*[\"“'‘]"
)
_DIGEST_RANT_FRAGMENT_RE = re.compile(
    r"(?i)\b(?:to quote the bible|religious crusade|doesn.?t that beat fighting|but remember|big, fat, hug|president djt)\b"
)
_HEADLINE_RAIL_CONTINUATION_RE = re.compile(r"(?i)^(?:however|but|and|why)\b")
_HEADLINE_RAIL_DEPENDENT_START_RE = re.compile(
    r"(?i)^(?:he|she|they|his|her|their|this|that|these|those|it|its|the same|the offender|the commander|in honor|isn'?t this)\b"
)
_HEADLINE_RAIL_BANTER_RE = re.compile(
    r"(?i)\b(?:what do you guys think|what do you think|just wanted to|taste and estimate|blue meth|"
    r"engagement farmers?|full post with explanation|best telegram channels)\b"
)
_HEADLINE_RAIL_SOFT_NEWS_RE = re.compile(
    r"(?i)\b(?:ceremony outfits?|commonwealth games|fashion|red carpet)\b"
)
_HEADLINE_RAIL_HISTORY_RE = re.compile(
    r"(?i)\b(?:russian empire|empress|order of st\.?|from 17\d{2}\b|from 18\d{2}\b|from 19\d{2}\b)\b"
)
_HEADLINE_RAIL_VAGUE_RESULT_RE = re.compile(
    r"(?i)^(?:the\s+)?(?:fire|situation|operation|incident|scene|work|area)\s+"
    r"(?:was|is)\s+(?:successfully\s+)?(?:extinguished|contained|completed|resolved|stabilized)\b"
)
_HEADLINE_RAIL_SOURCE_TAIL_RE = re.compile(
    r"(?i)^(?P<body>.+?)(?:,\s*|\s+)(?:according to|per)\s+"
    r"(?P<source>[A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,3})\.?$"
)
_HEADLINE_RAIL_SOURCE_PREFIX_RE = re.compile(
    r"(?i)^(?:according to|per)\s+"
    r"(?P<source>[A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,3})[:,]?\s*(?P<body>.+)$"
)
_HEADLINE_RAIL_CLAIM_CONTEXT_RE = re.compile(
    r"(?i)\b(?:offer(?:ed|s)?|proposal|propose(?:d|s)?|claim(?:ed|s)?|said|says|admit(?:ted|s)?|"
    r"warn(?:ed|s|ing)?|suggest(?:ed|s)?|plan(?:ned|s)?|intend(?:ed|s)?|seek(?:s|ing)?|"
    r"ask(?:ed|s)?|accuse(?:d|s)?|deny|denied|denies)\b"
)
_HEADLINE_RAIL_STANDALONE_ACTION_RE = re.compile(
    r"(?i)\b(?:offer(?:ed|s)?|admit(?:ted|s)?|detain(?:ed|s)?|face(?:s|d)?|wounded|killed|"
    r"attack(?:ed|s)?|hit|hits|launch(?:ed|es)?|strike(?:s|d)?|confirm(?:ed|s)?|order(?:ed|s)?|"
    r"extend(?:ed|s)?|approve(?:d|s)?|block(?:ed|s)?|unblock(?:ed|s)?|reopen(?:ed|s)?|close(?:d|s)?|"
    r"meet(?:s|ing)?|visit(?:s|ed)?|schedule(?:s|d)?|reveal(?:ed|s)?|help(?:ed|s)?|back(?:ed|s)?|"
    r"seiz(?:ed|es)|sentence(?:d|s)?|rule(?:d|s)?|fire(?:d|s)?|open(?:ed|s)?|shut(?:s|ting)?|"
    r"agree(?:d|s)?|reject(?:ed|s)?|refuse(?:d|s)?|remain(?:s|ed)?|continue(?:s|d)?|"
    r"activat(?:ed|es)|fall|falls|blast|blasts|seen|spotted)\b"
)
_HEADLINE_RAIL_GENERIC_SOURCE_RE = re.compile(
    r"(?i)^(?:sources?|media|channel|outlet|press)$|"
    r"^(?:local|foreign|enemy(?:'s)?|hebrew(?:-language)?|israeli)\s+(?:media|sources?)$"
)
_HEADLINE_RAIL_TOKEN_RE = re.compile(r"[a-z0-9]{3,}", flags=re.IGNORECASE)
_HEADLINE_RAIL_TOPIC_STOPWORDS = {
    "after",
    "again",
    "amid",
    "and",
    "around",
    "because",
    "from",
    "have",
    "into",
    "over",
    "per",
    "said",
    "says",
    "that",
    "their",
    "there",
    "they",
    "this",
    "those",
    "through",
    "under",
    "with",
    "would",
}


def _digest_line_key(text: str) -> str:
    cleaned = normalize_space(strip_telegram_html(str(text or ""))).lower()
    cleaned = re.sub(r"(?i)\b(?:initial|preliminary|early|unconfirmed)\s+reports?\s+(?:indicate|suggest|point to)\b", "", cleaned)
    cleaned = re.sub(r"(?i)\b(?:reports?|indicate|indicates|indicated|suggest|suggests|suggested)\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return _cache_key(cleaned)


def _digest_is_low_value_quote_or_rant(text: str) -> bool:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    lowered = cleaned.lower()
    if not cleaned:
        return True
    if _DIGEST_ALSO_MOVING_RE.fullmatch(cleaned):
        return False
    if _DIGEST_SIGNATURE_LINE_RE.fullmatch(lowered):
        return True
    if len(cleaned.split()) < 4 and not _DIGEST_SPECIFIC_ACTION_RE.search(cleaned):
        return True
    if _DIGEST_RANT_FRAGMENT_RE.search(cleaned):
        return True
    if _DIGEST_FIRST_PERSON_RE.search(cleaned) and not _DIGEST_SPECIFIC_ACTION_RE.search(cleaned):
        return True
    if _DIGEST_QUOTE_PREFIX_RE.search(cleaned) and (
        _feed_segment_is_incomplete(cleaned) or not _DIGEST_SPECIFIC_ACTION_RE.search(cleaned)
    ):
        return True
    if _DIGEST_QUOTE_FRAGMENT_RE.search(cleaned) and _feed_segment_is_incomplete(cleaned):
        return True
    return False


def _looks_like_digest_citation_prefix(prefix: str) -> bool:
    cleaned = normalize_space(prefix).strip(" .").lower()
    if not cleaned:
        return False
    if _looks_like_generated_source_prefix(prefix):
        return True
    return bool(_DIGEST_CITATION_WORD_RE.search(cleaned))


def _digest_apply_generic_uncertainty(text: str) -> str:
    cleaned = normalize_space(text)
    if not cleaned:
        return ""
    if _DIGEST_GENERIC_UNCERTAINTY_RE.match(cleaned) or _text_has_hedge_markers(cleaned):
        return cleaned
    return f"Initial reports indicate {cleaned}"


def _rewrite_reports_of_phrase(text: str) -> str:
    match = _DIGEST_REPORTS_OF_RE.match(normalize_space(text))
    if not match:
        return normalize_space(text)
    label = normalize_space(match.group("label"))
    rest = normalize_space(match.group("rest"))
    if not rest:
        return ""
    if label.lower().startswith(("preliminary", "initial", "early", "unconfirmed")):
        qualifier = label.split()[0].capitalize()
        return f"{qualifier} reports indicate {rest}"
    return f"Reports indicate {rest}"


def _digest_rewrite_source_attribution(text: str) -> tuple[str, int]:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    if not cleaned:
        return "", 0

    removed = 0
    prefix_removed = False

    prefix_match = re.match(
        r"^(?P<prefix>[A-Za-z][A-Za-z0-9&'._ /-]{1,60})(?P<sep>\s*[:\-–—|]+\s*)(?P<rest>.+)$",
        cleaned,
    )
    if prefix_match and _looks_like_digest_citation_prefix(prefix_match.group("prefix")):
        separator = normalize_space(prefix_match.group("sep"))
        rest = normalize_space(prefix_match.group("rest"))
        if not (separator == "-" and re.match(r"^[a-z][A-Za-z0-9-]{2,}", rest)):
            cleaned = rest
            removed += 1
            prefix_removed = True

    citation_match = _DIGEST_SOURCE_REPORT_RE.match(cleaned)
    if citation_match:
        cleaned = normalize_space(citation_match.group("rest"))
        removed += 1
        prefix_removed = True

    cleaned, inline_removed = _DIGEST_ACCORDING_TO_CITATION_RE.subn("", cleaned)
    if inline_removed:
        removed += inline_removed
        prefix_removed = True

    cleaned = normalize_space(cleaned)
    cleaned = _rewrite_reports_of_phrase(cleaned)
    if prefix_removed:
        cleaned = _digest_apply_generic_uncertainty(cleaned)
    return normalize_space(cleaned), removed


def _digest_has_citation_language(text: str) -> bool:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    lowered = cleaned.lower()
    if not cleaned:
        return False
    if "t.me/" in lowered or "telegram.me/" in lowered or _FEED_HANDLE_RE.search(cleaned):
        return True
    if _DIGEST_ACCORDING_TO_CITATION_RE.search(cleaned):
        return True
    if _DIGEST_CITATION_WORD_RE.search(cleaned):
        return True
    return False


def _strip_digest_display_noise(text: str) -> str:
    cleaned = normalize_space(text)
    if not cleaned:
        return ""
    cleaned = _DIGEST_CONTEXT_PREFIX_RE.sub("", cleaned)
    cleaned = _DIGEST_LEADING_MARKER_RE.sub("", cleaned)
    cleaned = re.sub(r":\s+[•●▪■◆◦▸]+\s+", ": ", cleaned)
    cleaned = _DIGEST_INLINE_BULLET_RE.sub(", ", cleaned)
    cleaned = _DIGEST_INLINE_ALERT_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    return normalize_space(cleaned.strip(" ,;:-|/[]{}()"))


def _digest_support_item_is_specific(text: str) -> bool:
    cleaned = normalize_space(text)
    if not cleaned:
        return False
    if len(cleaned.split()) < 5:
        return False
    return bool(_DIGEST_SPECIFIC_ACTION_RE.search(cleaned) or re.search(r"\b\d+\b", cleaned))


def _headline_rail_source_label(source: str) -> str:
    cleaned = normalize_space(source).strip(" ,;:.!?-")
    if not cleaned or _HEADLINE_RAIL_GENERIC_SOURCE_RE.fullmatch(cleaned):
        return ""
    words = cleaned.split()
    if len(words) > 4:
        return ""
    return cleaned


def _headline_rail_split_source_attribution(text: str) -> tuple[str, str]:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    if not cleaned:
        return "", ""

    for pattern in (_HEADLINE_RAIL_SOURCE_PREFIX_RE, _HEADLINE_RAIL_SOURCE_TAIL_RE):
        match = pattern.match(cleaned)
        if not match:
            continue
        body = normalize_space(match.group("body"))
        source = _headline_rail_source_label(match.group("source"))
        return body, source
    return cleaned, ""


def _headline_rail_line_issue(text: str) -> str:
    body, _source = _headline_rail_split_source_attribution(text)
    cleaned = normalize_space(body)
    lowered = cleaned.lower()
    if not cleaned:
        return "empty_output"
    if cleaned.endswith("?") or "?" in cleaned:
        return "question_line"
    if _HEADLINE_RAIL_CONTINUATION_RE.match(cleaned):
        return "continuation_line"
    if _HEADLINE_RAIL_DEPENDENT_START_RE.match(cleaned):
        return "dependent_line"
    if _HEADLINE_RAIL_BANTER_RE.search(cleaned):
        return "banter_line"
    if _HEADLINE_RAIL_SOFT_NEWS_RE.search(cleaned):
        return "soft_news"
    if _HEADLINE_RAIL_HISTORY_RE.search(cleaned):
        return "history_fragment"
    if _HEADLINE_RAIL_VAGUE_RESULT_RE.match(cleaned):
        return "vague_copy"
    if _digest_is_low_value_quote_or_rant(cleaned):
        return "low_value"
    if _feed_segment_is_incomplete(cleaned):
        return "incomplete_copy"
    if should_downgrade_explainer_urgency(cleaned) and not looks_like_live_event_update(cleaned):
        return "soft_news"
    if len(cleaned.split()) < 5 and not _DIGEST_SPECIFIC_ACTION_RE.search(cleaned):
        return "headline_too_thin"
    if not (
        looks_like_live_event_update(cleaned)
        or _DIGEST_SPECIFIC_ACTION_RE.search(cleaned)
        or _HEADLINE_RAIL_STANDALONE_ACTION_RE.search(cleaned)
        or re.search(r"\b\d+\b", cleaned)
    ):
        return "headline_too_thin"
    if any(fragment in lowered for fragment in _WEAK_COPY_PATTERNS):
        return "vague_copy"
    return ""


def _headline_rail_line_strength(text: str) -> float:
    body, source = _headline_rail_split_source_attribution(text)
    cleaned = normalize_space(body)
    if not cleaned:
        return 0.0
    score = 0.0
    if looks_like_live_event_update(cleaned):
        score += 2.5
    if _DIGEST_SPECIFIC_ACTION_RE.search(cleaned) or _HEADLINE_RAIL_STANDALONE_ACTION_RE.search(cleaned):
        score += 1.5
    score += min(2.0, float(len(re.findall(r"\b\d+\b", cleaned))))
    score += min(2.0, float(len(_extract_candidate_named_tokens(cleaned))))
    score += min(1.5, len(cleaned.split()) / 10.0)
    if source:
        score += 0.25
    return score


def _headline_rail_topic_tokens(text: str) -> set[str]:
    body, _source = _headline_rail_split_source_attribution(text)
    lowered = normalize_space(body).lower()
    lowered = re.sub(
        r"(?i)\b(?:initial|preliminary|early|unconfirmed)\s+reports?\s+(?:indicate|suggest|point to)\b",
        " ",
        lowered,
    )
    tokens = {
        token
        for token in _HEADLINE_RAIL_TOKEN_RE.findall(lowered)
        if token not in _HEADLINE_RAIL_TOPIC_STOPWORDS
    }
    return tokens


def _headline_rail_same_topic(left: str, right: str) -> bool:
    left_numbers = set(re.findall(r"\b\d+(?:[.,:/-]\d+)*\b", normalize_space(left)))
    right_numbers = set(re.findall(r"\b\d+(?:[.,:/-]\d+)*\b", normalize_space(right)))
    if left_numbers and right_numbers and not (left_numbers & right_numbers):
        return False

    left_key = normalize_space(" ".join(sorted(_headline_rail_topic_tokens(left))))
    right_key = normalize_space(" ".join(sorted(_headline_rail_topic_tokens(right))))
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    ratio = SequenceMatcher(None, left_key, right_key).ratio()
    if ratio >= 0.86:
        return True
    left_tokens = set(left_key.split())
    right_tokens = set(right_key.split())
    overlap = left_tokens & right_tokens
    if len(overlap) < 2:
        return False
    return (len(overlap) / max(1, min(len(left_tokens), len(right_tokens)))) >= 0.5


def _headline_rail_clean_line(text: str, *, max_chars: int) -> str:
    raw = normalize_space(strip_telegram_html(str(text or "")))
    if not raw:
        return ""
    body, source = _headline_rail_split_source_attribution(raw)
    cleaned = _digest_clean_line(body, max_chars=0, allow_short=True)
    if not cleaned:
        return ""
    issue = _headline_rail_line_issue(cleaned)
    if issue:
        return ""
    if source and _HEADLINE_RAIL_CLAIM_CONTEXT_RE.search(cleaned):
        cleaned = f"{cleaned.rstrip('.!?')}, per {source}"
    cleaned = _truncate_digest_fact(cleaned, max_chars=max_chars)
    if not cleaned or _headline_rail_line_issue(cleaned):
        return ""
    return cleaned


def _clean_headline_rail_items(
    values: Iterable[Any],
    *,
    max_lines: int,
    max_chars: int = 0,
    seen: set[str] | None = None,
) -> List[str]:
    cleaned_lines: list[str] = []
    line_keys: list[str] = []
    local_seen = seen if seen is not None else set()

    for value in values:
        if isinstance(value, (list, tuple)):
            nested = _clean_headline_rail_items(
                value,
                max_lines=max_lines,
                max_chars=max_chars,
                seen=local_seen,
            )
            for item in nested:
                if len(cleaned_lines) >= max(1, int(max_lines)):
                    return cleaned_lines
                key = _digest_line_key(item)
                if not key or key in local_seen:
                    continue
                local_seen.add(key)
                cleaned_lines.append(item)
                line_keys.append(key)
            continue

        raw = normalize_space(strip_telegram_html(str(value or "")))
        if not raw:
            continue
        for part in re.split(r"\n+", raw):
            candidate = _headline_rail_clean_line(part, max_chars=max_chars)
            key = _digest_line_key(candidate)
            if not candidate or not key:
                continue
            duplicate_index = None
            for idx, existing in enumerate(cleaned_lines):
                if _headline_rail_same_topic(existing, candidate):
                    duplicate_index = idx
                    break
            if duplicate_index is not None:
                if _headline_rail_line_strength(candidate) > _headline_rail_line_strength(cleaned_lines[duplicate_index]):
                    old_key = line_keys[duplicate_index]
                    if old_key in local_seen:
                        local_seen.discard(old_key)
                    cleaned_lines[duplicate_index] = candidate
                    line_keys[duplicate_index] = key
                    local_seen.add(key)
                continue
            if key in local_seen:
                continue
            local_seen.add(key)
            cleaned_lines.append(candidate)
            line_keys.append(key)
            if len(cleaned_lines) >= max(1, int(max_lines)):
                return cleaned_lines
    return cleaned_lines


def _digest_polish_support_item(text: str, *, max_chars: int) -> str:
    cleaned = _strip_digest_display_noise(_digest_finalize_sentence(text, max_chars=max_chars))
    if not cleaned:
        return ""
    if _digest_is_low_value_quote_or_rant(cleaned):
        return ""
    prefix_match = _DIGEST_GENERIC_UNCERTAINTY_RE.match(cleaned)
    if prefix_match:
        candidate = normalize_space(cleaned[prefix_match.end() :]).strip(" ,;:-")
        if _digest_support_item_is_specific(candidate):
            cleaned = candidate
    if _feed_segment_is_incomplete(cleaned):
        return ""
    cleaned = _truncate_digest_fact(cleaned, max_chars=max_chars)
    if not cleaned or _feed_segment_is_incomplete(cleaned):
        return ""
    return cleaned


def _digest_clean_line(text: str, *, max_chars: int, allow_short: bool = False) -> str:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    if not cleaned:
        return ""
    cleaned = _DIGEST_LEADING_NOISE_RE.sub("", cleaned).strip()
    cleaned = _DIGEST_HASHTAG_RE.sub("", cleaned)
    cleaned = _DIGEST_PROMO_FRAGMENT_RE.sub("", cleaned)
    cleaned, _ = _digest_rewrite_source_attribution(cleaned)
    cleaned = _DIGEST_SOURCE_PREFIX_RE.sub("", cleaned)
    cleaned = _clean_generated_delivery_segment(cleaned)
    cleaned, _ = _digest_rewrite_source_attribution(cleaned)
    cleaned = _DIGEST_PROMO_FRAGMENT_RE.sub("", cleaned)
    cleaned = _strip_digest_display_noise(cleaned)
    cleaned = re.sub(r"(?:[\U0001F300-\U0001FAFF\u2600-\u26FF\u2700-\u27BF\uFE0F]+\s*)+$", "", cleaned)
    cleaned = _rewrite_reports_of_phrase(cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = normalize_space(cleaned.strip(" ,;:-|/[]{}()•"))
    if not cleaned or _DIGEST_PUNCT_ONLY_RE.fullmatch(cleaned):
        return ""
    if _digest_is_low_value_quote_or_rant(cleaned):
        return ""
    if _digest_has_citation_language(cleaned):
        return ""
    if not allow_short and not _digest_needs_english_rewrite(cleaned, "English") and _feed_segment_is_incomplete(cleaned):
        return ""
    return _truncate_digest_fact(cleaned, max_chars=max_chars)


def _digest_finalize_sentence(text: str, *, max_chars: int) -> str:
    cleaned = _digest_clean_line(text, max_chars=max_chars, allow_short=True)
    if not cleaned:
        return ""
    if cleaned.endswith((".", "!", "?", "…")):
        return cleaned
    if len(cleaned.split()) >= 4:
        return f"{cleaned}."
    return cleaned


def _digest_priority_score(value: Any) -> int:
    normalized = normalize_space(str(value or "")).lower()
    if normalized in {"high", "critical", "3"}:
        return 3
    if normalized in {"medium", "2"}:
        return 2
    if normalized in {"low", "1"}:
        return 1
    try:
        numeric = int(value)
    except Exception:
        numeric = 1
    return max(1, min(numeric, 3))


def _digest_priority_label(value: Any) -> str:
    score = _digest_priority_score(value)
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    return "low"


def _coerce_digest_major_block(block: Any) -> Dict[str, Any] | None:
    if not isinstance(block, dict):
        return None
    headline = _digest_clean_line(
        block.get("headline") or block.get("title") or block.get("summary") or "",
        max_chars=120,
    )
    if not headline:
        return None
    lede = _digest_finalize_sentence(
        block.get("lede") or block.get("detail") or block.get("context") or "",
        max_chars=220,
    )
    facts_raw = block.get("facts")
    if not isinstance(facts_raw, list):
        facts_raw = []
    facts: List[str] = []
    seen: set[str] = set()
    headline_key = _digest_line_key(headline)
    lede_key = _digest_line_key(lede)
    for fact in facts_raw:
        cleaned = _digest_finalize_sentence(fact, max_chars=220)
        key = _digest_line_key(cleaned)
        if not cleaned or not key or key in seen or key == headline_key or key == lede_key:
            continue
        seen.add(key)
        facts.append(cleaned)
        if len(facts) >= 4:
            break
    if not lede and facts:
        lede = facts.pop(0)
    return {
        "headline": headline,
        "lede": lede,
        "facts": facts,
        "priority": _digest_priority_label(
            block.get("priority") or block.get("severity") or block.get("importance")
        ),
    }


def _collect_digest_story_sentences(
    values: Iterable[Any],
    *,
    total_max_chars: int = _DIGEST_STORY_MAX_CHARS,
    max_sentences: int = 3,
    sentence_max_chars: int = 220,
    seen: set[str] | None = None,
) -> List[str]:
    out: List[str] = []
    local_seen = seen if seen is not None else set()
    total_chars = 0
    for value in values:
        raw = normalize_space(strip_telegram_html(str(value or "")))
        if not raw:
            continue
        for part in _split_digest_sentences(raw):
            cleaned = _digest_finalize_sentence(part, max_chars=sentence_max_chars)
            key = _digest_line_key(cleaned)
            if not cleaned or not key or key in local_seen:
                continue
            projected = total_chars + len(cleaned) + (1 if out else 0)
            if out and projected > total_max_chars:
                return out
            local_seen.add(key)
            out.append(cleaned)
            total_chars = projected
            if len(out) >= max_sentences:
                return out
    return out


def _build_digest_story_from_fragments(
    values: Iterable[Any],
    *,
    total_max_chars: int = _DIGEST_STORY_MAX_CHARS,
    max_sentences: int = 3,
    seen: set[str] | None = None,
) -> str:
    sentences = _collect_digest_story_sentences(
        values,
        total_max_chars=total_max_chars,
        max_sentences=max_sentences,
        seen=seen,
    )
    return " ".join(sentences).strip()


def _clean_digest_support_items(
    values: Iterable[Any],
    *,
    max_chars: int = 220,
    seen: set[str] | None = None,
) -> List[str]:
    out: List[str] = []
    local_seen = seen if seen is not None else set()
    for value in values:
        if isinstance(value, (list, tuple)):
            nested = _clean_digest_support_items(value, max_chars=max_chars, seen=local_seen)
            out.extend(nested)
            continue
        raw = normalize_space(strip_telegram_html(str(value or "")))
        if not raw:
            continue
        for part in re.split(r"\n+", raw):
            cleaned = _digest_polish_support_item(part, max_chars=max_chars)
            key = _digest_line_key(cleaned)
            if not cleaned or not key or key in local_seen:
                continue
            local_seen.add(key)
            out.append(cleaned)
    return out


def _legacy_digest_to_narrative(
    *,
    scene_setter: str,
    major_blocks: Sequence[Dict[str, Any]],
    timeline_items: Sequence[str],
    max_lines: int,
) -> tuple[str, str, List[str], List[str]]:
    headline = ""
    if major_blocks:
        headline = _digest_clean_line(major_blocks[0].get("headline") or "", max_chars=120)
    headline_key = _digest_line_key(headline)

    story_fragments: List[str] = []
    if scene_setter:
        story_fragments.append(scene_setter)
    for block in list(major_blocks)[:3]:
        if block.get("lede"):
            story_fragments.append(str(block.get("lede") or ""))
        elif block.get("facts"):
            story_fragments.append(str((block.get("facts") or [""])[0]))
    story = _build_digest_story_from_fragments(
        story_fragments,
        total_max_chars=_DIGEST_STORY_MAX_CHARS,
        max_sentences=3,
        seen={headline_key} if headline_key else None,
    )

    highlight_candidates: List[str] = []
    also_candidates: List[str] = [str(item) for item in timeline_items if normalize_space(str(item))]
    for index, block in enumerate(major_blocks):
        facts = [str(item) for item in (block.get("facts") or []) if normalize_space(str(item))]
        if index == 0:
            highlight_candidates.extend(facts)
            continue
        lede = normalize_space(str(block.get("lede") or ""))
        if lede:
            highlight_candidates.append(lede)
        highlight_candidates.extend(facts)

    if len(also_candidates) > _digest_also_moving_cap(max_lines):
        highlight_candidates.extend(also_candidates[_digest_also_moving_cap(max_lines) :])
        also_candidates = also_candidates[: _digest_also_moving_cap(max_lines)]

    seen: set[str] = set()
    if headline_key:
        seen.add(headline_key)
    for sentence in _collect_digest_story_sentences([story], max_sentences=8):
        sentence_key = _digest_line_key(sentence)
        if sentence_key:
            seen.add(sentence_key)

    highlights = _clean_digest_support_items(highlight_candidates, seen=seen)
    also_moving = _clean_digest_support_items(also_candidates, max_chars=200, seen=seen)

    if not story and highlights:
        story = highlights.pop(0)
        story_key = _digest_line_key(story)
        if story_key:
            seen.add(story_key)
    if not headline:
        headline = _fallback_headline(story or " ".join(highlights[:2])) or ""
        headline = _digest_clean_line(headline, max_chars=120, allow_short=True)

    return headline, story, highlights, also_moving


def _digest_is_headline_rail(interval_minutes: int) -> bool:
    return digest_output_style(interval_minutes) == "headline_rail"


def _headline_rail_title(interval_minutes: int) -> str:
    minutes = max(1, int(interval_minutes))
    if minutes == 60:
        return "Top headlines from the last hour"
    if minutes < 60:
        return f"Top headlines from the last {minutes} minutes"
    hours = max(1, round(minutes / 60))
    return f"Top headlines from the last {hours} hours"


def _digest_payload_to_narrative(
    payload: Dict[str, Any],
    *,
    max_lines: int,
    interval_minutes: int,
) -> tuple[str, str, List[str], List[str]]:
    headline_mode = _digest_is_headline_rail(interval_minutes)
    if headline_mode:
        headline = _digest_clean_line(
            payload.get("headline") or _headline_rail_title(interval_minutes),
            max_chars=120,
            allow_short=True,
        )
        headline_key = _digest_line_key(headline)

        headlines_raw = payload.get("headlines")
        if not isinstance(headlines_raw, list):
            headlines_raw = payload.get("highlights")
        if not isinstance(headlines_raw, list):
            headlines_raw = payload.get("facts")
        if not isinstance(headlines_raw, list):
            headlines_raw = []

        also_raw = payload.get("also_moving")
        if not isinstance(also_raw, list):
            also_raw = []

        seen: set[str] = set()
        if headline_key:
            seen.add(headline_key)
        highlights = _clean_headline_rail_items(
            headlines_raw,
            max_lines=max_lines,
            max_chars=0,
            seen=seen,
        )
        remaining_for_also = max(0, max_lines - len(highlights))
        also_moving = _clean_headline_rail_items(
            also_raw,
            max_lines=max(1, remaining_for_also or 1),
            max_chars=0,
            seen=seen,
        )

        if not highlights:
            fallback_values: list[Any] = []
            for value in (
                payload.get("story"),
                payload.get("summary"),
                payload.get("scene_setter"),
            ):
                if normalize_space(str(value or "")):
                    fallback_values.append(value)
            legacy_blocks = payload.get("major_blocks")
            if isinstance(legacy_blocks, list):
                for block in legacy_blocks:
                    if not isinstance(block, dict):
                        continue
                    fallback_values.extend(
                        [
                            block.get("headline") or "",
                            block.get("lede") or "",
                            *(block.get("facts") or [] if isinstance(block.get("facts"), list) else []),
                        ]
                    )
            timeline_items = payload.get("timeline_items")
            if isinstance(timeline_items, list):
                fallback_values.extend(timeline_items)
            highlights = _clean_headline_rail_items(
                fallback_values,
                max_lines=max_lines,
                max_chars=0,
                seen=seen,
            )

        if len(also_moving) > _digest_also_moving_cap(max_lines):
            highlights.extend(also_moving[_digest_also_moving_cap(max_lines) :])
            also_moving = also_moving[: _digest_also_moving_cap(max_lines)]
        highlights, also_moving = _cap_headline_rail_support(
            highlights,
            also_moving,
            max_lines=max_lines,
        )
        if not headline and highlights:
            headline = _headline_rail_title(interval_minutes)
        return headline, "", highlights, also_moving

    headline = _digest_clean_line(payload.get("headline") or "", max_chars=120, allow_short=True)
    headline_key = _digest_line_key(headline)
    story = _build_digest_story_from_fragments(
        [payload.get("story") or payload.get("summary") or ""],
        total_max_chars=_DIGEST_STORY_MAX_CHARS,
        max_sentences=4,
        seen={headline_key} if headline_key else None,
    )

    highlights_raw = payload.get("highlights")
    if not isinstance(highlights_raw, list):
        highlights_raw = payload.get("facts")
    if not isinstance(highlights_raw, list):
        highlights_raw = []

    also_raw = payload.get("also_moving")
    if not isinstance(also_raw, list):
        also_raw = []

    seen: set[str] = set()
    if headline_key:
        seen.add(headline_key)
    for sentence in _collect_digest_story_sentences([story], max_sentences=8):
        sentence_key = _digest_line_key(sentence)
        if sentence_key:
            seen.add(sentence_key)

    highlights = _clean_digest_support_items(highlights_raw, seen=seen)
    also_moving = _clean_digest_support_items(also_raw, max_chars=200, seen=seen)

    if headline or story or highlights or also_moving:
        if len(also_moving) > _digest_also_moving_cap(max_lines):
            highlights.extend(also_moving[_digest_also_moving_cap(max_lines) :])
            also_moving = also_moving[: _digest_also_moving_cap(max_lines)]
        if not story and highlights:
            story = highlights.pop(0)
        if not headline:
            headline = _digest_clean_line(
                _fallback_headline(story or " ".join(highlights[:2])) or "",
                max_chars=120,
                allow_short=True,
            )
        return headline, story, highlights, also_moving

    scene_setter = normalize_space(str(payload.get("scene_setter") or ""))
    major_blocks_raw = payload.get("major_blocks")
    timeline_raw = payload.get("timeline_items")

    major_blocks: List[Dict[str, Any]] = []
    timeline_items: List[str] = []

    if isinstance(major_blocks_raw, list) and major_blocks_raw:
        for block in major_blocks_raw:
            coerced = _coerce_digest_major_block(block)
            if coerced:
                major_blocks.append(coerced)
    else:
        legacy_blocks = payload.get("blocks")
        if not isinstance(legacy_blocks, list):
            legacy_blocks = payload.get("items")
        if isinstance(legacy_blocks, list):
            ordered_legacy = sorted(
                [block for block in legacy_blocks if isinstance(block, dict)],
                key=lambda block: _digest_priority_score(
                    block.get("priority") or block.get("severity") or block.get("importance")
                ),
                reverse=True,
            )
            for block in ordered_legacy:
                coerced = _coerce_digest_major_block(block)
                if coerced:
                    major_blocks.append(coerced)

    if isinstance(timeline_raw, list):
        timeline_items.extend(str(item) for item in timeline_raw if normalize_space(str(item)))

    return _legacy_digest_to_narrative(
        scene_setter=scene_setter,
        major_blocks=major_blocks,
        timeline_items=timeline_items,
        max_lines=max_lines,
    )


def extract_digest_narrative_parts(
    text: str,
    *,
    max_lines: int,
) -> tuple[str, str, List[str], List[str]]:
    cleaned = _strip_digest_citations((text or "").strip())
    if not cleaned:
        return "", "", [], []

    quiet = quiet_period_message(30)
    if normalize_space(strip_telegram_html(cleaned)) == normalize_space(strip_telegram_html(quiet)):
        return "", "", [], []

    raw_blocks = re.split(r"(?:<br\s*/?>\s*){2,}|\n\s*\n", cleaned, flags=re.IGNORECASE)
    headline = ""
    story_fragments: List[str] = []
    highlight_candidates: List[str] = []
    also_candidates: List[str] = []
    saw_non_bullet_body = False

    for raw_block in raw_blocks:
        block_has_bold = bool(re.search(r"<b>.*?</b>", raw_block, flags=re.IGNORECASE | re.DOTALL))
        text_lines = re.sub(r"<br\s*/?>", "\n", raw_block, flags=re.IGNORECASE)
        lines_raw = [line.rstrip() for line in text_lines.splitlines() if line.strip()]
        if not lines_raw:
            continue

        parsed_lines: List[tuple[str, bool]] = []
        for raw_line in lines_raw:
            plain = normalize_space(strip_telegram_html(raw_line))
            is_bullet = plain.startswith(("•", "-", "*"))
            cleaned_line = _digest_finalize_sentence(raw_line, max_chars=240)
            if cleaned_line:
                parsed_lines.append((cleaned_line, is_bullet))
        if not parsed_lines:
            continue

        if _DIGEST_ALSO_MOVING_RE.fullmatch(normalize_space(parsed_lines[0][0])):
            also_candidates.extend(line for line, _is_bullet in parsed_lines[1:])
            continue

        start_index = 0
        if block_has_bold:
            candidate_headline = _digest_clean_line(lines_raw[0], max_chars=120, allow_short=True)
            if candidate_headline and not headline:
                headline = candidate_headline
            start_index = 1

        for line, is_bullet in parsed_lines[start_index:]:
            if is_bullet:
                highlight_candidates.append(line)
            else:
                saw_non_bullet_body = True
                story_fragments.append(line)

    story = _build_digest_story_from_fragments(
        story_fragments,
        total_max_chars=_DIGEST_STORY_MAX_CHARS,
        max_sentences=4,
        seen={_digest_line_key(headline)} if _digest_line_key(headline) else None,
    )
    if not headline:
        headline = _digest_clean_line(
            _fallback_headline(story or " ".join(highlight_candidates[:2])) or "",
            max_chars=120,
            allow_short=True,
        )

    if len(also_candidates) > _digest_also_moving_cap(max_lines):
        highlight_candidates.extend(also_candidates[_digest_also_moving_cap(max_lines) :])
        also_candidates = also_candidates[: _digest_also_moving_cap(max_lines)]

    seen: set[str] = set()
    headline_key = _digest_line_key(headline)
    if headline_key:
        seen.add(headline_key)
    for sentence in _collect_digest_story_sentences([story], max_sentences=8):
        sentence_key = _digest_line_key(sentence)
        if sentence_key:
            seen.add(sentence_key)
    highlights = _clean_digest_support_items(highlight_candidates, seen=seen)
    also_moving = _clean_digest_support_items(also_candidates, max_chars=200, seen=seen)

    if not story and highlights and not (headline and not saw_non_bullet_body):
        story = highlights.pop(0)

    return headline, story, highlights, also_moving


def _render_digest_layout(
    *,
    headline: str,
    story: str,
    highlights: Sequence[str],
    also_moving: Sequence[str],
    interval_minutes: int,
) -> str:
    quiet = quiet_period_message(interval_minutes)
    headline_mode = _digest_is_headline_rail(interval_minutes)

    clean_headline = _digest_clean_line(headline, max_chars=120, allow_short=True)
    if headline_mode and not clean_headline:
        clean_headline = _headline_rail_title(interval_minutes)
    clean_story = _build_digest_story_from_fragments([story], total_max_chars=_DIGEST_STORY_MAX_CHARS, max_sentences=4)
    if not clean_headline:
        return quiet

    seen: set[str] = set()
    headline_key = _digest_line_key(clean_headline)
    if headline_key:
        seen.add(headline_key)
    if clean_story:
        story_key = _digest_line_key(clean_story)
        if story_key:
            seen.add(story_key)

    if headline_mode:
        max_lines = _resolve_digest_max_lines()
        clean_highlights = _clean_headline_rail_items(
            highlights,
            max_lines=max_lines,
            max_chars=0,
            seen=seen,
        )
        remaining_for_story = max(0, max_lines - len(clean_highlights))
        story_headlines = _clean_headline_rail_items(
            _split_digest_sentences(clean_story),
            max_lines=max(1, remaining_for_story or 1),
            max_chars=0,
            seen=seen,
        )
        if story_headlines:
            clean_highlights = [*story_headlines, *clean_highlights]
        if not clean_highlights:
            clean_highlights = _clean_headline_rail_items(
                [clean_story],
                max_lines=max_lines,
                max_chars=0,
                seen=seen,
            )
        remaining_for_also = max(0, max_lines - len(clean_highlights))
        clean_also = _clean_headline_rail_items(
            also_moving,
            max_lines=max(1, remaining_for_also or 1),
            max_chars=0,
            seen=seen,
        )
        clean_highlights, clean_also = _cap_headline_rail_support(
            clean_highlights,
            clean_also,
            max_lines=max_lines,
        )
        if not clean_highlights and not clean_also:
            return quiet

        rendered_blocks = [
            "<br>".join(
                [
                    f"<b>{sanitize_telegram_html(clean_headline)}</b>",
                    *[f"• {sanitize_telegram_html(item)}" for item in clean_highlights],
                ]
            )
        ]
        if clean_also:
            also_lines = ["<i>Also moving</i>"]
            also_lines.extend(f"• {sanitize_telegram_html(item)}" for item in clean_also)
            rendered_blocks.append("<br>".join(also_lines))
        return sanitize_telegram_html("<br><br>".join(rendered_blocks))

    clean_highlights = _clean_digest_support_items(
        highlights,
        max_chars=220,
        seen=seen,
    )
    clean_also = _clean_digest_support_items(
        also_moving,
        max_chars=200,
        seen=seen,
    )
    if len(clean_also) > _digest_also_moving_cap():
        clean_highlights.extend(clean_also[_digest_also_moving_cap() :])
        clean_also = clean_also[: _digest_also_moving_cap()]

    if not clean_story:
        return quiet

    story_sentences = _collect_digest_story_sentences([clean_story], max_sentences=4)
    if story_sentences and SequenceMatcher(None, story_sentences[0].lower(), clean_headline.lower()).ratio() >= 0.86:
        trimmed_story = " ".join(story_sentences[1:]).strip()
        if trimmed_story:
            clean_story = trimmed_story
    if _digest_line_key(clean_story) == _digest_line_key(clean_headline):
        if clean_highlights:
            clean_story = clean_highlights.pop(0)
        elif clean_also:
            clean_story = clean_also.pop(0)
        else:
            return quiet

    main_lines = [
        f"<b>{sanitize_telegram_html(clean_headline)}</b>",
        sanitize_telegram_html(clean_story),
    ]
    main_lines.extend(f"• {sanitize_telegram_html(item)}" for item in clean_highlights)
    rendered_blocks = ["<br>".join(main_lines)]

    if clean_also:
        also_lines = ["<i>Also moving</i>"]
        also_lines.extend(f"• {sanitize_telegram_html(item)}" for item in clean_also)
        rendered_blocks.append("<br>".join(also_lines))

    return sanitize_telegram_html("<br><br>".join(rendered_blocks))


def _json_digest_to_html(payload: Dict[str, Any], *, interval_minutes: int, max_lines: int) -> str:
    if bool(payload.get("quiet")):
        return quiet_period_message(interval_minutes)
    headline, story, highlights, also_moving = _digest_payload_to_narrative(
        payload,
        max_lines=max_lines,
        interval_minutes=interval_minutes,
    )
    return _render_digest_layout(
        headline=headline,
        story=story,
        highlights=highlights,
        also_moving=also_moving,
        interval_minutes=interval_minutes,
    )


def _strip_digest_citations(text: str) -> str:
    cleaned = str(text or "")
    if not cleaned:
        return ""
    cleaned = re.sub(r'<a\s+href="[^"]*">\s*Read more\s*</a>', "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<a\s+href="[^"]*">.*?</a>', "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bRead more\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = _FEED_HANDLE_RE.sub("", cleaned)
    cleaned = _FEED_TELEGRAM_LINK_RE.sub("", cleaned)
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\[[^\]]+\]", "", cleaned)
    cleaned = re.sub(r"(?i)\b(?:source|outlet|channel)\s*[:\-–—|]+\s*", "", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
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
        "- Keep the same digest shape the input already uses: headline rail stays a headline rail, story digest stays a story digest\n"
        "- Make the English sound sharp, human, and slightly unhinged without inventing anything\n"
        "- Translate every non-English word or phrase into English\n"
        "- Remove citations, source names, bracket tags, outlet names, links, handles, hashtags, and any 'Read more' text\n"
        "- When attribution is necessary, use generic uncertainty only, such as 'initial reports indicate' or 'preliminary reports suggest'\n"
        "- Never name a channel, outlet, username, or source label\n"
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
    if not _digest_needs_english_rewrite(cleaned, "English"):
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
    headline, story, highlights, also_moving = extract_digest_narrative_parts(
        cleaned,
        max_lines=max_lines,
    )
    return _render_digest_layout(
        headline=headline,
        story=story,
        highlights=highlights,
        also_moving=also_moving,
        interval_minutes=interval_minutes,
    )


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
) -> Tuple[List[str], int, int]:
    min_len = int(max(1, int(getattr(config, "DIGEST_MIN_POST_LENGTH", 12))))

    eligible_posts: List[Dict[str, object]] = []
    for post in posts:
        text = _post_text(post)
        if len(text) < min_len:
            continue
        if _likely_noise(text):
            continue
        eligible_posts.append(post)

    lines: List[str] = []
    used_tokens = 0
    total_input = len(posts)

    for idx, post in enumerate(eligible_posts, start=1):
        text = _post_text(post)
        if len(text) > 1200:
            text = f"{text[:1197].rsplit(' ', 1)[0]}..."

        ts = _post_timestamp(post)
        ts_label = (
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if ts
            else "unknown-time"
        )

        line = f"{idx}. ({ts_label}) {text}"

        line_tokens = estimate_tokens_rough(line) + 8
        if lines and (used_tokens + line_tokens > token_budget):
            break
        if not lines and line_tokens > token_budget:
            # Guarantee at least one line by truncating aggressively.
            short = text[: max(120, token_budget * 4)]
            line = f"{idx}. ({ts_label}) {short}..."
            line_tokens = estimate_tokens_rough(line)

        lines.append(line)
        used_tokens += line_tokens

    return lines, len(lines), total_input


def _truncate_digest_fact(text: str, *, max_chars: int) -> str:
    cleaned = normalize_space(text)
    if max_chars <= 0:
        return cleaned
    if len(cleaned) <= max_chars:
        return cleaned
    punctuation_cut = max(cleaned.rfind(mark, 0, max_chars) for mark in ".!?;:")
    if punctuation_cut >= int(max_chars * 0.55):
        candidate = cleaned[: punctuation_cut + 1].strip()
    else:
        candidate = cleaned[:max_chars].rsplit(" ", 1)[0].rstrip(" ,;:-|/")
    if not candidate:
        return ""
    if _feed_segment_is_incomplete(candidate):
        candidate = re.sub(
            r"(?i)\b(?:and|or|but|with|after|before|near|from|to|of|in|on|at|by|as|for|while|since|during)\s+\S+$",
            "",
            candidate,
        ).rstrip(" ,;:-|/")
    candidate = normalize_space(candidate)
    if not candidate or _feed_segment_is_incomplete(candidate):
        return ""
    if not candidate.endswith((".", "!", "?")):
        candidate = f"{candidate}."
    return candidate


def _split_digest_sentences(text: str) -> List[str]:
    pieces = re.split(r"(?:\r?\n)+|(?<=[.!?])\s+", normalize_space(text))
    return [normalize_space(piece) for piece in pieces if normalize_space(piece)]


def _digest_posts_are_near_duplicates(left: str, right: str) -> bool:
    left_key = _digest_line_key(left)
    right_key = _digest_line_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    if len(shorter.split()) >= 8 and shorter in longer:
        return True
    return SequenceMatcher(None, left_key, right_key).ratio() >= 0.94


def _dedupe_prepared_digest_posts(posts: Sequence[Dict[str, object]]) -> tuple[List[Dict[str, object]], int]:
    unique_posts: List[Dict[str, object]] = []
    removed = 0
    for post in posts:
        text = _post_text(post)
        if not text:
            continue
        if any(_digest_posts_are_near_duplicates(text, _post_text(existing)) for existing in unique_posts):
            removed += 1
            continue
        unique_posts.append(dict(post))
    return unique_posts, removed


def _clean_digest_source_text(text: str) -> tuple[str, int, int]:
    raw = re.sub(r"(?i)<br\s*/?>", "\n", _strip_digest_citations(strip_telegram_html(str(text or ""))))
    removed = 0
    citation_removed = 0
    cleaned_lines: List[str] = []
    seen: set[str] = set()
    for part in re.split(r"\n+|(?<=[.!?])\s+", raw):
        rewritten, rewritten_count = _digest_rewrite_source_attribution(part)
        citation_removed += rewritten_count
        candidate = _digest_finalize_sentence(rewritten, max_chars=280)
        if not candidate:
            removed += 1
            continue
        key = candidate.lower()
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        cleaned_lines.append(candidate)
    return normalize_space(" ".join(cleaned_lines)), removed, citation_removed


async def _translate_digest_posts_to_english(
    posts: Sequence[Dict[str, object]],
    auth_manager: AuthManager,
) -> tuple[List[Dict[str, object]], int, int]:
    targets = [
        (idx, _post_text(post))
        for idx, post in enumerate(posts, start=1)
        if _digest_needs_english_rewrite(_post_text(post), "English")
    ]
    if not targets:
        return [dict(post) for post in posts], 0, 0

    payload = "\n".join(f"{idx}. {text}" for idx, text in targets)
    instructions = (
        "Translate each numbered digest evidence line into sharp English plain text.\n"
        'Return ONLY one JSON object with this schema: {"items":[{"id": number, "text": string}]}.\n'
        "Rules:\n"
        "- Keep the factual meaning and uncertainty markers.\n"
        "- Remove usernames, channel names, outlet labels, links, hashtags, and promo text.\n"
        "- Never write source-style attribution such as 'Hebrew sources report' or 'Channel X said'.\n"
        "- If the original line depends on unattributed media or source wording, rewrite it with generic uncertainty such as 'initial reports indicate'.\n"
        "- Do not add any new facts.\n"
        "- Keep each item as one clean English evidence line."
    )
    try:
        raw = await _call_codex_with_auth_repair(payload, auth_manager, instructions, verbosity="low")
        parsed = _try_parse_digest_json(raw) or {}
    except Exception:
        return [dict(post) for post in posts], 0, 0

    translated_items = parsed.get("items")
    if not isinstance(translated_items, list):
        return [dict(post) for post in posts], 0, 0

    translated_map: Dict[int, str] = {}
    citation_removed_total = 0
    for item in translated_items:
        if not isinstance(item, dict):
            continue
        try:
            item_id = int(item.get("id") or 0)
        except Exception:
            item_id = 0
        rewritten, rewritten_count = _digest_rewrite_source_attribution(item.get("text") or "")
        citation_removed_total += rewritten_count
        cleaned = _digest_finalize_sentence(rewritten, max_chars=280)
        if item_id > 0 and cleaned:
            translated_map[item_id] = cleaned

    translated_count = 0
    updated_posts: List[Dict[str, object]] = []
    for idx, post in enumerate(posts, start=1):
        updated = dict(post)
        translated = translated_map.get(idx)
        if translated:
            updated["raw_text"] = translated
            translated_count += 1
        updated_posts.append(updated)
    return updated_posts, translated_count, citation_removed_total


async def _prepare_digest_posts(
    posts: Sequence[Dict[str, object]],
    auth_manager: AuthManager,
) -> tuple[List[Dict[str, object]], Dict[str, int]]:
    prepared: List[Dict[str, object]] = []
    noise_stripped_count = 0
    citation_stripped_count = 0
    for post in posts:
        cleaned_text, removed, citation_removed = _clean_digest_source_text(_post_text(post))
        noise_stripped_count += removed
        citation_stripped_count += citation_removed
        if not cleaned_text or _likely_noise(cleaned_text):
            noise_stripped_count += 1
            continue
        updated = dict(post)
        updated["raw_text"] = cleaned_text
        prepared.append(updated)

    translated_posts, translation_applied_count, translated_citation_strips = await _translate_digest_posts_to_english(prepared, auth_manager)
    citation_stripped_count += translated_citation_strips
    deduped_posts, duplicate_collapsed_count = _dedupe_prepared_digest_posts(translated_posts)
    english_ready_posts = [
        post
        for post in deduped_posts
        if not _digest_needs_english_rewrite(_post_text(post), "English")
    ]
    noise_stripped_count += max(0, len(deduped_posts) - len(english_ready_posts))
    return english_ready_posts, {
        "noise_stripped_count": noise_stripped_count,
        "translation_applied_count": translation_applied_count,
        "citation_stripped_count": citation_stripped_count,
        "duplicate_collapsed_count": duplicate_collapsed_count,
    }


def _fallback_digest_layout(posts: Sequence[Dict[str, object]]) -> tuple[str, str, List[str], List[str]]:
    unique_posts: List[str] = []
    seen: set[str] = set()
    for post in posts:
        text = _post_text(post)
        if not text or _likely_noise(text) or _digest_needs_english_rewrite(text, "English"):
            continue
        fingerprint = _cache_key(text.lower())
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        unique_posts.append(text)

    if not unique_posts:
        return "", "", [], []

    headline = _digest_clean_line(_fallback_headline(unique_posts[0]) or unique_posts[0], max_chars=120)
    headline_key = _digest_line_key(headline)

    story_fragments: List[str] = []
    highlight_candidates: List[str] = []
    also_candidates: List[str] = []
    for index, text in enumerate(unique_posts):
        sentences = [_digest_finalize_sentence(part, max_chars=220) for part in _split_digest_sentences(text)]
        sentences = [sentence for sentence in sentences if sentence]
        if not sentences:
            continue
        if index < 3:
            story_fragments.append(sentences[0])
            highlight_candidates.extend(sentences[1:3])
        else:
            also_candidates.append(sentences[0])
            highlight_candidates.extend(sentences[1:2])

    story = _build_digest_story_from_fragments(
        story_fragments,
        total_max_chars=_DIGEST_STORY_MAX_CHARS,
        max_sentences=3,
        seen={headline_key} if headline_key else None,
    )

    seen_lines: set[str] = set()
    if headline_key:
        seen_lines.add(headline_key)
    for sentence in _collect_digest_story_sentences([story], max_sentences=8):
        sentence_key = _digest_line_key(sentence)
        if sentence_key:
            seen_lines.add(sentence_key)
    highlights = _clean_digest_support_items(highlight_candidates, seen=seen_lines)
    also_moving = _clean_digest_support_items(also_candidates, max_chars=200, seen=seen_lines)

    if len(also_moving) > _digest_also_moving_cap():
        highlights.extend(also_moving[_digest_also_moving_cap() :])
        also_moving = also_moving[: _digest_also_moving_cap()]

    if not story and highlights:
        story = highlights.pop(0)
    if not headline:
        headline = _digest_clean_line(_fallback_headline(story or " ".join(highlights[:2])) or "", max_chars=120)

    return headline, story, highlights, also_moving


def local_fallback_digest(posts: Sequence[Dict[str, object]], *, interval_minutes: int) -> str:
    if not posts:
        return quiet_period_message(interval_minutes)

    if _digest_is_headline_rail(interval_minutes):
        seen: set[str] = set()
        headline_candidates: list[str] = []
        for post in posts:
            text = _post_text(post)
            if not text or _likely_noise(text) or _digest_needs_english_rewrite(text, "English"):
                continue
            sentences = [_digest_finalize_sentence(part, max_chars=180) for part in _split_digest_sentences(text)]
            sentences = [sentence for sentence in sentences if sentence]
            candidate = _digest_clean_line(
                _fallback_headline(text) or (sentences[0] if sentences else text),
                max_chars=180,
                allow_short=True,
            )
            key = _digest_line_key(candidate)
            if not candidate or not key or key in seen:
                continue
            seen.add(key)
            headline_candidates.append(candidate)
        headlines = _clean_headline_rail_items(
            headline_candidates,
            max_lines=_resolve_digest_max_lines(),
            max_chars=180,
        )
        headlines, _ = _cap_headline_rail_support(
            headlines,
            [],
            max_lines=_resolve_digest_max_lines(),
        )
        return _render_digest_layout(
            headline=_headline_rail_title(interval_minutes),
            story="",
            highlights=headlines,
            also_moving=[],
            interval_minutes=interval_minutes,
        )

    headline, story, highlights, also_moving = _fallback_digest_layout(posts)
    return _render_digest_layout(
        headline=headline,
        story=story,
        highlights=highlights,
        also_moving=also_moving,
        interval_minutes=interval_minutes,
    )


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


_STRATEGIC_TREND_EVENT_MARKERS = (
    "strike",
    "strikes",
    "airstrike",
    "missile",
    "missiles",
    "drone",
    "drones",
    "launch",
    "launched",
    "salvo",
    "warning",
    "warnings",
    "alert",
    "alerts",
    "ceasefire",
    "truce",
    "talks",
    "negotiation",
    "negotiations",
    "diplomatic",
    "diplomacy",
    "official",
    "officials",
    "minister",
    "military",
    "army",
    "idf",
    "irgc",
)
_STRATEGIC_TREND_ESCALATION_MARKERS = (
    "escalating",
    "escalation",
    "intensifying",
    "intensified",
    "widening",
    "broader",
    "broadened",
    "expanded",
    "expanding",
    "stepped up",
    "more strikes",
    "fresh strikes",
    "another wave",
    "heavier",
    "higher tempo",
    "new wave",
    "wider front",
    "retaliation widened",
    "exchange widened",
)
_STRATEGIC_TREND_DEESCALATION_MARKERS = (
    "de-escalating",
    "deescalating",
    "de-escalation",
    "deescalation",
    "easing",
    "eased",
    "pace was lower",
    "lower than the earlier peak",
    "lower tempo",
    "slower pace",
    "reduced",
    "scaled back",
    "paused",
    "pause",
    "restraint",
    "not widening",
    "no widening",
    "held back",
    "cooling",
    "calmer",
    "diplomatic push",
    "ceasefire",
    "truce",
)
_STRATEGIC_TREND_MIXED_MARKERS = (
    "mixed signals",
    "no clear shift",
    "no clear decisive shift",
    "still active but",
    "still active, but",
    "remains active but",
    "active but lower",
    "continued but lower",
    "still active",
)
_STRATEGIC_TREND_RHETORIC_MARKERS = (
    "religious crusade",
    "to quote the bible",
    "speck of sawdust",
    "plank in your own eye",
    "look like some kind of",
    "unhinged",
    "glorious",
    "heroic",
    "evil enemy",
    "martyrdom",
)
_STRATEGIC_TREND_OFFTOPIC_MARKERS = (
    "support us",
    "follow us",
    "subscribe",
    "original msg",
    "fwd from",
)


@dataclass(frozen=True)
class StrategicTrendEvidence:
    source: str
    text: str
    signal: Literal["escalating", "de-escalating", "mixed", "steady"]
    score: float
    timestamp: int
    is_web: bool


@dataclass(frozen=True)
class StrategicTrendSummary:
    verdict: Literal["escalating", "de-escalating", "mixed", "insufficient"]
    qualification: str
    bullets: tuple[str, ...]
    escalating_evidence: tuple[StrategicTrendEvidence, ...]
    deescalating_evidence: tuple[StrategicTrendEvidence, ...]
    mixed_evidence: tuple[StrategicTrendEvidence, ...]
    distinct_sources: int
    recent_span_hours: float


def _query_is_quote_heavy(text: str) -> bool:
    raw = str(text or "")
    quote_count = raw.count('"') + raw.count("“") + raw.count("”") + raw.count("'")
    return quote_count >= 4


def _strategic_query_signal(text: str) -> Literal["escalating", "de-escalating", "mixed", "steady", "none"]:
    lowered = normalize_space(text).lower()
    if not lowered:
        return "none"
    if re.search(
        r"\b(?:not|no|without|neither side(?:\s+(?:was|is))?)\b.{0,24}\b(?:widen(?:ing)?|broader|expand(?:ing|ed)?)\b",
        lowered,
    ):
        return "de-escalating"
    escalation_hits = sum(1 for marker in _STRATEGIC_TREND_ESCALATION_MARKERS if marker in lowered)
    deescalation_hits = sum(1 for marker in _STRATEGIC_TREND_DEESCALATION_MARKERS if marker in lowered)
    mixed_hits = sum(1 for marker in _STRATEGIC_TREND_MIXED_MARKERS if marker in lowered)
    event_hits = sum(1 for marker in _STRATEGIC_TREND_EVENT_MARKERS if marker in lowered)

    if mixed_hits > 0 or (escalation_hits > 0 and deescalation_hits > 0):
        return "mixed"
    if escalation_hits > 0:
        return "escalating"
    if deescalation_hits > 0:
        return "de-escalating"
    if event_hits > 0:
        return "steady"
    return "none"


def _strategic_query_noise_penalty(text: str) -> float:
    lowered = normalize_space(text).lower()
    if not lowered:
        return 5.0
    penalty = 0.0
    if any(marker in lowered for marker in _STRATEGIC_TREND_OFFTOPIC_MARKERS):
        penalty += 3.5
    if any(marker in lowered for marker in _STRATEGIC_TREND_RHETORIC_MARKERS):
        penalty += 4.0
    if _query_is_quote_heavy(text):
        penalty += 2.6
    if len(lowered.split()) < 6:
        penalty += 1.2
    if _strategic_query_signal(text) == "none":
        penalty += 2.8
    return penalty


def _strategic_query_signal_bonus(text: str) -> float:
    lowered = normalize_space(text).lower()
    if not lowered:
        return 0.0
    event_hits = sum(1 for marker in _STRATEGIC_TREND_EVENT_MARKERS if marker in lowered)
    escalation_hits = sum(1 for marker in _STRATEGIC_TREND_ESCALATION_MARKERS if marker in lowered)
    deescalation_hits = sum(1 for marker in _STRATEGIC_TREND_DEESCALATION_MARKERS if marker in lowered)
    mixed_hits = sum(1 for marker in _STRATEGIC_TREND_MIXED_MARKERS if marker in lowered)
    official_hits = sum(
        1
        for marker in ("official", "officials", "minister", "spokesperson", "military", "idf", "irgc")
        if marker in lowered
    )

    bonus = min(2.8, event_hits * 0.3)
    bonus += min(2.4, max(escalation_hits, deescalation_hits) * 0.8)
    bonus += min(1.2, mixed_hits * 0.8)
    bonus += min(1.0, official_hits * 0.25)
    return bonus


def _strategic_query_verdict_title(verdict: Literal["escalating", "de-escalating", "mixed", "insufficient"]) -> str:
    if verdict == "escalating":
        return "Still active and escalating"
    if verdict == "de-escalating":
        return "Still active, but easing from the earlier peak"
    if verdict == "mixed":
        return "Still active with mixed signals; no clear decisive shift"
    return "Not enough recent evidence to judge direction"


def _build_strategic_trend_summary(
    query: str,
    context_messages: Sequence[Dict[str, object]],
) -> StrategicTrendSummary:
    now_ts = int(time.time())
    ranked = _rank_query_context(query, context_messages, limit=18)
    evidence: list[StrategicTrendEvidence] = []

    for item in ranked:
        source = normalize_space(str(item.get("source") or "unknown"))
        timestamp = int(item.get("timestamp") or 0)
        is_web = bool(item.get("is_web"))
        raw_text = normalize_space(str(item.get("text") or ""))
        if not raw_text:
            continue
        for sentence in _split_digest_sentences(raw_text):
            cleaned = _digest_finalize_sentence(sentence, max_chars=220)
            if not cleaned:
                continue
            signal = _strategic_query_signal(cleaned)
            if signal == "none":
                continue
            sentence_item = dict(item)
            sentence_item["text"] = cleaned
            score = _score_query_context_row(query, sentence_item, now_ts=now_ts)
            if score <= 0:
                continue
            evidence.append(
                StrategicTrendEvidence(
                    source=source,
                    text=cleaned,
                    signal=signal,
                    score=score,
                    timestamp=timestamp,
                    is_web=is_web,
                )
            )

    unique_sources = {item.source.lower() for item in evidence if normalize_space(item.source)}
    if not evidence:
        return StrategicTrendSummary(
            verdict="insufficient",
            qualification="There is too little recent, reliable evidence here to judge whether the conflict is intensifying or easing.",
            bullets=(),
            escalating_evidence=(),
            deescalating_evidence=(),
            mixed_evidence=(),
            distinct_sources=0,
            recent_span_hours=0.0,
        )

    best_by_source_signal: dict[tuple[str, str], StrategicTrendEvidence] = {}
    for item in sorted(evidence, key=lambda entry: (entry.score, entry.timestamp), reverse=True):
        key = (item.source.lower(), item.signal)
        if key not in best_by_source_signal:
            best_by_source_signal[key] = item

    escalating = tuple(
        sorted(
            (item for item in best_by_source_signal.values() if item.signal == "escalating"),
            key=lambda entry: (entry.score, entry.timestamp),
            reverse=True,
        )
    )
    deescalating = tuple(
        sorted(
            (item for item in best_by_source_signal.values() if item.signal == "de-escalating"),
            key=lambda entry: (entry.score, entry.timestamp),
            reverse=True,
        )
    )
    mixed = tuple(
        sorted(
            (item for item in best_by_source_signal.values() if item.signal in {"mixed", "steady"}),
            key=lambda entry: (entry.score, entry.timestamp),
            reverse=True,
        )
    )

    escalating_score = sum(item.score for item in escalating)
    deescalating_score = sum(item.score for item in deescalating)
    mixed_score = sum(item.score for item in mixed)
    escalating_sources = {item.source.lower() for item in escalating}
    deescalating_sources = {item.source.lower() for item in deescalating}
    timestamps = [item.timestamp for item in evidence if item.timestamp > 0]
    recent_span_hours = (
        round(max(0.0, (max(timestamps) - min(timestamps)) / 3600.0), 2)
        if len(timestamps) >= 2
        else 0.0
    )

    verdict: Literal["escalating", "de-escalating", "mixed", "insufficient"]
    if len(unique_sources) < 2 and not any(item.is_web for item in evidence):
        verdict = "insufficient"
    elif escalating_score > 0 and deescalating_score > 0:
        stronger_score = max(escalating_score, deescalating_score)
        weaker_score = min(escalating_score, deescalating_score)
        if stronger_score < (weaker_score + 3.0) or weaker_score >= 3.0:
            verdict = "mixed"
        elif escalating_score > deescalating_score and len(escalating_sources) >= 2:
            verdict = "escalating"
        elif deescalating_score > escalating_score and len(deescalating_sources) >= 2:
            verdict = "de-escalating"
        else:
            verdict = "mixed"
    elif escalating_score >= deescalating_score + 2.0 and len(escalating_sources) >= 2:
        verdict = "escalating"
    elif deescalating_score >= escalating_score + 2.0 and len(deescalating_sources) >= 2:
        verdict = "de-escalating"
    elif escalating or deescalating or mixed:
        verdict = "mixed"
    else:
        verdict = "insufficient"

    if verdict == "escalating":
        qualification = "Recent multi-source reporting still shows active exchanges and points to a broader or sharper phase, not a clear pullback."
        bullet_pool = [*escalating[:2], *mixed[:1], *deescalating[:1]]
    elif verdict == "de-escalating":
        qualification = "Recent reporting still shows active exchanges, but the pace looks lower than the earlier peak and there is no clear sign of immediate widening."
        bullet_pool = [*deescalating[:2], *mixed[:1], *escalating[:1]]
    elif verdict == "mixed":
        qualification = "Recent reporting still shows active exchanges, but the signals are mixed and there is no clear decisive shift in direction."
        bullet_pool = [*escalating[:1], *deescalating[:1], *mixed[:1]]
    else:
        qualification = "There is too little recent, reliable evidence here to judge whether the conflict is intensifying or easing."
        bullet_pool = [*mixed[:1], *escalating[:1], *deescalating[:1]]

    bullets: list[str] = []
    seen_keys: set[str] = set()
    for item in bullet_pool:
        key = _digest_line_key(item.text)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        bullets.append(item.text)
        if len(bullets) >= 3:
            break

    return StrategicTrendSummary(
        verdict=verdict,
        qualification=qualification,
        bullets=tuple(bullets),
        escalating_evidence=escalating,
        deescalating_evidence=deescalating,
        mixed_evidence=mixed,
        distinct_sources=len(unique_sources),
        recent_span_hours=recent_span_hours,
    )


def _render_strategic_trend_answer(summary: StrategicTrendSummary) -> str:
    parts = [f"<b>{sanitize_telegram_html(_strategic_query_verdict_title(summary.verdict))}</b>"]
    if summary.qualification:
        parts.append(sanitize_telegram_html(summary.qualification))
    for line in summary.bullets[:3]:
        parts.append(f"• {sanitize_telegram_html(line)}")
    return "<br>".join(parts)


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

    total = (
        keyword_score
        + alias_score
        + number_score
        + direct_phrase_score
        + similarity_score
        + intent_score
        + source_score
        + recency_score
    )
    if query_prefers_direct_answer(question):
        total += _strategic_query_signal_bonus(text)
        total -= _strategic_query_noise_penalty(text)
    return total


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

    if query_prefers_direct_answer(query):
        relevant = [
            row
            for score, row in scored_rows[:12]
            if score >= 1.2 and _strategic_query_signal(str(row.get("text") or "")) != "none"
        ]
        return bool(relevant)

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


def _query_collect_fallback_candidates(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    *,
    detailed: bool,
) -> list[tuple[str, Literal["high", "medium", "low"]]]:
    ranked = _rank_query_context(query, context_messages, limit=(10 if detailed else 6))
    ranked = ranked[: (8 if detailed else 6)]
    if not ranked:
        return []

    broad_query = is_broad_news_query(query)
    lowered_query = normalize_space(query).lower()
    query_terms = {
        normalize_space(term).lower()
        for term in [*expand_query_terms(query), *extract_query_keywords(query), *extract_query_numbers(query)]
        if len(normalize_space(term)) >= 2
    }
    if "aws" in lowered_query:
        query_terms.update({"amazon", "amazon web services"})
    now_ts = int(time.time())
    matched: list[tuple[int, float, str, Literal["high", "medium", "low"]]] = []
    fallback: list[tuple[int, float, str, Literal["high", "medium", "low"]]] = []

    for item in ranked:
        raw_text = normalize_space(str(item.get("text") or ""))
        if not raw_text:
            continue
        for sentence in _split_digest_sentences(raw_text):
            cleaned = _digest_finalize_sentence(sentence, max_chars=220)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            term_hits = sum(1 for term in query_terms if term in lowered)
            sentence_item = dict(item)
            sentence_item["text"] = cleaned
            score = _score_query_context_row(query, sentence_item, now_ts=now_ts) + min(3.0, term_hits * 1.15)
            if not detailed and len(cleaned) > 180:
                score -= 0.35
            target = matched if (term_hits > 0 or broad_query or not query_terms) else fallback
            target.append((term_hits, score, cleaned, _severity_from_text_heuristic(cleaned)))

    max_items = 5 if detailed else 3
    ranked_candidates = sorted(matched or fallback, key=lambda item: (item[0], item[1]), reverse=True)
    if len(ranked) == 1 and len(ranked_candidates) > max_items:
        ranked_candidates = [*ranked_candidates[: max_items - 1], ranked_candidates[-1]]
    results: list[tuple[str, Literal["high", "medium", "low"]]] = []
    seen_keys: set[str] = set()
    for _term_hits, _score, cleaned, severity in ranked_candidates:
        key = _digest_line_key(cleaned)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        results.append((cleaned, severity))
        if len(results) >= max_items:
            break
    return results


def _query_fallback_title(query: str, candidate_lines: Sequence[str]) -> str:
    if query_prefers_direct_answer(query):
        return _strategic_query_verdict_title(
            _build_strategic_trend_summary(
                query,
                [{"text": line, "source": "fallback", "timestamp": 0} for line in candidate_lines],
            ).verdict
        )
    if candidate_lines:
        headline = _fallback_headline(" ".join(candidate_lines[:2])) or _fallback_headline(candidate_lines[0])
        if headline:
            first_key = _digest_line_key(candidate_lines[0])
            headline_key = _digest_line_key(headline)
            if first_key and headline_key and (
                headline_key == first_key
                or SequenceMatcher(None, headline_key, first_key).ratio() >= 0.9
            ):
                headline = ""
        if headline:
            return headline
    if _query_is_identity_question(query):
        return "Best available answer"
    if _query_is_casualty_question(query):
        return "What the evidence shows"
    if query_prefers_direct_answer(query):
        return "Direct answer"
    if is_broad_news_query(query):
        return "Latest update"
    return "Direct answer"


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


def _hard_wrap_query_fallback_segment(text: str, *, limit: int) -> list[str]:
    cleaned = normalize_space(text)
    if not cleaned:
        return []
    if len(cleaned) <= limit:
        return [cleaned]

    chunks: list[str] = []
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
        if not piece:
            piece = normalize_space(window)
        if not piece:
            break
        chunks.append(piece)
        remaining = normalize_space(remaining[cut:].lstrip(" ,;:-/"))
    return chunks


def _split_query_fallback_segments(text: str, *, limit: int = 3200) -> list[str]:
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
        return _hard_wrap_query_fallback_segment(cleaned, limit=limit)

    segments: list[str] = []
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
        segments.extend(_hard_wrap_query_fallback_segment(sentence, limit=limit))
    if current:
        segments.append(current)
    return segments or _hard_wrap_query_fallback_segment(cleaned, limit=limit)


def _fallback_query_answer(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    *,
    detailed: bool,
) -> str:
    if not context_messages:
        return QUERY_NO_MATCH_TEXT

    if query_prefers_direct_answer(query):
        summary = _build_strategic_trend_summary(query, context_messages)
        return _render_strategic_trend_answer(summary)

    candidates = _query_collect_fallback_candidates(query, context_messages, detailed=detailed)
    if not candidates:
        return QUERY_NO_MATCH_TEXT

    candidate_lines = [line for line, _severity in candidates]
    intro_title = _query_fallback_title(query, candidate_lines)
    lead = _build_digest_story_from_fragments(
        candidate_lines[:2],
        total_max_chars=260 if not detailed else 360,
        max_sentences=2,
    )

    parts = [f"<b>{sanitize_telegram_html(intro_title)}</b>"]
    title_key = _digest_line_key(intro_title)
    lead_key = _digest_line_key(lead)
    if lead and lead_key and lead_key != title_key:
        parts.append(sanitize_telegram_html(lead))

    support: list[str] = []
    seen_support: set[str] = set()
    support_candidates = candidates[2:] if lead else candidates
    for line, severity in support_candidates:
        key = _digest_line_key(line)
        if not key or key == lead_key or key in seen_support:
            continue
        seen_support.add(key)
        prefix = "🔥" if severity == "high" else "⚠️"
        support.append(f"• {prefix} {sanitize_telegram_html(line)}")

    if not support and candidate_lines:
        fallback_line = candidate_lines[-1] if len(candidate_lines) > 1 else candidate_lines[0]
        support.append(f"• ⚠️ {sanitize_telegram_html(fallback_line)}")

    parts.extend(support[: (4 if detailed else 2)])
    return "<br>".join(parts)


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
    base_prompt = _filter_decision_system_prompt()
    retry_issue = ""

    for round_number in (1, 2):
        instructions = (
            _decision_retry_prompt(base_prompt, retry_issue)
            if round_number == 2 and retry_issue
            else base_prompt
        )
        try:
            raw = await _call_codex_with_auth_repair(
                compact,
                auth_manager,
                instructions,
                verbosity="low",
            )
        except Exception as exc:
            fallback = _fallback_filter_decision(
                cleaned,
                fallback_reason=_fallback_reason_from_exc(exc),
                ai_attempt_count=round_number,
                ai_quality_retry_used=(round_number == 2),
            )
            log_structured(
                LOGGER,
                "filter_decision_finalized",
                surface="feed",
                action=fallback.action,
                severity=fallback.severity,
                copy_origin=fallback.copy_origin,
                routing_origin=fallback.routing_origin,
                fallback_reason=fallback.fallback_reason,
                ai_attempt_count=fallback.ai_attempt_count,
                ai_quality_retry_used=fallback.ai_quality_retry_used,
                cached=fallback.cached,
            )
            return fallback

        raw = normalize_space(raw)
        if not raw:
            retry_issue = "empty_output"
            if round_number == 2:
                fallback = _fallback_filter_decision(
                    cleaned,
                    fallback_reason="empty_output",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                )
                log_structured(
                    LOGGER,
                    "filter_decision_finalized",
                    surface="feed",
                    action=fallback.action,
                    severity=fallback.severity,
                    copy_origin=fallback.copy_origin,
                    routing_origin=fallback.routing_origin,
                    fallback_reason=fallback.fallback_reason,
                    ai_attempt_count=fallback.ai_attempt_count,
                    ai_quality_retry_used=fallback.ai_quality_retry_used,
                    cached=fallback.cached,
                )
                return fallback
            continue

        payload = _try_parse_digest_json(raw)
        if payload is None:
            retry_issue = "invalid_payload"
            if round_number == 2:
                fallback = _fallback_filter_decision(
                    cleaned,
                    fallback_reason="invalid_payload",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                )
                log_structured(
                    LOGGER,
                    "filter_decision_finalized",
                    surface="feed",
                    action=fallback.action,
                    severity=fallback.severity,
                    copy_origin=fallback.copy_origin,
                    routing_origin=fallback.routing_origin,
                    fallback_reason=fallback.fallback_reason,
                    ai_attempt_count=fallback.ai_attempt_count,
                    ai_quality_retry_used=fallback.ai_quality_retry_used,
                    cached=fallback.cached,
                )
                return fallback
            continue

        decision = _validate_filter_decision(
            payload,
            cleaned,
            copy_origin="ai",
            routing_origin="ai",
            ai_attempt_count=round_number,
            ai_quality_retry_used=(round_number == 2),
        )
        quality_issue = _filter_decision_quality_issue(decision, cleaned)
        if quality_issue:
            retry_issue = quality_issue
            if round_number == 2:
                decision = _fallback_filter_decision(
                    cleaned,
                    fallback_reason="quality_rejected_after_retry",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                )
                log_structured(
                    LOGGER,
                    "filter_decision_finalized",
                    surface="feed",
                    action=decision.action,
                    severity=decision.severity,
                    copy_origin=decision.copy_origin,
                    routing_origin=decision.routing_origin,
                    fallback_reason=decision.fallback_reason,
                    ai_attempt_count=decision.ai_attempt_count,
                    ai_quality_retry_used=decision.ai_quality_retry_used,
                    cached=decision.cached,
                )
                return decision
            continue

        ai_decision_cache_set(
            normalized_hash=normalized_hash,
            prompt_version=FILTER_DECISION_PROMPT_VERSION,
            model=model,
            decision_json=json.dumps(decision.__dict__, separators=(",", ":")),
        )
        log_structured(
            LOGGER,
            "filter_decision_finalized",
            surface="feed",
            action=decision.action,
            severity=decision.severity,
            copy_origin=decision.copy_origin,
            routing_origin=decision.routing_origin,
            fallback_reason=decision.fallback_reason,
            ai_attempt_count=decision.ai_attempt_count,
            ai_quality_retry_used=decision.ai_quality_retry_used,
            cached=decision.cached,
        )
        return decision

    fallback = _fallback_filter_decision(
        cleaned,
        fallback_reason="quality_rejected_after_retry",
        ai_attempt_count=2,
        ai_quality_retry_used=True,
    )
    log_structured(
        LOGGER,
        "filter_decision_finalized",
        surface="feed",
        action=fallback.action,
        severity=fallback.severity,
        copy_origin=fallback.copy_origin,
        routing_origin=fallback.routing_origin,
        fallback_reason=fallback.fallback_reason,
        ai_attempt_count=fallback.ai_attempt_count,
        ai_quality_retry_used=fallback.ai_quality_retry_used,
        cached=fallback.cached,
    )
    return fallback


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
        return fallback

    if decision.action == "skip" or not decision.summary_html:
        _cache_set(key, None)
        return None

    if decision.copy_origin == "ai":
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


def _strip_why_it_matters_prefix(text: str) -> str:
    cleaned = normalize_space(strip_telegram_html(str(text or "")))
    cleaned = re.sub(r"^\s*why it matters:\s*", "", cleaned, flags=re.IGNORECASE)
    return normalize_space(cleaned)


def _context_contains_marker(text: str, markers: Sequence[str]) -> bool:
    normalized = normalize_taxonomy_text(text)
    for marker in markers:
        clean = normalize_taxonomy_text(marker)
        if not clean:
            continue
        if re.search(rf"(?<![a-z0-9]){re.escape(clean)}(?![a-z0-9])", normalized):
            return True
        if clean.isdigit():
            for word, digit in _NUMBER_WORD_MAP.items():
                if digit != clean:
                    continue
                if re.search(rf"(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])", normalized):
                    return True
    return False


def _context_evidence_prompt_input(current_text: str, evidence: ContextEvidence) -> str:
    current = normalize_space(strip_telegram_html(current_text))
    age_label = normalize_space(evidence.anchor_age_label)
    lines = [
        f"Current update: {current}",
        f"Prior anchor{' (' + age_label + ')' if age_label else ''}: {normalize_space(strip_telegram_html(evidence.anchor_text))}",
        f"Anchor detail: {normalize_space(evidence.anchor_detail)}",
        f"New delta: {normalize_space(evidence.delta_detail)}",
        f"Delta kind: {normalize_space(evidence.delta_kind).replace('_', ' ')}",
    ]
    if evidence.anchor_markers:
        lines.append(f"Anchor markers: {', '.join(evidence.anchor_markers[:6])}")
    if evidence.delta_markers:
        lines.append(f"Delta markers: {', '.join(evidence.delta_markers[:6])}")
    return "\n".join(lines)


def _vital_view_matches_evidence(
    text: str,
    current_text: str,
    evidence: ContextEvidence | None,
) -> bool:
    if evidence is None:
        return False
    lowered = normalize_space(strip_telegram_html(text)).lower()
    if not lowered:
        return False
    if _vital_view_is_too_generic(lowered):
        return False
    if any(fragment in lowered for fragment in _VITAL_UNSUPPORTED_IMPLICATION_FRAGMENTS):
        return False

    current_clean = normalize_space(strip_telegram_html(current_text)).lower()
    if current_clean and SequenceMatcher(None, lowered, current_clean).ratio() >= 0.9:
        return False

    detail = _strip_why_it_matters_prefix(text)
    if not detail:
        return False
    if not _context_contains_marker(detail, evidence.anchor_markers):
        return False
    if not _context_contains_marker(detail, evidence.delta_markers):
        return False
    return True


def resolve_vital_rational_view_for_delivery(
    current_text: str,
    candidate: str | None,
    recent_context: Sequence[str] | None,
    *,
    evidence: ContextEvidence | None = None,
) -> str:
    del recent_context
    if evidence is None:
        return ""
    cleaned_candidate = _cleanup_vital_view(str(candidate or ""))
    if cleaned_candidate and _vital_view_matches_evidence(cleaned_candidate, current_text, evidence):
        return cleaned_candidate
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
    base_prompt = _breaking_headline_prompt()
    retry_issue = ""

    for round_number in (1, 2):
        prompt = (
            _query_retry_prompt(base_prompt, retry_issue)
            if round_number == 2 and retry_issue
            else base_prompt
        )
        try:
            raw = await _call_codex_with_auth_repair(
                compact,
                auth_manager,
                prompt,
                verbosity="low",
            )
        except Exception:
            return resolve_breaking_headline_for_delivery(cleaned, None)

        headline = resolve_breaking_headline_for_delivery(cleaned, raw, allow_fallback=False)
        issue = _news_copy_quality_issue(headline, cleaned) if headline else "empty_output"
        if not issue and headline:
            _headline_cache_set(key, headline)
            return headline
        retry_issue = issue or "vague_copy"
        if round_number == 2:
            return resolve_breaking_headline_for_delivery(cleaned, None)

    return resolve_breaking_headline_for_delivery(cleaned, None)


async def summarize_vital_rational_view(
    text: str,
    auth_manager: AuthManager,
    *,
    recent_context: Sequence[str] | None = None,
    evidence: ContextEvidence | None = None,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned:
        return None

    del recent_context
    if evidence is None:
        return None

    context_key = "||".join(
        (
            normalize_space(evidence.delta_kind),
            normalize_space(evidence.anchor_age_label),
            normalize_space(evidence.anchor_detail),
            normalize_space(evidence.delta_detail),
            " ".join(evidence.anchor_markers[:6]),
            " ".join(evidence.delta_markers[:6]),
        )
    )
    key = _cache_key(
        f"vital_view::{VITAL_VIEW_PROMPT_VERSION}::{resolve_breaking_style_mode()}::{cleaned}::{context_key}"
    )
    cached = _vital_view_cache_get(key)
    if cached is not None or key in _VITAL_VIEW_CACHE:
        return cached

    compact = _context_evidence_prompt_input(cleaned, evidence)

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
            _vital_view_cache_set(key, None)
            return None
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        _vital_view_cache_set(key, None)
        return None
    except Exception:
        _vital_view_cache_set(key, None)
        return None

    view = resolve_vital_rational_view_for_delivery(cleaned, raw, None, evidence=evidence)
    if view:
        _vital_view_cache_set(key, view)
        return view

    _vital_view_cache_set(key, None)
    return None


def _make_generated_text_result(
    html: str,
    *,
    copy_origin: Literal["ai", "fallback"],
    fallback_reason: str = "",
    ai_attempt_count: int = 1,
    ai_quality_retry_used: bool = False,
    major_block_count: int = 0,
    timeline_item_count: int = 0,
    noise_stripped_count: int = 0,
    translation_applied_count: int = 0,
    citation_stripped_count: int = 0,
    duplicate_collapsed_count: int = 0,
    narrative_headline: str = "",
    narrative_story: str = "",
    narrative_highlights: Sequence[str] = (),
    narrative_also_moving: Sequence[str] = (),
) -> GeneratedTextResult:
    resolved_headline = normalize_space(narrative_headline)
    resolved_story = normalize_space(narrative_story)
    resolved_highlights = [
        normalize_space(str(item)) for item in narrative_highlights if normalize_space(str(item))
    ]
    resolved_also_moving = [
        normalize_space(str(item)) for item in narrative_also_moving if normalize_space(str(item))
    ]
    if html and not resolved_headline and not resolved_story and not resolved_highlights and not resolved_also_moving:
        parsed_headline, parsed_story, parsed_highlights, parsed_also_moving = extract_digest_narrative_parts(
            html,
            max_lines=max(_resolve_digest_max_lines(), 6),
        )
        resolved_headline = parsed_headline
        resolved_story = parsed_story
        resolved_highlights = parsed_highlights
        resolved_also_moving = parsed_also_moving
    return GeneratedTextResult(
        html=html,
        copy_origin=copy_origin,
        fallback_reason=_normalize_filter_fallback_reason(fallback_reason),
        ai_attempt_count=max(1, ai_attempt_count),
        ai_quality_retry_used=ai_quality_retry_used,
        major_block_count=max(0, int(major_block_count)),
        timeline_item_count=max(0, int(timeline_item_count)),
        noise_stripped_count=max(0, int(noise_stripped_count)),
        translation_applied_count=max(0, int(translation_applied_count)),
        citation_stripped_count=max(0, int(citation_stripped_count)),
        duplicate_collapsed_count=max(0, int(duplicate_collapsed_count)),
        narrative_headline=resolved_headline,
        narrative_story=resolved_story,
        narrative_highlights=tuple(resolved_highlights),
        narrative_also_moving=tuple(resolved_also_moving),
    )


def _digest_render_counts(text: str, *, interval_minutes: int) -> tuple[int, int]:
    headline, story, highlights, also_moving = extract_digest_narrative_parts(
        text,
        max_lines=max(_resolve_digest_max_lines(), 6),
    )
    has_primary_block = bool(headline and (story or highlights))
    return (1 if has_primary_block else 0, len(also_moving))


def _digest_quality_issue(text: str, quiet_text: str, *, interval_minutes: int) -> str:
    cleaned = normalize_space(strip_telegram_html(text))
    if not cleaned:
        return "empty_output"
    if cleaned == normalize_space(strip_telegram_html(quiet_text)):
        return ""
    if _digest_needs_english_rewrite(text, "English"):
        return "non_english_leftovers"
    lowered = cleaned.lower()
    if _digest_has_citation_language(cleaned):
        return "citation_leak"
    if _DIGEST_HASHTAG_RE.search(cleaned) or _DIGEST_PROMO_FRAGMENT_RE.search(cleaned):
        return "source_leak"
    headline, story, highlights, also_moving = extract_digest_narrative_parts(
        text,
        max_lines=max(_resolve_digest_max_lines(), 6),
    )
    headline_mode = _digest_is_headline_rail(interval_minutes)
    if not headline:
        return "headline_too_thin"
    if not story and not headline_mode:
        return "missing_story"
    if headline_mode and not highlights and not also_moving:
        return "headline_too_thin"

    if story:
        story_issue = _news_copy_quality_issue(story, story, allow_short=True)
        if story_issue in {"vague_copy", "incomplete_copy"}:
            return story_issue

    if story:
        if _digest_line_key(story) == _digest_line_key(headline):
            return "duplication"
        story_sentences = _collect_digest_story_sentences([story], max_sentences=1)
        if story_sentences and SequenceMatcher(None, story_sentences[0].lower(), headline.lower()).ratio() >= 0.86:
            return "duplication"
    if len(also_moving) > _digest_also_moving_cap():
        return "oversized_also_moving"

    seen_digest_lines: set[str] = set()
    digest_lines = [headline, *([story] if story else []), *highlights, *also_moving]
    for line in digest_lines:
        key = _digest_line_key(line)
        if not key:
            return "messy_layout"
        if key in seen_digest_lines:
            return "duplication"
        seen_digest_lines.add(key)
        if headline_mode:
            rail_issue = _headline_rail_line_issue(line)
            if rail_issue:
                return rail_issue
        issue = _news_copy_quality_issue(line, line, allow_short=True)
        if issue in {"vague_copy", "incomplete_copy"}:
            return issue

    if headline_mode:
        if len(highlights) < 1 and len(also_moving) < 1:
            return "headline_too_thin"
    elif not highlights and not also_moving and len(story.split()) < 8:
        return "headline_too_thin"
    return ""


def _query_quality_issue(answer_html: str, question: str) -> str:
    cleaned = normalize_space(strip_telegram_html(answer_html))
    if not cleaned:
        return "empty_output"
    lowered = cleaned.lower()
    if QUERY_NO_MATCH_TEXT.lower() in lowered:
        return ""
    if "based on the provided evidence" in lowered or "provided evidence" in lowered:
        return "vague_copy"
    if any(pattern in lowered for pattern in _WEAK_COPY_PATTERNS):
        return "vague_copy"
    if any(marker in lowered for marker in ("fwd from", "original msg", "support us", "subscribe")):
        return "citation_leak"
    if re.search(r"@\w{3,}", cleaned):
        return "citation_leak"
    if cleaned.endswith(("...", "…", ":", "-", "–", "—", "/")):
        return "incomplete_copy"
    if query_prefers_direct_answer(question):
        strategic_markers = (
            "still active and escalating",
            "still active, but easing from the earlier peak",
            "still active with mixed signals; no clear decisive shift",
            "not enough recent evidence to judge direction",
        )
        if not any(marker in lowered for marker in strategic_markers):
            return "strategic_verdict_missing"
        if any(marker in lowered for marker in _STRATEGIC_TREND_RHETORIC_MARKERS):
            return "offtopic_copy"
        if _query_is_quote_heavy(cleaned):
            return "offtopic_copy"
    bullet_count = cleaned.count("•")
    if not _query_requests_detail(question):
        if bullet_count > 4 or len(cleaned) > 800:
            return "messy_layout"
    elif bullet_count > 6 or len(cleaned) > 1400:
        return "messy_layout"
    if len(cleaned) < max(28, min(64, len(normalize_space(question)))):
        return "headline_too_thin"
    return ""


def _digest_retry_prompt(prompt: str, issue: str) -> str:
    return (
        f"{prompt}\n\nRevision feedback:\n- {_filter_retry_feedback(issue)}\n"
        "- Rebuild the digest in the correct window style: short rolling windows need a headline rail, while long windows need a story digest.\n"
        "- Do not leave any meaningful update behind.\n"
        "- Keep it English-only and remove all source branding, handles, hashtags, promo junk, and citation-style phrasing.\n"
        "- If uncertainty is needed, use generic wording only, such as 'initial reports indicate' or 'preliminary reports suggest'."
    )


def _query_retry_prompt(prompt: str, issue: str) -> str:
    return (
        f"{prompt}\n\nRevision feedback:\n- {_filter_retry_feedback(issue)}\n"
        "- Answer the exact question directly in the first line with concrete facts.\n"
        "- Keep it short, punchy, and direct: one sharp answer line, one short sentence, and no more than 3 short bullets unless the user asked for detail.\n"
        "- Do not paste raw source text, forwarded-message phrasing, promo fragments, or long evidence dumps."
    )


async def create_digest_summary_result(
    posts: List[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    interval_minutes: int | None = None,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> GeneratedTextResult:
    """
    Build one editorial digest from queued posts and keep provenance metadata.
    """
    if interval_minutes is None:
        interval_minutes = int(max(1, int(getattr(config, "DIGEST_INTERVAL_MINUTES", 30))))
    else:
        interval_minutes = int(max(1, interval_minutes))
    quiet_text = quiet_period_message(interval_minutes)
    if not posts:
        return _make_generated_text_result(quiet_text, copy_origin="fallback")

    manager = auth_manager or AuthManager(logger=LOGGER)
    max_lines = _resolve_digest_max_lines()
    prepared_posts, prep_stats = await _prepare_digest_posts(posts, manager)
    noise_stripped_count = int(prep_stats.get("noise_stripped_count") or 0)
    translation_applied_count = int(prep_stats.get("translation_applied_count") or 0)
    citation_stripped_count = int(prep_stats.get("citation_stripped_count") or 0)
    duplicate_collapsed_count = int(prep_stats.get("duplicate_collapsed_count") or 0)
    if not prepared_posts:
        return _make_generated_text_result(
            quiet_text,
            copy_origin="fallback",
            noise_stripped_count=noise_stripped_count,
            translation_applied_count=translation_applied_count,
            citation_stripped_count=citation_stripped_count,
            duplicate_collapsed_count=duplicate_collapsed_count,
        )

    token_budget = _resolve_digest_token_budget()
    context_budget = max(1000, token_budget - 2200)

    lines, included_count, total_input = _build_digest_context(
        prepared_posts,
        token_budget=context_budget,
    )
    if not lines or included_count == 0:
        return _make_generated_text_result(
            quiet_text,
            copy_origin="fallback",
            noise_stripped_count=noise_stripped_count,
            translation_applied_count=translation_applied_count,
            citation_stripped_count=citation_stripped_count,
            duplicate_collapsed_count=duplicate_collapsed_count,
        )

    prefer_json = bool(getattr(config, "DIGEST_PREFER_JSON_OUTPUT", True))
    if on_token is not None:
        prefer_json = False
    importance_scoring = bool(getattr(config, "DIGEST_IMPORTANCE_SCORING", True))
    include_links = False
    output_language = "English"

    user_payload = (
        "Batch metadata:\n"
        f"- Total queued posts: {len(posts)}\n"
        f"- Posts after cleanup: {len(prepared_posts)}\n"
        f"- Included after dedupe/noise/token budget: {included_count}\n"
        f"- Noise fragments stripped: {noise_stripped_count}\n"
        f"- Source/citation fragments stripped: {citation_stripped_count}\n"
        f"- Duplicate posts collapsed before writing: {duplicate_collapsed_count}\n"
        f"- Pre-translation lines rewritten to English: {translation_applied_count}\n"
        f"- Estimated input tokens: {sum(estimate_tokens_rough(x) for x in lines)}\n\n"
        + build_digest_input_block(lines)
    )
    base_prompt = build_digest_system_prompt(
        interval_minutes=interval_minutes,
        json_mode=prefer_json,
        importance_scoring=importance_scoring,
        include_links=include_links,
        output_language=output_language,
        include_source_tags=False,
    )
    retry_issue = ""

    for round_number in (1, 2):
        prompt = (
            _digest_retry_prompt(base_prompt, retry_issue)
            if round_number == 2 and retry_issue
            else base_prompt
        )
        try:
            content = await _call_codex_with_auth_repair(
                user_payload,
                manager,
                prompt,
                on_token=on_token,
            )
        except Exception as exc:
            fallback_html = local_fallback_digest(prepared_posts, interval_minutes=interval_minutes)
            major_block_count, timeline_item_count = _digest_render_counts(
                fallback_html,
                interval_minutes=interval_minutes,
            )
            result = _make_generated_text_result(
                fallback_html,
                copy_origin="fallback",
                fallback_reason=_fallback_reason_from_exc(exc),
                ai_attempt_count=round_number,
                ai_quality_retry_used=(round_number == 2),
                major_block_count=major_block_count,
                timeline_item_count=timeline_item_count,
                noise_stripped_count=noise_stripped_count,
                translation_applied_count=translation_applied_count,
                citation_stripped_count=citation_stripped_count,
                duplicate_collapsed_count=duplicate_collapsed_count,
            )
            log_structured(
                LOGGER,
                "digest_generation_result",
                copy_origin=result.copy_origin,
                fallback_reason=result.fallback_reason,
                ai_attempt_count=result.ai_attempt_count,
                ai_quality_retry_used=result.ai_quality_retry_used,
                output_chars=len(result.html),
                major_block_count=result.major_block_count,
                timeline_item_count=result.timeline_item_count,
                noise_stripped_count=result.noise_stripped_count,
                translation_applied_count=result.translation_applied_count,
                citation_stripped_count=result.citation_stripped_count,
                duplicate_collapsed_count=result.duplicate_collapsed_count,
                quality_issue="",
            )
            return result

        if not content.strip():
            retry_issue = "empty_output"
            if round_number == 2:
                fallback_html = local_fallback_digest(prepared_posts, interval_minutes=interval_minutes)
                major_block_count, timeline_item_count = _digest_render_counts(
                    fallback_html,
                    interval_minutes=interval_minutes,
                )
                result = _make_generated_text_result(
                    fallback_html,
                    copy_origin="fallback",
                    fallback_reason="empty_output",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                    major_block_count=major_block_count,
                    timeline_item_count=timeline_item_count,
                    noise_stripped_count=noise_stripped_count,
                    translation_applied_count=translation_applied_count,
                    citation_stripped_count=citation_stripped_count,
                    duplicate_collapsed_count=duplicate_collapsed_count,
                )
                log_structured(
                    LOGGER,
                    "digest_generation_result",
                    copy_origin=result.copy_origin,
                    fallback_reason=result.fallback_reason,
                    ai_attempt_count=result.ai_attempt_count,
                    ai_quality_retry_used=result.ai_quality_retry_used,
                    output_chars=len(result.html),
                    major_block_count=result.major_block_count,
                    timeline_item_count=result.timeline_item_count,
                    noise_stripped_count=result.noise_stripped_count,
                    translation_applied_count=result.translation_applied_count,
                    citation_stripped_count=result.citation_stripped_count,
                    duplicate_collapsed_count=result.duplicate_collapsed_count,
                    quality_issue=retry_issue,
                )
                return result
            continue

        if prefer_json:
            parsed = _try_parse_digest_json(content)
            if parsed is None:
                retry_issue = "invalid_payload"
                if round_number == 2:
                    fallback_html = local_fallback_digest(prepared_posts, interval_minutes=interval_minutes)
                    major_block_count, timeline_item_count = _digest_render_counts(
                        fallback_html,
                        interval_minutes=interval_minutes,
                    )
                    result = _make_generated_text_result(
                        fallback_html,
                        copy_origin="fallback",
                        fallback_reason="invalid_payload",
                        ai_attempt_count=2,
                        ai_quality_retry_used=True,
                        major_block_count=major_block_count,
                        timeline_item_count=timeline_item_count,
                        noise_stripped_count=noise_stripped_count,
                        translation_applied_count=translation_applied_count,
                        citation_stripped_count=citation_stripped_count,
                        duplicate_collapsed_count=duplicate_collapsed_count,
                    )
                    log_structured(
                        LOGGER,
                        "digest_generation_result",
                        copy_origin=result.copy_origin,
                        fallback_reason=result.fallback_reason,
                        ai_attempt_count=result.ai_attempt_count,
                        ai_quality_retry_used=result.ai_quality_retry_used,
                        output_chars=len(result.html),
                        major_block_count=result.major_block_count,
                        timeline_item_count=result.timeline_item_count,
                        noise_stripped_count=result.noise_stripped_count,
                        translation_applied_count=result.translation_applied_count,
                        citation_stripped_count=result.citation_stripped_count,
                        duplicate_collapsed_count=result.duplicate_collapsed_count,
                        quality_issue=retry_issue,
                    )
                    return result
                continue
            candidate = _json_digest_to_html(
                parsed,
                interval_minutes=interval_minutes,
                max_lines=max_lines,
            )
        else:
            candidate = content

        normalized = await _normalize_digest_output(
            candidate,
            manager,
            interval_minutes=interval_minutes,
            max_lines=max_lines,
        )
        quality_issue = _digest_quality_issue(
            normalized,
            quiet_text,
            interval_minutes=interval_minutes,
        )
        if quality_issue:
            retry_issue = quality_issue
            if round_number == 2:
                fallback_html = local_fallback_digest(prepared_posts, interval_minutes=interval_minutes)
                major_block_count, timeline_item_count = _digest_render_counts(
                    fallback_html,
                    interval_minutes=interval_minutes,
                )
                result = _make_generated_text_result(
                    fallback_html,
                    copy_origin="fallback",
                    fallback_reason="quality_rejected_after_retry",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                    major_block_count=major_block_count,
                    timeline_item_count=timeline_item_count,
                    noise_stripped_count=noise_stripped_count,
                    translation_applied_count=translation_applied_count,
                    citation_stripped_count=citation_stripped_count,
                    duplicate_collapsed_count=duplicate_collapsed_count,
                )
                log_structured(
                    LOGGER,
                    "digest_generation_result",
                    copy_origin=result.copy_origin,
                    fallback_reason=result.fallback_reason,
                    ai_attempt_count=result.ai_attempt_count,
                    ai_quality_retry_used=result.ai_quality_retry_used,
                    output_chars=len(result.html),
                    major_block_count=result.major_block_count,
                    timeline_item_count=result.timeline_item_count,
                    noise_stripped_count=result.noise_stripped_count,
                    translation_applied_count=result.translation_applied_count,
                    citation_stripped_count=result.citation_stripped_count,
                    duplicate_collapsed_count=result.duplicate_collapsed_count,
                    quality_issue=quality_issue,
                )
                return result
            continue

        major_block_count, timeline_item_count = _digest_render_counts(
            normalized,
            interval_minutes=interval_minutes,
        )
        result = _make_generated_text_result(
            normalized,
            copy_origin="ai",
            ai_attempt_count=round_number,
            ai_quality_retry_used=(round_number == 2),
            major_block_count=major_block_count,
            timeline_item_count=timeline_item_count,
            noise_stripped_count=noise_stripped_count,
            translation_applied_count=translation_applied_count,
            citation_stripped_count=citation_stripped_count,
            duplicate_collapsed_count=duplicate_collapsed_count,
        )
        log_structured(
            LOGGER,
            "digest_generation_result",
            copy_origin=result.copy_origin,
            fallback_reason=result.fallback_reason,
            ai_attempt_count=result.ai_attempt_count,
            ai_quality_retry_used=result.ai_quality_retry_used,
            output_chars=len(result.html),
            major_block_count=result.major_block_count,
            timeline_item_count=result.timeline_item_count,
            noise_stripped_count=result.noise_stripped_count,
            translation_applied_count=result.translation_applied_count,
            citation_stripped_count=result.citation_stripped_count,
            duplicate_collapsed_count=result.duplicate_collapsed_count,
            quality_issue="",
        )
        return result

    fallback_html = local_fallback_digest(prepared_posts, interval_minutes=interval_minutes)
    major_block_count, timeline_item_count = _digest_render_counts(
        fallback_html,
        interval_minutes=interval_minutes,
    )
    result = _make_generated_text_result(
        fallback_html,
        copy_origin="fallback",
        fallback_reason="quality_rejected_after_retry",
        ai_attempt_count=2,
        ai_quality_retry_used=True,
        major_block_count=major_block_count,
        timeline_item_count=timeline_item_count,
        noise_stripped_count=noise_stripped_count,
        translation_applied_count=translation_applied_count,
        citation_stripped_count=citation_stripped_count,
        duplicate_collapsed_count=duplicate_collapsed_count,
    )
    log_structured(
        LOGGER,
        "digest_generation_result",
        copy_origin=result.copy_origin,
        fallback_reason=result.fallback_reason,
        ai_attempt_count=result.ai_attempt_count,
        ai_quality_retry_used=result.ai_quality_retry_used,
        output_chars=len(result.html),
        major_block_count=result.major_block_count,
        timeline_item_count=result.timeline_item_count,
        noise_stripped_count=result.noise_stripped_count,
        translation_applied_count=result.translation_applied_count,
        citation_stripped_count=result.citation_stripped_count,
        duplicate_collapsed_count=result.duplicate_collapsed_count,
        quality_issue=retry_issue or "quality_rejected_after_retry",
    )
    return result


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
    result = await create_digest_summary_result(
        posts,
        auth_manager=auth_manager,
        interval_minutes=interval_minutes,
        on_token=on_token,
    )
    return result.html


async def generate_answer_from_context_result(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    conversation_history: Sequence[Dict[str, str]] | None = None,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> GeneratedTextResult:
    """
    Generate a conversational query answer from recent Telegram context messages.
    """
    question = normalize_space(query)
    if not question:
        return _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")

    if not context_messages:
        return _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")

    manager = auth_manager or AuthManager(logger=LOGGER)
    detailed = _query_requests_detail(question)
    high_risk_query = _query_is_high_risk(question)
    strategic_trend_query = query_prefers_direct_answer(question)
    strict_confirmation_query = _query_requires_multi_source_confirmation(question)
    scored_context = _scored_query_context(question, context_messages)
    ranked_context = _rank_query_context(question, context_messages, limit=24)
    distinct_sources = _query_distinct_sources(ranked_context[:10] or context_messages)
    output_language = _resolve_output_language()

    if strategic_trend_query:
        summary = _build_strategic_trend_summary(question, ranked_context or context_messages)
        result = _make_generated_text_result(
            _render_strategic_trend_answer(summary),
            copy_origin="fallback",
        )
        log_structured(
            LOGGER,
            "query_generation_result",
            copy_origin=result.copy_origin,
            fallback_reason=result.fallback_reason,
            ai_attempt_count=result.ai_attempt_count,
            ai_quality_retry_used=result.ai_quality_retry_used,
            output_chars=len(result.html),
        )
        return result

    # Strict guardrail for high-risk queries (leadership/death/succession):
    # if evidence diversity is weak, do not produce potentially false claims.
    if strict_confirmation_query and distinct_sources < 2:
        return _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")
    if not _query_confidence_allows_answer(question, scored_context[:12]):
        return _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")

    # Reserve room for prompt + answer generation.
    context_budget = max(2000, _query_context_token_budget() - 2600)
    context_lines, used_tokens = _build_query_context_lines(
        question,
        ranked_context,
        token_budget=context_budget,
    )
    if not context_lines:
        return _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")

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

    base_prompt = build_query_system_prompt(
        output_language=output_language,
        detailed=detailed,
        strategic_trend=strategic_trend_query,
    )
    retry_issue = ""

    for round_number in (1, 2):
        prompt = (
            _query_retry_prompt(base_prompt, retry_issue)
            if round_number == 2 and retry_issue
            else base_prompt
        )
        try:
            content = await _call_codex_with_auth_repair(
                user_payload,
                manager,
                prompt,
                verbosity="low" if not detailed else "medium",
                on_token=on_token,
            )
        except Exception as exc:
            result = _make_generated_text_result(
                _fallback_query_answer(question, ranked_context, detailed=detailed),
                copy_origin="fallback",
                fallback_reason=_fallback_reason_from_exc(exc),
                ai_attempt_count=round_number,
                ai_quality_retry_used=(round_number == 2),
            )
            log_structured(
                LOGGER,
                "query_generation_result",
                copy_origin=result.copy_origin,
                fallback_reason=result.fallback_reason,
                ai_attempt_count=result.ai_attempt_count,
                ai_quality_retry_used=result.ai_quality_retry_used,
                output_chars=len(result.html),
            )
            return result

        cleaned = normalize_space(content)
        if not cleaned:
            retry_issue = "empty_output"
            if round_number == 2:
                result = _make_generated_text_result(
                    _fallback_query_answer(question, ranked_context, detailed=detailed),
                    copy_origin="fallback",
                    fallback_reason="empty_output",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                )
                log_structured(
                    LOGGER,
                    "query_generation_result",
                    copy_origin=result.copy_origin,
                    fallback_reason=result.fallback_reason,
                    ai_attempt_count=result.ai_attempt_count,
                    ai_quality_retry_used=result.ai_quality_retry_used,
                    output_chars=len(result.html),
                )
                return result
            continue

        plain_cleaned = re.sub(r"</?[^>]+>", "", cleaned).lower()
        if QUERY_NO_MATCH_TEXT.lower() in cleaned.lower() or "no relevant information found" in plain_cleaned:
            result = _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="ai")
            log_structured(
                LOGGER,
                "query_generation_result",
                copy_origin=result.copy_origin,
                fallback_reason=result.fallback_reason,
                ai_attempt_count=result.ai_attempt_count,
                ai_quality_retry_used=result.ai_quality_retry_used,
                output_chars=len(result.html),
            )
            return result

        if strict_confirmation_query and distinct_sources < 2 and _answer_has_definitive_high_risk_claim(cleaned):
            result = _make_generated_text_result(QUERY_NO_MATCH_TEXT, copy_origin="fallback")
            log_structured(
                LOGGER,
                "query_generation_result",
                copy_origin=result.copy_origin,
                fallback_reason=result.fallback_reason,
                ai_attempt_count=result.ai_attempt_count,
                ai_quality_retry_used=result.ai_quality_retry_used,
                output_chars=len(result.html),
            )
            return result

        safe = sanitize_telegram_html(content.strip())
        quality_issue = _query_quality_issue(safe, question)
        if not safe:
            quality_issue = "empty_output"
        if quality_issue:
            retry_issue = quality_issue
            if round_number == 2:
                result = _make_generated_text_result(
                    _fallback_query_answer(question, ranked_context, detailed=detailed),
                    copy_origin="fallback",
                    fallback_reason="quality_rejected_after_retry",
                    ai_attempt_count=2,
                    ai_quality_retry_used=True,
                )
                log_structured(
                    LOGGER,
                    "query_generation_result",
                    copy_origin=result.copy_origin,
                    fallback_reason=result.fallback_reason,
                    ai_attempt_count=result.ai_attempt_count,
                    ai_quality_retry_used=result.ai_quality_retry_used,
                    output_chars=len(result.html),
                )
                return result
            continue

        result = _make_generated_text_result(
            safe,
            copy_origin="ai",
            ai_attempt_count=round_number,
            ai_quality_retry_used=(round_number == 2),
        )
        log_structured(
            LOGGER,
            "query_generation_result",
            copy_origin=result.copy_origin,
            fallback_reason=result.fallback_reason,
            ai_attempt_count=result.ai_attempt_count,
            ai_quality_retry_used=result.ai_quality_retry_used,
            output_chars=len(result.html),
        )
        return result

    result = _make_generated_text_result(
        _fallback_query_answer(question, ranked_context, detailed=detailed),
        copy_origin="fallback",
        fallback_reason="quality_rejected_after_retry",
        ai_attempt_count=2,
        ai_quality_retry_used=True,
    )
    log_structured(
        LOGGER,
        "query_generation_result",
        copy_origin=result.copy_origin,
        fallback_reason=result.fallback_reason,
        ai_attempt_count=result.ai_attempt_count,
        ai_quality_retry_used=result.ai_quality_retry_used,
        output_chars=len(result.html),
    )
    return result


async def generate_answer_from_context(
    query: str,
    context_messages: Sequence[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    conversation_history: Sequence[Dict[str, str]] | None = None,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    result = await generate_answer_from_context_result(
        query,
        context_messages,
        auth_manager=auth_manager,
        conversation_history=conversation_history,
        on_token=on_token,
    )
    return result.html
