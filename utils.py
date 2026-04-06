"""Shared utility helpers for digest routing, token budgeting, logging, and query search."""

from __future__ import annotations

import asyncio
from collections import Counter, deque
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from email.utils import parsedate_to_datetime
from html import escape as _escape_html, unescape as _unescape_html
import hashlib
import json
import logging
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Awaitable, Callable, Iterable, List, Sequence, Tuple
from urllib.parse import quote_plus, urlparse
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import config
from db import (
    load_recent_breaking,
    load_recent_media_signatures,
    purge_recent_breaking,
    purge_recent_media_signatures,
    save_recent_breaking,
    save_recent_media_signature,
)
from news_taxonomy import match_news_category, record_ontology_label_resolution
from news_signals import looks_like_live_event_update, should_downgrade_explainer_urgency
from shared_http import get_web_http_client


_QUERY_MAX_HOURS_BACK = 24 * 30


class _NamedFixedOffsetZone(tzinfo):
    def __init__(self, *, key: str, offset: timedelta, name: str) -> None:
        self.key = key
        self._offset = offset
        self._name = name

    def utcoffset(self, dt: datetime | None) -> timedelta:
        return self._offset

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str:
        return self._name


_RUNTIME_TZ_ALIASES = {
    "IST": "Asia/Kolkata",
}

_RUNTIME_TZ_FIXED_FALLBACKS = {
    "Asia/Kolkata": _NamedFixedOffsetZone(
        key="Asia/Kolkata",
        offset=timedelta(hours=5, minutes=30),
        name="IST",
    ),
}


def runtime_timezone():
    raw = str(getattr(config, "TIMEZONE", "UTC") or "").strip()
    if not raw:
        raw = "UTC"
    zone_name = _RUNTIME_TZ_ALIASES.get(raw.upper(), raw)
    try:
        return ZoneInfo(zone_name)
    except ZoneInfoNotFoundError:
        return _RUNTIME_TZ_FIXED_FALLBACKS.get(zone_name, timezone.utc)


def runtime_now() -> datetime:
    return datetime.now(runtime_timezone())


def estimate_tokens_rough(text: str) -> int:
    """Fast rough token estimate (works well enough for budgeting)."""
    if not text:
        return 0
    # Roughly ~4 chars/token average for mixed English + symbols.
    return max(1, len(text) // 4)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def media_duplicate_match_score(left: str, right: str) -> float:
    left_norm, _ = build_dupe_fingerprint(left)
    right_norm, _ = build_dupe_fingerprint(right)
    if not left_norm or not right_norm:
        return 1.0 if left_norm == right_norm else 0.0

    left_tokens = tuple(_tokenize_for_dupe(left_norm))
    right_tokens = tuple(_tokenize_for_dupe(right_norm))
    tfidf = _tfidf_cosine(left_tokens, right_tokens)
    fuzzy = 0.0
    try:
        from difflib import SequenceMatcher

        fuzzy = SequenceMatcher(None, left_norm, right_norm).ratio()
    except Exception:
        fuzzy = 0.0
    overlap = _set_jaccard(left_tokens, right_tokens)
    left_anchors = _anchor_tokens(left_tokens)
    right_anchors = _anchor_tokens(right_tokens)
    anchor_overlap = _set_dice(left_anchors, right_anchors)
    shared_tokens = set(left_tokens) & set(right_tokens)
    strong_shared = {
        token for token in shared_tokens if token.isdigit() or len(token) >= 5 or token in _BREAKING_HINTS
    }
    score = (0.35 * tfidf) + (0.20 * fuzzy) + (0.20 * overlap) + (0.25 * anchor_overlap)
    if len(strong_shared) >= 3:
        score = max(score, 0.34)
    if anchor_overlap >= 0.5 and overlap >= 0.18:
        score = max(score, 0.36)
    return max(0.0, min(1.0, score))


_ALERT_LABEL_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "🕯️ Casualty Alert",
        (
            "killed",
            "dead",
            "deaths",
            "casualties",
            "casualty",
            "injured",
            "wounded",
            "fatalities",
            "fatality",
            "massacre",
            "body count",
        ),
    ),
    (
        "🛡️ Interception Alert",
        (
            "intercepted",
            "interception",
            "interceptions",
            "shot down",
            "air defense",
            "air-defence",
            "air defence",
            "iron dome",
            "defenses activated",
            "defence activated",
        ),
    ),
    (
        "⚠️ Civil Warning",
        (
            "evacuate",
            "evacuation",
            "warning",
            "warnings",
            "shelter",
            "shelters",
            "airspace closed",
            "closure",
            "curfew",
            "stay indoors",
            "sirens",
        ),
    ),
    (
        "🚨 Strike Alert",
        (
            "airstrike",
            "air strike",
            "strike",
            "strikes",
            "missile",
            "missiles",
            "rocket",
            "rockets",
            "drone strike",
            "bombing",
            "blast",
            "blasts",
            "explosion",
            "explosions",
            "shelling",
            "raid",
            "raids",
        ),
    ),
    (
        "📣 Major Statement",
        (
            "said",
            "says",
            "announced",
            "declared",
            "statement",
            "spokesperson",
            "spokesman",
            "spokeswoman",
            "official says",
            "minister says",
            "president says",
            "trump says",
        ),
    ),
    (
        "🚢 Maritime Watch",
        (
            "vessel",
            "ship",
            "shipping",
            "cargo ship",
            "tanker",
            "strait of hormuz",
            "naval",
            "port",
            "crew rescued",
        ),
    ),
    (
        "🏛️ Leadership Alert",
        (
            "leader",
            "supreme leader",
            "president",
            "prime minister",
            "commander",
            "succession",
            "cabinet",
            "government reshuffle",
            "assassinated",
            "resigned",
        ),
    ),
    (
        "⚡ Disruption Alert",
        (
            "outage",
            "blackout",
            "cyber",
            "internet down",
            "power cut",
            "power outage",
            "communications down",
            "disruption",
        ),
    ),
)


_GENERIC_BREAKING = "Breaking"
_GENERIC_NEWS_UPDATE = "News Update"
_ENHANCED_CATEGORY_EMOJI_PREFIXES = {
    "air_defense": "🛡️",
    "missile_strike": "🎯",
    "rocket_fire": "🚀",
    "drone_attack": "🛸",
    "artillery_shelling": "💥",
    "border_clash": "⚔️",
    "maritime_security": "⚓",
    "piracy_hijacking": "🏴‍☠️",
    "sanctions_export_control": "⛓️",
    "covert_intelligence": "🕵️",
    "leadership_change": "🏛️",
    "treaty_agreement": "✍️",
    "corruption_probe": "🧾",
    "policing_public_safety": "🚔",
    "commodity_energy": "🛢️",
    "labor_strike": "🪧",
    "telecom_internet": "📡",
    "aviation_incident": "🛬",
    "shipping_port": "⚓",
    "rail_transit": "🚆",
    "industrial_fire_explosion": "🏭💥",
    "water_sanitation": "🚰",
    "earthquake": "🌍",
    "storm_typhoon": "🌪️",
    "heat_cold": "🌡️",
    "hospital_health_emergency": "🚑",
    "space_launch_incident": "🛰️",
}


def _generic_alert_label(severity: str) -> str:
    normalized_severity = normalize_space(severity).lower()
    if normalized_severity == "high":
        return _GENERIC_BREAKING
    return _GENERIC_NEWS_UPDATE


def _strip_label_prefix_icon(label: str) -> str:
    cleaned = normalize_space(label)
    if not cleaned:
        return ""
    parts = cleaned.split()
    while parts and not re.search(r"[A-Za-z]", parts[0]):
        parts.pop(0)
    return normalize_space(" ".join(parts)) or cleaned


def _enhance_taxonomy_label(category_key: str, label: str) -> str:
    plain_label = _strip_label_prefix_icon(label)
    if not plain_label:
        return label
    prefix = _ENHANCED_CATEGORY_EMOJI_PREFIXES.get(normalize_space(category_key).lower(), "")
    if not prefix:
        return label
    return f"{prefix} {plain_label}"


def _choose_alert_label_legacy(text: str, *, severity: str = "high") -> str:
    """
    Pick a more specific, human-readable alert label than generic "BREAKING".
    """
    lowered = normalize_space(text).lower()
    if lowered:
        for label, markers in _ALERT_LABEL_RULES:
            if any(marker in lowered for marker in markers):
                return label

    normalized_severity = normalize_space(severity).lower()
    if normalized_severity == "high":
        return "🔥 Flash Update"
    if normalized_severity == "medium":
        return "⚠️ Live Update"
    return "ℹ️ Situation Update"


def choose_alert_label(text: str, *, severity: str = "high") -> str:
    """
    Use taxonomy-backed themed labels when a concrete category is present.
    Explainer material falls back to a calm generic label.
    """
    normalized = normalize_space(text)
    if normalized and not should_downgrade_explainer_urgency(normalized):
        match = match_news_category(normalized)
        if match is not None:
            record_ontology_label_resolution(matched=True)
            return _enhance_taxonomy_label(match.category_key, match.label)
    record_ontology_label_resolution(matched=False)
    return _generic_alert_label(severity)


def build_alert_header(
    text: str,
    *,
    severity: str,
    source_title: str,
    include_source: bool,
) -> str:
    label = choose_alert_label(text, severity=severity)
    if include_source:
        safe_source = sanitize_telegram_html(source_title)
        return f"<b>{label} \u2022 {safe_source}</b>"
    return f"<b>{label}</b>"


@dataclass(frozen=True)
class QueryPlan:
    original_query: str
    cleaned_query: str
    hours_back: int
    start_ts: int | None
    end_ts: int | None
    explicit_time_filter: bool
    broad_query: bool
    keywords: tuple[str, ...]
    expanded_terms: tuple[str, ...]
    numbers: tuple[str, ...]
    search_variants: tuple[str | None, ...]


def build_query_plan(query: str, *, default_hours: int = 24) -> QueryPlan:
    original = normalize_space(query)
    hours_back, cleaned, start_ts, end_ts = parse_time_filter_from_query(original, default_hours)
    effective = cleaned or original
    explicit_time_filter = (
        hours_back != max(1, int(default_hours))
        or effective != original
        or start_ts is not None
        or end_ts is not None
    )
    broad = is_broad_news_query(original)
    keywords = tuple(extract_query_keywords(effective))
    expanded_terms = tuple(expand_query_terms(effective))
    numbers = tuple(extract_query_numbers(effective))
    variants = tuple(build_query_search_variants(effective, broad_query=broad))
    return QueryPlan(
        original_query=original,
        cleaned_query=effective,
        hours_back=hours_back,
        start_ts=start_ts,
        end_ts=end_ts,
        explicit_time_filter=explicit_time_filter,
        broad_query=broad,
        keywords=keywords,
        expanded_terms=expanded_terms,
        numbers=numbers,
        search_variants=variants,
    )


_QUERY_GENERIC_TERMS = {
    "about",
    "after",
    "all",
    "also",
    "and",
    "any",
    "are",
    "as",
    "brief",
    "briefing",
    "can",
    "could",
    "current",
    "day",
    "days",
    "detail",
    "details",
    "did",
    "do",
    "does",
    "development",
    "developments",
    "digest",
    "explain",
    "find",
    "for",
    "from",
    "give",
    "has",
    "have",
    "happened",
    "happening",
    "headline",
    "headlines",
    "how",
    "hour",
    "hours",
    "into",
    "is",
    "its",
    "last",
    "latest",
    "lookup",
    "look",
    "me",
    "my",
    "news",
    "now",
    "of",
    "on",
    "or",
    "past",
    "please",
    "present",
    "query",
    "recent",
    "recently",
    "recap",
    "regarding",
    "report",
    "reports",
    "reply",
    "replying",
    "roundup",
    "search",
    "searching",
    "show",
    "situation",
    "status",
    "subject",
    "summary",
    "than",
    "that",
    "the",
    "tell",
    "there",
    "this",
    "today",
    "up",
    "update",
    "updates",
    "want",
    "was",
    "were",
    "what",
    "which",
    "when",
    "where",
    "who",
    "would",
    "with",
    "why",
    "you",
    "yesterday",
}


_QUERY_ALIAS_MAP: dict[str, tuple[str, ...]] = {
    "tehran": ("tehran", "teheran", "تهران"),
    "iran": ("iran", "iranian", "ایران"),
    "israel": ("israel", "israeli", "اسرائیل", "اسراییل"),
    "gaza": ("gaza", "غزه"),
    "beirut": ("beirut", "بيروت", "بیروت"),
    "lebanon": ("lebanon", "لبنان"),
    "damascus": ("damascus", "دمشق"),
    "syria": ("syria", "syrian", "سوريا", "سوریه"),
    "baghdad": ("baghdad", "بغداد"),
    "iraq": ("iraq", "iraqi", "العراق", "عراق"),
    "erbil": ("erbil", "اربيل", "اربیل"),
    "basra": ("basra", "البصرة", "بصره", "بصرہ"),
    "hormuz": ("hormuz", "هرمز", "hormoz"),
    "dubai": ("dubai", "دبي", "دبی"),
    "bahrain": ("bahrain", "بحرين", "بحرین"),
    "telaviv": ("tel aviv", "تل أبيب", "تل‌آویو", "تل ابیب"),
    "tel": ("tel aviv", "تل أبيب", "تل‌آویو", "تل ابیب"),
    "haifa": ("haifa", "حيفا", "حیفا"),
    "yemen": ("yemen", "اليمن", "یمن"),
    "sanaa": ("sanaa", "صنعاء", "صنعا"),
    "aden": ("aden", "عدن"),
    "saada": ("saada", "صعدة", "صعده"),
    "saudi": ("saudi", "saudi arabia", "السعودية", "سعودی"),
    "riyadh": ("riyadh", "الرياض", "ریاض"),
    "uae": ("uae", "united arab emirates", "الإمارات", "امارات"),
    "emirates": ("uae", "united arab emirates", "الإمارات", "امارات"),
    "qatar": ("qatar", "قطر"),
    "jordan": ("jordan", "الأردن", "اردن"),
    "egypt": ("egypt", "مصر"),
}


_QUERY_FOCUS_LEAD_IN_RE = re.compile(
    r"^(?:"
    r"what(?:'s| is)?|which|who|where|when|why|"
    r"how(?: many| much)?|"
    r"tell me|show me|give me|find|search(?: for)?|look(?: up)?|"
    r"can you|could you|would you|do you know"
    r")\b",
    re.IGNORECASE,
)

_QUERY_FOCUS_TOPIC_RE = re.compile(
    r"^(?:"
    r"the\s+|"
    r"latest|recent|current|new|news|updates?|status|situation|coverage|"
    r"report(?:ing)?|brief(?:ing)?|summary|recap|about|on|for|regarding|re|around"
    r")+\b",
    re.IGNORECASE,
)


def extract_query_keywords(query: str) -> list[str]:
    """
    Extract meaningful subject terms from a natural-language query.

    Generic request words like "digest", "latest", "summary", "today" are
    intentionally removed so broad recap queries do not collapse into useless
    literal searches.
    """
    lowered = normalize_space(query).lower()
    if not lowered:
        return []
    tokens = re.findall(r"[a-z0-9]{3,}", lowered)
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token.isdigit():
            continue
        if token in _QUERY_GENERIC_TERMS:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def expand_query_terms(query: str) -> list[str]:
    """
    Expand keywords with lightweight multilingual aliases and transliterations.
    """
    base_keywords = extract_query_keywords(query)
    out: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        cleaned = normalize_space(value)
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(cleaned)

    for keyword in base_keywords:
        _push(keyword)
        alias_values = _QUERY_ALIAS_MAP.get(keyword.lower())
        if alias_values:
            for alias in alias_values:
                _push(alias)
    return out


def extract_query_focus_phrases(query: str) -> list[str]:
    """
    Pull shorter focus phrases from natural-language questions so search
    variants emphasize the subject, not the question framing.
    """
    normalized = normalize_space(query)
    if not normalized:
        return []

    phrases: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        cleaned = normalize_space(value)
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        phrases.append(cleaned)

    for segment in re.split(r"[,:;|]+", normalized):
        working = normalize_space(re.sub(r"[?!]+", " ", segment))
        if not working:
            continue

        previous = ""
        while working and working != previous:
            previous = working
            working = normalize_space(_QUERY_FOCUS_LEAD_IN_RE.sub("", working))
            working = normalize_space(_QUERY_FOCUS_TOPIC_RE.sub("", working))

        keywords = extract_query_keywords(working)
        if keywords:
            _push(" ".join(keywords[:5]))

        parts = re.split(r"\b(?:or|vs\.?|versus|and/or)\b", working, flags=re.IGNORECASE)
        if len(parts) <= 1:
            continue

        comparison_terms: list[str] = []
        for part in parts:
            part_keywords = extract_query_keywords(part)
            if not part_keywords:
                continue
            _push(" ".join(part_keywords[:3]))
            comparison_terms.extend(part_keywords[:2])

        comparison_terms = dedupe_preserve_order(comparison_terms)
        if len(comparison_terms) >= 2:
            _push(" ".join(comparison_terms[:4]))

    return phrases


def build_query_search_variants(query: str, *, broad_query: bool = False) -> list[str | None]:
    """
    Build multiple retrieval-friendly search variants from one user query.
    """
    normalized = normalize_space(query)
    base_keywords = extract_query_keywords(query)
    focus_phrases = extract_query_focus_phrases(query)
    expanded_terms = expand_query_terms(query)
    numbers = extract_query_numbers(query)

    variants: list[str | None] = []
    seen: set[str] = set()

    def _push(value: str | None) -> None:
        if value is None:
            key = "__none__"
            if key in seen:
                return
            seen.add(key)
            variants.append(None)
            return
        cleaned = normalize_space(value)
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        variants.append(cleaned)

    if normalized:
        _push(normalized)

    for phrase in focus_phrases[:6]:
        _push(phrase)

    if base_keywords:
        _push(" ".join(base_keywords[:5]))
        if len(base_keywords) >= 2:
            _push(" ".join(base_keywords[:2]))
            _push(" ".join(base_keywords[-2:]))
        if len(base_keywords) >= 3:
            _push(" ".join(base_keywords[:3]))
            _push(" ".join(base_keywords[-3:]))
        for term in base_keywords[:10]:
            _push(term)
        pairwise_terms = base_keywords[:6]
        for idx in range(len(pairwise_terms)):
            for jdx in range(idx + 1, len(pairwise_terms)):
                _push(f"{pairwise_terms[idx]} {pairwise_terms[jdx]}")

    if expanded_terms:
        base_keyword_keys = {item.lower() for item in base_keywords}
        alias_terms = [
            term for term in expanded_terms
            if term.lower() not in base_keyword_keys
        ]
        for term in alias_terms[:10]:
            _push(term)
        if alias_terms and base_keywords:
            _push(" ".join([base_keywords[0], alias_terms[0]]))

    if numbers:
        for number in numbers[:4]:
            _push(number)
        if base_keywords:
            _push(" ".join([*numbers[:2], *base_keywords[:2]]))
            _push(" ".join([*base_keywords[:2], *numbers[:2]]))

    if broad_query and not base_keywords and not focus_phrases and not numbers:
        _push(None)

    return variants or [None]


def extract_query_numbers(query: str) -> list[str]:
    """
    Extract meaningful numeric claims from a query.

    Examples:
    - "217 killed and 798 injured" -> ["217", "798"]
    - "last 24 hours" -> []
    """
    lowered = normalize_space(query).lower()
    if not lowered:
        return []
    values: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"\b\d{2,6}\b", lowered):
        if token in seen:
            continue
        if token in {"24", "48", "72"} and re.search(rf"\b{re.escape(token)}\s*(?:hours?|hrs?|h)\b", lowered):
            continue
        seen.add(token)
        values.append(token)
    return values


def is_broad_news_query(query: str) -> bool:
    """
    Detect recap-style prompts that should use broad time-window evidence rather
    than literal keyword matching.
    """
    lowered = normalize_space(query).lower()
    if not lowered:
        return False

    recap_markers = (
        "digest",
        "summary",
        "recap",
        "roundup",
        "briefing",
        "what happened",
        "latest updates",
        "latest developments",
        "top updates",
        "news update",
    )
    if any(marker in lowered for marker in recap_markers):
        return True

    if re.search(r"\b(?:today|yesterday)\b", lowered):
        return True

    if re.search(r"\b(?:last|past)\s+\d{1,3}\s*(?:hours?|hrs?|h|days?|d)\b", lowered):
        return not extract_query_keywords(lowered)

    topical_markers = (
        "latest",
        "recent",
        "news",
        "updates",
        "status",
        "situation",
        "overview",
        "brief",
        "briefing",
    )
    direct_question_markers = (
        "who",
        "why",
        "when",
        "where",
        "how many",
        "how much",
        "did",
        "does",
        "is there",
        "are there",
    )
    hard_fact_markers = (
        "leader",
        "successor",
        "succession",
        "president",
        "prime minister",
        "assassinated",
        "nuclear",
        "reactor",
    )
    keywords = extract_query_keywords(lowered)
    if (
        keywords
        and any(marker in lowered for marker in topical_markers)
        and not any(marker in lowered for marker in direct_question_markers)
        and not any(marker in lowered for marker in hard_fact_markers)
    ):
        return True

    return not extract_query_keywords(lowered)


def dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def split_markdown_chunks(text: str, max_chars: int = 3600) -> List[str]:
    """Split long markdown text into Telegram-safe chunks without breaking too hard."""
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    remaining = text
    while len(remaining) > max_chars:
        cut = remaining.rfind("\n", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = remaining.rfind(" ", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = max_chars

        part = remaining[:cut].rstrip()
        if part:
            chunks.append(part)
        remaining = remaining[cut:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


def split_html_chunks(text: str, max_chars: int = 3600) -> List[str]:
    """Split long HTML text into Telegram-safe chunks."""
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    remaining = text
    while len(remaining) > max_chars:
        cut = remaining.rfind("\n", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = remaining.rfind("<br>", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = remaining.rfind(" ", 0, max_chars)
        if cut < int(max_chars * 0.5):
            cut = max_chars
        if cut <= 0:
            cut = max_chars

        part = remaining[:cut].rstrip()
        if part:
            chunks.append(part)
        remaining = remaining[cut:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


_ALLOWED_HTML_TAGS = {
    "b",
    "i",
    "u",
    "s",
    "tg-spoiler",
    "tg-emoji",
    "code",
    "pre",
    "blockquote",
    "a",
}
_HTML_TAG_RE = re.compile(r"</?([a-zA-Z0-9-]+)([^>]*)>")
_HTML_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_HTML_EMOJI_ID_RE = re.compile(
    r"""emoji-id\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))""",
    re.IGNORECASE,
)
_HTML_HREF_RE = re.compile(
    r"""href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))""",
    re.IGNORECASE,
)
_HTML_CLASS_RE = re.compile(
    r"""class\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))""",
    re.IGNORECASE,
)


def _normalize_markdownish(text: str) -> str:
    value = text or ""
    # Lightweight markdown-to-plain cleanup before HTML rendering.
    value = re.sub(r"\*\*(.*?)\*\*", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"__(.*?)__", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"`{1,3}(.*?)`{1,3}", r"\1", value, flags=re.DOTALL)
    value = re.sub(r"\[(.*?)\]\((https?://[^\s)]+)\)", r"\1 (\2)", value)
    return value


def sanitize_telegram_html(text: str) -> str:
    """
    Allow only Telegram-safe HTML tags and escape everything else.
    """
    value = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    value = _normalize_markdownish(value)
    value = _HTML_BR_RE.sub("\n", value)
    if not value:
        return ""

    placeholders: dict[str, str] = {}
    idx = 0

    def _token(html_value: str) -> str:
        nonlocal idx
        marker = f"__TGHTML_{idx}__"
        placeholders[marker] = html_value
        idx += 1
        return marker

    def _replace_tag(match: re.Match[str]) -> str:
        raw_tag = match.group(1) or ""
        attrs = match.group(2) or ""
        tag = raw_tag.lower()
        full = match.group(0)
        is_closing = full.startswith("</")
        is_self_close = full.endswith("/>")

        if tag not in _ALLOWED_HTML_TAGS:
            return ""

        if is_closing:
            return _token(f"</{tag}>")

        if tag == "a":
            href_match = _HTML_HREF_RE.search(attrs)
            href = ""
            if href_match:
                href = (href_match.group(1) or href_match.group(2) or href_match.group(3) or "").strip()
            if not href:
                return ""
            safe_href = href if href.startswith(("http://", "https://", "tg://")) else ""
            if not safe_href:
                return ""
            return _token(f'<a href="{_escape_html(safe_href, quote=True)}">')

        if tag == "tg-emoji":
            emoji_match = _HTML_EMOJI_ID_RE.search(attrs)
            emoji_id = ""
            if emoji_match:
                emoji_id = (
                    emoji_match.group(1)
                    or emoji_match.group(2)
                    or emoji_match.group(3)
                    or ""
                ).strip()
            if not emoji_id.isdigit():
                return ""
            return _token(f'<tg-emoji emoji-id="{emoji_id}">')

        if tag == "pre":
            class_match = _HTML_CLASS_RE.search(attrs)
            class_name = ""
            if class_match:
                class_name = (
                    class_match.group(1) or class_match.group(2) or class_match.group(3) or ""
                ).strip()
            if class_name and re.fullmatch(r"language-[a-zA-Z0-9_+-]+", class_name):
                return _token(f'<pre class="{_escape_html(class_name, quote=True)}">')
            return _token("<pre>")

        if tag in {"b", "i", "u", "s", "tg-spoiler", "code", "blockquote"}:
            return _token(f"<{tag}>")

        if is_self_close:
            return _token(f"<{tag}>")
        return _token(f"<{tag}>")

    value = _HTML_TAG_RE.sub(_replace_tag, value)
    value = _escape_html(value, quote=False)
    for marker, html_tag in placeholders.items():
        value = value.replace(marker, html_tag)

    value = re.sub(r"[ \t]+\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def strip_telegram_html(text: str) -> str:
    """
    Convert Telegram HTML-like text into plain text.
    Useful as a fallback when parse_mode HTML is rejected by Telegram.
    """
    value = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not value:
        return ""

    value = _HTML_BR_RE.sub("\n", value)
    value = re.sub(r"</?[a-zA-Z0-9-]+(?:\s+[^>]*)?>", "", value)
    value = _unescape_html(value)
    value = re.sub(r"[ \t]+\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def load_custom_emoji_map(
    path: str | Path,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """
    Load emoji->custom_emoji_id map from JSON.

    Supported file shapes:
    - {"🔥": "123", "✅": "456"}
    - {"emoji_map": {"🔥": "123"}}
    - [{"emoji": "🔥", "custom_emoji_id": "123"}]
    """
    p = Path(path).expanduser()
    if not p.exists():
        if logger:
            logger.warning("Premium emoji map not found: %s", p)
        return {}

    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        if logger:
            logger.exception("Failed to parse premium emoji map: %s", p)
        return {}

    raw_map: dict[str, str] = {}
    if isinstance(payload, dict):
        candidate = payload.get("emoji_map")
        if isinstance(candidate, dict):
            for k, v in candidate.items():
                key = str(k)
                if isinstance(v, list):
                    picked = ""
                    for item in v:
                        value = str(item or "").strip()
                        if value.isdigit():
                            picked = value
                            break
                    if picked:
                        raw_map[key] = picked
                else:
                    raw_map[key] = str(v)
        else:
            for k, v in payload.items():
                key = str(k)
                if isinstance(v, list):
                    picked = ""
                    for item in v:
                        value = str(item or "").strip()
                        if value.isdigit():
                            picked = value
                            break
                    if picked:
                        raw_map[key] = picked
                else:
                    raw_map[key] = str(v)
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            emoji = str(item.get("emoji", "")).strip()
            custom_id = str(item.get("custom_emoji_id", "")).strip()
            if emoji and custom_id:
                raw_map[emoji] = custom_id

    cleaned: dict[str, str] = {}
    for emoji, custom_id in raw_map.items():
        e = str(emoji or "").strip()
        cid = str(custom_id or "").strip()
        if not e or not cid.isdigit():
            continue
        cleaned[e] = cid

    if logger:
        logger.info("Loaded %s premium emoji mappings from %s", len(cleaned), p)
    return cleaned


def apply_premium_emoji_html(text: str, emoji_map: dict[str, str]) -> str:
    """
    Replace mapped standard emojis with Telegram Premium <tg-emoji> tags.
    """
    if not text or not emoji_map:
        return text

    keys = [k for k in emoji_map.keys() if k]
    if not keys:
        return text

    # Preserve already-rendered custom emoji blocks.
    placeholders: dict[str, str] = {}
    idx = 0

    def _protect_block(match: re.Match[str]) -> str:
        nonlocal idx
        marker = f"__TGEMOJI_BLOCK_{idx}__"
        placeholders[marker] = match.group(0)
        idx += 1
        return marker

    protected = re.sub(
        r"<tg-emoji\b[^>]*>.*?</tg-emoji>",
        _protect_block,
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    pattern = re.compile("|".join(re.escape(k) for k in sorted(keys, key=len, reverse=True)))

    def _replace_segment(segment: str) -> str:
        def _replace(match: re.Match[str]) -> str:
            raw = match.group(0)
            custom_id = emoji_map.get(raw)
            if not custom_id:
                return raw
            return f'<tg-emoji emoji-id="{custom_id}">{raw}</tg-emoji>'

        return pattern.sub(_replace, segment)

    parts = re.split(r"(<[^>]+>)", protected)
    rebuilt: list[str] = []
    for part in parts:
        if not part:
            continue
        if part.startswith("<") and part.endswith(">"):
            rebuilt.append(part)
            continue
        rebuilt.append(_replace_segment(part))

    value = "".join(rebuilt)
    for marker, block in placeholders.items():
        value = value.replace(marker, block)
    return value


def format_ts(ts: int | None) -> str:
    if not ts:
        return "never"
    try:
        return datetime.fromtimestamp(int(ts), tz=runtime_timezone()).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return "never"


def format_eta(seconds: int | float | None) -> str:
    if seconds is None:
        return "unknown"
    sec = int(max(0, seconds))
    if sec < 60:
        return f"{sec}s"
    minutes, s = divmod(sec, 60)
    if minutes < 60:
        return f"{minutes}m {s}s"
    hours, m = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {m}m"
    days, h = divmod(hours, 24)
    return f"{days}d {h}h"


def parse_daily_times(values: Iterable[str]) -> List[Tuple[int, int]]:
    parsed: List[Tuple[int, int]] = []
    for raw in values:
        value = (raw or "").strip()
        if not value:
            continue
        if not re.fullmatch(r"[0-2][0-9]:[0-5][0-9]", value):
            continue
        hour = int(value[:2])
        minute = int(value[3:5])
        if hour > 23:
            continue
        parsed.append((hour, minute))

    parsed.sort(key=lambda x: (x[0], x[1]))
    deduped_labels = dedupe_preserve_order([f"{h:02d}:{m:02d}" for h, m in parsed])
    return [(int(v[:2]), int(v[3:5])) for v in deduped_labels]


def seconds_until_next_daily_time(daily_times: List[Tuple[int, int]]) -> int:
    if not daily_times:
        return 0

    now = runtime_now()
    candidates = []
    for hour, minute in daily_times:
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        candidates.append(candidate)

    next_run = min(candidates)
    delay = int((next_run - now).total_seconds())
    return max(1, delay)


def build_telegram_message_link(
    *,
    username: str | None,
    chat_id: int | str,
    message_id: int,
) -> str | None:
    if message_id <= 0:
        return None

    if username:
        clean = username.strip().lstrip("@")
        if clean:
            return f"https://t.me/{clean}/{message_id}"

    # Best-effort private chat/channel URL form:
    # -1001234567890 -> https://t.me/c/1234567890/<msg_id>
    try:
        cid = int(chat_id)
        if str(cid).startswith("-100"):
            private_id = str(cid)[4:]
            if private_id:
                return f"https://t.me/c/{private_id}/{message_id}"
    except Exception:
        return None
    return None


def log_structured(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields,
) -> None:
    payload = {
        "event": event,
        "ts": int(time.time()),
        **fields,
    }
    logger.log(level, json.dumps(payload, ensure_ascii=False, sort_keys=True))


def make_stream_html_preview(text: str) -> str:
    """
    Best-effort HTML safety for partial streamed text.
    It trims incomplete entities/tags and then sanitizes.
    """
    value = text or ""
    if not value:
        return value

    last_amp = value.rfind("&")
    last_semi = value.rfind(";")
    if last_amp > last_semi:
        value = value[:last_amp]

    last_lt = value.rfind("<")
    last_gt = value.rfind(">")
    if last_lt > last_gt:
        value = value[:last_lt]

    safe = sanitize_telegram_html(value)
    return safe.rstrip()


class NearDuplicateDetector:
    """
    Rolling near-duplicate detector.

    Preferred backend:
    - sentence-transformers all-MiniLM-L6-v2 embeddings + cosine similarity

    Fallback backend:
    - sparse token TF cosine similarity (no external dependencies)
    """

    def __init__(
        self,
        *,
        threshold: float = 0.83,
        max_items: int = 400,
        logger: logging.Logger | None = None,
    ) -> None:
        self.threshold = float(min(max(threshold, 0.5), 0.99))
        self.max_items = int(max(50, min(max_items, 5000)))
        self.logger = logger

        self._lock = threading.Lock()
        self._entries: deque[tuple[str, object]] = deque()
        self._text_counts: dict[str, int] = {}
        self._backend_name = "uninitialized"

        self._st_model = None
        self._np = None

    @property
    def backend_name(self) -> str:
        return self._backend_name

    def _ensure_backend(self) -> None:
        if self._backend_name != "uninitialized":
            return
        # Legacy detector remains no-HF to avoid heavyweight runtime downloads.
        self._backend_name = "tf_cosine_fallback"
        if self.logger:
            self.logger.info("Near-duplicate detector backend: TF cosine fallback (no-HF mode)")

    def _tokenize(self, text: str) -> list[str]:
        return re.findall(r"[a-z0-9]{2,}", text.lower())

    def _sparse_embed(self, text: str) -> dict[str, float]:
        tokens = self._tokenize(text)
        if not tokens:
            return {}
        tf = Counter(tokens)
        norm = math.sqrt(sum(v * v for v in tf.values()))
        if norm <= 0:
            return {}
        return {k: (v / norm) for k, v in tf.items()}

    def _embed(self, text: str) -> object:
        self._ensure_backend()
        if self._backend_name == "sentence_transformers" and self._st_model is not None:
            vector = self._st_model.encode(  # type: ignore[union-attr]
                [text],
                normalize_embeddings=True,
                convert_to_numpy=True,
            )[0]
            return vector
        return self._sparse_embed(text)

    def _cosine(self, a: object, b: object) -> float:
        if self._backend_name == "sentence_transformers" and self._np is not None:
            try:
                score = float(self._np.dot(a, b))
                return max(-1.0, min(1.0, score))
            except Exception:
                return 0.0

        if not isinstance(a, dict) or not isinstance(b, dict):
            return 0.0
        if not a or not b:
            return 0.0

        if len(a) > len(b):
            a, b = b, a
        dot = 0.0
        for key, value in a.items():
            other = b.get(key)
            if other is not None:
                dot += float(value) * float(other)
        return max(0.0, min(1.0, dot))

    def _append(self, text: str, vector: object) -> None:
        if len(self._entries) >= self.max_items:
            old_text, _old_vec = self._entries.popleft()
            prev = self._text_counts.get(old_text, 0)
            if prev <= 1:
                self._text_counts.pop(old_text, None)
            else:
                self._text_counts[old_text] = prev - 1

        self._entries.append((text, vector))
        self._text_counts[text] = self._text_counts.get(text, 0) + 1

    def warm_start(self, texts: Sequence[str]) -> None:
        with self._lock:
            for raw in texts[-self.max_items :]:
                cleaned = normalize_space(raw).lower()
                if not cleaned or len(cleaned) < 12:
                    continue
                if self._text_counts.get(cleaned, 0) > 0:
                    continue
                vector = self._embed(cleaned)
                self._append(cleaned, vector)

    def check_and_add(self, text: str) -> tuple[bool, float]:
        cleaned = normalize_space(text).lower()
        if not cleaned or len(cleaned) < 12:
            return False, 0.0

        with self._lock:
            if self._text_counts.get(cleaned, 0) > 0:
                return True, 1.0

            vector = self._embed(cleaned)
            best = 0.0
            for existing_text, existing_vec in self._entries:
                if existing_text == cleaned:
                    return True, 1.0
                score = self._cosine(vector, existing_vec)
                if score > best:
                    best = score
                if best >= self.threshold:
                    break

            if best >= self.threshold:
                return True, best

            self._append(cleaned, vector)
            return False, best

    def export_recent_texts(self, limit: int | None = None) -> list[str]:
        with self._lock:
            values = [text for text, _vec in self._entries]
        if limit is None or limit <= 0:
            return values
        return values[-int(limit) :]


def parse_time_filter_from_query(
    query: str,
    default_hours: int = 24,
) -> tuple[int, str, int | None, int | None]:
    """
    Parse time constraints from natural-language query.

    Supported examples:
    - "last 24 hours", "past 6h", "3d", "today", "yesterday"
    """
    text = normalize_space(query)
    lowered = text.lower()

    hours_back = max(1, int(default_hours))
    start_ts: int | None = None
    end_ts: int | None = None
    cleanup_patterns: list[str] = []

    def _shorthand_subject_ok(subject: str) -> bool:
        normalized = normalize_space(subject)
        if not normalized or "?" in normalized:
            return False
        if len(normalized.split()) > 3:
            return False
        blocked_markers = (
            "what",
            "why",
            "when",
            "where",
            "who",
            "how",
            "show",
            "give",
            "tell",
            "reply",
            "replying",
            "digest",
            "summary",
            "recap",
            "latest",
            "news",
            "update",
            "updates",
            "happened",
            "happening",
            "today",
            "yesterday",
            "last",
            "past",
        )
        subject_lowered = normalized.lower()
        return not any(re.search(rf"\b{re.escape(marker)}\b", subject_lowered) for marker in blocked_markers)

    shorthand_patterns = [
        r"^(?P<fragment>(?P<value>\d{1,3})\s*(?P<unit>hours?|hrs?|h|days?|d))$",
        r"^(?P<fragment>(?P<value>\d{1,3})\s*(?P<unit>hours?|hrs?|h|days?|d))\s+(?P<subject>.+)$",
        r"^(?P<subject>.+?)\s+(?P<fragment>(?P<value>\d{1,3})\s*(?P<unit>hours?|hrs?|h|days?|d))$",
    ]

    # explicit relative windows
    hour_patterns = [
        r"\b(?:last|past)\s+(\d{1,3})\s*(?:hours?|hrs?|h)\b",
    ]
    for pattern in hour_patterns:
        match = re.search(pattern, lowered)
        if match:
            hours_back = max(1, min(_QUERY_MAX_HOURS_BACK, int(match.group(1))))
            cleanup_patterns.append(pattern)
            break

    day_patterns = [
        r"\b(?:last|past)\s+(\d{1,2})\s*(?:days?|d)\b",
    ]
    for pattern in day_patterns:
        match = re.search(pattern, lowered)
        if match:
            hours_back = max(24, min(_QUERY_MAX_HOURS_BACK, int(match.group(1)) * 24))
            cleanup_patterns.append(pattern)
            break

    if not cleanup_patterns:
        for pattern in shorthand_patterns:
            match = re.match(pattern, lowered)
            if not match:
                continue
            subject = normalize_space(match.groupdict().get("subject") or "")
            if subject and not _shorthand_subject_ok(subject):
                continue
            value = int(match.group("value"))
            unit = match.group("unit").lower()
            if unit.startswith("d"):
                hours_back = max(24, min(_QUERY_MAX_HOURS_BACK, value * 24))
            else:
                hours_back = max(1, min(_QUERY_MAX_HOURS_BACK, value))
            cleanup_patterns.append(re.escape(match.group("fragment")))
            break

    # calendar-style shortcuts
    now_local = runtime_now()
    now_utc = now_local.astimezone(timezone.utc)
    if re.search(r"\byesterday\b", lowered):
        local_midnight_today = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        local_midnight_yesterday = local_midnight_today - timedelta(days=1)
        start_utc = local_midnight_yesterday.astimezone(timezone.utc)
        end_utc = local_midnight_today.astimezone(timezone.utc)
        start_ts = int(start_utc.timestamp())
        end_ts = int(end_utc.timestamp())
        hours_back = max(
            hours_back,
            max(1, math.ceil((now_utc - start_utc).total_seconds() / 3600)),
        )
        cleanup_patterns.append(r"\byesterday\b")
    if re.search(r"\btoday\b", lowered):
        local_midnight_today = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        start_utc = local_midnight_today.astimezone(timezone.utc)
        start_ts = int(start_utc.timestamp())
        end_ts = int(now_utc.timestamp())
        hours_back = max(
            hours_back,
            max(1, math.ceil((now_utc - start_utc).total_seconds() / 3600)),
        )
        cleanup_patterns.append(r"\btoday\b")

    cleaned = text
    for pattern in cleanup_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = normalize_space(cleaned)
    if not cleaned:
        cleaned = text

    return hours_back, cleaned, start_ts, end_ts


async def search_recent_messages(
    client: Any,
    monitored_chats: Sequence[int | str],
    query: str,
    *,
    max_messages: int = 50,
    default_hours_back: int = 24,
    progress_cb: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """
    Search recent Telegram posts across monitored chats.

    Strategy:
    1) server-side fuzzy search (`iter_messages(search=...)`) per chat
    2) fallback to scanning recent chat history if server search returns no matches
    """
    if not monitored_chats:
        return []

    from telethon.errors import FloodWaitError

    resolved_max = max(20, min(int(max_messages), 60))
    plan = build_query_plan(query, default_hours=default_hours_back)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=plan.hours_back)
    start_bound_ts = int(plan.start_ts or int(cutoff.timestamp()))
    end_bound_ts = int(plan.end_ts or 0)

    broad_query = plan.broad_query
    keyword_terms = set(plan.expanded_terms)
    query_numbers = set(plan.numbers)
    sparse_subject_query = bool(
        not broad_query and keyword_terms and len(keyword_terms) <= 6 and not query_numbers
    )

    stage_limit = max(10, min(60, resolved_max // max(1, min(len(monitored_chats), resolved_max)) + 8))
    if broad_query and not keyword_terms:
        # Broad recap queries need source diversity more than deep per-chat search.
        stage_limit = max(2, min(4, resolved_max // max(1, len(monitored_chats)) + 1))
    fallback_limit = max(30, min(120, stage_limit * 3))
    if sparse_subject_query:
        stage_limit = max(stage_limit, 16)
        fallback_limit = max(fallback_limit, 90)

    collected: dict[tuple[str, int], dict[str, Any]] = {}
    collected_lock = asyncio.Lock()
    scan_parallelism = max(1, min(len(monitored_chats), 6 if sparse_subject_query else 4))

    async def _emit_progress(**payload: Any) -> None:
        if progress_cb is None:
            return
        try:
            maybe = progress_cb(payload)
            if maybe is not None:
                await maybe
        except Exception:
            if logger:
                logger.debug("Query progress callback failed during Telegram search.", exc_info=True)

    def _extract_message_text(msg: Any) -> str:
        return normalize_space(str(getattr(msg, "message", "") or ""))

    async def _append_message(msg: Any, *, strict_keyword_filter: bool) -> None:
        message_id = int(getattr(msg, "id", 0) or 0)
        if message_id <= 0:
            return

        chat_id_raw = getattr(msg, "chat_id", None)
        if chat_id_raw is None:
            return
        chat_id = str(chat_id_raw)
        dedupe_key = (chat_id, message_id)

        dt = getattr(msg, "date", None)
        if not isinstance(dt, datetime):
            return
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        message_ts = int(dt.timestamp())
        if message_ts < start_bound_ts:
            return
        if end_bound_ts > 0 and message_ts >= end_bound_ts:
            return

        text_value = _extract_message_text(msg)
        if not text_value:
            return

        if strict_keyword_filter and (keyword_terms or query_numbers):
            lowered_text = text_value.lower()
            number_hit = any(number in lowered_text for number in query_numbers)
            keyword_hit = any(term in lowered_text for term in keyword_terms)
            if not keyword_hit and not number_hit:
                return

        chat_obj = getattr(msg, "chat", None)
        username = getattr(chat_obj, "username", None) if chat_obj is not None else None
        source = (
            normalize_space(str(getattr(chat_obj, "title", "") or ""))
            or normalize_space(str(username or ""))
            or chat_id
        )
        link = build_telegram_message_link(
            username=username if isinstance(username, str) else None,
            chat_id=chat_id,
            message_id=message_id,
        ) or ""

        async with collected_lock:
            if dedupe_key in collected:
                return
            collected[dedupe_key] = {
                "chat_id": chat_id,
                "message_id": message_id,
                "timestamp": int(dt.timestamp()),
                "date": dt.isoformat(),
                "source": source,
                "text": text_value,
                "link": link,
            }

    async def _reached_limit() -> bool:
        async with collected_lock:
            return len(collected) >= resolved_max

    async def _scan_chat(
        chat_ref: int | str,
        *,
        search_text: str | None,
        limit: int,
        strict_keyword_filter: bool,
    ) -> None:
        while True:
            try:
                if await _reached_limit():
                    return
                kwargs: dict[str, Any] = {"limit": int(limit)}
                if search_text:
                    kwargs["search"] = search_text

                async for msg in client.iter_messages(chat_ref, **kwargs):
                    await _append_message(msg, strict_keyword_filter=strict_keyword_filter)
                    if await _reached_limit():
                        return
                return
            except FloodWaitError as exc:
                wait_seconds = int(getattr(exc, "seconds", 1)) + 1
                if logger:
                    logger.warning("FloodWait during query search: sleeping %ss", wait_seconds)
                await asyncio.sleep(wait_seconds)
            except Exception:
                if logger:
                    logger.debug("Query search failed for chat=%s", chat_ref, exc_info=True)
                return

    async def _scan_chat_batch(
        *,
        search_text: str | None,
        limit: int,
        strict_keyword_filter: bool,
    ) -> None:
        semaphore = asyncio.Semaphore(scan_parallelism)

        async def _run(chat_ref: int | str) -> None:
            async with semaphore:
                await _scan_chat(
                    chat_ref,
                    search_text=search_text,
                    limit=limit,
                    strict_keyword_filter=strict_keyword_filter,
                )

        tasks = [
            asyncio.create_task(_run(chat_ref), name="query-scan-chat")
            for chat_ref in monitored_chats
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    # Stage 1: server-side fuzzy search using multiple variants. Long natural
    # language questions often perform poorly as a single literal search string.
    search_variants = list(plan.search_variants)

    for search_text in search_variants:
        await _emit_progress(
            scope="telegram",
            phase="variant",
            variant=search_text,
            window_hours=plan.hours_back,
        )
        await _scan_chat_batch(
            search_text=search_text,
            limit=stage_limit,
            strict_keyword_filter=False,
        )
        if await _reached_limit():
            break

    # Stage 2: fallback scan when server-side search is weak, not only fully empty.
    fallback_trigger = max(4, min(resolved_max, 10 if (keyword_terms or query_numbers) else 8))
    if len(collected) < fallback_trigger:
        await _emit_progress(
            scope="telegram",
            phase="fallback",
            variant=None,
            window_hours=plan.hours_back,
        )
        await _scan_chat_batch(
            search_text=None,
            limit=fallback_limit,
            strict_keyword_filter=bool(keyword_terms or query_numbers),
        )

    ordered = sorted(
        collected.values(),
        key=lambda item: int(item.get("timestamp", 0)),
        reverse=True,
    )
    return ordered[:resolved_max]


def _strip_html_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    return normalize_space(_unescape_html(text))


def _domain_allowed(hostname: str, allowed_domains: Sequence[str]) -> bool:
    host = normalize_space(hostname).lower().lstrip(".")
    if not host:
        return False
    normalized = [normalize_space(item).lower().lstrip(".") for item in allowed_domains if item]
    if not normalized:
        return True
    return any(host == domain or host.endswith(f".{domain}") for domain in normalized)


def _source_name_candidates(source_name: str) -> list[str]:
    """
    Convert feed source labels like "DW.com" or "Reuters" into domain-like
    candidates so allowlist checks still work when RSS item links use wrapper
    hosts such as news.google.com or bing.com.
    """
    raw = normalize_space(source_name).lower()
    if not raw:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        cleaned = normalize_space(value).lower().strip(".")
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        candidates.append(cleaned)

    base = re.sub(r"[^a-z0-9. -]+", "", raw)
    _push(base)
    compact = base.replace(" ", "")
    _push(compact)
    hyphenated = base.replace(" ", "-")
    _push(hyphenated)

    if "." not in compact and compact:
        for suffix in (".com", ".org", ".net", ".co.uk"):
            _push(f"{compact}{suffix}")

    return candidates


def _source_allowed(source_name: str, allowed_domains: Sequence[str]) -> bool:
    normalized = [normalize_space(item).lower().lstrip(".") for item in allowed_domains if item]
    if not normalized:
        return True
    for candidate in _source_name_candidates(source_name):
        if any(candidate == domain or candidate.endswith(f".{domain}") for domain in normalized):
            return True
    return False


def _parse_pub_datetime(value: str) -> datetime | None:
    raw = normalize_space(value)
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


async def search_recent_news_web(
    query: str,
    *,
    hours_back: int = 24,
    max_results: int = 12,
    allowed_domains: Sequence[str] | None = None,
    require_recent: bool = True,
    progress_cb: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """
    Strict web-news cross-check search (RSS based).

    Uses multiple RSS-backed news search endpoints and returns normalized context
    rows compatible with query answer generation.
    """
    plan = build_query_plan(query, default_hours=hours_back)
    text = plan.cleaned_query
    if len(text) < 3:
        return []

    resolved_hours = max(1, min(int(plan.hours_back), _QUERY_MAX_HOURS_BACK))
    resolved_max = max(3, min(int(max_results), 40))
    trusted_domains = list(allowed_domains or [])

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=resolved_hours)
    start_bound_ts = int(plan.start_ts or int(cutoff.timestamp()))
    end_bound_ts = int(plan.end_ts or 0)
    search_variants = [
        variant for variant in plan.search_variants
        if isinstance(variant, str) and normalize_space(variant)
    ]
    if not search_variants:
        search_variants = [text]

    providers = (
        (
            "bing_news_rss",
            lambda search_query: (
                f"https://www.bing.com/news/search?q={quote_plus(search_query)}&format=RSS"
            ),
        ),
        (
            "google_news_rss",
            lambda search_query: (
                "https://news.google.com/rss/search"
                f"?q={quote_plus(search_query)}&hl=en-US&gl=US&ceid=US:en"
            ),
        ),
    )

    async def _emit_progress(**payload: Any) -> None:
        if progress_cb is None:
            return
        try:
            maybe = progress_cb(payload)
            if maybe is not None:
                await maybe
        except Exception:
            if logger:
                logger.debug("Query progress callback failed during web search.", exc_info=True)

    async def _fetch_variant(
        http: Any,
        *,
        variant: str,
        provider_name: str,
        build_url,
    ) -> list[dict[str, Any]]:
        search_query = f"{variant} when:{resolved_hours}h"
        await _emit_progress(
            scope="web",
            phase="variant",
            variant=variant,
            provider=provider_name,
            window_hours=resolved_hours,
        )
        url = build_url(search_query)
        response = await http.get(url)
        if response.status_code != 200:
            if logger:
                logger.debug(
                    "Web cross-check RSS status=%s provider=%s variant=%s",
                    response.status_code,
                    provider_name,
                    variant,
                )
            return []

        try:
            root = ET.fromstring(response.text)
        except Exception:
            if logger:
                logger.debug(
                    "Web cross-check RSS parse failure provider=%s variant=%s.",
                    provider_name,
                    variant,
                    exc_info=True,
                )
            return []

        out: list[dict[str, Any]] = []
        local_dedupe: set[str] = set()
        for item in root.findall(".//item"):
            title = normalize_space(str(item.findtext("title") or ""))
            link = normalize_space(str(item.findtext("link") or ""))
            description = _strip_html_tags(str(item.findtext("description") or ""))
            pub_raw = normalize_space(str(item.findtext("pubDate") or ""))
            source_tag = item.find("source")
            source_name = normalize_space(source_tag.text if source_tag is not None else "")

            if not title or not link:
                continue
            if not link.startswith("http"):
                continue

            host = normalize_space(str(urlparse(link).hostname or "")).lower()
            host_allowed = _domain_allowed(host, trusted_domains)
            source_allowed = _source_allowed(source_name, trusted_domains)
            if not host_allowed and not source_allowed:
                continue

            pub_dt = _parse_pub_datetime(pub_raw)
            if require_recent:
                if pub_dt is None:
                    continue
                pub_ts = int(pub_dt.timestamp())
                if pub_ts < start_bound_ts:
                    continue
                if end_bound_ts > 0 and pub_ts >= end_bound_ts:
                    continue
                if pub_dt > now + timedelta(minutes=5):
                    continue
            if pub_dt is None:
                pub_dt = now

            if not source_name:
                source_name = host or "web"

            snippet = title
            if description and description.lower() not in title.lower():
                snippet = f"{title}. {description}"
            if len(snippet) > 700:
                snippet = f"{snippet[:697].rsplit(' ', 1)[0]}..."

            sig = f"{host}|{title.lower()}"
            if sig in local_dedupe:
                continue
            local_dedupe.add(sig)

            out.append(
                {
                    "chat_id": "web",
                    "message_id": 0,
                    "timestamp": int(pub_dt.timestamp()),
                    "date": pub_dt.isoformat(),
                    "source": f"Web:{source_name}",
                    "text": snippet,
                    "link": link,
                    "provider": provider_name,
                    "is_web": True,
                }
            )
        return out

    try:
        http = await get_web_http_client()
        original_headers = dict(http.headers)
        http.headers.update(headers)
        try:
            semaphore = asyncio.Semaphore(max(1, min(4, len(search_variants[:6]) * len(providers))))

            async def _run_variant_provider(variant: str, provider_name: str, build_url):
                async with semaphore:
                    return await _fetch_variant(
                        http,
                        variant=variant,
                        provider_name=provider_name,
                        build_url=build_url,
                    )

            tasks = [
                asyncio.create_task(
                    _run_variant_provider(variant, provider_name, build_url),
                    name="query-web-rss",
                )
                for variant in search_variants[:6]
                for provider_name, build_url in providers
            ]
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            http.headers.clear()
            http.headers.update(original_headers)
    except Exception:
        if logger:
            logger.debug("Web cross-check search failed.", exc_info=True)
        return []

    dedupe: set[str] = set()
    rows: list[dict[str, Any]] = []
    for result in task_results:
        if isinstance(result, Exception):
            if logger:
                logger.debug("Web cross-check task failed.", exc_info=result)
            continue
        for row in result:
            host = normalize_space(str(urlparse(str(row.get("link") or "")).hostname or "")).lower()
            title = normalize_space(str(row.get("text") or "")).lower()
            sig = f"{host}|{title}"
            if sig in dedupe:
                continue
            dedupe.add(sig)
            rows.append(row)

    rows.sort(key=lambda row: int(row.get("timestamp", 0)), reverse=True)
    return rows[:resolved_max]


@dataclass
class LiveStreamStats:
    total_chars: int
    estimated_tokens: int
    duration_seconds: float
    tokens_per_second: float
    edit_count: int
    message_count: int


class LiveTelegramStreamer:
    """
    Incremental message editor for token streaming UX.

    The streamer edits one placeholder message repeatedly and can emit
    continuation messages when final text exceeds Telegram-safe length.
    """

    def __init__(
        self,
        *,
        send_message,
        edit_message,
        get_message_id,
        placeholder_text: str,
        edit_interval_ms: int = 400,
        max_chars_per_edit: int = 120,
        typing_action_cb=None,
        typing_enabled: bool = True,
        max_message_chars: int = 3900,
        html_mode: bool = True,
    ) -> None:
        self._send_message = send_message
        self._edit_message = edit_message
        self._get_message_id = get_message_id
        self._typing_action_cb = typing_action_cb
        self._typing_enabled = bool(typing_enabled)
        self._html_mode = bool(html_mode)
        self._placeholder_text = placeholder_text
        self._edit_interval = max(0.1, float(edit_interval_ms) / 1000.0)
        self._max_chars_per_edit = max(40, min(int(max_chars_per_edit), 800))
        self._max_message_chars = max(1200, min(int(max_message_chars), 3900))

        self._full_text = ""
        self._rendered_chars = 0
        self._last_edit_ts = 0.0
        self._edit_count = 0
        self._started_ts = 0.0

        self._primary_ref = None
        self._all_refs: list[Any] = []
        self._typing_task: asyncio.Task | None = None
        self._closed = False

    async def start(self, *, initial_ref: Any | None = None) -> Any:
        self._started_ts = time.time()
        if initial_ref is None:
            self._primary_ref = await self._send_message(self._placeholder_text, None)
        else:
            self._primary_ref = initial_ref

        self._all_refs = [self._primary_ref]
        if self._typing_enabled and self._typing_action_cb is not None:
            self._typing_task = asyncio.create_task(self._typing_loop(), name="live-typing")
        return self._primary_ref

    async def _typing_loop(self) -> None:
        try:
            while not self._closed:
                try:
                    await self._typing_action_cb()
                except Exception:
                    pass
                await asyncio.sleep(4.0)
        except asyncio.CancelledError:
            return

    async def push(self, delta: str) -> None:
        if self._closed:
            return
        if not delta:
            return
        self._full_text += delta

        now = time.time()
        if now - self._last_edit_ts < self._edit_interval:
            return
        await self._flush_partial(force=False)

    async def _flush_partial(self, *, force: bool) -> None:
        if self._primary_ref is None or self._closed:
            return

        if force:
            target = len(self._full_text)
        else:
            target = min(len(self._full_text), self._rendered_chars + self._max_chars_per_edit)
            if target <= self._rendered_chars:
                return

        preview = self._full_text[:target]
        if self._html_mode:
            safe_preview = make_stream_html_preview(preview)
        else:
            safe_preview = preview.rstrip()
        if target < len(self._full_text):
            safe_preview = safe_preview.rstrip() + " ..."

        if not safe_preview.strip():
            return

        self._primary_ref = await self._edit_message(self._primary_ref, safe_preview)
        self._edit_count += 1
        self._rendered_chars = target
        self._last_edit_ts = time.time()

    async def finalize(
        self,
        final_text: str | None = None,
        *,
        chunks: Sequence[str] | None = None,
    ) -> LiveStreamStats:
        if final_text is not None:
            self._full_text = final_text

        cleaned_final = (self._full_text or "").rstrip()
        cleaned_final = re.sub(r"\s+\.\.\.$", "", cleaned_final).strip()
        if self._html_mode:
            cleaned_final = sanitize_telegram_html(cleaned_final)
        if not cleaned_final:
            cleaned_final = "<b>🟢 No relevant information found.</b>" if self._html_mode else "No relevant information found."

        final_chunks = list(chunks or ())
        if final_chunks:
            if self._html_mode:
                final_chunks = [
                    sanitize_telegram_html(str(chunk).strip())
                    for chunk in final_chunks
                    if str(chunk or "").strip()
                ]
            else:
                final_chunks = [str(chunk).strip() for chunk in final_chunks if str(chunk or "").strip()]
        else:
            final_chunks = (
                split_html_chunks(cleaned_final, max_chars=self._max_message_chars)
                if self._html_mode
                else split_markdown_chunks(cleaned_final, max_chars=self._max_message_chars)
            )
        if not final_chunks:
            final_chunks = [cleaned_final]

        if self._primary_ref is None:
            self._primary_ref = await self._send_message(final_chunks[0], None)
            self._all_refs = [self._primary_ref]
        else:
            self._primary_ref = await self._edit_message(self._primary_ref, final_chunks[0])
            self._all_refs = [self._primary_ref]
            self._edit_count += 1

        reply_to = self._get_message_id(self._primary_ref)
        for extra in final_chunks[1:]:
            ref = await self._send_message(extra, reply_to)
            self._all_refs.append(ref)
            reply_to = self._get_message_id(ref)

        self._closed = True
        if self._typing_task is not None:
            self._typing_task.cancel()
            await asyncio.gather(self._typing_task, return_exceptions=True)
            self._typing_task = None

        elapsed = max(0.001, time.time() - self._started_ts)
        est_tokens = estimate_tokens_rough(cleaned_final)
        return LiveStreamStats(
            total_chars=len(cleaned_final),
            estimated_tokens=est_tokens,
            duration_seconds=elapsed,
            tokens_per_second=float(est_tokens) / elapsed,
            edit_count=self._edit_count,
            message_count=len(self._all_refs),
        )


# -----------------------------------------------------------------------------
# Aggressive Global Duplicate Suppression
# -----------------------------------------------------------------------------

_DUPE_STOP_WORDS = {
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
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "with",
    "will",
    "this",
    "these",
    "those",
    "after",
    "before",
    "into",
    "over",
    "under",
    "via",
    "about",
    "says",
    "said",
    "reported",
    "report",
    "breaking",
    "urgent",
    "update",
    "flash",
    "just",
    "now",
    "today",
    "live",
    "news",
}

_BREAKING_HINTS = {
    "explosion",
    "strike",
    "airstrike",
    "drone",
    "missile",
    "killed",
    "dead",
    "casualties",
    "attack",
    "war",
    "mobilization",
    "evacuate",
    "evacuation",
    "state",
    "emergency",
    "earthquake",
    "flood",
    "wildfire",
    "blast",
    "hostage",
    "navy",
    "rescued",
}

_GEO_HINTS = {
    "gaza",
    "rafah",
    "beirut",
    "tehran",
    "damascus",
    "aleppo",
    "kyiv",
    "odessa",
    "moscow",
    "baghdad",
    "hormuz",
    "bushehr",
    "iran",
    "iraq",
    "israel",
    "lebanon",
    "syria",
    "yemen",
    "ukraine",
    "russia",
}

_EMOJI_RE = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"
    "\U0001F300-\U0001FAFF"
    "\u2600-\u27BF"
    "]+",
    flags=re.UNICODE,
)
_URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)
_TS_RE = re.compile(
    r"\b(?:\d{1,2}[:.]\d{2}(?::\d{2})?\s?(?:utc|gmt|am|pm)?)\b",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(?:\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b"
)
_NOISE_WORD_RE = re.compile(
    r"\b(?:breaking|urgent|flash|developing|update|news|alert)\b",
    re.IGNORECASE,
)
_NON_WORD_RE = re.compile(r"[^a-z0-9\s]")


def normalize_for_global_dupe(text: str) -> str:
    """
    Strong normalization tuned for cross-channel reposts/paraphrases.
    """
    value = normalize_space(text or "")
    if not value:
        return ""

    value = _URL_RE.sub(" ", value)
    value = _EMOJI_RE.sub(" ", value)
    value = _TS_RE.sub(" ", value)
    value = _DATE_RE.sub(" ", value)
    value = _NOISE_WORD_RE.sub(" ", value)
    value = value.lower()
    value = _NON_WORD_RE.sub(" ", value)
    value = normalize_space(value)
    return value


def build_dupe_fingerprint(text: str) -> tuple[str, str]:
    normalized = normalize_for_global_dupe(text)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest() if normalized else ""
    return normalized, digest


def _tokenize_for_dupe(normalized_text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]{2,}", normalized_text)
    return [token for token in tokens if token not in _DUPE_STOP_WORDS]


def _tfidf_cosine(tokens_a: Sequence[str], tokens_b: Sequence[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    tf_a = Counter(tokens_a)
    tf_b = Counter(tokens_b)
    vocab = set(tf_a.keys()) | set(tf_b.keys())
    if not vocab:
        return 0.0

    weights_a: dict[str, float] = {}
    weights_b: dict[str, float] = {}
    for term in vocab:
        df = int(term in tf_a) + int(term in tf_b)
        idf = math.log((2 + 1) / (df + 1)) + 1.0
        weights_a[term] = float(tf_a.get(term, 0)) * idf
        weights_b[term] = float(tf_b.get(term, 0)) * idf

    dot = sum(weights_a[t] * weights_b[t] for t in vocab)
    norm_a = math.sqrt(sum(v * v for v in weights_a.values()))
    norm_b = math.sqrt(sum(v * v for v in weights_b.values()))
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return max(0.0, min(1.0, dot / (norm_a * norm_b)))


def _set_jaccard(left: Sequence[str], right: Sequence[str]) -> float:
    if not left or not right:
        return 0.0
    a = set(left)
    b = set(right)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter <= 0:
        return 0.0
    union = len(a | b)
    if union <= 0:
        return 0.0
    return max(0.0, min(1.0, inter / union))


def _set_dice(left: Sequence[str], right: Sequence[str]) -> float:
    if not left or not right:
        return 0.0
    a = set(left)
    b = set(right)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter <= 0:
        return 0.0
    return max(0.0, min(1.0, (2.0 * inter) / (len(a) + len(b))))


def _token_bigrams(tokens: Sequence[str]) -> tuple[str, ...]:
    if len(tokens) < 2:
        return tuple()
    return tuple(f"{tokens[i]}_{tokens[i + 1]}" for i in range(len(tokens) - 1))


def _chargrams(text: str, n: int = 4) -> tuple[str, ...]:
    compact = re.sub(r"\s+", " ", text or "").strip()
    if len(compact) < n:
        return tuple()
    return tuple(compact[i : i + n] for i in range(len(compact) - n + 1))


def _anchor_tokens(tokens: Sequence[str]) -> tuple[str, ...]:
    anchors: list[str] = []
    for token in tokens:
        if not token:
            continue
        if token.isdigit():
            anchors.append(token)
            continue
        if token in _BREAKING_HINTS or token in _GEO_HINTS:
            anchors.append(token)
            continue
        # Long lexical anchors often survive paraphrases.
        if len(token) >= 8:
            anchors.append(token)
    return tuple(anchors)


def _cosine_dense(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for a, b in zip(vec_a, vec_b):
        dot += float(a) * float(b)
        norm_a += float(a) * float(a)
        norm_b += float(b) * float(b)
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    return max(0.0, min(1.0, dot / (math.sqrt(norm_a) * math.sqrt(norm_b))))


def _is_breaking_like(normalized_text: str) -> bool:
    if not normalized_text:
        return False
    tokens = set(_tokenize_for_dupe(normalized_text))
    if not tokens:
        return False
    if "state" in tokens and "emergency" in tokens:
        return True
    return bool(tokens & _BREAKING_HINTS)


@dataclass
class GlobalDuplicateResult:
    duplicate: bool
    final_score: float
    semantic_score: float
    tfidf_score: float
    fuzzy_score: float
    normalized_text: str
    text_hash: str
    matched_channel_id: str | None = None
    matched_message_id: int | None = None
    matched_hash: str = ""
    matched_timestamp: int = 0
    breaking_duplicate_recent: bool = False
    merged: bool = False
    source_name: str = ""
    matched_source_name: str = ""


@dataclass
class MediaDuplicateResult:
    duplicate: bool
    media_hash: str
    media_kind: str
    match_score: float = 0.0
    matched_channel_id: str | None = None
    matched_message_id: int | None = None
    matched_timestamp: int = 0


@dataclass
class _HybridRecord:
    channel_id: str
    message_id: int
    normalized_text: str
    token_key: str
    tokens: tuple[str, ...]
    bigrams: tuple[str, ...]
    chargrams: tuple[str, ...]
    anchors: tuple[str, ...]
    embedding: tuple[float, ...] | None
    timestamp: int
    text_hash: str


class HybridDuplicateEngine:
    """
    Aggressive hybrid duplicate detector:
    final_score = 0.5 * semantic + 0.3 * tfidf + 0.2 * fuzzy

    Semantic component:
    - sentence-transformers cosine when enabled
    - otherwise no-HF semantic proxy (token/bigram/chargram/anchor overlap)
    """

    def __init__(
        self,
        *,
        threshold: float = 0.87,
        history_hours: int = 4,
        max_items: int = 8000,
        use_sentence_transformers: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.threshold = float(min(max(threshold, 0.5), 0.99))
        self.history_hours = max(1, min(int(history_hours), 24))
        self.max_items = max(200, min(int(max_items), 100000))
        self.use_sentence_transformers = bool(use_sentence_transformers)
        self.logger = logger

        self._records: deque[_HybridRecord] = deque()
        self._lock = threading.Lock()

        self._st_model = None
        self._fuzz = None
        self._backend_name = "uninitialized"
        self._last_db_sync_ts = 0

    @property
    def backend_name(self) -> str:
        return self._backend_name

    @property
    def cache_size(self) -> int:
        with self._lock:
            return len(self._records)

    def _ensure_backends(self) -> None:
        if self._backend_name != "uninitialized":
            return

        semantic_loaded = False
        if self.use_sentence_transformers:
            try:
                from sentence_transformers import SentenceTransformer  # type: ignore

                model = None
                for candidate in ("all-MiniLM-L6-v2", "paraphrase-MiniLM-L6-v2"):
                    try:
                        model = SentenceTransformer(candidate)
                        break
                    except Exception:
                        continue
                if model is not None:
                    self._st_model = model
                    semantic_loaded = True
            except Exception as exc:
                semantic_loaded = False
                if self.logger:
                    self.logger.warning(
                        "sentence-transformers unavailable; using no-HF semantic proxy for dedupe (%s)",
                        exc,
                    )

        try:
            from rapidfuzz import fuzz  # type: ignore

            self._fuzz = fuzz
        except Exception:
            self._fuzz = None

        if semantic_loaded:
            self._backend_name = "hybrid_semantic_tfidf_fuzzy"
            if self.logger:
                self.logger.info(
                    "Hybrid duplicate backend: sentence-transformers + TF-IDF + rapidfuzz"
                )
        else:
            self._backend_name = "hybrid_nohf_semproxy_tfidf_fuzzy"
            if self.logger:
                self.logger.info(
                    "Hybrid duplicate backend: no-HF semantic proxy + TF-IDF + rapidfuzz"
                )

    def _serialize_embedding(self, embedding: tuple[float, ...] | None) -> bytes | None:
        if not embedding:
            return None
        try:
            return json.dumps(list(embedding), separators=(",", ":")).encode("utf-8")
        except Exception:
            return None

    def _deserialize_embedding(self, blob: bytes | None) -> tuple[float, ...] | None:
        if not blob:
            return None
        try:
            payload = json.loads(blob.decode("utf-8"))
            if not isinstance(payload, list):
                return None
            values = []
            for item in payload:
                try:
                    values.append(float(item))
                except Exception:
                    continue
            if not values:
                return None
            return tuple(values)
        except Exception:
            return None

    def _encode_semantic(self, normalized_text: str) -> tuple[float, ...] | None:
        self._ensure_backends()
        if self._st_model is None:
            return None
        try:
            vector = self._st_model.encode(  # type: ignore[union-attr]
                [normalized_text],
                normalize_embeddings=True,
                convert_to_numpy=True,
            )[0]
            return tuple(float(x) for x in vector.tolist())
        except Exception:
            return None

    def _fuzzy_similarity(self, key_a: str, key_b: str) -> float:
        if not key_a or not key_b:
            return 0.0
        if self._fuzz is not None:
            try:
                return float(self._fuzz.token_sort_ratio(key_a, key_b)) / 100.0
            except Exception:
                pass
        # stdlib fallback when rapidfuzz is unavailable
        from difflib import SequenceMatcher

        return SequenceMatcher(None, key_a, key_b).ratio()

    def _prune_locked(self, now_ts: int) -> None:
        cutoff = now_ts - (self.history_hours * 3600)
        while self._records and self._records[0].timestamp < cutoff:
            self._records.popleft()
        while len(self._records) > self.max_items:
            self._records.popleft()

    def _sync_from_db_locked(self, now_ts: int, *, force: bool = False) -> None:
        if not force and self._last_db_sync_ts > 0 and (now_ts - self._last_db_sync_ts) < 20:
            return

        rows = load_recent_breaking(
            since_ts=now_ts - (self.history_hours * 3600),
            limit=self.max_items,
        )
        rows = list(reversed(rows))
        rebuilt: deque[_HybridRecord] = deque()
        for row in rows:
            normalized_text = normalize_space(str(row.get("normalized_text") or ""))
            if len(normalized_text) < 12:
                continue
            rebuilt.append(
                self._build_record(
                    channel_id=str(row.get("channel_id") or ""),
                    message_id=int(row.get("message_id") or 0),
                    normalized_text=normalized_text,
                    timestamp=int(row.get("timestamp") or now_ts),
                    text_hash=str(row.get("hash") or ""),
                    embedding=self._deserialize_embedding(row.get("embedding_blob", b"")),  # type: ignore[arg-type]
                )
            )
        self._records = rebuilt
        self._prune_locked(now_ts)
        self._last_db_sync_ts = now_ts

    def _build_record(
        self,
        *,
        channel_id: str,
        message_id: int,
        normalized_text: str,
        timestamp: int,
        text_hash: str,
        embedding: tuple[float, ...] | None,
    ) -> _HybridRecord:
        tokens = tuple(_tokenize_for_dupe(normalized_text))
        token_key = " ".join(sorted(tokens))
        bigrams = _token_bigrams(tokens)
        chargrams = _chargrams(token_key, n=4)
        anchors = _anchor_tokens(tokens)
        return _HybridRecord(
            channel_id=str(channel_id),
            message_id=int(message_id),
            normalized_text=normalized_text,
            token_key=token_key,
            tokens=tokens,
            bigrams=bigrams,
            chargrams=chargrams,
            anchors=anchors,
            embedding=embedding,
            timestamp=int(timestamp),
            text_hash=text_hash,
        )

    def _semantic_proxy_score(
        self,
        *,
        current_tokens: Sequence[str],
        current_bigrams: Sequence[str],
        current_chargrams: Sequence[str],
        current_anchors: Sequence[str],
        current_key: str,
        record: _HybridRecord,
    ) -> tuple[float, float, float, float, float, float]:
        token_jacc = _set_jaccard(current_tokens, record.tokens)
        bigram_dice = _set_dice(current_bigrams, record.bigrams)
        char_jacc = _set_jaccard(current_chargrams, record.chargrams)
        anchor_dice = _set_dice(current_anchors, record.anchors)
        order_sim = self._fuzzy_similarity(current_key, record.token_key)

        # Weighted no-HF semantic proxy tuned for paraphrase-heavy news reposts.
        score = (
            (0.30 * token_jacc)
            + (0.25 * bigram_dice)
            + (0.22 * char_jacc)
            + (0.15 * anchor_dice)
            + (0.08 * order_sim)
        )
        return (
            max(0.0, min(1.0, score)),
            token_jacc,
            bigram_dice,
            char_jacc,
            anchor_dice,
            order_sim,
        )

    def warm_backends(self) -> None:
        self._ensure_backends()

    def warm_start_from_db(self, *, warm_hours: int = 2, ensure_backends: bool = True) -> None:
        now_ts = int(time.time())
        hours = max(1, min(int(warm_hours), self.history_hours))
        rows = load_recent_breaking(
            since_ts=now_ts - (hours * 3600),
            limit=self.max_items,
        )
        rows = list(reversed(rows))

        with self._lock:
            if ensure_backends:
                self._ensure_backends()
            for row in rows:
                normalized_text = normalize_space(str(row.get("normalized_text") or ""))
                if len(normalized_text) < 12:
                    continue
                record = self._build_record(
                    channel_id=str(row.get("channel_id") or ""),
                    message_id=int(row.get("message_id") or 0),
                    normalized_text=normalized_text,
                    timestamp=int(row.get("timestamp") or now_ts),
                    text_hash=str(row.get("hash") or ""),
                    embedding=self._deserialize_embedding(row.get("embedding_blob", b"")),  # type: ignore[arg-type]
                )
                self._records.append(record)
            self._prune_locked(now_ts)
            self._last_db_sync_ts = now_ts

    def check_and_store(
        self,
        *,
        channel_id: str,
        message_id: int,
        raw_text: str,
        timestamp: int | None = None,
    ) -> GlobalDuplicateResult:
        normalized_text, text_hash = build_dupe_fingerprint(raw_text)
        if len(normalized_text) < 12:
            return GlobalDuplicateResult(
                duplicate=False,
                final_score=0.0,
                semantic_score=0.0,
                tfidf_score=0.0,
                fuzzy_score=0.0,
                normalized_text=normalized_text,
                text_hash=text_hash,
            )

        now_ts = int(timestamp if timestamp is not None else time.time())
        self._ensure_backends()
        embedding = self._encode_semantic(normalized_text)
        current_tokens = tuple(_tokenize_for_dupe(normalized_text))
        current_key = " ".join(sorted(current_tokens))
        current_bigrams = _token_bigrams(current_tokens)
        current_chargrams = _chargrams(current_key, n=4)
        current_anchors = _anchor_tokens(current_tokens)

        best_score = 0.0
        best_semantic = 0.0
        best_tfidf = 0.0
        best_fuzzy = 0.0
        best_record: _HybridRecord | None = None
        effective_threshold = self.threshold

        with self._lock:
            self._prune_locked(now_ts)
            self._sync_from_db_locked(now_ts)

            for record in self._records:
                if record.channel_id == str(channel_id) and record.message_id == int(message_id):
                    continue

                if record.text_hash and record.text_hash == text_hash:
                    best_record = record
                    best_score = 1.0
                    best_semantic = 1.0
                    best_tfidf = 1.0
                    best_fuzzy = 1.0
                    break

                tfidf_score = _tfidf_cosine(current_tokens, record.tokens)
                fuzzy_score = self._fuzzy_similarity(current_key, record.token_key)
                semantic_score = 0.0
                token_jacc = 0.0
                bigram_dice = 0.0
                char_jacc = 0.0
                anchor_dice = 0.0
                order_sim = 0.0
                if embedding is not None and record.embedding is not None:
                    semantic_score = _cosine_dense(embedding, record.embedding)
                else:
                    (
                        semantic_score,
                        token_jacc,
                        bigram_dice,
                        char_jacc,
                        anchor_dice,
                        order_sim,
                    ) = self._semantic_proxy_score(
                        current_tokens=current_tokens,
                        current_bigrams=current_bigrams,
                        current_chargrams=current_chargrams,
                        current_anchors=current_anchors,
                        current_key=current_key,
                        record=record,
                    )
                    effective_threshold = min(effective_threshold, 0.82)

                final_score = (0.5 * semantic_score) + (0.3 * tfidf_score) + (0.2 * fuzzy_score)
                # Aggressive boost rules for paraphrased echoes in no-HF mode.
                if tfidf_score >= 0.72 and fuzzy_score >= 0.88:
                    final_score = max(final_score, 0.90)
                if semantic_score >= 0.78 and tfidf_score >= 0.65:
                    final_score = max(final_score, 0.89)
                if anchor_dice >= 0.66 and tfidf_score >= 0.62:
                    final_score = max(final_score, 0.88)
                if anchor_dice >= 0.50 and token_jacc >= 0.42 and fuzzy_score >= 0.62:
                    final_score = max(final_score, 0.89)
                if bigram_dice >= 0.45 and char_jacc >= 0.45 and tfidf_score >= 0.48:
                    final_score = max(final_score, 0.88)
                if order_sim >= 0.85 and token_jacc >= 0.40:
                    final_score = max(final_score, 0.87)
                if final_score > best_score:
                    best_score = final_score
                    best_semantic = semantic_score
                    best_tfidf = tfidf_score
                    best_fuzzy = fuzzy_score
                    best_record = record

            if best_score >= effective_threshold and best_record is not None:
                breaking_recent = (
                    _is_breaking_like(normalized_text)
                    and (now_ts - int(best_record.timestamp)) <= 1800
                )
                return GlobalDuplicateResult(
                    duplicate=True,
                    final_score=float(best_score),
                    semantic_score=float(best_semantic),
                    tfidf_score=float(best_tfidf),
                    fuzzy_score=float(best_fuzzy),
                    normalized_text=normalized_text,
                    text_hash=text_hash,
                    matched_channel_id=best_record.channel_id,
                    matched_message_id=best_record.message_id,
                    matched_hash=best_record.text_hash,
                    matched_timestamp=best_record.timestamp,
                    breaking_duplicate_recent=bool(breaking_recent),
                )

            # Persist non-duplicate anchor for future matching.
            new_record = self._build_record(
                channel_id=str(channel_id),
                message_id=int(message_id),
                normalized_text=normalized_text,
                timestamp=now_ts,
                text_hash=text_hash,
                embedding=embedding,
            )
            self._records.append(new_record)
            self._prune_locked(now_ts)

        save_recent_breaking(
            channel_id=str(channel_id),
            message_id=int(message_id),
            normalized_text=normalized_text,
            embedding_blob=self._serialize_embedding(embedding),
            timestamp=now_ts,
            text_hash=text_hash,
            history_hours=self.history_hours,
        )
        return GlobalDuplicateResult(
            duplicate=False,
            final_score=float(best_score),
            semantic_score=float(best_semantic),
            tfidf_score=float(best_tfidf),
            fuzzy_score=float(best_fuzzy),
            normalized_text=normalized_text,
            text_hash=text_hash,
        )

    def purge_old_records(self) -> int:
        now_ts = int(time.time())
        with self._lock:
            before = len(self._records)
            self._prune_locked(now_ts)
            after = len(self._records)
        db_pruned = purge_recent_breaking(history_hours=self.history_hours)
        return int((before - after) + db_pruned)


@dataclass
class DuplicateRuntime:
    engine: HybridDuplicateEngine
    logger: logging.Logger
    merge_instead_of_skip: bool = True
    source_resolver: Callable[[Any], Awaitable[str]] | None = None
    merge_callback: Callable[[GlobalDuplicateResult, str], Awaitable[bool]] | None = None


_DUPLICATE_RUNTIME: DuplicateRuntime | None = None


def configure_duplicate_runtime(runtime: DuplicateRuntime | None) -> None:
    global _DUPLICATE_RUNTIME
    _DUPLICATE_RUNTIME = runtime


def check_and_store_media_duplicate(
    *,
    channel_id: str,
    message_id: int,
    media_hash: str,
    raw_text: str,
    media_kind: str,
    timestamp: int | None = None,
    history_hours: int = 4,
) -> MediaDuplicateResult:
    cleaned_hash = normalize_space(media_hash)
    if not cleaned_hash:
        return MediaDuplicateResult(duplicate=False, media_hash="", media_kind=str(media_kind or ""))

    now_ts = int(timestamp if timestamp is not None else time.time())
    since_ts = now_ts - (max(1, min(int(history_hours), 24)) * 3600)
    normalized_text, _ = build_dupe_fingerprint(raw_text)

    rows = load_recent_media_signatures(
        media_hash=cleaned_hash,
        since_ts=since_ts,
        limit=25,
    )
    best_row: dict[str, object] | None = None
    best_score = 0.0

    for row in rows:
        if str(row.get("channel_id") or "") == str(channel_id) and int(row.get("message_id") or 0) == int(message_id):
            continue
        existing_text = str(row.get("normalized_text") or "")
        if not normalized_text or not existing_text:
            best_row = row
            best_score = 1.0
            break
        score = media_duplicate_match_score(normalized_text, existing_text)
        recent_delta = now_ts - int(row.get("timestamp") or now_ts)
        if recent_delta <= 1800:
            score = max(score, 0.35)
        if score > best_score:
            best_score = score
            best_row = row

    duplicate = best_row is not None and best_score >= 0.32
    if not duplicate:
        save_recent_media_signature(
            channel_id=str(channel_id),
            message_id=int(message_id),
            media_hash=cleaned_hash,
            normalized_text=normalized_text,
            media_kind=str(media_kind or ""),
            timestamp=now_ts,
            history_hours=history_hours,
        )
        return MediaDuplicateResult(
            duplicate=False,
            media_hash=cleaned_hash,
            media_kind=str(media_kind or ""),
            match_score=float(best_score),
        )

    purge_recent_media_signatures(history_hours=history_hours)
    return MediaDuplicateResult(
        duplicate=True,
        media_hash=cleaned_hash,
        media_kind=str(media_kind or ""),
        match_score=float(best_score),
        matched_channel_id=str(best_row.get("channel_id") or ""),
        matched_message_id=int(best_row.get("message_id") or 0),
        matched_timestamp=int(best_row.get("timestamp") or 0),
    )


async def is_duplicate_and_handle(event: Any) -> GlobalDuplicateResult:
    """
    Global persistent dedupe gate.
    Call this as the first step in message intake.
    """
    runtime = _DUPLICATE_RUNTIME
    if runtime is None:
        return GlobalDuplicateResult(
            duplicate=False,
            final_score=0.0,
            semantic_score=0.0,
            tfidf_score=0.0,
            fuzzy_score=0.0,
            normalized_text="",
            text_hash="",
        )

    msg = getattr(event, "message", None)
    if msg is None:
        return GlobalDuplicateResult(
            duplicate=False,
            final_score=0.0,
            semantic_score=0.0,
            tfidf_score=0.0,
            fuzzy_score=0.0,
            normalized_text="",
            text_hash="",
        )

    channel_id = str(getattr(msg, "chat_id", ""))
    message_id = int(getattr(msg, "id", 0) or 0)
    raw_text = str(getattr(msg, "message", "") or "").strip()
    if message_id <= 0 or not raw_text:
        return GlobalDuplicateResult(
            duplicate=False,
            final_score=0.0,
            semantic_score=0.0,
            tfidf_score=0.0,
            fuzzy_score=0.0,
            normalized_text="",
            text_hash="",
        )

    result = await asyncio.to_thread(
        runtime.engine.check_and_store,
        channel_id=channel_id,
        message_id=message_id,
        raw_text=raw_text,
        timestamp=int(time.time()),
    )
    if not result.duplicate:
        return result

    source_name = channel_id
    if runtime.source_resolver is not None:
        try:
            source_name = normalize_space(await runtime.source_resolver(msg)) or channel_id
        except Exception:
            source_name = channel_id
    matched_source_name = str(result.matched_channel_id or "unknown")

    merged = False
    if (
        result.breaking_duplicate_recent
        and runtime.merge_instead_of_skip
        and runtime.merge_callback is not None
    ):
        try:
            merged = bool(await runtime.merge_callback(result, source_name))
        except Exception:
            merged = False

    result.merged = merged
    result.source_name = source_name
    result.matched_source_name = matched_source_name

    log_structured(
        runtime.logger,
        "duplicate_suppressed",
        level=logging.INFO,
        incoming_channel=source_name,
        incoming_channel_id=channel_id,
        incoming_message_id=message_id,
        matched_channel=matched_source_name,
        matched_channel_id=result.matched_channel_id,
        matched_message_id=result.matched_message_id,
        matched_timestamp=result.matched_timestamp,
        score=round(float(result.final_score), 4),
        semantic=round(float(result.semantic_score), 4),
        tfidf=round(float(result.tfidf_score), 4),
        fuzzy=round(float(result.fuzzy_score), 4),
        threshold=round(float(runtime.engine.threshold), 4),
        breaking_recent=bool(result.breaking_duplicate_recent),
        merged=bool(merged),
    )
    return result
