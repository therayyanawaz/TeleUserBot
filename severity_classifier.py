"""Deterministic multi-factor severity classifier for breaking vs digest routing."""

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Tuple

from news_signals import detect_story_signals


# -----------------------------------------------------------------------------
# SEVERITY_CONFIG
# -----------------------------------------------------------------------------
# This is the single place to tune severity behavior.
# Keep this dictionary explicit and readable so operators can adjust policy
# without rewriting classifier logic.
SEVERITY_CONFIG: Dict[str, Any] = {
    # -------------------------------------------------------------------------
    # Source trust tiers
    # -------------------------------------------------------------------------
    # S-tier should be reserved for high-confidence, rapid war/OSINT channels.
    # A/B/C should be expanded over time based on observed precision.
    "source_tiers": {
        "S": [
            "The War Reporter",
            "OsintTV 📺️",
            "Bellum Acta - Intel, Urgent News and Archives",
            "Frontline Report",
            "RNN Alerts",
            "𝕳𝖔𝖔𝖕𝖔𝖊 𝕰𝖓",
        ],
        "A": [
            # Add trusted but slightly slower/noisier sources here.
        ],
        "B": [
            # Add normal mixed-signal sources here.
        ],
        "C": [
            # Add low-confidence / noisy channels here.
        ],
    },
    # Tier weight contribution (max 0.35 by requirement).
    "source_tier_bonus": {
        "S": 0.35,
        "A": 0.24,
        "B": 0.14,
        "C": 0.06,
        "UNKNOWN": 0.04,
    },
    # Optional source-name patterns that should be penalized as noisy.
    "noisy_source_patterns": [
        "recap",
        "digest",
        "roundup",
        "analysis",
        "opinion",
        "thread",
    ],
    # -------------------------------------------------------------------------
    # Strong urgency signals (max 0.30)
    # -------------------------------------------------------------------------
    "strong_urgency_signals": [
        "breaking",
        "just in",
        "moments ago",
        "happening now",
        "massive strike",
        "direct hit",
        "heavy casualties",
        "confirmed killed",
        "explosion in",
        "missile attack",
        "air raid",
        "live update",
        "urgent",
        "alert",
    ],
    # Emoji/symbol urgency triggers.
    "strong_urgency_emoji_signals": [
        "🚨",
        "⚡",
        "❗❗",
        "🔴",
        "🔥",
    ],
    # Raw per-signal score for urgency.
    "urgency_per_signal": 0.08,
    "urgency_max_score": 0.30,
    # -------------------------------------------------------------------------
    # Style/format signals (max 0.15)
    # -------------------------------------------------------------------------
    "style": {
        "short_text_threshold_chars": 220,
        "short_with_link_or_media_bonus": 0.09,
        "emoji_density_min_count": 4,
        "emoji_density_bonus": 0.04,
        "alert_emoji_extra_bonus": 0.02,
        "caps_ratio_threshold": 0.45,
        "caps_bonus": 0.03,
        "punctuation_spike_bonus": 0.03,
        "max_score": 0.15,
    },
    # -------------------------------------------------------------------------
    # Humanized vital opinion integration (max 0.15)
    # -------------------------------------------------------------------------
    # This score is multiplied by existing humanized_vital_probability.
    "humanized": {
        "max_score": 0.15,
        "vital_keywords": [
            "killed",
            "dead",
            "casualties",
            "injured",
            "strike",
            "missile",
            "air raid",
            "explosion",
            "shelling",
            "drone",
            "evacuation",
            "nuclear",
            "chemical",
            "hostage",
            "intercepted",
        ],
    },
    # -------------------------------------------------------------------------
    # Temporal/context signals (max 0.05)
    # -------------------------------------------------------------------------
    "temporal_context": {
        "max_score": 0.05,
        "city_bonus": 0.02,
        "right_now_bonus": 0.02,
        "root_post_bonus": 0.01,
        "cities": [
            "gaza",
            "rafah",
            "beirut",
            "tel aviv",
            "tehran",
            "damascus",
            "aleppo",
            "kyiv",
            "odessa",
            "moscow",
            "baghdad",
            "basra",
            "hormuz",
            "tripoli",
            "sanaa",
            "kharkiv",
            "jerusalem",
            "haifa",
            "ismailia",
            "bushehr",
        ],
        "right_now_phrases": [
            "right now",
            "happening now",
            "just now",
            "moments ago",
            "currently",
            "live now",
            "الآن",
            "الان",
            "همین الان",
        ],
    },
    # -------------------------------------------------------------------------
    # Negative penalties (down to -0.30)
    # -------------------------------------------------------------------------
    "negative_penalties": {
        "max_penalty_abs": 0.30,
        "recap_keywords": [
            "daily recap",
            "summary",
            "overview",
            "what we know",
            "analysis",
            "thread",
            "opinion",
            "what happened today",
        ],
        "recap_penalty": 0.24,
        "analysis_penalty": 0.08,
        "long_text_tokens_threshold": 400,
        "long_text_penalty": 0.08,
        "noisy_source_penalty": 0.06,
    },
    # -------------------------------------------------------------------------
    # Decision thresholds
    # -------------------------------------------------------------------------
    "thresholds": {
        "high": 0.82,
        "medium": 0.55,
    },
    # -------------------------------------------------------------------------
    # Hard rules
    # -------------------------------------------------------------------------
    "hard_rules": {
        # Non-S source must have at least N urgency signals unless
        # humanized probability is very high.
        "non_s_min_urgency_signals_for_high": 2,
        "non_s_humanized_override_prob": 0.80,
        # Never allow recap/summary style messages to become high.
        "never_high_if_recap": True,
    },
    # -------------------------------------------------------------------------
    # Per-source high-severity cooldown
    # -------------------------------------------------------------------------
    "source_cooldown": {
        "window_seconds": 3600,
        "max_high_per_window": 3,
    },
}


_URL_RE = re.compile(r"https?://|t\.me/", re.IGNORECASE)
_EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001FAFF\u2600-\u26FF\u2700-\u27BF]",
    flags=re.UNICODE,
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

_HIGH_SEVERITY_HISTORY: Dict[str, Deque[float]] = defaultdict(deque)
_COOLDOWN_LOCK = threading.Lock()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _normalize_key(text: str) -> str:
    return _normalize_space(text).casefold()


def _extract_source_name(msg: Dict[str, Any]) -> str:
    value = (
        msg.get("source")
        or msg.get("source_name")
        or msg.get("channel_title")
        or msg.get("channel")
        or "unknown"
    )
    return _normalize_space(str(value))


def _extract_text(msg: Dict[str, Any]) -> str:
    value = msg.get("text") or msg.get("raw_text") or msg.get("caption") or ""
    return _normalize_space(str(value))


def _extract_tokens(text: str) -> list[str]:
    return [token.casefold() for token in _TOKEN_RE.findall(text)]


def _contains_phrase(normalized_text: str, phrase: str) -> bool:
    key = _normalize_key(phrase)
    return bool(key and key in normalized_text)


def _find_matches(normalized_text: str, phrases: list[str]) -> list[str]:
    matches: list[str] = []
    for phrase in phrases:
        if _contains_phrase(normalized_text, phrase):
            matches.append(phrase)
    return matches


def _count_emoji(text: str) -> int:
    return len(_EMOJI_RE.findall(text or ""))


def _alert_emoji_hits(text: str) -> list[str]:
    matches: list[str] = []
    for emoji in SEVERITY_CONFIG["strong_urgency_emoji_signals"]:
        if emoji in text:
            matches.append(emoji)
    return matches


def _caps_ratio(text: str) -> float:
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return 0.0
    upper = [ch for ch in letters if ch.isupper()]
    return len(upper) / len(letters)


def _punctuation_spike(text: str) -> bool:
    return bool(re.search(r"[!?]{2,}|[‼️]{1,}", text))


def _detect_source_tier(source_name: str) -> tuple[str, float]:
    normalized_source = _normalize_key(source_name)
    tier_map = SEVERITY_CONFIG["source_tiers"]
    tier_bonus = SEVERITY_CONFIG["source_tier_bonus"]

    for tier in ("S", "A", "B", "C"):
        candidates = tier_map.get(tier, [])
        for candidate in candidates:
            key = _normalize_key(str(candidate))
            if not key:
                continue
            if normalized_source == key or key in normalized_source:
                return tier, float(tier_bonus.get(tier, 0.0))
    # Default unknown sources to C-tier for strictness.
    return "C", float(tier_bonus.get("C", tier_bonus.get("UNKNOWN", 0.0)))


def _calc_urgency_score(text: str) -> tuple[float, list[str], int]:
    normalized = _normalize_key(text)
    phrase_hits = _find_matches(normalized, SEVERITY_CONFIG["strong_urgency_signals"])
    emoji_hits = _alert_emoji_hits(text)
    all_hits = phrase_hits + emoji_hits
    strong_count = len(set(all_hits))
    score = min(
        float(SEVERITY_CONFIG["urgency_max_score"]),
        strong_count * float(SEVERITY_CONFIG["urgency_per_signal"]),
    )
    return score, all_hits, strong_count


def _calc_style_score(text: str, *, has_link: bool, has_media: bool) -> tuple[float, Dict[str, Any]]:
    style_cfg = SEVERITY_CONFIG["style"]
    score = 0.0

    details: Dict[str, Any] = {
        "short_with_link_or_media": False,
        "emoji_density_count": 0,
        "caps_ratio": 0.0,
        "punctuation_spike": False,
    }

    char_len = len(text)
    if char_len <= int(style_cfg["short_text_threshold_chars"]) and (has_link or has_media):
        score += float(style_cfg["short_with_link_or_media_bonus"])
        details["short_with_link_or_media"] = True

    emoji_count = _count_emoji(text)
    details["emoji_density_count"] = emoji_count
    if emoji_count >= int(style_cfg["emoji_density_min_count"]):
        score += float(style_cfg["emoji_density_bonus"])
        alert_hits = len(_alert_emoji_hits(text))
        if alert_hits >= 2:
            score += float(style_cfg["alert_emoji_extra_bonus"])

    caps_ratio = _caps_ratio(text)
    details["caps_ratio"] = round(caps_ratio, 4)
    if caps_ratio >= float(style_cfg["caps_ratio_threshold"]) and len(text) >= 40:
        score += float(style_cfg["caps_bonus"])

    punct_spike = _punctuation_spike(text)
    details["punctuation_spike"] = punct_spike
    if punct_spike:
        score += float(style_cfg["punctuation_spike_bonus"])

    score = min(float(style_cfg["max_score"]), score)
    details["score"] = round(score, 4)
    return score, details


def _calc_humanized_score(
    text: str,
    *,
    humanized_probability: float,
    urgency_hits: int,
) -> tuple[float, Dict[str, Any]]:
    cfg = SEVERITY_CONFIG["humanized"]
    keywords = cfg["vital_keywords"]
    normalized = _normalize_key(text)

    vital_hits = _find_matches(normalized, keywords)
    vital_density = min(1.0, len(vital_hits) / 3.0)
    if vital_density <= 0 and urgency_hits >= 3:
        vital_density = 0.5

    probability = max(0.0, min(1.0, float(humanized_probability)))
    score = float(cfg["max_score"]) * probability * vital_density

    details = {
        "probability": round(probability, 4),
        "vital_hit_count": len(vital_hits),
        "vital_hits": vital_hits[:8],
        "vital_density": round(vital_density, 4),
        "score": round(score, 4),
    }
    return score, details


def _calc_temporal_context_score(text: str, *, reply_to: int) -> tuple[float, Dict[str, Any]]:
    cfg = SEVERITY_CONFIG["temporal_context"]
    normalized = _normalize_key(text)
    score = 0.0

    city_hits = _find_matches(normalized, cfg["cities"])
    if city_hits:
        score += float(cfg["city_bonus"])

    right_now_hits = _find_matches(normalized, cfg["right_now_phrases"])
    if right_now_hits:
        score += float(cfg["right_now_bonus"])

    is_root_post = int(reply_to or 0) == 0
    if is_root_post:
        score += float(cfg["root_post_bonus"])

    score = min(float(cfg["max_score"]), score)
    details = {
        "city_hits": city_hits[:6],
        "right_now_hits": right_now_hits[:6],
        "is_root_post": is_root_post,
        "score": round(score, 4),
    }
    return score, details


def _calc_negative_penalty(
    text: str,
    *,
    source_name: str,
    text_tokens: int,
    story_signals: Dict[str, Any],
) -> tuple[float, Dict[str, Any]]:
    cfg = SEVERITY_CONFIG["negative_penalties"]
    normalized = _normalize_key(text)
    penalty = 0.0

    recap_hits = _find_matches(normalized, cfg["recap_keywords"])
    if recap_hits:
        penalty -= float(cfg["recap_penalty"])

    analysis_hits = [
        hit
        for hit in recap_hits
        if hit in {"analysis", "thread", "opinion", "overview", "what we know"}
    ]
    if analysis_hits:
        penalty -= float(cfg["analysis_penalty"])

    if int(text_tokens) > int(cfg["long_text_tokens_threshold"]):
        penalty -= float(cfg["long_text_penalty"])

    source_key = _normalize_key(source_name)
    noisy_patterns = SEVERITY_CONFIG["noisy_source_patterns"]
    noisy_source_matches = [pat for pat in noisy_patterns if _normalize_key(pat) in source_key]
    if noisy_source_matches:
        penalty -= float(cfg["noisy_source_penalty"])

    if bool(story_signals.get("explainer_like")):
        penalty -= 0.14
    if bool(story_signals.get("question_led")):
        penalty -= 0.06

    max_abs = float(cfg["max_penalty_abs"])
    penalty = max(-max_abs, min(0.0, penalty))

    details = {
        "recap_hits": recap_hits[:8],
        "analysis_hits": analysis_hits[:8],
        "long_text": int(text_tokens) > int(cfg["long_text_tokens_threshold"]),
        "noisy_source_matches": noisy_source_matches[:6],
        "explainer_like": bool(story_signals.get("explainer_like")),
        "question_led": bool(story_signals.get("question_led")),
        "downgrade_explainer": bool(story_signals.get("downgrade_explainer")),
        "penalty": round(penalty, 4),
    }
    return penalty, details


def _cooldown_state(source_key: str, now_ts: float) -> dict[str, Any]:
    cfg = SEVERITY_CONFIG["source_cooldown"]
    window = int(cfg["window_seconds"])
    max_high = int(cfg["max_high_per_window"])

    with _COOLDOWN_LOCK:
        bucket = _HIGH_SEVERITY_HISTORY[source_key]
        while bucket and (now_ts - bucket[0]) > window:
            bucket.popleft()
        allowed = len(bucket) < max_high
        return {
            "allowed": allowed,
            "in_window": len(bucket),
            "max_per_window": max_high,
            "window_seconds": window,
        }


def _register_high(source_key: str, now_ts: float) -> None:
    with _COOLDOWN_LOCK:
        _HIGH_SEVERITY_HISTORY[source_key].append(now_ts)


def classify_message_severity(msg: dict) -> tuple[str, float, dict[str, Any]]:
    """
    Returns (severity, total_score, breakdown)

    severity: "high" | "medium" | "low"
    breakdown: full explainable dict for logging
    """

    text = _extract_text(msg)
    source_name = _extract_source_name(msg)
    normalized = _normalize_key(text)
    has_media = bool(msg.get("has_media", False))
    has_link = bool(msg.get("has_link", False)) or bool(_URL_RE.search(text))
    text_tokens = int(msg.get("text_tokens") or max(1, len(_extract_tokens(text))))
    reply_to = int(msg.get("reply_to") or 0)
    humanized_probability = float(msg.get("humanized_vital_probability") or 0.0)
    now_ts = float(msg.get("timestamp") or time.time())
    channel_id = str(msg.get("channel_id") or "").strip()
    story_signals = detect_story_signals(text)

    tier, source_score = _detect_source_tier(source_name)
    urgency_score, urgency_hits, urgency_hit_count = _calc_urgency_score(text)
    style_score, style_details = _calc_style_score(text, has_link=has_link, has_media=has_media)
    humanized_score, humanized_details = _calc_humanized_score(
        text,
        humanized_probability=humanized_probability,
        urgency_hits=urgency_hit_count,
    )
    temporal_score, temporal_details = _calc_temporal_context_score(text, reply_to=reply_to)
    penalty_score, penalty_details = _calc_negative_penalty(
        text,
        source_name=source_name,
        text_tokens=text_tokens,
        story_signals=story_signals,
    )

    total_score = source_score + urgency_score + style_score + humanized_score + temporal_score + penalty_score
    total_score = max(0.0, min(1.0, total_score))

    thresholds = SEVERITY_CONFIG["thresholds"]
    if total_score >= float(thresholds["high"]):
        severity = "high"
    elif total_score >= float(thresholds["medium"]):
        severity = "medium"
    else:
        severity = "low"

    hard_rules = SEVERITY_CONFIG["hard_rules"]
    recap_present = bool(penalty_details["recap_hits"])
    forced_reasons: list[str] = []

    # Hard rule 1: recap/summary-like posts cannot be high.
    if severity == "high" and hard_rules.get("never_high_if_recap", True) and recap_present:
        severity = "medium"
        forced_reasons.append("downgrade_recap_guard")

    # Hard rule 2: non-S sources need at least 2 urgency hits OR very high humanized probability.
    if severity == "high" and tier != "S":
        min_hits = int(hard_rules["non_s_min_urgency_signals_for_high"])
        override_prob = float(hard_rules["non_s_humanized_override_prob"])
        if urgency_hit_count < min_hits and humanized_probability <= override_prob:
            severity = "medium"
            forced_reasons.append("downgrade_non_s_insufficient_urgency")

    if severity == "high" and bool(story_signals.get("downgrade_explainer")):
        severity = "medium"
        forced_reasons.append("downgrade_explainer_guard")

    # Hard rule 3: per-source cooldown for high severity.
    source_key = channel_id or _normalize_key(source_name) or "unknown-source"
    cooldown_info = _cooldown_state(source_key, now_ts)
    if severity == "high":
        if cooldown_info["allowed"]:
            _register_high(source_key, now_ts)
            cooldown_info["in_window"] = int(cooldown_info["in_window"]) + 1
        else:
            severity = "medium"
            forced_reasons.append("downgrade_source_high_cooldown")

    breakdown: Dict[str, Any] = {
        "source": source_name,
        "source_tier": tier,
        "source_score": round(source_score, 4),
        "urgency_score": round(urgency_score, 4),
        "urgency_hits": urgency_hits[:12],
        "urgency_hit_count": urgency_hit_count,
        "style_score": round(style_score, 4),
        "style": style_details,
        "humanized_score": round(humanized_score, 4),
        "humanized": humanized_details,
        "temporal_score": round(temporal_score, 4),
        "temporal": temporal_details,
        "penalty_score": round(penalty_score, 4),
        "penalties": penalty_details,
        "story_signals": {
            "explainer_hits": list(story_signals.get("explainer_hits") or []),
            "question_led": bool(story_signals.get("question_led")),
            "concrete_event": bool(story_signals.get("concrete_event")),
            "official_development": bool(story_signals.get("official_development")),
            "recency": bool(story_signals.get("recency")),
            "downgrade_explainer": bool(story_signals.get("downgrade_explainer")),
            "live_event_update": bool(story_signals.get("live_event_update")),
        },
        "has_link": has_link,
        "has_media": has_media,
        "text_tokens": text_tokens,
        "text_chars": len(text),
        "normalized_text_preview": normalized[:220],
        "cooldown": cooldown_info,
        "forced_reasons": forced_reasons,
    }
    return severity, round(total_score, 4), breakdown
