"""Shared ontology-backed signals for explainer routing and live-event labeling."""

from __future__ import annotations

import re
from typing import Any, Dict

from news_taxonomy import (
    _QUESTION_LEAD_RE,
    analyze_news_ontology,
    record_ontology_live_event_resolution,
)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def detect_story_signals(
    text: str,
    ai_signals: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Detect story signals from text using deterministic ontology analysis.

    When ai_signals is provided, the AI's semantic understanding overrides
    the deterministic signal detection. Fields like concrete_event,
    breaking_eligible, explainer_like, etc. are merged on top of the
    deterministic results so AI catches nuance the rules engine misses.
    """
    normalized = normalize_space(text)
    lead = normalize_space(re.split(r"(?<=[.!?])\s+|\n+", normalized, maxsplit=1)[0])
    analysis = analyze_news_ontology(normalized)
    match = analysis.top_match
    frame = analysis.event_frame
    signal = analysis.signal_breakdown

    question_led = bool(_QUESTION_LEAD_RE.search(lead)) or (
        "?" in lead and bool(_QUESTION_LEAD_RE.search(lead.rstrip("?").strip()))
    )
    explainer_hits = list(frame.negative_hits)
    official_development = bool(frame.official_hits) and not frame.hedged
    recency = bool(frame.recency_hits)
    concrete_event = bool(match) or bool(frame.actions or frame.targets or frame.counts)
    breaking_eligible = bool(match and match.breaking_eligible)
    explainer_like = bool(explainer_hits) or question_led
    live_event_update = bool(
        signal.live_event_score >= 1.6
        or (
            concrete_event
            and (official_development or recency)
            and signal.explainer_score < 0.8
        )
    )
    downgrade_explainer = explainer_like and not (
        official_development or (breaking_eligible and recency and concrete_event)
    )

    if concrete_event or frame.actions or frame.targets or frame.places:
        record_ontology_live_event_resolution(frame, matched=bool(match))

    result: Dict[str, Any] = {
        "normalized_text": normalized,
        "explainer_hits": explainer_hits,
        "question_led": question_led,
        "concrete_event": concrete_event,
        "breaking_eligible": breaking_eligible,
        "official_development": official_development,
        "recency": recency,
        "explainer_like": explainer_like,
        "downgrade_explainer": downgrade_explainer,
        "live_event_update": live_event_update and not downgrade_explainer,
        "category_key": match.category_key if match else "",
        "category_label": match.label if match else "",
        "all_category_keys": [item.category_key for item in analysis.matches[:6]],
        "ontology": {
            "category_key": match.category_key if match else "",
            "label": match.label if match else "",
            "confidence_score": float(match.confidence_score) if match else 0.0,
            "severity_bias": match.severity_bias if match else "",
            "breaking_eligible": bool(match.breaking_eligible) if match else False,
            "signal_breakdown": {
                "event_hit_count": signal.event_hit_count,
                "action_hit_count": signal.action_hit_count,
                "actor_hit_count": signal.actor_hit_count,
                "target_hit_count": signal.target_hit_count,
                "place_hit_count": signal.place_hit_count,
                "recency_hit_count": signal.recency_hit_count,
                "official_hit_count": signal.official_hit_count,
                "negative_hit_count": signal.negative_hit_count,
                "hedge_hit_count": signal.hedge_hit_count,
                "live_event_score": signal.live_event_score,
                "recency_score": signal.recency_score,
                "official_score": signal.official_score,
                "explainer_score": signal.explainer_score,
            },
            "event_frame": {
                "actors": list(frame.actors),
                "actions": list(frame.actions),
                "targets": list(frame.targets),
                "places": list(frame.places),
                "counts": list(frame.counts),
                "recency_hits": list(frame.recency_hits),
                "official_hits": list(frame.official_hits),
                "negative_hits": list(frame.negative_hits),
                "hedge_hits": list(frame.hedge_hits),
                "confirmation_state": frame.confirmation_state,
            },
        },
    }

    # AI override: merge AI signals on top of deterministic results
    if ai_signals:
        for bool_field in (
            "concrete_event",
            "breaking_eligible",
            "official_development",
            "recency",
            "explainer_like",
            "live_event_update",
            "question_led",
        ):
            ai_val = ai_signals.get(bool_field)
            if isinstance(ai_val, bool):
                result[bool_field] = ai_val
        # Merge explainer_hits and urgency_hits
        ai_hits = ai_signals.get("explainer_hits", [])
        if isinstance(ai_hits, list) and ai_hits:
            seen = set(result["explainer_hits"])
            for hit in ai_hits:
                hit_str = str(hit)
                if hit_str and hit_str not in seen:
                    seen.add(hit_str)
                    result["explainer_hits"].append(hit_str)
        ai_urgency = ai_signals.get("urgency_hits", [])
        if isinstance(ai_urgency, list) and ai_urgency:
            result["_ai_urgency_hits"] = [str(u) for u in ai_urgency]

    return result


def looks_like_explainer_text(text: str) -> bool:
    return bool(detect_story_signals(text)["explainer_like"])


def should_downgrade_explainer_urgency(text: str) -> bool:
    return bool(detect_story_signals(text)["downgrade_explainer"])


def looks_like_live_event_update(text: str) -> bool:
    return bool(detect_story_signals(text)["live_event_update"])
