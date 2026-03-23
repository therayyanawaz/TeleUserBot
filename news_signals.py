"""Shared heuristics for explainer routing and live-event labeling."""

from __future__ import annotations

import re
from typing import Any, Dict, List


_EXPLAINER_MARKERS = (
    "analysis",
    "opinion",
    "thread",
    "explainer",
    "deep dive",
    "what we know",
    "what explains",
    "reasons for",
    "ways to address",
    "takeaways",
    "lessons from",
    "overview",
    "recap",
    "roundup",
    "full report",
    "full analysis",
)
_QUESTION_LEAD_RE = re.compile(
    r"^(?:why|how|what explains|what caused|what does|can|could|should|would|will)\b",
    flags=re.IGNORECASE,
)
_CONCRETE_EVENT_MARKERS = (
    "intercepted",
    "interception",
    "interceptions",
    "shot down",
    "air defense",
    "air-defence",
    "air defence",
    "strike",
    "strikes",
    "airstrike",
    "air strike",
    "missile",
    "missiles",
    "rocket",
    "rockets",
    "drone strike",
    "explosion",
    "explosions",
    "blast",
    "blasts",
    "bombing",
    "shelling",
    "raid",
    "raids",
    "attacked",
    "attack",
    "attacks",
    "killed",
    "dead",
    "injured",
    "wounded",
    "casualties",
    "fatalities",
    "evacuation order",
    "sirens",
    "launched",
    "fired",
    "hit",
    "hits",
    "landed",
)
_OFFICIAL_PATTERNS = (
    r"\b(?:ministry|army|military|government|president|prime minister|defense minister|foreign minister|spokesperson|spokesman|spokeswoman|officials?)\s+(?:said|says|announced|confirmed|declared|ordered|issued|warned|approved)\b",
    r"\b(?:statement|decree|order|directive|communique)\b",
    r"\b(?:confirmed|announced|declared|ordered|issued|approved|warned)\b",
)
_RECENCY_MARKERS = (
    "today",
    "tonight",
    "overnight",
    "this morning",
    "this afternoon",
    "this evening",
    "this weekend",
    "just now",
    "moments ago",
    "minutes ago",
    "hours ago",
    "earlier today",
    "latest",
    "new",
)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def detect_story_signals(text: str) -> Dict[str, Any]:
    normalized = normalize_space(text)
    lowered = normalized.lower()
    lead = normalize_space(re.split(r"(?<=[.!?])\s+|\n+", normalized, maxsplit=1)[0])

    explainer_hits: List[str] = [marker for marker in _EXPLAINER_MARKERS if marker in lowered]
    question_led = bool(_QUESTION_LEAD_RE.search(lead)) or (
        "?" in lead and bool(_QUESTION_LEAD_RE.search(lead.rstrip("?").strip()))
    )
    concrete_event = any(marker in lowered for marker in _CONCRETE_EVENT_MARKERS)
    official_development = any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in _OFFICIAL_PATTERNS)
    recency = any(marker in lowered for marker in _RECENCY_MARKERS)

    explainer_like = bool(explainer_hits) or question_led
    downgrade_explainer = explainer_like and not (
        official_development or (concrete_event and recency)
    )
    live_event_update = not downgrade_explainer and (
        official_development or (concrete_event and (recency or len(normalized) <= 280))
    )

    return {
        "normalized_text": normalized,
        "explainer_hits": explainer_hits,
        "question_led": question_led,
        "concrete_event": concrete_event,
        "official_development": official_development,
        "recency": recency,
        "explainer_like": explainer_like,
        "downgrade_explainer": downgrade_explainer,
        "live_event_update": live_event_update,
    }


def looks_like_explainer_text(text: str) -> bool:
    signals = detect_story_signals(text)
    return bool(signals["explainer_like"])


def should_downgrade_explainer_urgency(text: str) -> bool:
    signals = detect_story_signals(text)
    return bool(signals["downgrade_explainer"])


def looks_like_live_event_update(text: str) -> bool:
    signals = detect_story_signals(text)
    return bool(signals["live_event_update"])
