from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Mapping, Sequence

from news_taxonomy import match_breaking_category, normalize_taxonomy_text
from utils import build_dupe_fingerprint, normalize_space, strip_telegram_html


_NUMBER_WORDS = {
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
}
_NUMBER_WORD_RE = re.compile(
    r"\b(" + "|".join(sorted((re.escape(key) for key in _NUMBER_WORDS), key=len, reverse=True)) + r")\b",
    flags=re.IGNORECASE,
)
_NUMBER_WORD_PATTERN = "|".join(sorted((re.escape(key) for key in _NUMBER_WORDS), key=len, reverse=True))
_DIGIT_RE = re.compile(r"\b\d+\b")
_TITLE_LOCATION_RE = re.compile(
    r"\b(?:in|near|over|at|around|outside|south of|north of|east of|west of|off|across|toward|towards)\s+"
    r"([A-Z][A-Za-z'`-]*(?:\s+(?:[A-Z][A-Za-z'`-]*|of|the|al|el|tel|aviv|bnei)){0,3})"
)
_LOWER_LOCATION_RE = re.compile(
    r"\b(?:in|near|over|at|around|outside|south of|north of|east of|west of|off|across|toward|towards)\s+"
    r"([a-z][a-z'`-]*(?:\s+[a-z][a-z'`-]*){0,3})"
)
_TARGET_RE = re.compile(
    r"\b(?:hit|hits|struck|strike on|strikes on|targeted|targets|impact on|impacts on|attack on|attacked)\s+"
    r"(?:an?|the)?\s*([a-z][a-z0-9'`.-]*(?:\s+[a-z][a-z0-9'`.-]*){0,3})",
    flags=re.IGNORECASE,
)
_GENERIC_LOCATION_WORDS = {
    "area",
    "areas",
    "city",
    "district",
    "front",
    "governorate",
    "province",
    "region",
    "site",
    "town",
    "zone",
    "left",
    "leaves",
    "injured",
    "reports",
    "report",
    "say",
    "says",
    "wounded",
}
_TRAILING_FILLER_WORDS = {
    "after",
    "amid",
    "and",
    "as",
    "before",
    "during",
    "following",
    "for",
    "from",
    "into",
    "near",
    "on",
    "over",
    "toward",
    "towards",
    "with",
}
_TOPIC_STOPWORDS = {
    "a",
    "after",
    "again",
    "amid",
    "an",
    "and",
    "another",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "its",
    "near",
    "of",
    "on",
    "over",
    "says",
    "say",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "with",
}
_CASUALTY_MARKERS = (
    "casualties",
    "casualty",
    "dead",
    "deaths",
    "fatalities",
    "fatality",
    "hurt",
    "injured",
    "injury",
    "killed",
    "wounded",
)
_EVENT_COUNT_MARKERS = (
    "drone",
    "drones",
    "interceptor",
    "interceptors",
    "missile",
    "missiles",
    "projectile",
    "projectiles",
    "rocket",
    "rockets",
    "strike",
    "strikes",
)
_OFFICIAL_MARKERS = (
    "confirmed",
    "confirms",
    "confirmation",
    "emergency services",
    "hospital",
    "magen david adom",
    "mda",
    "ministry",
    "official",
    "officials",
    "police",
    "spokesperson",
    "statement",
)
_ACTOR_PATTERNS = {
    "iran": ("iran", "iranian", "iran-backed"),
    "israel": ("israel", "israeli", "idf"),
    "hezbollah": ("hezbollah", "islamic resistance"),
    "hamas": ("hamas",),
    "houthis": ("houthi", "houthis", "ansar allah"),
    "united_states": ("u.s.", "u.s", "us", "american", "united states"),
    "mda": ("magen david adom", "mda"),
}
_PHASE_MARKERS = {
    "launch": ("launch", "launches", "fired", "fire", "barrage", "salvo", "sirens"),
    "interception": ("intercept", "intercepted", "interception", "downed", "air defense"),
    "impact": ("impact", "impacts", "hit", "hits", "landed", "strike", "struck", "explosion", "blast"),
    "damage": ("damage", "damaged", "destroyed", "fire", "burning"),
    "casualties": ("casualties", "wounded", "injured", "dead", "killed", "fatalities", "hurt"),
    "retaliation": ("retaliation", "retaliatory", "response", "counterstrike", "counter-strike"),
}


@dataclass(frozen=True)
class BreakingStoryFacts:
    normalized_text: str
    text_hash: str
    numbers: tuple[str, ...]
    casualty_numbers: tuple[str, ...]
    event_numbers: tuple[str, ...]
    locations: tuple[str, ...]
    actors: tuple[str, ...]
    targets: tuple[str, ...]
    phases: tuple[str, ...]
    official_confirmation: bool


@dataclass(frozen=True)
class BreakingStoryCandidate:
    topic_key: str
    taxonomy_key: str
    headline: str
    text: str
    source: str
    timestamp: int
    facts: BreakingStoryFacts


@dataclass(frozen=True)
class BreakingStoryResolution:
    decision: str
    cluster: Mapping[str, object] | None
    cluster_facts: BreakingStoryFacts | None
    score: float
    reason: str
    should_edit_root: bool = False
    mismatch_reason: str = ""


def _clean_text(value: str) -> str:
    return normalize_space(strip_telegram_html(str(value or "")))


def _sanitize_topic_key(value: str) -> str:
    normalized = normalize_taxonomy_text(value).replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized[:80]


def _extract_numbers(text: str) -> tuple[str, ...]:
    normalized = normalize_taxonomy_text(text)
    values: set[str] = set(_DIGIT_RE.findall(normalized))
    for token in _NUMBER_WORD_RE.findall(normalized):
        mapped = _NUMBER_WORDS.get(str(token).lower())
        if mapped:
            values.add(mapped)
    return tuple(sorted(values, key=lambda item: (len(item), item)))


def _extract_keyword_numbers(text: str, markers: Sequence[str]) -> tuple[str, ...]:
    normalized = normalize_taxonomy_text(text)
    found: set[str] = set()
    for marker in markers:
        escaped = re.escape(marker)
        patterns = (
            re.compile(rf"\b(\d+)\s+{escaped}\b"),
            re.compile(rf"\b{escaped}\s+(\d+)\b"),
            re.compile(rf"\b({_NUMBER_WORD_PATTERN})\s+{escaped}\b", flags=re.IGNORECASE),
            re.compile(rf"\b{escaped}\s+({_NUMBER_WORD_PATTERN})\b", flags=re.IGNORECASE),
        )
        for pattern in patterns:
            for match in pattern.findall(normalized):
                token = str(match).lower()
                found.add(_NUMBER_WORDS.get(token, token))
    return tuple(sorted(found, key=lambda item: (len(item), item)))


def _clean_location_candidate(value: str) -> str:
    cleaned = normalize_taxonomy_text(value)
    cleaned = re.sub(r"^(?:the|a)\s+", "", cleaned)
    parts = [part for part in cleaned.split() if part]
    while parts and parts[-1] in _TRAILING_FILLER_WORDS:
        parts.pop()
    while parts and parts[-1] in _GENERIC_LOCATION_WORDS:
        parts.pop()
    if not parts:
        return ""
    cleaned = " ".join(parts)
    if len(cleaned) < 3:
        return ""
    if cleaned in {"the", "area", "city"}:
        return ""
    return cleaned


def _extract_locations(headline: str, text: str) -> tuple[str, ...]:
    found: list[str] = []
    seen: set[str] = set()
    raw = " ".join(part.replace("\n", " ") for part in (headline, text) if part).strip()
    raw = re.sub(r"[!?;,]+", " ", raw)
    lower_source = normalize_taxonomy_text(raw)
    for pattern, source in ((_TITLE_LOCATION_RE, raw), (_LOWER_LOCATION_RE, lower_source)):
        for match in pattern.findall(source):
            cleaned = _clean_location_candidate(str(match))
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            found.append(cleaned)
    return tuple(found[:6])


def _extract_actors(text: str) -> tuple[str, ...]:
    normalized = normalize_taxonomy_text(text)
    found: list[str] = []
    for canonical, patterns in _ACTOR_PATTERNS.items():
        for phrase in patterns:
            if re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", normalized):
                found.append(canonical)
                break
    return tuple(found)


def _clean_target_candidate(value: str) -> str:
    cleaned = normalize_taxonomy_text(value)
    cleaned = re.sub(r"^(?:a|an|the)\s+", "", cleaned)
    cleaned = re.sub(r"\b(?:according|after|amid|as|at|for|from|in|near|on|with)\b.*$", "", cleaned)
    cleaned = normalize_space(cleaned)
    if len(cleaned) < 4:
        return ""
    return cleaned


def _extract_targets(text: str) -> tuple[str, ...]:
    found: list[str] = []
    seen: set[str] = set()
    for match in _TARGET_RE.findall(text):
        cleaned = _clean_target_candidate(str(match))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        found.append(cleaned)
    return tuple(found[:4])


def _extract_phases(text: str) -> tuple[str, ...]:
    normalized = normalize_taxonomy_text(text)
    found: list[str] = []
    for phase, markers in _PHASE_MARKERS.items():
        if any(re.search(rf"(?<![a-z0-9]){re.escape(marker)}(?![a-z0-9])", normalized) for marker in markers):
            found.append(phase)
    return tuple(found)


def _has_official_confirmation(text: str) -> bool:
    normalized = normalize_taxonomy_text(text)
    return any(re.search(rf"(?<![a-z0-9]){re.escape(marker)}(?![a-z0-9])", normalized) for marker in _OFFICIAL_MARKERS)


def _derive_topic_key(headline: str, text: str, taxonomy_key: str) -> str:
    normalized, _text_hash = build_dupe_fingerprint("\n".join(part for part in (headline, text) if part))
    tokens = [token for token in re.findall(r"[a-z0-9]+", normalized) if token not in _TOPIC_STOPWORDS]
    pieces: list[str] = []
    if taxonomy_key:
        pieces.append(taxonomy_key)
    pieces.extend(tokens[:5])
    return _sanitize_topic_key("_".join(pieces)) or taxonomy_key or "breaking_update"


def extract_breaking_story_facts(*, headline: str, text: str) -> BreakingStoryFacts:
    combined = "\n".join(part for part in (_clean_text(headline), _clean_text(text)) if part).strip()
    normalized_text, text_hash = build_dupe_fingerprint(combined)
    return BreakingStoryFacts(
        normalized_text=normalized_text,
        text_hash=text_hash,
        numbers=_extract_numbers(combined),
        casualty_numbers=_extract_keyword_numbers(combined, _CASUALTY_MARKERS),
        event_numbers=_extract_keyword_numbers(combined, _EVENT_COUNT_MARKERS),
        locations=_extract_locations(headline, text),
        actors=_extract_actors(combined),
        targets=_extract_targets(combined),
        phases=_extract_phases(combined),
        official_confirmation=_has_official_confirmation(combined),
    )


def build_breaking_story_candidate(
    *,
    text: str,
    headline: str,
    topic_key: str = "",
    source: str = "",
    timestamp: int | None = None,
    taxonomy_key: str = "",
) -> BreakingStoryCandidate:
    cleaned_headline = _clean_text(headline)
    cleaned_text = _clean_text(text)
    resolved_taxonomy = _sanitize_topic_key(taxonomy_key)
    if not resolved_taxonomy:
        match = match_breaking_category("\n".join(part for part in (cleaned_headline, cleaned_text) if part))
        if match is not None:
            resolved_taxonomy = match.category_key
    facts = extract_breaking_story_facts(headline=cleaned_headline, text=cleaned_text)
    resolved_topic = _sanitize_topic_key(topic_key) or _derive_topic_key(cleaned_headline, cleaned_text, resolved_taxonomy)
    return BreakingStoryCandidate(
        topic_key=resolved_topic,
        taxonomy_key=resolved_taxonomy,
        headline=cleaned_headline,
        text=cleaned_text,
        source=normalize_space(source),
        timestamp=int(timestamp if timestamp is not None else time.time()),
        facts=facts,
    )


def serialize_breaking_story_facts(facts: BreakingStoryFacts) -> str:
    payload = {
        "normalized_text": facts.normalized_text,
        "text_hash": facts.text_hash,
        "numbers": list(facts.numbers),
        "casualty_numbers": list(facts.casualty_numbers),
        "event_numbers": list(facts.event_numbers),
        "locations": list(facts.locations),
        "actors": list(facts.actors),
        "targets": list(facts.targets),
        "phases": list(facts.phases),
        "official_confirmation": bool(facts.official_confirmation),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def deserialize_breaking_story_facts(value: object) -> BreakingStoryFacts:
    payload: Mapping[str, object]
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except Exception:
            loaded = {}
        payload = loaded if isinstance(loaded, dict) else {}
    elif isinstance(value, Mapping):
        payload = value
    else:
        payload = {}
    return BreakingStoryFacts(
        normalized_text=normalize_space(str(payload.get("normalized_text") or "")),
        text_hash=normalize_space(str(payload.get("text_hash") or "")),
        numbers=tuple(str(item) for item in (payload.get("numbers") or []) if str(item)),
        casualty_numbers=tuple(str(item) for item in (payload.get("casualty_numbers") or []) if str(item)),
        event_numbers=tuple(str(item) for item in (payload.get("event_numbers") or []) if str(item)),
        locations=tuple(str(item) for item in (payload.get("locations") or []) if str(item)),
        actors=tuple(str(item) for item in (payload.get("actors") or []) if str(item)),
        targets=tuple(str(item) for item in (payload.get("targets") or []) if str(item)),
        phases=tuple(str(item) for item in (payload.get("phases") or []) if str(item)),
        official_confirmation=bool(payload.get("official_confirmation")),
    )


def compute_breaking_story_cluster_key(candidate: BreakingStoryCandidate) -> str:
    seed_parts = [
        candidate.topic_key,
        candidate.taxonomy_key,
        "|".join(candidate.facts.locations),
        "|".join(candidate.facts.actors),
        candidate.facts.text_hash,
        str(candidate.timestamp),
    ]
    return hashlib.sha1("::".join(seed_parts).encode("utf-8")).hexdigest()


def _headline_quality_score(headline: str) -> float:
    text = _clean_text(headline)
    if not text:
        return -100.0
    lowered = text.lower()
    score = 0.0
    if 35 <= len(text) <= 160:
        score += 5.0
    elif len(text) < 28:
        score -= 2.0
    else:
        score -= min(6.0, (len(text) - 160) / 25.0)
    score -= float(text.count("?")) * 3.0
    score -= float(text.count('"')) * 0.5
    score -= float(text.count(":")) * 0.5
    if lowered.startswith(("flash update", "live update", "breaking", "alert", "why ")):
        score -= 4.0
    if any(marker in lowered for marker in ("reportedly", "what explains", "reasons for", "ways to address")):
        score -= 3.0
    return score


def _set_overlap(left: Sequence[str], right: Sequence[str]) -> int:
    return len(set(left) & set(right))


def _normalized_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _cluster_match_score(
    candidate: BreakingStoryCandidate,
    cluster: Mapping[str, object],
    cluster_facts: BreakingStoryFacts,
) -> tuple[float, bool, str]:
    cluster_topic = _sanitize_topic_key(str(cluster.get("topic_key") or ""))
    cluster_taxonomy = _sanitize_topic_key(str(cluster.get("taxonomy_key") or ""))
    topic_match = bool(candidate.topic_key and cluster_topic and candidate.topic_key == cluster_topic)
    taxonomy_match = bool(candidate.taxonomy_key and cluster_taxonomy and candidate.taxonomy_key == cluster_taxonomy)
    actor_overlap = _set_overlap(candidate.facts.actors, cluster_facts.actors)
    location_overlap = _set_overlap(candidate.facts.locations, cluster_facts.locations)
    target_overlap = _set_overlap(candidate.facts.targets, cluster_facts.targets)
    phase_overlap = _set_overlap(candidate.facts.phases, cluster_facts.phases)
    similarity = _normalized_similarity(candidate.facts.normalized_text, cluster_facts.normalized_text)

    location_conflict = bool(candidate.facts.locations and cluster_facts.locations and not location_overlap)
    actor_conflict = bool(candidate.facts.actors and cluster_facts.actors and not actor_overlap)
    hard_conflict = False
    conflict_reason = ""
    if location_conflict and actor_conflict and similarity < 0.72:
        hard_conflict = True
        conflict_reason = "actor_location_conflict"
    elif actor_conflict and not location_overlap and similarity < 0.65:
        hard_conflict = True
        conflict_reason = "actor_conflict"
    elif location_conflict and not actor_overlap and similarity < 0.65:
        hard_conflict = True
        conflict_reason = "location_conflict"
    elif (
        candidate.taxonomy_key
        and cluster_taxonomy
        and candidate.taxonomy_key != cluster_taxonomy
        and not actor_overlap
        and not location_overlap
        and similarity < 0.58
    ):
        hard_conflict = True
        conflict_reason = "taxonomy_conflict"

    score = 0.0
    if topic_match:
        score += 50.0
    if taxonomy_match:
        score += 22.0
    score += min(24.0, float(actor_overlap) * 12.0)
    score += min(24.0, float(location_overlap) * 12.0)
    score += min(8.0, float(target_overlap) * 8.0)
    if phase_overlap:
        score += 6.0
    score += similarity * 20.0
    if candidate.facts.official_confirmation and cluster_facts.official_confirmation:
        score += 4.0
    return score, hard_conflict, conflict_reason


def _counts_changed(current: Sequence[str], previous: Sequence[str]) -> bool:
    current_set = set(current)
    previous_set = set(previous)
    if not current_set:
        return False
    return current_set != previous_set


def _has_material_delta(candidate: BreakingStoryCandidate, cluster_facts: BreakingStoryFacts) -> tuple[bool, str]:
    if _counts_changed(candidate.facts.casualty_numbers, cluster_facts.casualty_numbers):
        return True, "casualty_change"
    if _counts_changed(candidate.facts.event_numbers, cluster_facts.event_numbers):
        return True, "event_count_change"
    if (
        candidate.facts.locations
        and cluster_facts.locations
        and not (set(candidate.facts.locations) & set(cluster_facts.locations))
    ):
        return True, "location_change"
    if (
        candidate.facts.actors
        and cluster_facts.actors
        and not (set(candidate.facts.actors) & set(cluster_facts.actors))
    ):
        return True, "actor_change"
    if (
        candidate.facts.targets
        and cluster_facts.targets
        and not (set(candidate.facts.targets) & set(cluster_facts.targets))
    ):
        return True, "target_change"
    if candidate.facts.official_confirmation and not cluster_facts.official_confirmation:
        return True, "official_confirmation"
    new_phases = set(candidate.facts.phases) - set(cluster_facts.phases)
    if new_phases and (set(candidate.facts.locations) & set(cluster_facts.locations) or set(candidate.facts.actors) & set(cluster_facts.actors)):
        return True, "phase_change"
    return False, ""


def resolve_breaking_story_cluster(
    candidate: BreakingStoryCandidate,
    clusters: Sequence[Mapping[str, object]],
    *,
    now_ts: int | None = None,
    burst_seconds: int = 60,
) -> BreakingStoryResolution:
    resolved_now = int(now_ts if now_ts is not None else time.time())
    best_cluster: Mapping[str, object] | None = None
    best_facts: BreakingStoryFacts | None = None
    best_score = 0.0
    best_reason = ""
    best_mismatch_reason = ""
    best_mismatch_score = 0.0

    for cluster in clusters:
        cluster_facts = deserialize_breaking_story_facts(cluster.get("current_facts_json"))
        score, conflict, reason = _cluster_match_score(candidate, cluster, cluster_facts)
        if conflict:
            if score > best_mismatch_score:
                best_mismatch_score = score
                best_mismatch_reason = reason
            continue
        if score > best_score:
            best_score = score
            best_cluster = cluster
            best_facts = cluster_facts
            best_reason = reason or "matched"

    if best_cluster is None or best_facts is None or best_score < 48.0:
        return BreakingStoryResolution(
            decision="new_story",
            cluster=None,
            cluster_facts=None,
            score=best_score,
            reason="no_story_cluster_match",
            mismatch_reason=best_mismatch_reason,
        )

    material, material_reason = _has_material_delta(candidate, best_facts)
    if material:
        return BreakingStoryResolution(
            decision="material_update",
            cluster=best_cluster,
            cluster_facts=best_facts,
            score=best_score,
            reason=material_reason,
        )

    similarity = _normalized_similarity(candidate.facts.normalized_text, best_facts.normalized_text)
    cluster_headline = _clean_text(str(best_cluster.get("current_headline") or ""))
    cluster_update_count = int(best_cluster.get("update_count") or 0)
    within_burst = (resolved_now - int(best_cluster.get("opened_ts") or resolved_now)) <= max(1, int(burst_seconds))
    if candidate.headline and cluster_headline:
        candidate_quality = _headline_quality_score(candidate.headline)
        current_quality = _headline_quality_score(cluster_headline)
        if cluster_update_count == 0 and candidate_quality > (current_quality + 1.0) and similarity >= 0.35:
            return BreakingStoryResolution(
                decision="minor_refinement",
                cluster=best_cluster,
                cluster_facts=best_facts,
                score=best_score,
                reason="cleaner_headline",
                should_edit_root=bool(within_burst or similarity >= 0.5),
            )

    return BreakingStoryResolution(
        decision="duplicate_echo",
        cluster=best_cluster,
        cluster_facts=best_facts,
        score=best_score,
        reason="same_story_echo",
    )
