from __future__ import annotations

import json
from collections import Counter, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import re
import threading
import time
from typing import Pattern


_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2}
_PUNCT_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
    }
)
_DEFAULT_TAXONOMY_PATH = Path(__file__).with_name("news_taxonomy.json")
_HEALTH_WINDOW_SECONDS = 6 * 60 * 60
_DEFAULT_LIVE_EVENT_SCORE = 1.6
_DIGIT_RE = re.compile(r"\b\d+(?:[.,:/-]\d+)*\b")
_QUESTION_LEAD_RE = re.compile(
    r"^(?:why|how|what explains|what caused|what does|can|could|should|would|will)\b",
    flags=re.IGNORECASE,
)
_LOCATION_CUE_RE = re.compile(
    r"\b(?:in|at|near|over|from|toward|towards|into|across|outside|inside|around|off|south of|north of|east of|west of)\s+"
    r"([a-z][a-z0-9.-]*(?:\s+[a-z][a-z0-9.-]*){0,2})\b"
)
_STOPWORD_TOKENS = {
    "a",
    "an",
    "and",
    "alert",
    "after",
    "agreement",
    "all",
    "among",
    "around",
    "at",
    "be",
    "block",
    "breach",
    "change",
    "continued",
    "deal",
    "event",
    "fire",
    "for",
    "from",
    "high",
    "in",
    "incident",
    "into",
    "low",
    "medium",
    "news",
    "of",
    "on",
    "ops",
    "probe",
    "report",
    "ruling",
    "security",
    "state",
    "talks",
    "the",
    "to",
    "update",
    "watch",
}
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
}
_DEFAULT_COMPILER = {
    "action_aliases": {
        "strike": [
            "strike",
            "strikes",
            "struck",
            "air strike",
            "airstrike",
            "air-strike",
            "bombardment",
            "bombing",
            "raid",
            "raids",
            "shelling",
        ],
        "intercept": [
            "intercept",
            "intercepts",
            "intercepted",
            "interception",
            "interceptions",
            "shot down",
            "downed",
            "neutralized",
        ],
        "launch": [
            "launch",
            "launches",
            "launched",
            "launching",
            "rocket fire",
            "fired",
            "barrage",
            "salvo",
            "volley",
        ],
        "impact": [
            "impact",
            "impacts",
            "hit",
            "hits",
            "landed",
            "lands",
            "crashed",
            "exploded",
            "explosion",
            "blast",
        ],
        "warn": [
            "warning",
            "warnings",
            "warned",
            "sirens",
            "alert",
            "alerts",
            "evacuate",
            "evacuation",
            "shelter",
        ],
        "kill": [
            "killed",
            "dead",
            "deaths",
            "fatality",
            "fatalities",
            "casualty",
            "casualties",
            "injured",
            "wounded",
        ],
        "confirm": [
            "confirmed",
            "announced",
            "declared",
            "ordered",
            "issued",
            "approved",
            "warned",
            "statement",
            "communique",
        ],
        "resign": [
            "resign",
            "resigns",
            "resigned",
            "steps down",
            "step down",
            "ousted",
            "sacked",
            "dismissed",
        ],
        "protest": [
            "protest",
            "protests",
            "unrest",
            "demonstration",
            "demonstrations",
            "riot",
            "riots",
            "clashes",
        ],
        "disrupt": [
            "outage",
            "blackout",
            "shutdown",
            "disruption",
            "disruptions",
            "network disruption",
            "internet down",
            "power outage",
        ],
    },
    "actor_aliases": {
        "officials": [
            "officials",
            "authorities",
            "government",
            "ministry",
            "military",
            "army",
            "defense ministry",
            "defence ministry",
            "spokesperson",
            "spokesman",
            "spokeswoman",
        ],
        "united states": [
            "united states",
            "u.s.",
            "u.s",
            "us",
            "american",
            "america",
            "washington",
        ],
        "israel": ["israel", "israeli", "idf", "israeli army", "israeli military"],
        "iran": ["iran", "iranian", "tehran", "irgc"],
        "hezbollah": ["hezbollah", "islamic resistance"],
        "russia": ["russia", "russian", "moscow"],
        "ukraine": ["ukraine", "ukrainian", "kyiv"],
    },
    "target_aliases": {
        "air defense": [
            "air defense",
            "air defence",
            "air-defense",
            "iron dome",
            "surface to air",
            "surface-to-air",
            "anti-aircraft",
            "missile defense",
            "missile-defence",
        ],
        "missile": ["missile", "missiles", "ballistic missile", "cruise missile", "rocket", "rockets"],
        "drone": [
            "drone",
            "drones",
            "uav",
            "uavs",
            "unmanned aerial vehicle",
            "attack drone",
            "loitering munition",
        ],
        "shipping": [
            "ship",
            "ships",
            "shipping",
            "merchant vessel",
            "cargo ship",
            "tanker",
            "port",
            "harbor",
            "harbour",
        ],
        "grid": ["grid", "power grid", "electricity", "substation", "telecom", "network", "internet"],
    },
    "recency_cues": [
        "today",
        "tonight",
        "overnight",
        "this morning",
        "this afternoon",
        "this evening",
        "just now",
        "moments ago",
        "minutes ago",
        "hours ago",
        "latest",
        "new",
        "earlier today",
        "happening now",
        "live",
    ],
    "official_source_cues": [
        "officials said",
        "officials say",
        "officials confirmed",
        "official statement",
        "the ministry said",
        "military said",
        "the army said",
        "the president said",
        "prime minister said",
        "government announced",
        "confirmed",
        "announced",
        "declared",
        "ordered",
        "approved",
        "issued",
        "statement",
        "communique",
    ],
    "negative_framing_markers": [
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
    ],
    "hedge_markers": [
        "reportedly",
        "unconfirmed",
        "appears to",
        "appeared to",
        "may have",
        "might have",
        "could have",
        "according to reports",
        "report says",
        "reports say",
        "alleged",
        "allegedly",
    ],
}


class NewsTaxonomyError(ValueError):
    """Raised when the taxonomy file is missing or invalid."""


@dataclass(frozen=True)
class OntologyCompilerConfig:
    action_aliases: tuple[tuple[str, tuple[str, ...]], ...]
    actor_aliases: tuple[tuple[str, tuple[str, ...]], ...]
    target_aliases: tuple[tuple[str, tuple[str, ...]], ...]
    recency_cues: tuple[str, ...]
    official_source_cues: tuple[str, ...]
    negative_framing_markers: tuple[str, ...]
    hedge_markers: tuple[str, ...]
    _action_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _actor_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _target_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _recency_patterns: tuple[tuple[str, Pattern[str]], ...]
    _official_patterns: tuple[tuple[str, Pattern[str]], ...]
    _negative_patterns: tuple[tuple[str, Pattern[str]], ...]
    _hedge_patterns: tuple[tuple[str, Pattern[str]], ...]


@dataclass(frozen=True)
class NewsCategory:
    key: str
    label: str
    priority: int
    domain: str
    breaking_eligible: bool
    severity_bias: str
    primary_phrases: tuple[str, ...]
    alias_phrases: tuple[str, ...]
    required_any: tuple[str, ...]
    blocked_phrases: tuple[str, ...]
    event_nouns: tuple[str, ...]
    action_verbs: tuple[str, ...]
    actor_aliases: tuple[str, ...]
    target_nouns: tuple[str, ...]
    recency_cues: tuple[str, ...]
    official_source_cues: tuple[str, ...]
    negative_framing_markers: tuple[str, ...]
    _primary_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _alias_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _required_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _blocked_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _event_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _action_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _actor_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _target_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _recency_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _official_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]
    _negative_patterns: tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]


@dataclass(frozen=True)
class EventFrame:
    actors: tuple[str, ...]
    actions: tuple[str, ...]
    targets: tuple[str, ...]
    places: tuple[str, ...]
    counts: tuple[str, ...]
    recency_hits: tuple[str, ...]
    official_hits: tuple[str, ...]
    negative_hits: tuple[str, ...]
    hedge_hits: tuple[str, ...]

    @property
    def hedged(self) -> bool:
        return bool(self.hedge_hits)

    @property
    def confirmation_state(self) -> str:
        if self.official_hits and not self.hedged:
            return "confirmed"
        if self.hedged:
            return "hedged"
        return "unknown"


@dataclass(frozen=True)
class OntologySignalBreakdown:
    event_hit_count: int
    action_hit_count: int
    actor_hit_count: int
    target_hit_count: int
    place_hit_count: int
    recency_hit_count: int
    official_hit_count: int
    negative_hit_count: int
    hedge_hit_count: int
    live_event_score: float
    recency_score: float
    official_score: float
    explainer_score: float


@dataclass(frozen=True)
class OntologyMatch:
    category_key: str
    label: str
    confidence_score: float
    matched_primary: tuple[str, ...]
    matched_aliases: tuple[str, ...]
    breaking_eligible: bool
    severity_bias: str
    priority: int
    matched_required: tuple[str, ...] = ()
    event_hits: tuple[str, ...] = ()
    action_hits: tuple[str, ...] = ()
    actor_hits: tuple[str, ...] = ()
    target_hits: tuple[str, ...] = ()
    recency_hits: tuple[str, ...] = ()
    official_hits: tuple[str, ...] = ()
    negative_hits: tuple[str, ...] = ()
    rescue_applied: bool = False
    event_frame: EventFrame | None = None
    _stable_order: int = 0


NewsCategoryMatch = OntologyMatch


@dataclass(frozen=True)
class NewsTaxonomy:
    version: int
    path: Path
    categories: tuple[NewsCategory, ...]
    compiler: OntologyCompilerConfig


@dataclass(frozen=True)
class OntologyAnalysis:
    normalized_text: str
    top_match: OntologyMatch | None
    matches: tuple[OntologyMatch, ...]
    event_frame: EventFrame
    signal_breakdown: OntologySignalBreakdown


_TAXONOMY_CACHE: NewsTaxonomy | None = None
_HEALTH_LOCK = threading.Lock()
_LABEL_RESOLUTION_WINDOW: deque[tuple[float, bool]] = deque(maxlen=4096)
_LIVE_EVENT_RESOLUTION_WINDOW: deque[tuple[float, bool, tuple[str, ...]]] = deque(maxlen=4096)


def normalize_taxonomy_text(text: str) -> str:
    normalized = str(text or "").translate(_PUNCT_TRANSLATION).casefold()
    normalized = re.sub(r"[()\[\]{}|]", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _phrase_pattern(phrase: str) -> Pattern[str]:
    escaped = re.escape(phrase)
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])")


def _normalize_phrase_list(values: Iterable[object], field_name: str, *, key: str) -> tuple[str, ...]:
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
        raise NewsTaxonomyError(f"Category '{key}' field '{field_name}' must be a list of phrases.")
    phrases: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            raise NewsTaxonomyError(f"Category '{key}' field '{field_name}' contains a non-string phrase.")
        normalized = normalize_taxonomy_text(raw)
        if not normalized:
            raise NewsTaxonomyError(f"Category '{key}' field '{field_name}' contains an empty phrase.")
        if normalized in seen:
            raise NewsTaxonomyError(
                f"Category '{key}' field '{field_name}' contains duplicate phrase '{normalized}'."
            )
        seen.add(normalized)
        phrases.append(normalized)
    return tuple(phrases)


def _coerce_bool(value: object, *, key: str, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise NewsTaxonomyError(f"Category '{key}' field '{field_name}' must be true or false.")


def _simple_plural_variants(token: str) -> set[str]:
    variants = {token}
    if len(token) < 3:
        return variants
    if token.endswith("ies") and len(token) > 4:
        variants.add(token[:-3] + "y")
    elif token.endswith("es") and len(token) > 4:
        variants.add(token[:-2])
    elif token.endswith("s") and not token.endswith("ss") and len(token) > 4:
        variants.add(token[:-1])
    else:
        variants.add(token + "s")
        if token.endswith(("ch", "sh", "x", "z", "s")):
            variants.add(token + "es")
        if token.endswith("y") and len(token) > 3 and token[-2] not in {"a", "e", "i", "o", "u"}:
            variants.add(token[:-1] + "ies")
    return {variant for variant in variants if variant}


def _generate_phrase_variants(phrase: str) -> tuple[str, ...]:
    normalized = normalize_taxonomy_text(phrase)
    if not normalized:
        return ()
    tokens = tuple(token for token in normalized.replace("-", " ").split() if token)
    if not tokens:
        return ()
    variants: set[str] = {" ".join(tokens)}
    if len(tokens) > 1:
        variants.add("-".join(tokens))
        variants.add("".join(tokens))
    final_variants = _simple_plural_variants(tokens[-1])
    prefix = tokens[:-1]
    for final in final_variants:
        joined = prefix + (final,)
        variants.add(" ".join(joined))
        if len(joined) > 1:
            variants.add("-".join(joined))
            variants.add("".join(joined))
    return tuple(sorted(variants, key=lambda item: (-len(item), item)))


def _phrases_are_equivalent(left: str, right: str) -> bool:
    left_joined = normalize_taxonomy_text(left).replace("-", " ").replace(" ", "")
    right_joined = normalize_taxonomy_text(right).replace("-", " ").replace(" ", "")
    if left_joined == right_joined:
        return True
    left_tokens = normalize_taxonomy_text(left).replace("-", " ").split()
    right_tokens = normalize_taxonomy_text(right).replace("-", " ").split()
    if len(left_tokens) != len(right_tokens):
        return False
    for left_token, right_token in zip(left_tokens, right_tokens):
        if left_token == right_token:
            continue
        if right_token not in _simple_plural_variants(left_token) and left_token not in _simple_plural_variants(right_token):
            return False
    return True


def _compile_phrase_field(phrases: Sequence[str], *, key: str, field_name: str) -> tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]:
    compiled: list[tuple[str, tuple[tuple[str, Pattern[str]], ...]]] = []
    variant_owner: dict[str, str] = {}
    for phrase in phrases:
        variants = _generate_phrase_variants(phrase)
        if not variants:
            continue
        compiled_variants: list[tuple[str, Pattern[str]]] = []
        for variant in variants:
            owner = variant_owner.get(variant)
            if owner is not None and owner != phrase and not _phrases_are_equivalent(owner, phrase):
                raise NewsTaxonomyError(
                    f"Category '{key}' field '{field_name}' generates conflicting variant '{variant}' from '{owner}' and '{phrase}'."
                )
            variant_owner[variant] = owner or phrase
            compiled_variants.append((variant, _phrase_pattern(variant)))
        compiled.append((phrase, tuple(compiled_variants)))
    return tuple(compiled)


def _normalize_group_map(payload: object, field_name: str) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if payload is None:
        return ()
    if not isinstance(payload, Mapping):
        raise NewsTaxonomyError(f"News taxonomy compiler field '{field_name}' must be an object.")
    entries: list[tuple[str, tuple[str, ...]]] = []
    seen_keys: set[str] = set()
    for raw_key, raw_values in payload.items():
        canonical = normalize_taxonomy_text(str(raw_key or ""))
        if not canonical:
            raise NewsTaxonomyError(f"News taxonomy compiler field '{field_name}' contains an empty key.")
        if canonical in seen_keys:
            raise NewsTaxonomyError(f"News taxonomy compiler field '{field_name}' duplicates key '{canonical}'.")
        phrases = _normalize_phrase_list(raw_values, field_name, key=canonical)
        if not phrases:
            raise NewsTaxonomyError(
                f"News taxonomy compiler field '{field_name}' key '{canonical}' needs at least one phrase."
            )
        seen_keys.add(canonical)
        entries.append((canonical, phrases))
    return tuple(entries)


def _compile_group_patterns(
    entries: Sequence[tuple[str, Sequence[str]]],
    *,
    field_name: str,
) -> tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]:
    compiled: list[tuple[str, tuple[tuple[str, Pattern[str]], ...]]] = []
    for canonical, phrases in entries:
        phrase_pool: list[str] = [canonical, *phrases]
        variants: list[tuple[str, Pattern[str]]] = []
        seen: set[str] = set()
        for phrase in phrase_pool:
            for variant in _generate_phrase_variants(phrase):
                if variant in seen:
                    continue
                seen.add(variant)
                variants.append((variant, _phrase_pattern(variant)))
        if not variants:
            raise NewsTaxonomyError(
                f"News taxonomy compiler field '{field_name}' key '{canonical}' compiled no usable variants."
            )
        compiled.append((canonical, tuple(variants)))
    return tuple(compiled)


def _normalize_optional_category_phrases(payload: object, *, key: str, field_name: str) -> tuple[str, ...]:
    if payload is None:
        return ()
    return _normalize_phrase_list(payload, field_name, key=key)


def _derive_key_terms(key: str, label: str) -> tuple[str, ...]:
    values: list[str] = []
    for raw in [*key.split("_"), *normalize_taxonomy_text(label).split()]:
        token = raw.strip()
        if not token or token in _STOPWORD_TOKENS or token.isdigit() or not re.search(r"[a-z0-9]", token):
            continue
        if token not in values:
            values.append(token)
    return tuple(values)


def _derive_group_hits(
    phrases: Sequence[str],
    groups: Sequence[tuple[str, tuple[tuple[str, Pattern[str]], ...]]],
) -> tuple[str, ...]:
    haystack = " ".join(phrases)
    hits: list[str] = []
    for canonical, patterns in groups:
        if any(pattern.search(haystack) for _variant, pattern in patterns):
            hits.append(canonical)
    return tuple(hits)


def _select_group_patterns(
    keys: Sequence[str],
    groups: Sequence[tuple[str, tuple[tuple[str, Pattern[str]], ...]]],
) -> tuple[tuple[str, tuple[tuple[str, Pattern[str]], ...]], ...]:
    wanted = set(keys)
    selected: list[tuple[str, tuple[tuple[str, Pattern[str]], ...]]] = []
    for canonical, patterns in groups:
        if canonical in wanted:
            selected.append((canonical, patterns))
    return tuple(selected)


def _load_compiler_config(payload: object) -> OntologyCompilerConfig:
    raw = dict(_DEFAULT_COMPILER)
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            raw[str(key)] = value
    elif payload is not None:
        raise NewsTaxonomyError("News taxonomy 'compiler' field must be an object when provided.")

    action_aliases = _normalize_group_map(raw.get("action_aliases"), "action_aliases")
    actor_aliases = _normalize_group_map(raw.get("actor_aliases"), "actor_aliases")
    target_aliases = _normalize_group_map(raw.get("target_aliases"), "target_aliases")
    recency_cues = _normalize_phrase_list(raw.get("recency_cues", ()), "recency_cues", key="compiler")
    official_source_cues = _normalize_phrase_list(
        raw.get("official_source_cues", ()),
        "official_source_cues",
        key="compiler",
    )
    negative_framing_markers = _normalize_phrase_list(
        raw.get("negative_framing_markers", ()),
        "negative_framing_markers",
        key="compiler",
    )
    hedge_markers = _normalize_phrase_list(raw.get("hedge_markers", ()), "hedge_markers", key="compiler")

    return OntologyCompilerConfig(
        action_aliases=action_aliases,
        actor_aliases=actor_aliases,
        target_aliases=target_aliases,
        recency_cues=recency_cues,
        official_source_cues=official_source_cues,
        negative_framing_markers=negative_framing_markers,
        hedge_markers=hedge_markers,
        _action_patterns=_compile_group_patterns(action_aliases, field_name="action_aliases"),
        _actor_patterns=_compile_group_patterns(actor_aliases, field_name="actor_aliases"),
        _target_patterns=_compile_group_patterns(target_aliases, field_name="target_aliases"),
        _recency_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in recency_cues),
        _official_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in official_source_cues),
        _negative_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in negative_framing_markers),
        _hedge_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in hedge_markers),
    )


def _build_category(payload: object, *, compiler: OntologyCompilerConfig) -> NewsCategory:
    if not isinstance(payload, dict):
        raise NewsTaxonomyError("Each taxonomy category entry must be an object.")
    key = normalize_taxonomy_text(str(payload.get("key") or ""))
    if not key or not re.fullmatch(r"[a-z0-9_]+", key):
        raise NewsTaxonomyError("Every taxonomy category needs a snake_case 'key'.")

    label = str(payload.get("label") or "").strip()
    if not label:
        raise NewsTaxonomyError(f"Category '{key}' is missing a non-empty 'label'.")

    try:
        priority = int(payload.get("priority"))
    except Exception as exc:
        raise NewsTaxonomyError(f"Category '{key}' field 'priority' must be an integer.") from exc

    domain = normalize_taxonomy_text(str(payload.get("domain") or ""))
    if not domain:
        raise NewsTaxonomyError(f"Category '{key}' is missing a non-empty 'domain'.")

    severity_bias = normalize_taxonomy_text(str(payload.get("severity_bias") or ""))
    if severity_bias not in _SEVERITY_ORDER:
        raise NewsTaxonomyError(
            f"Category '{key}' field 'severity_bias' must be one of high, medium, low."
        )

    breaking_eligible = _coerce_bool(payload.get("breaking_eligible"), key=key, field_name="breaking_eligible")
    primary_phrases = _normalize_phrase_list(payload.get("primary_phrases", []), "primary_phrases", key=key)
    if not primary_phrases:
        raise NewsTaxonomyError(f"Category '{key}' must define at least one primary phrase.")
    alias_phrases = _normalize_phrase_list(payload.get("alias_phrases", []), "alias_phrases", key=key)
    required_any = _normalize_phrase_list(payload.get("required_any", []), "required_any", key=key)
    blocked_phrases = _normalize_phrase_list(payload.get("blocked_phrases", []), "blocked_phrases", key=key)

    explicit_event_nouns = _normalize_optional_category_phrases(payload.get("event_nouns"), key=key, field_name="event_nouns")
    explicit_action_verbs = _normalize_optional_category_phrases(payload.get("action_verbs"), key=key, field_name="action_verbs")
    explicit_actor_aliases = _normalize_optional_category_phrases(payload.get("actor_aliases"), key=key, field_name="actor_aliases")
    explicit_target_nouns = _normalize_optional_category_phrases(payload.get("target_nouns"), key=key, field_name="target_nouns")
    explicit_recency_cues = _normalize_optional_category_phrases(payload.get("recency_cues"), key=key, field_name="recency_cues")
    explicit_official_cues = _normalize_optional_category_phrases(payload.get("official_source_cues"), key=key, field_name="official_source_cues")
    explicit_negative_markers = _normalize_optional_category_phrases(payload.get("negative_framing_markers"), key=key, field_name="negative_framing_markers")

    derived_key_terms = _derive_key_terms(key, label)
    phrase_seed = tuple(dict.fromkeys((*primary_phrases, *alias_phrases)))
    event_nouns = explicit_event_nouns or tuple(dict.fromkeys((*derived_key_terms, *primary_phrases[:3])))
    action_verbs = explicit_action_verbs or _derive_group_hits(phrase_seed, compiler._action_patterns)
    actor_aliases = explicit_actor_aliases or _derive_group_hits(phrase_seed, compiler._actor_patterns)
    target_nouns = explicit_target_nouns or tuple(
        dict.fromkeys((*_derive_group_hits(phrase_seed, compiler._target_patterns), *derived_key_terms[:3]))
    )
    recency_cues = explicit_recency_cues or compiler.recency_cues[:4]
    official_source_cues = explicit_official_cues or compiler.official_source_cues[:4]
    negative_framing_markers = explicit_negative_markers or compiler.negative_framing_markers[:4]

    return NewsCategory(
        key=key,
        label=label,
        priority=priority,
        domain=domain,
        breaking_eligible=breaking_eligible,
        severity_bias=severity_bias,
        primary_phrases=primary_phrases,
        alias_phrases=alias_phrases,
        required_any=required_any,
        blocked_phrases=blocked_phrases,
        event_nouns=event_nouns,
        action_verbs=action_verbs,
        actor_aliases=actor_aliases,
        target_nouns=target_nouns,
        recency_cues=recency_cues,
        official_source_cues=official_source_cues,
        negative_framing_markers=negative_framing_markers,
        _primary_patterns=_compile_phrase_field(primary_phrases, key=key, field_name="primary_phrases"),
        _alias_patterns=_compile_phrase_field(alias_phrases, key=key, field_name="alias_phrases"),
        _required_patterns=_compile_phrase_field(required_any, key=key, field_name="required_any"),
        _blocked_patterns=_compile_phrase_field(blocked_phrases, key=key, field_name="blocked_phrases"),
        _event_patterns=_compile_phrase_field(event_nouns, key=key, field_name="event_nouns"),
        _action_patterns=_select_group_patterns(action_verbs, compiler._action_patterns)
        or _compile_phrase_field(action_verbs, key=key, field_name="action_verbs"),
        _actor_patterns=_select_group_patterns(actor_aliases, compiler._actor_patterns)
        or _compile_phrase_field(actor_aliases, key=key, field_name="actor_aliases"),
        _target_patterns=_select_group_patterns(target_nouns, compiler._target_patterns)
        or _compile_phrase_field(target_nouns, key=key, field_name="target_nouns"),
        _recency_patterns=_compile_phrase_field(recency_cues, key=key, field_name="recency_cues"),
        _official_patterns=_compile_phrase_field(official_source_cues, key=key, field_name="official_source_cues"),
        _negative_patterns=_compile_phrase_field(negative_framing_markers, key=key, field_name="negative_framing_markers"),
    )


def load_news_taxonomy(
    path: str | Path | None = None,
    *,
    force_reload: bool = False,
) -> NewsTaxonomy:
    global _TAXONOMY_CACHE

    taxonomy_path = Path(path) if path is not None else _DEFAULT_TAXONOMY_PATH
    taxonomy_path = taxonomy_path.resolve()
    if not force_reload and _TAXONOMY_CACHE is not None and _TAXONOMY_CACHE.path == taxonomy_path:
        return _TAXONOMY_CACHE
    if not taxonomy_path.exists():
        raise NewsTaxonomyError(f"News taxonomy file not found: {taxonomy_path}")

    try:
        payload = json.loads(taxonomy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise NewsTaxonomyError(f"Invalid JSON in news taxonomy: {exc}") from exc
    if not isinstance(payload, dict):
        raise NewsTaxonomyError("News taxonomy root must be a JSON object.")

    version = payload.get("version")
    if not isinstance(version, int) or version < 1:
        raise NewsTaxonomyError("News taxonomy must define an integer version >= 1.")

    compiler = _load_compiler_config(payload.get("compiler"))
    raw_categories = payload.get("categories")
    if not isinstance(raw_categories, list) or not raw_categories:
        raise NewsTaxonomyError("News taxonomy must define a non-empty 'categories' list.")

    categories: list[NewsCategory] = []
    seen_keys: set[str] = set()
    for raw_category in raw_categories:
        category = _build_category(raw_category, compiler=compiler)
        if category.key in seen_keys:
            raise NewsTaxonomyError(f"Duplicate taxonomy category key '{category.key}'.")
        seen_keys.add(category.key)
        categories.append(category)

    taxonomy = NewsTaxonomy(version=version, path=taxonomy_path, categories=tuple(categories), compiler=compiler)
    if path is None:
        _TAXONOMY_CACHE = taxonomy
    return taxonomy


def get_news_taxonomy() -> NewsTaxonomy:
    return load_news_taxonomy()


def _match_phrase_field(
    text: str,
    compiled: Sequence[tuple[str, Sequence[tuple[str, Pattern[str]]]]],
) -> tuple[str, ...]:
    hits: list[str] = []
    for canonical, variants in compiled:
        if any(pattern.search(text) for _variant, pattern in variants):
            hits.append(canonical)
    return tuple(hits)


def _extract_places(normalized_text: str) -> tuple[str, ...]:
    places: list[str] = []
    seen: set[str] = set()
    for match in _LOCATION_CUE_RE.finditer(normalized_text):
        value = normalize_taxonomy_text(match.group(1))
        if not value or value in seen:
            continue
        tokens = value.split()
        if not tokens or any(token in _STOPWORD_TOKENS for token in tokens):
            continue
        seen.add(value)
        places.append(value)
    return tuple(places[:8])


def _extract_counts(normalized_text: str) -> tuple[str, ...]:
    counts: list[str] = []
    seen: set[str] = set()
    for match in _DIGIT_RE.findall(normalized_text):
        if match not in seen:
            seen.add(match)
            counts.append(match)
    for token in normalized_text.split():
        mapped = _NUMBER_WORD_MAP.get(token)
        if mapped and mapped not in seen:
            seen.add(mapped)
            counts.append(mapped)
    return tuple(counts[:8])


def _extract_event_frame(text: str, compiler: OntologyCompilerConfig) -> EventFrame:
    normalized = normalize_taxonomy_text(text)
    return EventFrame(
        actors=_match_phrase_field(normalized, compiler._actor_patterns),
        actions=_match_phrase_field(normalized, compiler._action_patterns),
        targets=_match_phrase_field(normalized, compiler._target_patterns),
        places=_extract_places(normalized),
        counts=_extract_counts(normalized),
        recency_hits=_match_phrase_field(normalized, tuple((item, ((item, pattern),)) for item, pattern in compiler._recency_patterns)),
        official_hits=_match_phrase_field(normalized, tuple((item, ((item, pattern),)) for item, pattern in compiler._official_patterns)),
        negative_hits=_match_phrase_field(normalized, tuple((item, ((item, pattern),)) for item, pattern in compiler._negative_patterns)),
        hedge_hits=_match_phrase_field(normalized, tuple((item, ((item, pattern),)) for item, pattern in compiler._hedge_patterns)),
    )


def _score_category(
    normalized_text: str,
    category: NewsCategory,
    *,
    frame: EventFrame,
    stable_order: int,
) -> OntologyMatch | None:
    if _match_phrase_field(normalized_text, category._blocked_patterns):
        return None

    primary_hits = _match_phrase_field(normalized_text, category._primary_patterns)
    alias_hits = _match_phrase_field(normalized_text, category._alias_patterns)
    required_hits = _match_phrase_field(normalized_text, category._required_patterns)
    event_hits = _match_phrase_field(normalized_text, category._event_patterns)
    action_hits = _match_phrase_field(normalized_text, category._action_patterns)
    actor_hits = _match_phrase_field(normalized_text, category._actor_patterns)
    target_hits = _match_phrase_field(normalized_text, category._target_patterns)
    category_recency_hits = _match_phrase_field(normalized_text, category._recency_patterns)
    category_official_hits = _match_phrase_field(normalized_text, category._official_patterns)
    category_negative_hits = _match_phrase_field(normalized_text, category._negative_patterns)

    score = (3.0 * len(primary_hits)) + (2.0 * len(alias_hits))
    if required_hits:
        score += 1.0
    score += 0.75 * min(len(event_hits), 2)
    score += 0.75 * min(len(action_hits), 2)
    score += 0.5 * min(len(actor_hits), 2)
    score += 0.5 * min(len(target_hits), 2)
    score += 0.4 * min(len(category_recency_hits or frame.recency_hits), 1)
    score += 0.4 * min(len(category_official_hits or frame.official_hits), 1)

    negative_hits = tuple(dict.fromkeys((*category_negative_hits, *frame.negative_hits)))
    if negative_hits and not primary_hits:
        score -= min(1.25, 0.5 * len(negative_hits))
    if frame.hedge_hits and not primary_hits:
        score -= 0.2

    rescue_applied = False
    if score < 3.0:
        coherent_context = (
            (len(target_hits) + len(actor_hits) + len(frame.places) + len(frame.counts) >= 1)
            or ((len(event_hits) >= 1) and bool(frame.recency_hits or frame.official_hits))
        )
        coherent_event = (len(action_hits) + len(event_hits) >= 1) and coherent_context
        if coherent_event and not negative_hits:
            score = 3.0 + min(1.25, (0.5 * len(action_hits)) + (0.5 * len(event_hits)) + (0.25 * len(target_hits)))
            rescue_applied = True
        else:
            return None

    return OntologyMatch(
        category_key=category.key,
        label=category.label,
        confidence_score=round(score, 4),
        matched_primary=primary_hits,
        matched_aliases=alias_hits,
        breaking_eligible=category.breaking_eligible,
        severity_bias=category.severity_bias,
        priority=category.priority,
        matched_required=required_hits,
        event_hits=event_hits,
        action_hits=action_hits,
        actor_hits=actor_hits,
        target_hits=target_hits,
        recency_hits=tuple(dict.fromkeys((*category_recency_hits, *frame.recency_hits))),
        official_hits=tuple(dict.fromkeys((*category_official_hits, *frame.official_hits))),
        negative_hits=negative_hits,
        rescue_applied=rescue_applied,
        event_frame=frame,
        _stable_order=stable_order,
    )


def _build_signal_breakdown(match: OntologyMatch | None, frame: EventFrame) -> OntologySignalBreakdown:
    recency_score = min(1.0, 0.45 * len(frame.recency_hits))
    official_score = min(1.0, 0.4 * len(frame.official_hits))
    explainer_score = min(2.0, 0.65 * len(frame.negative_hits))
    live_event_score = (
        min(1.2, 0.35 * len(frame.actions))
        + min(0.8, 0.2 * len(frame.targets))
        + min(0.6, 0.2 * len(frame.places))
        + min(0.5, 0.15 * len(frame.counts))
        + recency_score
        + official_score
        + (0.35 if match and match.breaking_eligible else 0.0)
        - explainer_score
        - min(0.5, 0.2 * len(frame.hedge_hits))
    )
    live_event_score = max(0.0, round(live_event_score, 4))
    return OntologySignalBreakdown(
        event_hit_count=len(match.event_hits) if match else 0,
        action_hit_count=len(frame.actions),
        actor_hit_count=len(frame.actors),
        target_hit_count=len(frame.targets),
        place_hit_count=len(frame.places),
        recency_hit_count=len(frame.recency_hits),
        official_hit_count=len(frame.official_hits),
        negative_hit_count=len(frame.negative_hits),
        hedge_hit_count=len(frame.hedge_hits),
        live_event_score=live_event_score,
        recency_score=round(recency_score, 4),
        official_score=round(official_score, 4),
        explainer_score=round(explainer_score, 4),
    )


def analyze_news_ontology(
    text: str,
    *,
    breaking_only: bool = False,
) -> OntologyAnalysis:
    normalized = normalize_taxonomy_text(text)
    if not normalized:
        empty_frame = EventFrame((), (), (), (), (), (), (), (), ())
        empty_breakdown = OntologySignalBreakdown(0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)
        return OntologyAnalysis("", None, (), empty_frame, empty_breakdown)

    taxonomy = get_news_taxonomy()
    frame = _extract_event_frame(normalized, taxonomy.compiler)
    matches: list[OntologyMatch] = []
    for order, category in enumerate(taxonomy.categories):
        if breaking_only and not category.breaking_eligible:
            continue
        match = _score_category(normalized, category, frame=frame, stable_order=order)
        if match is not None:
            matches.append(match)

    matches.sort(
        key=lambda item: (
            -item.confidence_score,
            -max((len(phrase) for phrase in item.matched_primary), default=0),
            -item.priority,
            -len(item.matched_primary),
            item._stable_order,
        )
    )
    top_match = matches[0] if matches else None
    breakdown = _build_signal_breakdown(top_match, frame)
    return OntologyAnalysis(normalized, top_match, tuple(matches), frame, breakdown)


def find_news_category_matches(
    text: str,
    *,
    breaking_only: bool = False,
) -> list[NewsCategoryMatch]:
    return list(analyze_news_ontology(text, breaking_only=breaking_only).matches)


def match_news_category(
    text: str,
    *,
    breaking_only: bool = False,
) -> NewsCategoryMatch | None:
    return analyze_news_ontology(text, breaking_only=breaking_only).top_match


def match_breaking_category(text: str) -> NewsCategoryMatch | None:
    return match_news_category(text, breaking_only=True)


def derive_taxonomy_phrases(
    *,
    breaking_only: bool = False,
    min_severity_bias: str | None = None,
) -> tuple[str, ...]:
    taxonomy = get_news_taxonomy()
    min_rank = _SEVERITY_ORDER.get(str(min_severity_bias or "").strip().lower(), -1)
    phrases: list[str] = []
    seen: set[str] = set()
    for category in taxonomy.categories:
        if breaking_only and not category.breaking_eligible:
            continue
        if min_rank >= 0 and _SEVERITY_ORDER[category.severity_bias] < min_rank:
            continue
        for phrase in (
            *category.primary_phrases,
            *category.alias_phrases,
            *category.event_nouns,
            *category.action_verbs,
            *category.target_nouns,
        ):
            if phrase in seen:
                continue
            seen.add(phrase)
            phrases.append(phrase)
    return tuple(phrases)


def record_ontology_label_resolution(*, matched: bool) -> None:
    with _HEALTH_LOCK:
        _LABEL_RESOLUTION_WINDOW.append((time.time(), bool(matched)))


def record_ontology_live_event_resolution(
    frame: EventFrame,
    *,
    matched: bool,
) -> None:
    if not frame.actions and not frame.targets and not frame.counts and not frame.places:
        return
    tokens = tuple(dict.fromkeys((*frame.actions, *frame.targets, *frame.places)))
    with _HEALTH_LOCK:
        _LIVE_EVENT_RESOLUTION_WINDOW.append((time.time(), bool(matched), tokens[:8]))


def reset_ontology_health_stats() -> None:
    with _HEALTH_LOCK:
        _LABEL_RESOLUTION_WINDOW.clear()
        _LIVE_EVENT_RESOLUTION_WINDOW.clear()


def _trim_health_windows(now_ts: float) -> None:
    cutoff = now_ts - _HEALTH_WINDOW_SECONDS
    while _LABEL_RESOLUTION_WINDOW and _LABEL_RESOLUTION_WINDOW[0][0] < cutoff:
        _LABEL_RESOLUTION_WINDOW.popleft()
    while _LIVE_EVENT_RESOLUTION_WINDOW and _LIVE_EVENT_RESOLUTION_WINDOW[0][0] < cutoff:
        _LIVE_EVENT_RESOLUTION_WINDOW.popleft()


def get_ontology_health_snapshot() -> dict[str, object]:
    taxonomy = get_news_taxonomy()
    now_ts = time.time()
    with _HEALTH_LOCK:
        _trim_health_windows(now_ts)
        label_total = len(_LABEL_RESOLUTION_WINDOW)
        label_fallbacks = sum(1 for _ts, matched in _LABEL_RESOLUTION_WINDOW if not matched)
        live_total = len(_LIVE_EVENT_RESOLUTION_WINDOW)
        live_unmatched = sum(1 for _ts, matched, _tokens in _LIVE_EVENT_RESOLUTION_WINDOW if not matched)
        token_counter: Counter[str] = Counter()
        for _ts, matched, tokens in _LIVE_EVENT_RESOLUTION_WINDOW:
            if matched:
                continue
            token_counter.update(tokens)

    return {
        "version": int(taxonomy.version),
        "compiled_category_count": len(taxonomy.categories),
        "fallback_label_rate": round((label_fallbacks / label_total), 4) if label_total else 0.0,
        "unmatched_live_event_rate": round((live_unmatched / live_total), 4) if live_total else 0.0,
        "top_unmatched_event_tokens": [token for token, _count in token_counter.most_common(8)],
    }
