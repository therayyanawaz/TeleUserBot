from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
import re
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


class NewsTaxonomyError(ValueError):
    """Raised when the taxonomy file is missing or invalid."""


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
    _primary_patterns: tuple[tuple[str, Pattern[str]], ...]
    _alias_patterns: tuple[tuple[str, Pattern[str]], ...]
    _required_patterns: tuple[tuple[str, Pattern[str]], ...]
    _blocked_patterns: tuple[tuple[str, Pattern[str]], ...]


@dataclass(frozen=True)
class NewsCategoryMatch:
    category_key: str
    label: str
    confidence_score: float
    matched_primary: tuple[str, ...]
    matched_aliases: tuple[str, ...]
    breaking_eligible: bool
    severity_bias: str
    priority: int
    matched_required: tuple[str, ...] = ()
    _stable_order: int = 0


@dataclass(frozen=True)
class NewsTaxonomy:
    version: int
    path: Path
    categories: tuple[NewsCategory, ...]


_TAXONOMY_CACHE: NewsTaxonomy | None = None


def normalize_taxonomy_text(text: str) -> str:
    normalized = str(text or "").translate(_PUNCT_TRANSLATION).casefold()
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


def _build_category(payload: object) -> NewsCategory:
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
        _primary_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in primary_phrases),
        _alias_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in alias_phrases),
        _required_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in required_any),
        _blocked_patterns=tuple((phrase, _phrase_pattern(phrase)) for phrase in blocked_phrases),
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

    raw_categories = payload.get("categories")
    if not isinstance(raw_categories, list) or not raw_categories:
        raise NewsTaxonomyError("News taxonomy must define a non-empty 'categories' list.")

    categories: list[NewsCategory] = []
    seen_keys: set[str] = set()
    for raw_category in raw_categories:
        category = _build_category(raw_category)
        if category.key in seen_keys:
            raise NewsTaxonomyError(f"Duplicate taxonomy category key '{category.key}'.")
        seen_keys.add(category.key)
        categories.append(category)

    taxonomy = NewsTaxonomy(version=version, path=taxonomy_path, categories=tuple(categories))
    if path is None:
        _TAXONOMY_CACHE = taxonomy
    return taxonomy


def get_news_taxonomy() -> NewsTaxonomy:
    return load_news_taxonomy()


def _match_phrases(text: str, compiled: Sequence[tuple[str, Pattern[str]]]) -> list[str]:
    hits: list[str] = []
    for phrase, pattern in compiled:
        if pattern.search(text):
            hits.append(phrase)
    return hits


def find_news_category_matches(
    text: str,
    *,
    breaking_only: bool = False,
) -> list[NewsCategoryMatch]:
    normalized = normalize_taxonomy_text(text)
    if not normalized:
        return []

    taxonomy = get_news_taxonomy()
    matches: list[NewsCategoryMatch] = []
    for order, category in enumerate(taxonomy.categories):
        if breaking_only and not category.breaking_eligible:
            continue

        if _match_phrases(normalized, category._blocked_patterns):
            continue

        primary_hits = _match_phrases(normalized, category._primary_patterns)
        alias_hits = _match_phrases(normalized, category._alias_patterns)
        required_hits = _match_phrases(normalized, category._required_patterns)
        confidence_score = (3 * len(primary_hits)) + (2 * len(alias_hits))
        if required_hits:
            confidence_score += 1
        if confidence_score < 3:
            continue

        matches.append(
            NewsCategoryMatch(
                category_key=category.key,
                label=category.label,
                confidence_score=float(confidence_score),
                matched_primary=tuple(primary_hits),
                matched_aliases=tuple(alias_hits),
                breaking_eligible=category.breaking_eligible,
                severity_bias=category.severity_bias,
                priority=category.priority,
                matched_required=tuple(required_hits),
                _stable_order=order,
            )
        )

    matches.sort(
        key=lambda item: (
            -item.confidence_score,
            -max((len(phrase) for phrase in item.matched_primary), default=0),
            -item.priority,
            -len(item.matched_primary),
            item._stable_order,
        )
    )
    return matches


def match_news_category(
    text: str,
    *,
    breaking_only: bool = False,
) -> NewsCategoryMatch | None:
    matches = find_news_category_matches(text, breaking_only=breaking_only)
    if not matches:
        return None
    return matches[0]


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
        for phrase in (*category.primary_phrases, *category.alias_phrases):
            if phrase in seen:
                continue
            seen.add(phrase)
            phrases.append(phrase)
    return tuple(phrases)
