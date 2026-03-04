"""AI filter and digest generation via Codex OAuth subscription backend."""

from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import re
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import httpx

import config
from auth import AuthManager
from prompts import (
    QUERY_NO_MATCH_TEXT,
    build_digest_input_block,
    build_digest_system_prompt,
    build_query_system_prompt,
    quiet_period_message,
)
from utils import estimate_tokens_rough as _estimate_tokens_rough
from utils import normalize_space, sanitize_telegram_html


CACHE_MAX_ITEMS = 2048
DEFAULT_CODEX_MODEL = "gpt-5.3-codex"
DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api"
DEFAULT_CODEX_ORIGINATOR = "pi"
DEFAULT_DIGEST_MAX_POSTS = 80
DEFAULT_DIGEST_MAX_TOKENS = 18000

LOGGER = logging.getLogger("tg_news_userbot.ai_filter")
_SUMMARY_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_SEVERITY_CACHE: "OrderedDict[str, str]" = OrderedDict()
_HEADLINE_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_VITAL_VIEW_CACHE: "OrderedDict[str, Optional[str]]" = OrderedDict()
_QUOTA_WARNING_LOGGED = False

# Quota health tracking: capture recent 429/rate-limit events.
_QUOTA_429_EVENTS: deque[int] = deque(maxlen=512)


class _CodexAuthError(RuntimeError):
    """Raised when backend auth fails (401/403)."""


class _CodexRateLimitError(RuntimeError):
    """Raised when backend signals usage/limit exhaustion."""


class _CodexApiError(RuntimeError):
    """Raised for non-auth Codex backend errors."""


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


def _fallback_summary(text: str) -> Optional[str]:
    if _likely_noise(text):
        return None

    candidates = []
    for segment in re.split(r"\n+|(?<=[.!?])\s+", text):
        line = segment.strip(" \t-•")
        if len(line) < 24:
            continue
        if len(line) > 220:
            line = f"{line[:217].rsplit(' ', 1)[0]}..."
        candidates.append(line)
        if len(candidates) >= 3:
            break

    if not candidates:
        compact = text.strip()
        if len(compact) < 24:
            return None
        if len(compact) > 280:
            compact = f"{compact[:277].rsplit(' ', 1)[0]}..."
        candidates = [compact]

    return sanitize_telegram_html("<br>".join(candidates[:3]))


def _severity_from_text_heuristic(text: str) -> Literal["high", "medium", "low"]:
    lowered = text.lower()

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
    first = cleaned.split(". ")[0].strip()
    if len(first) > 120:
        first = f"{first[:117].rsplit(' ', 1)[0]}..."
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
        "You are a strict news filter.\n"
        f"Always write output in {language}.\n"
        f"If the input is in another language, translate it into {language} while summarizing.\n"
        "Output must be Telegram HTML only.\n"
        "Allowed tags: <b>, <i>, <u>, <s>, <tg-spoiler>, <code>, <pre>, <blockquote>, <a href>, <br>.\n"
        "If this message is real news or useful info, return a 2-3 line HTML summary.\n"
        "If it is spam, ads, promotional, or noise, reply only: SKIP"
    )


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
    return (
        "You write emergency wire headlines.\n"
        f"Output language must be {language}. Translate if needed.\n"
        "Return exactly one short sentence (8-18 words) with facts only.\n"
        "No prefix, no markdown, no source tag, no explanation."
    )


def _vital_rational_view_prompt() -> str:
    language = _resolve_output_language()
    try:
        max_words = int(getattr(config, "HUMANIZED_VITAL_OPINION_MAX_WORDS", 20))
    except Exception:
        max_words = 20
    max_words = max(10, min(max_words, 30))
    return (
        "You write concise, rational public-interest context for urgent news.\n"
        f"Output language must be {language}. Translate if needed.\n"
        f"Return exactly one sentence (max {max_words} words), neutral and evidence-aware.\n"
        "Start with: Why it matters: \n"
        "Do not speculate. Do not take sides. No hashtags. No markdown."
    )


def _build_codex_payload(
    text: str,
    instructions: str,
    *,
    verbosity: str = "medium",
    stream: bool = True,
) -> dict:
    payload: dict[str, object] = {
        "model": _resolve_codex_model(),
        "store": False,
        "stream": bool(stream),
        "instructions": instructions,
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
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
    ) -> None:
        self._cleaned = cleaned
        self._auth_context = auth_context
        self._instructions = instructions
        self._verbosity = verbosity

        self._client: httpx.AsyncClient | None = None
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
        )

        timeout = httpx.Timeout(90.0, connect=20.0)
        self._client = httpx.AsyncClient(timeout=timeout)
        self._stream_ctx = self._client.stream(
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
        if self._client is not None:
            await self._client.aclose()
            self._client = None
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
) -> _StreamingCodexResponse:
    return _StreamingCodexResponse(
        cleaned=cleaned,
        auth_context=auth_context,
        instructions=instructions,
        verbosity=verbosity,
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
    )
    timeout = httpx.Timeout(90.0, connect=20.0)
    async with httpx.AsyncClient(timeout=timeout) as http:
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
) -> str:
    if _streaming_enabled():
        try:
            fragments: list[str] = []
            async with streaming_codex_response(
                cleaned,
                auth_context,
                instructions,
                verbosity=verbosity,
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

        source_tag = ""
        if _include_source_tags():
            source_tag_value = normalize_space(str(item.get("source_tag") or ""))
            if not source_tag_value:
                sources = item.get("sources")
                if isinstance(sources, list) and sources:
                    source_tag_value = normalize_space(str(sources[0]))
            source_tag = f" <i>[{source_tag_value}]</i>" if source_tag_value else ""

        read_more = normalize_space(str(item.get("read_more") or ""))
        read_more_part = ""
        if read_more.startswith("http") and bool(getattr(config, "DIGEST_INCLUDE_READ_MORE_LINKS", True)):
            read_more_part = f' <a href="{read_more}">Read more</a>'

        line = f"{emoji} {headline}{source_tag}{read_more_part}".strip()
        lines.append(line)

        if len(lines) >= max_lines:
            break

    if not lines:
        return quiet_period_message(interval_minutes)

    return sanitize_telegram_html("<br>".join(lines[:max_lines]))


def _html_digest_cleanup(text: str, *, interval_minutes: int, max_lines: int) -> str:
    cleaned = (text or "").strip()
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
    include_sources = _include_source_tags()
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
        if len(line) > 220:
            line = f"{line[:217].rsplit(' ', 1)[0]}..."
        if not include_sources:
            # Remove bracketed source tags.
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
    include_links = bool(getattr(config, "DIGEST_INCLUDE_READ_MORE_LINKS", True))
    include_sources = _include_source_tags()

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
    include_sources = _include_source_tags()
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


def _query_context_token_budget() -> int:
    raw = getattr(config, "DIGEST_MAX_TOKENS", 18000)
    try:
        value = int(raw)
    except Exception:
        value = 18000
    return max(4000, min(value, 18000))


def _build_query_context_lines(
    context_messages: Sequence[Dict[str, object]],
    *,
    token_budget: int,
) -> tuple[list[str], int]:
    lines: list[str] = []
    used = 0
    seen = set()

    for idx, item in enumerate(context_messages, start=1):
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
        if len(text) > 900:
            text = f"{text[:897].rsplit(' ', 1)[0]}..."

        line = f"{idx}. [{source}] ({date_label}) {text}"
        if link.startswith("http"):
            line += f" | link={link}"

        line_tokens = estimate_tokens_rough(line) + 10
        if lines and used + line_tokens > token_budget:
            break
        lines.append(line)
        used += line_tokens

    return lines, used


def _fallback_query_answer(
    context_messages: Sequence[Dict[str, object]],
    *,
    detailed: bool,
) -> str:
    if not context_messages:
        return QUERY_NO_MATCH_TEXT

    selected = context_messages[: (6 if detailed else 3)]
    lines = []
    for item in selected:
        text = normalize_space(str(item.get("text") or ""))
        if not text:
            continue
        if len(text) > 180:
            text = f"{text[:177].rsplit(' ', 1)[0]}..."
        source = normalize_space(str(item.get("source") or "Source"))
        link = normalize_space(str(item.get("link") or ""))
        prefix = "🔥" if _severity_from_text_heuristic(text) == "high" else "⚠️"
        line = f"• {prefix} {text} <i>[{source}]</i>"
        if link.startswith("http"):
            line += f' <a href="{link}">source</a>'
        lines.append(line)

    if not lines:
        return QUERY_NO_MATCH_TEXT
    return sanitize_telegram_html("<b>Latest updates</b><br>" + "<br>".join(lines))


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
        auth_context = await auth_manager.get_auth_context()
        content = await _call_codex(
            cleaned,
            auth_context,
            instructions=_summary_system_prompt(),
        )
    except _CodexAuthError:
        try:
            auth_context = await auth_manager.refresh_auth_context()
            content = await _call_codex(
                cleaned,
                auth_context,
                instructions=_summary_system_prompt(),
            )
        except Exception:
            fallback = _fallback_summary(cleaned)
            _cache_set(key, fallback)
            return fallback
    except _CodexRateLimitError as exc:
        global _QUOTA_WARNING_LOGGED
        if not _QUOTA_WARNING_LOGGED:
            LOGGER.error(
                "Codex subscription limit reached (429). "
                "Using local fallback summarizer until quota resets. %s",
                exc,
            )
            _QUOTA_WARNING_LOGGED = True
        fallback = _fallback_summary(cleaned)
        _cache_set(key, fallback)
        return fallback
    except (httpx.HTTPError, _CodexApiError, ValueError) as exc:
        LOGGER.error("Codex backend failed, using local fallback: %s", exc)
        fallback = _fallback_summary(cleaned)
        _cache_set(key, fallback)
        return fallback
    except Exception:
        fallback = _fallback_summary(cleaned)
        _cache_set(key, fallback)
        return fallback

    if not content:
        fallback = _fallback_summary(cleaned)
        _cache_set(key, fallback)
        return fallback

    if content.upper().startswith("SKIP"):
        _cache_set(key, None)
        return None

    safe = sanitize_telegram_html(content)
    _cache_set(key, safe)
    return safe


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
    text = normalize_space(raw)
    if not text:
        return ""
    text = re.sub(r"^[*`#>\-\s]+", "", text).strip()
    text = re.sub(r"\[/?[A-Za-z0-9_ -]+\]", "", text).strip()
    if "\n" in text:
        text = text.splitlines()[0].strip()
    if len(text) > 140:
        text = f"{text[:137].rsplit(' ', 1)[0]}..."
    return text


def _cleanup_vital_view(raw: str) -> str:
    text = normalize_space(raw)
    if not text:
        return ""
    text = re.sub(r"^[*`#>\-\s]+", "", text).strip()
    text = re.sub(r"</?[^>]+>", "", text).strip()
    if "\n" in text:
        text = text.splitlines()[0].strip()
    if not text.lower().startswith("why it matters:"):
        text = f"Why it matters: {text}"
    try:
        max_words = int(getattr(config, "HUMANIZED_VITAL_OPINION_MAX_WORDS", 20))
    except Exception:
        max_words = 20
    max_words = max(10, min(max_words, 30))
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[: max_words - 1]).rstrip(".,;:") + "..."
    return text


def _fallback_vital_view(text: str) -> str:
    lowered = text.lower()
    if any(k in lowered for k in ("killed", "dead", "casualt", "injur")):
        return "Why it matters: Casualty reports can rapidly escalate security and humanitarian risk."
    if any(k in lowered for k in ("nuclear", "reactor", "power plant")):
        return "Why it matters: Any incident near nuclear infrastructure raises regional safety concerns."
    if any(k in lowered for k in ("missile", "drone", "airstrike", "strike", "explosion")):
        return "Why it matters: Military exchanges increase escalation risk and can trigger rapid retaliation."
    if any(k in lowered for k in ("evacu", "airspace", "closed", "emergency")):
        return "Why it matters: Official restrictions often signal broader disruption in the coming hours."
    return "Why it matters: This may affect regional stability; verify updates across multiple reliable sources."


async def summarize_breaking_headline(
    text: str,
    auth_manager: AuthManager,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned:
        return None

    key = _cache_key(f"headline::{cleaned}")
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
            fallback = _fallback_headline(cleaned)
            _headline_cache_set(key, fallback)
            return fallback
    except (_CodexRateLimitError, httpx.HTTPError, _CodexApiError, ValueError):
        fallback = _fallback_headline(cleaned)
        _headline_cache_set(key, fallback)
        return fallback
    except Exception:
        fallback = _fallback_headline(cleaned)
        _headline_cache_set(key, fallback)
        return fallback

    headline = _cleanup_headline(raw)
    if headline:
        _headline_cache_set(key, headline)
        return headline

    fallback = _fallback_headline(cleaned)
    _headline_cache_set(key, fallback)
    return fallback


async def summarize_vital_rational_view(
    text: str,
    auth_manager: AuthManager,
) -> Optional[str]:
    cleaned = text.strip()
    if not cleaned:
        return None

    key = _cache_key(f"vital_view::{cleaned}")
    cached = _vital_view_cache_get(key)
    if cached is not None or key in _VITAL_VIEW_CACHE:
        return cached

    compact = cleaned if len(cleaned) <= 2200 else f"{cleaned[:2197].rsplit(' ', 1)[0]}..."
    fallback = _cleanup_vital_view(_fallback_vital_view(cleaned))

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

    view = _cleanup_vital_view(raw)
    if view:
        _vital_view_cache_set(key, view)
        return view

    _vital_view_cache_set(key, fallback)
    return fallback


async def create_digest_summary(
    posts: List[Dict[str, object]],
    auth_manager: AuthManager | None = None,
    *,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    """
    Build one editorial digest from queued posts.

    Returns Telegram-HTML digest body or the exact quiet-period sentence.
    """
    interval_minutes = int(max(1, int(getattr(config, "DIGEST_INTERVAL_MINUTES", 30))))
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
    include_links = bool(getattr(config, "DIGEST_INCLUDE_READ_MORE_LINKS", True))
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
        include_source_tags=_include_source_tags(),
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
            return _json_digest_to_html(
                parsed,
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
            include_source_tags=_include_source_tags(),
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

    return _html_digest_cleanup(
        content,
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
    output_language = _resolve_output_language()

    # Reserve room for prompt + answer generation.
    context_budget = max(2000, _query_context_token_budget() - 2600)
    context_lines, used_tokens = _build_query_context_lines(
        context_messages,
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
        f"- Context lines included: {len(context_lines)}",
        f"- Estimated context tokens: {used_tokens}",
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
            return _fallback_query_answer(context_messages, detailed=detailed)
    except _CodexRateLimitError:
        return _fallback_query_answer(context_messages, detailed=detailed)
    except (httpx.HTTPError, _CodexApiError, ValueError):
        return _fallback_query_answer(context_messages, detailed=detailed)
    except Exception:
        return _fallback_query_answer(context_messages, detailed=detailed)

    cleaned = normalize_space(content)
    if not cleaned:
        return _fallback_query_answer(context_messages, detailed=detailed)

    plain_cleaned = re.sub(r"</?[^>]+>", "", cleaned).lower()
    if QUERY_NO_MATCH_TEXT.lower() in cleaned.lower() or "no relevant information found" in plain_cleaned:
        return QUERY_NO_MATCH_TEXT

    safe = sanitize_telegram_html(content.strip())
    if not safe:
        return QUERY_NO_MATCH_TEXT
    return safe
