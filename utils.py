"""Shared utility helpers for digest routing, token budgeting, logging, and query search."""

from __future__ import annotations

import asyncio
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import re
import threading
import time
from typing import Any, Iterable, List, Sequence, Tuple


def estimate_tokens_rough(text: str) -> int:
    """Fast rough token estimate (works well enough for budgeting)."""
    if not text:
        return 0
    # Roughly ~4 chars/token average for mixed English + symbols.
    return max(1, len(text) // 4)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


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


def format_ts(ts: int | None) -> str:
    if not ts:
        return "never"
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
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

    now = datetime.now()
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


def make_stream_markdown_preview(text: str) -> str:
    """
    Best-effort markdown safety for partial streamed text.
    Prevents common broken rendering while editing progressively.
    """
    value = text or ""
    if not value:
        return value

    # Avoid hanging incomplete markdown links.
    if value.count("[") > value.count("]"):
        cut = value.rfind("[")
        if cut >= 0:
            value = value[:cut]
    if value.count("(") > value.count(")") and "](" in value:
        cut = value.rfind("](")
        if cut >= 0:
            value = value[:cut]

    # Balance simple markdown markers for partial render.
    for marker in ("**", "__", "`"):
        if marker == "`":
            if value.count(marker) % 2 == 1:
                value += marker
        else:
            if value.count(marker) % 2 == 1:
                value += marker

    return value.rstrip()


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

        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
            import numpy as np  # type: ignore

            self._st_model = SentenceTransformer("all-MiniLM-L6-v2")
            self._np = np
            self._backend_name = "sentence_transformers"
            if self.logger:
                self.logger.info(
                    "Near-duplicate detector backend: sentence-transformers/all-MiniLM-L6-v2"
                )
            return
        except Exception as exc:
            self._backend_name = "tf_cosine_fallback"
            if self.logger:
                self.logger.warning(
                    "sentence-transformers unavailable; using TF cosine fallback for dedupe (%s)",
                    exc,
                )

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


def parse_time_filter_from_query(query: str, default_hours: int = 24) -> tuple[int, str]:
    """
    Parse time constraints from natural-language query.

    Supported examples:
    - "last 24 hours", "past 6h", "3d", "today", "yesterday"
    """
    text = normalize_space(query)
    lowered = text.lower()

    hours_back = max(1, int(default_hours))
    cleanup_patterns: list[str] = []

    # explicit relative windows
    patterns = [
        r"\b(?:last|past)\s+(\d{1,3})\s*(?:hours?|hrs?|h)\b",
        r"\b(\d{1,3})\s*(?:hours?|hrs?|h)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            hours_back = max(1, min(24 * 30, int(match.group(1))))
            cleanup_patterns.append(pattern)
            break

    day_patterns = [
        r"\b(?:last|past)\s+(\d{1,2})\s*(?:days?|d)\b",
        r"\b(\d{1,2})\s*(?:days?|d)\b",
    ]
    for pattern in day_patterns:
        match = re.search(pattern, lowered)
        if match:
            hours_back = max(24, min(24 * 30, int(match.group(1)) * 24))
            cleanup_patterns.append(pattern)
            break

    # calendar-style shortcuts
    if re.search(r"\byesterday\b", lowered):
        hours_back = max(hours_back, 48)
        cleanup_patterns.append(r"\byesterday\b")
    if re.search(r"\btoday\b", lowered):
        now_local = datetime.now()
        hours_back = max(hours_back, now_local.hour + 1)
        cleanup_patterns.append(r"\btoday\b")

    cleaned = text
    for pattern in cleanup_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = normalize_space(cleaned)
    if not cleaned:
        cleaned = text

    return hours_back, cleaned


async def search_recent_messages(
    client: Any,
    monitored_chats: Sequence[int | str],
    query: str,
    *,
    max_messages: int = 50,
    default_hours_back: int = 24,
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
    hours_back, cleaned_query = parse_time_filter_from_query(query, default_hours_back)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    keyword_terms = {
        token for token in re.findall(r"[a-z0-9]{3,}", cleaned_query.lower()) if token
    }

    stage_limit = max(10, min(60, resolved_max // max(1, min(len(monitored_chats), resolved_max)) + 8))
    fallback_limit = max(30, min(120, stage_limit * 3))

    collected: dict[tuple[str, int], dict[str, Any]] = {}

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
        if dedupe_key in collected:
            return

        dt = getattr(msg, "date", None)
        if not isinstance(dt, datetime):
            return
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt < cutoff:
            return

        text_value = _extract_message_text(msg)
        if not text_value:
            return

        if strict_keyword_filter and keyword_terms:
            lowered_text = text_value.lower()
            if not any(term in lowered_text for term in keyword_terms):
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

        collected[dedupe_key] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "timestamp": int(dt.timestamp()),
            "date": dt.isoformat(),
            "source": source,
            "text": text_value,
            "link": link,
        }

    async def _scan_chat(
        chat_ref: int | str,
        *,
        search_text: str | None,
        limit: int,
        strict_keyword_filter: bool,
    ) -> None:
        while True:
            try:
                kwargs: dict[str, Any] = {"limit": int(limit)}
                if search_text:
                    kwargs["search"] = search_text

                async for msg in client.iter_messages(chat_ref, **kwargs):
                    await _append_message(msg, strict_keyword_filter=strict_keyword_filter)
                    if len(collected) >= resolved_max:
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

    # Stage 1: server-side fuzzy search
    search_text = cleaned_query if cleaned_query else query
    for chat_ref in monitored_chats:
        await _scan_chat(
            chat_ref,
            search_text=search_text,
            limit=stage_limit,
            strict_keyword_filter=False,
        )
        if len(collected) >= resolved_max:
            break

    # Stage 2: fallback scan if no results
    if not collected:
        for chat_ref in monitored_chats:
            await _scan_chat(
                chat_ref,
                search_text=None,
                limit=fallback_limit,
                strict_keyword_filter=True,
            )
            if len(collected) >= resolved_max:
                break

    ordered = sorted(
        collected.values(),
        key=lambda item: int(item.get("timestamp", 0)),
        reverse=True,
    )
    return ordered[:resolved_max]


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
    ) -> None:
        self._send_message = send_message
        self._edit_message = edit_message
        self._get_message_id = get_message_id
        self._typing_action_cb = typing_action_cb
        self._typing_enabled = bool(typing_enabled)
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
        safe_preview = make_stream_markdown_preview(preview)
        if target < len(self._full_text):
            safe_preview = safe_preview.rstrip() + " ..."

        if not safe_preview.strip():
            return

        self._primary_ref = await self._edit_message(self._primary_ref, safe_preview)
        self._edit_count += 1
        self._rendered_chars = target
        self._last_edit_ts = time.time()

    async def finalize(self, final_text: str | None = None) -> LiveStreamStats:
        if final_text is not None:
            self._full_text = final_text

        cleaned_final = (self._full_text or "").rstrip()
        cleaned_final = re.sub(r"\s+\.\.\.$", "", cleaned_final).strip()
        if not cleaned_final:
            cleaned_final = "No matching information found in recent updates."

        chunks = split_markdown_chunks(cleaned_final, max_chars=self._max_message_chars)
        if not chunks:
            chunks = [cleaned_final]

        if self._primary_ref is None:
            self._primary_ref = await self._send_message(chunks[0], None)
            self._all_refs = [self._primary_ref]
        else:
            self._primary_ref = await self._edit_message(self._primary_ref, chunks[0])
            self._all_refs = [self._primary_ref]
            self._edit_count += 1

        reply_to = self._get_message_id(self._primary_ref)
        for extra in chunks[1:]:
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
