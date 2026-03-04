"""Shared utility helpers for digest routing, token budgeting, and logging."""

from __future__ import annotations

from collections import Counter, deque
from datetime import datetime, timedelta
import json
import logging
import math
import re
import threading
import time
from typing import Iterable, List, Sequence, Tuple


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
