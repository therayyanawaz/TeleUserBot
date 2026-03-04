"""SQLite persistence for dedupe, digest queue, and digest runtime metadata."""

from __future__ import annotations

import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, List, Tuple

from auth import DB_PATH, ensure_runtime_dir


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _to_rows_dict(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "channel_id": str(row["channel_id"]),
                "message_id": int(row["message_id"]),
                "source_name": str(row["source_name"] or ""),
                "raw_text": str(row["raw_text"]),
                "message_link": str(row["message_link"] or ""),
                "timestamp": int(row["timestamp"]),
                "processed": int(row["processed"]),
                "batch_id": str(row["batch_id"] or ""),
            }
        )
    return result


def init_db() -> None:
    ensure_runtime_dir()
    with _transaction() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_messages (
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (channel_id, message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                source_name TEXT,
                raw_text TEXT NOT NULL,
                message_link TEXT,
                timestamp INTEGER NOT NULL,
                processed INTEGER NOT NULL DEFAULT 0,
                batch_id TEXT,
                created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                UNIQUE(channel_id, message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_meta (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch())
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dupe_text_cache (
                text_norm TEXT PRIMARY KEY,
                last_seen INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recent_breaking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                normalized_text TEXT NOT NULL,
                embedding_blob BLOB,
                timestamp INTEGER NOT NULL,
                hash TEXT NOT NULL,
                UNIQUE(channel_id, message_id)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_digest_queue_pending "
            "ON digest_queue(processed, timestamp, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_digest_queue_batch "
            "ON digest_queue(batch_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dupe_text_cache_last_seen "
            "ON dupe_text_cache(last_seen)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recent_breaking_timestamp "
            "ON recent_breaking(timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recent_breaking_hash "
            "ON recent_breaking(hash)"
        )


def is_seen(channel_id: str, message_id: int) -> bool:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT 1
            FROM seen_messages
            WHERE channel_id = ? AND message_id = ?
            LIMIT 1
            """,
            (channel_id, message_id),
        )
        return cur.fetchone() is not None


def mark_seen(channel_id: str, message_id: int) -> None:
    with _transaction() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO seen_messages (channel_id, message_id)
            VALUES (?, ?)
            """,
            (channel_id, message_id),
        )


def mark_seen_many(channel_id: str, message_ids: Iterable[int]) -> None:
    pairs = {(channel_id, int(message_id)) for message_id in message_ids}
    if not pairs:
        return

    with _transaction() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO seen_messages (channel_id, message_id)
            VALUES (?, ?)
            """,
            list(pairs),
        )


def save_to_digest_queue(
    channel_id: str,
    message_id: int,
    raw_text: str,
    *,
    timestamp: int | None = None,
    source_name: str | None = None,
    message_link: str | None = None,
) -> None:
    cleaned = (raw_text or "").strip()
    if not cleaned:
        return

    ts = int(timestamp if timestamp is not None else time.time())
    with _transaction() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO digest_queue (
                channel_id,
                message_id,
                source_name,
                raw_text,
                message_link,
                timestamp,
                processed,
                batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL)
            """,
            (
                channel_id,
                int(message_id),
                (source_name or "").strip() or None,
                cleaned,
                (message_link or "").strip() or None,
                ts,
            ),
        )


def claim_digest_batch(limit: int, *, batch_id: str | None = None) -> Tuple[str, List[Dict[str, object]]]:
    if limit <= 0:
        return "", []

    resolved_batch_id = (batch_id or uuid.uuid4().hex).strip()
    if not resolved_batch_id:
        resolved_batch_id = uuid.uuid4().hex

    with _transaction() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, source_name, raw_text, message_link, timestamp, processed, batch_id
            FROM digest_queue
            WHERE processed = 0
            ORDER BY timestamp ASC, id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        if not rows:
            return "", []

        ids = [int(row["id"]) for row in rows]
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"UPDATE digest_queue SET processed = 1, batch_id = ? WHERE id IN ({placeholders})",  # noqa: S608
            [resolved_batch_id, *ids],
        )

        claimed = conn.execute(
            """
            SELECT id, channel_id, message_id, source_name, raw_text, message_link, timestamp, processed, batch_id
            FROM digest_queue
            WHERE batch_id = ?
            ORDER BY timestamp ASC, id ASC
            """,
            (resolved_batch_id,),
        ).fetchall()

    return resolved_batch_id, _to_rows_dict(claimed)


def ack_digest_batch(batch_id: str) -> int:
    if not batch_id.strip():
        return 0
    with _transaction() as conn:
        cur = conn.execute(
            """
            DELETE FROM digest_queue
            WHERE batch_id = ? AND processed = 1
            """,
            (batch_id,),
        )
        return int(cur.rowcount or 0)


def restore_digest_batch(batch_id: str) -> int:
    if not batch_id.strip():
        return 0
    with _transaction() as conn:
        cur = conn.execute(
            """
            UPDATE digest_queue
            SET processed = 0,
                batch_id = NULL
            WHERE batch_id = ?
            """,
            (batch_id,),
        )
        return int(cur.rowcount or 0)


def load_batch_rows(batch_id: str) -> List[Dict[str, object]]:
    if not batch_id.strip():
        return []
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, source_name, raw_text, message_link, timestamp, processed, batch_id
            FROM digest_queue
            WHERE batch_id = ?
            ORDER BY timestamp ASC, id ASC
            """,
            (batch_id,),
        ).fetchall()
    return _to_rows_dict(rows)


def load_and_clear_digest_queue(limit: int | None = None) -> List[Dict[str, object]]:
    """Compatibility helper: atomically claim then delete one batch."""
    resolved_limit = int(limit if (limit is not None and limit > 0) else 1000000)
    batch_id, rows = claim_digest_batch(resolved_limit)
    if not rows:
        return []
    ack_digest_batch(batch_id)
    return rows


def count_pending() -> int:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT COUNT(*)
            FROM digest_queue
            WHERE processed = 0
            """
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)


def count_inflight() -> int:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT COUNT(*)
            FROM digest_queue
            WHERE processed = 1
            """
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)


def set_meta(key: str, value: str) -> None:
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO digest_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, int(time.time())),
        )


def get_meta(key: str, default: str | None = None) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT value FROM digest_meta WHERE key = ? LIMIT 1",
            (key,),
        ).fetchone()
    if row is None:
        return default
    return str(row["value"])


def delete_meta(key: str) -> None:
    with _transaction() as conn:
        conn.execute("DELETE FROM digest_meta WHERE key = ?", (key,))


def set_last_digest_timestamp(ts: int) -> None:
    set_meta("last_digest_ts", str(int(ts)))


def get_last_digest_timestamp() -> int | None:
    raw = get_meta("last_digest_ts")
    if raw is None:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    if value <= 0:
        return None
    return value


def _normalize_text_for_dupe(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def save_dupe_text(text: str, *, max_rows: int = 400) -> None:
    normalized = _normalize_text_for_dupe(text)
    if len(normalized) < 12:
        return

    limit = max(50, min(int(max_rows), 5000))
    now = int(time.time())
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO dupe_text_cache(text_norm, last_seen)
            VALUES (?, ?)
            ON CONFLICT(text_norm) DO UPDATE SET
                last_seen = excluded.last_seen
            """,
            (normalized, now),
        )

        conn.execute(
            """
            DELETE FROM dupe_text_cache
            WHERE text_norm IN (
                SELECT text_norm
                FROM dupe_text_cache
                ORDER BY last_seen DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (limit,),
        )


def load_recent_dupe_texts(limit: int = 400) -> List[str]:
    resolved_limit = max(1, min(int(limit), 5000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT text_norm
            FROM dupe_text_cache
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (resolved_limit,),
    ).fetchall()
    return [str(row["text_norm"]) for row in rows if row["text_norm"]]


def save_recent_breaking(
    *,
    channel_id: str,
    message_id: int,
    normalized_text: str,
    embedding_blob: bytes | None,
    timestamp: int | None = None,
    text_hash: str,
    history_hours: int = 4,
) -> int:
    """
    Persist one dedupe candidate and prune records older than history window.
    """
    cleaned = re.sub(r"\s+", " ", (normalized_text or "").strip())
    if not cleaned:
        return 0

    ts = int(timestamp if timestamp is not None else time.time())
    hours = max(1, min(int(history_hours), 24))
    cutoff = ts - (hours * 3600)

    with _transaction() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO recent_breaking (
                channel_id,
                message_id,
                normalized_text,
                embedding_blob,
                timestamp,
                hash
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(channel_id),
                int(message_id),
                cleaned,
                embedding_blob,
                ts,
                str(text_hash),
            ),
        )
        pruned = conn.execute(
            "DELETE FROM recent_breaking WHERE timestamp < ?",
            (cutoff,),
        )
    return int(pruned.rowcount or 0)


def purge_recent_breaking(*, history_hours: int = 4) -> int:
    now = int(time.time())
    hours = max(1, min(int(history_hours), 24))
    cutoff = now - (hours * 3600)
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM recent_breaking WHERE timestamp < ?",
            (cutoff,),
        )
    return int(cur.rowcount or 0)


def load_recent_breaking(*, since_ts: int, limit: int = 10000) -> List[Dict[str, object]]:
    resolved_limit = max(1, min(int(limit), 50000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, normalized_text, embedding_blob, timestamp, hash
            FROM recent_breaking
            WHERE timestamp >= ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (int(since_ts), resolved_limit),
        ).fetchall()

    out: List[Dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "id": int(row["id"]),
                "channel_id": str(row["channel_id"]),
                "message_id": int(row["message_id"]),
                "normalized_text": str(row["normalized_text"] or ""),
                "embedding_blob": bytes(row["embedding_blob"] or b""),
                "timestamp": int(row["timestamp"]),
                "hash": str(row["hash"] or ""),
            }
        )
    return out
