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


def _to_archive_rows_dict(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
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
            }
        )
    return result


def _to_inbound_job_rows(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "job_key": str(row["job_key"]),
                "priority": int(row["priority"]),
                "stage": str(row["stage"]),
                "status": str(row["status"]),
                "run_after_ts": int(row["run_after_ts"]),
                "channel_id": str(row["channel_id"]),
                "message_id": int(row["message_id"]),
                "payload_json": str(row["payload_json"] or "{}"),
                "retry_count": int(row["retry_count"]),
                "max_retries": int(row["max_retries"]),
                "last_error": str(row["last_error"] or ""),
                "last_error_at": int(row["last_error_at"] or 0),
                "claimed_by": str(row["claimed_by"] or ""),
                "claimed_at": int(row["claimed_at"] or 0),
                "created_at": int(row["created_at"]),
                "updated_at": int(row["updated_at"]),
            }
        )
    return result


def _to_breaking_story_cluster_rows(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "cluster_id": str(row["cluster_id"]),
                "cluster_key": str(row["cluster_key"]),
                "topic_key": str(row["topic_key"] or ""),
                "taxonomy_key": str(row["taxonomy_key"] or ""),
                "root_message_id": int(row["root_message_id"] or 0),
                "root_sent_ref": str(row["root_sent_ref"] or ""),
                "current_headline": str(row["current_headline"] or ""),
                "current_facts_json": str(row["current_facts_json"] or "{}"),
                "opened_ts": int(row["opened_ts"] or 0),
                "updated_ts": int(row["updated_ts"] or 0),
                "last_delivery_ts": int(row["last_delivery_ts"] or 0),
                "update_count": int(row["update_count"] or 0),
                "status": str(row["status"] or "active"),
            }
        )
    return result


def _to_breaking_story_event_rows(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "cluster_id": str(row["cluster_id"]),
                "source_channel_id": str(row["source_channel_id"] or ""),
                "source_message_id": int(row["source_message_id"] or 0),
                "delivery_message_id": int(row["delivery_message_id"] or 0),
                "normalized_text": str(row["normalized_text"] or ""),
                "display_text": str(row["display_text"] or ""),
                "facts_json": str(row["facts_json"] or "{}"),
                "delta_kind": str(row["delta_kind"] or ""),
                "created_ts": int(row["created_ts"] or 0),
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
                created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                UNIQUE(channel_id, message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_meta (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                source_name TEXT,
                raw_text TEXT NOT NULL,
                message_link TEXT,
                timestamp INTEGER NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                UNIQUE(channel_id, message_id)
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
            """
            CREATE TABLE IF NOT EXISTS recent_media_signatures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                media_hash TEXT NOT NULL,
                normalized_text TEXT,
                media_kind TEXT,
                timestamp INTEGER NOT NULL,
                UNIQUE(channel_id, message_id, media_hash)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_delivery_refs (
                channel_id TEXT NOT NULL,
                source_message_id INTEGER NOT NULL,
                destination_message_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                PRIMARY KEY (channel_id, source_message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inbound_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key TEXT NOT NULL UNIQUE,
                priority INTEGER NOT NULL DEFAULT 50,
                stage TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                run_after_ts INTEGER NOT NULL,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 4,
                last_error TEXT,
                last_error_at INTEGER,
                claimed_by TEXT,
                claimed_at INTEGER,
                created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_decision_cache (
                cache_key TEXT PRIMARY KEY,
                normalized_hash TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                model TEXT NOT NULL,
                decision_json TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS breaking_story_clusters (
                cluster_id TEXT PRIMARY KEY,
                cluster_key TEXT NOT NULL UNIQUE,
                topic_key TEXT NOT NULL,
                taxonomy_key TEXT NOT NULL,
                root_message_id INTEGER NOT NULL,
                root_sent_ref TEXT,
                current_headline TEXT NOT NULL,
                current_facts_json TEXT NOT NULL,
                opened_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                last_delivery_ts INTEGER NOT NULL,
                update_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active'
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS breaking_story_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id TEXT NOT NULL,
                source_channel_id TEXT NOT NULL,
                source_message_id INTEGER NOT NULL,
                delivery_message_id INTEGER NOT NULL,
                normalized_text TEXT NOT NULL,
                display_text TEXT,
                facts_json TEXT NOT NULL,
                delta_kind TEXT NOT NULL,
                created_ts INTEGER NOT NULL
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
            "CREATE INDEX IF NOT EXISTS idx_digest_archive_timestamp "
            "ON digest_archive(timestamp, id)"
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recent_media_signatures_hash "
            "ON recent_media_signatures(media_hash, timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_recent_media_signatures_timestamp "
            "ON recent_media_signatures(timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_source_delivery_refs_timestamp "
            "ON source_delivery_refs(timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbound_jobs_stage_status "
            "ON inbound_jobs(stage, status, run_after_ts, priority, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbound_jobs_channel_message "
            "ON inbound_jobs(channel_id, message_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbound_jobs_status_updated "
            "ON inbound_jobs(status, updated_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ai_decision_cache_created_at "
            "ON ai_decision_cache(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_breaking_story_clusters_status_updated "
            "ON breaking_story_clusters(status, updated_ts DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_breaking_story_clusters_topic "
            "ON breaking_story_clusters(topic_key, updated_ts DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_breaking_story_events_cluster_created "
            "ON breaking_story_events(cluster_id, created_ts DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_breaking_story_events_created "
            "ON breaking_story_events(created_ts DESC)"
        )


def enqueue_inbound_job(
    *,
    job_key: str,
    stage: str,
    channel_id: str,
    message_id: int,
    payload_json: str,
    priority: int = 50,
    run_after_ts: int | None = None,
    max_retries: int = 4,
) -> bool:
    cleaned_job_key = str(job_key or "").strip()
    cleaned_stage = str(stage or "").strip()
    if not cleaned_job_key or not cleaned_stage:
        return False

    now_ts = int(time.time())
    scheduled_ts = int(run_after_ts if run_after_ts is not None else now_ts)
    with _transaction() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO inbound_jobs (
                job_key,
                priority,
                stage,
                status,
                run_after_ts,
                channel_id,
                message_id,
                payload_json,
                retry_count,
                max_retries,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                cleaned_job_key,
                int(priority),
                cleaned_stage,
                scheduled_ts,
                str(channel_id),
                int(message_id),
                str(payload_json or "{}"),
                max(1, int(max_retries)),
                now_ts,
                now_ts,
            ),
        )
        return int(cur.rowcount or 0) > 0


def claim_inbound_jobs(stage: str, limit: int, *, worker_id: str) -> List[Dict[str, object]]:
    cleaned_stage = str(stage or "").strip()
    cleaned_worker_id = str(worker_id or "").strip() or "worker"
    resolved_limit = max(1, int(limit))
    now_ts = int(time.time())

    with _transaction() as conn:
        rows = conn.execute(
            """
            SELECT id
            FROM inbound_jobs
            WHERE stage = ?
              AND status = 'pending'
              AND run_after_ts <= ?
            ORDER BY priority DESC, run_after_ts ASC, created_at ASC, id ASC
            LIMIT ?
            """,
            (cleaned_stage, now_ts, resolved_limit),
        ).fetchall()
        if not rows:
            return []

        ids = [int(row["id"]) for row in rows]
        conn.executemany(
            """
            UPDATE inbound_jobs
            SET status = 'in_progress',
                claimed_by = ?,
                claimed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            [(cleaned_worker_id, now_ts, now_ts, job_id) for job_id in ids],
        )

        placeholders = ",".join("?" for _ in ids)
        claimed = conn.execute(
            f"""
            SELECT id, job_key, priority, stage, status, run_after_ts, channel_id, message_id,
                   payload_json, retry_count, max_retries, last_error, last_error_at,
                   claimed_by, claimed_at, created_at, updated_at
            FROM inbound_jobs
            WHERE id IN ({placeholders})
            ORDER BY priority DESC, run_after_ts ASC, created_at ASC, id ASC
            """,
            ids,
        ).fetchall()

    return _to_inbound_job_rows(claimed)


def advance_inbound_job(
    job_id: int,
    *,
    next_stage: str,
    payload_json: str,
    priority: int | None = None,
    run_after_ts: int | None = None,
) -> None:
    now_ts = int(time.time())
    next_ts = int(run_after_ts if run_after_ts is not None else now_ts)
    with _transaction() as conn:
        if priority is None:
            conn.execute(
                """
                UPDATE inbound_jobs
                SET stage = ?,
                    status = 'pending',
                    run_after_ts = ?,
                    payload_json = ?,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(next_stage), next_ts, str(payload_json or "{}"), now_ts, int(job_id)),
            )
        else:
            conn.execute(
                """
                UPDATE inbound_jobs
                SET stage = ?,
                    status = 'pending',
                    priority = ?,
                    run_after_ts = ?,
                    payload_json = ?,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    str(next_stage),
                    int(priority),
                    next_ts,
                    str(payload_json or "{}"),
                    now_ts,
                    int(job_id),
                ),
            )


def complete_inbound_job(job_id: int, *, payload_json: str | None = None) -> None:
    now_ts = int(time.time())
    with _transaction() as conn:
        if payload_json is None:
            conn.execute(
                """
                UPDATE inbound_jobs
                SET status = 'done',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now_ts, int(job_id)),
            )
        else:
            conn.execute(
                """
                UPDATE inbound_jobs
                SET status = 'done',
                    payload_json = ?,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (str(payload_json or "{}"), now_ts, int(job_id)),
            )


def retry_or_dead_letter_inbound_job(
    job_id: int,
    *,
    error_text: str,
    retry_delay_seconds: int,
) -> bool:
    now_ts = int(time.time())
    with _transaction() as conn:
        row = conn.execute(
            """
            SELECT retry_count, max_retries
            FROM inbound_jobs
            WHERE id = ?
            LIMIT 1
            """,
            (int(job_id),),
        ).fetchone()
        if row is None:
            return False

        retry_count = int(row["retry_count"] or 0) + 1
        max_retries = max(1, int(row["max_retries"] or 1))
        if retry_count >= max_retries:
            conn.execute(
                """
                UPDATE inbound_jobs
                SET status = 'dead',
                    retry_count = ?,
                    last_error = ?,
                    last_error_at = ?,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (retry_count, str(error_text or ""), now_ts, now_ts, int(job_id)),
            )
            return False

        conn.execute(
            """
            UPDATE inbound_jobs
            SET status = 'pending',
                retry_count = ?,
                last_error = ?,
                last_error_at = ?,
                run_after_ts = ?,
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (
                retry_count,
                str(error_text or ""),
                now_ts,
                now_ts + max(1, int(retry_delay_seconds)),
                now_ts,
                int(job_id),
            ),
        )
        return True


def reset_in_progress_inbound_jobs(*, older_than_seconds: int = 0) -> int:
    now_ts = int(time.time())
    threshold = now_ts - max(0, int(older_than_seconds))
    with _transaction() as conn:
        cur = conn.execute(
            """
            UPDATE inbound_jobs
            SET status = 'pending',
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = ?
            WHERE status = 'in_progress'
              AND COALESCE(claimed_at, 0) <= ?
            """,
            (now_ts, threshold),
        )
        return int(cur.rowcount or 0)


def count_inbound_jobs(*, stage: str | None = None, status: str | None = None) -> int:
    clauses: List[str] = []
    params: List[object] = []
    if stage:
        clauses.append("stage = ?")
        params.append(str(stage))
    if status:
        clauses.append("status = ?")
        params.append(str(status))

    query = "SELECT COUNT(*) FROM inbound_jobs"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    with _connect() as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    return int(row[0] if row else 0)


def load_inbound_job_counts() -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT stage, status, COUNT(*) AS row_count
            FROM inbound_jobs
            GROUP BY stage, status
            """
        ).fetchall()
    for row in rows:
        stage = str(row["stage"] or "")
        status = str(row["status"] or "")
        if not stage or not status:
            continue
        bucket = out.setdefault(stage, {})
        bucket[status] = int(row["row_count"] or 0)
    return out


def oldest_pending_inbound_job_age_seconds() -> int | None:
    now_ts = int(time.time())
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT MIN(created_at) AS min_created_at
            FROM inbound_jobs
            WHERE status = 'pending'
            """
        ).fetchone()
    if row is None or row["min_created_at"] is None:
        return None
    created_at = int(row["min_created_at"] or 0)
    if created_at <= 0:
        return None
    return max(0, now_ts - created_at)


def load_recent_inbound_job_failures(limit: int = 10) -> List[Dict[str, object]]:
    resolved_limit = max(1, int(limit))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, job_key, stage, status, retry_count, max_retries, last_error, last_error_at
            FROM inbound_jobs
            WHERE last_error_at IS NOT NULL
            ORDER BY last_error_at DESC, id DESC
            LIMIT ?
            """,
            (resolved_limit,),
        ).fetchall()
    result: List[Dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "job_key": str(row["job_key"]),
                "stage": str(row["stage"] or ""),
                "status": str(row["status"] or ""),
                "retry_count": int(row["retry_count"] or 0),
                "max_retries": int(row["max_retries"] or 0),
                "last_error": str(row["last_error"] or ""),
                "last_error_at": int(row["last_error_at"] or 0),
            }
        )
    return result


def ai_decision_cache_get(
    *,
    normalized_hash: str,
    prompt_version: str,
    model: str,
    max_age_hours: int,
) -> str | None:
    cache_key = f"{normalized_hash}::{prompt_version}::{model}"
    cutoff_ts = int(time.time()) - (max(1, int(max_age_hours)) * 3600)
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT decision_json
            FROM ai_decision_cache
            WHERE cache_key = ?
              AND created_at >= ?
            LIMIT 1
            """,
            (cache_key, cutoff_ts),
        ).fetchone()
    if row is None:
        return None
    return str(row["decision_json"] or "")


def ai_decision_cache_set(
    *,
    normalized_hash: str,
    prompt_version: str,
    model: str,
    decision_json: str,
) -> None:
    cache_key = f"{normalized_hash}::{prompt_version}::{model}"
    now_ts = int(time.time())
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO ai_decision_cache (
                cache_key,
                normalized_hash,
                prompt_version,
                model,
                decision_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                decision_json = excluded.decision_json,
                created_at = excluded.created_at
            """,
            (
                cache_key,
                str(normalized_hash or ""),
                str(prompt_version or ""),
                str(model or ""),
                str(decision_json or "{}"),
                now_ts,
            ),
        )


def purge_ai_decision_cache(*, older_than_ts: int) -> int:
    cutoff_ts = int(max(0, older_than_ts))
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM ai_decision_cache WHERE created_at < ?",
            (cutoff_ts,),
        )
        return int(cur.rowcount or 0)


def count_ai_decision_cache_entries() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(*) FROM ai_decision_cache").fetchone()
    return int(row[0] if row else 0)


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
                created_at,
                processed,
                batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)
            """,
            (
                channel_id,
                int(message_id),
                (source_name or "").strip() or None,
                cleaned,
                (message_link or "").strip() or None,
                ts,
                ts,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO digest_archive (
                channel_id,
                message_id,
                source_name,
                raw_text,
                message_link,
                timestamp,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                channel_id,
                int(message_id),
                (source_name or "").strip() or None,
                cleaned,
                (message_link or "").strip() or None,
                ts,
                ts,
            ),
        )


def save_to_digest_archive(
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
            INSERT OR IGNORE INTO digest_archive (
                channel_id,
                message_id,
                source_name,
                raw_text,
                message_link,
                timestamp,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                channel_id,
                int(message_id),
                (source_name or "").strip() or None,
                cleaned,
                (message_link or "").strip() or None,
                ts,
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
        conn.executemany(
            """
            UPDATE digest_queue
            SET processed = 1,
                batch_id = ?
            WHERE id = ?
            """,
            [(resolved_batch_id, row_id) for row_id in ids],
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


def load_queue_since(since_ts: int, limit: int) -> List[Dict[str, object]]:
    """
    Load recent digest queue rows for query-time evidence search.

    This intentionally includes both pending and inflight rows because a query
    should be able to see the freshest posts even if a digest batch has already
    claimed them but not yet acknowledged/deleted them.
    """
    resolved_limit = max(1, int(limit))
    since = int(max(0, since_ts))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, source_name, raw_text, message_link, timestamp, processed, batch_id
            FROM digest_queue
            WHERE timestamp >= ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (since, resolved_limit),
        ).fetchall()
    out = _to_rows_dict(rows)
    out.reverse()
    return out


def clear_digest_queue(*, include_inflight: bool = True) -> int:
    """Delete queued digest rows. Used by periodic queue purge policy."""
    resolved_scope = "all" if include_inflight else "pending"
    return clear_digest_queue_scoped(resolved_scope)


def clear_digest_queue_scoped(scope: str) -> int:
    """
    Scope options:
    - all: delete pending + inflight
    - pending: delete only unprocessed rows
    - inflight: delete only claimed rows
    """
    normalized = (scope or "").strip().lower()
    if normalized not in {"all", "pending", "inflight"}:
        normalized = "pending"

    with _transaction() as conn:
        if normalized == "all":
            cur = conn.execute("DELETE FROM digest_queue")
        elif normalized == "pending":
            cur = conn.execute("DELETE FROM digest_queue WHERE processed = 0")
        else:
            cur = conn.execute("DELETE FROM digest_queue WHERE processed = 1")
        return int(cur.rowcount or 0)


def load_archive_since(since_ts: int, limit: int) -> List[Dict[str, object]]:
    resolved_limit = max(1, int(limit))
    since = int(max(0, since_ts))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, source_name, raw_text, message_link, timestamp
            FROM digest_archive
            WHERE timestamp >= ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (since, resolved_limit),
        ).fetchall()
    # Preserve chronological order for digest coherence.
    out = _to_archive_rows_dict(rows)
    out.reverse()
    return out


def prune_archive_older_than(cutoff_ts: int) -> int:
    cutoff = int(max(0, cutoff_ts))
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM digest_archive WHERE timestamp < ?",
            (cutoff,),
        )
        return int(cur.rowcount or 0)


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
    except (ValueError, TypeError):
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


def save_recent_media_signature(
    *,
    channel_id: str,
    message_id: int,
    media_hash: str,
    normalized_text: str,
    media_kind: str,
    timestamp: int | None = None,
    history_hours: int = 4,
) -> int:
    cleaned_hash = str(media_hash or "").strip()
    if not cleaned_hash:
        return 0

    cleaned_text = re.sub(r"\s+", " ", (normalized_text or "").strip())
    ts = int(timestamp if timestamp is not None else time.time())
    hours = max(1, min(int(history_hours), 24))
    cutoff = ts - (hours * 3600)

    with _transaction() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO recent_media_signatures (
                channel_id,
                message_id,
                media_hash,
                normalized_text,
                media_kind,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(channel_id),
                int(message_id),
                cleaned_hash,
                cleaned_text,
                str(media_kind or ""),
                ts,
            ),
        )
        pruned = conn.execute(
            "DELETE FROM recent_media_signatures WHERE timestamp < ?",
            (cutoff,),
        )
    return int(pruned.rowcount or 0)


def purge_recent_media_signatures(*, history_hours: int = 4) -> int:
    now = int(time.time())
    hours = max(1, min(int(history_hours), 24))
    cutoff = now - (hours * 3600)
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM recent_media_signatures WHERE timestamp < ?",
            (cutoff,),
        )
    return int(cur.rowcount or 0)


def load_recent_media_signatures(
    *,
    media_hash: str,
    since_ts: int,
    limit: int = 50,
) -> List[Dict[str, object]]:
    cleaned_hash = str(media_hash or "").strip()
    if not cleaned_hash:
        return []
    resolved_limit = max(1, min(int(limit), 1000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, channel_id, message_id, media_hash, normalized_text, media_kind, timestamp
            FROM recent_media_signatures
            WHERE media_hash = ? AND timestamp >= ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (cleaned_hash, int(since_ts), resolved_limit),
        ).fetchall()

    out: List[Dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "id": int(row["id"]),
                "channel_id": str(row["channel_id"]),
                "message_id": int(row["message_id"]),
                "media_hash": str(row["media_hash"] or ""),
                "normalized_text": str(row["normalized_text"] or ""),
                "media_kind": str(row["media_kind"] or ""),
                "timestamp": int(row["timestamp"]),
            }
        )
    return out


def save_source_delivery_ref(
    *,
    channel_id: str,
    source_message_id: int,
    destination_message_id: int,
    timestamp: int | None = None,
) -> None:
    ts = int(timestamp if timestamp is not None else time.time())
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO source_delivery_refs (
                channel_id,
                source_message_id,
                destination_message_id,
                timestamp
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(channel_id, source_message_id) DO UPDATE SET
                destination_message_id = excluded.destination_message_id,
                timestamp = excluded.timestamp
            """,
            (
                str(channel_id),
                int(source_message_id),
                int(destination_message_id),
                ts,
            ),
        )


def load_source_delivery_ref(*, channel_id: str, source_message_id: int) -> int | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT destination_message_id
            FROM source_delivery_refs
            WHERE channel_id = ? AND source_message_id = ?
            LIMIT 1
            """,
            (str(channel_id), int(source_message_id)),
        ).fetchone()
    if row is None:
        return None
    value = int(row["destination_message_id"] or 0)
    return value or None


def purge_source_delivery_refs(*, history_hours: int = 168) -> int:
    now = int(time.time())
    hours = max(1, min(int(history_hours), 24 * 60))
    cutoff = now - (hours * 3600)
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM source_delivery_refs WHERE timestamp < ?",
            (cutoff,),
        )
    return int(cur.rowcount or 0)


def save_breaking_story_cluster(
    *,
    cluster_id: str,
    cluster_key: str,
    topic_key: str,
    taxonomy_key: str,
    root_message_id: int,
    root_sent_ref: str,
    current_headline: str,
    current_facts_json: str,
    opened_ts: int,
    updated_ts: int,
    last_delivery_ts: int,
    update_count: int,
    status: str = "active",
) -> None:
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO breaking_story_clusters (
                cluster_id,
                cluster_key,
                topic_key,
                taxonomy_key,
                root_message_id,
                root_sent_ref,
                current_headline,
                current_facts_json,
                opened_ts,
                updated_ts,
                last_delivery_ts,
                update_count,
                status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id) DO UPDATE SET
                cluster_key = excluded.cluster_key,
                topic_key = excluded.topic_key,
                taxonomy_key = excluded.taxonomy_key,
                root_message_id = excluded.root_message_id,
                root_sent_ref = excluded.root_sent_ref,
                current_headline = excluded.current_headline,
                current_facts_json = excluded.current_facts_json,
                opened_ts = excluded.opened_ts,
                updated_ts = excluded.updated_ts,
                last_delivery_ts = excluded.last_delivery_ts,
                update_count = excluded.update_count,
                status = excluded.status
            """,
            (
                str(cluster_id),
                str(cluster_key),
                str(topic_key),
                str(taxonomy_key),
                int(root_message_id),
                str(root_sent_ref or ""),
                str(current_headline or ""),
                str(current_facts_json or "{}"),
                int(opened_ts),
                int(updated_ts),
                int(last_delivery_ts),
                int(update_count),
                str(status or "active"),
            ),
        )


def load_active_breaking_story_clusters(*, since_ts: int, limit: int = 200) -> List[Dict[str, object]]:
    resolved_limit = max(1, min(int(limit), 1000))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                cluster_id,
                cluster_key,
                topic_key,
                taxonomy_key,
                root_message_id,
                root_sent_ref,
                current_headline,
                current_facts_json,
                opened_ts,
                updated_ts,
                last_delivery_ts,
                update_count,
                status
            FROM breaking_story_clusters
            WHERE status = 'active' AND updated_ts >= ?
            ORDER BY updated_ts DESC, cluster_id DESC
            LIMIT ?
            """,
            (int(since_ts), resolved_limit),
        ).fetchall()
    return _to_breaking_story_cluster_rows(rows)


def save_breaking_story_event(
    *,
    cluster_id: str,
    source_channel_id: str,
    source_message_id: int,
    delivery_message_id: int,
    normalized_text: str,
    display_text: str,
    facts_json: str,
    delta_kind: str,
    created_ts: int | None = None,
) -> None:
    ts = int(created_ts if created_ts is not None else time.time())
    with _transaction() as conn:
        conn.execute(
            """
            INSERT INTO breaking_story_events (
                cluster_id,
                source_channel_id,
                source_message_id,
                delivery_message_id,
                normalized_text,
                display_text,
                facts_json,
                delta_kind,
                created_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(cluster_id),
                str(source_channel_id),
                int(source_message_id),
                int(delivery_message_id),
                re.sub(r"\s+", " ", str(normalized_text or "")).strip(),
                str(display_text or ""),
                str(facts_json or "{}"),
                str(delta_kind or ""),
                ts,
            ),
        )


def load_breaking_story_events(*, cluster_id: str, limit: int = 20) -> List[Dict[str, object]]:
    resolved_limit = max(1, min(int(limit), 500))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                cluster_id,
                source_channel_id,
                source_message_id,
                delivery_message_id,
                normalized_text,
                display_text,
                facts_json,
                delta_kind,
                created_ts
            FROM breaking_story_events
            WHERE cluster_id = ?
            ORDER BY created_ts DESC, id DESC
            LIMIT ?
            """,
            (str(cluster_id), resolved_limit),
        ).fetchall()
    out = _to_breaking_story_event_rows(rows)
    out.reverse()
    return out


def count_active_breaking_story_clusters(*, since_ts: int) -> int:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM breaking_story_clusters
            WHERE status = 'active' AND updated_ts >= ?
            """,
            (int(since_ts),),
        ).fetchone()
    return int(row[0] if row else 0)


def load_breaking_story_decision_counts(*, since_ts: int) -> Dict[str, int]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT delta_kind, COUNT(*) AS total
            FROM breaking_story_events
            WHERE created_ts >= ?
            GROUP BY delta_kind
            """,
            (int(since_ts),),
        ).fetchall()
    counts: Dict[str, int] = {}
    for row in rows:
        counts[str(row["delta_kind"] or "")] = int(row["total"] or 0)
    return counts


def purge_breaking_story_history(*, older_than_ts: int) -> Tuple[int, int]:
    cutoff = int(max(0, older_than_ts))
    with _transaction() as conn:
        deleted_events = conn.execute(
            "DELETE FROM breaking_story_events WHERE created_ts < ?",
            (cutoff,),
        )
        deleted_clusters = conn.execute(
            "DELETE FROM breaking_story_clusters WHERE updated_ts < ?",
            (cutoff,),
        )
    return int(deleted_clusters.rowcount or 0), int(deleted_events.rowcount or 0)
