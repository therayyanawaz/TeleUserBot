from __future__ import annotations

import sqlite3

import db


def test_inbound_job_lifecycle(isolated_db):
    inserted = db.enqueue_inbound_job(
        job_key="single:123:456",
        stage="triage",
        channel_id="123",
        message_id=456,
        payload_json='{"kind":"single","message_ids":[456]}',
        priority=50,
        max_retries=4,
    )
    assert inserted is True

    duplicate_insert = db.enqueue_inbound_job(
        job_key="single:123:456",
        stage="triage",
        channel_id="123",
        message_id=456,
        payload_json='{"kind":"single","message_ids":[456]}',
    )
    assert duplicate_insert is False

    triage_jobs = db.claim_inbound_jobs("triage", 1, worker_id="triage-worker")
    assert len(triage_jobs) == 1
    assert triage_jobs[0]["status"] == "in_progress"

    db.advance_inbound_job(
        int(triage_jobs[0]["id"]),
        next_stage="ai_decision",
        payload_json='{"kind":"single","message_ids":[456],"candidate_text":"hello"}',
        priority=90,
    )

    ai_jobs = db.claim_inbound_jobs("ai_decision", 1, worker_id="ai-worker")
    assert len(ai_jobs) == 1
    assert ai_jobs[0]["priority"] == 90

    db.complete_inbound_job(int(ai_jobs[0]["id"]))
    counts = db.load_inbound_job_counts()
    assert counts["ai_decision"]["done"] == 1


def test_inbound_job_retry_and_dead_letter(isolated_db):
    inserted = db.enqueue_inbound_job(
        job_key="single:999:1001",
        stage="triage",
        channel_id="999",
        message_id=1001,
        payload_json='{"kind":"single","message_ids":[1001]}',
        priority=50,
        max_retries=2,
    )
    assert inserted is True

    jobs = db.claim_inbound_jobs("triage", 1, worker_id="triage-worker")
    job_id = int(jobs[0]["id"])

    first_retry = db.retry_or_dead_letter_inbound_job(
        job_id,
        error_text="temporary failure",
        retry_delay_seconds=1,
    )
    assert first_retry is True

    second_retry = db.retry_or_dead_letter_inbound_job(
        job_id,
        error_text="permanent failure",
        retry_delay_seconds=1,
    )
    assert second_retry is False

    failures = db.load_recent_inbound_job_failures(limit=5)
    assert failures[0]["status"] == "dead"
    assert failures[0]["retry_count"] == 2


def test_ai_decision_cache_round_trip(isolated_db):
    db.ai_decision_cache_set(
        normalized_hash="abc123",
        prompt_version="v1",
        model="gpt-test",
        decision_json='{"action":"deliver"}',
    )

    cached = db.ai_decision_cache_get(
        normalized_hash="abc123",
        prompt_version="v1",
        model="gpt-test",
        max_age_hours=72,
    )
    assert cached == '{"action":"deliver"}'


def test_digest_writes_do_not_depend_on_created_at_default(tmp_path, monkeypatch):
    db_path = tmp_path / "teleuserbot-legacy-defaults.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE digest_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            source_name TEXT,
            raw_text TEXT NOT NULL,
            message_link TEXT,
            timestamp INTEGER NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,
            batch_id TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(channel_id, message_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE digest_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            source_name TEXT,
            raw_text TEXT NOT NULL,
            message_link TEXT,
            timestamp INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(channel_id, message_id)
        )
        """
    )
    conn.commit()
    conn.close()

    db.save_to_digest_queue(
        "chan-1",
        101,
        "Queued text",
        timestamp=1700000000,
        source_name="Source",
        message_link="https://t.me/example/101",
    )
    db.save_to_digest_archive(
        "chan-2",
        202,
        "Archived text",
        timestamp=1700000100,
        source_name="Source",
        message_link="https://t.me/example/202",
    )

    conn = sqlite3.connect(str(db_path))
    queue_row = conn.execute(
        "SELECT timestamp, created_at FROM digest_queue WHERE channel_id = 'chan-1' AND message_id = 101"
    ).fetchone()
    archive_rows = conn.execute(
        "SELECT timestamp, created_at FROM digest_archive ORDER BY channel_id, message_id"
    ).fetchall()
    conn.close()

    assert queue_row == (1700000000, 1700000000)
    assert archive_rows == [
        (1700000000, 1700000000),
        (1700000100, 1700000100),
    ]


def test_claim_digest_window_persists_active_window_and_pages_rows(isolated_db):
    db.save_to_digest_queue("chan-1", 101, "Window item one", timestamp=1810, source_name="Desk")
    db.save_to_digest_queue("chan-1", 102, "Window item two", timestamp=2500, source_name="Desk")
    db.save_to_digest_queue("chan-1", 103, "Next window item", timestamp=3700, source_name="Desk")

    batch_id, claimed = db.claim_digest_window(3600, window_start_ts=1800)

    assert batch_id
    assert claimed == 2

    active_batch_id, active_window_end, active_rows = db.load_active_digest_window_claim()
    assert active_batch_id == batch_id
    assert active_window_end == 3600
    assert active_rows == 2

    first_page = db.load_batch_rows_page(batch_id, limit=1)
    second_page = db.load_batch_rows_page(batch_id, after_id=first_page[-1]["id"], limit=10)

    assert len(first_page) == 1
    assert [row["message_id"] for row in first_page + second_page] == [101, 102]

    acked = db.ack_digest_window(batch_id, window_end_ts=3600)

    assert acked == 2
    assert db.get_meta(db.ROLLING_DIGEST_LAST_COMPLETED_KEY) == "3600"
    assert db.load_active_digest_window_claim() == ("", None, 0)
    assert db.count_pending() == 1


def test_claim_digest_window_respects_lower_bound_and_leaves_older_backlog_pending(isolated_db):
    db.save_to_digest_queue("chan-1", 201, "Older backlog item", timestamp=1200, source_name="Desk")
    db.save_to_digest_queue("chan-1", 202, "Current window item", timestamp=1810, source_name="Desk")
    db.save_to_digest_queue("chan-1", 203, "Current window item two", timestamp=2500, source_name="Desk")

    batch_id, claimed = db.claim_digest_window(3600, window_start_ts=1800)

    assert batch_id
    assert claimed == 2
    page = db.load_batch_rows_page(batch_id, limit=10)
    assert [row["message_id"] for row in page] == [202, 203]
    assert db.count_pending() == 1


def test_restore_digest_window_resets_claim_and_keeps_rows_pending(isolated_db):
    db.save_to_digest_queue("chan-1", 201, "Window item", timestamp=1810, source_name="Desk")

    batch_id, claimed = db.claim_digest_window(3600, window_start_ts=1800)
    restored = db.restore_digest_window(batch_id)

    assert claimed == 1
    assert restored == 1
    assert db.load_active_digest_window_claim() == ("", None, 0)
    assert db.count_pending() == 1


def test_ack_digest_window_does_not_rewind_last_completed_checkpoint(isolated_db):
    db.set_meta(db.ROLLING_DIGEST_LAST_COMPLETED_KEY, "3600")
    db.save_to_digest_queue("chan-1", 301, "Older backlog item", timestamp=1200, source_name="Desk")

    batch_id, claimed = db.claim_digest_window(1800, window_start_ts=0)
    acked = db.ack_digest_window(batch_id, window_end_ts=1800)

    assert claimed == 1
    assert acked == 1
    assert db.get_meta(db.ROLLING_DIGEST_LAST_COMPLETED_KEY) == "3600"


def test_claim_digest_window_for_catchup_excludes_current_open_window(isolated_db):
    db.save_to_digest_queue("chan-1", 401, "Older overdue item", timestamp=1200, source_name="Desk")
    db.save_to_digest_queue("chan-1", 402, "Latest closed window item", timestamp=3500, source_name="Desk")
    db.save_to_digest_queue("chan-1", 403, "Current open window item", timestamp=3610, source_name="Desk")

    batch_id, claimed = db.claim_digest_window(3600, window_start_ts=0)

    assert batch_id
    assert claimed == 2
    page = db.load_batch_rows_page(batch_id, limit=10)
    assert [row["message_id"] for row in page] == [401, 402]
    assert db.count_pending() == 1
