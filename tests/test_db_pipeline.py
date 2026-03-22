from __future__ import annotations

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
