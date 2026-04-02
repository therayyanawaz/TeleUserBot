from __future__ import annotations

import main


def test_next_digest_window_end_to_process_uses_oldest_pending_when_no_checkpoint(monkeypatch):
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "get_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: 1810)

    assert main._next_digest_window_end_to_process(now_ts=3700) == 3600


def test_compute_next_scheduler_delay_seconds_aligns_to_next_half_hour_boundary(monkeypatch):
    monkeypatch.setattr(main, "_next_digest_window_end_to_process", lambda now_ts=None: None)
    monkeypatch.setattr(main, "_next_digest_boundary_timestamp", lambda now_ts=None: 5400)
    monkeypatch.setattr(main.time, "time", lambda: 3610)
    main.digest_retry_backoff_seconds = 0

    assert main._compute_next_scheduler_delay_seconds() == 1790


def test_compute_next_scheduler_delay_seconds_runs_immediately_when_window_is_due(monkeypatch):
    monkeypatch.setattr(main, "_next_digest_window_end_to_process", lambda now_ts=None: 3600)
    main.digest_retry_backoff_seconds = 0

    assert main._compute_next_scheduler_delay_seconds() == 0
