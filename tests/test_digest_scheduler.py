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


def test_format_digest_message_uses_clean_two_line_header():
    formatted = main._format_digest_message(
        "<b>Central Israel Impact Reports</b><br>Initial impact reports concentrated around Petah Tikva.",
        total_updates=41,
        sources=[],
        title="30-Minute Digest • 22:00-22:30 • Part 1/2",
    )

    lines = formatted.splitlines()
    assert lines[0] == "<b>30-Minute Digest • 22:00-22:30 • Part 1/2</b>"
    assert "41 updates reviewed" in lines[1]
    assert "41 updates reviewed" in formatted
    assert "headlines from" not in formatted


def test_split_digest_body_blocks_preserves_block_boundaries():
    first_block = (
        "<b>Central Israel Impact Reports</b><br>"
        "Initial impact reports concentrated around Petah Tikva.<br>"
        "• Preliminary reports pointed to direct hits in Petah Tikva."
    )
    second_block = (
        "<b>Baghdad Under Fire</b><br>"
        "Explosions and strike reports built around Victoria Base.<br>"
        "• Iraqi drone strike claims targeted the US base in Baghdad."
    )
    chunks = main._split_digest_body_blocks(
        f"{first_block}<br><br>{second_block}",
        max_chars=len(first_block) + 20,
    )

    assert len(chunks) == 2
    assert "<b>Central Israel Impact Reports</b>" in chunks[0]
    assert "<b>Baghdad Under Fire</b>" not in chunks[0]
    assert "<b>Baghdad Under Fire</b>" in chunks[1]
