from __future__ import annotations

from datetime import datetime

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
        title="30-Minute Digest (22:00-22:30)",
        interval_minutes=30,
    )

    lines = formatted.splitlines()
    assert lines[0] == "<b>📰 30-Minute Digest (22:00-22:30)</b>"
    assert "41 headlines tracked" in lines[1]
    assert "41 headlines tracked" in formatted
    assert "headlines from" not in formatted


def test_format_digest_message_uses_story_metadata_for_hourly_digest():
    formatted = main._format_digest_message(
        "<b>Central Israel Impact Reports</b><br>Initial impact reports concentrated around Petah Tikva.",
        total_updates=12,
        sources=[],
        title="60-Minute Digest (22:00-23:00)",
        interval_minutes=60,
    )

    lines = formatted.splitlines()
    assert lines[0] == "<b>📰 60-Minute Digest (22:00-23:00)</b>"
    assert "12 updates reviewed" in lines[1]
    assert "headlines tracked" not in formatted


def test_format_digest_message_uses_runtime_timezone(monkeypatch):
    monkeypatch.setattr(
        main,
        "runtime_now",
        lambda: datetime.fromisoformat("2026-04-05T09:30:00+05:30"),
    )

    formatted = main._format_digest_message(
        "<b>Headline</b><br>Story.",
        total_updates=5,
        sources=[],
        title="30-Minute Digest (09:00-09:30)",
        interval_minutes=30,
    )

    lines = formatted.splitlines()
    assert "09:30 UTC+05:30" in lines[1]


def test_digest_window_label_uses_runtime_timezone(monkeypatch):
    monkeypatch.setattr(
        main,
        "runtime_timezone",
        lambda: datetime.fromisoformat("2026-04-05T00:00:00+05:30").tzinfo,
    )

    assert main._digest_window_label(0, 1800) == "05:30-06:00"


def test_rolling_digest_title_omits_part_label_for_first_message(monkeypatch):
    monkeypatch.setattr(main, "_digest_window_label", lambda *_args, **_kwargs: "22:00-22:30")

    assert (
        main._rolling_digest_title(79200, 81000, part_index=1, part_count=2)
        == "30-Minute Digest (22:00-22:30)"
    )
    assert (
        main._rolling_digest_title(79200, 81000, part_index=2, part_count=2)
        == "30-Minute Digest (22:00-22:30) • Part 2/2"
    )


def test_split_digest_body_blocks_repeats_story_context_across_parts():
    digest_body = (
        "<b>Central Israel Impact Reports</b><br>"
        "Initial impact reports concentrated around Petah Tikva across the window. "
        "Emergency crews and air-defense activity spread across central Israel through the morning.<br>"
        "• Preliminary reports pointed to direct hits in Petah Tikva.<br>"
        "• Damage assessments moved across multiple neighborhoods.<br>"
        "• Emergency crews were dispatched to several sites.<br>"
        "• Follow-on alerts stayed active across the district.<br>"
        "<br><i>Also moving</i><br>"
        "• Iraqi drone strike claims targeted the US base in Baghdad."
    )
    chunks = main._split_digest_body_blocks(
        digest_body,
        max_chars=300,
    )

    assert len(chunks) >= 2
    assert all(len(chunk) <= 300 for chunk in chunks)
    assert all("<b>Central Israel Impact Reports</b>" in chunk for chunk in chunks)
    assert all("Initial impact reports concentrated around Petah Tikva" in chunk for chunk in chunks)
    assert "Also moving" in chunks[-1]


def test_split_digest_body_blocks_preserves_headline_rail_across_parts():
    digest_body = (
        "<b>Top headlines from the last 30 minutes</b><br>"
        + "<br>".join(
            f"• Headline {idx} closes cleanly with a concrete fact."
            for idx in range(1, 8)
        )
    )

    chunks = main._split_digest_body_blocks(
        digest_body,
        max_chars=220,
    )

    assert len(chunks) >= 2
    assert all(len(chunk) <= 220 for chunk in chunks)
    assert all("<b>Top headlines from the last 30 minutes</b>" in chunk for chunk in chunks)
    assert all("Headline" in chunk for chunk in chunks)
