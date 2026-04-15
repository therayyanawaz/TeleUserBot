from __future__ import annotations

from datetime import datetime

import pytest

import main


def test_next_digest_window_end_to_process_uses_oldest_pending_when_no_checkpoint(monkeypatch):
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "get_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: 1810)

    assert main._next_digest_window_end_to_process(now_ts=3700) == 3600


def test_next_digest_window_end_to_process_prioritizes_stranded_older_backlog(monkeypatch):
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "_get_last_completed_digest_window_end", lambda: 3600)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: 1200)

    assert main._next_digest_window_end_to_process(now_ts=5400) == 1800


def test_digest_backlog_snapshot_marks_stale_recovery_due(monkeypatch):
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: 1200)

    snapshot = main._digest_backlog_snapshot(now_ts=9000)

    assert snapshot["oldest_closed_window_end"] == 1800
    assert snapshot["stale_intervals"] == 4
    assert snapshot["recovery_due"] is True


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
        main._rolling_digest_title(79200, 81000, interval_minutes=30, part_index=1, part_count=2)
        == "30-Minute Digest (22:00-22:30)"
    )
    assert (
        main._rolling_digest_title(79200, 81000, interval_minutes=30, part_index=2, part_count=2)
        == "30-Minute Digest (22:00-22:30) • Part 2/2"
    )


def test_rolling_digest_title_uses_real_interval(monkeypatch):
    monkeypatch.setattr(main, "_digest_window_label", lambda *_args, **_kwargs: "22:00-23:00")

    assert (
        main._rolling_digest_title(79200, 82800, interval_minutes=60, part_index=1, part_count=1)
        == "60-Minute Digest (22:00-23:00)"
    )


@pytest.mark.asyncio
async def test_build_window_digest_messages_merges_short_window_groups_into_one_message(monkeypatch):
    async def fake_run_post_processors(text, context):
        return text

    async def fake_hydrate_digest_posts(rows):
        return rows

    async def fake_create_digest_summary_result(_posts, _auth_manager, interval_minutes):
        idx = fake_create_digest_summary_result.calls
        fake_create_digest_summary_result.calls += 1
        if idx == 0:
            html = (
                "<b>Top headlines from the last 30 minutes</b><br>"
                "• First headline from group one.<br>"
                "• Second headline from group one."
            )
        else:
            html = (
                "<b>Top headlines from the last 30 minutes</b><br>"
                "• Third headline from group two.<br>"
                "• Fourth headline from group two."
            )
        return type(
            "Result",
            (),
            {
                "html": html,
                "copy_origin": "ai",
                "fallback_reason": "",
                "major_block_count": 1,
                "timeline_item_count": 0,
                "noise_stripped_count": 0,
                "translation_applied_count": 0,
                "citation_stripped_count": 0,
                "duplicate_collapsed_count": 0,
            },
        )()

    fake_create_digest_summary_result.calls = 0

    monkeypatch.setattr(main, "_digest_input_token_budget", lambda: 1000)
    monkeypatch.setattr(main, "_digest_window_page_size", lambda: 100)
    monkeypatch.setattr(main, "_digest_send_chunk_size", lambda: 3600)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "_run_post_processors", fake_run_post_processors)
    monkeypatch.setattr(
        main,
        "_iter_digest_row_groups",
        lambda *_args, **_kwargs: iter([[{"id": 1}], [{"id": 2}]]),
    )
    monkeypatch.setattr(main, "_hydrate_digest_posts", fake_hydrate_digest_posts)
    monkeypatch.setattr(main, "create_digest_summary_result", fake_create_digest_summary_result)

    messages, stats = await main._build_window_digest_messages(
        lambda after_id=0, limit=100: [],
        total_updates=4,
        interval_minutes=30,
        title_builder=lambda part_index, part_count: main._rolling_digest_title(
            79200,
            81000,
            interval_minutes=30,
            part_index=part_index,
            part_count=part_count,
        ),
        context={},
    )

    assert len(messages) == 1
    assert "First headline from group one." in messages[0]
    assert "Fourth headline from group two." in messages[0]
    assert "Part 1/" not in messages[0]
    assert stats["part_count"] == 1


@pytest.mark.asyncio
async def test_enrich_headline_rail_items_rewrites_ambiguous_line_from_search(monkeypatch):
    async def fake_search_recent_messages(*_args, **_kwargs):
        return [
            {
                "timestamp": 20,
                "source": "Desk",
                "text": "John D. Rockefeller wiped out traditional and herbal healing in favor of a lab-only model.",
            }
        ]

    async def fake_search_recent_news_web(*_args, **_kwargs):
        return [
            {
                "timestamp": 30,
                "source": "Web:History",
                "text": "John D. Rockefeller backed the Flexner-era reforms that reshaped mainstream medicine.",
                "is_web": True,
            }
        ]

    async def fake_headline_context_ai_rewrite(headline, evidence_lines):
        assert headline.startswith("He wiped out")
        assert any("Rockefeller" in line for line in evidence_lines)
        return "John D. Rockefeller wiped out traditional and herbal healing and redefined real medicine."

    monkeypatch.setattr(main, "digest_output_style", lambda _interval: "headline_rail")
    monkeypatch.setattr(main, "_digest_headline_context_enabled", lambda: True)
    monkeypatch.setattr(main, "_digest_headline_context_max_items", lambda: 2)
    monkeypatch.setattr(main, "_digest_headline_context_hours_back", lambda: 168)
    monkeypatch.setattr(main, "_digest_headline_context_telegram_max_messages", lambda: 8)
    monkeypatch.setattr(main, "_digest_headline_context_web_max_results", lambda: 6)
    monkeypatch.setattr(main, "_query_web_allowed_domains", lambda: ["reuters.com"])
    monkeypatch.setattr(main, "_require_client", lambda: object())
    monkeypatch.setattr(main, "monitored_source_chat_ids", [12345])
    monkeypatch.setattr(main, "search_recent_messages", fake_search_recent_messages)
    monkeypatch.setattr(main, "search_recent_news_web", fake_search_recent_news_web)
    monkeypatch.setattr(main, "_headline_context_ai_rewrite", fake_headline_context_ai_rewrite)

    resolved = await main._enrich_headline_rail_items(
        [
            "He wiped out traditional and herbal healing and redefined real medicine as whatever served his interests.",
            "Port reopened after overnight inspection.",
        ],
        interval_minutes=30,
    )

    assert resolved[0].startswith("John D. Rockefeller wiped out")
    assert resolved[1] == "Port reopened after overnight inspection."


@pytest.mark.asyncio
async def test_enrich_headline_rail_items_falls_back_to_subject_candidate(monkeypatch):
    async def fake_search_recent_messages(*_args, **_kwargs):
        return [
            {
                "timestamp": 20,
                "source": "Desk",
                "text": "John D. Rockefeller used his wealth to back medical reforms and marginalize herbal healing.",
            },
            {
                "timestamp": 19,
                "source": "Desk",
                "text": "John D. Rockefeller helped redefine mainstream medicine around institutions he funded.",
            },
        ]

    async def fake_search_recent_news_web(*_args, **_kwargs):
        return []

    async def fake_headline_context_ai_rewrite(_headline, _evidence_lines):
        return None

    monkeypatch.setattr(main, "digest_output_style", lambda _interval: "headline_rail")
    monkeypatch.setattr(main, "_digest_headline_context_enabled", lambda: True)
    monkeypatch.setattr(main, "_digest_headline_context_max_items", lambda: 2)
    monkeypatch.setattr(main, "_digest_headline_context_hours_back", lambda: 168)
    monkeypatch.setattr(main, "_digest_headline_context_telegram_max_messages", lambda: 8)
    monkeypatch.setattr(main, "_digest_headline_context_web_max_results", lambda: 0)
    monkeypatch.setattr(main, "_query_web_allowed_domains", lambda: [])
    monkeypatch.setattr(main, "_require_client", lambda: object())
    monkeypatch.setattr(main, "monitored_source_chat_ids", [12345])
    monkeypatch.setattr(main, "search_recent_messages", fake_search_recent_messages)
    monkeypatch.setattr(main, "search_recent_news_web", fake_search_recent_news_web)
    monkeypatch.setattr(main, "_headline_context_ai_rewrite", fake_headline_context_ai_rewrite)

    resolved = await main._enrich_headline_rail_items(
        ["He wiped out traditional and herbal healing and redefined real medicine as whatever served his interests."],
        interval_minutes=30,
    )

    assert resolved == [
        "John D. Rockefeller wiped out traditional and herbal healing and redefined real medicine as whatever served his interests."
    ]


@pytest.mark.asyncio
async def test_flush_digest_queue_once_passes_computed_window_start_to_claim(monkeypatch):
    claim_args = {}

    async def fake_build_window_digest_messages(*args, **kwargs):
        return (
            ["<b>Top headlines from the last 30 minutes</b><br>• Headline."],
            {
                "part_count": 1,
                "quiet": False,
                "response_chars": 20,
                "ai_parts": 1,
                "fallback_parts": 0,
                "major_block_count": 1,
                "timeline_item_count": 0,
                "noise_stripped_count": 0,
                "translation_applied_count": 0,
                "citation_stripped_count": 0,
                "duplicate_collapsed_count": 0,
            },
        )

    async def fake_send_digest_message_sequence(messages):
        return None

    async def fake_pin_digest_message_ref(ref, *, kind):
        return None

    def fake_claim_digest_window(window_end_ts, *, batch_id=None, window_start_ts=None):
        claim_args["window_end_ts"] = window_end_ts
        claim_args["window_start_ts"] = window_start_ts
        return "batch-1", 1

    monkeypatch.setattr(main, "load_active_digest_window_claim", lambda: ("", None, 0))
    monkeypatch.setattr(main, "_next_digest_window_end_to_process", lambda: 3600)
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: None)
    monkeypatch.setattr(main, "claim_digest_window", fake_claim_digest_window)
    monkeypatch.setattr(main, "_build_window_digest_messages", fake_build_window_digest_messages)
    monkeypatch.setattr(main, "_send_digest_message_sequence", fake_send_digest_message_sequence)
    monkeypatch.setattr(main, "_pin_digest_message_ref", fake_pin_digest_message_ref)
    monkeypatch.setattr(main, "ack_digest_window", lambda batch_id, *, window_end_ts: 1)
    monkeypatch.setattr(main, "set_last_digest_timestamp", lambda _ts: None)
    monkeypatch.setattr(main, "count_pending", lambda: 0)
    monkeypatch.setattr(main, "get_quota_health", lambda: {"status": "healthy"})

    processed = await main._flush_digest_queue_once()

    assert processed is True
    assert claim_args == {"window_end_ts": 3600, "window_start_ts": 1800}


@pytest.mark.asyncio
async def test_flush_digest_queue_once_uses_catchup_recovery_for_stale_backlog(monkeypatch):
    claim_args = {}
    ack_args = {}

    async def fake_build_catchup_digest_messages(*args, **kwargs):
        return (
            ["<b>Catch-up Digest (00:00-02:30)</b><br>Backlog condensed cleanly."],
            {
                "part_count": 1,
                "quiet": False,
                "response_chars": 40,
                "ai_parts": 1,
                "fallback_parts": 0,
                "major_block_count": 1,
                "timeline_item_count": 0,
                "noise_stripped_count": 0,
                "translation_applied_count": 0,
                "citation_stripped_count": 0,
                "duplicate_collapsed_count": 0,
            },
        )

    async def fake_send_digest_message_sequence(messages):
        return None

    async def fake_pin_digest_message_ref(ref, *, kind):
        raise AssertionError("catch-up recovery should not rotate hourly pin")

    def fake_claim_digest_window(window_end_ts, *, batch_id=None, window_start_ts=None):
        claim_args["window_end_ts"] = window_end_ts
        claim_args["window_start_ts"] = window_start_ts
        return "batch-catchup", 5

    def fake_ack_digest_window(batch_id, *, window_end_ts):
        ack_args["batch_id"] = batch_id
        ack_args["window_end_ts"] = window_end_ts
        return 5

    monkeypatch.setattr(main, "load_active_digest_window_claim", lambda: ("", None, 0))
    monkeypatch.setattr(main, "_digest_interval_seconds", lambda: 1800)
    monkeypatch.setattr(main, "peek_oldest_pending_digest_timestamp", lambda: 1200)
    monkeypatch.setattr(main.time, "time", lambda: 9000)
    monkeypatch.setattr(main, "claim_digest_window", fake_claim_digest_window)
    monkeypatch.setattr(main, "_build_catchup_digest_messages", fake_build_catchup_digest_messages)
    monkeypatch.setattr(main, "_send_digest_message_sequence", fake_send_digest_message_sequence)
    monkeypatch.setattr(main, "_pin_digest_message_ref", fake_pin_digest_message_ref)
    monkeypatch.setattr(main, "ack_digest_window", fake_ack_digest_window)
    monkeypatch.setattr(main, "set_last_digest_timestamp", lambda _ts: None)
    monkeypatch.setattr(main, "count_pending", lambda: 3)
    monkeypatch.setattr(main, "get_quota_health", lambda: {"status": "healthy"})

    processed = await main._flush_digest_queue_once()

    assert processed is True
    assert claim_args == {"window_end_ts": 9000, "window_start_ts": 0}
    assert ack_args == {"batch_id": "batch-catchup", "window_end_ts": 9000}
    assert main.digest_recovery_state["active"] is False


@pytest.mark.asyncio
async def test_build_catchup_digest_messages_uses_story_mode_and_compacts(monkeypatch):
    async def fake_run_post_processors(text, context):
        return text

    async def fake_hydrate_digest_posts(rows):
        return rows

    async def fake_create_digest_summary_result(posts, _auth_manager, interval_minutes):
        fake_create_digest_summary_result.calls.append((len(posts), interval_minutes))
        if len(posts) == 2 and interval_minutes > 60:
            html = (
                "<b>Backlog compresses into one Tehran catch-up brief</b><br>"
                "Several delayed windows were condensed into one compact recap covering strikes, interceptions, and official responses.<br>"
                "• Air defenses were activated over the city.<br>"
                "• Officials confirmed fresh restrictions."
            )
        else:
            html = (
                "<b>Tehran absorbs another late-night wave of alerts</b><br>"
                "Air defenses lit up again while officials issued new restrictions.<br>"
                "• Air defenses were activated over the city.<br>"
                "• Officials confirmed fresh restrictions."
            )
        return type(
            "Result",
            (),
            {
                "html": html,
                "copy_origin": "ai",
                "fallback_reason": "",
                "major_block_count": 1,
                "timeline_item_count": 0,
                "noise_stripped_count": 0,
                "translation_applied_count": 0,
                "citation_stripped_count": 0,
                "duplicate_collapsed_count": 0,
            },
        )()

    fake_create_digest_summary_result.calls = []

    monkeypatch.setattr(main, "_digest_input_token_budget", lambda: 1000)
    monkeypatch.setattr(main, "_digest_window_page_size", lambda: 100)
    monkeypatch.setattr(main, "_digest_send_chunk_size", lambda: 3600)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "_run_post_processors", fake_run_post_processors)
    monkeypatch.setattr(
        main,
        "_iter_digest_row_groups",
        lambda *_args, **_kwargs: iter(
            [
                [{"id": 1, "timestamp": 1200, "raw_text": "Group one raw text."}],
                [{"id": 2, "timestamp": 4800, "raw_text": "Group two raw text."}],
            ]
        ),
    )
    monkeypatch.setattr(main, "_hydrate_digest_posts", fake_hydrate_digest_posts)
    monkeypatch.setattr(main, "create_digest_summary_result", fake_create_digest_summary_result)

    messages, stats = await main._build_catchup_digest_messages(
        lambda after_id=0, limit=100: [],
        total_updates=12,
        window_start_ts=0,
        window_end_ts=7200,
        context={"surface": "rolling_digest_catchup"},
    )

    assert len(messages) == 1
    assert "<b>📰 Catch-up Digest (" in messages[0]
    assert "updates reviewed" in messages[0]
    assert "Part 1/" not in messages[0]
    assert stats["part_count"] == 1
    assert fake_create_digest_summary_result.calls[-1][1] > 60


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
