from __future__ import annotations

import json
import logging
import sys

import main
from runtime_presenter import (
    RuntimeActivityFormatter,
    RuntimeEventView,
    build_runtime_event_view,
    render_runtime_event_text,
)


def _make_record(
    message: str,
    *,
    level: int = logging.INFO,
    logger_name: str = "tg_news_userbot",
    exc_info=None,
) -> logging.LogRecord:
    logger = logging.getLogger(logger_name)
    record = logger.makeRecord(
        logger_name,
        level,
        __file__,
        10,
        message,
        args=(),
        exc_info=exc_info,
    )
    record.created = 1775072206.0
    return record


def test_structured_event_renders_readable_activity_block():
    record = _make_record(
        json.dumps(
            {
                "event": "story_cluster_created",
                "source": "Basira Press",
                "severity": "high",
                "message_id": 13174,
                "cluster_id": "a4f90cff1ab644dd908240b849c4c8e9",
                "channel_id": "-1001647229236",
            },
            sort_keys=True,
        )
    )

    view = build_runtime_event_view(record)
    text = render_runtime_event_text(view, surface="file", color=False, width=112)

    assert view.category == "story"
    assert "Story cluster created" in text
    assert "Basira Press" in text
    assert "Cluster Id:" in text
    assert '"event"' not in text
    assert "{" not in text


def test_exception_event_renders_traceback_cleanly():
    try:
        raise ValueError("pipeline exploded")
    except ValueError:
        record = _make_record(
            "Pipeline stage failed",
            level=logging.ERROR,
            exc_info=sys.exc_info(),
        )

    formatter = RuntimeActivityFormatter(surface="file", sanitize=main._sanitize_log_text)
    text = formatter.format(record)

    assert "Pipeline stage failed" in text
    assert "Traceback:" in text
    assert "ValueError: pipeline exploded" in text


def test_console_formatter_adds_ansi_but_file_formatter_does_not():
    record = _make_record("Readable console message")

    console_formatter = RuntimeActivityFormatter(
        surface="console",
        sanitize=main._sanitize_log_text,
        color=True,
    )
    file_formatter = RuntimeActivityFormatter(
        surface="file",
        sanitize=main._sanitize_log_text,
        color=False,
    )

    console_text = console_formatter.format(record)
    file_text = file_formatter.format(record)

    assert "\x1b[" in console_text
    assert "\x1b[" not in file_text


def test_prettified_output_redacts_sensitive_tokens():
    record = _make_record(
        "Bearer bearer-secret-token TG_USERBOT_AUTH_JSON_B64=super-secret-b64 "
        'BOT_DESTINATION_TOKEN=123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd {"access_token":"access-secret"} '
        "http://localhost:1455/auth/callback?code=oauth-secret-code"
    )

    formatter = RuntimeActivityFormatter(surface="file", sanitize=main._sanitize_log_text)
    text = formatter.format(record).replace("\n", " ")

    assert "bearer-secret-token" not in text
    assert "super-secret-b64" not in text
    assert "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd" not in text
    assert "access-secret" not in text
    assert "oauth-secret-code" not in text
    assert "[REDACTED]" in text


def test_narrow_width_wrap_preserves_hierarchy():
    view = RuntimeEventView(
        created_at=1775072206.0,
        timestamp_short="19:36:46",
        timestamp_full="2026-04-01 19:36:46",
        level_name="INFO",
        levelno=logging.INFO,
        logger_name="tg_news_userbot",
        category="digest",
        title="Digest scheduler online with a very long headline that needs wrapping",
        summary="The scheduler resumed after a long backlog and is now draining the queue with structured updates.",
        badges=("DIGEST", "RUNNING"),
        details=(
            ("Queue", "1439 pending / 0 inflight"),
            ("Window", "Daily 00:00 UTC with hourly batches and queue clear disabled"),
        ),
        traceback_text="",
        event_name="digest_scheduler_start",
    )

    text = render_runtime_event_text(view, surface="file", color=False, width=68)
    lines = text.splitlines()

    assert len(lines) > 4
    assert any(line.startswith("  ") for line in lines[1:])
    assert any(line.startswith("    ") for line in lines[1:])
