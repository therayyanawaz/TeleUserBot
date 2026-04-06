from __future__ import annotations

import asyncio
import json
import logging

import pytest

import main


def _flush_runtime_handlers() -> None:
    for handler in (main._runtime_file_handler, main._error_handler, main._console_handler):
        if handler is not None:
            handler.flush()


def _structured_payloads(caplog: pytest.LogCaptureFixture, event: str) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        try:
            payload = json.loads(record.getMessage())
        except json.JSONDecodeError:
            continue
        if payload.get("event") == event:
            payloads.append(payload)
    return payloads


@pytest.fixture(autouse=True)
def _isolated_runtime_logging(tmp_path, monkeypatch):
    main._reset_runtime_logging()
    main.pipeline_worker_tasks.clear()
    main.pipeline_query_web_semaphore = None
    main.pipeline_media_semaphore = None
    main.startup_phase = "booting"
    main.startup_ready = False
    main.startup_error = ""

    runtime_log = tmp_path / "runtime.log"
    error_log = tmp_path / "errors.log"
    monkeypatch.setattr(main, "RUNTIME_LOG_PATH", runtime_log)
    monkeypatch.setattr(main, "ERROR_LOG_PATH", error_log)
    monkeypatch.setattr(main, "_LOG_ROTATION_MAX_BYTES", 10 * 1024 * 1024)
    monkeypatch.setattr(main, "_LOG_ROTATION_BACKUP_COUNT", 5)

    yield {"runtime_log": runtime_log, "error_log": error_log}

    main.pipeline_worker_tasks.clear()
    main.pipeline_query_web_semaphore = None
    main.pipeline_media_semaphore = None
    main._reset_runtime_logging()


def test_configure_runtime_logging_deletes_old_logs(_isolated_runtime_logging):
    runtime_log = _isolated_runtime_logging["runtime_log"]
    error_log = _isolated_runtime_logging["error_log"]

    runtime_log.write_text("old runtime log", encoding="utf-8")
    (runtime_log.parent / "runtime.log.1").write_text("old runtime backup", encoding="utf-8")
    error_log.write_text("old error log", encoding="utf-8")
    (error_log.parent / "errors.log.1").write_text("old error backup", encoding="utf-8")

    main._configure_runtime_logging()
    main.LOGGER.info("fresh runtime entry")
    main.LOGGER.error("fresh error entry")
    _flush_runtime_handlers()

    assert runtime_log.exists()
    assert error_log.exists()
    assert not (runtime_log.parent / "runtime.log.1").exists()
    assert not (error_log.parent / "errors.log.1").exists()
    assert "old runtime backup" not in runtime_log.read_text(encoding="utf-8")
    assert "old error log" not in error_log.read_text(encoding="utf-8")


def test_runtime_log_rotation_caps_backups(_isolated_runtime_logging, monkeypatch):
    runtime_log = _isolated_runtime_logging["runtime_log"]
    error_log = _isolated_runtime_logging["error_log"]
    monkeypatch.setattr(main, "_LOG_ROTATION_MAX_BYTES", 300)
    monkeypatch.setattr(main, "_LOG_ROTATION_BACKUP_COUNT", 2)

    main._configure_runtime_logging()
    assert main._console_handler is not None
    main._console_handler.setLevel(logging.CRITICAL)

    for index in range(120):
        main.LOGGER.debug("runtime rotation %03d %s", index, "x" * 96)
        main.LOGGER.error("error rotation %03d %s", index, "y" * 96)

    _flush_runtime_handlers()

    runtime_files = sorted(path.name for path in runtime_log.parent.glob("runtime.log*"))
    error_files = sorted(path.name for path in error_log.parent.glob("errors.log*"))

    assert runtime_files
    assert error_files
    assert len(runtime_files) <= 3
    assert len(error_files) <= 3
    assert "runtime.log.3" not in runtime_files
    assert "errors.log.3" not in error_files


def test_runtime_log_captures_debug_but_console_stays_info(
    _isolated_runtime_logging,
    capsys,
):
    runtime_log = _isolated_runtime_logging["runtime_log"]
    main._configure_runtime_logging()

    third_party_logger = logging.getLogger("httpx")
    previous_level = third_party_logger.level
    third_party_logger.setLevel(logging.DEBUG)
    try:
        main.LOGGER.debug("debug only")
        main.LOGGER.info("info visible")
        third_party_logger.debug("third party debug")
        _flush_runtime_handlers()
    finally:
        third_party_logger.setLevel(previous_level)

    captured = capsys.readouterr()
    console_output = captured.out + captured.err
    runtime_text = runtime_log.read_text(encoding="utf-8")

    assert "debug only" in runtime_text
    assert "info visible" in runtime_text
    assert "third party debug" in runtime_text
    assert "info visible" in console_output
    assert "debug only" not in console_output
    assert "third party debug" not in console_output


def test_runtime_logs_format_structured_events_as_activity_blocks(
    _isolated_runtime_logging,
    capsys,
):
    runtime_log = _isolated_runtime_logging["runtime_log"]
    error_log = _isolated_runtime_logging["error_log"]
    main._configure_runtime_logging()

    main.log_structured(
        main.LOGGER,
        "story_cluster_created",
        channel_id="-1001647229236",
        message_id=13174,
        source="Basira Press",
        severity="high",
        cluster_id="a4f90cff1ab644dd908240b849c4c8e9",
    )
    _flush_runtime_handlers()

    captured = capsys.readouterr()
    console_output = (captured.out + captured.err).replace("\n", " ")
    runtime_text = runtime_log.read_text(encoding="utf-8")
    error_text = error_log.read_text(encoding="utf-8")

    assert "Story cluster created" in runtime_text
    assert "Basira Press" in runtime_text
    assert "Cluster Id:" in runtime_text
    assert '"event"' not in runtime_text
    assert "\x1b[" not in runtime_text
    assert "Story cluster created" in console_output
    assert '"event"' not in console_output
    assert error_text == ""


def test_runtime_logs_redact_secrets(_isolated_runtime_logging):
    runtime_log = _isolated_runtime_logging["runtime_log"]
    error_log = _isolated_runtime_logging["error_log"]
    main._configure_runtime_logging()
    assert main._console_handler is not None
    main._console_handler.setLevel(logging.CRITICAL)

    secret_message = (
        "Bearer bearer-secret-token "
        "authorization='Bearer header-secret-token' "
        "TG_USERBOT_AUTH_JSON_B64=\"super-secret-b64\" "
        "BOT_DESTINATION_TOKEN=123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd "
        '{"access_token":"access-secret","refresh_token":"refresh-secret"} '
        "http://localhost:1455/auth/callback?code=oauth-secret-code&state=ok"
    )
    main.LOGGER.error(secret_message)
    _flush_runtime_handlers()

    runtime_text = runtime_log.read_text(encoding="utf-8").replace("\n", " ")
    error_text = error_log.read_text(encoding="utf-8").replace("\n", " ")

    for text in (runtime_text, error_text):
        assert "bearer-secret-token" not in text
        assert "header-secret-token" not in text
        assert "super-secret-b64" not in text
        assert "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd" not in text
        assert "access-secret" not in text
        assert "refresh-secret" not in text
        assert "oauth-secret-code" not in text
        assert "Bearer [REDACTED]" in text
        assert "authorization='Bearer [REDACTED]" in text
        assert 'TG_USERBOT_AUTH_JSON_B64=[REDACTED]' in text
        assert "BOT_DESTINATION_TOKEN=[REDACTED]" in text
        assert '"access_token":"[REDACTED]"' in text or '"access_token":"[REDACTED]' in text
        assert '"refresh_token":"[REDACTED]"' in text or '"refresh_token":"[REDACTED]' in text
        assert "code=[REDACTED]" in text


def test_parse_linux_process_status_memory():
    status_text = "\n".join(
        [
            "Name:\tpython",
            "VmRSS:\t12345 kB",
            "VmHWM:\t23456 kB",
            "VmSwap:\t34567 kB",
        ]
    )

    parsed = main._parse_linux_process_status_memory(status_text)

    assert parsed == {
        "memory_current_bytes": 12345 * 1024,
        "memory_peak_bytes": 23456 * 1024,
        "memory_swap_bytes": 34567 * 1024,
    }


def test_set_startup_phase_emits_memory_snapshot(_isolated_runtime_logging, monkeypatch, caplog):
    main._configure_runtime_logging()
    monkeypatch.setattr(
        main,
        "_process_memory_snapshot",
        lambda: {
            "memory_source": "test",
            "memory_current_bytes": 11,
            "memory_peak_bytes": 22,
            "memory_swap_bytes": 0,
            "memory_budget_bytes": 33,
        },
    )

    with caplog.at_level(logging.DEBUG):
        main._set_startup_phase("auth", reason="unit_test_phase")

    memory_events = _structured_payloads(caplog, "memory_snapshot")
    assert memory_events
    assert memory_events[-1]["memory_reason"] == "startup_phase_changed"
    assert memory_events[-1]["startup_phase"] == "auth"
    assert memory_events[-1]["phase_reason"] == "unit_test_phase"
    assert memory_events[-1]["memory_source"] == "test"


@pytest.mark.asyncio
async def test_pipeline_worker_lifecycle_emits_memory_snapshots(
    _isolated_runtime_logging,
    monkeypatch,
    caplog,
):
    main._configure_runtime_logging()
    monkeypatch.setattr(
        main,
        "_process_memory_snapshot",
        lambda: {
            "memory_source": "test",
            "memory_current_bytes": 101,
            "memory_peak_bytes": 202,
            "memory_swap_bytes": 0,
            "memory_budget_bytes": 303,
        },
    )
    monkeypatch.setattr(main, "reset_in_progress_inbound_jobs", lambda older_than_seconds=0: None)
    monkeypatch.setattr(main, "purge_ai_decision_cache", lambda older_than_ts: None)
    monkeypatch.setattr(
        main,
        "_pipeline_worker_targets",
        lambda: {
            main.INBOUND_STAGE_TRIAGE: 1,
            main.INBOUND_STAGE_AI_DECISION: 1,
            main.INBOUND_STAGE_DELIVERY: 1,
            main.INBOUND_STAGE_ARCHIVE: 1,
            "query_web": 1,
        },
    )

    async def fake_worker(stage: str, worker_index: int) -> None:
        del stage, worker_index
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    monkeypatch.setattr(main, "_run_inbound_stage_worker", fake_worker)

    with caplog.at_level(logging.DEBUG):
        await main._start_pipeline_workers()
        await asyncio.sleep(0)
        await main._stop_pipeline_workers()

    memory_events = _structured_payloads(caplog, "memory_snapshot")
    reasons = {str(payload.get("memory_reason")) for payload in memory_events}

    assert "pipeline_workers_started" in reasons
    assert "pipeline_workers_stopped" in reasons

    started_event = next(payload for payload in memory_events if payload.get("memory_reason") == "pipeline_workers_started")
    stopped_event = next(payload for payload in memory_events if payload.get("memory_reason") == "pipeline_workers_stopped")

    assert started_event["pipeline_worker_count"] == 4
    assert stopped_event["pipeline_worker_count"] == 0
    assert stopped_event["cancelled_worker_count"] == 4
