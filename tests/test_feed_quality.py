from __future__ import annotations

import json

import pytest

import ai_filter
import main
import severity_classifier
import utils


def test_fallback_filter_decision_downgrades_explainer_with_urgent_keywords():
    text = (
        "Reasons for MQ-9 Reaper losses and ways to address them. "
        "The MQ-9 Reaper UAV is the aircraft most frequently shot down among those "
        "the United States uses against Iran."
    )

    decision = ai_filter._fallback_filter_decision(text)

    assert decision.action in {"digest", "skip"}
    assert decision.severity != "high"


def test_normalize_feed_summary_rewrites_question_led_title():
    raw_text = (
        "Officials confirmed the port will reopen Friday after three days of disruption "
        "to the main cargo route."
    )
    summary_html = ai_filter.normalize_feed_summary_html(
        "How the port reopening could reshape trade?<br>The move follows three days of disruption.",
        raw_text,
    )
    plain = ai_filter.strip_telegram_html(summary_html)

    assert plain
    assert "How the port reopening" not in plain
    assert "Officials confirmed the port will reopen Friday" in plain
    assert "?" not in plain


def test_choose_alert_label_falls_back_to_generic_for_explainer_copy():
    label = utils.choose_alert_label(
        "Reasons for MQ-9 Reaper losses and ways to address them.",
        severity="high",
    )

    assert "Interception Alert" not in label
    assert "Flash Update" in label


def test_choose_alert_label_keeps_thematic_label_for_concrete_event():
    label = utils.choose_alert_label(
        "Two drones were intercepted over Haifa overnight, officials said.",
        severity="high",
    )

    assert "Interception Alert" in label


def test_severity_classifier_downgrades_explainer_with_shot_down_keyword():
    severity, _score, breakdown = severity_classifier.classify_message_severity(
        {
            "text": (
                "Analysis: Why Iran keeps shooting down MQ-9 Reapers. "
                "The piece looks at recurring losses and possible fixes."
            ),
            "source": "Research Desk",
            "channel_id": "-1001",
            "message_id": 7,
            "has_media": False,
            "has_link": True,
            "reply_to": 0,
            "timestamp": 1774275669,
            "text_tokens": 42,
            "humanized_vital_probability": 0.35,
        }
    )

    assert severity != "high"
    assert breakdown["story_signals"]["downgrade_explainer"] is True


def test_format_summary_text_normalizes_bad_feed_copy(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)

    rendered = main._format_summary_text(
        "NYT",
        "How the port reopening could reshape trade?<br>The move follows three days of disruption.",
        raw_text=(
            "Officials confirmed the port will reopen Friday after three days of disruption "
            "to the main cargo route."
        ),
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "How the port reopening" not in plain
    assert "Officials confirmed the port will reopen Friday" in plain


@pytest.mark.asyncio
async def test_handle_ai_inbound_job_uses_ai_decision_severity(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_decide_filter_action(_text, _auth_manager):
        return ai_filter.FilterDecision(
            action="digest",
            severity="medium",
            summary_html="<b>Port will reopen Friday</b>",
            headline_html="",
            story_bridge_html="",
            confidence=0.88,
            reason_code="explainer_digest",
            topic_key="port_reopen",
            needs_ocr_translation=False,
        )

    monkeypatch.setattr(
        main,
        "_load_inbound_payload",
        lambda _job: {
            "candidate_text": "Reasons for MQ-9 Reaper losses and ways to address them.",
            "severity": "high",
        },
    )
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "decide_filter_action", fake_decide_filter_action)
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "classic")
    monkeypatch.setattr(main, "_pipeline_payload_json", lambda payload: json.dumps(payload))
    monkeypatch.setattr(main, "_pipeline_priority_for_severity", lambda severity: severity)

    def fake_advance_inbound_job(job_id, **kwargs):
        captured["job_id"] = job_id
        captured.update(kwargs)

    monkeypatch.setattr(main, "advance_inbound_job", fake_advance_inbound_job)

    await main._handle_ai_inbound_job({"id": 11})

    payload = json.loads(str(captured["payload_json"]))
    assert payload["triage_severity"] == "high"
    assert payload["severity"] == "medium"
    assert payload["filter_decision"]["action"] == "digest"
    assert captured["priority"] == "medium"
