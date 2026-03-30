from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import ai_filter
import main
import severity_classifier
import utils


def _severity_payload(
    text: str,
    *,
    source: str = "Desk Wire",
    channel_id: str = "-1001",
    message_id: int = 1,
    has_media: bool = False,
    has_link: bool = True,
    reply_to: int = 0,
    text_tokens: int | None = None,
    humanized_vital_probability: float = 0.35,
) -> dict[str, object]:
    return {
        "text": text,
        "source": source,
        "channel_id": channel_id,
        "message_id": message_id,
        "has_media": has_media,
        "has_link": has_link,
        "reply_to": reply_to,
        "timestamp": 1774275669,
        "text_tokens": text_tokens or len(text.split()),
        "humanized_vital_probability": humanized_vital_probability,
    }


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
    assert label == "Breaking"


def test_choose_alert_label_keeps_thematic_label_for_concrete_event():
    label = utils.choose_alert_label(
        "Two drones were intercepted over Haifa overnight, officials said.",
        severity="high",
    )

    assert "Interception Alert" in label


def test_severity_classifier_downgrades_explainer_with_shot_down_keyword():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    severity, _score, breakdown = severity_classifier.classify_message_severity(
        _severity_payload(
            (
                "Analysis: Why Iran keeps shooting down MQ-9 Reapers. "
                "The piece looks at recurring losses and possible fixes."
            ),
            source="Research Desk",
            channel_id="-1001",
            message_id=7,
            has_media=False,
            text_tokens=42,
        )
    )

    assert severity != "high"
    assert breakdown["story_signals"]["downgrade_explainer"] is True
    assert breakdown["score_band"] != "high"
    assert breakdown["final_score"] < severity_classifier.severity_score_floor("high")


def test_severity_classifier_returns_high_band_for_trusted_breaking_post():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    severity, score, breakdown = severity_classifier.classify_message_severity(
        _severity_payload(
            "Breaking: Officials confirm missile strike hit Haifa moments ago after air raid sirens. 🚨🔥",
            source="The War Reporter",
            channel_id="-2001",
            message_id=1,
            has_media=False,
        )
    )

    thresholds = severity_classifier.severity_thresholds()
    assert severity == "high"
    assert breakdown["score_band"] == "high"
    assert breakdown["raw_score"] >= thresholds["high"]
    assert score >= thresholds["high"]
    assert score == breakdown["final_score"]


def test_severity_classifier_returns_medium_band_for_meaningful_update():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    severity, score, breakdown = severity_classifier.classify_message_severity(
        _severity_payload(
            "Officials confirmed the port reopened overnight after three days of disruption and cargo traffic resumed in stages.",
            source="Desk Wire",
            channel_id="-2002",
            message_id=2,
            has_media=True,
        )
    )

    thresholds = severity_classifier.severity_thresholds()
    assert severity == "medium"
    assert breakdown["score_band"] == "medium"
    assert thresholds["medium"] <= score < thresholds["high"]
    assert score == breakdown["final_score"]


def test_severity_classifier_returns_low_band_for_noisy_context_post():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    severity, score, breakdown = severity_classifier.classify_message_severity(
        _severity_payload(
            "Analysis: Why regional shipping patterns could change if officials keep adjusting port schedules over the coming weeks.",
            source="Research Desk",
            channel_id="-2003",
            message_id=3,
            has_media=False,
        )
    )

    thresholds = severity_classifier.severity_thresholds()
    assert severity == "low"
    assert breakdown["score_band"] == "low"
    assert 0.0 <= score < thresholds["medium"]
    assert score == breakdown["final_score"]


def test_severity_classifier_cooldown_downgrade_stays_out_of_high_band():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    payload = _severity_payload(
        "Breaking: Officials confirm missile strike hit Haifa moments ago after air raid sirens. 🚨🔥",
        source="The War Reporter",
        channel_id="-2401",
        has_media=False,
    )
    final = None
    for message_id in range(1, 5):
        final = severity_classifier.classify_message_severity(
            dict(payload, message_id=message_id)
        )

    assert final is not None
    severity, score, breakdown = final
    assert severity == "medium"
    assert breakdown["raw_score"] >= severity_classifier.severity_score_floor("high")
    assert breakdown["score_band"] == "medium"
    assert score < severity_classifier.severity_score_floor("high")
    assert "downgrade_source_high_cooldown" in breakdown["forced_reasons"]
    assert breakdown["calibration_reason"] == "downgrade_source_high_cooldown"


def test_severity_classifier_preserves_medium_band_ordering():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    base = severity_classifier.classify_message_severity(
        _severity_payload(
            "Officials confirmed services resumed overnight after three days of disruption at the main cargo port, but inspections are still ongoing.",
            source="Desk Wire",
            channel_id="-2501",
            message_id=1,
            has_media=True,
        )
    )
    stronger = severity_classifier.classify_message_severity(
        _severity_payload(
            "Officials confirmed the port reopened overnight after three days of disruption and cargo traffic resumed in stages.",
            source="Desk Wire",
            channel_id="-2502",
            message_id=2,
            has_media=True,
        )
    )

    assert base[0] == stronger[0] == "medium"
    assert base[2]["score_band"] == stronger[2]["score_band"] == "medium"
    assert stronger[1] > base[1]


def test_severity_classifier_preserves_high_band_ordering():
    severity_classifier._HIGH_SEVERITY_HISTORY.clear()
    lower = severity_classifier.classify_message_severity(
        _severity_payload(
            "Breaking: Officials confirm missile strike hit Haifa moments ago after air raid sirens. 🚨🔥",
            source="The War Reporter",
            channel_id="-2601",
            message_id=1,
            has_media=False,
        )
    )
    higher = severity_classifier.classify_message_severity(
        _severity_payload(
            "Breaking: Officials confirm multiple missile strikes hit Haifa and Tel Aviv just now, casualties reported, interception failure under review. 🚨🔥⚡",
            source="The War Reporter",
            channel_id="-2602",
            message_id=2,
            has_media=True,
        )
    )

    assert lower[0] == higher[0] == "high"
    assert lower[2]["score_band"] == higher[2]["score_band"] == "high"
    assert higher[1] > lower[1]


def test_format_summary_text_normalizes_bad_feed_copy(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_summary_text(
        "NYT",
        "How the port reopening could reshape trade?<br>The move follows three days of disruption.",
        raw_text=(
            "Officials confirmed the port will reopen Friday after three days of disruption "
            "to the main cargo route."
        ),
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "〔" in rendered
    assert rendered.count("<b>") == 1
    assert "<u>" not in rendered
    assert "How the port reopening" not in plain
    assert "Officials confirmed the port will reopen Friday" in plain
    assert "NYT" not in plain


def test_format_summary_text_hides_source_even_when_source_tags_enabled(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: True)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_summary_text(
        "Bellum Acta",
        "<b>Port reopened overnight</b><br>Operations resumed after a three-day shutdown.",
        raw_text="Officials confirmed the port reopened overnight after a three-day shutdown.",
        severity="medium",
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "Bellum Acta" not in plain
    assert plain.splitlines()[0].startswith("〔")


def test_format_summary_text_removes_duplicate_alert_prefixes(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_summary_text(
        "War & News Alert",
        "<b>Flash Update Hezbollah claims a direct drone hit on an army vehicle</b><br>1h ago the story moved from launches into strikes.",
        raw_text=(
            "Hezbollah claims a direct drone hit on an army vehicle in Mays al-Jabal."
        ),
        severity="high",
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "Flash Update Flash Update" not in plain
    assert "War & News Alert" not in plain
    assert "Why it matters" in plain


def test_format_summary_text_strips_source_prefixes_and_promos(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")
    main.source_title_cache.clear()
    main.source_alias_cache.clear()
    main._register_source_aliases("-1001", "Node of Time EN", "NewResistance")

    rendered = main._format_summary_text(
        "Node of Time EN",
        "Bloomberg: Officials say the port reopened overnight after a three-day shutdown.<br>"
        "Our channel: Node of Time EN<br>"
        "Subscribe @NewResistance",
        raw_text="Officials say the port reopened overnight after a three-day shutdown.",
        severity="medium",
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert rendered.count("<b>") == 1
    assert "<u>" not in rendered
    assert "Bloomberg:" not in plain
    assert "Node of Time EN" not in plain
    assert "@NewResistance" not in plain
    assert "Subscribe" not in plain
    assert "Officials say the port reopened overnight after a three-day shutdown" in plain


def test_format_summary_text_returns_empty_for_promo_only_caption(monkeypatch):
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")
    main.source_title_cache.clear()
    main.source_alias_cache.clear()
    main._register_source_aliases("-1001", "Node of Time EN", "NewResistance")

    rendered = main._format_summary_text(
        "Node of Time EN",
        "Our channel: Node of Time EN<br>Subscribe @NewResistance",
        raw_text="Our channel: Node of Time EN Subscribe @NewResistance",
        severity="medium",
    )

    assert rendered == ""


def test_validate_filter_decision_strips_source_prefixes_and_promo_from_model_output():
    decision = ai_filter._validate_filter_decision(
        {
            "action": "deliver",
            "severity": "medium",
            "summary_html": "Bloomberg: Officials say the port reopened overnight.<br>Subscribe @NewResistance",
            "headline_html": "",
            "story_bridge_html": "",
            "confidence": 0.8,
            "reason_code": "delivery",
            "topic_key": "port_reopen",
            "needs_ocr_translation": False,
        },
        "Officials say the port reopened overnight after a three-day shutdown.",
    )
    plain = ai_filter.strip_telegram_html(decision.summary_html)

    assert "Bloomberg:" not in plain
    assert "@NewResistance" not in plain
    assert "Subscribe" not in plain
    assert "Officials say the port reopened overnight" in plain


def test_prepare_media_caption_chunks_preserves_complete_sentences():
    caption = (
        "〔Breaking〕<br><br>"
        "Officials say the port reopened overnight.<br><br>"
        + " ".join(
            f"Sentence {idx} closes cleanly."
            for idx in range(1, 90)
        )
    )

    first, overflow = main._prepare_media_caption_chunks(caption, allow_premium_tags=False)

    assert first is not None
    assert overflow
    for chunk in [first, *overflow]:
        plain = ai_filter.strip_telegram_html(chunk).strip()
        assert len(plain) <= main._MEDIA_CAPTION_MAX_CHARS
        assert plain.endswith((".", "!", "?", "〕"))


@pytest.mark.asyncio
async def test_send_album_single_item_preserves_original_caption(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_send_single_media(msg, caption, *, reply_to=None):
        captured["msg"] = msg
        captured["caption"] = caption
        captured["reply_to"] = reply_to
        return {"message_id": 88}

    monkeypatch.setattr(main, "_destination_uses_bot_api", lambda: True)
    monkeypatch.setattr(main, "_send_single_media", fake_send_single_media)

    message = SimpleNamespace()
    caption = "〔Breaking〕<br><br>Headline.<br><br>" + " ".join(
        f"Sentence {idx} remains complete."
        for idx in range(1, 60)
    )
    sent_ref = await main._send_album([message], caption, reply_to=21)

    assert sent_ref == {"message_id": 88}
    assert captured["msg"] is message
    assert captured["caption"] == caption
    assert captured["reply_to"] == 21


@pytest.mark.asyncio
async def test_send_media_caption_overflow_replies_to_media(monkeypatch):
    sent_calls: list[tuple[str, int | None]] = []

    async def fake_send_text_with_ref(text, reply_to=None):
        sent_calls.append((text, reply_to))
        return {"message_id": 400 + len(sent_calls)}

    monkeypatch.setattr(main, "_send_text_with_ref", fake_send_text_with_ref)

    await main._send_media_caption_overflow(
        ["Overflow one.", "Overflow two."],
        sent_ref={"message_id": 321},
    )

    assert sent_calls == [("Overflow one.", 321), ("Overflow two.", 321)]


@pytest.mark.asyncio
async def test_handle_delivery_inbound_job_skips_when_caption_is_redacted_empty(monkeypatch):
    archived: dict[str, object] = {}
    main.source_title_cache.clear()
    main.source_alias_cache.clear()
    main._register_source_aliases("-1001", "Node of Time EN", "NewResistance")

    monkeypatch.setattr(
        main,
        "_load_inbound_payload",
        lambda _job: {
            "channel_id": "-1001",
            "source": "Node of Time EN",
            "has_media": False,
            "severity": "medium",
            "candidate_text": "Our channel: Node of Time EN Subscribe @NewResistance",
            "filter_decision": {
                "action": "deliver",
                "summary_html": "Our channel: Node of Time EN<br>Subscribe @NewResistance",
                "topic_key": "promo_only",
            },
        },
    )
    async def fake_fetch_messages(_payload):
        return [SimpleNamespace(id=7, chat_id=-1001, media=None)]

    async def fake_resolve_source_reply_target(_msg):
        return None

    async def fake_resolve_dynamic_delivery_context(**_kwargs):
        return ""

    monkeypatch.setattr(main, "_fetch_messages_for_payload", fake_fetch_messages)
    monkeypatch.setattr(main, "_resolve_source_reply_target", fake_resolve_source_reply_target)
    monkeypatch.setattr(main, "_resolve_dynamic_delivery_context", fake_resolve_dynamic_delivery_context)
    monkeypatch.setattr(main, "_is_digest_mode_enabled", lambda: False)

    async def fake_advance_job_to_archive(_job, payload):
        archived["payload"] = dict(payload)

    async def fail_send(*_args, **_kwargs):
        raise AssertionError("delivery should have been skipped")

    monkeypatch.setattr(main, "_advance_job_to_archive", fake_advance_job_to_archive)
    monkeypatch.setattr(main, "_send_text_with_ref", fail_send)

    await main._handle_delivery_inbound_job({"id": 14})

    payload = archived["payload"]
    assert payload["final_action"] == "skip"
    assert payload["skip_reason"] == "caption_redacted_empty"


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


@pytest.mark.asyncio
async def test_handle_triage_inbound_job_keyword_override_uses_high_band_floor(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(main, "_load_inbound_payload", lambda _job: {})
    monkeypatch.setattr(main, "_is_severity_routing_enabled", lambda: True)
    monkeypatch.setattr(
        main,
        "_classify_severity_with_breakdown",
        lambda **_kwargs: ("medium", 0.12, {"raw_score": 0.12}),
    )
    monkeypatch.setattr(main, "_contains_breaking_keyword", lambda _text: True)
    monkeypatch.setattr(main, "should_downgrade_explainer_urgency", lambda _text: False)
    monkeypatch.setattr(main, "looks_like_live_event_update", lambda _text: True)
    monkeypatch.setattr(main, "_pipeline_payload_json", lambda payload: json.dumps(payload))
    monkeypatch.setattr(main, "_pipeline_priority_for_severity", lambda severity: severity)

    async def fake_fetch_messages(_payload):
        return [
            SimpleNamespace(
                id=91,
                chat_id=-10077,
                media=None,
                message="Breaking update: officials confirm new strikes and intercept activity near Haifa right now.",
            )
        ]

    async def fake_source_info(_msg):
        return "Desk Wire", "https://t.me/example/91"

    def fake_advance_inbound_job(job_id, **kwargs):
        captured["job_id"] = job_id
        captured.update(kwargs)

    monkeypatch.setattr(main, "_fetch_messages_for_payload", fake_fetch_messages)
    monkeypatch.setattr(main, "_source_info", fake_source_info)
    monkeypatch.setattr(main, "advance_inbound_job", fake_advance_inbound_job)

    await main._handle_triage_inbound_job({"id": 51})

    payload = json.loads(str(captured["payload_json"]))
    assert payload["severity"] == "high"
    assert payload["severity_score"] == severity_classifier.severity_score_floor("high")
    assert payload["severity_breakdown"]["keyword_override"] is True
    assert captured["priority"] == "high"


@pytest.mark.asyncio
async def test_resolve_dynamic_delivery_context_uses_evidence_backed_candidate(monkeypatch):
    main.delivery_context_stats.clear()
    monkeypatch.setattr(main.time, "time", lambda: 1700003600)
    monkeypatch.setattr(
        main,
        "load_archive_since",
        lambda *_args, **_kwargs: [
            {
                "raw_text": "Officials said rockets landed near Haifa overnight.",
                "timestamp": 1700000000,
                "source_name": "Wire",
            }
        ],
    )
    monkeypatch.setattr(main, "auth_ready", False)

    resolved = await main._resolve_dynamic_delivery_context(
        current_text="Officials say two rockets landed near Acre overnight.",
        headline="Officials say two rockets landed near Acre overnight.",
        candidate_context="Why it matters: Earlier reports centered on Haifa; this update places the same exchange in Acre.",
        source_title="Wire",
    )

    assert "Haifa" in resolved
    assert "Acre" in resolved
    assert main.delivery_context_stats["context_generated"] >= 1


@pytest.mark.asyncio
async def test_resolve_dynamic_delivery_context_omits_without_anchor(monkeypatch):
    main.delivery_context_stats.clear()
    monkeypatch.setattr(main.time, "time", lambda: 1700003600)
    monkeypatch.setattr(
        main,
        "load_archive_since",
        lambda *_args, **_kwargs: [
            {
                "raw_text": "Wheat prices rose in Buenos Aires after new export guidance.",
                "timestamp": 1700000000,
                "source_name": "Desk",
            }
        ],
    )
    monkeypatch.setattr(main, "auth_ready", False)

    resolved = await main._resolve_dynamic_delivery_context(
        current_text="Officials confirmed the port reopened overnight in Basra.",
        headline="Officials confirmed the port reopened overnight in Basra.",
        source_title="Desk",
    )

    assert resolved == ""
    assert main.delivery_context_stats["context_omitted_no_anchor"] >= 1


@pytest.mark.asyncio
async def test_resolve_dynamic_delivery_context_omits_without_delta(monkeypatch):
    main.delivery_context_stats.clear()
    monkeypatch.setattr(main.time, "time", lambda: 1700003600)
    monkeypatch.setattr(
        main,
        "load_archive_since",
        lambda *_args, **_kwargs: [
            {
                "raw_text": "Officials said rockets landed near Haifa overnight.",
                "timestamp": 1700000000,
                "source_name": "Wire",
            }
        ],
    )
    monkeypatch.setattr(main, "auth_ready", False)

    resolved = await main._resolve_dynamic_delivery_context(
        current_text="Officials say rockets landed near Haifa overnight.",
        headline="Officials say rockets landed near Haifa overnight.",
        source_title="Wire",
    )

    assert resolved == ""
    assert main.delivery_context_stats["context_omitted_no_delta"] >= 1
