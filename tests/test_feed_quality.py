from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import ai_filter
import main
import prompts
import severity_classifier
import utils


def _severity_payload(
    text: str,
    *,
    source: str = "Desk Wire",
    channel_id: str = "-1001",
    message_id: int = 1,
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
    assert label.startswith("🛡️ ")


def test_choose_alert_label_upgrades_industrial_incident_emoji():
    label = utils.choose_alert_label(
        "Officials confirmed a factory explosion after a major blast at the plant overnight.",
        severity="high",
    )

    assert label == "🏭💥 Industrial Incident"


def test_choose_alert_label_upgrades_network_disruption_emoji():
    label = utils.choose_alert_label(
        "Authorities confirmed a telecom outage after the latest mobile network outage today.",
        severity="medium",
    )

    assert label == "📡 Network Disruption"


def test_query_status_copy_stays_polished():
    initial = main._query_search_status(include_web=True)
    crosscheck = main._query_crosscheck_status(high_risk=False)
    writing = main._query_writing_status()

    assert "->" not in initial
    assert "trusted web sources" in initial
    assert "->" not in crosscheck
    assert "latest details" in crosscheck
    assert "Thinking" not in writing
    assert "strongest evidence" in writing


def test_digest_output_style_only_uses_headline_rail_for_thirty_minute_windows():
    assert prompts.digest_output_style(30) == "headline_rail"
    assert prompts.digest_output_style(60) == "story_digest"
    assert prompts.digest_output_style(24 * 60) == "story_digest"


def test_query_analysis_status_reads_like_a_sentence():
    status = main._query_analysis_status(100, 4)

    assert status == "100 Telegram updates and 4 trusted web reports gathered. Building your answer now... ⏳"
    assert "->" not in status


def test_query_analysis_status_handles_singular_counts():
    status = main._query_analysis_status(1, 1)

    assert status == "1 Telegram update and 1 trusted web report gathered. Building your answer now... ⏳"


def test_strip_query_answer_citations_removes_known_channel_aliases_but_keeps_named_entities():
    main.source_alias_cache.clear()
    main.source_title_cache.clear()
    main._register_source_aliases("-1001", "War & News Alert", "war_news_alert")

    cleaned = main._strip_query_answer_citations(
        "<b>War & News Alert</b><br>"
        "NBC News said Donald Trump spoke after the strike.<br>"
        "War & News Alert: Oracle's Dubai facility was among the reported targets.<br>"
        "@war_news_alert"
    )
    plain = ai_filter.strip_telegram_html(cleaned)

    assert "War & News Alert" not in plain
    assert "@war_news_alert" not in plain
    assert "NBC News" in plain
    assert "Donald Trump" in plain
    assert "Oracle's Dubai facility" in plain
    main.source_alias_cache.clear()
    main.source_title_cache.clear()


def test_strip_known_source_aliases_is_contextual_not_global():
    main.source_alias_cache.clear()
    main.source_title_cache.clear()
    main._register_source_aliases("-1001", "Middle East Eye", "middleeasteye")

    assert (
        main._strip_known_source_aliases("Middle East Eye reported strikes hit the area.")
        == "Middle East Eye reported strikes hit the area."
    )
    assert main._strip_known_source_aliases("Middle East Eye: strikes hit the area.") == "Strikes hit the area."

    main.source_alias_cache.clear()
    main.source_title_cache.clear()


def test_strip_query_answer_citations_rewrites_known_channel_attribution_without_dropping_fact():
    main.source_alias_cache.clear()
    main.source_title_cache.clear()
    main._register_source_aliases("-1001", "Middle East Eye", "middleeasteye")

    cleaned = main._strip_query_answer_citations(
        "Middle East Eye reported strikes hit the area.<br>"
        "According to Middle East Eye, explosions were heard nearby."
    )
    plain = ai_filter.strip_telegram_html(cleaned)

    assert "Middle East Eye" not in plain
    assert "Reports indicate strikes hit the area." in plain
    assert "explosions were heard nearby." in plain

    main.source_alias_cache.clear()
    main.source_title_cache.clear()


def test_strip_query_answer_citations_removes_inline_known_channel_attribution():
    main.source_alias_cache.clear()
    main.source_title_cache.clear()
    main._register_source_aliases("-1001", "Middle East Eye", "middleeasteye")

    cleaned = main._strip_query_answer_citations(
        "The strike, according to Middle East Eye, hit a fuel depot.<br>"
        "Explosions were heard nearby, according to @middleeasteye."
    )
    plain = ai_filter.strip_telegram_html(cleaned)

    assert "Middle East Eye" not in plain
    assert "@middleeasteye" not in plain
    assert "according to" not in plain.lower()
    assert "The strike hit a fuel depot." in plain
    assert "Explosions were heard nearby." in plain

    main.source_alias_cache.clear()
    main.source_title_cache.clear()


def test_strip_query_answer_citations_removes_reported_by_known_channel_forms():
    main.source_alias_cache.clear()
    main.source_title_cache.clear()
    main._register_source_aliases("-1001", "Middle East Eye", "middleeasteye")

    cleaned = main._strip_query_answer_citations(
        "Strikes hit the area, reported by Middle East Eye.<br>"
        "The strike, as reported by @middleeasteye, hit a fuel depot."
    )
    plain = ai_filter.strip_telegram_html(cleaned)

    assert "Middle East Eye" not in plain
    assert "@middleeasteye" not in plain
    assert "reported by" not in plain.lower()
    assert "Strikes hit the area." in plain
    assert "The strike hit a fuel depot." in plain

    main.source_alias_cache.clear()
    main.source_title_cache.clear()


def test_build_query_plan_marks_explicit_time_filters():
    default_plan = utils.build_query_plan("What happened in Tehran?")
    explicit_plan = utils.build_query_plan("What happened today in Tehran?")

    assert default_plan.explicit_time_filter is False
    assert explicit_plan.explicit_time_filter is True


def test_build_query_plan_does_not_treat_noisy_day_phrase_as_time_filter():
    plan = utils.build_query_plan("why user query replying with 1 day digest")

    assert plan.explicit_time_filter is False
    assert plan.hours_back == 24


def test_build_query_search_variants_prioritize_subject_terms_for_choice_queries():
    variants = utils.build_query_search_variants("Which data centers did Iran hit, Oracle or AWS?")

    assert "data centers iran hit" in variants
    assert "oracle aws" in variants
    assert "oracle" in variants
    assert "aws" in variants
    assert "which data centers did" not in variants


@pytest.mark.asyncio
async def test_search_recent_messages_scans_chats_in_parallel():
    class _FakeClient:
        def __init__(self) -> None:
            self.started: list[float] = []

        async def iter_messages(self, chat_ref, **kwargs):
            self.started.append(asyncio.get_running_loop().time())
            await asyncio.sleep(0.05)
            if False:
                yield None
            return

    client = _FakeClient()

    started_at = asyncio.get_running_loop().time()
    rows = await utils.search_recent_messages(
        client,
        ["chat-1", "chat-2", "chat-3", "chat-4"],
        "Oracle Dubai",
        max_messages=20,
        default_hours_back=24,
    )
    elapsed = asyncio.get_running_loop().time() - started_at

    assert rows == []
    assert len(client.started) >= 4
    assert max(client.started[:4]) - min(client.started[:4]) < 0.02
    assert elapsed < 0.45


def test_parse_time_filter_today_uses_configured_timezone(monkeypatch):
    class _FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 4, 4, 0, 30, tzinfo=timezone.utc)
            if tz is None:
                return base.replace(tzinfo=None)
            return base.astimezone(tz)

    monkeypatch.setattr(utils.config, "TIMEZONE", "Asia/Kolkata", raising=False)
    monkeypatch.setattr(utils, "datetime", _FakeDateTime)

    hours_back, cleaned, start_ts, end_ts = utils.parse_time_filter_from_query(
        "What happened today in Tehran?",
        default_hours=1,
    )

    assert hours_back == 6
    assert cleaned == "What happened in Tehran?"
    assert start_ts == int(datetime(2026, 4, 3, 18, 30, tzinfo=timezone.utc).timestamp())
    assert end_ts == int(datetime(2026, 4, 4, 0, 30, tzinfo=timezone.utc).timestamp())


def test_parse_time_filter_yesterday_uses_calendar_day_boundaries(monkeypatch):
    class _FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 4, 4, 20, 0, tzinfo=timezone.utc)
            if tz is None:
                return base.replace(tzinfo=None)
            return base.astimezone(tz)

    monkeypatch.setattr(utils.config, "TIMEZONE", "Asia/Kolkata", raising=False)
    monkeypatch.setattr(utils, "datetime", _FakeDateTime)

    hours_back, cleaned, start_ts, end_ts = utils.parse_time_filter_from_query(
        "What happened yesterday in Tehran?",
        default_hours=1,
    )

    assert hours_back == 26
    assert cleaned == "What happened in Tehran?"
    assert start_ts == int(datetime(2026, 4, 3, 18, 30, tzinfo=timezone.utc).timestamp())
    assert end_ts == int(datetime(2026, 4, 4, 18, 30, tzinfo=timezone.utc).timestamp())


def test_parse_time_filter_accepts_subject_shorthand_window():
    hours_back, cleaned, start_ts, end_ts = utils.parse_time_filter_from_query(
        "Tehran 3d",
        default_hours=24,
    )

    assert hours_back == 72
    assert cleaned == "Tehran"
    assert start_ts is None
    assert end_ts is None


def test_wrap_query_digest_answer_uses_today_title():
    wrapped = main._wrap_query_digest_answer(
        "Major events unfolded across the city.",
        hours_back=6,
        query_text="What happened today in Tehran?",
        explicit_time_filter=True,
    )

    assert wrapped.startswith("<b>Today digest</b><br><br>")


def test_wrap_query_digest_answer_uses_yesterday_title_instead_of_horizon():
    wrapped = main._wrap_query_digest_answer(
        "Strikes and outages dominated the day.",
        hours_back=26,
        query_text="What happened yesterday in Tehran?",
        explicit_time_filter=True,
    )

    assert wrapped.startswith("<b>Yesterday digest</b><br><br>")
    assert "26-hour digest" not in wrapped


def test_wrap_query_digest_answer_uses_explicit_day_window_title():
    wrapped = main._wrap_query_digest_answer(
        "A month of developments reshaped the front.",
        hours_back=24 * 30,
        query_text="What happened in the last 30 days in Tehran?",
        explicit_time_filter=True,
    )

    assert wrapped.startswith("<b>30-day digest</b><br><br>")


def test_wrap_query_digest_answer_uses_neutral_title_without_explicit_window():
    wrapped = main._wrap_query_digest_answer(
        "Major events unfolded across the city.",
        hours_back=24,
        query_text="What happened in Tehran?",
        explicit_time_filter=False,
    )

    assert wrapped.startswith("<b>Digest</b><br><br>")
    assert "1-day digest" not in wrapped


def test_wrap_query_digest_answer_uses_shorthand_day_title():
    wrapped = main._wrap_query_digest_answer(
        "A month of developments reshaped the front.",
        hours_back=24 * 3,
        query_text="Tehran 3d",
        explicit_time_filter=True,
    )

    assert wrapped.startswith("<b>3-day digest</b><br><br>")


def test_runtime_timezone_accepts_ist_alias(monkeypatch):
    monkeypatch.setattr(utils.config, "TIMEZONE", "IST", raising=False)

    tz = utils.runtime_timezone()

    assert getattr(tz, "key", "") == "Asia/Kolkata"


def test_format_ts_uses_runtime_timezone(monkeypatch):
    monkeypatch.setattr(utils.config, "TIMEZONE", "Asia/Kolkata", raising=False)

    assert utils.format_ts(0) == "never"
    assert utils.format_ts(1800) == "1970-01-01 06:00:00 IST"


def test_load_archive_query_context_uses_runtime_timezone_for_date_labels(monkeypatch):
    monkeypatch.setattr(
        main,
        "load_archive_since",
        lambda *_args, **_kwargs: [
            {
                "timestamp": 1712277000,
                "channel_id": "-1001",
                "message_id": 10,
                "source_name": "Archive Source",
                "message_link": "",
                "raw_text": "Strike reports continued overnight.",
            }
        ],
    )
    monkeypatch.setattr(main, "extract_query_keywords", lambda _query: ["strike"])
    monkeypatch.setattr(main, "expand_query_terms", lambda _query: ["strike"])
    monkeypatch.setattr(main, "extract_query_numbers", lambda _query: [])
    monkeypatch.setattr(main, "runtime_timezone", lambda: utils.runtime_timezone())
    monkeypatch.setattr(main.config, "TIMEZONE", "Asia/Kolkata", raising=False)

    rows = main._load_archive_query_context(
        query_text="strike",
        hours_back=24,
        limit=5,
        broad_query=False,
    )

    assert rows
    assert rows[0]["date"].endswith("+05:30")


def test_filter_stored_query_rows_respects_end_bound_for_yesterday_window():
    rows = [
        {
            "timestamp": int(datetime(2026, 4, 4, 17, 0, tzinfo=timezone.utc).timestamp()),
            "channel_id": "-1001",
            "message_id": 10,
            "source_name": "Archive Source",
            "message_link": "",
            "raw_text": "Strike reports continued yesterday evening.",
        },
        {
            "timestamp": int(datetime(2026, 4, 4, 19, 0, tzinfo=timezone.utc).timestamp()),
            "channel_id": "-1001",
            "message_id": 11,
            "source_name": "Archive Source",
            "message_link": "",
            "raw_text": "Strike reports continued after midnight local time.",
        },
    ]

    filtered = main._filter_stored_query_rows(
        rows,
        query_text="strike",
        broad_query=False,
        start_ts=int(datetime(2026, 4, 3, 18, 30, tzinfo=timezone.utc).timestamp()),
        end_ts=int(datetime(2026, 4, 4, 18, 30, tzinfo=timezone.utc).timestamp()),
    )

    assert [item["message_id"] for item in filtered] == [10]


@pytest.mark.asyncio
async def test_search_recent_news_web_respects_default_seven_day_query_contract(monkeypatch):
    urls: list[str] = []

    class _FakeResponse:
        status_code = 200
        text = (
            "<rss><channel><item>"
            "<title>Oracle site named in regional reporting</title>"
            "<link>https://example.com/report</link>"
            "<description>Amazon infrastructure in Bahrain was also mentioned.</description>"
            "</item></channel></rss>"
        )

    class _FakeHTTP:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        async def get(self, url: str):
            urls.append(url)
            return _FakeResponse()

    async def fake_get_web_http_client():
        return _FakeHTTP()

    monkeypatch.setattr(utils, "get_web_http_client", fake_get_web_http_client)

    results = await utils.search_recent_news_web(
        "Which data centers did Iran hit, Oracle or AWS?",
        hours_back=24 * 7,
        max_results=3,
        allowed_domains=[],
        require_recent=False,
    )

    assert results
    assert any("168h" in url for url in urls)
    assert all("72h" not in url for url in urls)


@pytest.mark.asyncio
async def test_search_recent_news_web_honors_explicit_thirty_day_window(monkeypatch):
    urls: list[str] = []

    class _FakeResponse:
        status_code = 200
        text = (
            "<rss><channel><item>"
            "<title>Regional reporting referenced multiple infrastructure sites</title>"
            "<link>https://example.com/report</link>"
            "<description>Oracle and Amazon-linked facilities were both mentioned.</description>"
            "</item></channel></rss>"
        )

    class _FakeHTTP:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        async def get(self, url: str):
            urls.append(url)
            return _FakeResponse()

    async def fake_get_web_http_client():
        return _FakeHTTP()

    monkeypatch.setattr(utils, "get_web_http_client", fake_get_web_http_client)

    results = await utils.search_recent_news_web(
        "Which data centers did Iran hit in the last 30 days, Oracle or AWS?",
        hours_back=24 * 30,
        max_results=3,
        allowed_domains=[],
        require_recent=False,
    )

    assert results
    assert any("720h" in url for url in urls)
    assert all("168h" not in url for url in urls)


def test_strip_query_answer_citations_keeps_follow_up_language_while_removing_follow_promos():
    cleaned = main._strip_query_answer_citations(
        "NBC News confirmed follow-up explosions after the strike.<br>"
        "Follow Us | Discussion<br>"
        "Follow @desk_wire"
    )
    plain = ai_filter.strip_telegram_html(cleaned)

    assert "follow-up explosions" in plain
    assert "Follow Us" not in plain
    assert "@desk_wire" not in plain


def test_clean_generated_delivery_segment_keeps_follow_up_language():
    assert (
        ai_filter._clean_generated_delivery_segment(
            "Follow-up explosions were reported across the district."
        )
        == "Follow-up explosions were reported across the district."
    )
    assert ai_filter._clean_generated_delivery_segment("Follow @DeskWire") == ""


def test_strip_caption_promo_noise_keeps_follow_up_language():
    assert (
        main._strip_caption_promo_noise("Follow-up explosions were reported across the district.")
        == "Follow-up explosions were reported across the district."
    )


def test_fallback_query_answer_extracts_relevant_sentences_from_noisy_blob():
    answer = ai_filter._fallback_query_answer(
        "Which data centers did Iran hit, Oracle or AWS?",
        [
            {
                "text": (
                    "84 billion rubles to combat proxies. Iran attacked IT giants and aviation in the Middle East. "
                    "The Oracle data center in Dubai was also attacked, according to Jerusalem Post. "
                    "Strikes were carried out on Amazon's cloud computing center in Bahrain. "
                    "Two Majors on X. Support us. Original msg."
                ),
                "source": "Desk Wire",
                "timestamp": 1700000000,
            }
        ],
        detailed=False,
    )
    plain = ai_filter.strip_telegram_html(answer)

    assert "Oracle data center in Dubai" in plain
    assert "Amazon's cloud computing center in Bahrain" in plain
    assert "84 billion rubles" not in plain
    assert "Two Majors" not in plain
    assert "Support us" not in plain


def test_query_quality_issue_rejects_verbose_source_dump():
    html = (
        "<b>Direct answer</b><br>"
        "• Fwd from Desk Wire<br>"
        "• Oracle data center in Dubai was hit.<br>"
        "• Amazon cloud computing center in Bahrain was hit.<br>"
        "• Original msg<br>"
        "• Support us<br>"
        "• @examplehandle"
    )

    assert ai_filter._query_quality_issue(html, "Which data centers were hit?") in {
        "citation_leak",
        "messy_layout",
    }


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
        )
    )
    stronger = severity_classifier.classify_message_severity(
        _severity_payload(
            "Officials confirmed the port reopened overnight after three days of disruption and cargo traffic resumed in stages.",
            source="Desk Wire",
            channel_id="-2502",
            message_id=2,
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
        )
    )
    higher = severity_classifier.classify_message_severity(
        _severity_payload(
            "Breaking: Officials confirm multiple missile strikes hit Haifa and Tel Aviv just now, casualties reported, interception failure under review. 🚨🔥⚡",
            source="The War Reporter",
            channel_id="-2602",
            message_id=2,
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
    assert rendered.count("<b>") >= 1


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
        },
        "Officials say the port reopened overnight after a three-day shutdown.",
    )
    plain = ai_filter.strip_telegram_html(decision.summary_html)

    assert "Bloomberg:" not in plain
    assert "@NewResistance" not in plain
    assert "Subscribe" not in plain
    assert "Officials say the port reopened overnight" in plain


def test_fallback_query_answer_preserves_full_long_evidence_line():
    long_line = (
        "Esmaeil Baghaei said Iran was not involved in the Pakistan-led talks and "
        "described the latest rumors as politically motivated spin while reiterating "
        "that no direct negotiation channel is currently active. The final marker is cobalt horizon."
    )

    answer = ai_filter._fallback_query_answer(
        "What's the update in war?",
        [{"text": long_line, "source": "Wire", "timestamp": 1700000000}],
        detailed=False,
    )
    plain = ai_filter.strip_telegram_html(answer)

    assert "cobalt horizon." in plain
    assert "cobalt horizon..." not in plain


def test_fallback_query_answer_keeps_selection_limit():
    context = [
        {"text": f"War update {idx} closes cleanly with a distinct marker {idx}.", "source": "Wire", "timestamp": 1700000000 - idx}
        for idx in range(1, 9)
    ]

    answer = ai_filter._fallback_query_answer("What's the update in war?", context, detailed=False)
    plain = ai_filter.strip_telegram_html(answer)

    assert 1 <= plain.count("•") <= 4


def test_fallback_query_answer_splits_oversize_single_evidence_line_cleanly():
    long_line = " ".join(
        f"Sentence {idx} closes cleanly."
        for idx in range(1, 140)
    )

    answer = ai_filter._fallback_query_answer(
        "What's the update in war?",
        [{"text": long_line, "source": "Wire", "timestamp": 1700000000}],
        detailed=False,
    )
    plain = ai_filter.strip_telegram_html(answer)

    assert plain.count("•") >= 1
    assert "Sentence 139 closes cleanly." in plain
    assert not plain.rstrip().endswith("...")


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


def test_caption_fragment_is_usable_rejects_dangling_possessive_tail():
    assert not main._caption_fragment_is_usable(
        "Joint Chiefs detail a 45-hour CSAR while an A-10 was hit but its."
    )


def test_render_digest_body_sections_strips_nested_markers_and_drops_fragments():
    rendered = main._render_digest_body_sections(
        "Top headlines from the last 30 minutes",
        "",
        [
            "Context- 🔴BSF arrested a smuggler in Karimpur with 24 kg of silver.",
            "The targeted assassination wave hit 3 vehicles: ● a pickup in Meifdoun ● a Rapid on the coastal road.",
            '"It is highly likely that some traces of interference will be found" on the attempt to blow up...',
        ],
        [],
    )

    plain = ai_filter.strip_telegram_html(rendered)
    assert "Context-" not in plain
    assert "🔴" not in plain
    assert "●" not in plain
    assert "blow up..." not in plain


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


def test_prepare_query_answer_chunks_prefers_bullet_boundaries():
    answer = (
        "<b>What's the update in war</b><br>"
        "• 🔥 First long bullet closes cleanly after several clauses and still ends with a full stop. "
        "Sentence two also closes cleanly.<br>"
        "• ⚠️ Second long bullet closes cleanly after several clauses and still ends with a full stop. "
        "Sentence two also closes cleanly.<br>"
        "• ⚠️ Third long bullet closes cleanly after several clauses and still ends with a full stop. "
        "Sentence two also closes cleanly."
    )

    chunks = main._prepare_query_answer_chunks(answer, max_chars=160)

    assert len(chunks) >= 2
    assert all(len(chunk) <= 160 for chunk in chunks)
    assert ai_filter.strip_telegram_html(chunks[0]).startswith("What's the update in war")
    assert ai_filter.strip_telegram_html(chunks[1]).lstrip().startswith("•")


@pytest.mark.asyncio
async def test_deliver_query_final_answer_threads_continuations(monkeypatch):
    calls: list[dict[str, object]] = []

    async def fake_safe_reply_markdown(_event, text, *, edit_message=None, reply_to=None, prefer_bot_identity=False, bot_chat_id=None):
        if edit_message is not None:
            ref = {"message_id": int(edit_message.get("message_id", 0) or 900)}
        else:
            ref = {"message_id": 900 + len(calls) + 1}
        calls.append(
            {
                "text": text,
                "edit_message": edit_message,
                "reply_to": reply_to,
                "prefer_bot_identity": prefer_bot_identity,
                "bot_chat_id": bot_chat_id,
                "message_id": ref["message_id"],
            }
        )
        return ref

    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)

    answer = (
        "<b>What's the update in war</b><br>"
        + "<br>".join(
            f"• ⚠️ Bullet {idx} closes cleanly after several clauses and still ends with a full stop."
            for idx in range(1, 7)
        )
    )

    stats = await main._deliver_query_final_answer(
        SimpleNamespace(chat_id=12345),
        progress_message={"message_id": 900},
        final_text=answer,
        max_chars=160,
    )

    assert len(calls) >= 2
    assert calls[0]["edit_message"] == {"message_id": 900}
    assert calls[1]["reply_to"] == 900
    if len(calls) >= 3:
        assert calls[2]["reply_to"] == calls[1]["message_id"]
    assert stats.message_count == len(calls)


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
async def test_queue_single_message_for_digest_skips_media_only_posts(monkeypatch):
    queued: dict[str, object] = {}

    monkeypatch.setattr(main, "is_seen", lambda *_args, **_kwargs: False)
    marked: list[tuple[str, int]] = []
    monkeypatch.setattr(main, "mark_seen", lambda channel_id, message_id: marked.append((channel_id, message_id)))
    monkeypatch.setattr(main, "count_pending", lambda: 1)
    monkeypatch.setattr(main, "_archive_for_query_search", lambda *_args, **_kwargs: None)

    async def fake_source_info(_msg):
        return "Desk Wire", "https://t.me/example/11"

    async def fail_send(*_args, **_kwargs):
        raise AssertionError("media-only post should be ignored")

    def fake_queue(channel_id, message_id, raw_text, **kwargs):
        queued["channel_id"] = channel_id
        queued["message_id"] = message_id
        queued["raw_text"] = raw_text
        queued["kwargs"] = kwargs

    monkeypatch.setattr(main, "_source_info", fake_source_info)
    monkeypatch.setattr(main, "_queue_for_digest", fake_queue)

    await main._queue_single_message_for_digest(
        SimpleNamespace(chat_id=-1001, id=11, media=True, message="")
    )

    assert queued == {}
    assert marked == [("-1001", 11)]


@pytest.mark.asyncio
async def test_handle_delivery_inbound_job_queues_deliver_action_when_digest_mode_enabled(monkeypatch):
    archived: dict[str, object] = {}
    queued: dict[str, object] = {}

    monkeypatch.setattr(
        main,
        "_load_inbound_payload",
        lambda _job: {
            "channel_id": "-1001",
            "source": "Desk Wire",
            "severity": "medium",
            "candidate_text": "Officials reopened the port after three days of disruption.",
            "filter_decision": {
                "action": "deliver",
                "summary_html": "<b>Officials reopened the port</b><br>The move follows three days of disruption.",
                "copy_origin": "ai",
                "routing_origin": "ai",
                "fallback_reason": "",
                "ai_attempt_count": 1,
                "ai_quality_retry_used": False,
            },
        },
    )

    async def fake_fetch_messages(_payload):
        return [SimpleNamespace(id=77, chat_id=-1001, media=None)]

    async def fake_advance_job_to_archive(_job, payload):
        archived["payload"] = dict(payload)

    async def fail_send(*_args, **_kwargs):
        raise AssertionError("digest mode should not send per-post delivery")

    def fake_queue(channel_id, message_id, raw_text, **kwargs):
        queued["channel_id"] = channel_id
        queued["message_id"] = message_id
        queued["raw_text"] = raw_text
        queued["kwargs"] = kwargs

    monkeypatch.setattr(main, "_fetch_messages_for_payload", fake_fetch_messages)
    monkeypatch.setattr(main, "_advance_job_to_archive", fake_advance_job_to_archive)
    monkeypatch.setattr(main, "_send_text_with_ref", fail_send)
    monkeypatch.setattr(main, "_queue_for_digest", fake_queue)
    monkeypatch.setattr(main, "_is_digest_mode_enabled", lambda: True)
    monkeypatch.setattr(main, "count_pending", lambda: 1)

    await main._handle_delivery_inbound_job({"id": 21})

    assert queued["channel_id"] == "-1001"
    assert queued["message_id"] == 77
    assert "Officials reopened the port" in str(queued["raw_text"])
    assert archived["payload"]["final_action"] == "digest_queued"


@pytest.mark.asyncio
async def test_handle_delivery_inbound_job_skips_media_only_payload(monkeypatch):
    archived: dict[str, object] = {}

    monkeypatch.setattr(
        main,
        "_load_inbound_payload",
        lambda _job: {
            "channel_id": "-1001",
            "source": "Desk Wire",
            "severity": "medium",
            "candidate_text": "",
            "filter_decision": {
                "action": "deliver",
                "copy_origin": "ai",
                "routing_origin": "ai",
                "fallback_reason": "",
                "ai_attempt_count": 1,
                "ai_quality_retry_used": False,
            },
        },
    )

    async def fake_fetch_messages(_payload):
        return [SimpleNamespace(id=88, chat_id=-1001, media=True)]

    async def fake_advance_job_to_archive(_job, payload):
        archived["payload"] = dict(payload)

    async def fail_send(*_args, **_kwargs):
        raise AssertionError("media-only payload should be skipped")

    monkeypatch.setattr(main, "_fetch_messages_for_payload", fake_fetch_messages)
    monkeypatch.setattr(main, "_advance_job_to_archive", fake_advance_job_to_archive)
    monkeypatch.setattr(main, "count_pending", lambda: 1)

    await main._handle_delivery_inbound_job({"id": 22})

    assert archived["payload"]["final_action"] == "skip"
    assert archived["payload"]["skip_reason"] == "empty_message"


@pytest.mark.asyncio
async def test_handle_triage_inbound_job_skips_media_only_payload(monkeypatch):
    archived: dict[str, object] = {}

    monkeypatch.setattr(
        main,
        "_load_inbound_payload",
        lambda _job: {
            "channel_id": "-1001",
            "kind": "single",
        },
    )

    async def fake_fetch_messages(_payload):
        return [SimpleNamespace(id=99, chat_id=-1001, media=True, message="")]

    async def fake_source_info(_msg):
        return "Desk Wire", ""

    async def fake_advance_job_to_archive(_job, payload):
        archived["payload"] = dict(payload)

    monkeypatch.setattr(main, "_fetch_messages_for_payload", fake_fetch_messages)
    monkeypatch.setattr(main, "_source_info", fake_source_info)
    monkeypatch.setattr(main, "_advance_job_to_archive", fake_advance_job_to_archive)

    await main._handle_triage_inbound_job({"id": 23})

    assert archived["payload"]["final_action"] == "skip"
    assert archived["payload"]["skip_reason"] == "empty_message"


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
    assert payload["filter_decision"]["copy_origin"] == "ai"
    assert payload["filter_decision"]["routing_origin"] == "ai"
    assert payload["filter_decision"]["fallback_reason"] == ""
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
async def test_stream_query_answer_uses_threaded_final_delivery(monkeypatch):
    calls: list[dict[str, object]] = []

    async def fake_safe_reply_markdown(_event, text, *, edit_message=None, reply_to=None, prefer_bot_identity=False, bot_chat_id=None):
        if edit_message is not None:
            ref = {"message_id": int(edit_message.get("message_id", 0) or 1000)}
        else:
            ref = {"message_id": 1000 + len(calls) + 1}
        calls.append(
            {
                "text": text,
                "edit_message": edit_message,
                "reply_to": reply_to,
                "message_id": ref["message_id"],
            }
        )
        return ref

    async def fake_generate_answer_from_context(*_args, **_kwargs):
        return (
            "<b>What's the update in war</b><br>"
            + "<br>".join(
                f"• ⚠️ Bullet {idx} closes cleanly after several clauses and still ends with a full stop."
                for idx in range(1, 7)
            )
        )

    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)
    monkeypatch.setattr(main, "generate_answer_from_context", fake_generate_answer_from_context)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    original_prepare_chunks = main._prepare_query_answer_chunks
    monkeypatch.setattr(
        main,
        "_prepare_query_answer_chunks",
        lambda text, max_chars=None: original_prepare_chunks(text, max_chars=160),
    )

    answer, stats = await main._stream_query_answer(
        SimpleNamespace(chat_id=777),
        progress_message={"message_id": 1000},
        query_text="What's the update in war?",
        results=[{"text": "placeholder"}],
        history=[],
        prefer_bot_identity=False,
        bot_chat_id=None,
        root_reply_to=77,
        final_suffix_html="",
    )

    assert "What's the update in war" in answer
    assert len(calls) >= 2
    assert calls[0]["edit_message"] == {"message_id": 1000}
    send_calls = [call for call in calls if call["reply_to"] is not None]
    assert send_calls[0]["reply_to"] == 1000
    if len(send_calls) >= 2:
        assert send_calls[1]["reply_to"] == send_calls[0]["message_id"]
    assert stats.message_count == len(calls) - 1


@pytest.mark.asyncio
async def test_handle_query_request_expands_to_seven_days_before_web_crosscheck(monkeypatch):
    main.query_last_request_ts.clear()
    progress_texts: list[str] = []
    search_hours: list[int] = []
    web_hours: list[int] = []

    async def fake_safe_reply_markdown(_event, text, *, edit_message=None, reply_to=None, prefer_bot_identity=False, bot_chat_id=None):
        progress_texts.append(text)
        return edit_message or {"message_id": 700}

    async def fake_search_recent_messages(_client, _monitored, _query, *, max_messages=50, default_hours_back=24, progress_cb=None, logger=None):
        search_hours.append(default_hours_back)
        if len(search_hours) == 1:
            return [
                {
                    "text": "The Oracle data center in Dubai was also attacked.",
                    "source": "Desk Wire",
                    "timestamp": 1700000000,
                }
            ]
        return [
            {
                "text": "The Oracle data center in Dubai was also attacked.",
                "source": "Desk Wire",
                "timestamp": 1700000000,
            },
            {
                "text": "Strikes were carried out on Amazon's cloud computing center in Bahrain.",
                "source": "Desk Wire",
                "timestamp": 1699999000,
            },
        ]

    async def fake_search_recent_news_web(_query, *, hours_back, max_results, allowed_domains, require_recent, progress_cb=None, logger=None):
        web_hours.append(hours_back)
        return [
            {
                "text": "Regional reporting cited Oracle in Dubai and Amazon infrastructure in Bahrain.",
                "source": "Reuters",
                "timestamp": 1700000100,
                "is_web": True,
                "link": "https://example.com/report",
            }
        ]

    async def fake_generate_answer_from_context(*_args, **_kwargs):
        return (
            "<b>Oracle Dubai and Bahrain cloud site</b><br>"
            "Oracle's Dubai facility and an Amazon-linked cloud site in Bahrain are the main targets cited."
        )

    monkeypatch.setattr(main, "_is_query_runtime_available", lambda: True)
    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)
    monkeypatch.setattr(main, "_require_client", lambda: object())
    monkeypatch.setattr(main, "search_recent_messages", fake_search_recent_messages)
    monkeypatch.setattr(main, "_search_recent_news_web_bounded", fake_search_recent_news_web)
    monkeypatch.setattr(main, "_load_queue_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_load_archive_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_is_streaming_enabled", lambda: False)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "generate_answer_from_context", fake_generate_answer_from_context)
    monkeypatch.setattr(main, "_query_web_require_min_sources", lambda: 1)
    monkeypatch.setattr(main, "_append_query_history", lambda *_args, **_kwargs: None)

    await main._handle_query_request(
        event_ref=SimpleNamespace(chat_id=777),
        text="Which data centers did Iran hit, Oracle or AWS?",
        sender_id=77,
        chat_id="chat-77",
        reply_to=None,
        prefer_bot_identity=False,
        bot_chat_id=None,
    )

    assert search_hours == [24, 168]
    assert web_hours == [168]
    assert any(main._query_expand_status() in text for text in progress_texts)
    assert any("trusted web sources" in text for text in progress_texts)


@pytest.mark.asyncio
async def test_handle_query_request_honors_explicit_thirty_day_window_for_web_crosscheck(monkeypatch):
    main.query_last_request_ts.clear()
    progress_texts: list[str] = []
    search_hours: list[int] = []
    web_hours: list[int] = []

    async def fake_safe_reply_markdown(_event, text, *, edit_message=None, reply_to=None, prefer_bot_identity=False, bot_chat_id=None):
        progress_texts.append(text)
        return edit_message or {"message_id": 701}

    async def fake_search_recent_messages(_client, _monitored, _query, *, max_messages=50, default_hours_back=24, progress_cb=None, logger=None):
        search_hours.append(default_hours_back)
        return [
            {
                "text": "Oracle's Dubai facility was cited in attack reporting.",
                "source": "Desk Wire",
                "timestamp": 1700000000,
            },
            {
                "text": "Amazon-linked cloud infrastructure in Bahrain was also named.",
                "source": "Desk Wire",
                "timestamp": 1699999000,
            },
            {
                "text": "Additional retrospective reporting discussed broader regional infrastructure targeting.",
                "source": "Desk Wire",
                "timestamp": 1699998000,
            },
        ]

    async def fake_search_recent_news_web(_query, *, hours_back, max_results, allowed_domains, require_recent, progress_cb=None, logger=None):
        web_hours.append(hours_back)
        return [
            {
                "text": "Cross-check reporting over the same window referenced Oracle in Dubai and Amazon-linked infrastructure in Bahrain.",
                "source": "Reuters",
                "timestamp": 1700000100,
                "is_web": True,
                "link": "https://example.com/report",
            }
        ]

    async def fake_generate_answer_from_context(*_args, **_kwargs):
        return (
            "<b>Oracle Dubai and Bahrain cloud site</b><br>"
            "Oracle's Dubai facility and an Amazon-linked cloud site in Bahrain are the main targets cited."
        )

    monkeypatch.setattr(main, "_is_query_runtime_available", lambda: True)
    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)
    monkeypatch.setattr(main, "_require_client", lambda: object())
    monkeypatch.setattr(main, "search_recent_messages", fake_search_recent_messages)
    monkeypatch.setattr(main, "_search_recent_news_web_bounded", fake_search_recent_news_web)
    monkeypatch.setattr(main, "_load_queue_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_load_archive_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_is_streaming_enabled", lambda: False)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "generate_answer_from_context", fake_generate_answer_from_context)
    monkeypatch.setattr(main, "_query_web_require_min_sources", lambda: 1)
    monkeypatch.setattr(main, "_append_query_history", lambda *_args, **_kwargs: None)

    await main._handle_query_request(
        event_ref=SimpleNamespace(chat_id=778),
        text="Which data centers did Iran hit in the last 30 days, Oracle or AWS?",
        sender_id=78,
        chat_id="chat-78",
        reply_to=None,
        prefer_bot_identity=False,
        bot_chat_id=None,
    )

    assert search_hours == [720]
    assert web_hours == [720]
    assert all(text != main._query_expand_status() for text in progress_texts)
    assert any("trusted web sources" in text for text in progress_texts)


@pytest.mark.asyncio
async def test_handle_query_request_streams_search_terms_into_progress(monkeypatch):
    main.query_last_request_ts.clear()
    progress_texts: list[str] = []

    async def fake_safe_reply_markdown(_event, text, *, edit_message=None, reply_to=None, prefer_bot_identity=False, bot_chat_id=None):
        progress_texts.append(text)
        return edit_message or {"message_id": 702}

    async def fake_search_recent_messages(_client, _monitored, _query, *, max_messages=50, default_hours_back=24, progress_cb=None, logger=None):
        if progress_cb is not None:
            await progress_cb(
                {
                    "scope": "telegram",
                    "phase": "variant",
                    "variant": "oracle aws",
                    "window_hours": default_hours_back,
                }
            )
            await progress_cb(
                {
                    "scope": "telegram",
                    "phase": "fallback",
                    "variant": None,
                    "window_hours": default_hours_back,
                }
            )
        return [
            {
                "text": "Oracle's Dubai facility was cited in attack reporting.",
                "source": "Desk Wire",
                "timestamp": 1700000000,
            }
        ]

    async def fake_search_recent_news_web(_query, *, hours_back, max_results, allowed_domains, require_recent, progress_cb=None, logger=None):
        if progress_cb is not None:
            await progress_cb(
                {
                    "scope": "web",
                    "phase": "variant",
                    "provider": "google_news_rss",
                    "variant": "oracle aws",
                    "window_hours": hours_back,
                }
            )
        return [
            {
                "text": "Google News and Reuters both referenced Oracle in Dubai.",
                "source": "Reuters",
                "timestamp": 1700000100,
                "is_web": True,
                "link": "https://example.com/report",
            }
        ]

    async def fake_generate_answer_from_context(*_args, **_kwargs):
        return "<b>Oracle Dubai</b><br>Oracle's Dubai facility is the main cited target."

    monkeypatch.setattr(main, "_is_query_runtime_available", lambda: True)
    monkeypatch.setattr(main, "_safe_reply_markdown", fake_safe_reply_markdown)
    monkeypatch.setattr(main, "_require_client", lambda: object())
    monkeypatch.setattr(main, "search_recent_messages", fake_search_recent_messages)
    monkeypatch.setattr(main, "_search_recent_news_web_bounded", fake_search_recent_news_web)
    monkeypatch.setattr(main, "_load_queue_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_load_archive_query_context", lambda **_kwargs: [])
    monkeypatch.setattr(main, "_is_streaming_enabled", lambda: False)
    monkeypatch.setattr(main, "_require_auth_manager", lambda: object())
    monkeypatch.setattr(main, "generate_answer_from_context", fake_generate_answer_from_context)
    monkeypatch.setattr(main, "_query_web_require_min_sources", lambda: 1)
    monkeypatch.setattr(main, "_append_query_history", lambda *_args, **_kwargs: None)

    await main._handle_query_request(
        event_ref=SimpleNamespace(chat_id=779),
        text="Which data centers did Iran hit, Oracle or AWS?",
        sender_id=79,
        chat_id="chat-79",
        reply_to=None,
        prefer_bot_identity=False,
        bot_chat_id=None,
    )

    assert any("oracle aws" in text.lower() for text in progress_texts)
    assert any("recent history scan" in text.lower() for text in progress_texts)
    assert any("Google News RSS" in text for text in progress_texts)


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
