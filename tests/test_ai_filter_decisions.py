from __future__ import annotations

import importlib
import json

import httpx
import pytest

import ai_filter
import db


class _FakeAuthManager:
    async def get_auth_context(self):
        return {"token": "fake"}

    async def refresh_auth_context(self):
        return {"token": "fake"}


@pytest.mark.asyncio
async def test_decide_filter_action_uses_persistent_cache(isolated_db, monkeypatch):
    importlib.reload(ai_filter)
    ai_filter._FILTER_DECISION_CACHE_HITS = 0
    ai_filter._FILTER_DECISION_CACHE_MISSES = 0

    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        return json.dumps(
            {
                "action": "deliver",
                "severity": "high",
                "summary_html": (
                    "<b>Officials confirmed new power-grid restrictions across three districts</b><br>"
                    "Authorities said the order takes effect tonight after the latest outage."
                ),
                "headline_html": "Officials confirmed new power-grid restrictions across three districts",
                "story_bridge_html": "Why it matters: context",
                "confidence": 0.92,
                "reason_code": "newsworthy_update",
                "topic_key": "power_grid",
                "needs_ocr_translation": False,
            }
        )

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = (
        "Officials confirmed new power-grid restrictions across three districts, "
        "and authorities said the order takes effect tonight after the latest outage."
    )
    first = await ai_filter.decide_filter_action(text, _FakeAuthManager())
    second = await ai_filter.decide_filter_action(text, _FakeAuthManager())

    assert first.action == "deliver"
    assert first.copy_origin == "ai"
    assert second.cached is True
    assert calls["count"] == 1


def test_validate_filter_decision_sanitizes_and_clamps():
    decision = ai_filter._validate_filter_decision(
        {
            "action": "deliver",
            "severity": "critical",
            "summary_html": "<script>alert(1)</script><b>Keep</b>",
            "headline_html": "<i>Headline</i>",
            "story_bridge_html": "<div>Bridge</div>",
            "confidence": 9.0,
            "reason_code": "bad value !!!",
            "topic_key": "Topic Key / 123",
            "needs_ocr_translation": "yes",
        },
        "A long enough news text that should not be treated as noise.",
    )

    assert decision.action == "deliver"
    assert decision.severity in {"high", "medium", "low"}
    assert "<script>" not in decision.summary_html
    assert decision.confidence == 1.0
    assert decision.reason_code == "bad_value"
    assert " " not in decision.topic_key
    assert decision.topic_key.lower() == decision.topic_key
    assert decision.needs_ocr_translation is True


@pytest.mark.asyncio
async def test_decide_filter_action_falls_back_on_invalid_json(isolated_db, monkeypatch):
    importlib.reload(ai_filter)
    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        return "this is not valid json"

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = "Regional authorities issued a late-night statement on transport disruptions."
    decision = await ai_filter.decide_filter_action(text, _FakeAuthManager())

    assert decision.action in {"deliver", "digest"}
    assert decision.summary_html
    assert decision.copy_origin == "fallback"
    assert decision.fallback_reason == "invalid_payload"
    assert decision.ai_attempt_count == 2
    assert decision.ai_quality_retry_used is True
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_decide_filter_action_retries_weak_ai_copy_before_accepting(isolated_db, monkeypatch):
    importlib.reload(ai_filter)
    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return json.dumps(
                {
                    "action": "deliver",
                    "severity": "high",
                    "summary_html": "<b>Situation update in Tehran</b>",
                    "headline_html": "Situation update in Tehran",
                    "story_bridge_html": "",
                    "confidence": 0.88,
                    "reason_code": "breaking_update",
                    "topic_key": "tehran_update",
                    "needs_ocr_translation": False,
                }
            )
        return json.dumps(
            {
                "action": "deliver",
                "severity": "high",
                "summary_html": (
                    "<b>Iranian officials ordered a new airspace restriction over Tehran</b><br>"
                    "Authorities said the measure takes effect immediately after the latest warning."
                ),
                "headline_html": "Iranian officials ordered a new airspace restriction over Tehran",
                "story_bridge_html": "",
                "confidence": 0.91,
                "reason_code": "breaking_update",
                "topic_key": "tehran_airspace",
                "needs_ocr_translation": False,
            }
        )

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = (
        "Iranian officials ordered a new airspace restriction over Tehran, "
        "and authorities said the measure takes effect immediately after the latest warning."
    )
    decision = await ai_filter.decide_filter_action(text, _FakeAuthManager())

    assert decision.copy_origin == "ai"
    assert decision.ai_attempt_count == 2
    assert decision.ai_quality_retry_used is True
    assert "airspace restriction over Tehran" in decision.summary_html
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_call_codex_non_stream_retries_transport_errors(monkeypatch):
    attempts = {"count": 0}
    reset_calls: list[str] = []

    class _FakeClient:
        async def post(self, _url, headers=None, json=None):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise httpx.TransportError(
                    "Server closed the connection: [Errno 104] Connection reset by peer"
                )
            return httpx.Response(
                200,
                json={
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": "Recovered response",
                                }
                            ],
                        }
                    ]
                },
            )

    async def fake_get_codex_http_client():
        return _FakeClient()

    async def fake_reset_shared_http_client(name: str):
        reset_calls.append(name)

    monkeypatch.setattr(ai_filter, "get_codex_http_client", fake_get_codex_http_client)
    monkeypatch.setattr(ai_filter, "reset_shared_http_client", fake_reset_shared_http_client)

    result = await ai_filter._call_codex_non_stream(
        "headline",
        {
            "access_token": "token",
            "account_id": "acct-123",
        },
        "instructions",
    )

    assert result == "Recovered response"
    assert attempts["count"] == 2
    assert reset_calls == ["codex"]


@pytest.mark.asyncio
async def test_create_digest_summary_result_falls_back_after_weak_ai_retry(monkeypatch):
    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        return "⚠️ Situation update in Tehran"

    async def fake_normalize_digest_output(content, *_args, **_kwargs):
        return content

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)
    monkeypatch.setattr(ai_filter, "_normalize_digest_output", fake_normalize_digest_output)
    monkeypatch.setattr(ai_filter.config, "DIGEST_PREFER_JSON_OUTPUT", False, raising=False)

    result = await ai_filter.create_digest_summary_result(
        [
            {
                "text": "Iranian officials ordered a new airspace restriction over Tehran.",
                "source_name": "Desk",
            }
        ],
        auth_manager=_FakeAuthManager(),
        interval_minutes=60,
    )

    assert result.copy_origin == "fallback"
    assert result.fallback_reason == "quality_rejected_after_retry"
    assert result.ai_attempt_count == 2
    assert result.ai_quality_retry_used is True
    assert calls["count"] == 2


def test_json_digest_to_html_renders_story_blocks():
    html = ai_filter._json_digest_to_html(
        {
            "quiet": False,
            "scene_setter": "Port operations and nearby security activity drove the window.",
            "major_blocks": [
                {
                    "headline": "Port reopens after a three-day shutdown",
                    "lede": "Officials said cargo traffic resumes at dawn.",
                    "priority": "medium",
                    "facts": [
                        "Security checks remain in place around the eastern gate.",
                    ],
                }
            ],
            "timeline_items": [
                "Air defenses were activated over the northern district after a fresh barrage."
            ],
        },
        interval_minutes=30,
        max_lines=12,
    )

    assert "Port operations and nearby security activity drove the window." in html
    assert "<b>Port reopens after a three-day shutdown</b>" in html
    assert "Officials said cargo traffic resumes at dawn." in html
    assert "• Security checks remain in place around the eastern gate." in html
    assert "<i>Also moving</i>" in html
    assert "• Air defenses were activated over the northern district after a fresh barrage." in html


def test_local_fallback_digest_keeps_all_distinct_updates():
    html = ai_filter.local_fallback_digest(
        [
            {
                "text": "Officials reopened the port after three days of disruption. Cargo traffic resumes at dawn.",
                "source_name": "Desk",
            },
            {
                "text": "Air defenses fired over the northern district after a fresh barrage. Residents reported new blasts near the ridge.",
                "source_name": "Desk",
            },
        ],
        interval_minutes=30,
    )
    plain = ai_filter.strip_telegram_html(html)

    assert "Officials reopened the port after three days of disruption" in plain
    assert "Cargo traffic resumes at dawn" in plain
    assert "Air defenses fired over the northern district after a fresh barrage" in plain
    assert "Residents reported new blasts near the ridge" in plain
    assert plain.count("Officials reopened the port after three days of disruption") == 1
    assert plain.count("Air defenses fired over the northern district after a fresh barrage") == 1


def test_html_digest_cleanup_strips_promo_handles_and_duplicate_blocks():
    raw = """
There is a confirmed fall of fission fragments in a number of locations in the middle of the entity.
• 🌟🌟Follow Us | Discussion | 🤔 Boost the Channel 💚

🇮🇷🇮🇷⚔️🏴‍☠️🇺🇸 Enemy media: Preliminary reports of a direct hit in Petah Tikva.
• @stayfreeworld

A fall in Beit She'an 🌟🌟Follow Us | Discussion | 🤔 Boost the Channel 💚
• A fall in Beit She'an 🌟🌟Follow Us | Discussion | 🤔 Boost the Channel 💚

<i>Also moving</i>
• Report of more launches towards Israel @AlHaqNews
• #Paylaş
""".strip()

    html = ai_filter._html_digest_cleanup(raw, interval_minutes=30, max_lines=12)
    plain = ai_filter.strip_telegram_html(html)

    assert "Follow Us" not in plain
    assert "Boost the Channel" not in plain
    assert "@stayfreeworld" not in plain
    assert "@AlHaqNews" not in plain
    assert "#Paylaş" not in plain
    assert "Enemy media:" not in plain
    assert plain.count("A fall in Beit She'an") == 1
    assert "Also moving" in plain


@pytest.mark.asyncio
async def test_prepare_digest_posts_translates_non_english_lines(monkeypatch):
    async def fake_call_codex_with_auth_repair(_payload, _auth_manager, _instructions, **_kwargs):
        return json.dumps(
            {
                "items": [
                    {
                        "id": 1,
                        "text": "Hebrew-language sources reported continuous unusual explosions in Tel Aviv.",
                    }
                ]
            }
        )

    monkeypatch.setattr(ai_filter, "_call_codex_with_auth_repair", fake_call_codex_with_auth_repair)

    prepared, stats = await ai_filter._prepare_digest_posts(
        [
            {
                "text": "☄️ İvrit mənbələri: Tel-Əvivdə fasiləsiz qeyri-adi partlayış səsləri eşidilir #Şərh_yaz.",
                "source_name": "Desk",
            }
        ],
        _FakeAuthManager(),
    )

    assert prepared[0]["raw_text"] == "Initial reports indicate continuous unusual explosions in Tel Aviv."
    assert stats["translation_applied_count"] == 1
    assert stats["citation_stripped_count"] >= 1


def test_digest_clean_line_rewrites_citation_style_attribution_to_generic_uncertainty():
    cleaned = ai_filter._digest_clean_line(
        "Hebrew-language sources reported continuous unusual explosions in Tel Aviv.",
        max_chars=220,
        allow_short=True,
    )

    assert cleaned == "Initial reports indicate continuous unusual explosions in Tel Aviv."


@pytest.mark.asyncio
async def test_prepare_digest_posts_collapses_duplicate_citation_variants():
    prepared, stats = await ai_filter._prepare_digest_posts(
        [
            {
                "text": "Enemy media: Preliminary reports of a direct hit in Petah Tikva.",
                "source_name": "Desk",
            },
            {
                "text": "Fox: Preliminary reports of a direct hit in Petah Tikva.",
                "source_name": "Desk",
            },
        ],
        _FakeAuthManager(),
    )

    assert len(prepared) == 1
    assert prepared[0]["raw_text"] == "Preliminary reports indicate a direct hit in Petah Tikva."
    assert stats["citation_stripped_count"] >= 2
    assert stats["duplicate_collapsed_count"] == 1


def test_digest_quality_issue_rejects_direct_citation_language():
    html = (
        "<b>Central Israel Impact Reports</b><br>"
        "Hebrew-language sources reported continuous unusual explosions in Tel Aviv."
    )

    assert ai_filter._digest_quality_issue(html, ai_filter.quiet_period_message(30)) == "citation_leak"


def test_digest_quality_issue_rejects_source_leaks_and_messy_layout():
    html = (
        "<b>Central Israel Impact Reports</b><br>"
        "Central Israel Impact Reports<br>"
        "• @stayfreeworld"
    )

    assert ai_filter._digest_quality_issue(html, ai_filter.quiet_period_message(30)) in {
        "citation_leak",
        "source_leak",
        "messy_layout",
    }


@pytest.mark.asyncio
async def test_generate_answer_from_context_result_uses_ai_after_quality_retry(monkeypatch):
    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return "<b>There are developments</b><br>• ⚠️ Situation update in Tehran"
        return (
            "<b>Tehran Airspace Restriction</b><br>"
            "• ⚠️ Iranian officials ordered a new airspace restriction over Tehran.<br>"
            "• ⚠️ Authorities said it takes effect immediately after the latest warning."
        )

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)
    monkeypatch.setattr(ai_filter, "_query_confidence_allows_answer", lambda *_args, **_kwargs: True)

    result = await ai_filter.generate_answer_from_context_result(
        "What's the latest out of Tehran?",
        [{"text": "Iranian officials ordered a new airspace restriction over Tehran."}],
        auth_manager=_FakeAuthManager(),
    )

    assert result.copy_origin == "ai"
    assert result.ai_attempt_count == 2
    assert result.ai_quality_retry_used is True
    assert "airspace restriction over Tehran" in result.html
    assert calls["count"] == 2
