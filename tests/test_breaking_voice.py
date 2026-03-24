from __future__ import annotations

import pytest

import ai_filter
import main


class _FakeAuthManager:
    async def get_auth_context(self):
        return {"token": "fake"}

    async def refresh_auth_context(self):
        return {"token": "fake"}


def test_resolve_breaking_headline_for_delivery_rejects_invented_numbers(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    source = "Officials say two rockets landed near Haifa overnight."
    candidate = "Officials say five rockets landed near Haifa overnight."

    assert ai_filter.resolve_breaking_headline_for_delivery(source, candidate) == ai_filter._safe_breaking_headline_fallback(source)


def test_resolve_breaking_headline_for_delivery_preserves_hedging(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    source = "Local reports say an explosion may have hit a warehouse near Isfahan."
    candidate = "Explosion hits warehouse near Isfahan."

    resolved = ai_filter.resolve_breaking_headline_for_delivery(source, candidate)

    assert resolved == ai_filter._safe_breaking_headline_fallback(source)
    assert "may" in resolved.lower() or "report" in resolved.lower()


def test_validate_filter_decision_clears_breaking_bridge_in_unhinged_mode(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    decision = ai_filter._validate_filter_decision(
        {
            "action": "deliver",
            "severity": "high",
            "summary_html": "<b>Summary</b>",
            "headline_html": "Officials say five rockets landed near Haifa overnight.",
            "story_bridge_html": "Why it matters: context",
            "confidence": 0.8,
            "reason_code": "breaking_update",
            "topic_key": "haifa",
            "needs_ocr_translation": False,
        },
        "Officials say two rockets landed near Haifa overnight.",
    )

    assert decision.story_bridge_html == ""
    assert decision.headline_html == ai_filter._safe_breaking_headline_fallback(
        "Officials say two rockets landed near Haifa overnight."
    )


def test_validate_filter_decision_accepts_contextual_bridge_in_unhinged_mode(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    decision = ai_filter._validate_filter_decision(
        {
            "action": "deliver",
            "severity": "high",
            "summary_html": "<b>Summary</b>",
            "headline_html": "Officials say two rockets landed near Acre overnight.",
            "story_bridge_html": "Why it matters: Earlier alerts were centered on Haifa; this now pushes the same thread into Acre.",
            "confidence": 0.8,
            "reason_code": "breaking_update",
            "topic_key": "acre",
            "needs_ocr_translation": False,
        },
        "Officials say two rockets landed near Acre overnight.",
        recent_context=["42m ago: Officials said rockets landed near Haifa overnight."],
    )

    assert "Haifa" in decision.story_bridge_html
    assert "Acre" in decision.story_bridge_html


def test_resolve_vital_rational_view_for_delivery_rejects_generic_bridge(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    resolved = ai_filter.resolve_vital_rational_view_for_delivery(
        "Officials say two rockets landed near Acre overnight.",
        "Why it matters: This raises regional stability concerns.",
        ["42m ago: Officials said rockets landed near Haifa overnight."],
    )

    assert "regional stability" not in resolved.lower()
    assert "Haifa" in resolved
    assert "Acre" in resolved


def test_fallback_story_bridge_builds_spread_bridge(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    bridge = ai_filter._fallback_story_bridge(
        "Officials say two rockets landed near Acre overnight.",
        ["42m ago: Officials said rockets landed near Haifa overnight."],
    )

    assert bridge is not None
    assert "Haifa" in bridge
    assert "Acre" in bridge


def test_fallback_story_bridge_omits_when_no_relation(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)

    bridge = ai_filter._fallback_story_bridge(
        "A court hearing was delayed in Rabat.",
        ["2h ago: Wheat prices rose in Buenos Aires after new export guidance."],
    )

    assert bridge is None


@pytest.mark.asyncio
async def test_summarize_breaking_headline_uses_style_specific_cache(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "classic", raising=False)
    ai_filter._HEADLINE_CACHE.clear()

    calls = {"count": 0}

    async def fake_call_codex(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return "Two rockets landed near Haifa overnight."
        return "Officials say two rockets landed near Haifa overnight."

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = "Officials say two rockets landed near Haifa overnight."
    classic = await ai_filter.summarize_breaking_headline(text, _FakeAuthManager())

    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)
    unhinged = await ai_filter.summarize_breaking_headline(text, _FakeAuthManager())
    repeated = await ai_filter.summarize_breaking_headline(text, _FakeAuthManager())

    assert classic == "Two rockets landed near Haifa overnight."
    assert unhinged == "Officials say two rockets landed near Haifa overnight."
    assert repeated == unhinged
    assert calls["count"] == 2


def test_format_breaking_text_unhinged_uses_editorial_card_without_context(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_breaking_text("NYT", "Two rockets landed near Haifa overnight.", None)
    plain = ai_filter.strip_telegram_html(rendered)

    assert "〔" in rendered
    assert "Why it matters" not in rendered
    assert "Two rockets landed near Haifa overnight" in plain
    assert "NYT" not in plain


def test_format_breaking_text_unhinged_supports_context_line(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_breaking_text(
        "NYT",
        "Two rockets landed near Acre overnight.",
        "Why it matters: Earlier alerts were centered on Haifa; this now pushes the same thread into Acre.",
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "Why it matters" in plain
    assert "Haifa" in plain
    assert "Acre" in plain
    assert "〔" in rendered


def test_format_breaking_text_unhinged_supports_context_block(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "editorial_card")

    rendered = main._format_breaking_text(
        "NYT",
        "Two rockets landed near Acre overnight.",
        "Why it matters: Earlier alerts were centered on Haifa.\nThis now pushes the same thread into Acre.",
    )
    plain = ai_filter.strip_telegram_html(rendered)

    assert "Why it matters" in plain
    assert "Haifa" in plain
    assert "Acre" in plain


def test_format_breaking_text_classic_keeps_bridge_in_legacy_mode(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "classic")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
    monkeypatch.setattr(main, "_resolve_outbound_post_layout", lambda: "legacy")

    rendered = main._format_breaking_text("NYT", "Two rockets landed near Haifa overnight.", "Why it matters: context")

    assert "<br><br>" in rendered
    assert "Why it matters: context." in rendered


def test_should_attach_vital_opinion_unhinged_uses_context_not_probability(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_humanized_vital_opinion_enabled", lambda: True)
    monkeypatch.setattr(main, "_humanized_vital_opinion_probability", lambda: 0.0)

    assert main._should_attach_vital_opinion("test", []) is False
    assert main._should_attach_vital_opinion("test", ["1h ago: Earlier strike near Haifa."]) is True
