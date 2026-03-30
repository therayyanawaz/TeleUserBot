from __future__ import annotations

import pytest

import ai_filter
import main
from breaking_story import build_breaking_story_candidate, derive_context_evidence


class _FakeAuthManager:
    async def get_auth_context(self):
        return {"token": "fake"}

    async def refresh_auth_context(self):
        return {"token": "fake"}


def _spread_evidence():
    anchor = build_breaking_story_candidate(
        text="Officials said rockets landed near Haifa overnight.",
        headline="Officials said rockets landed near Haifa overnight.",
        topic_key="haifa_rockets",
        timestamp=1700000000,
    )
    current = build_breaking_story_candidate(
        text="Officials say two rockets landed near Acre overnight.",
        headline="Officials say two rockets landed near Acre overnight.",
        topic_key="haifa_rockets",
        timestamp=1700000300,
    )
    evidence, _score, _reason = derive_context_evidence(current, anchor, age_label="42m ago")
    assert evidence is not None
    return evidence


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
    evidence = _spread_evidence()

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
        context_evidence=evidence,
    )

    assert "Haifa" in decision.story_bridge_html
    assert "Acre" in decision.story_bridge_html


def test_resolve_vital_rational_view_for_delivery_rejects_generic_bridge(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)
    evidence = _spread_evidence()

    resolved = ai_filter.resolve_vital_rational_view_for_delivery(
        "Officials say two rockets landed near Acre overnight.",
        "Why it matters: This raises regional stability concerns.",
        None,
        evidence=evidence,
    )

    assert resolved == ""


def test_resolve_vital_rational_view_for_delivery_accepts_evidence_backed_bridge(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)
    evidence = _spread_evidence()

    bridge = ai_filter.resolve_vital_rational_view_for_delivery(
        "Officials say two rockets landed near Acre overnight.",
        "Why it matters: Earlier reports centered on Haifa; this update places the same exchange in Acre.",
        None,
        evidence=evidence,
    )

    assert bridge
    assert "Haifa" in bridge
    assert "Acre" in bridge


def test_resolve_vital_rational_view_for_delivery_rejects_headline_restate(monkeypatch):
    monkeypatch.setattr(ai_filter.config, "BREAKING_STYLE_MODE", "unhinged", raising=False)
    evidence = _spread_evidence()

    bridge = ai_filter.resolve_vital_rational_view_for_delivery(
        "Officials say two rockets landed near Acre overnight.",
        "Why it matters: Officials say two rockets landed near Acre overnight.",
        None,
        evidence=evidence,
    )

    assert bridge == ""


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

    assert rendered.count("<b>") == 1
    assert "<u>" not in rendered
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
    assert "same thread into Acre" in plain
    assert rendered.count("<b>") == 1
    assert "<u>" not in rendered


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
    assert "<u>" not in rendered


def test_should_attach_vital_opinion_unhinged_uses_context_not_probability(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_humanized_vital_opinion_enabled", lambda: True)
    monkeypatch.setattr(main, "_humanized_vital_opinion_probability", lambda: 0.0)

    assert main._should_attach_vital_opinion("test", []) is False
    assert main._should_attach_vital_opinion("test", ["1h ago: Earlier strike near Haifa."]) is True
