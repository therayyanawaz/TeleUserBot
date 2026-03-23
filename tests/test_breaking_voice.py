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


def test_format_breaking_text_unhinged_is_single_line(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "unhinged")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)

    rendered = main._format_breaking_text("NYT", "Two rockets landed near Haifa overnight.", "Why it matters: context")

    assert "<br>" not in rendered.lower()
    assert "Why it matters" not in rendered
    assert "Two rockets landed near Haifa overnight." in rendered


def test_format_breaking_text_classic_keeps_bridge(monkeypatch):
    monkeypatch.setattr(main, "resolve_breaking_style_mode", lambda: "classic")
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)

    rendered = main._format_breaking_text("NYT", "Two rockets landed near Haifa overnight.", "Why it matters: context")

    assert "<br><br>" in rendered
    assert "Why it matters: context." in rendered
