from __future__ import annotations

import importlib
import json

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
                "severity": "medium",
                "summary_html": "<b>Summary</b> text",
                "headline_html": "Headline",
                "story_bridge_html": "Why it matters: context",
                "confidence": 0.92,
                "reason_code": "newsworthy_update",
                "topic_key": "headline",
                "needs_ocr_translation": False,
            }
        )

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = "Officials confirmed a significant infrastructure update affecting multiple districts."
    first = await ai_filter.decide_filter_action(text, _FakeAuthManager())
    second = await ai_filter.decide_filter_action(text, _FakeAuthManager())

    assert first.action == "deliver"
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

    async def fake_call_codex(*args, **kwargs):
        return "this is not valid json"

    monkeypatch.setattr(ai_filter, "_call_codex", fake_call_codex)

    text = "Regional authorities issued a late-night statement on transport disruptions."
    decision = await ai_filter.decide_filter_action(text, _FakeAuthManager())

    assert decision.action in {"deliver", "digest"}
    assert decision.summary_html
