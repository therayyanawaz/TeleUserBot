from __future__ import annotations

import json

import pytest

import main
import news_taxonomy
import utils


def _positive_sample(category: news_taxonomy.NewsCategory) -> str:
    phrase = category.primary_phrases[0]
    if category.domain == "war_security":
        return f"Officials confirmed {phrase} near the border minutes ago."
    if category.domain == "state_politics":
        return f"Officials announced {phrase} after emergency talks today."
    if category.domain == "economy_business":
        return f"Markets reacted after {phrase} was confirmed today."
    if category.domain == "infrastructure_transport_tech":
        return f"Authorities confirmed {phrase} after the latest disruption today."
    return f"Emergency officials confirmed {phrase} in the latest update today."


def _negative_sample(category: news_taxonomy.NewsCategory) -> str:
    joined = category.primary_phrases[0].replace(" ", "").replace("-", "")
    return f"The identifier pre{joined}post was archived in a spreadsheet string, not a live report."


@pytest.mark.parametrize(
    "category",
    news_taxonomy.get_news_taxonomy().categories,
    ids=lambda item: item.key,
)
def test_each_category_has_positive_and_negative_fixture(category: news_taxonomy.NewsCategory) -> None:
    positive = _positive_sample(category)
    match = news_taxonomy.match_news_category(positive)

    assert match is not None
    assert match.category_key == category.key

    negative = _negative_sample(category)
    negative_matches = news_taxonomy.find_news_category_matches(negative)
    assert not any(item.category_key == category.key for item in negative_matches)


def test_taxonomy_schema_requires_valid_entries(tmp_path) -> None:
    payload = {
        "version": 1,
        "categories": [
            {
                "key": "duplicate_case",
                "label": "Label",
                "priority": 1,
                "domain": "war_security",
                "breaking_eligible": True,
                "severity_bias": "urgent",
                "primary_phrases": [],
                "alias_phrases": [],
                "required_any": [],
                "blocked_phrases": [],
            }
        ],
    }
    path = tmp_path / "bad_taxonomy.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(news_taxonomy.NewsTaxonomyError):
        news_taxonomy.load_news_taxonomy(path, force_reload=True)


def test_taxonomy_contains_expected_category_count() -> None:
    taxonomy = news_taxonomy.get_news_taxonomy()

    assert len(taxonomy.categories) == 60


def test_precedence_prefers_higher_scoring_category() -> None:
    match = news_taxonomy.match_news_category(
        "Officials confirmed a missile strike after a ballistic missile barrage despite air defense activation."
    )

    assert match is not None
    assert match.category_key == "missile_strike"


def test_precedence_prefers_court_ruling_when_it_has_stronger_hits() -> None:
    match = news_taxonomy.match_news_category(
        "A court ruling from the Supreme Court blocked the decree after the president spoke."
    )

    assert match is not None
    assert match.category_key == "court_ruling"


def test_unmatched_post_falls_back_to_generic_label() -> None:
    label = utils.choose_alert_label(
        "A museum exhibition reopened downtown after renovation work finished.",
        severity="medium",
    )

    assert label == "News Update"


def test_breaking_keyword_detection_requires_live_event_context() -> None:
    assert main._contains_breaking_keyword("Analysis: why interception attempts keep failing.") is False
    assert main._contains_breaking_keyword(
        "Officials confirmed interceptions over Haifa minutes ago."
    ) is True


def test_legacy_breaking_keywords_do_not_create_specific_labels(monkeypatch) -> None:
    monkeypatch.setattr(main.config, "BREAKING_NEWS_KEYWORDS", ["alert"], raising=False)

    label = utils.choose_alert_label(
        "Alert: analysis of missile defense procurement trends.",
        severity="high",
    )

    assert label == "Breaking"
