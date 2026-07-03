from __future__ import annotations

import time
from unittest.mock import patch

import ai_filter


class RailResult(list):
    def __init__(self, headlines: list[str], also_moving: list[str]) -> None:
        super().__init__(headlines)
        self._also_moving = list(also_moving)

    def __getitem__(self, item):
        if isinstance(item, str):
            if item == "headlines":
                return list(self)
            if item == "also_moving":
                return list(self._also_moving)
            raise KeyError(item)
        return super().__getitem__(item)


def make_post(
    *,
    channel_id: int,
    text: str,
    severity: str = "medium",
    age_seconds: int = 300,
    topic_key: str = "misc",
    same_topic_source_count: int = 1,
    ai_index: int = 0,
):
    """Build a minimal synthetic post for ranker testing."""
    return {
        "channel_id": channel_id,
        "text": text,
        "severity": severity,
        "timestamp": int(time.time()) - age_seconds,
        "topic_key": topic_key,
        "same_topic_source_count": same_topic_source_count,
        "ai_index": ai_index,
    }


def build_final_rail(posts: list[dict], max_lines: int = 3) -> RailResult:
    fixed_now = 1_000_000
    base_posts = sorted(posts, key=lambda post: int(post.get("ai_index") or 0))

    ranking_posts: list[dict] = []
    headline_candidates: list[str] = []

    for post in base_posts:
        cleaned = ai_filter._headline_rail_clean_line(post["text"], max_chars=0)
        if cleaned:
            headline_candidates.append(cleaned)

        source_count = max(1, int(post.get("same_topic_source_count") or 1))
        age_seconds = max(0, int(time.time()) - int(post["timestamp"]))
        for offset in range(source_count):
            ranking_posts.append(
                {
                    "channel_id": str(int(post["channel_id"]) + offset),
                    "source_name": f"Source {int(post['channel_id']) + offset}",
                    "raw_text": post["text"],
                    "text": post["text"],
                    "timestamp": fixed_now - age_seconds,
                    "severity": post["severity"],
                    "topic_key": post.get("topic_key") or "misc",
                    "is_web": False,
                }
            )

    with patch("ai_filter.time.time", return_value=fixed_now), patch.object(
        ai_filter.config, "RECENCY_HALF_LIFE_SEC", 300.0, create=True
    ), patch(
        "ai_filter.headlined_story_exists", return_value=False
    ), patch("ai_filter.headlined_story_add"), patch(
        "ai_filter.headlined_story_last_severity", return_value=""
    ):
        cleaned_headlines = ai_filter._clean_headline_rail_items(
            headline_candidates,
            max_lines=max_lines,
            max_chars=0,
        )
        ranked = ai_filter.rank_headline_rail_items(
            cleaned_headlines,
            ranking_posts,
        )
        headlines, also_moving = ai_filter.apply_cross_digest_headline_dedup(
            ranked,
            max_lines=max_lines,
        )
    return RailResult(headlines, also_moving)


def test_high_severity_beats_low_severity():
    posts = [
        make_post(
            channel_id=1,
            text="Election Commission announced minor local election results in Lahore.",
            severity="low",
            ai_index=0,
            topic_key="elections",
        ),
        make_post(
            channel_id=2,
            text="Iran closes Strait of Hormuz, IRGC blocks tankers.",
            severity="high",
            ai_index=1,
            topic_key="hormuz",
        ),
    ]
    rail = build_final_rail(posts, max_lines=2)
    assert "Hormuz" in rail[0] or "IRGC" in rail[0] or "Iran" in rail[0]


def test_fresh_beats_stale_equal_severity():
    posts = [
        make_post(
            channel_id=3,
            text="Lebanese army reopens roads in Tyre after overnight clashes.",
            severity="medium",
            age_seconds=1700,
            ai_index=1,
            topic_key="beirut",
        ),
        make_post(
            channel_id=4,
            text="Hezbollah fires rockets at northern Israel after sirens.",
            severity="medium",
            age_seconds=60,
            ai_index=0,
            topic_key="rockets",
        ),
    ]
    rail = build_final_rail(posts, max_lines=2)
    assert "Hezbollah" in rail[0] or "rockets" in rail[0].lower()


def test_high_severity_old_beats_low_severity_fresh():
    posts = [
        make_post(
            channel_id=5,
            text="Iran closes Strait of Hormuz after IRGC deployment.",
            severity="high",
            age_seconds=1500,
            ai_index=0,
            topic_key="hormuz",
        ),
        make_post(
            channel_id=6,
            text="Border patrol reported minor movement near the crossing.",
            severity="low",
            age_seconds=30,
            ai_index=1,
            topic_key="border",
        ),
    ]
    rail = build_final_rail(posts, max_lines=2)
    assert "Hormuz" in rail[0] or "Iran" in rail[0]


def test_corroborated_beats_single_source():
    posts = [
        make_post(
            channel_id=7,
            text="US Treasury issues fresh Iran sanctions order.",
            severity="medium",
            same_topic_source_count=1,
            ai_index=1,
            topic_key="trump_iran",
        ),
        make_post(
            channel_id=8,
            text="Russia launches missile strike on Kharkiv after sirens.",
            severity="medium",
            same_topic_source_count=4,
            ai_index=0,
            topic_key="kharkiv",
        ),
    ]
    rail = build_final_rail(posts, max_lines=2)
    assert "Kharkiv" in rail[0] or "Russia" in rail[0]


def test_promo_line_never_enters_rail():
    posts = [
        make_post(
            channel_id=9,
            text="DM if you've enrolled in CDS journey new batch.",
            severity="low",
            ai_index=0,
            topic_key="promo",
        ),
        make_post(
            channel_id=10,
            text="Lebanese army reopens roads in southern Lebanon after clashes.",
            severity="medium",
            ai_index=1,
            topic_key="lebanon",
        ),
    ]
    rail = build_final_rail(posts, max_lines=2)
    assert not any("DM" in line or "batch" in line for line in rail)
    assert any("Lebanon" in line or "Lebanese" in line for line in rail)


def test_rail_order_is_deterministic():
    posts = [
        make_post(
            channel_id=11,
            text="Pakistan detains US nationals at Srinagar airport.",
            severity="medium",
            ai_index=0,
            topic_key="srinagar",
        ),
        make_post(
            channel_id=12,
            text="Iran intercepts two oil tankers in Hormuz.",
            severity="high",
            ai_index=1,
            topic_key="hormuz",
        ),
        make_post(
            channel_id=13,
            text="Belarus prepares second front in Ukraine.",
            severity="medium",
            ai_index=2,
            topic_key="belarus",
        ),
    ]
    rail_1 = build_final_rail(posts, max_lines=3)
    rail_2 = build_final_rail(posts, max_lines=3)
    assert rail_1 == rail_2


def test_cross_digest_dedup_demotes_repeat_topic():
    topic_line = "Iran again blocks tanker in Hormuz."
    repeat_topic_key = ai_filter._headline_rail_topic_key(topic_line)
    fixed_now = 1_000_000
    posts = [
        make_post(
            channel_id=14,
            text=topic_line,
            severity="medium",
            ai_index=0,
            topic_key="hormuz_closure",
        ),
        make_post(
            channel_id=15,
            text="IDF soldier killed by IED in Lebanon village.",
            severity="high",
            ai_index=1,
            topic_key="idf_ied",
        ),
    ]

    base_posts = sorted(posts, key=lambda post: int(post.get("ai_index") or 0))
    headline_candidates = [
        ai_filter._headline_rail_clean_line(post["text"], max_chars=0)
        for post in base_posts
        if ai_filter._headline_rail_clean_line(post["text"], max_chars=0)
    ]
    ranking_posts = [
        {
            "channel_id": str(post["channel_id"]),
            "source_name": f"Source {post['channel_id']}",
            "raw_text": post["text"],
            "text": post["text"],
            "timestamp": fixed_now - max(0, int(time.time()) - int(post["timestamp"])),
            "severity": post["severity"],
            "topic_key": post["topic_key"],
            "is_web": False,
        }
        for post in base_posts
    ]

    with patch("ai_filter.time.time", return_value=fixed_now), patch.object(
        ai_filter.config, "RECENCY_HALF_LIFE_SEC", 300.0, create=True
    ), patch(
        "ai_filter.headlined_story_exists",
        side_effect=lambda topic_key, within_seconds: topic_key == repeat_topic_key,
    ), patch("ai_filter.headlined_story_add"), patch(
        "ai_filter.headlined_story_last_severity", return_value="medium"
    ):
        cleaned_headlines = ai_filter._clean_headline_rail_items(
            headline_candidates,
            max_lines=2,
            max_chars=0,
        )
        ranked = ai_filter.rank_headline_rail_items(cleaned_headlines, ranking_posts)
        result = ai_filter.apply_cross_digest_headline_dedup(ranked, max_lines=2)
        main_rail, also_moving = result

    assert not any("Hormuz" in line or "tanker" in line for line in main_rail)
    assert any("Hormuz" in line or "tanker" in line for line in also_moving)
