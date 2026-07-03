from __future__ import annotations

from types import SimpleNamespace

import pytest

import db
import main
from breaking_story import (
    build_breaking_story_candidate,
    compute_breaking_story_cluster_key,
    derive_context_evidence,
    resolve_breaking_story_cluster,
    serialize_breaking_story_facts,
)


def _cluster_row(candidate, *, root_message_id: int = 9001, update_count: int = 0, opened_ts: int = 1700000000):
    return {
        "cluster_id": "cluster-1",
        "cluster_key": compute_breaking_story_cluster_key(candidate),
        "topic_key": candidate.topic_key,
        "taxonomy_key": candidate.taxonomy_key,
        "root_message_id": root_message_id,
        "root_sent_ref": '{"message_id":9001}',
        "current_headline": candidate.headline,
        "current_facts_json": serialize_breaking_story_facts(candidate.facts),
        "opened_ts": opened_ts,
        "updated_ts": opened_ts,
        "last_delivery_ts": opened_ts,
        "update_count": update_count,
        "status": "active",
    }


def test_story_cluster_suppresses_same_incident_echo():
    earlier = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured, according to Magen David Adom.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    later = build_breaking_story_candidate(
        text="Nine wounded after an Iranian rocket hit Bnei Brak in the Tel Aviv area, emergency services say.",
        headline="Nine wounded after Iranian rocket hit in Bnei Brak.",
        topic_key="bnei_brak_impact",
        timestamp=1700000030,
    )

    resolution = resolve_breaking_story_cluster(later, [_cluster_row(earlier)], now_ts=1700000030)

    assert resolution.decision == "duplicate_echo"


def test_story_cluster_detects_casualty_change():
    earlier = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    later = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 12 injured, according to Magen David Adom.",
        headline="Iranian rocket impact in Bnei Brak leaves 12 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000060,
    )

    resolution = resolve_breaking_story_cluster(later, [_cluster_row(earlier)], now_ts=1700000060)

    assert resolution.decision == "material_update"
    assert resolution.reason == "casualty_change"


def test_story_cluster_rejects_unrelated_story():
    local_strike = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    broader_policy = build_breaking_story_candidate(
        text="Israeli officials signal weeks more fighting in Iran, saying key military targets still stand.",
        headline="Israeli officials signal weeks more fighting in Iran.",
        topic_key="iran_war_timeline",
        timestamp=1700000090,
    )

    resolution = resolve_breaking_story_cluster(broader_policy, [_cluster_row(local_strike)], now_ts=1700000090)

    assert resolution.decision == "new_story"


def test_story_cluster_detects_official_confirmation():
    earlier = build_breaking_story_candidate(
        text="Local reports say a missile may have struck near Haifa.",
        headline="Local reports say a missile may have struck near Haifa.",
        topic_key="haifa_strike",
        timestamp=1700000000,
    )
    later = build_breaking_story_candidate(
        text="Magen David Adom confirms a missile strike near Haifa left four wounded.",
        headline="Magen David Adom confirms missile strike near Haifa left four wounded.",
        topic_key="haifa_strike",
        timestamp=1700000120,
    )

    resolution = resolve_breaking_story_cluster(later, [_cluster_row(earlier)], now_ts=1700000120)

    assert resolution.decision == "material_update"
    assert resolution.reason in {"official_confirmation", "casualty_change"}


def test_derive_context_evidence_detects_casualty_change():
    earlier = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    later = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 12 injured, according to Magen David Adom.",
        headline="Iranian rocket impact in Bnei Brak leaves 12 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000060,
    )

    evidence, _score, reason = derive_context_evidence(later, earlier, age_label="1m ago")

    assert reason == "casualty_change"
    assert evidence is not None
    assert "9" in evidence.anchor_detail
    assert "12" in evidence.delta_detail


def test_derive_context_evidence_rejects_no_delta_continuation():
    earlier = build_breaking_story_candidate(
        text="Officials said rockets landed near Haifa overnight.",
        headline="Officials said rockets landed near Haifa overnight.",
        topic_key="haifa_rockets",
        timestamp=1700000000,
    )
    later = build_breaking_story_candidate(
        text="Officials say rockets landed near Haifa overnight.",
        headline="Officials say rockets landed near Haifa overnight.",
        topic_key="haifa_rockets",
        timestamp=1700000030,
    )

    evidence, _score, reason = derive_context_evidence(later, earlier, age_label="moments ago")

    assert evidence is None
    assert reason == "no_delta"


def test_breaking_story_cluster_db_round_trip(isolated_db):
    candidate = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    cluster_key = compute_breaking_story_cluster_key(candidate)

    db.save_breaking_story_cluster(
        cluster_id="cluster-db",
        cluster_key=cluster_key,
        topic_key=candidate.topic_key,
        taxonomy_key=candidate.taxonomy_key,
        root_message_id=777,
        root_sent_ref='{"message_id":777}',
        current_headline=candidate.headline,
        current_facts_json=serialize_breaking_story_facts(candidate.facts),
        opened_ts=1700000000,
        updated_ts=1700000000,
        last_delivery_ts=1700000000,
        update_count=0,
    )
    db.save_breaking_story_event(
        cluster_id="cluster-db",
        source_channel_id="-1001",
        source_message_id=10,
        delivery_message_id=777,
        normalized_text=candidate.facts.normalized_text,
        display_text=candidate.headline,
        facts_json=serialize_breaking_story_facts(candidate.facts),
        delta_kind="new_story",
        created_ts=1700000000,
    )

    clusters = db.load_active_breaking_story_clusters(since_ts=1699999000)
    events = db.load_breaking_story_events(cluster_id="cluster-db")
    counts = db.load_breaking_story_decision_counts(since_ts=1699999000)

    assert clusters[0]["cluster_id"] == "cluster-db"
    assert events[0]["delta_kind"] == "new_story"
    assert counts["new_story"] == 1


@pytest.mark.asyncio
async def test_apply_breaking_story_cluster_policy_suppresses_echo(isolated_db, monkeypatch):
    main.breaking_story_ref_cache.clear()
    monkeypatch.setattr(main, "auth_ready", False)
    monkeypatch.setattr(main.time, "time", lambda: 1700000030)
    async def fake_root_ref(_cluster):
        return {"message_id": 444}
    monkeypatch.setattr(main, "_resolve_breaking_story_root_ref", fake_root_ref)

    earlier = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    db.save_breaking_story_cluster(
        cluster_id="cluster-suppress",
        cluster_key=compute_breaking_story_cluster_key(earlier),
        topic_key=earlier.topic_key,
        taxonomy_key=earlier.taxonomy_key,
        root_message_id=444,
        root_sent_ref='{"message_id":444}',
        current_headline=earlier.headline,
        current_facts_json=serialize_breaking_story_facts(earlier.facts),
        opened_ts=1700000000,
        updated_ts=1700000000,
        last_delivery_ts=1700000000,
        update_count=0,
    )

    result = await main._apply_breaking_story_cluster_policy(
        messages=[SimpleNamespace(id=11, chat_id=-1001, media=None)],
        source="Channel",
        candidate_text="Nine wounded after an Iranian rocket hit Bnei Brak in the Tel Aviv area.",
        headline="Nine wounded after Iranian rocket hit in Bnei Brak.",
        topic_key="bnei_brak_impact",
    )

    assert result["final_action"] == "breaking_suppressed"
    assert int(result["delivery_message_id"]) == 444


@pytest.mark.asyncio
async def test_apply_breaking_story_cluster_policy_threads_material_update(isolated_db, monkeypatch):
    main.breaking_story_ref_cache.clear()
    monkeypatch.setattr(main, "auth_ready", False)
    monkeypatch.setattr(main.time, "time", lambda: 1700000060)
    async def fake_root_ref(_cluster):
        return {"message_id": 444}
    monkeypatch.setattr(main, "_resolve_breaking_story_root_ref", fake_root_ref)

    sent = {}

    async def fake_send_text_with_ref(text, reply_to=None):
        sent["text"] = text
        sent["reply_to"] = reply_to
        return {"message_id": 555}

    monkeypatch.setattr(main, "_send_text_with_ref", fake_send_text_with_ref)

    earlier = build_breaking_story_candidate(
        text="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        headline="Iranian rocket impact in Bnei Brak leaves 9 injured.",
        topic_key="bnei_brak_impact",
        timestamp=1700000000,
    )
    db.save_breaking_story_cluster(
        cluster_id="cluster-thread",
        cluster_key=compute_breaking_story_cluster_key(earlier),
        topic_key=earlier.topic_key,
        taxonomy_key=earlier.taxonomy_key,
        root_message_id=444,
        root_sent_ref='{"message_id":444}',
        current_headline=earlier.headline,
        current_facts_json=serialize_breaking_story_facts(earlier.facts),
        opened_ts=1700000000,
        updated_ts=1700000000,
        last_delivery_ts=1700000000,
        update_count=0,
    )

    result = await main._apply_breaking_story_cluster_policy(
        messages=[SimpleNamespace(id=12, chat_id=-1001, media=None)],
        source="Channel",
        candidate_text="Iranian rocket impact in Bnei Brak leaves 12 injured, according to Magen David Adom.",
        headline="Iranian rocket impact in Bnei Brak leaves 12 injured.",
        topic_key="bnei_brak_impact",
    )

    assert result["final_action"] == "breaking_delivery_threaded"
    assert sent["reply_to"] == 444
    assert int(result["delivery_message_id"]) == 555


@pytest.mark.asyncio
async def test_apply_breaking_story_cluster_policy_edits_minor_refinement(isolated_db, monkeypatch):
    main.breaking_story_ref_cache.clear()
    monkeypatch.setattr(main, "auth_ready", False)
    monkeypatch.setattr(main.time, "time", lambda: 1700000045)

    edits = {}

    async def fake_resolve_root_ref(_cluster):
        return {"message_id": 444}

    async def fake_edit_sent_text(ref, text):
        edits["ref"] = ref
        edits["text"] = text
        return {"message_id": 444}

    monkeypatch.setattr(main, "_resolve_breaking_story_root_ref", fake_resolve_root_ref)
    monkeypatch.setattr(main, "_edit_sent_text", fake_edit_sent_text)

    earlier = build_breaking_story_candidate(
        text="Hezbollah claims a direct drone hit on an army vehicle in Mays al-Jabal.",
        headline="Hezbollah claims direct drone hit on army vehicle in Mays al-Jabal.",
        topic_key="mays_al_jabal_drone_hit",
        timestamp=1700000000,
    )
    db.save_breaking_story_cluster(
        cluster_id="cluster-edit",
        cluster_key=compute_breaking_story_cluster_key(earlier),
        topic_key=earlier.topic_key,
        taxonomy_key=earlier.taxonomy_key,
        root_message_id=444,
        root_sent_ref='{"message_id":444}',
        current_headline="Flash Update Hezbollah claims a direct drone hit on an army vehicle in Mays al-Jabal.",
        current_facts_json=serialize_breaking_story_facts(earlier.facts),
        opened_ts=1700000000,
        updated_ts=1700000000,
        last_delivery_ts=1700000000,
        update_count=0,
    )

    result = await main._apply_breaking_story_cluster_policy(
        messages=[SimpleNamespace(id=13, chat_id=-1001, media=None)],
        source="Channel",
        candidate_text="Hezbollah claims a direct drone hit on an army vehicle in Mays al-Jabal.",
        headline="Hezbollah says a drone hit an army vehicle in Mays al-Jabal.",
        topic_key="mays_al_jabal_drone_hit",
    )

    assert result["final_action"] == "breaking_root_edited"
    assert edits["ref"]["message_id"] == 444
