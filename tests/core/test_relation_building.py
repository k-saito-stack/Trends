from __future__ import annotations

from packages.core.models import Candidate, CandidateType, RawCandidate
from packages.core.relation_building import apply_candidate_relations, build_candidate_relations


def test_build_candidate_relations_links_track_and_artist() -> None:
    raw_candidates = [
        RawCandidate(
            name="ダーリン",
            type=CandidateType.MUSIC_TRACK,
            source_id="APPLE_MUSIC_JP",
            source_item_id="song-1",
            candidate_id="track_1",
            extra={"artist": "Mrs. GREEN APPLE"},
        ),
        RawCandidate(
            name="Mrs. GREEN APPLE",
            type=CandidateType.MUSIC_ARTIST,
            source_id="APPLE_MUSIC_JP",
            source_item_id="song-1",
            candidate_id="artist_1",
            extra={"track": "ダーリン"},
        ),
    ]

    relations = build_candidate_relations(
        raw_candidates,
        created_at="2026-03-07T07:00:00+09:00",
    )

    relation_types = {
        (item.src_candidate_id, item.relation_type, item.dst_candidate_id)
        for item in relations
    }
    assert ("track_1", "performed_by", "artist_1") in relation_types
    assert ("artist_1", "performs", "track_1") in relation_types


def test_build_candidate_relations_keeps_tiktok_topics_separate_but_related() -> None:
    raw_candidates = [
        RawCandidate(
            name="#ラブブ",
            type=CandidateType.HASHTAG,
            source_id="TIKTOK_CREATIVE_CENTER_VIDEOS",
            source_item_id="video-1",
            candidate_id="cand_hashtag",
        ),
        RawCandidate(
            name="ラブブをバッグにつける",
            type=CandidateType.BEHAVIOR,
            source_id="TIKTOK_CREATIVE_CENTER_VIDEOS",
            source_item_id="video-1",
            candidate_id="cand_behavior",
        ),
        RawCandidate(
            name="ラブブ",
            type=CandidateType.PRODUCT,
            source_id="TIKTOK_CREATIVE_CENTER_VIDEOS",
            source_item_id="video-1",
            candidate_id="cand_product",
        ),
    ]

    relations = build_candidate_relations(raw_candidates)

    relation_types = {
        (item.src_candidate_id, item.relation_type, item.dst_candidate_id)
        for item in relations
    }
    assert ("cand_hashtag", "about", "cand_product") in relation_types
    assert ("cand_behavior", "about", "cand_product") in relation_types
    assert ("cand_behavior", "co_occurs", "cand_hashtag") in relation_types


def test_apply_candidate_relations_updates_related_candidate_ids() -> None:
    candidates = {
        "cand_a": Candidate(
            candidate_id="cand_a",
            type=CandidateType.HASHTAG,
            canonical_name="#ラブブ",
            display_name="#ラブブ",
        ),
        "cand_b": Candidate(
            candidate_id="cand_b",
            type=CandidateType.PRODUCT,
            canonical_name="ラブブ",
            display_name="ラブブ",
        ),
    }
    relations = build_candidate_relations(
        [
            RawCandidate(
                name="#ラブブ",
                type=CandidateType.HASHTAG,
                source_id="TIKTOK_CREATIVE_CENTER_HASHTAGS",
                source_item_id="item",
                candidate_id="cand_a",
            ),
            RawCandidate(
                name="ラブブ",
                type=CandidateType.PRODUCT,
                source_id="TIKTOK_CREATIVE_CENTER_HASHTAGS",
                source_item_id="item",
                candidate_id="cand_b",
            ),
        ]
    )

    apply_candidate_relations(candidates, relations)

    assert candidates["cand_a"].related_candidate_ids == ["cand_b"]
    assert candidates["cand_b"].related_candidate_ids == ["cand_a"]
