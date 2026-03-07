"""Deterministic candidate relation building.

Relations let the pipeline keep distinct candidate nodes while preserving
useful links such as TikTok video themes <-> hashtags or track <-> artist.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from packages.core.models import Candidate, CandidateRelation, CandidateType, RawCandidate
from packages.core.normalize import normalize_for_matching
from packages.core.topic_normalize import topic_match_key


def build_candidate_relations(
    raw_candidates: Iterable[RawCandidate],
    created_at: str = "",
) -> list[CandidateRelation]:
    grouped: dict[str, list[RawCandidate]] = defaultdict(list)
    for raw_candidate in raw_candidates:
        if not raw_candidate.candidate_id:
            continue
        grouped[_relation_group_key(raw_candidate)].append(raw_candidate)

    relation_map: dict[str, CandidateRelation] = {}
    for group_items in grouped.values():
        _add_music_relations(group_items, relation_map, created_at)
        _add_work_relations(group_items, relation_map, created_at)
        _add_topic_context_relations(group_items, relation_map, created_at)

    return sorted(relation_map.values(), key=lambda item: item.document_id)


def apply_candidate_relations(
    candidates_by_id: dict[str, Candidate],
    relations: Iterable[CandidateRelation],
) -> None:
    related_map: dict[str, set[str]] = defaultdict(set)
    for relation in relations:
        if relation.src_candidate_id == relation.dst_candidate_id:
            continue
        related_map[relation.src_candidate_id].add(relation.dst_candidate_id)
        related_map[relation.dst_candidate_id].add(relation.src_candidate_id)

    for candidate_id, related_ids in related_map.items():
        candidate = candidates_by_id.get(candidate_id)
        if candidate is None:
            continue
        candidate.related_candidate_ids = sorted(
            set(candidate.related_candidate_ids) | related_ids
        )


def _add_music_relations(
    group_items: list[RawCandidate],
    relation_map: dict[str, CandidateRelation],
    created_at: str,
) -> None:
    tracks = [item for item in group_items if item.type == CandidateType.MUSIC_TRACK]
    artists = [item for item in group_items if item.type == CandidateType.MUSIC_ARTIST]

    for track in tracks:
        track_artist = normalize_for_matching(str(track.extra.get("artist", "")))
        if not track_artist:
            continue
        for artist in artists:
            if normalize_for_matching(artist.name) != track_artist:
                continue
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=track.candidate_id,
                    relation_type="performed_by",
                    dst_candidate_id=artist.candidate_id,
                    confidence=0.96,
                    source=_relation_source(track, artist),
                    created_at=created_at,
                ),
            )
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=artist.candidate_id,
                    relation_type="performs",
                    dst_candidate_id=track.candidate_id,
                    confidence=0.96,
                    source=_relation_source(track, artist),
                    created_at=created_at,
                ),
            )


def _add_topic_context_relations(
    group_items: list[RawCandidate],
    relation_map: dict[str, CandidateRelation],
    created_at: str,
) -> None:
    topics = [
        item
        for item in group_items
        if item.type.default_kind.value == "TOPIC" and item.candidate_id
    ]
    context_items = [
        item
        for item in group_items
        if item.type
        in {
            CandidateType.PERSON,
            CandidateType.GROUP,
            CandidateType.MUSIC_ARTIST,
            CandidateType.MUSIC_TRACK,
            CandidateType.WORK,
            CandidateType.SHOW,
            CandidateType.REALITY_SHOW,
            CandidateType.PRODUCT,
            CandidateType.BRAND,
        }
        and item.candidate_id
    ]

    for topic in topics:
        for context_item in context_items:
            if topic.candidate_id == context_item.candidate_id:
                continue
            confidence = 0.84 if topic.type == CandidateType.HASHTAG else 0.74
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=topic.candidate_id,
                    relation_type="about",
                    dst_candidate_id=context_item.candidate_id,
                    confidence=confidence,
                    source=_relation_source(topic, context_item),
                    created_at=created_at,
                ),
            )

    if len(topics) < 2:
        return

    topic_ids = sorted({item.candidate_id for item in topics if item.candidate_id})
    for index, left_id in enumerate(topic_ids):
        for right_id in topic_ids[index + 1 :]:
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=left_id,
                    relation_type="co_occurs",
                    dst_candidate_id=right_id,
                    confidence=0.55,
                    source="deterministic:shared_item",
                    created_at=created_at,
                ),
            )
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=right_id,
                    relation_type="co_occurs",
                    dst_candidate_id=left_id,
                    confidence=0.55,
                    source="deterministic:shared_item",
                    created_at=created_at,
                ),
            )


def _add_work_relations(
    group_items: list[RawCandidate],
    relation_map: dict[str, CandidateRelation],
    created_at: str,
) -> None:
    works = [
        item
        for item in group_items
        if item.type in {CandidateType.WORK, CandidateType.SHOW, CandidateType.REALITY_SHOW}
    ]
    people = [
        item
        for item in group_items
        if item.type in {CandidateType.PERSON, CandidateType.GROUP, CandidateType.MUSIC_ARTIST}
    ]
    for work in works:
        work_name = normalize_for_matching(work.name)
        for person in people:
            show_hint = normalize_for_matching(str(person.extra.get("show", "")))
            title_hint = normalize_for_matching(str(person.extra.get("from_title", "")))
            if show_hint and show_hint == work_name:
                relation_type = (
                    "appears_in_reality_show"
                    if work.type == CandidateType.REALITY_SHOW
                    else "features_in"
                )
                confidence = 0.87
            elif title_hint and title_hint == work_name:
                relation_type = "associated_with_work"
                confidence = 0.42
            else:
                continue
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=person.candidate_id,
                    relation_type=relation_type,
                    dst_candidate_id=work.candidate_id,
                    confidence=confidence,
                    source=_relation_source(person, work),
                    created_at=created_at,
                ),
            )
            reverse_relation = (
                "has_cast"
                if relation_type != "associated_with_work"
                else "references"
            )
            _register_relation(
                relation_map,
                CandidateRelation(
                    src_candidate_id=work.candidate_id,
                    relation_type=reverse_relation,
                    dst_candidate_id=person.candidate_id,
                    confidence=confidence,
                    source=_relation_source(person, work),
                    created_at=created_at,
                ),
            )


def _relation_group_key(raw_candidate: RawCandidate) -> str:
    if raw_candidate.source_item_id:
        return f"{raw_candidate.source_id}:{raw_candidate.source_item_id}"
    if raw_candidate.evidence:
        title_key = normalize_for_matching(raw_candidate.evidence.title)
        url = raw_candidate.evidence.url
        return f"{raw_candidate.source_id}:{raw_candidate.rank or 0}:{url}:{title_key}"
    return f"{raw_candidate.source_id}:{raw_candidate.rank or 0}:{_surface_key(raw_candidate)}"


def _surface_key(raw_candidate: RawCandidate) -> str:
    if raw_candidate.type.default_kind.value == "TOPIC":
        return topic_match_key(raw_candidate.name)
    return normalize_for_matching(raw_candidate.name)


def _relation_source(left: RawCandidate, right: RawCandidate) -> str:
    if left.source_id == right.source_id:
        return f"{left.source_id}:shared_item"
    return "deterministic"


def _register_relation(
    relation_map: dict[str, CandidateRelation],
    relation: CandidateRelation,
) -> None:
    existing = relation_map.get(relation.document_id)
    if existing is None or relation.confidence > existing.confidence:
        relation_map[relation.document_id] = relation
