from __future__ import annotations

from packages.core.entity_resolve import create_entity_candidate, resolve_entity_candidate
from packages.core.models import CandidateKind, CandidateType
from packages.core.topic_resolve import create_topic_candidate, resolve_topic_candidate


def test_entity_resolution_and_topic_resolution_stay_separate() -> None:
    entity = create_entity_candidate("Ado", CandidateType.MUSIC_ARTIST)
    topic = create_topic_candidate("adoメイク", CandidateType.BEHAVIOR)

    alias_index = {"ado": entity.candidate_id, "adoメイク": topic.candidate_id}
    entity_index = {f"{CandidateType.MUSIC_ARTIST.value}:ado": entity.candidate_id}
    topic_index = {"adoメイク": topic.candidate_id}

    assert resolve_entity_candidate("Ado", CandidateType.MUSIC_ARTIST, entity_index, alias_index) == entity.candidate_id
    assert resolve_topic_candidate("adoメイク", alias_index, topic_index) == topic.candidate_id
    assert entity.kind == CandidateKind.ENTITY
    assert topic.kind == CandidateKind.TOPIC
