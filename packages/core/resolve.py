"""Compatibility layer around entity/topic resolution."""

from __future__ import annotations

from packages.core.entity_resolve import (
    build_entity_key_index,
    create_entity_candidate,
    resolve_entity_candidate,
)
from packages.core.models import Candidate, CandidateKind, CandidateStatus, CandidateType
from packages.core.normalize import normalize_for_matching
from packages.core.topic_normalize import topic_match_key
from packages.core.topic_resolve import (
    build_topic_key_index,
    create_topic_candidate,
    resolve_topic_candidate,
)


def resolve_candidate(
    name: str,
    candidate_type: CandidateType,
    existing_candidates: dict[str, Candidate],
    alias_index: dict[str, str],
    key_index: dict[str, str],
) -> str | None:
    if candidate_type.default_kind == CandidateKind.TOPIC:
        topic_index = build_topic_key_index(existing_candidates.values())
        candidate_id = resolve_topic_candidate(name, alias_index, topic_index)
    else:
        entity_index = build_entity_key_index(existing_candidates.values())
        candidate_id = resolve_entity_candidate(name, candidate_type, entity_index, alias_index)

    if candidate_id is None:
        return None
    candidate = existing_candidates.get(candidate_id)
    if candidate is None or candidate.status == CandidateStatus.BLOCKED:
        return None
    return candidate_id


def build_alias_index(candidates: dict[str, Candidate]) -> dict[str, str]:
    alias_index: dict[str, str] = {}
    for candidate in candidates.values():
        if candidate.status == CandidateStatus.BLOCKED:
            continue
        aliases = [candidate.canonical_name, *candidate.aliases]
        for alias in aliases:
            if candidate.kind == CandidateKind.TOPIC:
                alias_index[topic_match_key(alias)] = candidate.candidate_id
            else:
                alias_index[normalize_for_matching(alias)] = candidate.candidate_id
    return alias_index


def build_key_index(candidates: dict[str, Candidate]) -> dict[str, str]:
    index: dict[str, str] = {}
    index.update(build_entity_key_index(candidates.values()))
    index.update(build_topic_key_index(candidates.values()))
    return index


def create_new_candidate(
    name: str,
    candidate_type: CandidateType,
    candidate_id: str,
    aliases: list[str] | None = None,
) -> Candidate:
    if candidate_type.default_kind == CandidateKind.TOPIC:
        candidate = create_topic_candidate(name, candidate_type=candidate_type)
    else:
        candidate = create_entity_candidate(name, candidate_type, aliases=aliases)
    candidate.candidate_id = candidate_id
    if aliases and candidate.kind == CandidateKind.ENTITY:
        candidate.aliases = aliases
    return candidate
