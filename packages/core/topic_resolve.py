"""Topic-specific resolution helpers."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

from ulid import ULID

from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateStatus,
    CandidateType,
    DomainClass,
)
from packages.core.topic_normalize import normalize_topic_text, topic_match_key

JST = timezone(timedelta(hours=9))


def build_topic_key_index(candidates: Iterable[Candidate]) -> dict[str, str]:
    index: dict[str, str] = {}
    for candidate in candidates:
        if candidate.status == CandidateStatus.BLOCKED:
            continue
        if (candidate.kind or candidate.type.default_kind) != CandidateKind.TOPIC:
            continue
        index[topic_match_key(candidate.canonical_name)] = candidate.candidate_id
        for alias in candidate.aliases:
            index[topic_match_key(alias)] = candidate.candidate_id
    return index


def resolve_topic_candidate(
    surface: str,
    alias_index: dict[str, str],
    topic_index: dict[str, str],
) -> str | None:
    match_key = topic_match_key(surface)
    if match_key in alias_index:
        return alias_index[match_key]
    return topic_index.get(match_key)


def create_topic_candidate(
    surface: str,
    candidate_type: CandidateType = CandidateType.PHRASE,
    domain_class: DomainClass = DomainClass.OTHER,
) -> Candidate:
    now = datetime.now(JST).isoformat()
    canonical = normalize_topic_text(surface)
    return Candidate(
        candidate_id=str(ULID()),
        type=candidate_type,
        kind=CandidateKind.TOPIC,
        canonical_name=canonical,
        display_name=canonical,
        aliases=[],
        match_key=topic_match_key(canonical),
        created_at=now,
        last_seen_at=now,
        status=CandidateStatus.ACTIVE,
        domain_class=domain_class,
    )
