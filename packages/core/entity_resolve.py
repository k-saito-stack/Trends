"""Entity-specific resolution helpers."""

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
from packages.core.normalize import normalize_for_matching, normalize_name

JST = timezone(timedelta(hours=9))


def build_entity_key_index(candidates: Iterable[Candidate]) -> dict[str, str]:
    index: dict[str, str] = {}
    for candidate in candidates:
        if candidate.status != CandidateStatus.ACTIVE:
            continue
        if (candidate.kind or candidate.type.default_kind) != CandidateKind.ENTITY:
            continue
        index[f"{candidate.type.value}:{normalize_for_matching(candidate.canonical_name)}"] = (
            candidate.candidate_id
        )
        for alias in candidate.aliases:
            index[f"{candidate.type.value}:{normalize_for_matching(alias)}"] = (
                candidate.candidate_id
            )
    return index


def resolve_entity_candidate(
    name: str,
    candidate_type: CandidateType,
    key_index: dict[str, str],
    alias_index: dict[str, str],
) -> str | None:
    alias_key = normalize_for_matching(name)
    if alias_key in alias_index:
        return alias_index[alias_key]
    return key_index.get(f"{candidate_type.value}:{alias_key}")


def create_entity_candidate(
    name: str,
    candidate_type: CandidateType,
    aliases: list[str] | None = None,
    domain_class: DomainClass = DomainClass.OTHER,
) -> Candidate:
    now = datetime.now(JST).isoformat()
    canonical = normalize_name(name)
    return Candidate(
        candidate_id=str(ULID()),
        type=candidate_type,
        kind=CandidateKind.ENTITY,
        canonical_name=canonical,
        display_name=canonical,
        aliases=aliases or [],
        match_key=normalize_for_matching(canonical),
        created_at=now,
        last_seen_at=now,
        status=CandidateStatus.ACTIVE,
        domain_class=domain_class,
    )
