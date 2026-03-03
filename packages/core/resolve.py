"""Candidate ID resolution.

Resolves raw candidate names to canonical candidate IDs.
Priority: alias dict -> key dict -> new creation.

Spec reference: Section 9 (Candidate Model - Resolve)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from packages.core.models import Candidate, CandidateStatus, CandidateType
from packages.core.normalize import normalize_for_matching, normalize_name

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def resolve_candidate(
    name: str,
    candidate_type: CandidateType,
    existing_candidates: dict[str, Candidate],
    alias_index: dict[str, str],
    key_index: dict[str, str],
) -> str | None:
    """Resolve a raw candidate name to an existing or new candidate ID.

    Resolution priority (spec):
    1. alias_index[normalized_name] -> existing candidate ID
    2. key_index[type:normalized_name] -> existing candidate ID
    3. New candidate creation (returns None to signal creation needed)

    Args:
        name: Raw candidate name
        candidate_type: Type of the candidate
        existing_candidates: Map of candidateId -> Candidate
        alias_index: Map of normalized alias -> candidateId
        key_index: Map of "type:normalizedName" -> candidateId

    Returns:
        candidateId if resolved, None if new creation needed
    """
    norm = normalize_for_matching(name)

    # Priority 1: Alias lookup
    if norm in alias_index:
        cand_id = alias_index[norm]
        if cand_id in existing_candidates:
            candidate = existing_candidates[cand_id]
            if candidate.status != CandidateStatus.BLOCKED:
                return cand_id

    # Priority 2: Key lookup (type:normalizedName)
    key = f"{candidate_type.value}:{norm}"
    if key in key_index:
        cand_id = key_index[key]
        if cand_id in existing_candidates:
            candidate = existing_candidates[cand_id]
            if candidate.status != CandidateStatus.BLOCKED:
                return cand_id

    # Priority 3: New creation needed
    return None


def build_alias_index(candidates: dict[str, Candidate]) -> dict[str, str]:
    """Build a lookup index from normalized aliases to candidate IDs."""
    index: dict[str, str] = {}
    for cand_id, candidate in candidates.items():
        if candidate.status == CandidateStatus.BLOCKED:
            continue
        # Canonical name
        norm_canonical = normalize_for_matching(candidate.canonical_name)
        index[norm_canonical] = cand_id
        # All aliases
        for alias in candidate.aliases:
            norm_alias = normalize_for_matching(alias)
            index[norm_alias] = cand_id
    return index


def build_key_index(candidates: dict[str, Candidate]) -> dict[str, str]:
    """Build a lookup index from type:normalizedName to candidate IDs."""
    index: dict[str, str] = {}
    for cand_id, candidate in candidates.items():
        if candidate.status == CandidateStatus.BLOCKED:
            continue
        norm = normalize_for_matching(candidate.canonical_name)
        key = f"{candidate.type.value}:{norm}"
        index[key] = cand_id
    return index


def create_new_candidate(
    name: str,
    candidate_type: CandidateType,
    candidate_id: str,
    aliases: list[str] | None = None,
) -> Candidate:
    """Create a new Candidate record."""
    now = datetime.now(JST).isoformat()
    display_name = normalize_name(name)
    return Candidate(
        candidate_id=candidate_id,
        type=candidate_type,
        canonical_name=normalize_for_matching(name),
        display_name=display_name,
        aliases=aliases or [],
        created_at=now,
        last_seen_at=now,
        status=CandidateStatus.ACTIVE,
    )
