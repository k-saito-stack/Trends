"""Candidate and feature persistence helpers."""

from __future__ import annotations

import logging
from typing import Any

from packages.core import firestore_client
from packages.core.alias_registry import build_alias_records, save_alias_records
from packages.core.models import Candidate, DailyCandidateFeature, DailySourceFeature, Observation, RankedCandidateV2

logger = logging.getLogger(__name__)


def load_all_candidates() -> dict[str, Candidate]:
    """Load all active candidates from Firestore.

    Returns:
        Dict of candidateId -> Candidate
    """
    docs = firestore_client.get_collection("candidates")
    candidates: dict[str, Candidate] = {}
    for doc in docs:
        cand = Candidate.from_dict(doc)
        candidates[cand.candidate_id] = cand
    return candidates


def save_candidate(candidate: Candidate) -> None:
    """Save or update a candidate in Firestore."""
    firestore_client.set_document(
        "candidates", candidate.candidate_id, candidate.to_dict()
    )


def save_candidates_batch(candidates: dict[str, Candidate]) -> None:
    """Save multiple candidates in a batch write."""
    operations: list[tuple[str, str, dict[str, Any]]] = [
        ("candidates", cand_id, cand.to_dict())
        for cand_id, cand in candidates.items()
    ]
    if operations:
        firestore_client.batch_write(operations)
        logger.info("Saved %d candidates", len(operations))


def load_candidates_by_ids(candidate_ids: list[str]) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    for candidate_id in candidate_ids:
        data = firestore_client.get_document("candidates", candidate_id)
        if data is not None:
            candidate = Candidate.from_dict(data)
            candidates[candidate.candidate_id] = candidate
    return candidates


def upsert_touched_candidates(candidates: dict[str, Candidate]) -> None:
    if not candidates:
        return
    save_candidates_batch(candidates)
    save_alias_records(build_alias_records(candidates.values()))


def save_observations(observations: list[Observation]) -> None:
    operations = [
        ("raw_observations", observation.observation_id, observation.to_dict())
        for observation in observations
    ]
    if operations:
        firestore_client.batch_write(operations)


def save_daily_source_features(features: list[DailySourceFeature]) -> None:
    operations = [
        ("daily_source_features", feature.document_id, feature.to_dict())
        for feature in features
    ]
    if operations:
        firestore_client.batch_write(operations)


def save_daily_candidate_features(features: list[DailyCandidateFeature]) -> None:
    operations = [
        ("daily_candidate_features", feature.document_id, feature.to_dict())
        for feature in features
    ]
    if operations:
        firestore_client.batch_write(operations)


def save_daily_rankings_v2(date: str, items: list[RankedCandidateV2]) -> None:
    operations = [
        (f"daily_rankings_v2/{date}/items", item.candidate_id, item.to_dict())
        for item in items
    ]
    if operations:
        firestore_client.batch_write(operations)
