"""Candidate and feature persistence helpers."""

from __future__ import annotations

import logging
from typing import Any

from packages.core import firestore_client
from packages.core.alias_registry import build_alias_records, save_alias_records
from packages.core.models import (
    Candidate,
    CandidateRelation,
    DailyCandidateFeature,
    DailyRankingItem,
    DailyRankingMeta,
    DailySourceFeature,
    HindsightLabel,
    Observation,
    RankedCandidateV2,
    RankingEvaluation,
    SourcePosterior,
)

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
    firestore_client.set_document("candidates", candidate.candidate_id, candidate.to_dict())


def save_candidates_batch(candidates: dict[str, Candidate]) -> None:
    """Save multiple candidates in a batch write."""
    operations: list[tuple[str, str, dict[str, Any]]] = [
        ("candidates", cand_id, cand.to_dict()) for cand_id, cand in candidates.items()
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
        ("daily_source_features", feature.document_id, feature.to_dict()) for feature in features
    ]
    if operations:
        firestore_client.batch_write(operations)


def save_daily_candidate_features(features: list[DailyCandidateFeature]) -> None:
    operations = [
        ("daily_candidate_features", feature.document_id, feature.to_dict()) for feature in features
    ]
    if operations:
        firestore_client.batch_write(operations)


def load_daily_source_features_by_dates(dates: list[str]) -> list[DailySourceFeature]:
    date_set = {value for value in dates if value}
    if not date_set:
        return []
    docs = firestore_client.get_collection("daily_source_features")
    return [
        DailySourceFeature.from_dict(doc)
        for doc in docs
        if str(doc.get("date", "")) in date_set
    ]


def load_daily_candidate_features_by_dates(dates: list[str]) -> list[DailyCandidateFeature]:
    date_set = {value for value in dates if value}
    if not date_set:
        return []
    docs = firestore_client.get_collection("daily_candidate_features")
    return [
        DailyCandidateFeature.from_dict(doc)
        for doc in docs
        if str(doc.get("date", "")) in date_set
    ]


def load_hindsight_labels(date: str) -> dict[str, HindsightLabel]:
    docs = firestore_client.get_collection(f"hindsight_labels/{date}/items")
    labels = [HindsightLabel.from_dict(doc) for doc in docs]
    return {label.candidate_id: label for label in labels if label.candidate_id}


def load_daily_ranking_items(
    date: str,
    *,
    collection_root: str = "daily_rankings",
) -> list[DailyRankingItem]:
    docs = firestore_client.get_collection(f"{collection_root}/{date}/items", order_by="rank")
    return [DailyRankingItem.from_dict(doc) for doc in docs]


def load_daily_ranking_meta(
    date: str,
    *,
    collection_root: str = "daily_rankings",
) -> DailyRankingMeta | None:
    doc = firestore_client.get_document(collection_root, date)
    if doc is None:
        return None
    return DailyRankingMeta.from_dict(doc)


def save_hindsight_labels(labels: list[HindsightLabel]) -> None:
    operations = [
        (f"hindsight_labels/{label.date}/items", label.document_id, label.to_dict())
        for label in labels
    ]
    if operations:
        firestore_client.batch_write(operations)


def load_ranking_evaluations_by_dates(dates: list[str]) -> list[RankingEvaluation]:
    date_set = {value for value in dates if value}
    if not date_set:
        return []
    docs = firestore_client.get_collection("shadow_evaluations")
    return [
        RankingEvaluation.from_dict(doc)
        for doc in docs
        if str(doc.get("date", "")) in date_set
    ]


def save_ranking_evaluations(evaluations: list[RankingEvaluation]) -> None:
    operations = [
        ("shadow_evaluations", evaluation.document_id, evaluation.to_dict())
        for evaluation in evaluations
    ]
    if operations:
        firestore_client.batch_upsert(operations)


def save_shadow_rollout_status(target_date: str, data: dict[str, Any]) -> None:
    firestore_client.set_document("shadow_rollout_status", target_date, data)
    firestore_client.set_document("shadow_rollout_status", "current", data)


def save_unresolved_resolution_items(target_date: str, items: list[dict[str, Any]]) -> None:
    operations = [
        (f"unresolved_resolution_queue/{target_date}/items", str(item["pairId"]), item)
        for item in items
        if item.get("pairId")
    ]
    if operations:
        firestore_client.batch_upsert(operations)


def load_source_posteriors() -> dict[str, SourcePosterior]:
    docs = firestore_client.get_collection("source_posteriors")
    posteriors = [SourcePosterior.from_dict(doc) for doc in docs]
    return {
        posterior.source_id: posterior
        for posterior in posteriors
        if posterior.source_id
    }


def save_source_posteriors(posteriors: list[SourcePosterior]) -> None:
    operations = [
        ("source_posteriors", posterior.document_id, posterior.to_dict())
        for posterior in posteriors
    ]
    if operations:
        firestore_client.batch_write(operations)


def save_candidate_relations(relations: list[CandidateRelation]) -> None:
    operations = [
        ("candidate_relations", relation.document_id, relation.to_dict())
        for relation in relations
    ]
    if operations:
        firestore_client.batch_upsert(operations)


def save_daily_rankings_v2(date: str, items: list[RankedCandidateV2]) -> None:
    operations = [
        (f"daily_rankings_v2/{date}/items", item.candidate_id, item.to_dict()) for item in items
    ]
    if operations:
        firestore_client.batch_write(operations)
