"""Build and apply unresolved merge/link judgments for top shadow candidates."""

from __future__ import annotations

import hashlib
from difflib import SequenceMatcher
from itertools import combinations
from typing import Any

from packages.core.models import (
    Candidate,
    CandidateKind,
    CandidateRelation,
    CandidateType,
    DailyCandidateFeature,
)
from packages.core.normalize import normalize_for_matching, normalize_name
from packages.core.topic_normalize import topic_match_key

TOP_WINDOW_DEFAULT = 200
MAX_JUDGMENTS_NORMAL = 30
MAX_JUDGMENTS_BACKFILL = 100
TOPIC_TYPES = {
    CandidateType.HASHTAG,
    CandidateType.PHRASE,
    CandidateType.BEHAVIOR,
    CandidateType.STYLE,
    CandidateType.PRODUCT,
    CandidateType.KEYWORD,
}
FORBIDDEN_TYPE_PAIRS = {
    frozenset({CandidateType.PERSON, CandidateType.GROUP}),
    frozenset({CandidateType.MUSIC_TRACK, CandidateType.MUSIC_ARTIST}),
    frozenset({CandidateType.SHOW, CandidateType.WORK}),
    frozenset({CandidateType.SHOW, CandidateType.REALITY_SHOW}),
}


def build_unresolved_pairs(
    candidate_features: list[DailyCandidateFeature],
    candidates_by_id: dict[str, Candidate],
    *,
    top_window: int = TOP_WINDOW_DEFAULT,
    max_pairs: int = MAX_JUDGMENTS_NORMAL,
) -> list[dict[str, Any]]:
    ranked = sorted(candidate_features, key=lambda item: (-item.primary_score, item.candidate_id))
    shortlisted = ranked[:top_window]
    pairs: list[dict[str, Any]] = []

    for left_feature, right_feature in combinations(shortlisted, 2):
        left_candidate = candidates_by_id.get(left_feature.candidate_id)
        right_candidate = candidates_by_id.get(right_feature.candidate_id)
        if left_candidate is None or right_candidate is None:
            continue
        if not _pair_guardrails_pass(left_candidate, right_candidate):
            continue

        support = _deterministic_support(
            left_feature,
            right_feature,
            left_candidate,
            right_candidate,
        )
        if support["priority"] < 0.72:
            continue

        pair_id = _build_pair_id(left_candidate.candidate_id, right_candidate.candidate_id)
        pairs.append(
            {
                "pairId": pair_id,
                "left": _candidate_payload(left_feature, left_candidate),
                "right": _candidate_payload(right_feature, right_candidate),
                "priority": round(float(support["priority"]), 4),
                "reasons": support["reasons"],
                "deterministicSupportCount": int(support["merge_support"]),
                "mergeRecommended": bool(support["merge_support"] > 0),
            }
        )

    pairs.sort(
        key=lambda item: (
            -float(item.get("priority", 0.0)),
            str(item["left"].get("candidateId", "")),
            str(item["right"].get("candidateId", "")),
        )
    )
    return pairs[:max_pairs]


def apply_resolution_results(
    pairs: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    *,
    created_at: str,
) -> tuple[list[dict[str, Any]], list[CandidateRelation]]:
    queue_items: list[dict[str, Any]] = []
    relation_map: dict[str, CandidateRelation] = {}

    for pair, decision in zip(pairs, decisions, strict=True):
        normalized = _normalize_decision(pair, decision, created_at)
        queue_items.append(normalized)

        final_action = str(normalized.get("finalAction", "NONE"))
        if final_action == "NONE":
            continue

        src_candidate_id = str(pair["left"].get("candidateId", ""))
        dst_candidate_id = str(pair["right"].get("candidateId", ""))
        relation_type = (
            "possible_duplicate" if final_action == "MERGE_RECOMMENDED" else "llm_link_only"
        )
        confidence = float(normalized.get("confidence", 0.0))
        metadata = {
            "pairId": normalized.get("pairId", ""),
            "decision": normalized.get("decision", ""),
            "reasons": list(normalized.get("heuristicReasons", [])),
            "mergeRecommended": bool(normalized.get("mergeRecommended", False)),
        }

        _register_relation(
            relation_map,
            CandidateRelation(
                src_candidate_id=src_candidate_id,
                relation_type=relation_type,
                dst_candidate_id=dst_candidate_id,
                confidence=confidence,
                source="llm_resolution",
                created_at=created_at,
                metadata=metadata,
            ),
        )
        _register_relation(
            relation_map,
            CandidateRelation(
                src_candidate_id=dst_candidate_id,
                relation_type=relation_type,
                dst_candidate_id=src_candidate_id,
                confidence=confidence,
                source="llm_resolution",
                created_at=created_at,
                metadata=metadata,
            ),
        )

    relations = sorted(relation_map.values(), key=lambda item: item.document_id)
    return queue_items, relations


def max_llm_judgments_for_date(target_date: str, today_date: str) -> int:
    return MAX_JUDGMENTS_NORMAL if target_date >= today_date else MAX_JUDGMENTS_BACKFILL


def _pair_guardrails_pass(left: Candidate, right: Candidate) -> bool:
    if left.candidate_id == right.candidate_id:
        return False
    if left.manual_lock or right.manual_lock:
        return False
    left_kind = left.kind or left.type.default_kind
    right_kind = right.kind or right.type.default_kind
    if left_kind != right_kind:
        return False
    if frozenset({left.type, right.type}) in FORBIDDEN_TYPE_PAIRS:
        return False
    if left_kind == CandidateKind.ENTITY and left.type != right.type:
        return False
    if _external_ids_conflict(left, right):
        return False
    return not (
        right.candidate_id in left.related_candidate_ids
        or left.candidate_id in right.related_candidate_ids
    )


def _external_ids_conflict(left: Candidate, right: Candidate) -> bool:
    shared_providers = set(left.external_ids) & set(right.external_ids)
    return any(
        left.external_ids[provider] != right.external_ids[provider]
        for provider in shared_providers
    )


def _deterministic_support(
    left_feature: DailyCandidateFeature,
    right_feature: DailyCandidateFeature,
    left_candidate: Candidate,
    right_candidate: Candidate,
) -> dict[str, Any]:
    left_name = left_candidate.display_name or left_candidate.canonical_name
    right_name = right_candidate.display_name or right_candidate.canonical_name
    left_key = _surface_key(left_name, left_candidate.kind or left_candidate.type.default_kind)
    right_key = _surface_key(right_name, right_candidate.kind or right_candidate.type.default_kind)

    reasons: list[str] = []
    priority = 0.0
    merge_support = 0

    if left_key and right_key and left_key == right_key:
        reasons.append("exact_match_key")
        priority += 1.1
        merge_support += 1

    alias_overlap = _alias_overlap(left_candidate, right_candidate)
    if alias_overlap:
        reasons.append("alias_overlap")
        priority += 0.95
        merge_support += 1

    left_compact = normalize_for_matching(left_name)
    right_compact = normalize_for_matching(right_name)
    if _contains_surface(left_compact, right_compact):
        reasons.append("surface_contains")
        priority += 0.45

    similarity = SequenceMatcher(a=left_compact, b=right_compact).ratio()
    if similarity >= 0.88:
        reasons.append("sequence_ratio")
        priority += 0.55
        if similarity >= 0.95:
            merge_support += 1

    token_overlap = _token_overlap(left_name, right_name)
    if token_overlap >= 0.67:
        reasons.append("token_overlap")
        priority += 0.35

    shared_families = len(set(left_feature.source_families) & set(right_feature.source_families))
    if shared_families:
        reasons.append("shared_family")
        priority += 0.12

    if left_feature.lane == right_feature.lane:
        reasons.append("shared_lane")
        priority += 0.08

    if left_candidate.domain_class == right_candidate.domain_class:
        reasons.append("shared_domain")
        priority += 0.05

    score_bonus = min(0.2, (left_feature.primary_score + right_feature.primary_score) * 0.02)
    priority += score_bonus

    return {
        "priority": priority,
        "merge_support": merge_support,
        "reasons": reasons,
    }


def _candidate_payload(feature: DailyCandidateFeature, candidate: Candidate) -> dict[str, Any]:
    return {
        "candidateId": candidate.candidate_id,
        "name": candidate.display_name or candidate.canonical_name,
        "candidateType": candidate.type.value,
        "candidateKind": (candidate.kind or candidate.type.default_kind).value,
        "domainClass": candidate.domain_class.value,
        "sourceFamilies": list(feature.source_families),
        "lane": feature.lane.value,
        "primaryScore": feature.primary_score,
    }


def _normalize_decision(
    pair: dict[str, Any],
    decision: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    raw_decision = str(decision.get("decision", "unknown")).strip().lower()
    if raw_decision == "merge" and int(pair.get("deterministicSupportCount", 0)) > 0:
        final_action = "MERGE_RECOMMENDED"
    elif raw_decision == "link":
        final_action = "LINK_ONLY"
    else:
        final_action = "NONE"
    return {
        "pairId": pair.get("pairId", ""),
        "leftCandidateId": str(pair["left"].get("candidateId", "")),
        "rightCandidateId": str(pair["right"].get("candidateId", "")),
        "leftName": str(pair["left"].get("name", "")),
        "rightName": str(pair["right"].get("name", "")),
        "decision": raw_decision,
        "confidence": float(decision.get("confidence", 0.0) or 0.0),
        "provider": str(decision.get("provider", "")),
        "model": str(decision.get("model", "")),
        "reason": str(decision.get("reason", "")),
        "cacheHit": bool(decision.get("cacheHit", False)),
        "heuristicReasons": list(pair.get("reasons", [])),
        "priority": float(pair.get("priority", 0.0)),
        "deterministicSupportCount": int(pair.get("deterministicSupportCount", 0)),
        "mergeRecommended": bool(pair.get("mergeRecommended", False)),
        "finalAction": final_action,
        "createdAt": created_at,
    }


def _surface_key(name: str, kind: CandidateKind) -> str:
    return topic_match_key(name) if kind == CandidateKind.TOPIC else normalize_for_matching(name)


def _alias_overlap(left: Candidate, right: Candidate) -> bool:
    left_aliases = {
        _surface_key(alias, left.kind or left.type.default_kind)
        for alias in [left.canonical_name, *left.aliases]
        if alias
    }
    right_aliases = {
        _surface_key(alias, right.kind or right.type.default_kind)
        for alias in [right.canonical_name, *right.aliases]
        if alias
    }
    left_aliases.discard("")
    right_aliases.discard("")
    return bool(left_aliases & right_aliases)


def _contains_surface(left_key: str, right_key: str) -> bool:
    if not left_key or not right_key:
        return False
    if min(len(left_key), len(right_key)) < 5:
        return False
    return left_key in right_key or right_key in left_key


def _token_overlap(left_name: str, right_name: str) -> float:
    left_tokens = {token for token in normalize_name(left_name).casefold().split() if token}
    right_tokens = {token for token in normalize_name(right_name).casefold().split() if token}
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens & right_tokens
    union = left_tokens | right_tokens
    return len(intersection) / len(union) if union else 0.0


def _build_pair_id(left_candidate_id: str, right_candidate_id: str) -> str:
    ordered = sorted([left_candidate_id, right_candidate_id])
    return hashlib.sha1(f"{ordered[0]}|{ordered[1]}".encode()).hexdigest()


def _register_relation(
    relation_map: dict[str, CandidateRelation],
    relation: CandidateRelation,
) -> None:
    existing = relation_map.get(relation.document_id)
    if existing is None or relation.confidence > existing.confidence:
        relation_map[relation.document_id] = relation
