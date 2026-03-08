"""Candidate-level relation support propagation."""

from __future__ import annotations

from collections import defaultdict

from packages.core.models import CandidateRelation, DailySourceFeature
from packages.core.relation_weights import load_relation_weights

NETFLIX_SOURCES = {"NETFLIX_TV_JP", "NETFLIX_FILMS_JP"}
TVER_SOURCES = {"TVER_RANKING_JP"}
RELATION_TYPES = {
    "features_in",
    "voice_acts_in",
    "theme_song_of",
    "appears_in_reality_show",
    "associated_with_work",
}


def build_relation_support_features(
    feature_map: dict[str, list[DailySourceFeature]],
    relations: list[CandidateRelation],
) -> dict[str, dict[str, float]]:
    weights = load_relation_weights()
    support: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "netflix_relation_support_show": 0.0,
            "netflix_relation_support_music": 0.0,
            "netflix_relation_support_people": 0.0,
            "relation_support_total": 0.0,
            "relation_confirmed_support": 0.0,
            "tver_relation_support": 0.0,
        }
    )

    for relation in relations:
        if relation.relation_type not in RELATION_TYPES:
            continue
        src_features = feature_map.get(relation.src_candidate_id, [])
        if not src_features:
            continue
        base_signal = max((feature.surprise01 for feature in src_features), default=0.0)
        if base_signal <= 0:
            continue
        relation_weight = weights.get(
            relation.relation_type,
            weights.get("associated_with_work", 0.35),
        )
        propagated = min(1.0, base_signal * relation_weight * max(0.25, relation.confidence))

        destination = support[relation.dst_candidate_id]
        destination["relation_support_total"] = min(
            1.0,
            destination["relation_support_total"] + propagated,
        )

        src_source_ids = {feature.source_id for feature in src_features}
        if src_source_ids & NETFLIX_SOURCES:
            if relation.relation_type in {
                "features_in",
                "voice_acts_in",
                "appears_in_reality_show",
            }:
                destination["netflix_relation_support_people"] = min(
                    1.0,
                    destination["netflix_relation_support_people"] + propagated,
                )
            if relation.relation_type == "theme_song_of":
                destination["netflix_relation_support_music"] = min(
                    1.0,
                    destination["netflix_relation_support_music"] + propagated,
                )
            destination["netflix_relation_support_show"] = min(
                1.0,
                destination["netflix_relation_support_show"] + propagated,
            )
        if src_source_ids & TVER_SOURCES:
            destination["tver_relation_support"] = min(
                1.0,
                destination["tver_relation_support"] + propagated * 0.8,
            )

        if any(feature.source_role.value == "CONFIRMATION" for feature in src_features):
            destination["relation_confirmed_support"] = min(
                1.0,
                destination["relation_confirmed_support"] + propagated,
            )

    return {candidate_id: dict(values) for candidate_id, values in support.items()}
