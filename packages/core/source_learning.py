"""Source posterior learning from hindsight labels."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from packages.core.models import (
    CandidateKind,
    DailySourceFeature,
    HindsightLabel,
    SourceFamily,
    SourcePosterior,
    SourceRole,
)

ASIA_COUNTRIES = {"JP", "KR", "TW", "HK", "TH", "VN", "ID", "PH", "MY", "SG"}


@dataclass
class SourcePrior:
    rel_alpha: float
    rel_beta: float
    persist_alpha: float
    persist_beta: float
    region_alpha: float
    region_beta: float
    lead_prior_days: float
    lead_prior_weight: float


@dataclass
class _PosteriorAccumulator:
    observations: int = 0
    positives: int = 0
    negatives: int = 0
    public_total: int = 0
    public_pos: int = 0
    topic_total: int = 0
    topic_pos: int = 0
    persistence_total: int = 0
    persistence_pos: int = 0
    region_total: int = 0
    region_pos: int = 0
    lead_sum_days: float = 0.0
    lead_count: int = 0


def get_source_prior(source_role: SourceRole, family_primary: SourceFamily) -> SourcePrior:
    if source_role == SourceRole.CONFIRMATION:
        return SourcePrior(6.0, 3.0, 5.0, 4.0, 4.0, 2.0, 1.0, 6.0)
    if source_role == SourceRole.EDITORIAL:
        return SourcePrior(5.0, 4.0, 5.0, 4.0, 4.0, 3.0, 2.0, 5.0)
    if source_role == SourceRole.COMMERCE:
        return SourcePrior(5.0, 5.0, 6.0, 4.0, 4.0, 3.0, 2.5, 5.0)
    if family_primary == SourceFamily.SOCIAL_DISCOVERY:
        return SourcePrior(4.0, 4.0, 4.0, 5.0, 4.0, 3.0, 2.0, 6.0)
    return SourcePrior(4.0, 5.0, 4.0, 5.0, 4.0, 3.0, 3.0, 5.0)


def build_source_bucket_key(
    source_id: str,
    candidate_type: str,
    source_role: str,
    region_bucket: str,
) -> str:
    return "|".join([source_id, candidate_type, source_role, region_bucket])


def infer_region_bucket(source_id: str, metadata: dict[str, Any]) -> str:
    countries = [
        str(country).upper()
        for country in metadata.get("countries", [])
        if isinstance(country, str) and country
    ]
    if "JP" in countries:
        return "JP"
    if countries and any(country in ASIA_COUNTRIES for country in countries):
        return "ASIA"

    region = str(metadata.get("countryCode", "") or metadata.get("region", "")).upper()
    if region == "JP" or source_id.endswith("_JP"):
        return "JP"
    if region in ASIA_COUNTRIES or source_id.endswith("_KR"):
        return "ASIA"
    if source_id.endswith("_GLOBAL"):
        return "GLOBAL"
    return "DEFAULT"


def compute_source_posteriors(
    source_features: Iterable[DailySourceFeature],
    labels_by_date: dict[str, dict[str, HindsightLabel]],
    *,
    updated_at: str,
) -> list[SourcePosterior]:
    accumulators: dict[str, _PosteriorAccumulator] = defaultdict(_PosteriorAccumulator)
    bucket_accumulators: dict[str, dict[str, _PosteriorAccumulator]] = defaultdict(
        lambda: defaultdict(_PosteriorAccumulator)
    )
    priors: dict[str, SourcePrior] = {}
    bucket_meta: dict[str, tuple[str, str, str]] = {}

    for feature in source_features:
        label = labels_by_date.get(feature.date, {}).get(feature.candidate_id)
        if label is None:
            continue
        if 7 not in label.available_breakout_horizons:
            continue

        source_id = feature.source_id
        prior = get_source_prior(feature.source_role, feature.family_primary)
        priors[source_id] = prior
        region_bucket = infer_region_bucket(source_id, feature.metadata)
        bucket_key = build_source_bucket_key(
            source_id,
            feature.candidate_type.value,
            feature.source_role.value,
            region_bucket,
        )
        bucket_meta[bucket_key] = (
            feature.candidate_type.value,
            feature.source_role.value,
            region_bucket,
        )

        is_positive = bool(label.breakout_7d or label.mass_now)
        is_public_positive = bool(label.public_confirm_7d or label.jp_confirm_3d)
        persistence_available = 7 in label.available_mass_horizons
        is_persistent = bool(label.mass_7d) if persistence_available else False
        is_non_jp = region_bucket != "JP"
        is_topic = feature.candidate_kind == CandidateKind.TOPIC
        is_topic_positive = bool(
            is_topic
            and not label.trivial_noise_7d
            and (label.jp_confirm_3d or label.public_confirm_7d or label.breakout_7d)
        )

        for accumulator in (accumulators[source_id], bucket_accumulators[source_id][bucket_key]):
            accumulator.observations += 1
            if is_positive:
                accumulator.positives += 1
            else:
                accumulator.negatives += 1
            accumulator.public_total += 1
            if is_public_positive:
                accumulator.public_pos += 1
            if is_topic:
                accumulator.topic_total += 1
                if is_topic_positive:
                    accumulator.topic_pos += 1
            if persistence_available:
                accumulator.persistence_total += 1
                if is_persistent:
                    accumulator.persistence_pos += 1
            if is_non_jp:
                accumulator.region_total += 1
                if is_positive:
                    accumulator.region_pos += 1
            if label.lead_days is not None:
                accumulator.lead_sum_days += float(label.lead_days)
                accumulator.lead_count += 1

    posteriors: list[SourcePosterior] = []
    for source_id, accumulator in accumulators.items():
        prior = priors.get(source_id, SourcePrior(4, 4, 4, 4, 4, 3, 2.0, 5.0))
        buckets: dict[str, dict[str, Any]] = {}
        for bucket_key, bucket_acc in bucket_accumulators[source_id].items():
            candidate_type, source_role, region_bucket = bucket_meta[bucket_key]
            buckets[bucket_key] = _serialize_bucket(
                bucket_acc,
                prior,
                candidate_type=candidate_type,
                source_role=source_role,
                region_bucket=region_bucket,
            )
        summary = _serialize_bucket(
            accumulator,
            prior,
            candidate_type="*",
            source_role="*",
            region_bucket="DEFAULT",
        )
        posteriors.append(
            SourcePosterior(
                source_id=source_id,
                updated_at=updated_at,
                reliability=float(summary["reliability"]),
                lead_score=float(summary["leadScore"]),
                persistence=float(summary["persistence"]),
                region_fit=float(summary["regionFit"]),
                public_precision=float(summary["publicPrecision"]),
                topic_precision=float(summary["topicPrecision"]),
                observations=accumulator.observations,
                positives=accumulator.positives,
                negatives=accumulator.negatives,
                buckets=buckets,
            )
        )
    return posteriors


def resolve_source_posterior(
    source_id: str,
    candidate_type: str,
    source_role: str,
    metadata: dict[str, Any],
    posteriors: dict[str, SourcePosterior],
) -> dict[str, Any]:
    posterior = posteriors.get(source_id)
    region_bucket = infer_region_bucket(source_id, metadata)
    bucket_key = build_source_bucket_key(source_id, candidate_type, source_role, region_bucket)

    if posterior is None:
        return {
            "bucketKey": bucket_key,
            "reliability": 1.0,
            "leadScore": 0.0,
            "persistence": 0.0,
            "regionFit": 1.0,
            "multiplier": 1.0,
        }

    bucket = posterior.buckets.get(bucket_key, {})
    reliability = float(bucket.get("reliability", posterior.reliability))
    lead_score = float(bucket.get("leadScore", posterior.lead_score))
    persistence = float(bucket.get("persistence", posterior.persistence))
    region_fit = float(bucket.get("regionFit", posterior.region_fit))
    multiplier = compute_posterior_multiplier(
        reliability=reliability,
        lead_score=lead_score,
        persistence=persistence,
        region_fit=region_fit,
    )
    return {
        "bucketKey": bucket_key,
        "reliability": reliability,
        "leadScore": lead_score,
        "persistence": persistence,
        "regionFit": region_fit,
        "multiplier": multiplier,
    }


def compute_posterior_multiplier(
    *,
    reliability: float,
    lead_score: float,
    persistence: float,
    region_fit: float,
) -> float:
    base = 0.45 + (0.55 * reliability) + (0.12 * persistence) + (0.1 * lead_score)
    if region_fit < 1.0:
        base *= 0.85 + (0.15 * region_fit)
    return max(0.65, min(1.35, base))


def _serialize_bucket(
    accumulator: _PosteriorAccumulator,
    prior: SourcePrior,
    *,
    candidate_type: str,
    source_role: str,
    region_bucket: str,
) -> dict[str, Any]:
    reliability = (prior.rel_alpha + accumulator.positives) / (
        prior.rel_alpha + prior.rel_beta + accumulator.positives + accumulator.negatives
    )
    persistence = (prior.persist_alpha + accumulator.persistence_pos) / (
        prior.persist_alpha + prior.persist_beta + accumulator.persistence_total
    )
    public_precision = (prior.rel_alpha + accumulator.public_pos) / (
        prior.rel_alpha + prior.rel_beta + accumulator.public_total
    )
    topic_precision = (prior.rel_alpha + accumulator.topic_pos) / (
        prior.rel_alpha + prior.rel_beta + accumulator.topic_total
    )
    region_fit = 1.0
    if region_bucket != "JP":
        region_fit = (prior.region_alpha + accumulator.region_pos) / (
            prior.region_alpha + prior.region_beta + accumulator.region_total
        )
    mean_lead_days = (
        (prior.lead_prior_days * prior.lead_prior_weight) + accumulator.lead_sum_days
    ) / (prior.lead_prior_weight + accumulator.lead_count)
    lead_score = 1.0 / (1.0 + max(0.0, mean_lead_days))
    return {
        "candidateType": candidate_type,
        "sourceRole": source_role,
        "regionBucket": region_bucket,
        "observations": accumulator.observations,
        "positives": accumulator.positives,
        "negatives": accumulator.negatives,
        "reliability": reliability,
        "persistence": persistence,
        "publicPrecision": public_precision,
        "topicPrecision": topic_precision,
        "regionFit": region_fit,
        "meanLeadDays": mean_lead_days,
        "leadScore": lead_score,
        "posteriorMultiplier": compute_posterior_multiplier(
            reliability=reliability,
            lead_score=lead_score,
            persistence=persistence,
            region_fit=region_fit,
        ),
    }
