from __future__ import annotations

from packages.core.models import (
    CandidateKind,
    CandidateType,
    DailySourceFeature,
    DomainClass,
    ExtractionConfidence,
    HindsightLabel,
    SourceFamily,
    SourceRole,
)
from packages.core.source_learning import compute_source_posteriors, resolve_source_posterior


def _source_feature(
    *,
    candidate_id: str,
    surprise01: float,
) -> DailySourceFeature:
    return DailySourceFeature(
        date="2026-03-01",
        source_id="TIKTOK_CREATIVE_CENTER_HASHTAGS",
        candidate_id=candidate_id,
        candidate_type=CandidateType.HASHTAG,
        candidate_kind=CandidateKind.TOPIC,
        source_role=SourceRole.DISCOVERY,
        family_primary=SourceFamily.SOCIAL_DISCOVERY,
        signal_value=1.0,
        anomaly_score=2.5,
        surprise01=surprise01,
        momentum=surprise01,
        extraction_confidence=ExtractionConfidence.HIGH,
        domain_class=DomainClass.CONSUMER_CULTURE,
        metadata={"countries": ["JP", "KR"], "countryRanks": {"JP": 1, "KR": 3}},
    )


def test_compute_source_posteriors_builds_bucketed_stats() -> None:
    features = [
        _source_feature(candidate_id="cand_pos", surprise01=0.9),
        _source_feature(candidate_id="cand_neg", surprise01=0.4),
    ]
    labels_by_date = {
        "2026-03-01": {
            "cand_pos": HindsightLabel(
                date="2026-03-01",
                candidate_id="cand_pos",
                breakout_7d=True,
                mass_now=False,
                mass_7d=True,
                lead_days=2,
                available_breakout_horizons=[7],
                available_mass_horizons=[7],
            ),
            "cand_neg": HindsightLabel(
                date="2026-03-01",
                candidate_id="cand_neg",
                breakout_7d=False,
                mass_now=False,
                mass_7d=False,
                available_breakout_horizons=[7],
                available_mass_horizons=[7],
            ),
        }
    }

    posteriors = compute_source_posteriors(
        features,
        labels_by_date,
        updated_at="2026-03-08T07:00:00+09:00",
    )

    assert len(posteriors) == 1
    posterior = posteriors[0]
    assert posterior.source_id == "TIKTOK_CREATIVE_CENTER_HASHTAGS"
    assert posterior.observations == 2
    assert 0.0 < posterior.reliability < 1.0
    assert 0.0 < posterior.public_precision <= 1.0
    assert 0.0 < posterior.topic_precision <= 1.0

    resolved = resolve_source_posterior(
        "TIKTOK_CREATIVE_CENTER_HASHTAGS",
        CandidateType.HASHTAG.value,
        SourceRole.DISCOVERY.value,
        {"countries": ["JP", "KR"]},
        {posterior.source_id: posterior},
    )

    assert resolved["bucketKey"].endswith("|HASHTAG|DISCOVERY|JP")
    assert resolved["multiplier"] > 0.7
