from __future__ import annotations

from packages.core.labels import build_hindsight_labels
from packages.core.models import (
    CandidateKind,
    CandidateType,
    DailyCandidateFeature,
    DomainClass,
    RankingLane,
)


def _feature(
    *,
    date: str,
    candidate_id: str,
    source_families: list[str],
    mass_heat: float = 0.0,
    broad_confirmation: float = 0.0,
) -> DailyCandidateFeature:
    return DailyCandidateFeature(
        date=date,
        candidate_id=candidate_id,
        display_name="ラブブ",
        candidate_type=CandidateType.HASHTAG,
        candidate_kind=CandidateKind.TOPIC,
        lane=RankingLane.WORDS_BEHAVIORS,
        domain_class=DomainClass.CONSUMER_CULTURE,
        source_families=source_families,
        mass_heat=mass_heat,
        broad_confirmation=broad_confirmation,
    )


def test_build_hindsight_labels_marks_breakout_and_lead_days() -> None:
    anchor = _feature(
        date="2026-03-01",
        candidate_id="cand_1",
        source_families=["SOCIAL_DISCOVERY"],
    )
    future = _feature(
        date="2026-03-08",
        candidate_id="cand_1",
        source_families=["SOCIAL_DISCOVERY", "MUSIC_CHART"],
        mass_heat=1.1,
        broad_confirmation=0.7,
    )

    labels = build_hindsight_labels(
        "2026-03-01",
        [anchor],
        {
            "2026-03-01": {"cand_1": anchor},
            "2026-03-08": {"cand_1": future},
        },
        available_breakout_horizons=[1, 3, 7],
        available_mass_horizons=[3, 7],
        created_at="2026-03-08T07:00:00+09:00",
    )

    assert len(labels) == 1
    assert labels[0].breakout_7d is True
    assert labels[0].breakout_1d is False
    assert labels[0].mass_7d is True
    assert labels[0].lead_days == 7
    assert labels[0].new_confirmation_families == ["MUSIC_CHART"]
