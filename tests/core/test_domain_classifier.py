from __future__ import annotations

from packages.core.domain_classifier import classify_domain, is_main_ranking_domain
from packages.core.models import CandidateType, DomainClass


def test_music_sources_map_to_entertainment() -> None:
    result = classify_domain(CandidateType.MUSIC_TRACK, "BILLBOARD_JAPAN", text="BOW AND ARROW")
    assert result == DomainClass.ENTERTAINMENT


def test_fashion_terms_map_to_fashion_beauty() -> None:
    result = classify_domain(CandidateType.PRODUCT, "WEAR_WORDS", text="スポーツサングラス")
    assert result == DomainClass.FASHION_BEAUTY


def test_business_keywords_are_excluded_from_main_ranking() -> None:
    result = classify_domain(
        CandidateType.KEYWORD, "EDITORIAL_MAGAZINE", text="会社四季報 2026年春号"
    )
    assert result == DomainClass.BUSINESS_PROFESSIONAL
    assert is_main_ranking_domain(result) is False
