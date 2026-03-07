from __future__ import annotations

from unittest.mock import patch

from packages.connectors.google_trends import GoogleTrendingNowConnector
from packages.core.models import CandidateType, DomainClass, RawCandidate


def test_parse_trending_now_rows_extracts_breakdown_and_rank() -> None:
    connector = GoogleTrendingNowConnector(
        source_id="TRENDS_JP_24H_ENT",
        category="ENTERTAINMENT",
    )
    payload = """
    <tr><td>timelesz / 寺西拓人</td><td>20万+</td><td>3 時間前</td></tr>
    <tr><td>レイヤーカット</td><td>10万+</td><td>6 時間前</td></tr>
    """

    rows = connector._parse_trend_rows(payload)

    assert rows[0]["title"] == "timelesz / 寺西拓人"
    assert rows[0]["queryVariants"] == ["timelesz", "寺西拓人"]
    assert rows[0]["trendBreakdownCount"] == 2
    assert rows[0]["rank"] == 1
    assert rows[1]["rank"] == 2


@patch("packages.connectors.google_trends.extract_topic_candidates")
def test_beauty_trends_filters_to_allowed_candidate_types(mock_extract: object) -> None:
    mock_extract.return_value = [
        RawCandidate(
            name="レイヤーカット",
            type=CandidateType.STYLE,
            source_id="TRENDS_JP_24H_BEAUTY_FASHION",
        ),
        RawCandidate(
            name="雑音ワード",
            type=CandidateType.KEYWORD,
            source_id="TRENDS_JP_24H_BEAUTY_FASHION",
        ),
    ]
    connector = GoogleTrendingNowConnector(
        source_id="TRENDS_JP_24H_BEAUTY_FASHION",
        category="BEAUTY_FASHION",
    )

    candidates = connector.extract_candidates(
        [
            {
                "title": "レイヤーカット / 春メイク",
                "rank": 1,
                "searchVolumeText": "10万+",
                "startedText": "2 時間前",
                "category": "BEAUTY_FASHION",
            }
        ]
    )

    assert [candidate.name for candidate in candidates] == ["レイヤーカット"]
    assert candidates[0].type == CandidateType.STYLE
    assert candidates[0].rank == 1
    assert candidates[0].domain_class == DomainClass.FASHION_BEAUTY
