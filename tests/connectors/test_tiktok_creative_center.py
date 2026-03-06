from __future__ import annotations

from pathlib import Path

from packages.connectors.tiktok_creative_center import TikTokCreativeCenterConnector
from packages.core.models import CandidateType


def test_parse_tiktok_creative_center_fixture() -> None:
    html = Path("tests/fixtures/html/tiktok_creative_center/sample.html").read_text(
        encoding="utf-8"
    )
    connector = TikTokCreativeCenterConnector()
    items = connector.parse_items(html)
    candidates = connector.extract_candidates(items)

    assert len(items) == 3
    assert any(candidate.type == CandidateType.HASHTAG for candidate in candidates)
    assert any(candidate.name == "#ラブブチャレンジ" for candidate in candidates)


def test_parse_tiktok_creative_center_ignores_table_headers() -> None:
    html = """
    <div class="HashtagList_header">
      <span class="HashtagList_hashtagListColumnRank__D49rM">Rank</span>
      <span class="HashtagList_hashtagListColumnLeft__Xh91A">Hashtags</span>
      <span class="HashtagList_hashtagListColumnPav__MvYe4">Posts</span>
      <span class="HashtagList_hashtagListColumnRight__DaF0r">Trend</span>
      <span class="HashtagList_hashtagListColumnCenter__GfgIM">Creators</span>
      <span class="HashtagList_hashtagListColumnBtn__tzN5C">Actions</span>
    </div>
    <a
      data-testid="cc_commonCom-trend_hashtag_item-0"
      href="/business/creativecenter/hashtag/livestory/pc/en"
    >
      <div class="CardPc_title__Cx6Ph">
        <span class="CardPc_titleText__RYOWo"># <!-- -->livestory</span>
      </div>
      <a
        class="CardPc_hashtagDetailBtn__h_zEJ"
        href="/business/creativecenter/hashtag/livestory?countryCode=&amp;period=7"
      >
        <span class="GoDetailBtn_text__UNL0i">See analytics</span>
      </a>
    </a>
    <a
      data-testid="cc_commonCom-trend_hashtag_item-1"
      href="/business/creativecenter/hashtag/championsleague/pc/en"
    >
      <div class="CardPc_title__Cx6Ph">
        <span class="CardPc_titleText__RYOWo"># <!-- -->championsleague</span>
      </div>
      <a
        class="CardPc_hashtagDetailBtn__h_zEJ"
        href="/business/creativecenter/hashtag/championsleague?countryCode=&amp;period=7"
      >
        <span class="GoDetailBtn_text__UNL0i">See analytics</span>
      </a>
    </a>
    """
    connector = TikTokCreativeCenterConnector()

    items = connector.parse_items(html)

    assert [item["keyword"] for item in items] == ["#livestory", "#championsleague"]


def test_parse_tiktok_creative_center_api_payload() -> None:
    connector = TikTokCreativeCenterConnector(country_codes=["JP"])

    items = connector.parse_api_items(
        {
            "data": {
                "list": [
                    {"hashtag_name": "にほん", "rank": 1, "publish_cnt": 23420},
                    {"hashtag_name": "メガ割", "rank": 2, "publish_cnt": 19800},
                ]
            }
        },
        "JP",
    )

    assert items == [
        {
            "keyword": "#にほん",
            "rank": 1,
            "countryCode": "JP",
            "publishCount": 23420,
            "videoViews": 0,
        },
        {
            "keyword": "#メガ割",
            "rank": 2,
            "countryCode": "JP",
            "publishCount": 19800,
            "videoViews": 0,
        },
    ]


def test_merge_tiktok_regional_items_prefers_japan_and_multi_market_overlap() -> None:
    connector = TikTokCreativeCenterConnector(country_codes=["JP", "KR", "TH"], max_results=10)

    items = connector.merge_regional_items(
        {
            "JP": [
                {"keyword": "#にほん", "rank": 1, "countryCode": "JP"},
                {"keyword": "#メガ割", "rank": 2, "countryCode": "JP"},
            ],
            "KR": [
                {"keyword": "#にほん", "rank": 3, "countryCode": "KR"},
                {"keyword": "#새학기", "rank": 1, "countryCode": "KR"},
            ],
            "TH": [
                {"keyword": "#にほん", "rank": 4, "countryCode": "TH"},
                {"keyword": "#ยังเจ", "rank": 1, "countryCode": "TH"},
            ],
        }
    )

    assert [item["keyword"] for item in items] == ["#にほん", "#メガ割"]
    assert items[0]["countries"] == ["JP", "KR", "TH"]
    assert items[0]["countryRanks"] == {"JP": 1, "KR": 3, "TH": 4}
    assert items[1]["countries"] == ["JP"]


def test_extract_candidates_preserves_regional_metadata() -> None:
    connector = TikTokCreativeCenterConnector(country_codes=["JP", "KR"])

    candidates = connector.extract_candidates(
        [
            {
                "keyword": "#ライブ配信",
                "rank": 1,
                "countries": ["JP", "KR"],
                "countryRanks": {"JP": 1, "KR": 3},
                "regionalScore": 0.82,
            }
        ]
    )

    assert len(candidates) == 1
    assert candidates[0].extra["countries"] == ["JP", "KR"]
    assert candidates[0].extra["countryRanks"] == {"JP": 1, "KR": 3}
    assert candidates[0].extra["regionalScore"] == 0.82
