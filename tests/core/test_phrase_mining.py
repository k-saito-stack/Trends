from __future__ import annotations

from packages.core.behavior_patterns import extract_behavior_phrases
from packages.core.phrase_mining import extract_hashtags, extract_topic_phrases


def test_extract_hashtags() -> None:
    assert extract_hashtags("今日は #平成レトロ と #シール交換 が来てる") == ["#平成レトロ", "#シール交換"]


def test_extract_behavior_patterns() -> None:
    found = extract_behavior_phrases("ラブブをつけるのが流行。シール交換も人気。")
    assert "ラブブをつける" in found
    assert "シール交換" in found


def test_extract_topic_phrases_includes_style_and_behavior() -> None:
    found = extract_topic_phrases("バレエコアコーデとスポーツサングラス、#平成レトロが急上昇")
    assert "#平成レトロ" in found
    assert "バレエコアコーデ" in found
    assert "スポーツサングラス" in found
