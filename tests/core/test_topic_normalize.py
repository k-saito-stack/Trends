from __future__ import annotations

from packages.core.topic_normalize import normalize_topic_text, should_keep_topic, topic_match_key


def test_normalize_topic_text_lemmatizes_and_strips_noise() -> None:
    assert normalize_topic_text("  ラブブをつけてる!!! ") == "ラブブをつける"


def test_topic_match_key_collapses_spacing_and_symbols() -> None:
    assert topic_match_key("#平成 レトロ") == "平成レトロ"


def test_should_keep_topic_filters_generic_words() -> None:
    assert should_keep_topic("ランキング") is False
    assert should_keep_topic("お守りデザイン") is True


def test_should_keep_topic_filters_generic_english_ui_labels() -> None:
    assert should_keep_topic("Rank") is False
    assert should_keep_topic("See analytics") is False
    assert should_keep_topic("#championsleague") is True
