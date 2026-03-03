"""Tests for proper noun detection (noise filter)."""

from __future__ import annotations

from packages.core.proper_noun import is_proper_noun


class TestIsProperNoun:
    def test_normal_name_passes(self) -> None:
        assert is_proper_noun("YOASOBI") is True
        assert is_proper_noun("米津玄師") is True
        assert is_proper_noun("新しい学校のリーダーズ") is True

    def test_too_short_rejected(self) -> None:
        assert is_proper_noun("AB") is False
        assert is_proper_noun("x") is False

    def test_whitelist_short_passes(self) -> None:
        assert is_proper_noun("IU") is True
        assert is_proper_noun("AI") is True

    def test_digits_only_rejected(self) -> None:
        assert is_proper_noun("12345") is False

    def test_symbols_only_rejected(self) -> None:
        assert is_proper_noun("!!!") is False
        assert is_proper_noun("---") is False

    def test_blacklist_rejected(self) -> None:
        assert is_proper_noun("公式") is False
        assert is_proper_noun("ランキング") is False
        assert is_proper_noun("news") is False
        assert is_proper_noun("official") is False

    def test_hiragana_short_rejected(self) -> None:
        # 3-char hiragana-only -> noise
        assert is_proper_noun("あいう") is False

    def test_hiragana_long_passes(self) -> None:
        # 4+ char hiragana -> might be real name
        assert is_proper_noun("あいうえ") is True

    def test_custom_blacklist(self) -> None:
        assert is_proper_noun("custom_word", blacklist={"custom_word"}) is False

    def test_custom_whitelist(self) -> None:
        assert is_proper_noun("XY", whitelist={"XY"}) is True
