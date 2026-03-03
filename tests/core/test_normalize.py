"""Tests for candidate name normalization."""

from __future__ import annotations

from packages.core.normalize import (
    extract_bracket_aliases,
    normalize_for_matching,
    normalize_name,
)


class TestNormalizeName:
    def test_nfkc_fullwidth_to_halfwidth(self) -> None:
        # Full-width "ＡＢＣ" -> "ABC"
        assert normalize_name("\uff21\uff22\uff23") == "ABC"

    def test_strip_whitespace(self) -> None:
        assert normalize_name("  hello  ") == "hello"

    def test_compress_whitespace(self) -> None:
        assert normalize_name("hello   world") == "hello world"

    def test_remove_trailing_punctuation(self) -> None:
        assert normalize_name("注目!!") == "注目"
        assert normalize_name("話題？？") == "話題"
        assert normalize_name("すごい。") == "すごい"

    def test_combined_normalization(self) -> None:
        assert normalize_name("  ＹＯＡＳＯＢＩ  !!! ") == "YOASOBI"

    def test_empty_string(self) -> None:
        assert normalize_name("") == ""


class TestExtractBracketAliases:
    def test_fullwidth_brackets(self) -> None:
        canonical, aliases = extract_bracket_aliases("timelesz（タイムレス）")
        assert canonical == "timelesz"
        assert aliases == ["タイムレス"]

    def test_halfwidth_brackets(self) -> None:
        canonical, aliases = extract_bracket_aliases("YOASOBI (ヨアソビ)")
        assert canonical == "YOASOBI"
        assert aliases == ["ヨアソビ"]

    def test_no_brackets(self) -> None:
        canonical, aliases = extract_bracket_aliases("plain name")
        assert canonical == "plain name"
        assert aliases == []

    def test_multiple_brackets(self) -> None:
        canonical, aliases = extract_bracket_aliases("ABC（えーびーしー）（エービーシー）")
        assert canonical == "ABC"
        assert len(aliases) == 2


class TestNormalizeForMatching:
    def test_lowercase(self) -> None:
        assert normalize_for_matching("YOASOBI") == "yoasobi"

    def test_remove_whitespace_and_symbols(self) -> None:
        assert normalize_for_matching("King & Prince") == "king&prince"
        assert normalize_for_matching("Ado - 新時代") == "ado新時代"

    def test_same_candidates_match(self) -> None:
        # These should produce the same key
        assert normalize_for_matching("米津 玄師") == normalize_for_matching("米津玄師")
