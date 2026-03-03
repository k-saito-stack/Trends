"""Tests for summary generation."""

from __future__ import annotations

from packages.core.models import BucketScore
from packages.core.summary import MODE_LLM, MODE_OFF, MODE_TEMPLATE, generate_summary


class TestGenerateSummary:
    def test_off_mode_returns_empty(self) -> None:
        result = generate_summary("YOASOBI", 10.0, [], [], mode=MODE_OFF)
        assert result == ""

    def test_template_mode_with_buckets(self) -> None:
        breakdown = [
            BucketScore(bucket="YOUTUBE", score=5.0),
            BucketScore(bucket="MUSIC", score=3.0),
        ]
        result = generate_summary(
            "YOASOBI", 10.0, breakdown, [], mode=MODE_TEMPLATE
        )
        assert "YOASOBI" in result
        assert "YOUTUBE" in result
        assert "10.0" in result

    def test_template_mode_no_buckets(self) -> None:
        result = generate_summary("Ado", 5.0, [], [], mode=MODE_TEMPLATE)
        assert "Ado" in result
        assert "トレンド" in result

    def test_llm_mode_fallback_to_template(self) -> None:
        # Without LLM client, should fall back to template
        breakdown = [BucketScore(bucket="TRENDS", score=7.0)]
        result = generate_summary(
            "米津玄師", 7.0, breakdown, [], mode=MODE_LLM
        )
        assert "米津玄師" in result
        # Should still produce valid output (template fallback)
        assert len(result) > 0

    def test_default_mode_is_template(self) -> None:
        breakdown = [BucketScore(bucket="TRENDS", score=5.0)]
        result = generate_summary("Test", 5.0, breakdown, [])
        assert "Test" in result
