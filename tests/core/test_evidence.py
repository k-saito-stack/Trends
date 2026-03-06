"""Tests for evidence selection."""

from __future__ import annotations

from packages.core.evidence import build_evidence_pool, select_evidence_top3
from packages.core.models import Evidence


class TestSelectEvidenceTop3:
    def test_source_diversity(self) -> None:
        pool = [
            Evidence(source_id="YOUTUBE", title="YT1", url="u1"),
            Evidence(source_id="YOUTUBE", title="YT2", url="u2"),
            Evidence(source_id="TRENDS", title="TR1", url="u3"),
            Evidence(source_id="MUSIC", title="MU1", url="u4"),
        ]
        result = select_evidence_top3(pool)
        # Should pick 1 per source: YT1, TR1, MU1
        assert len(result) == 3
        sources = {e.source_id for e in result}
        assert sources == {"YOUTUBE", "TRENDS", "MUSIC"}

    def test_max_items_cap(self) -> None:
        pool = [Evidence(source_id=f"S{i}", title=f"T{i}", url=f"u{i}") for i in range(10)]
        result = select_evidence_top3(pool, max_items=3)
        assert len(result) == 3

    def test_empty_pool(self) -> None:
        assert select_evidence_top3([]) == []

    def test_single_source(self) -> None:
        pool = [
            Evidence(source_id="YOUTUBE", title="YT1", url="u1"),
            Evidence(source_id="YOUTUBE", title="YT2", url="u2"),
        ]
        result = select_evidence_top3(pool)
        assert len(result) == 1
        assert result[0].title == "YT1"


class TestBuildEvidencePool:
    def test_sorted_by_signal_value(self) -> None:
        raw = [
            {"source_id": "A", "title": "Low", "url": "u1", "signal_value": 1.0},
            {"source_id": "B", "title": "High", "url": "u2", "signal_value": 10.0},
            {"source_id": "C", "title": "Mid", "url": "u3", "signal_value": 5.0},
        ]
        pool = build_evidence_pool(raw)
        assert pool[0].title == "High"
        assert pool[1].title == "Mid"
        assert pool[2].title == "Low"

    def test_missing_fields_default(self) -> None:
        raw = [{"source_id": "A", "title": "T", "url": "u"}]
        pool = build_evidence_pool(raw)
        assert pool[0].published_at == ""
        assert pool[0].metric == ""
        assert pool[0].snippet == ""
