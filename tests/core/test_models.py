"""Tests for data models."""

from packages.core.models import (
    AlgorithmConfig,
    AppConfig,
    BucketScore,
    Candidate,
    CandidateStatus,
    CandidateType,
    ChangeLog,
    DailyRankingItem,
    Evidence,
    MusicConfig,
    RawCandidate,
    SourceState,
)


class TestCandidate:
    def test_to_dict_roundtrip(self) -> None:
        candidate = Candidate(
            candidate_id="cand_001",
            type=CandidateType.PERSON,
            canonical_name="test_person",
            display_name="Test Person",
            aliases=["TP"],
            created_at="2026-03-01",
            last_seen_at="2026-03-03",
            status=CandidateStatus.ACTIVE,
            source_state={
                "YOUTUBE_TREND_JP": SourceState(
                    m=1.5, v=0.3, last_sig=2.1, last_updated="2026-03-03",
                    observation_count=5,
                )
            },
            trend_history_7d=[0.0, 1.2, 2.3, 1.8, 3.0, 2.5, 4.0],
        )
        d = candidate.to_dict()
        restored = Candidate.from_dict(d)

        assert restored.candidate_id == "cand_001"
        assert restored.type == CandidateType.PERSON
        assert restored.canonical_name == "test_person"
        assert restored.display_name == "Test Person"
        assert restored.aliases == ["TP"]
        assert restored.status == CandidateStatus.ACTIVE
        assert "YOUTUBE_TREND_JP" in restored.source_state
        state = restored.source_state["YOUTUBE_TREND_JP"]
        assert state.m == 1.5
        assert state.v == 0.3
        assert state.last_sig == 2.1
        assert state.observation_count == 5
        assert len(restored.trend_history_7d) == 7

    def test_from_dict_defaults(self) -> None:
        candidate = Candidate.from_dict({})
        assert candidate.candidate_id == ""
        assert candidate.type == CandidateType.KEYWORD
        assert candidate.status == CandidateStatus.ACTIVE
        assert candidate.source_state == {}
        assert candidate.trend_history_7d == []


class TestDailyRankingItem:
    def test_to_dict(self) -> None:
        item = DailyRankingItem(
            rank=1,
            candidate_id="cand_001",
            candidate_type="PERSON",
            display_name="Test Person",
            trend_score=15.3,
            breakdown_buckets=[
                BucketScore(bucket="YOUTUBE", score=5.0),
                BucketScore(bucket="TRENDS", score=3.0),
            ],
            evidence_top3=[
                Evidence(
                    source_id="YOUTUBE_TREND_JP",
                    title="Test Video",
                    url="https://youtube.com/watch?v=abc",
                    metric="rank:1",
                ),
            ],
            summary="Testing summary",
            sparkline_7d=[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 15.3],
        )
        d = item.to_dict()
        assert d["rank"] == 1
        assert d["trendScore"] == 15.3
        assert len(d["breakdownBuckets"]) == 2
        assert len(d["evidenceTop3"]) == 1
        assert d["evidenceTop3"][0]["sourceId"] == "YOUTUBE_TREND_JP"


class TestAppConfig:
    def test_from_dict(self) -> None:
        data = {
            "topK": 20,
            "timezone": "Asia/Tokyo",
            "runTimeJST": "07:00",
            "retentionMonths": 12,
            "environment": "poc-personal-gcp",
            "degrade": {
                "monthlyBudgetJPY": 5000,
                "thresholds": {
                    "templateAtRatio": 0.6,
                    "xSearchReduceAtRatio": 0.8,
                },
            },
        }
        config = AppConfig.from_dict(data)
        assert config.top_k == 20
        assert config.monthly_budget_jpy == 5000
        assert config.template_at_ratio == 0.6

    def test_defaults(self) -> None:
        config = AppConfig.from_dict({})
        assert config.top_k == 20
        assert config.monthly_budget_jpy == 5000

    def test_roundtrip(self) -> None:
        original = AppConfig()
        d = original.to_dict()
        restored = AppConfig.from_dict(d)
        assert original.top_k == restored.top_k
        assert original.monthly_budget_jpy == restored.monthly_budget_jpy


class TestAlgorithmConfig:
    def test_from_dict(self) -> None:
        data = {
            "halfLifeDays": 7,
            "beta": 0.1,
            "warmupDays": 3,
            "minSig": 2.0,
            "multiWeight": 1.0,
            "momentumLambda": 0.7,
            "maxXClip": 50,
        }
        config = AlgorithmConfig.from_dict(data)
        assert config.half_life_days == 7.0
        assert config.beta == 0.1
        assert config.warmup_days == 3
        assert config.min_sig == 2.0

    def test_roundtrip(self) -> None:
        original = AlgorithmConfig()
        d = original.to_dict()
        restored = AlgorithmConfig.from_dict(d)
        assert original.half_life_days == restored.half_life_days
        assert original.beta == restored.beta


class TestMusicConfig:
    def test_defaults(self) -> None:
        config = MusicConfig()
        assert config.weights["JP"] == 1.0
        assert config.weights["GLOBAL"] == 0.25
        assert "APPLE_MUSIC_JP" in config.sources


class TestChangeLog:
    def test_to_dict(self) -> None:
        log = ChangeLog(
            log_id="log_001",
            collection="config",
            document_path="config/algorithm",
            changed_by="user@kodansha.co.jp",
            changed_at="2026-03-03T10:00:00+09:00",
            before={"halfLifeDays": 7},
            after={"halfLifeDays": 10},
        )
        d = log.to_dict()
        assert d["logId"] == "log_001"
        assert d["before"]["halfLifeDays"] == 7
        assert d["after"]["halfLifeDays"] == 10


class TestRawCandidate:
    def test_creation(self) -> None:
        rc = RawCandidate(
            name="Test Artist",
            type=CandidateType.MUSIC_ARTIST,
            source_id="APPLE_MUSIC_JP",
            rank=5,
            metric_value=0.43,
        )
        assert rc.name == "Test Artist"
        assert rc.type == CandidateType.MUSIC_ARTIST
        assert rc.rank == 5
