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
    DailyRankingMeta,
    Evidence,
    MusicConfig,
    RawCandidate,
    SourceDailySnapshot,
    SourceState,
    SourceTopItem,
    SourceWeightingConfig,
    SourceWeightSnapshot,
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
                    m=1.5,
                    v=0.3,
                    last_sig=2.1,
                    last_updated="2026-03-03",
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


class TestDailyRankingMeta:
    def test_roundtrip_with_publish_fields(self) -> None:
        meta = DailyRankingMeta(
            date="2026-03-06",
            generated_at="2026-03-06T11:15:00+09:00",
            run_id="01KK1234567890",
            top_k=20,
            degrade_state={"xSearchEnabled": True},
            status="PUBLISHED",
            published_at="2026-03-06T11:16:00+09:00",
            latest_published_run_id="01KK1234567890",
            publish_health={"publicEligible": True},
        )

        restored = DailyRankingMeta.from_dict(meta.to_dict())
        assert restored.date == "2026-03-06"
        assert restored.run_id == "01KK1234567890"
        assert restored.published_at == "2026-03-06T11:16:00+09:00"
        assert restored.latest_published_run_id == "01KK1234567890"
        assert restored.publish_health == {"publicEligible": True}


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

    def test_top_k_accepts_arbitrary_positive_values(self) -> None:
        config = AppConfig.from_dict({"topK": 50})
        assert config.top_k == 50

    def test_top_k_preserves_positive_values_below_20(self) -> None:
        config = AppConfig.from_dict({"topK": 15})
        assert config.top_k == 15

    def test_top_k_falls_back_to_default_when_invalid(self) -> None:
        invalid = AppConfig.from_dict({"topK": 0})
        assert invalid.top_k == 20


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


class TestSourceWeightingConfig:
    def test_roundtrip(self) -> None:
        original = SourceWeightingConfig()
        restored = SourceWeightingConfig.from_dict(original.to_dict())

        assert restored.window_days == original.window_days
        assert restored.horizon_days == original.horizon_days
        assert restored.top_k_for_future == original.top_k_for_future


class TestMusicConfig:
    def test_defaults(self) -> None:
        config = MusicConfig()
        assert config.weights["JP"] == 1.0
        assert config.weights["KR"] == 0.85
        assert "APPLE_MUSIC_JP" in config.sources
        assert "APPLE_MUSIC_KR" in config.sources
        assert "APPLE_MUSIC_GLOBAL" not in config.sources


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


class TestSourceDailySnapshot:
    def test_roundtrip(self) -> None:
        snapshot = SourceDailySnapshot(
            date="2026-03-03",
            source_id="YOUTUBE_TREND_JP",
            ok=True,
            item_count=50,
            top_m=[
                SourceTopItem(candidate_id="cand_001", momentum=3.2),
                SourceTopItem(candidate_id="cand_002", momentum=2.1),
            ],
            generated_at="2026-03-03T07:05:00+09:00",
        )

        restored = SourceDailySnapshot.from_dict(snapshot.to_dict())
        assert restored.document_id == "2026-03-03_YOUTUBE_TREND_JP"
        assert restored.top_m[0].candidate_id == "cand_001"
        assert restored.top_m[1].momentum == 2.1


class TestSourceWeightSnapshot:
    def test_roundtrip(self) -> None:
        snapshot = SourceWeightSnapshot(
            date="2026-03-03",
            generated_at="2026-03-03T07:10:00+09:00",
            window_days=30,
            horizon_days=3,
            half_life_days=7.0,
            n_ref=50,
            weights={"YOUTUBE_TREND_JP": 1.2, "NETFLIX_TV_JP": 0.6},
            factors={
                "YOUTUBE_TREND_JP": {"R": 1.0, "F": 0.97, "G": 1.0, "C": 0.9, "I": 0.7, "S": 1.0}
            },
        )

        restored = SourceWeightSnapshot.from_dict(snapshot.to_dict())
        assert restored.weights["YOUTUBE_TREND_JP"] == 1.2
        assert restored.factors["YOUTUBE_TREND_JP"]["C"] == 0.9


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
