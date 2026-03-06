"""Data models for Trends platform.

Spec reference: Section 12 (Data Model / Firestore)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# --- Candidate Types (fixed, spec section 9) ---

class CandidateType(StrEnum):
    PERSON = "PERSON"
    GROUP = "GROUP"
    WORK = "WORK"
    MUSIC_TRACK = "MUSIC_TRACK"
    MUSIC_ARTIST = "MUSIC_ARTIST"
    KEYWORD = "KEYWORD"


class CandidateStatus(StrEnum):
    ACTIVE = "ACTIVE"
    MERGED = "MERGED"
    BLOCKED = "BLOCKED"


# --- Display Buckets (fixed, spec section 10.7) ---

class DisplayBucket(StrEnum):
    TRENDS = "TRENDS"
    YOUTUBE = "YOUTUBE"
    X = "X"
    NEWS_RSS = "NEWS_RSS"
    RANKINGS_STREAM = "RANKINGS_STREAM"
    MUSIC = "MUSIC"
    MAGAZINES = "MAGAZINES"
    INSTAGRAM_BOOST = "INSTAGRAM_BOOST"


# --- Source State (EWMA statistics per candidate x source) ---

@dataclass
class SourceState:
    """EWMA state for a candidate-source pair."""
    m: float = 0.0           # EWMA mean
    v: float = 0.0           # EWMA variance
    last_sig: float = 0.0    # most recent sig_beta value
    last_updated: str = ""   # ISO date string (YYYY-MM-DD)
    observation_count: int = 0  # for warmup tracking
    sig_history: list[float] = field(default_factory=list)  # [sig_t, sig_{t-1}, sig_{t-2}]


# --- Evidence ---

@dataclass
class Evidence:
    """A single evidence item for a candidate card."""
    source_id: str
    title: str
    url: str
    published_at: str = ""
    metric: str = ""       # e.g. "rank:3", "viewCount:120000"
    snippet: str = ""


# --- Breakdown ---

@dataclass
class BucketScore:
    """Score contribution from a single display bucket."""
    bucket: str
    score: float
    details: list[dict[str, Any]] = field(default_factory=list)


# --- Candidate (master record) ---

@dataclass
class Candidate:
    """Candidate master record stored in /candidates/{candidateId}."""
    candidate_id: str
    type: CandidateType
    canonical_name: str
    display_name: str
    aliases: list[str] = field(default_factory=list)
    created_at: str = ""
    last_seen_at: str = ""
    status: CandidateStatus = CandidateStatus.ACTIVE
    source_state: dict[str, SourceState] = field(default_factory=dict)
    trend_history_7d: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to Firestore-compatible dict."""
        return {
            "candidateId": self.candidate_id,
            "type": self.type.value,
            "canonicalName": self.canonical_name,
            "displayName": self.display_name,
            "aliases": self.aliases,
            "createdAt": self.created_at,
            "lastSeenAt": self.last_seen_at,
            "status": self.status.value,
            "sourceState": {
                k: {
                    "m": v.m,
                    "v": v.v,
                    "lastSig": v.last_sig,
                    "lastUpdated": v.last_updated,
                    "observationCount": v.observation_count,
                    "sigHistory": v.sig_history,
                }
                for k, v in self.source_state.items()
            },
            "trendHistory7d": self.trend_history_7d,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Candidate:
        """Create from Firestore document dict."""
        source_state = {}
        for k, v in data.get("sourceState", {}).items():
            source_state[k] = SourceState(
                m=v.get("m", 0.0),
                v=v.get("v", 0.0),
                last_sig=v.get("lastSig", 0.0),
                last_updated=v.get("lastUpdated", ""),
                observation_count=v.get("observationCount", 0),
                sig_history=v.get("sigHistory", []),
            )
        return cls(
            candidate_id=data.get("candidateId", ""),
            type=CandidateType(data.get("type", "KEYWORD")),
            canonical_name=data.get("canonicalName", ""),
            display_name=data.get("displayName", ""),
            aliases=data.get("aliases", []),
            created_at=data.get("createdAt", ""),
            last_seen_at=data.get("lastSeenAt", ""),
            status=CandidateStatus(data.get("status", "ACTIVE")),
            source_state=source_state,
            trend_history_7d=data.get("trendHistory7d", []),
        )


# --- Daily Ranking Item (card) ---

@dataclass
class DailyRankingItem:
    """A single card in /daily_rankings/{date}/items/{candidateId}."""
    rank: int
    candidate_id: str
    candidate_type: str
    display_name: str
    trend_score: float
    breakdown_buckets: list[BucketScore] = field(default_factory=list)
    breakdown_details: list[dict[str, Any]] = field(default_factory=list)
    sparkline_7d: list[float | None] = field(default_factory=list)
    evidence_top3: list[Evidence] = field(default_factory=list)
    summary: str = ""
    power: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to Firestore-compatible dict."""
        return {
            "rank": self.rank,
            "candidateId": self.candidate_id,
            "candidateType": self.candidate_type,
            "displayName": self.display_name,
            "trendScore": self.trend_score,
            "breakdownBuckets": [
                {"bucket": b.bucket, "score": b.score, "details": b.details}
                for b in self.breakdown_buckets
            ],
            "breakdownDetails": self.breakdown_details,
            "sparkline7d": self.sparkline_7d,
            "evidenceTop3": [
                {
                    "sourceId": e.source_id,
                    "title": e.title,
                    "url": e.url,
                    "publishedAt": e.published_at,
                    "metric": e.metric,
                    "snippet": e.snippet,
                }
                for e in self.evidence_top3
            ],
            "summary": self.summary,
            "power": self.power,
        }


# --- Daily Ranking Metadata ---

@dataclass
class DailyRankingMeta:
    """Metadata for /daily_rankings/{date}."""
    date: str
    generated_at: str
    run_id: str
    top_k: int = 20
    degrade_state: dict[str, bool] = field(default_factory=dict)
    algorithm_version: str = "v1"
    music_weights: dict[str, float] = field(default_factory=lambda: {"JP": 1.0, "GLOBAL": 0.25})
    status: str = "PUBLISHED"

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "generatedAt": self.generated_at,
            "runId": self.run_id,
            "topK": self.top_k,
            "degradeState": self.degrade_state,
            "algorithmVersion": self.algorithm_version,
            "musicWeights": self.music_weights,
            "status": self.status,
        }


# --- Source weighting snapshots ---

@dataclass
class SourceTopItem:
    """A candidate entry in source_daily topM."""
    candidate_id: str
    momentum: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidateId": self.candidate_id,
            "momentum": self.momentum,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceTopItem:
        return cls(
            candidate_id=data.get("candidateId", ""),
            momentum=float(data.get("momentum", 0.0)),
        )


@dataclass
class SourceDailySnapshot:
    """Daily source-level momentum snapshot for source weighting."""
    date: str
    source_id: str
    ok: bool
    item_count: int
    top_m: list[SourceTopItem] = field(default_factory=list)
    generated_at: str = ""

    @property
    def document_id(self) -> str:
        return f"{self.date}_{self.source_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "sourceId": self.source_id,
            "ok": self.ok,
            "itemCount": self.item_count,
            "topM": [item.to_dict() for item in self.top_m],
            "generatedAt": self.generated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceDailySnapshot:
        return cls(
            date=data.get("date", ""),
            source_id=data.get("sourceId", ""),
            ok=bool(data.get("ok", False)),
            item_count=int(data.get("itemCount", 0)),
            top_m=[
                SourceTopItem.from_dict(item)
                for item in data.get("topM", [])
            ],
            generated_at=data.get("generatedAt", ""),
        )


@dataclass
class SourceWeightSnapshot:
    """Computed source weights stored for next-day use."""
    date: str
    generated_at: str
    window_days: int
    horizon_days: int
    half_life_days: float
    n_ref: int
    weights: dict[str, float] = field(default_factory=dict)
    factors: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "generatedAt": self.generated_at,
            "windowDays": self.window_days,
            "horizonDays": self.horizon_days,
            "halfLifeDays": self.half_life_days,
            "nRef": self.n_ref,
            "weights": self.weights,
            "factors": self.factors,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceWeightSnapshot:
        return cls(
            date=data.get("date", ""),
            generated_at=data.get("generatedAt", ""),
            window_days=int(data.get("windowDays", 30)),
            horizon_days=int(data.get("horizonDays", 3)),
            half_life_days=float(data.get("halfLifeDays", 7.0)),
            n_ref=int(data.get("nRef", 50)),
            weights={
                str(source_id): float(weight)
                for source_id, weight in data.get("weights", {}).items()
            },
            factors={
                str(source_id): dict(values)
                for source_id, values in data.get("factors", {}).items()
            },
        )


# --- Raw Candidate (intermediate, not stored directly) ---

@dataclass
class RawCandidate:
    """Intermediate candidate extracted from a source (before resolve)."""
    name: str
    type: CandidateType
    source_id: str
    rank: int | None = None
    metric_value: float = 0.0
    evidence: Evidence | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# --- Config models ---

@dataclass
class AppConfig:
    """Config from /config/app."""
    top_k: int = 20
    timezone: str = "Asia/Tokyo"
    run_time_jst: str = "07:00"
    retention_months: int = 12
    environment: str = "poc-personal-gcp"
    monthly_budget_jpy: int = 5000
    template_at_ratio: float = 0.6
    x_search_reduce_at_ratio: float = 0.8

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AppConfig:
        degrade = data.get("degrade", {})
        thresholds = degrade.get("thresholds", {})
        top_k = max(int(data.get("topK", 20)), 20)
        return cls(
            top_k=top_k,
            timezone=data.get("timezone", "Asia/Tokyo"),
            run_time_jst=data.get("runTimeJST", "07:00"),
            retention_months=data.get("retentionMonths", 12),
            environment=data.get("environment", "poc-personal-gcp"),
            monthly_budget_jpy=degrade.get("monthlyBudgetJPY", 5000),
            template_at_ratio=thresholds.get("templateAtRatio", 0.6),
            x_search_reduce_at_ratio=thresholds.get("xSearchReduceAtRatio", 0.8),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "topK": self.top_k,
            "timezone": self.timezone,
            "runTimeJST": self.run_time_jst,
            "retentionMonths": self.retention_months,
            "environment": self.environment,
            "degrade": {
                "monthlyBudgetJPY": self.monthly_budget_jpy,
                "thresholds": {
                    "templateAtRatio": self.template_at_ratio,
                    "xSearchReduceAtRatio": self.x_search_reduce_at_ratio,
                },
            },
        }


@dataclass
class AlgorithmConfig:
    """Config from /config/algorithm."""
    half_life_days: float = 7.0
    beta: float = 0.1
    warmup_days: int = 3
    min_sig: float = 2.0
    multi_weight: float = 1.0
    momentum_lambda: float = 0.7
    max_x_clip: float = 50.0
    power_weight: float = 0.15

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AlgorithmConfig:
        return cls(
            half_life_days=data.get("halfLifeDays", 7.0),
            beta=data.get("beta", 0.1),
            warmup_days=data.get("warmupDays", 3),
            min_sig=data.get("minSig", 2.0),
            multi_weight=data.get("multiWeight", 1.0),
            momentum_lambda=data.get("momentumLambda", 0.7),
            max_x_clip=data.get("maxXClip", 50.0),
            power_weight=data.get("powerWeight", 0.15),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "halfLifeDays": self.half_life_days,
            "beta": self.beta,
            "warmupDays": self.warmup_days,
            "minSig": self.min_sig,
            "multiWeight": self.multi_weight,
            "momentumLambda": self.momentum_lambda,
            "maxXClip": self.max_x_clip,
            "powerWeight": self.power_weight,
        }


@dataclass
class MusicConfig:
    """Config from /config/music."""
    weights: dict[str, float] = field(default_factory=lambda: {"JP": 1.0, "GLOBAL": 0.25})
    sources: list[str] = field(default_factory=lambda: ["APPLE_MUSIC_JP", "APPLE_MUSIC_GLOBAL"])

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MusicConfig:
        return cls(
            weights=data.get("weights", {"JP": 1.0, "GLOBAL": 0.25}),
            sources=data.get("sources", ["APPLE_MUSIC_JP", "APPLE_MUSIC_GLOBAL"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "weights": self.weights,
            "sources": self.sources,
        }


@dataclass
class SourceWeightingConfig:
    """Config from /config/source_weighting."""
    enabled: bool = True
    window_days: int = 30
    horizon_days: int = 3
    top_k_for_future: int = 20
    top_m_default: int = 20
    n_ref: int = 50
    i_min: float = 0.2
    s_min: float = 0.5
    epsilon: float = 1e-9
    apply_weights_from_next_day: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceWeightingConfig:
        return cls(
            enabled=bool(data.get("enabled", True)),
            window_days=int(data.get("windowDays", 30)),
            horizon_days=int(data.get("horizonDays", 3)),
            top_k_for_future=int(data.get("topKForFuture", 20)),
            top_m_default=int(data.get("topMDefault", 20)),
            n_ref=int(data.get("nRef", 50)),
            i_min=float(data.get("iMin", 0.2)),
            s_min=float(data.get("sMin", 0.5)),
            epsilon=float(data.get("epsilon", 1e-9)),
            apply_weights_from_next_day=bool(
                data.get("applyWeightsFromNextDay", True)
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "windowDays": self.window_days,
            "horizonDays": self.horizon_days,
            "topKForFuture": self.top_k_for_future,
            "topMDefault": self.top_m_default,
            "nRef": self.n_ref,
            "iMin": self.i_min,
            "sMin": self.s_min,
            "epsilon": self.epsilon,
            "applyWeightsFromNextDay": self.apply_weights_from_next_day,
        }


@dataclass
class NerConfig:
    """Config from /config/ner."""
    enabled: bool = True
    max_entities_per_item: int = 5
    model_name: str = "ja_ginza"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NerConfig:
        return cls(
            enabled=data.get("enabled", True),
            max_entities_per_item=data.get("maxEntitiesPerItem", 5),
            model_name=data.get("modelName", "ja_ginza"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "maxEntitiesPerItem": self.max_entities_per_item,
            "modelName": self.model_name,
        }


# --- Change Log ---

@dataclass
class ChangeLog:
    """Record in /change_logs/{logId}."""
    log_id: str
    collection: str       # e.g. "config", "queries", "candidates"
    document_path: str    # e.g. "config/algorithm"
    changed_by: str       # email
    changed_at: str       # ISO datetime
    before: dict[str, Any] = field(default_factory=dict)
    after: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "logId": self.log_id,
            "collection": self.collection,
            "documentPath": self.document_path,
            "changedBy": self.changed_by,
            "changedAt": self.changed_at,
            "before": self.before,
            "after": self.after,
        }
