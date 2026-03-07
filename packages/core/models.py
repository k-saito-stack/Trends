"""Core data models for the Trends v2 architecture.

The repo still carries a few v1 compatibility shapes because tests and
connectors already depend on them, but new work should target the
observation-first models added here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class CandidateKind(StrEnum):
    ENTITY = "ENTITY"
    TOPIC = "TOPIC"


class CandidateType(StrEnum):
    PERSON = "PERSON"
    GROUP = "GROUP"
    MUSIC_ARTIST = "MUSIC_ARTIST"
    MUSIC_TRACK = "MUSIC_TRACK"
    WORK = "WORK"
    SHOW = "SHOW"
    REALITY_SHOW = "REALITY_SHOW"
    PHRASE = "PHRASE"
    HASHTAG = "HASHTAG"
    BEHAVIOR = "BEHAVIOR"
    STYLE = "STYLE"
    PRODUCT = "PRODUCT"
    BRAND = "BRAND"
    KEYWORD = "KEYWORD"

    @property
    def default_kind(self) -> CandidateKind:
        if self in {
            CandidateType.PHRASE,
            CandidateType.HASHTAG,
            CandidateType.BEHAVIOR,
            CandidateType.STYLE,
            CandidateType.PRODUCT,
            CandidateType.KEYWORD,
        }:
            return CandidateKind.TOPIC
        return CandidateKind.ENTITY


class CandidateStatus(StrEnum):
    ACTIVE = "ACTIVE"
    MERGED = "MERGED"
    BLOCKED = "BLOCKED"


class DomainClass(StrEnum):
    ENTERTAINMENT = "ENTERTAINMENT"
    FASHION_BEAUTY = "FASHION_BEAUTY"
    CONSUMER_CULTURE = "CONSUMER_CULTURE"
    GENERAL_NEWS = "GENERAL_NEWS"
    BUSINESS_PROFESSIONAL = "BUSINESS_PROFESSIONAL"
    OTHER = "OTHER"


class SourceRole(StrEnum):
    DISCOVERY = "DISCOVERY"
    CONFIRMATION = "CONFIRMATION"
    EDITORIAL = "EDITORIAL"
    COMMERCE = "COMMERCE"
    REFERENCE = "REFERENCE"
    EVIDENCE_ONLY = "EVIDENCE_ONLY"


class SourceFamily(StrEnum):
    SEARCH = "SEARCH"
    SOCIAL_DISCOVERY = "SOCIAL_DISCOVERY"
    MUSIC_CHART = "MUSIC_CHART"
    SHOW_CHART = "SHOW_CHART"
    FASHION_STYLE = "FASHION_STYLE"
    COMMERCE = "COMMERCE"
    EDITORIAL = "EDITORIAL"
    REFERENCE = "REFERENCE"
    VIDEO_CONFIRM = "VIDEO_CONFIRM"
    AD_COMMERCE_AUX = "AD_COMMERCE_AUX"
    UNKNOWN = "UNKNOWN"


class SourceStatus(StrEnum):
    CORE = "CORE"
    EXPERIMENTAL = "EXPERIMENTAL"
    DISABLED = "DISABLED"


class AvailabilityTier(StrEnum):
    CORE = "core"
    OPTIONAL = "optional"
    EXPERIMENTAL = "experimental"


class AccessMode(StrEnum):
    API = "API"
    RSS = "RSS"
    HTML = "HTML"
    LLM_SEARCH = "LLM_SEARCH"
    MANUAL_LOGIN = "MANUAL_LOGIN"


class ExtractionConfidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    @property
    def weight(self) -> float:
        return {
            ExtractionConfidence.HIGH: 1.0,
            ExtractionConfidence.MEDIUM: 0.7,
            ExtractionConfidence.LOW: 0.4,
        }[self]


class RankingLane(StrEnum):
    PEOPLE_MUSIC = "people_music"
    SHOWS_FORMATS = "shows_formats"
    WORDS_BEHAVIORS = "words_behaviors"
    STYLE_PRODUCTS = "style_products"
    SHADOW = "shadow"


DEFAULT_MUSIC_WEIGHTS = {"JP": 1.0, "KR": 0.85, "GLOBAL": 0.1}
DEFAULT_MUSIC_SOURCES = ["APPLE_MUSIC_JP", "APPLE_MUSIC_KR"]


class AliasProvenance(StrEnum):
    MANUAL = "manual"
    SOURCE_ID_LINKED = "source_id_linked"
    RULE_GENERATED = "rule_generated"
    LLM_SUGGESTED = "llm_suggested"


class DisplayBucket(StrEnum):
    TRENDS = "TRENDS"
    YOUTUBE = "YOUTUBE"
    X = "X"
    NEWS_RSS = "NEWS_RSS"
    RANKINGS_STREAM = "RANKINGS_STREAM"
    MUSIC = "MUSIC"
    MAGAZINES = "MAGAZINES"
    INSTAGRAM_BOOST = "INSTAGRAM_BOOST"


@dataclass
class SourceState:
    """EWMA state for a candidate-source pair."""

    m: float = 0.0
    v: float = 0.0
    last_sig: float = 0.0
    last_updated: str = ""
    observation_count: int = 0
    sig_history: list[float] = field(default_factory=list)


@dataclass
class Evidence:
    """A single evidence item for ranking cards and summaries."""

    source_id: str
    title: str
    url: str
    published_at: str = ""
    metric: str = ""
    snippet: str = ""


@dataclass
class BucketScore:
    """Legacy breakdown bucket used by the web UI."""

    bucket: str
    score: float
    details: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Candidate:
    """Candidate master record stored in /candidates/{candidateId}."""

    candidate_id: str
    type: CandidateType
    canonical_name: str
    display_name: str
    aliases: list[str] = field(default_factory=list)
    kind: CandidateKind | None = None
    match_key: str = ""
    created_at: str = ""
    last_seen_at: str = ""
    status: CandidateStatus = CandidateStatus.ACTIVE
    domain_class: DomainClass = DomainClass.OTHER
    maturity: float = 0.0
    source_families: list[str] = field(default_factory=list)
    related_entity_ids: list[str] = field(default_factory=list)
    related_candidate_ids: list[str] = field(default_factory=list)
    external_ids: dict[str, str] = field(default_factory=dict)
    manual_lock: bool = False
    source_state: dict[str, SourceState] = field(default_factory=dict)
    trend_history_7d: list[float] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind is None:
            self.kind = self.type.default_kind
        if not self.match_key:
            self.match_key = self.canonical_name

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidateId": self.candidate_id,
            "type": self.type.value,
            "kind": self.kind.value if self.kind else self.type.default_kind.value,
            "canonicalName": self.canonical_name,
            "displayName": self.display_name,
            "matchKey": self.match_key,
            "aliases": self.aliases,
            "createdAt": self.created_at,
            "lastSeenAt": self.last_seen_at,
            "status": self.status.value,
            "domainClass": self.domain_class.value,
            "maturity": self.maturity,
            "sourceFamilies": self.source_families,
            "relatedEntityIds": self.related_entity_ids,
            "relatedCandidateIds": self.related_candidate_ids,
            "externalIds": self.external_ids,
            "manualLock": self.manual_lock,
            "sourceState": {
                key: {
                    "m": state.m,
                    "v": state.v,
                    "lastSig": state.last_sig,
                    "lastUpdated": state.last_updated,
                    "observationCount": state.observation_count,
                    "sigHistory": state.sig_history,
                }
                for key, state in self.source_state.items()
            },
            "trendHistory7d": self.trend_history_7d,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Candidate:
        source_state = {
            key: SourceState(
                m=value.get("m", 0.0),
                v=value.get("v", 0.0),
                last_sig=value.get("lastSig", 0.0),
                last_updated=value.get("lastUpdated", ""),
                observation_count=value.get("observationCount", 0),
                sig_history=list(value.get("sigHistory", [])),
            )
            for key, value in data.get("sourceState", {}).items()
        }

        raw_type = data.get("type", CandidateType.KEYWORD.value)
        candidate_type = CandidateType(raw_type)
        raw_kind = data.get("kind")
        kind = CandidateKind(raw_kind) if raw_kind else candidate_type.default_kind
        raw_domain = data.get("domainClass", DomainClass.OTHER.value)

        return cls(
            candidate_id=data.get("candidateId", ""),
            type=candidate_type,
            kind=kind,
            canonical_name=data.get("canonicalName", ""),
            display_name=data.get("displayName", ""),
            match_key=data.get("matchKey", data.get("canonicalName", "")),
            aliases=list(data.get("aliases", [])),
            created_at=data.get("createdAt", ""),
            last_seen_at=data.get("lastSeenAt", ""),
            status=CandidateStatus(data.get("status", CandidateStatus.ACTIVE.value)),
            domain_class=DomainClass(raw_domain),
            maturity=float(data.get("maturity", 0.0)),
            source_families=list(data.get("sourceFamilies", [])),
            related_entity_ids=list(data.get("relatedEntityIds", [])),
            related_candidate_ids=list(data.get("relatedCandidateIds", [])),
            external_ids={
                str(key): str(value)
                for key, value in dict(data.get("externalIds", {})).items()
                if value
            },
            manual_lock=bool(data.get("manualLock", False)),
            source_state=source_state,
            trend_history_7d=list(data.get("trendHistory7d", [])),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class DailyRankingItem:
    """Legacy daily ranking item used by the published feed."""

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
    coming_score: float | None = None
    mass_heat: float | None = None
    primary_score: float | None = None
    candidate_kind: str | None = None
    lane: str | None = None
    maturity: float | None = None
    source_families: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "rank": self.rank,
            "candidateId": self.candidate_id,
            "candidateType": self.candidate_type,
            "displayName": self.display_name,
            "trendScore": self.trend_score,
            "breakdownBuckets": [
                {"bucket": bucket.bucket, "score": bucket.score, "details": bucket.details}
                for bucket in self.breakdown_buckets
            ],
            "breakdownDetails": self.breakdown_details,
            "sparkline7d": self.sparkline_7d,
            "evidenceTop3": [
                {
                    "sourceId": evidence.source_id,
                    "title": evidence.title,
                    "url": evidence.url,
                    "publishedAt": evidence.published_at,
                    "metric": evidence.metric,
                    "snippet": evidence.snippet,
                }
                for evidence in self.evidence_top3
            ],
            "summary": self.summary,
            "power": self.power,
        }
        if self.coming_score is not None:
            payload["comingScore"] = self.coming_score
        if self.mass_heat is not None:
            payload["massHeat"] = self.mass_heat
        if self.primary_score is not None:
            payload["primaryScore"] = self.primary_score
        if self.candidate_kind is not None:
            payload["candidateKind"] = self.candidate_kind
        if self.lane is not None:
            payload["lane"] = self.lane
        if self.maturity is not None:
            payload["maturity"] = self.maturity
        if self.source_families:
            payload["sourceFamilies"] = self.source_families
        return payload


@dataclass
class DailyRankingMeta:
    """Metadata for /daily_rankings/{date} and v2 shadow feeds."""

    date: str
    generated_at: str
    run_id: str
    top_k: int = 20
    degrade_state: dict[str, bool] = field(default_factory=dict)
    algorithm_version: str = "v2-shadow"
    music_weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_MUSIC_WEIGHTS))
    status: str = "PUBLISHED"
    published_at: str = ""
    latest_published_run_id: str = ""

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
            "publishedAt": self.published_at,
            "latestPublishedRunId": self.latest_published_run_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DailyRankingMeta:
        return cls(
            date=data.get("date", ""),
            generated_at=data.get("generatedAt", ""),
            run_id=data.get("runId", ""),
            top_k=int(data.get("topK", 20)),
            degrade_state=dict(data.get("degradeState", {})),
            algorithm_version=data.get("algorithmVersion", "v2-shadow"),
            music_weights=dict(data.get("musicWeights", DEFAULT_MUSIC_WEIGHTS)),
            status=data.get("status", "PUBLISHED"),
            published_at=data.get("publishedAt", ""),
            latest_published_run_id=data.get("latestPublishedRunId", ""),
        )


@dataclass
class SourceTopItem:
    """A candidate entry in a daily source snapshot."""

    candidate_id: str
    momentum: float

    def to_dict(self) -> dict[str, Any]:
        return {"candidateId": self.candidate_id, "momentum": self.momentum}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceTopItem:
        return cls(
            candidate_id=data.get("candidateId", ""),
            momentum=float(data.get("momentum", 0.0)),
        )


@dataclass
class SourceDailySnapshot:
    """Daily source-level momentum snapshot."""

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
            top_m=[SourceTopItem.from_dict(item) for item in data.get("topM", [])],
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


@dataclass
class RawCandidate:
    """Intermediate candidate extracted from a source item."""

    name: str
    type: CandidateType
    source_id: str
    kind: CandidateKind | None = None
    rank: int | None = None
    metric_value: float = 0.0
    evidence: Evidence | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.HIGH
    domain_class: DomainClass = DomainClass.OTHER
    observation_id: str = ""
    source_item_id: str = ""
    observed_at: str = ""
    candidate_id: str = ""
    related_entity_ids: list[str] = field(default_factory=list)
    match_key: str = ""

    def __post_init__(self) -> None:
        if self.kind is None:
            self.kind = self.type.default_kind


@dataclass
class Observation:
    """Observation-first record generated from one source item."""

    observation_id: str
    date: str
    source_id: str
    source_item_id: str
    candidate_id: str
    candidate_type: CandidateType
    candidate_kind: CandidateKind
    surface: str
    canonical_name: str
    match_key: str
    signal_value: float
    source_role: SourceRole
    family_primary: SourceFamily
    family_secondary: SourceFamily | None = None
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    domain_class: DomainClass = DomainClass.OTHER
    url: str = ""
    title: str = ""
    rank: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "observationId": self.observation_id,
            "date": self.date,
            "sourceId": self.source_id,
            "sourceItemId": self.source_item_id,
            "candidateId": self.candidate_id,
            "candidateType": self.candidate_type.value,
            "candidateKind": self.candidate_kind.value,
            "surface": self.surface,
            "canonicalName": self.canonical_name,
            "matchKey": self.match_key,
            "signalValue": self.signal_value,
            "sourceRole": self.source_role.value,
            "familyPrimary": self.family_primary.value,
            "familySecondary": self.family_secondary.value if self.family_secondary else "",
            "extractionConfidence": self.extraction_confidence.value,
            "domainClass": self.domain_class.value,
            "url": self.url,
            "title": self.title,
            "rank": self.rank,
            "metadata": self.metadata,
        }


@dataclass
class DailySourceFeature:
    """Source-local daily feature for one candidate."""

    date: str
    source_id: str
    candidate_id: str
    candidate_type: CandidateType
    candidate_kind: CandidateKind
    source_role: SourceRole
    family_primary: SourceFamily
    family_secondary: SourceFamily | None = None
    signal_value: float = 0.0
    anomaly_score: float = 0.0
    surprise01: float = 0.0
    momentum: float = 0.0
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    domain_class: DomainClass = DomainClass.OTHER
    posterior_reliability: float = 1.0
    posterior_lead: float = 0.0
    posterior_persistence: float = 0.0
    observation_ids: list[str] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def document_id(self) -> str:
        return f"{self.date}_{self.source_id}_{self.candidate_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "sourceId": self.source_id,
            "candidateId": self.candidate_id,
            "candidateType": self.candidate_type.value,
            "candidateKind": self.candidate_kind.value,
            "sourceRole": self.source_role.value,
            "familyPrimary": self.family_primary.value,
            "familySecondary": self.family_secondary.value if self.family_secondary else "",
            "signalValue": self.signal_value,
            "anomalyScore": self.anomaly_score,
            "surprise01": self.surprise01,
            "momentum": self.momentum,
            "extractionConfidence": self.extraction_confidence.value,
            "domainClass": self.domain_class.value,
            "posteriorReliability": self.posterior_reliability,
            "posteriorLead": self.posterior_lead,
            "posteriorPersistence": self.posterior_persistence,
            "observationIds": self.observation_ids,
            "evidence": [
                {
                    "sourceId": item.source_id,
                    "title": item.title,
                    "url": item.url,
                    "publishedAt": item.published_at,
                    "metric": item.metric,
                    "snippet": item.snippet,
                }
                for item in self.evidence
            ],
            "metadata": self.metadata,
        }


@dataclass
class DailyCandidateFeature:
    """Candidate-level daily feature after family aggregation."""

    date: str
    candidate_id: str
    display_name: str
    candidate_type: CandidateType
    candidate_kind: CandidateKind
    lane: RankingLane
    domain_class: DomainClass
    source_families: list[str] = field(default_factory=list)
    discovery_rise: float = 0.0
    cross_family_confirm: float = 0.0
    lead_lag_bonus: float = 0.0
    novelty: float = 0.0
    domain_fit: float = 0.0
    extraction_confidence: float = 0.0
    maturity_penalty: float = 0.0
    redundancy_penalty: float = 0.0
    broad_confirmation: float = 0.0
    sustained_presence: float = 0.0
    mainstream_reach: float = 0.0
    breakout_prob_1d: float = 0.0
    breakout_prob_3d: float = 0.0
    breakout_prob_7d: float = 0.0
    mass_prob: float = 0.0
    coming_score: float = 0.0
    mass_heat: float = 0.0
    primary_score: float = 0.0
    ranking_gate_passed: bool = False
    related_entity_ids: list[str] = field(default_factory=list)
    related_candidate_ids: list[str] = field(default_factory=list)
    source_contrib: dict[str, float] = field(default_factory=dict)
    evidence: list[Evidence] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def document_id(self) -> str:
        return f"{self.date}_{self.candidate_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "candidateId": self.candidate_id,
            "displayName": self.display_name,
            "candidateType": self.candidate_type.value,
            "candidateKind": self.candidate_kind.value,
            "lane": self.lane.value,
            "domainClass": self.domain_class.value,
            "sourceFamilies": self.source_families,
            "discoveryRise": self.discovery_rise,
            "crossFamilyConfirm": self.cross_family_confirm,
            "leadLagBonus": self.lead_lag_bonus,
            "novelty": self.novelty,
            "domainFit": self.domain_fit,
            "extractionConfidence": self.extraction_confidence,
            "maturityPenalty": self.maturity_penalty,
            "redundancyPenalty": self.redundancy_penalty,
            "broadConfirmation": self.broad_confirmation,
            "sustainedPresence": self.sustained_presence,
            "mainstreamReach": self.mainstream_reach,
            "breakoutProb1d": self.breakout_prob_1d,
            "breakoutProb3d": self.breakout_prob_3d,
            "breakoutProb7d": self.breakout_prob_7d,
            "massProb": self.mass_prob,
            "comingScore": self.coming_score,
            "massHeat": self.mass_heat,
            "primaryScore": self.primary_score,
            "rankingGatePassed": self.ranking_gate_passed,
            "relatedEntityIds": self.related_entity_ids,
            "relatedCandidateIds": self.related_candidate_ids,
            "sourceContrib": self.source_contrib,
            "metadata": self.metadata,
        }


@dataclass
class RankedCandidateV2:
    """Published ranking item for /daily_rankings_v2/{date}/items/{itemId}."""

    rank: int
    candidate_id: str
    display_name: str
    candidate_type: CandidateType
    candidate_kind: CandidateKind
    lane: RankingLane
    domain_class: DomainClass
    coming_score: float
    mass_heat: float
    primary_score: float
    maturity: float
    source_families: list[str] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "candidateId": self.candidate_id,
            "displayName": self.display_name,
            "candidateType": self.candidate_type.value,
            "candidateKind": self.candidate_kind.value,
            "lane": self.lane.value,
            "domainClass": self.domain_class.value,
            "comingScore": self.coming_score,
            "massHeat": self.mass_heat,
            "primaryScore": self.primary_score,
            "maturity": self.maturity,
            "sourceFamilies": self.source_families,
            "evidenceTop5": [
                {
                    "sourceId": item.source_id,
                    "title": item.title,
                    "url": item.url,
                    "publishedAt": item.published_at,
                    "metric": item.metric,
                    "snippet": item.snippet,
                }
                for item in self.evidence
            ],
            "summary": self.summary,
            "metadata": self.metadata,
        }


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
    shadow_days: int = 14

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AppConfig:
        degrade = data.get("degrade", {})
        thresholds = degrade.get("thresholds", {})
        top_k = _coerce_top_k(data.get("topK", cls.top_k))
        return cls(
            top_k=top_k,
            timezone=data.get("timezone", "Asia/Tokyo"),
            run_time_jst=data.get("runTimeJST", "07:00"),
            retention_months=data.get("retentionMonths", 12),
            environment=data.get("environment", "poc-personal-gcp"),
            monthly_budget_jpy=degrade.get("monthlyBudgetJPY", 5000),
            template_at_ratio=thresholds.get("templateAtRatio", 0.6),
            x_search_reduce_at_ratio=thresholds.get("xSearchReduceAtRatio", 0.8),
            shadow_days=int(data.get("shadowDays", 14)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "topK": self.top_k,
            "timezone": self.timezone,
            "runTimeJST": self.run_time_jst,
            "retentionMonths": self.retention_months,
            "environment": self.environment,
            "shadowDays": self.shadow_days,
            "degrade": {
                "monthlyBudgetJPY": self.monthly_budget_jpy,
                "thresholds": {
                    "templateAtRatio": self.template_at_ratio,
                    "xSearchReduceAtRatio": self.x_search_reduce_at_ratio,
                },
            },
        }


def _coerce_top_k(value: Any, default: int = 20) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


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
    power_weight: float = 0.0
    ranking_gate_discovery_threshold: float = 0.55
    mass_heat_weight: float = 0.35
    source_weight_floor: float = 0.25
    family_params: dict[str, dict[str, float]] = field(
        default_factory=lambda: {
            SourceFamily.SEARCH.value: {
                "halfLifeDays": 3.0,
                "warmupDays": 1,
                "momentumLambda": 0.75,
            },
            SourceFamily.SOCIAL_DISCOVERY.value: {
                "halfLifeDays": 3.0,
                "warmupDays": 1,
                "momentumLambda": 0.75,
            },
            SourceFamily.FASHION_STYLE.value: {
                "halfLifeDays": 4.0,
                "warmupDays": 1,
                "momentumLambda": 0.75,
            },
            SourceFamily.COMMERCE.value: {
                "halfLifeDays": 6.0,
                "warmupDays": 2,
                "momentumLambda": 0.70,
            },
            SourceFamily.MUSIC_CHART.value: {
                "halfLifeDays": 7.0,
                "warmupDays": 2,
                "momentumLambda": 0.70,
            },
            SourceFamily.SHOW_CHART.value: {
                "halfLifeDays": 10.0,
                "warmupDays": 2,
                "momentumLambda": 0.80,
            },
            SourceFamily.EDITORIAL.value: {
                "halfLifeDays": 5.0,
                "warmupDays": 1,
                "momentumLambda": 0.65,
            },
        }
    )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AlgorithmConfig:
        return cls(
            half_life_days=float(data.get("halfLifeDays", 7.0)),
            beta=float(data.get("beta", 0.1)),
            warmup_days=int(data.get("warmupDays", 3)),
            min_sig=float(data.get("minSig", 2.0)),
            multi_weight=float(data.get("multiWeight", 1.0)),
            momentum_lambda=float(data.get("momentumLambda", 0.7)),
            max_x_clip=float(data.get("maxXClip", 50.0)),
            power_weight=float(data.get("powerWeight", 0.0)),
            ranking_gate_discovery_threshold=float(data.get("rankingGateDiscoveryThreshold", 0.55)),
            mass_heat_weight=float(data.get("massHeatWeight", 0.35)),
            source_weight_floor=float(data.get("sourceWeightFloor", 0.25)),
            family_params=dict(data.get("familyParams", cls().family_params)),
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
            "rankingGateDiscoveryThreshold": self.ranking_gate_discovery_threshold,
            "massHeatWeight": self.mass_heat_weight,
            "sourceWeightFloor": self.source_weight_floor,
            "familyParams": self.family_params,
        }


@dataclass
class MusicConfig:
    """Config from /config/music."""

    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_MUSIC_WEIGHTS))
    sources: list[str] = field(default_factory=lambda: list(DEFAULT_MUSIC_SOURCES))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MusicConfig:
        return cls(
            weights=data.get("weights", DEFAULT_MUSIC_WEIGHTS),
            sources=data.get("sources", DEFAULT_MUSIC_SOURCES),
        )

    def to_dict(self) -> dict[str, Any]:
        return {"weights": self.weights, "sources": self.sources}


@dataclass
class SourceWeightingConfig:
    """Config from /config/source_weighting."""

    enabled: bool = True
    window_days: int = 30
    horizon_days: int = 7
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
            horizon_days=int(data.get("horizonDays", 7)),
            top_k_for_future=int(data.get("topKForFuture", 20)),
            top_m_default=int(data.get("topMDefault", 20)),
            n_ref=int(data.get("nRef", 50)),
            i_min=float(data.get("iMin", 0.2)),
            s_min=float(data.get("sMin", 0.5)),
            epsilon=float(data.get("epsilon", 1e-9)),
            apply_weights_from_next_day=bool(data.get("applyWeightsFromNextDay", True)),
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
            enabled=bool(data.get("enabled", True)),
            max_entities_per_item=int(data.get("maxEntitiesPerItem", 5)),
            model_name=data.get("modelName", "ja_ginza"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "maxEntitiesPerItem": self.max_entities_per_item,
            "modelName": self.model_name,
        }


@dataclass
class ChangeLog:
    """Record in /change_logs/{logId}."""

    log_id: str
    collection: str
    document_path: str
    changed_by: str
    changed_at: str
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
