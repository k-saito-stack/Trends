"""Observation-first daily batch for Trends v2."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ulid import ULID

from batch.cost_tracker import estimate_run_cost, record_run_cost
from batch.degrade import DegradeState, compute_degrade_state
from packages.connectors.base import BaseConnector, ConnectorRunResult
from packages.connectors.registry import build_source_plan_from_catalog
from packages.core import candidate_store
from packages.core.alias_registry import build_alias_records, load_alias_index
from packages.core.config import (
    load_algorithm_config,
    load_all_source_configs,
    load_app_config,
    load_source_weighting_config,
)
from packages.core.diversification import infer_lane
from packages.core.domain_classifier import classify_domain
from packages.core.evaluation import (
    build_ranked_entries_from_features,
    build_ranked_entries_from_items,
    compare_variant_metrics,
    evaluate_ranked_entries,
)
from packages.core.labels import build_hindsight_labels
from packages.core.models import (
    BucketScore,
    Candidate,
    CandidateKind,
    DailyCandidateFeature,
    DailyRankingItem,
    DailyRankingMeta,
    DailySourceFeature,
    DomainClass,
    Evidence,
    ExtractionConfidence,
    HindsightLabel,
    Observation,
    RankingEvaluation,
    RawCandidate,
    SourceWeightSnapshot,
)
from packages.core.publish_health import evaluate_publish_health
from packages.core.ranking import build_ranked_candidates_v2
from packages.core.relation_building import apply_candidate_relations, build_candidate_relations
from packages.core.resolution_llm import resolve_uncertain_pairs
from packages.core.resolve import (
    build_alias_index,
    build_key_index,
    create_new_candidate,
    resolve_candidate,
)
from packages.core.rollout_gate import evaluate_shadow_rollout
from packages.core.run_logger import end_run, start_run, update_run_source
from packages.core.scoring_v2 import (
    compute_candidate_feature,
    compute_source_feature_score,
    group_features_by_candidate,
)
from packages.core.source_catalog import get_source_entry
from packages.core.source_health import build_source_health_records
from packages.core.source_learning import compute_source_posteriors, resolve_source_posterior
from packages.core.source_weighting import (
    build_source_daily_snapshots,
    compute_weight_snapshot,
    filter_weighted_source_ids,
    load_current_source_weights,
    load_source_daily,
)
from packages.core.summary import MODE_LLM, generate_summary
from packages.core.unresolved_resolution import (
    apply_resolution_results,
    build_unresolved_pairs,
    max_llm_judgments_for_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LOCK_TIMEOUT_MINUTES = 30
PUBLISH_COLLECTIONS = ("daily_rankings", "daily_rankings_v2", "daily_rankings_v2_shadow")


@dataclass(frozen=True)
class BatchRuntimeOptions:
    shadow_only: bool = False
    publish: bool = True
    persist_observations: bool = True
    persist_features: bool = True
    persist_candidates: bool = True
    persist_labels: bool = True
    persist_source_posteriors: bool = True
    persist_evaluations: bool = True
    skip_slow_sources: bool = False
    source_include: tuple[str, ...] = field(default_factory=tuple)
    source_exclude: tuple[str, ...] = field(default_factory=tuple)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv_arg(values: list[str] | None) -> tuple[str, ...]:
    parsed: list[str] = []
    for value in values or []:
        for chunk in str(value).split(","):
            normalized = chunk.strip()
            if normalized:
                parsed.append(normalized)
    return tuple(dict.fromkeys(parsed))


def get_target_date(date_arg: str) -> str:
    if date_arg == "today":
        return datetime.now(JST).strftime("%Y-%m-%d")
    return date_arg


def acquire_lock(
    target_date: str,
    run_id: str,
    allow_completed_rerun: bool = False,
) -> bool:
    from packages.core import firestore_client

    lock_id = f"_lock_{target_date}"
    now_iso = datetime.now(JST).isoformat()

    created = firestore_client.create_document(
        "runs",
        lock_id,
        {
            "status": "RUNNING",
            "runId": run_id,
            "targetDate": target_date,
            "startedAt": now_iso,
        },
    )
    if created:
        return True

    lock_doc = firestore_client.get_document("runs", lock_id)
    if lock_doc is None:
        return False

    status = lock_doc.get("status", "")
    if status == "COMPLETED":
        if not allow_completed_rerun:
            logger.info(
                "Date %s already completed (run=%s). Skipping.",
                target_date,
                lock_doc.get("runId", "?"),
            )
            return False
        firestore_client.set_document(
            "runs",
            lock_id,
            {
                "status": "RUNNING",
                "runId": run_id,
                "targetDate": target_date,
                "startedAt": now_iso,
            },
        )
        return True

    if status == "RUNNING":
        started_at = lock_doc.get("startedAt", "")
        if started_at:
            try:
                started = datetime.fromisoformat(started_at)
                elapsed = datetime.now(JST) - started
                if elapsed.total_seconds() > LOCK_TIMEOUT_MINUTES * 60:
                    firestore_client.set_document(
                        "runs",
                        lock_id,
                        {
                            "status": "RUNNING",
                            "runId": run_id,
                            "targetDate": target_date,
                            "startedAt": now_iso,
                        },
                    )
                    return True
            except (ValueError, TypeError):
                pass
        logger.info("Another run is active for %s. Skipping.", target_date)
        return False

    firestore_client.set_document(
        "runs",
        lock_id,
        {
            "status": "RUNNING",
            "runId": run_id,
            "targetDate": target_date,
            "startedAt": now_iso,
        },
    )
    return True


def release_lock(target_date: str, run_id: str, status: str = "COMPLETED") -> None:
    from packages.core import firestore_client

    firestore_client.update_document(
        "runs",
        f"_lock_{target_date}",
        {"status": status, "runId": run_id, "endedAt": datetime.now(JST).isoformat()},
    )


def _load_existing_published_meta(target_date: str) -> DailyRankingMeta | None:
    from packages.core import firestore_client

    day_doc = firestore_client.get_document("daily_rankings", target_date)
    if day_doc:
        day_meta = DailyRankingMeta.from_dict(day_doc)
        if day_meta.published_at and day_meta.latest_published_run_id:
            return day_meta

    run_docs = firestore_client.get_collection(f"daily_rankings/{target_date}/runs")
    published_runs = [
        DailyRankingMeta.from_dict(doc)
        for doc in run_docs
        if doc.get("publishedAt") and doc.get("runId")
    ]
    if not published_runs:
        return None
    return max(published_runs, key=lambda meta: meta.published_at)


def _should_use_light_publish(existing_published_meta: DailyRankingMeta | None) -> bool:
    if _is_truthy(os.environ.get("BATCH_FORCE_FULL_PERSIST")):
        return False

    explicit_light_publish = os.environ.get("BATCH_LIGHT_PUBLISH_ONLY")
    if explicit_light_publish is not None:
        return _is_truthy(explicit_light_publish)

    return existing_published_meta is not None


def _build_item_collection_paths(
    target_date: str,
    run_id: str,
    light_publish: bool,
    shadow_only: bool = False,
) -> tuple[str, ...]:
    if light_publish:
        return (f"daily_rankings/{target_date}/runs/{run_id}/items",)
    if shadow_only:
        return (
            f"daily_rankings_v2_shadow/{target_date}/items",
            f"daily_rankings/{target_date}/runs/{run_id}/items",
        )
    return (
        f"daily_rankings/{target_date}/items",
        f"daily_rankings_v2_shadow/{target_date}/items",
        f"daily_rankings/{target_date}/runs/{run_id}/items",
    )


def _build_reset_collection_paths(
    target_date: str,
    light_publish: bool,
    shadow_only: bool = False,
) -> tuple[str, ...]:
    if light_publish:
        return ()
    if shadow_only:
        return (f"daily_rankings_v2_shadow/{target_date}/items",)
    return (
        f"daily_rankings/{target_date}/items",
        f"daily_rankings_v2/{target_date}/items",
        f"daily_rankings_v2_shadow/{target_date}/items",
    )


def _build_publish_collections(light_publish: bool, shadow_only: bool) -> tuple[str, ...]:
    if light_publish and shadow_only:
        return ()
    if not light_publish and shadow_only:
        return ("daily_rankings_v2_shadow",)
    if light_publish:
        return ("daily_rankings",)
    return PUBLISH_COLLECTIONS


def _build_publish_meta(
    target_date: str,
    generated_at: str,
    run_id: str,
    top_k: int,
    degrade: DegradeState,
    *,
    status: str,
    published_at: str = "",
    latest_published_run_id: str = "",
    publish_health: dict[str, Any] | None = None,
) -> DailyRankingMeta:
    return DailyRankingMeta(
        date=target_date,
        generated_at=generated_at,
        run_id=run_id,
        top_k=top_k,
        degrade_state=degrade.to_dict(),  # type: ignore[arg-type]
        status=status,
        published_at=published_at,
        latest_published_run_id=latest_published_run_id,
        publish_health=dict(publish_health or {}),
    )


def _create_connectors(source_cfgs: list[dict[str, Any]] | None = None) -> list[BaseConnector]:
    from packages.connectors.registry import build_connectors

    source_plan = build_source_plan(source_cfgs or [], BatchRuntimeOptions())
    return build_connectors(source_cfgs or [], source_plan=source_plan)


def build_source_plan(
    source_cfgs: list[dict[str, Any]],
    options: BatchRuntimeOptions,
) -> list[dict[str, Any]]:
    include_set = set(options.source_include)
    exclude_set = set(options.source_exclude)
    plan = build_source_plan_from_catalog(source_cfgs)
    filtered: list[dict[str, Any]] = []

    for entry in plan:
        source_id = str(entry.get("sourceId", ""))
        if include_set and source_id not in include_set:
            continue
        if source_id in exclude_set:
            continue
        if options.skip_slow_sources and _is_slow_source(entry):
            continue
        filtered.append(entry)
    return filtered


def _is_slow_source(plan_entry: dict[str, Any]) -> bool:
    access_mode = str(plan_entry.get("accessMode", ""))
    return access_mode in {"HTML", "MANUAL_LOGIN", "LLM_SEARCH"}


def _validate_runtime_source_cfgs(source_cfgs: list[dict[str, Any]]) -> dict[str, list[str]]:
    from packages.connectors.registry import validate_runtime_source_cfgs

    return validate_runtime_source_cfgs(source_cfgs)


def _apply_runtime_feature_flags(
    degrade: DegradeState, source_cfgs: list[dict[str, Any]]
) -> DegradeState:
    cfg_by_id = {str(cfg["sourceId"]): cfg for cfg in source_cfgs if cfg.get("sourceId")}
    x_search_cfg = cfg_by_id.get("X_SEARCH")
    degrade.x_search_enabled = bool(
        x_search_cfg and x_search_cfg.get("enabled", False) and degrade.x_search_enabled
    )
    return degrade


def _build_runtime_source_cfg_map(
    source_cfgs: list[dict[str, Any]],
    connectors: list[BaseConnector],
) -> dict[str, dict[str, Any]]:
    cfg_map = {str(cfg["sourceId"]): dict(cfg) for cfg in source_cfgs if cfg.get("sourceId")}
    for connector in connectors:
        cfg = dict(cfg_map.get(connector.source_id, {}))
        cfg.setdefault("sourceId", connector.source_id)
        if not cfg.get("fetchLimit"):
            fetch_limit = getattr(connector, "max_results", None) or getattr(
                connector, "max_items_per_feed", None
            )
            if isinstance(fetch_limit, int) and fetch_limit > 0:
                cfg["fetchLimit"] = fetch_limit
        cfg_map[connector.source_id] = cfg
    return cfg_map


def main(date_arg: str = "today", options: BatchRuntimeOptions | None = None) -> None:
    target_date = get_target_date(date_arg)
    run_id = str(ULID())
    errors: list[str] = []
    runtime_options = options or BatchRuntimeOptions()
    allow_completed_rerun = _is_truthy(os.environ.get("BATCH_ALLOW_RERUN_COMPLETED"))

    logger.info("=== Trends Daily Batch v2 ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Target date: %s", target_date)

    lock_acquired = False
    try:
        if not acquire_lock(target_date, run_id, allow_completed_rerun=allow_completed_rerun):
            return
        lock_acquired = True
        _run_pipeline(target_date, run_id, errors, runtime_options)
    except Exception:
        if lock_acquired:
            with contextlib.suppress(Exception):
                release_lock(target_date, run_id, status="FAILED")
        raise


def _run_pipeline(
    target_date: str,
    run_id: str,
    errors: list[str],
    options: BatchRuntimeOptions,
) -> None:
    published_successfully = False
    top_candidates: list[Any] = []

    logger.info("Step 0: Loading configs...")
    app_config = _safe_load(load_app_config)
    algo_config = _safe_load(load_algorithm_config)
    source_weighting_config = _safe_load(load_source_weighting_config)
    source_cfg_list = _safe_load(load_all_source_configs, fallback=[])
    runtime_cfg_validation = _validate_runtime_source_cfgs(source_cfg_list)
    if any(runtime_cfg_validation.values()):
        logger.warning("Runtime source config drift detected: %s", runtime_cfg_validation)
    source_plan = build_source_plan(source_cfg_list, options)

    degrade = DegradeState()
    try:
        from batch.cost_tracker import get_budget_ratio

        budget_ratio = get_budget_ratio(app_config.monthly_budget_jpy)
        degrade = compute_degrade_state(budget_ratio, app_config)
        degrade = _apply_runtime_feature_flags(degrade, source_cfg_list)
    except Exception as exc:
        logger.warning("Failed to compute degrade state: %s", exc)

    existing_published_meta = _load_existing_published_meta(target_date)
    light_publish = _should_use_light_publish(existing_published_meta)
    if light_publish:
        logger.info(
            "Existing published snapshot detected for %s; using light publish mode.",
            target_date,
        )

    persist_observations = options.persist_observations and not light_publish
    persist_features = options.persist_features and not light_publish
    persist_candidates = options.persist_candidates and not light_publish
    persist_labels = options.persist_labels and not light_publish
    persist_source_posteriors = options.persist_source_posteriors and not light_publish
    persist_evaluations = options.persist_evaluations

    if not light_publish:
        with contextlib.suppress(Exception):
            start_run(run_id, target_date, degrade.to_dict())

    from packages.connectors.registry import build_connectors

    connectors = build_connectors(source_cfg_list, source_plan=source_plan)
    if not connectors:
        raise RuntimeError("No connectors selected by the current source plan")
    source_cfg_map = _build_runtime_source_cfg_map(source_cfg_list, connectors)

    weighted_source_ids = filter_weighted_source_ids(
        [connector.source_id for connector in connectors]
    )
    try:
        source_weights = load_current_source_weights(
            target_date=target_date,
            source_cfgs=source_cfg_map,
            source_ids=weighted_source_ids,
            algo_cfg=algo_config,
            weighting_cfg=source_weighting_config,
        )
    except Exception as exc:
        errors.append(f"source_weights_current: {exc}")
        logger.warning("Failed to load current source weights: %s", exc)
        source_weights = {}
    try:
        current_source_posteriors = candidate_store.load_source_posteriors()
    except Exception as exc:
        errors.append(f"source_posteriors_current: {exc}")
        logger.warning("Failed to load current source posteriors: %s", exc)
        current_source_posteriors = {}

    existing_candidates = _load_existing_candidates()
    alias_index = build_alias_index(existing_candidates)
    key_index = build_key_index(existing_candidates)
    touched_candidates: dict[str, Candidate] = {}
    observations: list[Observation] = []
    resolved_raw_candidates: list[RawCandidate] = []
    raw_by_candidate_source: dict[tuple[str, str], list[RawCandidate]] = defaultdict(list)
    source_ok: dict[str, bool] = {}
    source_item_count: dict[str, int] = {}
    source_kept_count: dict[str, int] = {}
    source_errors: dict[str, str] = {}
    source_fallback_used: dict[str, str] = {}
    source_availability_tier: dict[str, str] = {}
    source_response_ms: dict[str, int] = {}
    sources_used: list[str] = []

    logger.info("Step 1-4: Fetch, parse, extract, resolve...")
    for connector in connectors:
        source_id = connector.source_id
        entry = get_source_entry(source_id)
        logger.info("  Source: %s", source_id)

        try:
            run_result: ConnectorRunResult = connector.run()
        except Exception as exc:
            run_result = ConnectorRunResult(
                source_id=source_id, ok=False, item_count=0, error=str(exc)
            )

        source_ok[source_id] = run_result.ok
        source_item_count[source_id] = run_result.item_count
        source_kept_count[source_id] = run_result.kept_item_count
        if run_result.fallback_used:
            source_fallback_used[source_id] = run_result.fallback_used
        if run_result.response_ms is not None:
            source_response_ms[source_id] = run_result.response_ms
        if entry is not None:
            source_availability_tier[source_id] = entry.availability_tier.value
        if run_result.error:
            source_errors[source_id] = run_result.error
            errors.append(f"{source_id}: {run_result.error}")

        if not light_publish:
            with contextlib.suppress(Exception):
                update_run_source(
                    run_id, source_id, len(run_result.candidates), error=run_result.error
                )

        if not run_result.ok or entry is None:
            continue

        sources_used.append(source_id)
        for raw_candidate in run_result.candidates:
            resolved = _resolve_raw_candidate(
                raw_candidate, existing_candidates, alias_index, key_index
            )
            candidate = resolved
            candidate.last_seen_at = target_date
            if (
                raw_candidate.domain_class != candidate.domain_class
                and raw_candidate.domain_class.value != "OTHER"
            ):
                candidate.domain_class = raw_candidate.domain_class
            candidate_id = candidate.candidate_id
            touched_candidates[candidate_id] = candidate
            raw_candidate.candidate_id = candidate_id
            resolved_raw_candidates.append(raw_candidate)

            observation = _build_observation(target_date, raw_candidate, candidate, entry)
            observations.append(observation)
            raw_by_candidate_source[(candidate_id, source_id)].append(raw_candidate)

    logger.info(
        "Resolved %d touched candidates across %d observations",
        len(touched_candidates),
        len(observations),
    )

    candidate_relations = build_candidate_relations(
        resolved_raw_candidates,
        created_at=datetime.now(JST).isoformat(),
    )
    apply_candidate_relations(touched_candidates, candidate_relations)

    logger.info("Step 5-8: Build local features, score, rank...")
    source_features: list[DailySourceFeature] = []
    source_momentum: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (candidate_id, source_id), raw_items in raw_by_candidate_source.items():
        candidate = touched_candidates[candidate_id]
        entry = get_source_entry(source_id)
        if entry is None:
            continue
        total_signal = sum(item.metric_value for item in raw_items)
        source_weight = source_weights.get(source_id, 1.0)
        metadata = _build_source_feature_metadata(raw_items, source_weight)
        posterior_stats = resolve_source_posterior(
            source_id,
            candidate.type.value,
            entry.role.value,
            metadata,
            current_source_posteriors,
        )
        anomaly_score, surprise01 = compute_source_feature_score(
            candidate,
            source_id,
            total_signal,
            algo_config,
            target_date,
            entry.family_primary,
            posterior_multiplier=float(posterior_stats["multiplier"]),
        )
        weighted_surprise = (
            min(entry.max_weight_cap, max(algo_config.source_weight_floor, source_weight))
            * surprise01
        )
        evidence = _dedupe_evidence(item.evidence for item in raw_items)
        feature = DailySourceFeature(
            date=target_date,
            source_id=source_id,
            candidate_id=candidate_id,
            candidate_type=candidate.type,
            candidate_kind=candidate.kind or candidate.type.default_kind,
            source_role=entry.role,
            family_primary=entry.family_primary,
            family_secondary=entry.family_secondary,
            signal_value=total_signal,
            anomaly_score=anomaly_score,
            surprise01=min(1.0, weighted_surprise),
            momentum=min(1.0, weighted_surprise),
            extraction_confidence=_max_confidence(raw_items),
            domain_class=_pick_domain(candidate, raw_items),
            posterior_reliability=float(posterior_stats["reliability"]),
            posterior_lead=float(posterior_stats["leadScore"]),
            posterior_persistence=float(posterior_stats["persistence"]),
            observation_ids=[
                obs.observation_id
                for obs in observations
                if obs.candidate_id == candidate_id and obs.source_id == source_id
            ],
            evidence=evidence[:5],
            metadata=dict(
                metadata,
                posteriorBucket=str(posterior_stats["bucketKey"]),
                posteriorMultiplier=float(posterior_stats["multiplier"]),
                posteriorRegionFit=float(posterior_stats["regionFit"]),
            ),
        )
        source_features.append(feature)
        source_momentum[source_id].append((candidate_id, feature.surprise01))

    candidate_feature_list = []
    feature_map = group_features_by_candidate(source_features)
    for candidate_id, features in feature_map.items():
        candidate = touched_candidates[candidate_id]
        lane = infer_lane(candidate.type)
        domain_class = _pick_feature_domain(candidate, features)
        if domain_class != DomainClass.OTHER:
            candidate.domain_class = domain_class
        candidate_feature = compute_candidate_feature(
            date=target_date,
            candidate=candidate,
            lane=lane,
            domain_class=domain_class,
            source_features=features,
            algo_config=algo_config,
        )
        candidate.trend_history_7d.append(candidate_feature.primary_score)
        candidate.trend_history_7d = candidate.trend_history_7d[-7:]
        candidate.source_families = candidate_feature.source_families
        candidate.maturity = round(
            min(1.5, candidate.maturity * 0.6 + candidate_feature.mass_heat * 0.4), 4
        )
        touched_candidates[candidate_id] = candidate
        candidate_feature_list.append(candidate_feature)

    sorted_features = sorted(candidate_feature_list, key=lambda feature: -feature.primary_score)
    ranked_candidates = build_ranked_candidates_v2(
        sorted_features, touched_candidates, top_k=app_config.top_k
    )
    top_candidates = ranked_candidates
    logger.info("Selected %d ranked candidates", len(ranked_candidates))

    llm_summary_calls = 0
    llm_resolution_calls = 0
    unresolved_pairs: list[dict[str, Any]] = []
    unresolved_queue_items: list[dict[str, Any]] = []
    llm_client = None
    if degrade.summary_mode == MODE_LLM:
        with contextlib.suppress(Exception):
            from packages.core.llm_client import LLMClient

            llm_client = LLMClient()

    resolution_client = None
    with contextlib.suppress(Exception):
        from packages.core.llm_client import LLMClient

        resolution_client = LLMClient()

    resolution_limit = max_llm_judgments_for_date(
        target_date,
        datetime.now(JST).date().isoformat(),
    )
    unresolved_pairs = build_unresolved_pairs(
        candidate_feature_list,
        touched_candidates,
        top_window=max(200, app_config.top_k),
        max_pairs=resolution_limit,
    )
    if unresolved_pairs:
        resolution_results = resolve_uncertain_pairs(
            unresolved_pairs,
            llm_client=resolution_client,
        )
        llm_resolution_calls = sum(
            1
            for result in resolution_results
            if not bool(result.get("cacheHit", False))
            and str(result.get("provider", "")) not in {"", "none"}
        )
        unresolved_queue_items, unresolved_relations = apply_resolution_results(
            unresolved_pairs,
            resolution_results,
            created_at=datetime.now(JST).isoformat(),
        )
        if unresolved_relations:
            relation_map = {relation.document_id: relation for relation in candidate_relations}
            for relation in unresolved_relations:
                relation_map[relation.document_id] = relation
            candidate_relations = sorted(relation_map.values(), key=lambda item: item.document_id)
            apply_candidate_relations(touched_candidates, unresolved_relations)

    for ranked_item in ranked_candidates:
        matched_feature = next(
            (
                item
                for item in candidate_feature_list
                if item.candidate_id == ranked_item.candidate_id
            ),
            None,
        )
        breakdown = _build_legacy_breakdown(matched_feature)
        ranked_item.summary = generate_summary(
            candidate_name=ranked_item.display_name,
            trend_score=ranked_item.primary_score,
            breakdown=breakdown,
            evidence=ranked_item.evidence,
            mode=degrade.summary_mode,
            llm_client=llm_client,
        )
        if (
            degrade.summary_mode == MODE_LLM
            and llm_client
            and getattr(llm_client, "available", False)
        ):
            llm_summary_calls += 1

    logger.info("Step 9: Writing observations and rankings...")
    generated_at = datetime.now(JST).isoformat()

    source_daily_snapshots = []
    if persist_features:
        source_daily_snapshots = build_source_daily_snapshots(
            target_date=target_date,
            generated_at=generated_at,
            source_ids=weighted_source_ids,
            source_ok=source_ok,
            source_item_count=source_item_count,
            source_momentum=source_momentum,
            source_cfgs=source_cfg_map,
            weighting_cfg=source_weighting_config,
        )

    next_weight_snapshot: SourceWeightSnapshot | None = None
    if weighted_source_ids and persist_features:
        try:
            history_map = load_source_daily(target_date, source_weighting_config)
            for snapshot in source_daily_snapshots:
                history_map[(snapshot.date, snapshot.source_id)] = snapshot
            next_weight_snapshot = compute_weight_snapshot(
                target_date=target_date,
                generated_at=generated_at,
                source_cfgs=source_cfg_map,
                source_ids=weighted_source_ids,
                algo_cfg=algo_config,
                weighting_cfg=source_weighting_config,
                source_daily_records=list(history_map.values()),
            )
        except Exception as exc:
            errors.append(f"source_weight_compute: {exc}")
            logger.warning("Failed to compute next source weights: %s", exc)

    hindsight_labels: list[HindsightLabel] = []
    next_source_posteriors = []
    ranking_evaluations: list[RankingEvaluation] = []
    rollout_status: dict[str, Any] = {}
    write_ops_estimate = 0
    if persist_labels or persist_source_posteriors or persist_evaluations:
        try:
            label_plan = _build_label_plan(target_date)
            label_dates = _collect_label_related_dates(target_date, label_plan)
            historical_candidate_features = candidate_store.load_daily_candidate_features_by_dates(
                label_dates
            )
            candidate_feature_history = _index_candidate_features_by_date(
                historical_candidate_features
            )
            candidate_feature_history[target_date] = {
                feature.candidate_id: feature for feature in candidate_feature_list
            }

            labels_by_date: dict[str, dict[str, HindsightLabel]] = {}
            for anchor_date, horizons in label_plan.items():
                anchor_features = list(candidate_feature_history.get(anchor_date, {}).values())
                if not anchor_features:
                    continue
                existing_labels = candidate_store.load_hindsight_labels(anchor_date)
                computed_labels = build_hindsight_labels(
                    anchor_date,
                    anchor_features,
                    candidate_feature_history,
                    available_breakout_horizons=horizons["breakout"],
                    available_mass_horizons=horizons["mass"],
                    created_at=generated_at,
                )
                merged_labels = [
                    _merge_hindsight_label(existing_labels.get(label.candidate_id), label)
                    for label in computed_labels
                ]
                hindsight_labels.extend(merged_labels)
                labels_by_date[anchor_date] = {
                    label.candidate_id: label for label in merged_labels
                }

            source_history = candidate_store.load_daily_source_features_by_dates(
                list(labels_by_date)
            )
            next_source_posteriors = compute_source_posteriors(
                source_history,
                labels_by_date,
                updated_at=generated_at,
            )
            if persist_evaluations:
                ranking_evaluations = _build_ranking_evaluations(
                    label_plan=label_plan,
                    labels_by_date=labels_by_date,
                    candidate_feature_history=candidate_feature_history,
                    generated_at=generated_at,
                    run_id=run_id,
                    top_k=min(20, app_config.top_k),
                )
        except Exception as exc:
            errors.append(f"source_learning: {exc}")
            logger.warning(
                "Failed to update hindsight labels/source posteriors/evaluations: %s",
                exc,
            )

    ranking_items = [
        DailyRankingItem(
            rank=item.rank,
            candidate_id=item.candidate_id,
            candidate_type=item.candidate_type.value,
            display_name=item.display_name,
            trend_score=item.primary_score,
            breakdown_buckets=_build_legacy_breakdown(
                next(
                    (
                        feature
                        for feature in candidate_feature_list
                        if feature.candidate_id == item.candidate_id
                    ),
                    None,
                )
            ),
            sparkline_7d=_to_sparkline(
                touched_candidates[item.candidate_id].trend_history_7d[-7:]
            ),
            evidence_top3=item.evidence[:3],
            summary=item.summary,
            coming_score=item.coming_score,
            mass_heat=item.mass_heat,
            primary_score=item.primary_score,
            candidate_kind=item.candidate_kind.value,
            lane=item.lane.value,
            maturity=item.maturity,
            source_families=item.source_families,
        )
        for item in ranked_candidates
    ]
    source_health_records = build_source_health_records(
        target_date,
        source_ok,
        source_item_count,
        source_kept_count=source_kept_count,
        errors=source_errors,
        availability_tiers=source_availability_tier,
        fallback_used=source_fallback_used,
        response_ms=source_response_ms,
    )
    publish_health = evaluate_publish_health(
        source_health_records,
        source_plan,
        ranking_items,
        top_window=min(20, app_config.top_k),
    )
    effective_shadow_only = options.shadow_only or bool(publish_health.get("shadowOnly"))
    write_ops_estimate = _estimate_write_operations(
        light_publish=light_publish,
        publish_enabled=options.publish,
        effective_shadow_only=effective_shadow_only,
        connector_count=len(connectors),
        ranking_item_count=len(ranking_items),
        observations_count=len(observations),
        source_feature_count=len(source_features),
        candidate_feature_count=len(candidate_feature_list),
        touched_candidate_count=len(touched_candidates),
        alias_record_count=len(build_alias_records(touched_candidates.values())),
        candidate_relation_count=len(candidate_relations),
        unresolved_queue_count=len(unresolved_queue_items),
        source_health_count=len(source_health_records),
        source_daily_count=len(source_daily_snapshots),
        has_weight_snapshot=next_weight_snapshot is not None,
        hindsight_label_count=len(hindsight_labels),
        source_posterior_count=len(next_source_posteriors),
        ranking_evaluation_count=len(ranking_evaluations),
        has_rollout_status=bool(ranking_evaluations),
        persist_observations=persist_observations,
        persist_features=persist_features,
        persist_candidates=persist_candidates,
        persist_labels=persist_labels,
        persist_source_posteriors=persist_source_posteriors,
        persist_evaluations=persist_evaluations,
        existing_published_meta=existing_published_meta,
        target_date=target_date,
        run_id=run_id,
    )
    for evaluation in ranking_evaluations:
        evaluation.metadata["writeOpsEstimate"] = write_ops_estimate
    if persist_evaluations and ranking_evaluations:
        rollout_status = _build_shadow_rollout_status(
            target_date=target_date,
            ranking_evaluations=ranking_evaluations,
            shadow_days=app_config.shadow_days,
            top_k=min(20, app_config.top_k),
            generated_at=generated_at,
        )
        rollout_status["writeOpsEstimate"] = write_ops_estimate
    if effective_shadow_only and options.publish:
        logger.warning("Public publish gated; shadow-only mode engaged: %s", publish_health)

    try:
        from packages.core import firestore_client

        if options.publish:
            meta = _build_publish_meta(
                target_date=target_date,
                generated_at=generated_at,
                run_id=run_id,
                top_k=app_config.top_k,
                degrade=degrade,
                status="BUILDING",
                published_at=(
                    existing_published_meta.published_at if existing_published_meta else ""
                ),
                latest_published_run_id=(
                    existing_published_meta.latest_published_run_id
                    if existing_published_meta
                    else ""
                ),
                publish_health=publish_health,
            )
            prepublish_collections = _build_publish_collections(
                light_publish=light_publish,
                shadow_only=effective_shadow_only,
            )
            if existing_published_meta is None:
                for collection_name in prepublish_collections:
                    firestore_client.set_document(collection_name, target_date, meta.to_dict())
            firestore_client.set_document(
                f"daily_rankings/{target_date}/runs", run_id, meta.to_dict()
            )

            for collection_path in _build_reset_collection_paths(
                target_date,
                light_publish,
                shadow_only=effective_shadow_only,
            ):
                firestore_client.delete_collection_documents(collection_path)

            item_ops = []
            item_collection_paths = _build_item_collection_paths(
                target_date,
                run_id,
                light_publish,
                shadow_only=effective_shadow_only,
            )
            for item in ranking_items:
                item_dict = item.to_dict()
                for collection_path in item_collection_paths:
                    item_ops.append((collection_path, item.candidate_id, item_dict))
            if item_ops:
                if light_publish:
                    firestore_client.batch_write_with_chunk_size(item_ops, chunk_size=10)
                else:
                    firestore_client.batch_write(item_ops)

        if persist_observations:
            candidate_store.save_observations(observations)
        if persist_features:
            candidate_store.save_daily_source_features(source_features)
            candidate_store.save_daily_candidate_features(candidate_feature_list)
        if persist_candidates:
            candidate_store.upsert_touched_candidates(touched_candidates)
            candidate_store.save_candidate_relations(candidate_relations)
            if unresolved_queue_items:
                candidate_store.save_unresolved_resolution_items(
                    target_date,
                    unresolved_queue_items,
                )
        if persist_features and not effective_shadow_only:
            candidate_store.save_daily_rankings_v2(target_date, ranked_candidates)

        if persist_features:
            source_health_ops = [
                ("source_health", record.document_id, record.to_dict())
                for record in source_health_records
            ]
            if source_health_ops:
                firestore_client.batch_write(source_health_ops)

            source_daily_ops = [
                ("source_daily", snapshot.document_id, snapshot.to_dict())
                for snapshot in source_daily_snapshots
            ]
            if source_daily_ops:
                firestore_client.batch_write(source_daily_ops)

            if next_weight_snapshot is not None:
                firestore_client.set_document(
                    "source_weights", next_weight_snapshot.date, next_weight_snapshot.to_dict()
                )
                firestore_client.set_document(
                    "config", "source_weights_current", next_weight_snapshot.to_dict()
                )
        if persist_labels and hindsight_labels:
            candidate_store.save_hindsight_labels(hindsight_labels)
        if persist_source_posteriors and next_source_posteriors:
            candidate_store.save_source_posteriors(next_source_posteriors)
        if persist_evaluations and ranking_evaluations:
            candidate_store.save_ranking_evaluations(ranking_evaluations)
        if persist_evaluations and rollout_status:
            candidate_store.save_shadow_rollout_status(target_date, rollout_status)

        if options.publish:
            published_at = datetime.now(JST).isoformat()
            final_publish_status = "PUBLISHED" if not effective_shadow_only else "SHADOW_ONLY"
            publish_meta = _build_publish_meta(
                target_date=target_date,
                generated_at=generated_at,
                run_id=run_id,
                top_k=app_config.top_k,
                degrade=degrade,
                status=final_publish_status,
                published_at=published_at,
                latest_published_run_id=run_id,
                publish_health=publish_health,
            )
            collections_to_publish = _build_publish_collections(
                light_publish=light_publish,
                shadow_only=effective_shadow_only,
            )
            for collection in collections_to_publish:
                firestore_client.set_document(collection, target_date, publish_meta.to_dict())
            firestore_client.update_document(
                f"daily_rankings/{target_date}/runs",
                run_id,
                {
                    "status": final_publish_status,
                    "publishedAt": published_at,
                    "latestPublishedRunId": run_id if collections_to_publish else "",
                    "publishHealth": publish_health,
                },
            )
            published_successfully = True
        else:
            published_successfully = True
    except Exception as exc:
        errors.append(f"publish: {exc}")
        logger.error("Publish failed: %s", exc)

    logger.info("Step 10: Finalize run...")
    cost_jpy = estimate_run_cost(
        sources_used,
        0,
        llm_summary_calls,
        llm_resolution_calls,
    )
    if not light_publish:
        with contextlib.suppress(Exception):
            record_run_cost(
                run_id,
                target_date,
                cost_jpy,
                {
                    "sources": sources_used,
                    "xSearchCalls": 0,
                    "llmSummaryCalls": llm_summary_calls,
                    "llmResolutionCalls": llm_resolution_calls,
                    "writeOpsEstimate": write_ops_estimate,
                },
            )

    run_status = (
        "SUCCESS"
        if published_successfully and not errors
        else "PARTIAL"
        if published_successfully
        else "FAILED"
    )
    if not light_publish:
        with contextlib.suppress(Exception):
            end_run(
                run_id,
                status=run_status,
                candidate_count=len(touched_candidates),
                top_k=len(top_candidates),
                errors=errors or None,
                cost_jpy=cost_jpy,
                write_ops_estimate=write_ops_estimate,
                llm_resolution_calls=llm_resolution_calls,
            )

    with contextlib.suppress(Exception):
        release_lock(
            target_date, run_id, status="COMPLETED" if published_successfully else "FAILED"
        )

    _write_connector_summary(
        source_ok,
        errors,
        source_item_count=source_item_count,
        source_kept_count=source_kept_count,
        source_fallback_used=source_fallback_used,
        source_response_ms=source_response_ms,
        runtime_cfg_validation=runtime_cfg_validation,
        source_plan=source_plan,
        publish_health=publish_health,
        ranking_evaluations=ranking_evaluations,
        rollout_status=rollout_status,
        unresolved_queue_items=unresolved_queue_items,
        llm_resolution_calls=llm_resolution_calls,
        write_ops_estimate=write_ops_estimate,
    )

    if options.publish and not published_successfully:
        raise RuntimeError(f"Daily ranking publish failed for {target_date}")


def _safe_load(loader: Any, fallback: Any | None = None) -> Any:
    try:
        return loader()
    except Exception as exc:
        logger.warning("Config loader failed (%s): %s", getattr(loader, "__name__", loader), exc)
        if fallback is not None:
            return fallback
        raise


def _load_existing_candidates() -> dict[str, Candidate]:
    try:
        alias_index = load_alias_index()
        if alias_index:
            return candidate_store.load_candidates_by_ids(sorted(set(alias_index.values())))
    except Exception as exc:
        logger.warning("Failed to load alias registry: %s", exc)

    # Migration fallback only. Subsequent runs should rely on candidate_aliases.
    try:
        logger.warning("Alias registry empty; falling back to full candidate bootstrap")
        return candidate_store.load_all_candidates()
    except Exception as exc:
        logger.warning("Failed to bootstrap existing candidates: %s", exc)
        return {}


def _build_label_plan(target_date: str) -> dict[str, dict[str, list[int]]]:
    target = date.fromisoformat(target_date)
    return {
        (target - timedelta(days=1)).isoformat(): {"breakout": [1], "mass": []},
        (target - timedelta(days=3)).isoformat(): {"breakout": [1, 3], "mass": [3]},
        (target - timedelta(days=7)).isoformat(): {"breakout": [1, 3, 7], "mass": [3, 7]},
        (target - timedelta(days=14)).isoformat(): {
            "breakout": [1, 3, 7, 14],
            "mass": [3, 7],
        },
    }


def _collect_label_related_dates(
    target_date: str,
    label_plan: dict[str, dict[str, list[int]]],
) -> list[str]:
    target = date.fromisoformat(target_date)
    min_anchor = min(date.fromisoformat(anchor_date) for anchor_date in label_plan)
    return [
        (min_anchor + timedelta(days=offset)).isoformat()
        for offset in range((target - min_anchor).days + 1)
    ]


def _index_candidate_features_by_date(
    features: list[DailyCandidateFeature],
) -> dict[str, dict[str, DailyCandidateFeature]]:
    indexed: dict[str, dict[str, DailyCandidateFeature]] = defaultdict(dict)
    for feature in features:
        indexed[feature.date][feature.candidate_id] = feature
    return indexed


def _merge_hindsight_label(
    existing: HindsightLabel | None,
    computed: HindsightLabel,
) -> HindsightLabel:
    if existing is None:
        return computed

    return HindsightLabel(
        date=computed.date,
        candidate_id=computed.candidate_id,
        breakout_1d=computed.breakout_1d or existing.breakout_1d,
        breakout_3d=computed.breakout_3d or existing.breakout_3d,
        breakout_7d=computed.breakout_7d or existing.breakout_7d,
        breakout_14d=computed.breakout_14d or existing.breakout_14d,
        mass_now=computed.mass_now or existing.mass_now,
        mass_3d=computed.mass_3d or existing.mass_3d,
        mass_7d=computed.mass_7d or existing.mass_7d,
        new_confirmation_families=sorted(
            set(existing.new_confirmation_families) | set(computed.new_confirmation_families)
        ),
        lead_days=(
            min(existing.lead_days, computed.lead_days)
            if existing.lead_days is not None and computed.lead_days is not None
            else computed.lead_days
            if computed.lead_days is not None
            else existing.lead_days
        ),
        available_breakout_horizons=sorted(
            set(existing.available_breakout_horizons) | set(computed.available_breakout_horizons)
        ),
        available_mass_horizons=sorted(
            set(existing.available_mass_horizons) | set(computed.available_mass_horizons)
        ),
        created_at=computed.created_at or existing.created_at,
    )


def _build_ranking_evaluations(
    *,
    label_plan: dict[str, dict[str, list[int]]],
    labels_by_date: dict[str, dict[str, HindsightLabel]],
    candidate_feature_history: dict[str, dict[str, DailyCandidateFeature]],
    generated_at: str,
    run_id: str,
    top_k: int,
) -> list[RankingEvaluation]:
    evaluations: list[RankingEvaluation] = []
    for anchor_date in sorted(label_plan):
        labels = labels_by_date.get(anchor_date)
        feature_by_candidate = candidate_feature_history.get(anchor_date, {})
        if not labels or not feature_by_candidate:
            continue

        shadow_items = candidate_store.load_daily_ranking_items(
            anchor_date,
            collection_root="daily_rankings_v2_shadow",
        )
        public_items = candidate_store.load_daily_ranking_items(
            anchor_date,
            collection_root="daily_rankings",
        )
        shadow_meta = candidate_store.load_daily_ranking_meta(
            anchor_date,
            collection_root="daily_rankings_v2_shadow",
        )
        if shadow_meta is None:
            shadow_meta = candidate_store.load_daily_ranking_meta(
                anchor_date,
                collection_root="daily_rankings",
            )
        public_meta = candidate_store.load_daily_ranking_meta(
            anchor_date,
            collection_root="daily_rankings",
        )

        variants: dict[str, RankingEvaluation] = {}
        if shadow_items:
            shadow_entries = build_ranked_entries_from_items(shadow_items, feature_by_candidate)
            shadow_source = "stored_items"
        else:
            shadow_entries = build_ranked_entries_from_features(list(feature_by_candidate.values()))
            shadow_source = "feature_replay"

        shadow_metrics = evaluate_ranked_entries(
            shadow_entries,
            labels,
            candidate_feature_history,
            anchor_date=anchor_date,
            top_k=top_k,
        )
        variants["shadow_v2"] = RankingEvaluation(
            date=anchor_date,
            variant="shadow_v2",
            top_k=top_k,
            breakout_horizon_days=7,
            source_collection="daily_rankings_v2_shadow",
            ranking_source=shadow_source,
            item_count=len(shadow_entries),
            metrics=shadow_metrics,
            publish_health=dict(shadow_meta.publish_health if shadow_meta else {}),
            run_id=run_id,
            created_at=generated_at,
        )

        if public_items:
            public_entries = build_ranked_entries_from_items(public_items, feature_by_candidate)
            public_metrics = evaluate_ranked_entries(
                public_entries,
                labels,
                candidate_feature_history,
                anchor_date=anchor_date,
                top_k=top_k,
            )
            variants["public_main"] = RankingEvaluation(
                date=anchor_date,
                variant="public_main",
                top_k=top_k,
                breakout_horizon_days=7,
                source_collection="daily_rankings",
                ranking_source="stored_items",
                item_count=len(public_entries),
                metrics=public_metrics,
                publish_health=dict(public_meta.publish_health if public_meta else {}),
                run_id=run_id,
                created_at=generated_at,
            )

        if "public_main" in variants:
            comparison = compare_variant_metrics(
                variants["shadow_v2"].metrics,
                variants["public_main"].metrics,
                top_k=top_k,
            )
            variants["shadow_v2"].compared_variant = "public_main"
            variants["shadow_v2"].comparison = comparison
            variants["public_main"].compared_variant = "shadow_v2"
            variants["public_main"].comparison = {
                key: round(-value, 4) for key, value in comparison.items()
            }

        evaluations.extend(variants.values())
    return evaluations


def _build_shadow_rollout_status(
    *,
    target_date: str,
    ranking_evaluations: list[RankingEvaluation],
    shadow_days: int,
    top_k: int,
    generated_at: str,
) -> dict[str, Any]:
    if shadow_days <= 0:
        return {}

    recent_dates = [
        (date.fromisoformat(target_date) - timedelta(days=offset)).isoformat()
        for offset in range(1, shadow_days + 1)
    ]
    existing_evaluations = candidate_store.load_ranking_evaluations_by_dates(recent_dates)
    merged: dict[str, RankingEvaluation] = {
        evaluation.document_id: evaluation for evaluation in existing_evaluations
    }
    for evaluation in ranking_evaluations:
        merged[evaluation.document_id] = evaluation

    summary = evaluate_shadow_rollout(
        list(merged.values()),
        window_days=shadow_days,
        top_k=top_k,
    )
    summary.update(
        {
            "targetDate": target_date,
            "generatedAt": generated_at,
            "runId": next((item.run_id for item in ranking_evaluations if item.run_id), ""),
        }
    )
    return summary


def _estimate_write_operations(
    *,
    light_publish: bool,
    publish_enabled: bool,
    effective_shadow_only: bool,
    connector_count: int,
    ranking_item_count: int,
    observations_count: int,
    source_feature_count: int,
    candidate_feature_count: int,
    touched_candidate_count: int,
    alias_record_count: int,
    candidate_relation_count: int,
    unresolved_queue_count: int,
    source_health_count: int,
    source_daily_count: int,
    has_weight_snapshot: bool,
    hindsight_label_count: int,
    source_posterior_count: int,
    ranking_evaluation_count: int,
    has_rollout_status: bool,
    persist_observations: bool,
    persist_features: bool,
    persist_candidates: bool,
    persist_labels: bool,
    persist_source_posteriors: bool,
    persist_evaluations: bool,
    existing_published_meta: DailyRankingMeta | None,
    target_date: str,
    run_id: str,
) -> int:
    total = 0

    if not light_publish:
        total += 1  # start_run
        total += connector_count  # update_run_source
        total += 2  # cost log run + monthly total
        total += 1  # end_run
    total += 1  # lock release

    if publish_enabled:
        prepublish_collections = _build_publish_collections(
            light_publish=light_publish,
            shadow_only=effective_shadow_only,
        )
        item_collection_paths = _build_item_collection_paths(
            target_date,
            run_id,
            light_publish,
            shadow_only=effective_shadow_only,
        )
        total += 1  # daily_rankings/{date}/runs/{run_id}
        if existing_published_meta is None:
            total += len(prepublish_collections)
        total += ranking_item_count * len(item_collection_paths)
        total += len(prepublish_collections) + 1  # final publish docs + run status update

    if persist_observations:
        total += observations_count
    if persist_features:
        total += source_feature_count + candidate_feature_count
        total += source_health_count + source_daily_count
        if has_weight_snapshot:
            total += 2
        if not effective_shadow_only:
            total += ranking_item_count
    if persist_candidates:
        total += touched_candidate_count + alias_record_count
        total += candidate_relation_count + unresolved_queue_count
    if persist_labels:
        total += hindsight_label_count
    if persist_source_posteriors:
        total += source_posterior_count
    if persist_evaluations:
        total += ranking_evaluation_count
        if has_rollout_status:
            total += 2

    return total


def _resolve_raw_candidate(
    raw_candidate: RawCandidate,
    existing_candidates: dict[str, Candidate],
    alias_index: dict[str, str],
    key_index: dict[str, str],
) -> Candidate:
    if raw_candidate.kind == CandidateKind.ENTITY and not _passes_entity_precision(raw_candidate):
        raw_candidate.kind = CandidateKind.TOPIC

    candidate_id = resolve_candidate(
        raw_candidate.name,
        raw_candidate.type,
        existing_candidates,
        alias_index,
        key_index,
        external_ids=(
            dict(raw_candidate.extra.get("externalIds", {}))
            if isinstance(raw_candidate.extra.get("externalIds"), dict)
            else None
        ),
    )
    if candidate_id is None:
        candidate_id = str(ULID())
        new_candidate = create_new_candidate(
            raw_candidate.name,
            raw_candidate.type,
            candidate_id,
            aliases=raw_candidate.extra.get("aliases") if raw_candidate.extra else None,
        )
        new_candidate.kind = raw_candidate.kind or raw_candidate.type.default_kind
        new_candidate.domain_class = raw_candidate.domain_class
        existing_candidates[candidate_id] = new_candidate
        alias_index.clear()
        alias_index.update(build_alias_index(existing_candidates))
        key_index.clear()
        key_index.update(build_key_index(existing_candidates))
    return existing_candidates[candidate_id]


def _passes_entity_precision(raw_candidate: RawCandidate) -> bool:
    if raw_candidate.kind == CandidateKind.TOPIC:
        return False
    from packages.core.proper_noun import is_proper_noun

    return is_proper_noun(raw_candidate.name)


def _build_observation(
    target_date: str,
    raw_candidate: RawCandidate,
    candidate: Candidate,
    source_entry: Any,
) -> Observation:
    evidence = raw_candidate.evidence
    return Observation(
        observation_id=raw_candidate.observation_id or str(ULID()),
        date=target_date,
        source_id=raw_candidate.source_id,
        source_item_id=raw_candidate.source_item_id
        or f"{raw_candidate.source_id}:{raw_candidate.rank or 0}:{candidate.candidate_id}",
        candidate_id=candidate.candidate_id,
        candidate_type=raw_candidate.type,
        candidate_kind=raw_candidate.kind or raw_candidate.type.default_kind,
        surface=raw_candidate.name,
        canonical_name=candidate.canonical_name,
        match_key=candidate.match_key,
        signal_value=raw_candidate.metric_value,
        source_role=source_entry.role,
        family_primary=source_entry.family_primary,
        family_secondary=source_entry.family_secondary,
        extraction_confidence=raw_candidate.extraction_confidence,
        domain_class=raw_candidate.domain_class,
        url=evidence.url if evidence else "",
        title=evidence.title if evidence else raw_candidate.name,
        rank=raw_candidate.rank,
        metadata=dict(raw_candidate.extra),
    )


def _dedupe_evidence(evidence_items: Any) -> list[Evidence]:
    result: list[Evidence] = []
    seen: set[tuple[str, str]] = set()
    for item in evidence_items:
        if item is None:
            continue
        key = (item.source_id, item.title)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _build_source_feature_metadata(
    raw_items: list[RawCandidate],
    source_weight: float,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"sourceWeight": source_weight}
    regions: list[str] = []
    countries: list[str] = []
    country_ranks: dict[str, int] = {}
    best_regional_score: float | None = None

    for item in raw_items:
        region = item.extra.get("region")
        if isinstance(region, str) and region:
            regions.append(region)

        for country in item.extra.get("countries", []):
            if isinstance(country, str) and country:
                countries.append(country)

        raw_country_ranks = item.extra.get("countryRanks", {})
        if isinstance(raw_country_ranks, dict):
            for country, rank in raw_country_ranks.items():
                if not isinstance(country, str) or not country:
                    continue
                try:
                    normalized_rank = int(rank)
                except (TypeError, ValueError):
                    continue
                previous = country_ranks.get(country)
                country_ranks[country] = (
                    normalized_rank if previous is None else min(previous, normalized_rank)
                )

        regional_score = item.extra.get("regionalScore")
        if regional_score is not None:
            try:
                normalized_score = float(regional_score)
            except (TypeError, ValueError):
                normalized_score = None
            if normalized_score is not None:
                if best_regional_score is None:
                    best_regional_score = normalized_score
                else:
                    best_regional_score = max(best_regional_score, normalized_score)

    if regions:
        metadata["regions"] = sorted(dict.fromkeys(regions))
    if countries:
        metadata["countries"] = sorted(
            dict.fromkeys(countries),
            key=lambda country: (country != "JP", country),
        )
    if country_ranks:
        metadata["countryRanks"] = {
            country: country_ranks[country]
            for country in sorted(
                country_ranks,
                key=lambda country: (country != "JP", country_ranks[country], country),
            )
        }
    if best_regional_score is not None:
        metadata["regionalScore"] = best_regional_score

    return metadata


def _max_confidence(raw_items: list[RawCandidate]) -> ExtractionConfidence:
    ordered = [ExtractionConfidence.LOW, ExtractionConfidence.MEDIUM, ExtractionConfidence.HIGH]
    best = max(raw_items, key=lambda item: ordered.index(item.extraction_confidence))
    return best.extraction_confidence


def _pick_domain(candidate: Candidate, raw_items: list[RawCandidate]) -> DomainClass:
    for item in raw_items:
        if item.domain_class != DomainClass.OTHER:
            return item.domain_class
    for item in raw_items:
        inferred = classify_domain(item.type, item.source_id, text=item.name, metadata=item.extra)
        if inferred != DomainClass.OTHER:
            return inferred
    return candidate.domain_class


def _pick_feature_domain(
    candidate: Candidate,
    features: list[DailySourceFeature],
) -> DomainClass:
    for feature in features:
        if feature.domain_class != DomainClass.OTHER:
            return feature.domain_class
    for feature in features:
        metadata = {"title": feature.evidence[0].title} if feature.evidence else {}
        inferred = classify_domain(
            feature.candidate_type,
            feature.source_id,
            text=candidate.display_name or candidate.canonical_name,
            metadata=metadata,
        )
        if inferred != DomainClass.OTHER:
            return inferred
    return candidate.domain_class


def _build_legacy_breakdown(feature: DailyCandidateFeature | None) -> list[BucketScore]:
    if feature is None:
        return []
    family_scores = dict(feature.metadata.get("familyScores", {}))
    return [
        BucketScore(bucket=bucket, score=float(score))
        for bucket, score in sorted(family_scores.items(), key=lambda item: -float(item[1]))
        if float(score) > 0
    ]


def _to_sparkline(history: list[float]) -> list[float | None]:
    return [float(score) for score in history]


def _write_connector_summary(
    source_ok: dict[str, bool],
    errors: list[str],
    *,
    source_item_count: dict[str, int] | None = None,
    source_kept_count: dict[str, int] | None = None,
    source_fallback_used: dict[str, str] | None = None,
    source_response_ms: dict[str, int] | None = None,
    runtime_cfg_validation: dict[str, list[str]] | None = None,
    source_plan: list[dict[str, Any]] | None = None,
    publish_health: dict[str, Any] | None = None,
    ranking_evaluations: list[RankingEvaluation] | None = None,
    rollout_status: dict[str, Any] | None = None,
    unresolved_queue_items: list[dict[str, Any]] | None = None,
    llm_resolution_calls: int = 0,
    write_ops_estimate: int = 0,
) -> None:
    result_path = os.environ.get("BATCH_RESULT_PATH", "/tmp/batch_result.json")
    source_item_count = source_item_count or {}
    source_kept_count = source_kept_count or {}
    source_fallback_used = source_fallback_used or {}
    source_response_ms = source_response_ms or {}
    failed = {sid: False for sid, ok in source_ok.items() if not ok}
    error_map: dict[str, str] = {}
    for err in errors:
        if ":" in err:
            sid, msg = err.split(":", 1)
            if sid.strip() in failed:
                error_map[sid.strip()] = msg.strip()
    summary = {
        "sources": {
            sid: {
                "ok": ok,
                "error": error_map.get(sid, ""),
                "rawItemCount": source_item_count.get(sid, 0),
                "keptItemCount": source_kept_count.get(sid, 0),
                "fallbackUsed": source_fallback_used.get(sid, ""),
                "responseMs": source_response_ms.get(sid),
            }
            for sid, ok in source_ok.items()
        },
        "failed_sources": list(failed.keys()),
        "runtimeConfigValidation": runtime_cfg_validation or {},
        "sourcePlan": source_plan or [],
        "publishHealth": publish_health or {},
        "rankingEvaluations": [evaluation.to_dict() for evaluation in ranking_evaluations or []],
        "rolloutStatus": rollout_status or {},
        "unresolvedResolution": {
            "pairCount": len(unresolved_queue_items or []),
            "mergeRecommendedCount": sum(
                1
                for item in unresolved_queue_items or []
                if item.get("finalAction") == "MERGE_RECOMMENDED"
            ),
            "linkOnlyCount": sum(
                1
                for item in unresolved_queue_items or []
                if item.get("finalAction") == "LINK_ONLY"
            ),
            "llmResolutionCalls": llm_resolution_calls,
        },
        "writeOpsEstimate": write_ops_estimate,
    }
    with contextlib.suppress(Exception), open(result_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trends daily batch")
    parser.add_argument("--date", default="today", help="Target date (YYYY-MM-DD or 'today')")
    parser.add_argument("--shadow-only", action="store_true", help="Publish only shadow outputs")
    parser.add_argument(
        "--publish",
        dest="publish",
        action="store_true",
        default=True,
        help="Publish ranking outputs",
    )
    parser.add_argument(
        "--no-publish",
        dest="publish",
        action="store_false",
        help="Skip ranking publish and only persist diagnostics/features",
    )
    parser.add_argument(
        "--persist-observations",
        dest="persist_observations",
        action="store_true",
        default=True,
        help="Persist raw observations",
    )
    parser.add_argument(
        "--no-persist-observations",
        dest="persist_observations",
        action="store_false",
        help="Do not persist raw observations",
    )
    parser.add_argument(
        "--persist-features",
        dest="persist_features",
        action="store_true",
        default=True,
        help="Persist source/candidate features and source health",
    )
    parser.add_argument(
        "--no-persist-features",
        dest="persist_features",
        action="store_false",
        help="Do not persist source/candidate features",
    )
    parser.add_argument(
        "--persist-candidates",
        dest="persist_candidates",
        action="store_true",
        default=True,
        help="Persist touched candidates and alias updates",
    )
    parser.add_argument(
        "--no-persist-candidates",
        dest="persist_candidates",
        action="store_false",
        help="Do not persist touched candidates",
    )
    parser.add_argument(
        "--persist-labels",
        dest="persist_labels",
        action="store_true",
        default=True,
        help="Persist hindsight labels",
    )
    parser.add_argument(
        "--no-persist-labels",
        dest="persist_labels",
        action="store_false",
        help="Do not persist hindsight labels",
    )
    parser.add_argument(
        "--persist-source-posteriors",
        dest="persist_source_posteriors",
        action="store_true",
        default=True,
        help="Persist learned source posterior stats",
    )
    parser.add_argument(
        "--no-persist-source-posteriors",
        dest="persist_source_posteriors",
        action="store_false",
        help="Do not persist learned source posterior stats",
    )
    parser.add_argument(
        "--persist-evaluations",
        dest="persist_evaluations",
        action="store_true",
        default=True,
        help="Persist hindsight-based ranking evaluation snapshots",
    )
    parser.add_argument(
        "--no-persist-evaluations",
        dest="persist_evaluations",
        action="store_false",
        help="Do not persist hindsight-based ranking evaluation snapshots",
    )
    parser.add_argument(
        "--skip-slow-sources",
        action="store_true",
        help="Skip HTML/manual/LLM sources for faster diagnostic runs",
    )
    parser.add_argument(
        "--source-include",
        action="append",
        default=[],
        help="Limit run to these source IDs (comma-separated or repeated)",
    )
    parser.add_argument(
        "--source-exclude",
        action="append",
        default=[],
        help="Exclude these source IDs (comma-separated or repeated)",
    )
    args = parser.parse_args()
    main(
        date_arg=args.date,
        options=BatchRuntimeOptions(
            shadow_only=args.shadow_only,
            publish=args.publish,
            persist_observations=args.persist_observations,
            persist_features=args.persist_features,
            persist_candidates=args.persist_candidates,
            persist_labels=args.persist_labels,
            persist_source_posteriors=args.persist_source_posteriors,
            persist_evaluations=args.persist_evaluations,
            skip_slow_sources=args.skip_slow_sources,
            source_include=_parse_csv_arg(args.source_include),
            source_exclude=_parse_csv_arg(args.source_exclude),
        ),
    )
