"""Observation-first daily batch for Trends v2."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from ulid import ULID

from batch.cost_tracker import estimate_run_cost, record_run_cost
from batch.degrade import DegradeState, compute_degrade_state
from packages.connectors.base import BaseConnector, ConnectorRunResult
from packages.core import candidate_store
from packages.core.alias_registry import load_alias_index
from packages.core.config import (
    load_algorithm_config,
    load_all_source_configs,
    load_app_config,
    load_source_weighting_config,
)
from packages.core.diversification import infer_lane
from packages.core.domain_classifier import classify_domain
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
    Observation,
    RawCandidate,
    SourceWeightSnapshot,
)
from packages.core.ranking import build_ranked_candidates_v2
from packages.core.resolve import (
    build_alias_index,
    build_key_index,
    create_new_candidate,
    resolve_candidate,
)
from packages.core.run_logger import end_run, start_run, update_run_source
from packages.core.scoring_v2 import (
    compute_candidate_feature,
    compute_source_feature_score,
    group_features_by_candidate,
)
from packages.core.source_catalog import get_source_entry
from packages.core.source_health import build_source_health_records
from packages.core.source_weighting import (
    build_source_daily_snapshots,
    compute_weight_snapshot,
    filter_weighted_source_ids,
    load_current_source_weights,
    load_source_daily,
)
from packages.core.summary import MODE_LLM, generate_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
LOCK_TIMEOUT_MINUTES = 30
PUBLISH_COLLECTIONS = ("daily_rankings", "daily_rankings_v2", "daily_rankings_v2_shadow")


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


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
) -> tuple[str, ...]:
    if light_publish:
        return (f"daily_rankings/{target_date}/runs/{run_id}/items",)
    return (
        f"daily_rankings/{target_date}/items",
        f"daily_rankings_v2_shadow/{target_date}/items",
        f"daily_rankings/{target_date}/runs/{run_id}/items",
    )


def _build_reset_collection_paths(target_date: str, light_publish: bool) -> tuple[str, ...]:
    if light_publish:
        return ()
    return (
        f"daily_rankings/{target_date}/items",
        f"daily_rankings_v2/{target_date}/items",
        f"daily_rankings_v2_shadow/{target_date}/items",
    )


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
    )


def _create_connectors(source_cfgs: list[dict[str, Any]] | None = None) -> list[BaseConnector]:
    from packages.connectors.registry import build_connectors

    return build_connectors(source_cfgs or [])


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


def main(date_arg: str = "today") -> None:
    target_date = get_target_date(date_arg)
    run_id = str(ULID())
    errors: list[str] = []
    allow_completed_rerun = _is_truthy(os.environ.get("BATCH_ALLOW_RERUN_COMPLETED"))

    logger.info("=== Trends Daily Batch v2 ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Target date: %s", target_date)

    lock_acquired = False
    try:
        if not acquire_lock(target_date, run_id, allow_completed_rerun=allow_completed_rerun):
            return
        lock_acquired = True
        _run_pipeline(target_date, run_id, errors)
    except Exception:
        if lock_acquired:
            with contextlib.suppress(Exception):
                release_lock(target_date, run_id, status="FAILED")
        raise


def _run_pipeline(target_date: str, run_id: str, errors: list[str]) -> None:
    published_successfully = False
    top_candidates: list[Any] = []

    logger.info("Step 0: Loading configs...")
    app_config = _safe_load(load_app_config)
    algo_config = _safe_load(load_algorithm_config)
    source_weighting_config = _safe_load(load_source_weighting_config)
    source_cfg_list = _safe_load(load_all_source_configs, fallback=[])

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

    if not light_publish:
        with contextlib.suppress(Exception):
            start_run(run_id, target_date, degrade.to_dict())

    connectors = _create_connectors(source_cfg_list)
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

    existing_candidates = _load_existing_candidates()
    alias_index = build_alias_index(existing_candidates)
    key_index = build_key_index(existing_candidates)
    touched_candidates: dict[str, Candidate] = {}
    observations: list[Observation] = []
    raw_by_candidate_source: dict[tuple[str, str], list[RawCandidate]] = defaultdict(list)
    source_ok: dict[str, bool] = {}
    source_item_count: dict[str, int] = {}
    source_errors: dict[str, str] = {}
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

            observation = _build_observation(target_date, raw_candidate, candidate, entry)
            observations.append(observation)
            raw_by_candidate_source[(candidate_id, source_id)].append(raw_candidate)

    logger.info(
        "Resolved %d touched candidates across %d observations",
        len(touched_candidates),
        len(observations),
    )

    logger.info("Step 5-8: Build local features, score, rank...")
    source_features: list[DailySourceFeature] = []
    source_momentum: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (candidate_id, source_id), raw_items in raw_by_candidate_source.items():
        candidate = touched_candidates[candidate_id]
        entry = get_source_entry(source_id)
        if entry is None:
            continue
        total_signal = sum(item.metric_value for item in raw_items)
        anomaly_score, surprise01 = compute_source_feature_score(
            candidate,
            source_id,
            total_signal,
            algo_config,
            target_date,
            entry.family_primary,
        )
        source_weight = source_weights.get(source_id, 1.0)
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
            observation_ids=[
                obs.observation_id
                for obs in observations
                if obs.candidate_id == candidate_id and obs.source_id == source_id
            ],
            evidence=evidence[:5],
            metadata=_build_source_feature_metadata(raw_items, source_weight),
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
    llm_client = None
    if degrade.summary_mode == MODE_LLM:
        with contextlib.suppress(Exception):
            from packages.core.llm_client import LLMClient

            llm_client = LLMClient()

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
    if not light_publish:
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
    if weighted_source_ids and not light_publish:
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

    try:
        from packages.core import firestore_client

        meta = _build_publish_meta(
            target_date=target_date,
            generated_at=generated_at,
            run_id=run_id,
            top_k=app_config.top_k,
            degrade=degrade,
            status="BUILDING",
            published_at=existing_published_meta.published_at if existing_published_meta else "",
            latest_published_run_id=(
                existing_published_meta.latest_published_run_id if existing_published_meta else ""
            ),
        )
        if existing_published_meta is None:
            for collection_name in PUBLISH_COLLECTIONS:
                firestore_client.set_document(collection_name, target_date, meta.to_dict())
        firestore_client.set_document(f"daily_rankings/{target_date}/runs", run_id, meta.to_dict())

        for collection_path in _build_reset_collection_paths(target_date, light_publish):
            firestore_client.delete_collection_documents(collection_path)

        item_ops = []
        item_collection_paths = _build_item_collection_paths(target_date, run_id, light_publish)
        for item in ranking_items:
            item_dict = item.to_dict()
            for collection_path in item_collection_paths:
                item_ops.append((collection_path, item.candidate_id, item_dict))
        if item_ops:
            if light_publish:
                firestore_client.batch_write_with_chunk_size(item_ops, chunk_size=10)
            else:
                firestore_client.batch_write(item_ops)

        if not light_publish:
            candidate_store.save_observations(observations)
            candidate_store.save_daily_source_features(source_features)
            candidate_store.save_daily_candidate_features(candidate_feature_list)
            candidate_store.upsert_touched_candidates(touched_candidates)
            candidate_store.save_daily_rankings_v2(target_date, ranked_candidates)

            source_health_ops = [
                ("source_health", record.document_id, record.to_dict())
                for record in build_source_health_records(
                    target_date, source_ok, source_item_count, errors=source_errors
                )
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

        published_at = datetime.now(JST).isoformat()
        publish_meta = _build_publish_meta(
            target_date=target_date,
            generated_at=generated_at,
            run_id=run_id,
            top_k=app_config.top_k,
            degrade=degrade,
            status="PUBLISHED",
            published_at=published_at,
            latest_published_run_id=run_id,
        )
        collections_to_publish = ("daily_rankings",) if light_publish else PUBLISH_COLLECTIONS
        for collection in collections_to_publish:
            firestore_client.set_document(collection, target_date, publish_meta.to_dict())
        firestore_client.update_document(
            f"daily_rankings/{target_date}/runs",
            run_id,
            {
                "status": "PUBLISHED",
                "publishedAt": published_at,
                "latestPublishedRunId": run_id,
            },
        )
        published_successfully = True
    except Exception as exc:
        errors.append(f"publish: {exc}")
        logger.error("Publish failed: %s", exc)

    logger.info("Step 10: Finalize run...")
    cost_jpy = estimate_run_cost(sources_used, 0, llm_summary_calls)
    if not light_publish:
        with contextlib.suppress(Exception):
            record_run_cost(
                run_id,
                target_date,
                cost_jpy,
                {"sources": sources_used, "xSearchCalls": 0, "llmSummaryCalls": llm_summary_calls},
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
            )

    with contextlib.suppress(Exception):
        release_lock(
            target_date, run_id, status="COMPLETED" if published_successfully else "FAILED"
        )

    _write_connector_summary(source_ok, errors)

    if not published_successfully:
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


def _write_connector_summary(source_ok: dict[str, bool], errors: list[str]) -> None:
    result_path = os.environ.get("BATCH_RESULT_PATH", "/tmp/batch_result.json")
    failed = {sid: False for sid, ok in source_ok.items() if not ok}
    error_map: dict[str, str] = {}
    for err in errors:
        if ":" in err:
            sid, msg = err.split(":", 1)
            if sid.strip() in failed:
                error_map[sid.strip()] = msg.strip()
    summary = {
        "sources": {
            sid: {"ok": ok, "error": error_map.get(sid, "")} for sid, ok in source_ok.items()
        },
        "failed_sources": list(failed.keys()),
    }
    with contextlib.suppress(Exception), open(result_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trends daily batch")
    parser.add_argument("--date", default="today", help="Target date (YYYY-MM-DD or 'today')")
    args = parser.parse_args()
    main(date_arg=args.date)
