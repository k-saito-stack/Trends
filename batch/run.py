"""Daily batch entry point.

Orchestrates the 12-step pipeline:
  0) Load config, cost_logs, candidates
  1) Start run (ULID, targetDate, degradeState)
  2) Ingest (fetch from each source)
  3) Extract raw candidates
  4) Normalize -> Resolve (assign candidate IDs)
  5) Compute daily signals x(s,q,t)
  6) EWMA/EWMVar update -> sig -> momentum
  7) Aggregate to buckets + multiBonus
  8) Select TopK
  9) Select EvidenceTop3
  10) Generate summary
  11) Write to Firestore
  12) End logging + cost_logs

Spec reference: Section 13 (Daily Batch Runbook)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from ulid import ULID

from batch.cost_tracker import estimate_run_cost, record_run_cost
from batch.degrade import DegradeState, compute_degrade_state
from packages.connectors.apple_music import AppleMusicConnector
from packages.connectors.base import BaseConnector, ConnectorRunResult, SignalResult
from packages.connectors.google_trends import GoogleTrendsConnector
from packages.connectors.rakuten_magazine import RakutenMagazineConnector
from packages.connectors.rss_feeds import RSSFeedConnector
from packages.connectors.x_search import XTrendingConnector
from packages.connectors.youtube import YouTubeConnector
from packages.core import candidate_store
from packages.core.config import (
    load_algorithm_config,
    load_all_source_configs,
    load_app_config,
    load_music_config,
    load_source_weighting_config,
)
from packages.core.evidence import build_evidence_pool, select_evidence_top3
from packages.core.models import (
    CandidateType,
    DailyRankingItem,
    DailyRankingMeta,
    RawCandidate,
    SourceWeightingConfig,
    SourceWeightSnapshot,
)
from packages.core.normalize import extract_bracket_aliases, normalize_name
from packages.core.proper_noun import is_proper_noun
from packages.core.ranking import compute_candidate_score, select_top_k
from packages.core.resolve import (
    build_alias_index,
    build_key_index,
    create_new_candidate,
    resolve_candidate,
)
from packages.core.run_logger import end_run, start_run, update_run_source
from packages.core.scoring import momentum, update_source_state
from packages.core.source_weighting import (
    build_source_daily_snapshots,
    compute_weight_snapshot,
    filter_weighted_source_ids,
    load_current_source_weights,
    load_source_daily,
)
from packages.core.summary import generate_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# Lock timeout: if a run has been RUNNING for longer than this, it's considered stale
LOCK_TIMEOUT_MINUTES = 30


def get_target_date(date_arg: str) -> str:
    """Parse the --date argument and return YYYY-MM-DD in JST."""
    if date_arg == "today":
        return datetime.now(JST).strftime("%Y-%m-%d")
    return date_arg


def acquire_lock(target_date: str, run_id: str) -> bool:
    """Acquire a per-date lock to prevent duplicate runs.

    Returns True if the lock was acquired (proceed with batch).
    Returns False if the date was already processed or another run is active.
    """
    from packages.core import firestore_client

    lock_id = f"_lock_{target_date}"
    now_iso = datetime.now(JST).isoformat()

    # Try atomic create (fails if doc already exists)
    created = firestore_client.create_document("runs", lock_id, {
        "status": "RUNNING",
        "runId": run_id,
        "targetDate": target_date,
        "startedAt": now_iso,
    })

    if created:
        return True

    # Lock doc exists — check its status
    lock_doc = firestore_client.get_document("runs", lock_id)
    if lock_doc is None:
        return False

    lock_status = lock_doc.get("status", "")

    if lock_status == "COMPLETED":
        logger.info("Date %s already completed (run=%s). Skipping.",
                     target_date, lock_doc.get("runId", "?"))
        return False

    if lock_status == "RUNNING":
        # Check if the lock is stale (timed out)
        started_at = lock_doc.get("startedAt", "")
        if started_at:
            try:
                started = datetime.fromisoformat(started_at)
                elapsed = datetime.now(JST) - started
                if elapsed.total_seconds() > LOCK_TIMEOUT_MINUTES * 60:
                    logger.warning(
                        "Stale lock for %s (started %s). Overriding.",
                        target_date, started_at,
                    )
                    firestore_client.set_document("runs", lock_id, {
                        "status": "RUNNING",
                        "runId": run_id,
                        "targetDate": target_date,
                        "startedAt": now_iso,
                    })
                    return True
            except (ValueError, TypeError):
                pass

        logger.info("Another run is active for %s. Skipping.", target_date)
        return False

    # Unknown status — treat as stale and override
    firestore_client.set_document("runs", lock_id, {
        "status": "RUNNING",
        "runId": run_id,
        "targetDate": target_date,
        "startedAt": now_iso,
    })
    return True


def release_lock(target_date: str, run_id: str, status: str = "COMPLETED") -> None:
    """Release the per-date lock by marking it as completed."""
    from packages.core import firestore_client

    lock_id = f"_lock_{target_date}"
    firestore_client.update_document("runs", lock_id, {
        "status": status,
        "runId": run_id,
        "endedAt": datetime.now(JST).isoformat(),
    })


def _create_connectors(source_cfgs: list[dict[str, Any]] | None = None) -> list[BaseConnector]:
    """Create connector instances from Firestore source configs.

    Falls back to hardcoded defaults if config loading fails.
    """
    try:
        from packages.connectors.registry import build_connectors
        if source_cfgs:
            return build_connectors(source_cfgs)
        logger.warning("No source configs found, using defaults")
    except Exception as e:
        logger.warning("Failed to load source configs: %s (using defaults)", e)

    # Fallback: hardcoded defaults
    return [
        YouTubeConnector(),
        AppleMusicConnector(region="JP"),
        AppleMusicConnector(region="GLOBAL"),
        GoogleTrendsConnector(),
        RSSFeedConnector(),
        RakutenMagazineConnector(),
        XTrendingConnector(),
    ]


def _build_runtime_source_cfg_map(
    source_cfgs: list[dict[str, Any]],
    connectors: list[BaseConnector],
) -> dict[str, dict[str, Any]]:
    """Build source config map and backfill minimal metadata from connectors."""
    cfg_map = {
        str(cfg["sourceId"]): dict(cfg)
        for cfg in source_cfgs
        if cfg.get("sourceId")
    }

    for connector in connectors:
        source_id = connector.source_id
        cfg = dict(cfg_map.get(source_id, {}))
        cfg.setdefault("sourceId", source_id)
        if not cfg.get("fetchLimit"):
            for attr_name in ("max_results", "max_items_per_feed"):
                value = getattr(connector, attr_name, None)
                if isinstance(value, int) and value > 0:
                    cfg["fetchLimit"] = value
                    break
        cfg_map[source_id] = cfg

    return cfg_map


def main(date_arg: str = "today") -> None:
    """Run the daily batch pipeline."""
    target_date = get_target_date(date_arg)
    run_id = str(ULID())
    errors: list[str] = []

    logger.info("=== Trends Daily Batch ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Target date: %s", target_date)

    # Acquire per-date lock (prevent duplicate runs)
    lock_acquired = False
    try:
        if not acquire_lock(target_date, run_id):
            logger.info("=== Batch skipped (lock not acquired) ===")
            return
        lock_acquired = True
        logger.info("Lock acquired for %s", target_date)
    except Exception as e:
        logger.error("Lock check failed; aborting batch run: %s", e)
        raise

    try:
        _run_pipeline(target_date, run_id, errors)
    except Exception as e:
        logger.error("Batch pipeline failed with unexpected error: %s", e)
        errors.append(f"FATAL: {e}")
        if lock_acquired:
            try:
                release_lock(target_date, run_id, status="FAILED")
            except Exception as lock_err:
                logger.warning("Failed to release lock as FAILED: %s", lock_err)
        raise


def _run_pipeline(target_date: str, run_id: str, errors: list[str]) -> None:
    """Execute the main batch pipeline (extracted for lock safety)."""
    published_successfully = False

    # ── Step 0: Load config + candidates ──
    logger.info("Step 0: Loading config and candidates...")
    source_cfg_list: list[dict[str, Any]] = []
    source_weighting_config = SourceWeightingConfig()
    try:
        app_config = load_app_config()
        algo_config = load_algorithm_config()
        music_config = load_music_config()
    except Exception as e:
        logger.error("Failed to load config: %s", e)
        logger.info("Using default config values")
        from packages.core.models import AlgorithmConfig, AppConfig, MusicConfig
        app_config = AppConfig()
        algo_config = AlgorithmConfig()
        music_config = MusicConfig()

    try:
        source_cfg_list = load_all_source_configs()
    except Exception as e:
        logger.warning("Failed to load source configs (using connector defaults): %s", e)
        source_cfg_list = []

    try:
        source_weighting_config = load_source_weighting_config()
    except Exception as e:
        logger.warning("Failed to load source weighting config (using defaults): %s", e)
        source_weighting_config = SourceWeightingConfig()

    try:
        existing_candidates = candidate_store.load_all_candidates()
        logger.info("Loaded %d existing candidates", len(existing_candidates))
    except Exception as e:
        logger.warning("Failed to load candidates (starting fresh): %s", e)
        existing_candidates = {}

    # Compute degrade state
    degrade = DegradeState()
    try:
        from batch.cost_tracker import get_budget_ratio
        budget_ratio = get_budget_ratio(app_config.monthly_budget_jpy)
        degrade = compute_degrade_state(budget_ratio, app_config)
        if degrade.reason:
            logger.info("Degrade: %s", degrade.reason)
    except Exception as e:
        logger.warning("Failed to compute degrade state: %s", e)

    # ── Step 1: Start run ──
    logger.info("Step 1: Starting run...")
    try:
        start_run(run_id, target_date, degrade.to_dict())
    except Exception as e:
        logger.warning("Failed to log run start: %s", e)

    # ── Step 2-3: Ingest + Extract raw candidates ──
    logger.info("Step 2-3: Ingesting from sources...")
    connectors = _create_connectors(source_cfg_list)
    source_cfg_map = _build_runtime_source_cfg_map(source_cfg_list, connectors)
    all_raw_candidates: list[RawCandidate] = []
    all_signals: dict[str, list[SignalResult]] = {}  # source_id -> signals
    source_ok: dict[str, bool] = {}  # source_id -> fetch success flag
    source_item_count: dict[str, int] = {}  # source_id -> fetched item count
    sources_used: list[str] = []

    for connector in connectors:
        source_id = connector.source_id
        logger.info("  Fetching: %s", source_id)
        try:
            run_result: ConnectorRunResult = connector.run()
            source_ok[source_id] = run_result.ok
            source_item_count[source_id] = run_result.item_count
            if run_result.ok:
                sources_used.append(source_id)
            all_raw_candidates.extend(run_result.candidates)
            all_signals[source_id] = run_result.signals

            # Log source result
            with contextlib.suppress(Exception):
                update_run_source(
                    run_id, source_id, len(run_result.candidates),
                    error=run_result.error,
                )

            if run_result.error and not run_result.ok:
                errors.append(f"{source_id}: {run_result.error}")

        except Exception as e:
            error_msg = f"{source_id}: {e}"
            errors.append(error_msg)
            source_ok[source_id] = False
            source_item_count[source_id] = 0
            logger.error("  Failed: %s", error_msg)
            with contextlib.suppress(Exception):
                update_run_source(run_id, source_id, 0, error=str(e))

    logger.info(
        "Ingestion complete: %d raw candidates, %d sources OK",
        len(all_raw_candidates),
        len(sources_used),
    )

    # ── Step 4: Normalize -> Resolve ──
    logger.info("Step 4: Normalize and resolve candidates...")
    alias_index = build_alias_index(existing_candidates)
    key_index = build_key_index(existing_candidates)

    # Map: candidate_id -> {source_id -> signal_value}
    candidate_signals: dict[str, dict[str, float]] = {}
    # Map: candidate_id -> list of evidence dicts
    candidate_evidence: dict[str, list[dict[str, Any]]] = {}
    new_candidate_count = 0

    for raw in all_raw_candidates:
        # Normalize
        display_name = normalize_name(raw.name)
        canonical, aliases = extract_bracket_aliases(display_name)

        # Noise filter
        if not is_proper_noun(canonical):
            continue

        # Resolve
        cand_id = resolve_candidate(
            canonical, raw.type, existing_candidates, alias_index, key_index
        )

        if cand_id is None:
            # Create new candidate
            cand_id = str(ULID())
            new_cand = create_new_candidate(
                canonical, raw.type, cand_id, aliases=aliases
            )
            existing_candidates[cand_id] = new_cand
            # Update indices
            alias_index = build_alias_index(existing_candidates)
            key_index = build_key_index(existing_candidates)
            new_candidate_count += 1

        # Update last_seen_at
        existing_candidates[cand_id].last_seen_at = target_date

        # Initialize signal tracking for this candidate
        if cand_id not in candidate_signals:
            candidate_signals[cand_id] = {}
        if cand_id not in candidate_evidence:
            candidate_evidence[cand_id] = []

    logger.info(
        "Resolved: %d active candidates (%d new)",
        len(candidate_signals),
        new_candidate_count,
    )

    # ── Step 5: Compute daily signals x(s,q,t) ──
    logger.info("Step 5: Computing daily signals...")
    for source_id, signals in all_signals.items():
        for sig in signals:
            # Find candidate_id for this signal
            norm_name = normalize_name(sig.candidate_name)
            cand_id = resolve_candidate(
                norm_name,
                CandidateType.KEYWORD,  # Default type for signal matching
                existing_candidates,
                alias_index,
                key_index,
            )
            if cand_id is None:
                continue

            if cand_id not in candidate_signals:
                candidate_signals[cand_id] = {}

            # Aggregate signals by source (sum if multiple)
            current = candidate_signals[cand_id].get(source_id, 0.0)
            candidate_signals[cand_id][source_id] = current + sig.signal_value

            # Collect evidence
            if sig.evidence and cand_id not in candidate_evidence:
                candidate_evidence[cand_id] = []
            if sig.evidence:
                candidate_evidence[cand_id].append({
                    "source_id": source_id,
                    "title": sig.evidence.title,
                    "url": sig.evidence.url,
                    "published_at": sig.evidence.published_at,
                    "metric": sig.evidence.metric,
                    "snippet": sig.evidence.snippet,
                    "signal_value": sig.signal_value,
                })

    # ── Step 6: EWMA/EWMVar update -> sig -> momentum ──
    logger.info("Step 6: Updating EWMA states and computing sig...")
    # sig_history: cand_id -> {source_id -> [sig_t, sig_{t-1}, sig_{t-2}]}
    sig_by_source: dict[str, dict[str, list[float]]] = {}
    source_momentum_scores: dict[str, list[tuple[str, float]]] = {}

    # Collect all OK sources for 0-observation injection
    ok_sources = [sid for sid, ok in source_ok.items() if ok]

    # Process all candidates that have signals OR have existing state
    active_candidate_ids = set(candidate_signals.keys()) | set(existing_candidates.keys())

    for cand_id in active_candidate_ids:
        candidate = existing_candidates.get(cand_id)
        if candidate is None:
            continue

        sig_by_source[cand_id] = {}

        for source_id in ok_sources:
            # x=0 if source succeeded but candidate had no signal
            x_value = candidate_signals.get(cand_id, {}).get(source_id, 0.0)

            # Only update sources where this candidate has existing state
            # OR where it appeared today (to avoid creating state for every
            # candidate x source combination)
            has_existing_state = source_id in candidate.source_state
            has_signal_today = (
                cand_id in candidate_signals
                and source_id in candidate_signals[cand_id]
            )
            if not has_existing_state and not has_signal_today:
                continue

            # Get or create source state
            state = candidate.source_state.get(source_id)
            if state is None:
                from packages.core.models import SourceState
                state = SourceState()

            # Update state (x=0.0 is a valid observation)
            updated_state, sig_value = update_source_state(
                state, x_value, algo_config, target_date
            )
            candidate.source_state[source_id] = updated_state

            # Build sig history: [sig_t, sig_{t-1}, sig_{t-2}]
            prev_sigs: list[float] = updated_state.sig_history[:2]
            sig_history_list: list[float] = [sig_value] + prev_sigs
            sig_by_source[cand_id][source_id] = sig_history_list

            # Update per-source sig history (keep last 3)
            updated_state.sig_history = sig_history_list[:3]

            source_mom = momentum(sig_history_list, algo_config.momentum_lambda)
            if source_mom > 0:
                source_momentum_scores.setdefault(source_id, []).append(
                    (cand_id, source_mom)
                )

        # For failed sources (ok=False), skip update entirely (x=None)
        # -> state is preserved as-is from previous day

    # ── Step 7: Aggregate to buckets + multiBonus ──
    logger.info("Step 7: Computing TrendScores...")
    candidate_score_list: list[dict[str, Any]] = []
    weighted_source_ids = filter_weighted_source_ids(
        [connector.source_id for connector in connectors]
    )
    source_weights: dict[str, float] | None = None
    if source_weighting_config.enabled and weighted_source_ids:
        try:
            source_weights = load_current_source_weights(
                target_date=target_date,
                source_cfgs=source_cfg_map,
                source_ids=weighted_source_ids,
                algo_cfg=algo_config,
                weighting_cfg=source_weighting_config,
            )
            logger.info("Loaded source weights for %d sources", len(source_weights))
        except Exception as e:
            logger.warning("Failed to load source weights, using legacy fallback: %s", e)
            errors.append(f"source_weights_current: {e}")
            source_weights = None

    for cand_id, source_sigs in sig_by_source.items():
        candidate = existing_candidates.get(cand_id)
        if candidate is None:
            continue

        score, breakdown, multi_bonus = compute_candidate_score(
            source_sigs,
            algo_config,
            music_config,
            source_weights=source_weights,
        )

        # Update trend history (keep last 7 days)
        candidate.trend_history_7d.append(score)
        if len(candidate.trend_history_7d) > 7:
            candidate.trend_history_7d = candidate.trend_history_7d[-7:]

        candidate_score_list.append({
            "candidate_id": cand_id,
            "trend_score": score,
            "breakdown": breakdown,
            "multi_bonus": multi_bonus,
            "candidate": candidate,
        })

    generated_at = datetime.now(JST).isoformat()
    source_daily_snapshots = build_source_daily_snapshots(
        target_date=target_date,
        generated_at=generated_at,
        source_ids=weighted_source_ids,
        source_ok=source_ok,
        source_item_count=source_item_count,
        source_momentum=source_momentum_scores,
        source_cfgs=source_cfg_map,
        weighting_cfg=source_weighting_config,
    )
    next_source_weight_snapshot: SourceWeightSnapshot | None = None
    if source_weighting_config.enabled and weighted_source_ids:
        try:
            history_map = load_source_daily(target_date, source_weighting_config)
            for snapshot in source_daily_snapshots:
                history_map[(snapshot.date, snapshot.source_id)] = snapshot
            next_source_weight_snapshot = compute_weight_snapshot(
                target_date=target_date,
                generated_at=generated_at,
                source_cfgs=source_cfg_map,
                source_ids=weighted_source_ids,
                algo_cfg=algo_config,
                weighting_cfg=source_weighting_config,
                source_daily_records=list(history_map.values()),
            )
        except Exception as e:
            logger.warning("Failed to compute next source weights: %s", e)
            errors.append(f"source_weight_compute: {e}")

    # ── Step 8: Preliminary TopM (wider pool for wiki enrichment) ──
    from packages.core.ranking import compute_final_score
    preliminary_top_m = min(80, len(candidate_score_list))
    logger.info(
        "Step 8: Preliminary Top %d (from %d)...",
        preliminary_top_m, len(candidate_score_list),
    )
    preliminary_candidates = select_top_k(candidate_score_list, top_k=preliminary_top_m)

    # ── Step 9: X Search enrichment + Wikipedia power + EvidenceTop3 ──
    logger.info("Step 9: Enriching evidence (X search: %s)...", degrade.x_search_enabled)
    x_search_calls = 0

    if degrade.x_search_enabled:
        from packages.connectors.x_search import XSearchConnector
        x_connector = XSearchConnector()
        x_limit = min(degrade.x_search_max, len(preliminary_candidates))

        for entry in preliminary_candidates[:x_limit]:
            cand = entry["candidate"]
            try:
                x_evidence = x_connector.search_candidate(cand.display_name)
                x_search_calls += 1
                # Add X evidence to the pool
                cand_id = entry["candidate_id"]
                for ev in x_evidence:
                    if cand_id not in candidate_evidence:
                        candidate_evidence[cand_id] = []
                    candidate_evidence[cand_id].append({
                        "source_id": ev.source_id,
                        "title": ev.title,
                        "url": ev.url,
                        "metric": ev.metric,
                        "snippet": ev.snippet,
                        "signal_value": 1.0,
                    })
            except Exception as e:
                logger.warning("X Search failed for %s: %s", cand.display_name, e)

    # Wikipedia power score (free API, no key needed)
    logger.info("Step 9b: Fetching Wikipedia power scores...")
    try:
        from packages.connectors.wikipedia import WikipediaConnector
        wiki = WikipediaConnector()
        # Date range: last 7 days
        end_dt = datetime.strptime(target_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=7)
        wiki_start = start_dt.strftime("%Y%m%d")
        wiki_end = end_dt.strftime("%Y%m%d")

        for entry in preliminary_candidates:
            cand = entry["candidate"]
            wiki_title = cand.display_name.replace(" ", "_")
            try:
                pv = wiki.fetch_pageviews(wiki_title, wiki_start, wiki_end)
                if pv is not None and pv > 0:
                    entry["power"] = wiki.compute_power_score(pv)
                    logger.info("  Wiki: %s -> %d PV (power=%.2f)",
                                cand.display_name, pv, entry["power"])
            except Exception as e:
                logger.debug("Wiki failed for %s: %s", cand.display_name, e)
    except Exception as e:
        logger.warning("Wikipedia connector failed: %s", e)

    # Compute final_score (trend + power boost) and re-rank
    compute_final_score(preliminary_candidates, power_weight=algo_config.power_weight)
    top_candidates = select_top_k(preliminary_candidates, top_k=app_config.top_k)
    logger.info("Selected %d candidates (with power boost)", len(top_candidates))

    # Build final evidence for each candidate
    for entry in top_candidates:
        cand_id = entry["candidate_id"]
        raw_ev = candidate_evidence.get(cand_id, [])
        pool = build_evidence_pool(raw_ev)
        entry["evidence_top3"] = select_evidence_top3(pool)

    # ── Step 10: Generate summary ──
    logger.info("Step 10: Generating summaries (mode: %s)...", degrade.summary_mode)
    llm_summary_calls = 0
    llm_client = None
    if degrade.summary_mode == "LLM":
        from packages.core.llm_client import LLMClient
        llm_client = LLMClient()

    for entry in top_candidates:
        cand = entry["candidate"]
        entry["summary"] = generate_summary(
            candidate_name=cand.display_name,
            trend_score=entry["trend_score"],
            breakdown=entry["breakdown"],
            evidence=entry.get("evidence_top3", []),
            mode=degrade.summary_mode,
            llm_client=llm_client,
        )
        if degrade.summary_mode == "LLM" and llm_client and llm_client.available:
            llm_summary_calls += 1

    # ── Step 11: Write to Firestore (status flag pattern) ──
    logger.info("Step 11: Writing results to Firestore...")
    run_meta_collection = f"daily_rankings/{target_date}/runs"

    try:
        # Build DailyRankingItems
        ranking_items: list[DailyRankingItem] = []
        for rank, entry in enumerate(top_candidates, start=1):
            cand = entry["candidate"]
            item = DailyRankingItem(
                rank=rank,
                candidate_id=entry["candidate_id"],
                candidate_type=cand.type.value,
                display_name=cand.display_name,
                trend_score=entry["trend_score"],
                breakdown_buckets=entry["breakdown"],
                evidence_top3=entry.get("evidence_top3", []),
                summary=entry.get("summary", ""),
                sparkline_7d=cand.trend_history_7d[-7:],
                power=entry.get("power"),
            )
            ranking_items.append(item)

        from packages.core import firestore_client

        source_daily_ops: list[tuple[str, str, dict[str, Any]]] = []
        for snapshot in source_daily_snapshots:
            source_daily_ops.append((
                "source_daily",
                snapshot.document_id,
                snapshot.to_dict(),
            ))
        if source_daily_ops:
            firestore_client.batch_write(source_daily_ops)

        if next_source_weight_snapshot is not None:
            firestore_client.set_document(
                "source_weights",
                next_source_weight_snapshot.date,
                next_source_weight_snapshot.to_dict(),
            )
            firestore_client.set_document(
                "config",
                "source_weights_current",
                next_source_weight_snapshot.to_dict(),
            )

        root_items_collection = f"daily_rankings/{target_date}/items"
        run_items_collection = f"daily_rankings/{target_date}/runs/{run_id}/items"

        # Phase 1: Write metadata with status=BUILDING
        meta = DailyRankingMeta(
            date=target_date,
            generated_at=generated_at,
            run_id=run_id,
            top_k=app_config.top_k,
            degrade_state=degrade.to_dict(),  # type: ignore[arg-type]
            music_weights=music_config.weights,
            status="BUILDING",
        )
        firestore_client.set_document(
            "daily_rankings", target_date, meta.to_dict()
        )
        firestore_client.set_document(
            run_meta_collection, run_id, meta.to_dict()
        )

        # Clear stale items from a previous rerun before writing the new set.
        deleted_items = firestore_client.delete_collection_documents(
            root_items_collection
        )
        if deleted_items:
            logger.info("Deleted %d stale ranking items for %s", deleted_items, target_date)

        # Phase 2: Write items as subcollection (batch write)
        item_ops: list[tuple[str, str, dict[str, Any]]] = []
        for item in ranking_items:
            item_dict = item.to_dict()
            item_ops.append((
                root_items_collection,
                item.candidate_id,
                item_dict,
            ))
            item_ops.append((
                run_items_collection,
                item.candidate_id,
                item_dict,
            ))
        if item_ops:
            firestore_client.batch_write(item_ops)

        # Phase 3: Save updated candidates
        candidate_store.save_candidates_batch(existing_candidates)

        # Phase 4: Mark as PUBLISHED (atomic switch)
        published_at = datetime.now(JST).isoformat()
        publish_fields = {
            "status": "PUBLISHED",
            "publishedAt": published_at,
            "latestPublishedRunId": run_id,
        }
        firestore_client.update_document(
            "daily_rankings", target_date, publish_fields
        )
        firestore_client.update_document(
            run_meta_collection,
            run_id,
            {
                "status": "PUBLISHED",
                "publishedAt": published_at,
                "latestPublishedRunId": run_id,
            },
        )
        published_successfully = True

        logger.info("Written %d ranking items to Firestore (PUBLISHED)", len(ranking_items))

    except Exception as e:
        error_msg = f"Firestore write failed: {e}"
        errors.append(error_msg)
        logger.error(error_msg)
        # Mark as FAILED if metadata was already written
        try:
            from packages.core import firestore_client as fc
            fc.update_document("daily_rankings", target_date, {"status": "FAILED"})
            fc.update_document(run_meta_collection, run_id, {"status": "FAILED"})
        except Exception:
            pass

    # ── Step 12: End logging + cost_logs ──
    logger.info("Step 12: Recording run completion...")
    cost_jpy = estimate_run_cost(sources_used, x_search_calls, llm_summary_calls)

    try:
        record_run_cost(run_id, target_date, cost_jpy, {
            "sources": sources_used,
            "xSearchCalls": x_search_calls,
            "llmSummaryCalls": llm_summary_calls,
        })
    except Exception as e:
        logger.warning("Failed to record cost: %s", e)

    if published_successfully and not errors:
        run_status = "SUCCESS"
    elif published_successfully:
        run_status = "PARTIAL"
    else:
        run_status = "FAILED"
    try:
        end_run(
            run_id,
            status=run_status,
            candidate_count=len(existing_candidates),
            top_k=len(top_candidates),
            errors=errors if errors else None,
            cost_jpy=cost_jpy,
        )
    except Exception as e:
        logger.warning("Failed to log run end: %s", e)

    # Release per-date lock (lock vocabulary: COMPLETED/FAILED only)
    try:
        release_lock(
            target_date,
            run_id,
            status="COMPLETED" if published_successfully else "FAILED",
        )
    except Exception as e:
        logger.warning("Failed to release lock: %s", e)

    # Write connector result summary for CI notification
    _write_connector_summary(source_ok, errors)

    logger.info("=== Batch Complete (%s) ===", run_status)
    logger.info(
        "Result: %d candidates scored, %d in Top-%d, cost=%.1f JPY",
        len(candidate_score_list),
        len(top_candidates),
        app_config.top_k,
        cost_jpy,
    )

    if not published_successfully:
        raise RuntimeError(f"Daily ranking publish failed for {target_date}")


def _write_connector_summary(
    source_ok: dict[str, bool], errors: list[str]
) -> None:
    """Write connector results to a JSON file for CI notification.

    The file path is read from BATCH_RESULT_PATH env var,
    defaulting to /tmp/batch_result.json.
    """
    result_path = os.environ.get("BATCH_RESULT_PATH", "/tmp/batch_result.json")
    failed = {sid: False for sid, ok in source_ok.items() if not ok}
    # Extract error messages per source
    error_map: dict[str, str] = {}
    for err in errors:
        if ":" in err:
            sid, msg = err.split(":", 1)
            sid = sid.strip()
            if sid in failed:
                error_map[sid] = msg.strip()

    summary = {
        "sources": {
            sid: {"ok": ok, "error": error_map.get(sid, "")}
            for sid, ok in source_ok.items()
        },
        "failed_sources": list(failed.keys()),
    }
    try:
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        logger.info("Connector summary written to %s", result_path)
    except Exception as e:
        logger.warning("Failed to write connector summary: %s", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trends daily batch")
    parser.add_argument(
        "--date",
        default="today",
        help="Target date (YYYY-MM-DD or 'today')",
    )
    args = parser.parse_args()
    main(date_arg=args.date)
