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
  8) Select Top15
  9) Select EvidenceTop3
  10) Generate summary
  11) Write to Firestore
  12) End logging + cost_logs

Spec reference: Section 13 (Daily Batch Runbook)
"""

from __future__ import annotations

import argparse
import contextlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ulid import ULID

from batch.cost_tracker import estimate_run_cost, record_run_cost
from batch.degrade import DegradeState, compute_degrade_state
from packages.connectors.apple_music import AppleMusicConnector
from packages.connectors.base import BaseConnector, SignalResult
from packages.connectors.google_trends import GoogleTrendsConnector
from packages.connectors.rakuten_magazine import RakutenMagazineConnector
from packages.connectors.rss_feeds import RSSFeedConnector
from packages.connectors.youtube import YouTubeConnector
from packages.core import candidate_store
from packages.core.config import load_algorithm_config, load_app_config, load_music_config
from packages.core.evidence import build_evidence_pool, select_evidence_top3
from packages.core.models import (
    CandidateType,
    DailyRankingItem,
    DailyRankingMeta,
    RawCandidate,
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
from packages.core.scoring import update_source_state
from packages.core.summary import generate_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def get_target_date(date_arg: str) -> str:
    """Parse the --date argument and return YYYY-MM-DD in JST."""
    if date_arg == "today":
        return datetime.now(JST).strftime("%Y-%m-%d")
    return date_arg


def _create_connectors() -> list[BaseConnector]:
    """Create all active connector instances."""
    return [
        YouTubeConnector(),
        AppleMusicConnector(region="JP"),
        AppleMusicConnector(region="GLOBAL"),
        GoogleTrendsConnector(),
        RSSFeedConnector(),
        RakutenMagazineConnector(),
    ]


def main(date_arg: str = "today") -> None:
    """Run the daily batch pipeline."""
    target_date = get_target_date(date_arg)
    run_id = str(ULID())
    errors: list[str] = []

    logger.info("=== Trends Daily Batch ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Target date: %s", target_date)

    # ── Step 0: Load config + candidates ──
    logger.info("Step 0: Loading config and candidates...")
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
    connectors = _create_connectors()
    all_raw_candidates: list[RawCandidate] = []
    all_signals: dict[str, list[SignalResult]] = {}  # source_id -> signals
    sources_used: list[str] = []

    for connector in connectors:
        source_id = connector.source_id
        logger.info("  Fetching: %s", source_id)
        try:
            raw_cands, signals = connector.run()
            sources_used.append(source_id)
            all_raw_candidates.extend(raw_cands)
            all_signals[source_id] = signals

            # Log source result
            with contextlib.suppress(Exception):
                update_run_source(run_id, source_id, len(raw_cands))

        except Exception as e:
            error_msg = f"{source_id}: {e}"
            errors.append(error_msg)
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

    for cand_id, source_signals in candidate_signals.items():
        candidate = existing_candidates.get(cand_id)
        if candidate is None:
            continue

        sig_by_source[cand_id] = {}

        for source_id, x_value in source_signals.items():
            # Get or create source state
            state = candidate.source_state.get(source_id)
            if state is None:
                from packages.core.models import SourceState
                state = SourceState()

            # Update state
            updated_state, sig_value = update_source_state(
                state, x_value, algo_config, target_date
            )
            candidate.source_state[source_id] = updated_state

            # Build sig history: [sig_t, sig_{t-1}, sig_{t-2}]
            # sig_{t-1} and sig_{t-2} come from stored history
            prev_sigs: list[float] = (
                candidate.trend_history_7d[-2:] if candidate.trend_history_7d else []
            )
            sig_history: list[float] = [sig_value] + prev_sigs
            sig_by_source[cand_id][source_id] = sig_history

    # ── Step 7: Aggregate to buckets + multiBonus ──
    logger.info("Step 7: Computing TrendScores...")
    candidate_score_list: list[dict[str, Any]] = []

    for cand_id, source_sigs in sig_by_source.items():
        candidate = existing_candidates.get(cand_id)
        if candidate is None:
            continue

        score, breakdown, multi_bonus = compute_candidate_score(
            source_sigs, algo_config, music_config
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

    # ── Step 8: Select Top15 ──
    logger.info("Step 8: Selecting Top %d...", app_config.top_k)
    top_candidates = select_top_k(candidate_score_list, top_k=app_config.top_k)
    logger.info("Selected %d candidates", len(top_candidates))

    # ── Step 9: Select EvidenceTop3 ──
    logger.info("Step 9: Selecting evidence...")
    for entry in top_candidates:
        cand_id = entry["candidate_id"]
        raw_ev = candidate_evidence.get(cand_id, [])
        pool = build_evidence_pool(raw_ev)
        entry["evidence_top3"] = select_evidence_top3(pool)

    # ── Step 10: Generate summary ──
    logger.info("Step 10: Generating summaries (mode: %s)...", degrade.summary_mode)
    for entry in top_candidates:
        cand = entry["candidate"]
        entry["summary"] = generate_summary(
            candidate_name=cand.display_name,
            trend_score=entry["trend_score"],
            breakdown=entry["breakdown"],
            evidence=entry.get("evidence_top3", []),
            mode=degrade.summary_mode,
        )

    # ── Step 11: Write to Firestore ──
    logger.info("Step 11: Writing results to Firestore...")
    x_search_calls = 0
    llm_summary_calls = 0
    if degrade.summary_mode == "LLM":
        llm_summary_calls = len(top_candidates)

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
            )
            ranking_items.append(item)

        # Write metadata
        from packages.core import firestore_client
        meta = DailyRankingMeta(
            date=target_date,
            generated_at=datetime.now(JST).isoformat(),
            run_id=run_id,
            top_k=app_config.top_k,
            degrade_state=degrade.to_dict(),  # type: ignore[arg-type]
            music_weights=music_config.weights,
        )
        firestore_client.set_document(
            "daily_rankings", target_date, meta.to_dict()
        )

        # Write items as subcollection
        for item in ranking_items:
            firestore_client.set_subcollection_document(
                "daily_rankings", target_date,
                "items", item.candidate_id,
                item.to_dict(),
            )

        # Save updated candidates
        candidate_store.save_candidates_batch(existing_candidates)

        logger.info("Written %d ranking items to Firestore", len(ranking_items))

    except Exception as e:
        error_msg = f"Firestore write failed: {e}"
        errors.append(error_msg)
        logger.error(error_msg)

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

    status = "SUCCESS" if not errors else "PARTIAL"
    try:
        end_run(
            run_id,
            status=status,
            candidate_count=len(existing_candidates),
            top_k=len(top_candidates),
            errors=errors if errors else None,
            cost_jpy=cost_jpy,
        )
    except Exception as e:
        logger.warning("Failed to log run end: %s", e)

    logger.info("=== Batch Complete (%s) ===", status)
    logger.info(
        "Result: %d candidates scored, %d in Top-%d, cost=%.1f JPY",
        len(candidate_score_list),
        len(top_candidates),
        app_config.top_k,
        cost_jpy,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trends daily batch")
    parser.add_argument(
        "--date",
        default="today",
        help="Target date (YYYY-MM-DD or 'today')",
    )
    args = parser.parse_args()
    main(date_arg=args.date)
