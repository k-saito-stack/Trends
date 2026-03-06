"""Scoring engine: EWMA / EWMVar / sig_beta / momentum / multiBonus.

Core statistical engine for trend detection.
Implements SigniTrend-inspired significance scoring.

Spec reference: Section 10 (Scoring), Appendix D (pseudocode)
"""

from __future__ import annotations

import math

from packages.core.models import AlgorithmConfig, SourceState


def alpha_from_half_life(half_life_days: float) -> float:
    """Convert half-life (days) to EWMA decay factor alpha.

    alpha = 1 - exp(log(0.5) / half_life)

    Example: half_life=7 -> ~0.0953 (1 week for influence to halve)
    """
    if half_life_days <= 0:
        return 1.0
    return 1.0 - math.exp(math.log(0.5) / half_life_days)


def ewma_update(m_prev: float, x: float, alpha: float) -> float:
    """Update EWMA mean.

    m_t = (1 - alpha) * m_{t-1} + alpha * x_t
    """
    return (1 - alpha) * m_prev + alpha * x


def ewmvar_update(v_prev: float, x: float, m_prev: float, alpha: float) -> float:
    """Update EWMA variance (stable version).

    v_t = (1 - alpha) * (v_{t-1} + alpha * (x_t - m_{t-1})^2)
    """
    return (1 - alpha) * (v_prev + alpha * (x - m_prev) ** 2)


def sig_beta(x: float, m: float, v: float, beta: float) -> float:
    """Compute significance score.

    sig_beta(x_t) = (x_t - max(m_t, beta)) / (sqrt(v_t) + beta)

    - beta prevents division by zero and cuts noise
    - Higher sig = more above baseline = stronger "signal"
    """
    denom = math.sqrt(max(v, 0.0)) + beta
    return (x - max(m, beta)) / denom


def momentum(sig_history: list[float], lam: float = 0.7) -> float:
    """Compute momentum from recent sig values.

    momentum_t = max(0, sig_t) + lam * max(0, sig_{t-1}) + lam^2 * max(0, sig_{t-2})

    Rewards consecutive days of positive significance.
    """
    s0 = max(0.0, sig_history[0]) if len(sig_history) > 0 else 0.0
    s1 = max(0.0, sig_history[1]) if len(sig_history) > 1 else 0.0
    s2 = max(0.0, sig_history[2]) if len(sig_history) > 2 else 0.0
    return s0 + lam * s1 + (lam ** 2) * s2


def multi_source_bonus(
    active_source_count: int,
    multi_weight: float = 1.0,
) -> float:
    """Compute multi-source agreement bonus.

    multiBonus = multiWeight * clamp(activeSources - 1, 0, 3)

    Rewards candidates that show up in multiple sources simultaneously.
    """
    return multi_weight * max(0, min(active_source_count - 1, 3))


def update_source_state(
    state: SourceState,
    x: float | None,
    config: AlgorithmConfig,
    target_date: str,
) -> tuple[SourceState, float]:
    """Update EWMA state for one candidate-source pair and return sig.

    Args:
        state: Current EWMA state
        x: Today's signal value (None = missing/failed)
        config: Algorithm parameters
        target_date: Today's date string

    Returns:
        (updated_state, sig_value)
        sig=0.0 during warmup or if x is None
    """
    alpha = alpha_from_half_life(config.half_life_days)

    # Missing data: skip update, return 0
    if x is None:
        return state, 0.0

    # Clip extreme values
    x = min(x, config.max_x_clip)

    new_count = state.observation_count + 1
    new_m = ewma_update(state.m, x, alpha)
    new_v = ewmvar_update(state.v, x, state.m, alpha)

    # Warmup: learn statistics but don't produce sig
    if new_count <= config.warmup_days:
        return SourceState(
            m=new_m,
            v=new_v,
            last_sig=0.0,
            last_updated=target_date,
            observation_count=new_count,
            sig_history=state.sig_history,
        ), 0.0

    # Measure surprise against the previous baseline, then update state.
    sig = sig_beta(x, state.m, state.v, config.beta)

    return SourceState(
        m=new_m,
        v=new_v,
        last_sig=sig,
        last_updated=target_date,
        observation_count=new_count,
        sig_history=state.sig_history,
    ), sig


def rank_exposure(rank: int) -> float:
    """E(rank) = 1 / log2(rank + 1)

    Standard DCG-inspired rank weighting.
    rank=1 -> 1.0, rank=2 -> 0.63, rank=10 -> 0.29
    """
    return 1.0 / math.log2(rank + 1)
