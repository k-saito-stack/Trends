"""LLM-assisted unresolved merge judge with cache-first behavior."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

from packages.core import firestore_client
from packages.core.llm_client import LLMClient

JST = timezone(timedelta(hours=9))
DEFAULT_PROMPT_VERSION = "resolution-v1"
ALLOWED_DECISIONS = {"merge", "link", "separate", "unknown"}


def resolve_uncertain_pairs(
    pairs: list[dict[str, Any]],
    *,
    llm_client: LLMClient | None = None,
    provider_name: str | None = None,
    model: str = "",
    prompt_version: str = DEFAULT_PROMPT_VERSION,
) -> list[dict[str, Any]]:
    client = llm_client
    if client is None:
        client = LLMClient(
            provider_name=provider_name,
            model=model or "grok-4-1-fast-non-reasoning",
        )

    return [
        judge_merge_or_link(
            pair.get("left", {}),
            pair.get("right", {}),
            llm_client=client,
            prompt_version=prompt_version,
        )
        for pair in pairs
    ]


def judge_merge_or_link(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    llm_client: LLMClient | None = None,
    provider_name: str | None = None,
    model: str = "",
    prompt_version: str = DEFAULT_PROMPT_VERSION,
) -> dict[str, Any]:
    cache_key = _build_cache_key(left, right, prompt_version)
    cached = firestore_client.get_document("llm_resolution_cache", cache_key)
    if cached is not None:
        return dict(cached, cacheHit=True)

    client = llm_client
    if client is None:
        client = LLMClient(
            provider_name=provider_name,
            model=model or "grok-4-1-fast-non-reasoning",
        )

    if not client.available:
        result = _default_resolution_result(left, right, prompt_version)
        firestore_client.upsert_document("llm_resolution_cache", cache_key, result)
        return dict(result, cacheHit=False)

    response = client.chat_json(_build_messages(left, right))
    result = _normalize_llm_result(
        response,
        left,
        right,
        prompt_version,
        provider_name=client.provider_name,
        model=client.model,
    )
    firestore_client.upsert_document("llm_resolution_cache", cache_key, result)
    return dict(result, cacheHit=False)


def _build_messages(left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You are judging whether two trend candidates should be merged into one node, "
                "linked as related nodes, or kept separate. Reply with compact JSON only."
            ),
        },
        {
            "role": "user",
            "content": (
                "Decide one of merge/link/separate.\n"
                f"left={left}\n"
                f"right={right}\n"
                'Return {"decision":"merge|link|separate","confidence":0..1,"reason":"..."}'
            ),
        },
    ]


def _normalize_llm_result(
    response: dict[str, Any] | list[Any] | None,
    left: dict[str, Any],
    right: dict[str, Any],
    prompt_version: str,
    *,
    provider_name: str,
    model: str,
) -> dict[str, Any]:
    now = datetime.now(JST).isoformat()
    payload = response if isinstance(response, dict) else {}
    decision = str(payload.get("decision", "unknown")).strip().lower()
    if decision not in ALLOWED_DECISIONS:
        decision = "unknown"
    confidence_raw = payload.get("confidence", 0.0)
    try:
        confidence = max(0.0, min(1.0, float(confidence_raw)))
    except (TypeError, ValueError):
        confidence = 0.0
    reason = str(payload.get("reason", "")).strip()
    return {
        "inputSurfaces": [left.get("name", ""), right.get("name", "")],
        "decision": decision,
        "confidence": confidence,
        "provider": provider_name,
        "model": model,
        "promptVersion": prompt_version,
        "createdAt": now,
        "reason": reason,
    }


def _default_resolution_result(
    left: dict[str, Any],
    right: dict[str, Any],
    prompt_version: str,
) -> dict[str, Any]:
    now = datetime.now(JST).isoformat()
    return {
        "inputSurfaces": [left.get("name", ""), right.get("name", "")],
        "decision": "unknown",
        "confidence": 0.0,
        "provider": "none",
        "model": "",
        "promptVersion": prompt_version,
        "createdAt": now,
        "reason": "llm_unavailable",
    }


def _build_cache_key(
    left: dict[str, Any],
    right: dict[str, Any],
    prompt_version: str,
) -> str:
    ordered = sorted(
        [
            _surface_signature(left),
            _surface_signature(right),
        ]
    )
    digest = hashlib.sha1(f"{prompt_version}|{ordered[0]}|{ordered[1]}".encode()).hexdigest()
    return digest


def _surface_signature(payload: dict[str, Any]) -> str:
    return "|".join(
        [
            str(payload.get("candidateType", "")).strip(),
            str(payload.get("candidateKind", "")).strip(),
            str(payload.get("name", "")).strip().lower(),
            str(payload.get("domainClass", "")).strip(),
        ]
    )
