"""Shared fetch helpers for HTML-based connectors."""

from __future__ import annotations

from hashlib import sha1
from typing import Any

import requests


def build_fetch_metadata(
    response: requests.Response,
    *,
    url: str,
    fallback_used: str = "",
    body_excerpt_limit: int = 512,
) -> dict[str, Any]:
    body = _response_text(response)
    metadata: dict[str, Any] = {
        "url": url,
        "httpStatus": int(getattr(response, "status_code", 0) or 0),
        "responseBytes": len(body.encode("utf-8", errors="ignore")),
        "bodyHash": sha1(body.encode("utf-8", errors="ignore")).hexdigest()[:16],
        "bodyExcerpt": body[:body_excerpt_limit],
    }
    if fallback_used:
        metadata["fallbackUsed"] = fallback_used
    return metadata


def mark_parse_counts(metadata: dict[str, Any], *, parse_raw_count: int) -> dict[str, Any]:
    updated = dict(metadata)
    updated["parseRawCount"] = int(parse_raw_count)
    return updated


def mark_soft_fail(
    metadata: dict[str, Any],
    *,
    error_type: str,
) -> dict[str, Any]:
    updated = dict(metadata)
    updated["errorType"] = error_type
    updated["isSoftFail"] = True
    return updated


def _response_text(response: requests.Response) -> str:
    try:
        return response.text or ""
    except Exception:
        content = getattr(response, "content", b"")
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="ignore")
        return str(content or "")
