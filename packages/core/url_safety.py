"""Helpers for validating external URLs before storage or rendering."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlparse, urlunparse

MAX_URL_LENGTH = 2048
ALLOWED_SCHEMES = {"http", "https"}
DISALLOWED_SCHEMES = {"javascript", "data", "file", "blob", "about"}


def sanitize_external_url(url: str) -> str:
    """Return a safe external URL or an empty string when invalid."""
    raw = str(url or "").strip()
    if not raw or len(raw) > MAX_URL_LENGTH:
        return ""

    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()
    if scheme in DISALLOWED_SCHEMES or scheme not in ALLOWED_SCHEMES:
        return ""

    if parsed.username or parsed.password:
        return ""

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname or is_private_or_local_host(hostname):
        return ""

    sanitized = parsed._replace(fragment="", netloc=parsed.netloc.strip())
    return urlunparse(sanitized)


def is_private_or_local_host(host: str) -> bool:
    normalized = host.strip().lower()
    if not normalized:
        return True
    if normalized in {"localhost", "0.0.0.0"}:
        return True
    if normalized.endswith(".local"):
        return True

    try:
        ip = ipaddress.ip_address(normalized)
    except ValueError:
        return False

    return any(
        (
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_unspecified,
            ip.is_reserved,
            ip.is_multicast,
        )
    )
