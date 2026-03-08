from __future__ import annotations

from packages.core.url_safety import is_private_or_local_host, sanitize_external_url


def test_sanitize_external_url_rejects_javascript_scheme() -> None:
    assert sanitize_external_url("javascript:alert(1)") == ""


def test_sanitize_external_url_rejects_private_host() -> None:
    assert sanitize_external_url("https://127.0.0.1/admin") == ""
    assert sanitize_external_url("https://localhost:8080/health") == ""


def test_sanitize_external_url_allows_normal_https() -> None:
    assert sanitize_external_url("https://example.com/path#fragment") == "https://example.com/path"


def test_is_private_or_local_host_detects_rfc1918_ranges() -> None:
    assert is_private_or_local_host("10.0.0.8") is True
    assert is_private_or_local_host("172.20.10.3") is True
    assert is_private_or_local_host("192.168.1.5") is True
    assert is_private_or_local_host("example.com") is False
