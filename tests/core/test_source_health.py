from __future__ import annotations

from packages.core.source_health import build_source_health_records, classify_source_failure


def test_build_source_health_records_includes_latency_fallback_and_failure_class() -> None:
    records = build_source_health_records(
        "2026-03-07",
        {"YAHOO_REALTIME": True, "ZOZO_RANKING": False},
        {"YAHOO_REALTIME": 12, "ZOZO_RANKING": 0},
        source_kept_count={"YAHOO_REALTIME": 7, "ZOZO_RANKING": 0},
        errors={"ZOZO_RANKING": "Read timeout"},
        availability_tiers={"YAHOO_REALTIME": "public", "ZOZO_RANKING": "public"},
        fallback_used={"YAHOO_REALTIME": "theme_page"},
        response_ms={"YAHOO_REALTIME": 842},
        source_metadata={
            "YAHOO_REALTIME": {
                "httpStatus": 200,
                "responseBytes": 1024,
                "bodyHash": "abc123",
                "bodyExcerpt": "excerpt",
                "parseRawCount": 12,
                "isSoftFail": False,
            }
        },
    )

    assert records[0].to_dict()["responseMs"] == 842
    assert records[0].to_dict()["fallbackUsed"] == "theme_page"
    assert records[0].to_dict()["httpStatus"] == 200
    assert records[0].to_dict()["parseRawCount"] == 12
    assert records[1].failure_class == "timeout"


def test_classify_source_failure_distinguishes_zero_kept() -> None:
    assert (
        classify_source_failure(ok=True, raw_item_count=10, kept_item_count=0, error="")
        == "zero_kept"
    )
