from __future__ import annotations

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate


class _SignalFailConnector(BaseConnector):
    def __init__(self) -> None:
        super().__init__(source_id="TEST_SIGNAL_FAIL")

    def fetch(self) -> FetchResult:
        return FetchResult(items=[{"name": "test"}], item_count=1)

    def extract_candidates(self, items: list[dict[str, object]]) -> list[RawCandidate]:
        return [
            RawCandidate(
                name="test",
                type=CandidateType.KEYWORD,
                source_id=self.source_id,
                evidence=Evidence(source_id=self.source_id, title="test", url=""),
            )
        ]

    def compute_signals(
        self,
        items: list[dict[str, object]],
        candidates: list[RawCandidate],
    ) -> list[SignalResult]:
        raise RuntimeError("boom")


def test_signal_failure_is_not_reported_as_success() -> None:
    connector = _SignalFailConnector()
    result = connector.run()

    assert result.ok is False
    assert result.error == "signal: boom"
    assert result.metadata["failureStage"] == "signal"
