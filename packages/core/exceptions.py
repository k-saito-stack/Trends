"""Custom exceptions for Trends platform."""


class TrendsError(Exception):
    """Base exception for all Trends errors."""


class FetchError(TrendsError):
    """Error during data fetching from an external source."""

    def __init__(self, source_id: str, message: str) -> None:
        self.source_id = source_id
        super().__init__(f"[{source_id}] Fetch failed: {message}")


class ParseError(TrendsError):
    """Error during parsing of fetched data."""

    def __init__(self, source_id: str, message: str) -> None:
        self.source_id = source_id
        super().__init__(f"[{source_id}] Parse failed: {message}")


class ConfigError(TrendsError):
    """Error related to configuration."""
