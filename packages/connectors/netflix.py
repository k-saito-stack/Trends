"""Netflix Top 10 connector (Japan: Films & TV).

Scrapes the Netflix Tudum Top 10 page for Japan.
Weekly updated rankings (every Tuesday).

Pages:
- https://www.netflix.com/tudum/top10/japan/films
- https://www.netflix.com/tudum/top10/japan/tv
"""

from __future__ import annotations

import logging
import math
from html.parser import HTMLParser
from typing import Any

import requests

from packages.connectors.base import BaseConnector, FetchResult, SignalResult
from packages.core.models import CandidateType, Evidence, RawCandidate
from packages.core.ner import extract_entities

logger = logging.getLogger(__name__)

NETFLIX_TOP10_URLS = {
    "films": "https://www.netflix.com/tudum/top10/japan/films",
    "tv": "https://www.netflix.com/tudum/top10/japan/tv",
}


class _Top10TableParser(HTMLParser):
    """Parse Netflix Top 10 HTML table to extract rank + title pairs."""

    def __init__(self) -> None:
        super().__init__()
        self.items: list[dict[str, Any]] = []
        self._in_rank = False
        self._in_title = False
        self._current_rank = ""
        self._current_title = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        cls = attr_dict.get("class") or ""
        if "rank" in cls.split():
            self._in_rank = True
            self._current_rank = ""
        elif "title" in cls.split():
            self._in_title = True
            self._current_title = ""

    def handle_endtag(self, tag: str) -> None:
        if self._in_rank and tag in ("td", "div", "span"):
            self._in_rank = False
        if self._in_title and tag == "td":
            self._in_title = False
            rank_str = self._current_rank.strip()
            title = self._current_title.strip()
            if rank_str and title:
                try:
                    rank = int(rank_str)
                except ValueError:
                    rank = len(self.items) + 1
                self.items.append({"rank": rank, "title": title})

    def handle_data(self, data: str) -> None:
        if self._in_rank:
            self._current_rank += data
        elif self._in_title:
            self._current_title += data


def parse_top10_html(html: str) -> list[dict[str, Any]]:
    """Extract Top 10 items from Netflix Tudum HTML."""
    parser = _Top10TableParser()
    parser.feed(html)
    return parser.items


class NetflixTop10Connector(BaseConnector):
    """Connector for Netflix Top 10 Japan (one category per instance)."""

    def __init__(
        self,
        category: str = "tv",
        max_results: int = 10,
        **kwargs: Any,
    ) -> None:
        cat_label = "TV" if category == "tv" else "FILMS"
        source_id = f"NETFLIX_{cat_label}_JP"
        super().__init__(source_id=source_id, stability="B", **kwargs)
        self.category = category
        self.max_results = max_results
        self.page_url = NETFLIX_TOP10_URLS.get(category, NETFLIX_TOP10_URLS["tv"])

    def fetch(self) -> FetchResult:
        """Fetch and parse the Netflix Top 10 page."""
        try:
            resp = requests.get(
                self.page_url,
                timeout=30,
                headers={"Accept-Language": "ja,en;q=0.5"},
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            return FetchResult(error=str(e))

        items = parse_top10_html(resp.text)
        items = items[: self.max_results]
        return FetchResult(items=items, item_count=len(items))

    def extract_candidates(self, items: list[dict[str, Any]]) -> list[RawCandidate]:
        """Extract candidates from Netflix Top 10 items.

        Each title is added as a WORK candidate.
        NER is run on the title for additional person/group extraction.
        """
        candidates: list[RawCandidate] = []

        for item in items:
            rank = item.get("rank", 0)
            title = item.get("title", "")
            if not title:
                continue

            evidence = Evidence(
                source_id=self.source_id,
                title=title,
                url=self.page_url,
                metric=f"rank:{rank}",
            )

            # Title as WORK candidate
            candidates.append(RawCandidate(
                name=title,
                type=CandidateType.WORK,
                source_id=self.source_id,
                rank=rank,
                metric_value=_rank_exposure(rank),
                evidence=evidence,
                extra={"category": self.category},
            ))

            # NER on title for person/group extraction
            entities = extract_entities(title, max_entities=3)
            for ent_text, ent_type in entities:
                if ent_text == title:
                    continue
                try:
                    cand_type = CandidateType(ent_type)
                except ValueError:
                    cand_type = CandidateType.KEYWORD
                candidates.append(RawCandidate(
                    name=ent_text,
                    type=cand_type,
                    source_id=self.source_id,
                    rank=rank,
                    metric_value=_rank_exposure(rank),
                    evidence=evidence,
                    extra={"category": self.category, "from_title": title},
                ))

        return candidates

    def compute_signals(
        self, items: list[dict[str, Any]], candidates: list[RawCandidate]
    ) -> list[SignalResult]:
        """Compute daily signal using rank exposure."""
        signals: dict[str, SignalResult] = {}

        for cand in candidates:
            key = cand.name
            if key in signals:
                signals[key].signal_value += cand.metric_value
            else:
                signals[key] = SignalResult(
                    candidate_name=key,
                    signal_value=cand.metric_value,
                    evidence=cand.evidence,
                )

        return list(signals.values())


def _rank_exposure(rank: int) -> float:
    """E(rank) = 1 / log2(rank + 1)"""
    return 1.0 / math.log2(rank + 1)
