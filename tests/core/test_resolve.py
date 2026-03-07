"""Tests for candidate ID resolution."""

from __future__ import annotations

from packages.core.models import Candidate, CandidateStatus, CandidateType
from packages.core.resolve import (
    build_alias_index,
    build_external_id_index,
    build_key_index,
    create_new_candidate,
    resolve_candidate,
)


def _make_candidate(
    cand_id: str,
    name: str,
    ctype: CandidateType = CandidateType.PERSON,
    aliases: list[str] | None = None,
    status: CandidateStatus = CandidateStatus.ACTIVE,
) -> Candidate:
    """Helper to build a Candidate for testing."""
    return Candidate(
        candidate_id=cand_id,
        type=ctype,
        canonical_name=name.lower().replace(" ", ""),
        display_name=name,
        aliases=aliases or [],
        status=status,
    )


class TestResolveCandidate:
    def test_resolve_by_alias(self) -> None:
        cand = _make_candidate("C001", "YOASOBI", aliases=["ヨアソビ"])
        candidates = {"C001": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate("ヨアソビ", CandidateType.PERSON, candidates, alias_idx, key_idx)
        assert result == "C001"

    def test_resolve_by_key(self) -> None:
        cand = _make_candidate("C002", "Ado")
        candidates = {"C002": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate("Ado", CandidateType.PERSON, candidates, alias_idx, key_idx)
        assert result == "C002"

    def test_resolve_new_returns_none(self) -> None:
        candidates: dict[str, Candidate] = {}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate(
            "NewArtist", CandidateType.PERSON, candidates, alias_idx, key_idx
        )
        assert result is None

    def test_blocked_candidate_not_resolved(self) -> None:
        cand = _make_candidate("C003", "Blocked", status=CandidateStatus.BLOCKED)
        candidates = {"C003": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate("Blocked", CandidateType.PERSON, candidates, alias_idx, key_idx)
        assert result is None

    def test_merged_candidate_not_resolved(self) -> None:
        cand = _make_candidate("C003M", "Merged", status=CandidateStatus.MERGED)
        candidates = {"C003M": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate("Merged", CandidateType.PERSON, candidates, alias_idx, key_idx)
        assert result is None

    def test_resolve_by_external_id_first(self) -> None:
        cand = _make_candidate("C004", "Mrs. GREEN APPLE", CandidateType.MUSIC_ARTIST)
        cand.external_ids = {"apple_music_artist": "am-123"}
        candidates = {"C004": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate(
            "Mrs. GREEN APPLE",
            CandidateType.MUSIC_ARTIST,
            candidates,
            alias_idx,
            key_idx,
            external_ids={"apple_music_artist": "am-123"},
        )

        assert result == "C004"

    def test_manual_lock_blocks_unlisted_surface(self) -> None:
        cand = Candidate(
            candidate_id="C005",
            type=CandidateType.GROUP,
            canonical_name="Snow Man",
            display_name="Snow Man",
            aliases=[],
            status=CandidateStatus.ACTIVE,
        )
        cand.manual_lock = True
        candidates = {"C005": cand}
        alias_idx = build_alias_index(candidates)
        key_idx = build_key_index(candidates)

        result = resolve_candidate(
            "SnowMan",
            CandidateType.GROUP,
            candidates,
            alias_idx,
            key_idx,
        )

        assert result is None


class TestBuildAliasIndex:
    def test_includes_canonical_and_aliases(self) -> None:
        cand = _make_candidate("C001", "YOASOBI", aliases=["ヨアソビ", "夜遊び"])
        idx = build_alias_index({"C001": cand})

        assert "yoasobi" in idx
        assert "ヨアソビ" in idx or "よあそび" in idx
        assert idx["yoasobi"] == "C001"

    def test_blocked_excluded(self) -> None:
        cand = _make_candidate("C001", "Blocked", status=CandidateStatus.BLOCKED)
        idx = build_alias_index({"C001": cand})
        assert len(idx) == 0

    def test_merged_excluded(self) -> None:
        cand = _make_candidate("C001", "Merged", status=CandidateStatus.MERGED)
        idx = build_alias_index({"C001": cand})
        assert len(idx) == 0


class TestCreateNewCandidate:
    def test_creates_with_correct_fields(self) -> None:
        cand = create_new_candidate("YOASOBI", CandidateType.PERSON, "C-NEW-001")
        assert cand.candidate_id == "C-NEW-001"
        assert cand.type == CandidateType.PERSON
        assert cand.display_name == "YOASOBI"
        assert cand.status == CandidateStatus.ACTIVE
        assert cand.created_at != ""

    def test_creates_with_aliases(self) -> None:
        cand = create_new_candidate(
            "timelesz", CandidateType.GROUP, "C-NEW-002", aliases=["タイムレス"]
        )
        assert cand.aliases == ["タイムレス"]


class TestBuildExternalIdIndex:
    def test_builds_provider_scoped_external_id_index(self) -> None:
        cand = _make_candidate("C006", "Ado", CandidateType.MUSIC_ARTIST)
        cand.external_ids = {"youtube_channel": "yt-1"}

        index = build_external_id_index({"C006": cand})

        assert index == {"youtube_channel:yt-1": "C006"}
