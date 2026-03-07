"""Alias registry utilities for entity/topic resolution."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from packages.core import firestore_client
from packages.core.models import AliasProvenance, Candidate, CandidateKind, CandidateStatus
from packages.core.normalize import normalize_for_matching
from packages.core.topic_normalize import topic_match_key


@dataclass
class AliasRecord:
    alias_id: str
    candidate_id: str
    candidate_kind: CandidateKind
    alias: str
    match_key: str
    provenance: AliasProvenance

    def to_dict(self) -> dict[str, str]:
        return {
            "aliasId": self.alias_id,
            "candidateId": self.candidate_id,
            "candidateKind": self.candidate_kind.value,
            "alias": self.alias,
            "matchKey": self.match_key,
            "provenance": self.provenance.value,
        }


def build_alias_records(candidates: Iterable[Candidate]) -> list[AliasRecord]:
    records: list[AliasRecord] = []
    for candidate in candidates:
        if candidate.status != CandidateStatus.ACTIVE:
            continue
        for alias in [candidate.canonical_name, *candidate.aliases]:
            match_key = (
                topic_match_key(alias)
                if candidate.kind == CandidateKind.TOPIC
                else normalize_for_matching(alias)
            )
            alias_id = f"{candidate.candidate_id}:{match_key}"
            records.append(
                AliasRecord(
                    alias_id=alias_id,
                    candidate_id=candidate.candidate_id,
                    candidate_kind=candidate.kind or candidate.type.default_kind,
                    alias=alias,
                    match_key=match_key,
                    provenance=AliasProvenance.SOURCE_ID_LINKED,
                )
            )
    return records


def load_alias_index() -> dict[str, str]:
    docs = firestore_client.get_collection("candidate_aliases")
    return {
        str(doc.get("matchKey", "")): str(doc.get("candidateId", ""))
        for doc in docs
        if doc.get("matchKey") and doc.get("candidateId")
    }


def save_alias_records(records: Iterable[AliasRecord]) -> None:
    operations = [("candidate_aliases", record.alias_id, record.to_dict()) for record in records]
    if operations:
        firestore_client.batch_write(operations)


def delete_alias_records_for_candidate(candidate_id: str) -> int:
    docs = firestore_client.get_collection("candidate_aliases")
    alias_ids = [
        str(doc.get("aliasId", ""))
        for doc in docs
        if str(doc.get("candidateId", "")) == candidate_id and doc.get("aliasId")
    ]
    for alias_id in alias_ids:
        firestore_client.delete_document("candidate_aliases", alias_id)
    return len(alias_ids)
