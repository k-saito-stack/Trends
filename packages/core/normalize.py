"""Candidate name normalization.

Low-risk operations only:
- Unicode NFKC normalization
- Whitespace trim/compression
- Minor symbol removal (trailing !, ? etc.)
- Bracket alias extraction: "timelesz（タイムレス）" -> ("timelesz", ["タイムレス"])

Spec reference: Section 9 (Candidate Model - Normalize)
"""

from __future__ import annotations

import re
import unicodedata


def normalize_name(name: str) -> str:
    """Normalize a candidate name.

    Steps:
    1. Unicode NFKC normalization (full-width -> half-width etc.)
    2. Strip leading/trailing whitespace
    3. Compress consecutive whitespace
    4. Remove trailing punctuation (!, ?, 。, etc.)
    """
    # NFKC: converts full-width characters, compatibility forms
    text = unicodedata.normalize("NFKC", name)
    # Strip
    text = text.strip()
    # Compress whitespace
    text = re.sub(r"\s+", " ", text)
    # Remove trailing punctuation
    text = re.sub(r"[!?！？。、…]+$", "", text)
    text = text.strip()
    return text


def extract_bracket_aliases(name: str) -> tuple[str, list[str]]:
    """Extract aliases from bracket notation.

    Examples:
        "timelesz（タイムレス）" -> ("timelesz", ["タイムレス"])
        "YOASOBI (ヨアソビ)"   -> ("YOASOBI", ["ヨアソビ"])
        "plain name"            -> ("plain name", [])

    Supports both full-width and half-width brackets.
    """
    aliases: list[str] = []

    # Match full-width brackets（...）
    fw_pattern = r"[（\(]([^）\)]+)[）\)]"
    matches = re.findall(fw_pattern, name)
    for match in matches:
        alias = match.strip()
        if alias:
            aliases.append(alias)

    # Remove bracket portions to get canonical name
    canonical = re.sub(fw_pattern, "", name).strip()
    canonical = normalize_name(canonical)

    return canonical, aliases


def normalize_for_matching(name: str) -> str:
    """Produce a normalized key suitable for matching/dedup.

    More aggressive normalization for comparison:
    - Lowercase
    - Remove all whitespace
    - Remove common punctuation
    """
    text = normalize_name(name)
    text = text.lower()
    text = re.sub(r"[\s\-_・:：/／]", "", text)
    return text
