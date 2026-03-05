"""Named Entity Recognition (NER) module using GiNZA/spaCy.

Extracts PERSON, GROUP, WORK entities from Japanese text.
Model is lazy-loaded and cached globally to avoid repeated loading.

Spec reference: Section 6.2 (NER extraction)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Global cache for spaCy model (lazy-loaded once)
_nlp: Any = None

# GiNZA NER label -> CandidateType mapping
# ref: https://github.com/megagonlabs/ginza
LABEL_TO_TYPE: dict[str, str] = {
    "Person": "PERSON",
    "PERSON": "PERSON",
    # Organization can represent groups/bands
    "Organization": "GROUP",
    "ORG": "GROUP",
    "ORGANIZATION": "GROUP",
    # Product/Work_Of_Art for creative works
    "Product": "WORK",
    "PRODUCT": "WORK",
    "Work_Of_Art": "WORK",
    "WORK_OF_ART": "WORK",
    # GPE/Location -> skip (not relevant for trend detection)
    # Event -> could be KEYWORD
    "Event": "KEYWORD",
    "EVENT": "KEYWORD",
}


def _load_model() -> Any:
    """Lazy-load the GiNZA spaCy model (cached globally)."""
    global _nlp
    if _nlp is not None:
        return _nlp

    try:
        import spacy
        _nlp = spacy.load("ja_ginza")
        logger.info("GiNZA model loaded successfully")
    except (ImportError, OSError) as e:
        logger.warning("GiNZA/spaCy not available: %s (NER will be skipped)", e)
        _nlp = None

    return _nlp


def extract_entities(
    text: str,
    max_entities: int = 10,
) -> list[tuple[str, str]]:
    """Extract named entities from Japanese text.

    Args:
        text: Input text (title, headline, etc.)
        max_entities: Maximum number of entities to return

    Returns:
        List of (entity_text, candidate_type) tuples.
        candidate_type is one of: PERSON, GROUP, WORK, KEYWORD
    """
    nlp = _load_model()
    if nlp is None:
        return []

    # Truncate long text to control processing time
    truncated = text[:500]

    try:
        doc = nlp(truncated)
    except Exception as e:
        logger.debug("NER processing failed: %s", e)
        return []

    seen: set[str] = set()
    results: list[tuple[str, str]] = []

    for ent in doc.ents:
        ent_text = ent.text.strip()
        if not ent_text or ent_text in seen:
            continue

        # Map NER label to CandidateType
        cand_type = LABEL_TO_TYPE.get(ent.label_)
        if cand_type is None:
            continue

        seen.add(ent_text)
        results.append((ent_text, cand_type))

        if len(results) >= max_entities:
            break

    return results
