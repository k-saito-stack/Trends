"""Proper noun detection (noise filter).

Prevents common/generic words from becoming candidates.
Precision-first: better to miss a candidate than to flood with noise.

Spec reference: Section 9 (Candidate Model - Proper Noun Detection)
"""

from __future__ import annotations

import re

# Generic words that should NOT become candidates
# This is the default blacklist; the authoritative list is in Firestore
DEFAULT_BLACKLIST = {
    # Japanese generic terms
    "公式", "新作", "予告", "人気", "発表", "最新", "速報", "話題",
    "注目", "おすすめ", "まとめ", "ランキング", "特集", "独占",
    "配信", "放送", "開始", "決定", "情報", "解禁", "初公開",
    "無料", "限定", "特別", "完全", "最強", "衝撃", "感動",
    "ニュース", "エンタメ", "芸能", "音楽", "映画", "ドラマ",
    # English generic terms
    "official", "new", "latest", "best", "top", "breaking",
    "news", "update", "live", "full", "video", "music",
    "trailer", "teaser", "preview", "review", "reaction",
}

# Short whitelist: known proper nouns that are 2 chars or less
DEFAULT_WHITELIST = {
    "IU", "AI", "UA", "YUI",
}


def is_proper_noun(
    name: str,
    blacklist: set[str] | None = None,
    whitelist: set[str] | None = None,
) -> bool:
    """Check if a name looks like a proper noun (worth tracking).

    Returns True if the name passes all filters.
    Returns False if it's likely noise.

    Rules (from spec):
    1. 2 chars or less -> reject (unless in whitelist)
    2. Digits only or symbols only -> reject
    3. Blacklist-only words -> reject
    4. Japanese hiragana-only and <= 3 chars -> reject (unless whitelist)
    """
    if blacklist is None:
        blacklist = DEFAULT_BLACKLIST
    if whitelist is None:
        whitelist = DEFAULT_WHITELIST

    stripped = name.strip()

    # Whitelist override
    if stripped in whitelist or stripped.upper() in whitelist:
        return True

    # Rule 1: Too short
    if len(stripped) <= 2:
        return False

    # Rule 2: Digits only
    if re.match(r"^\d+$", stripped):
        return False

    # Rule 2: Symbols only
    if re.match(r"^[\W_]+$", stripped):
        return False

    # Rule 3: Blacklist match (exact, case-insensitive)
    if stripped.lower() in {w.lower() for w in blacklist}:
        return False
    if stripped in blacklist:
        return False

    # Rule 4: Hiragana-only and short
    return not (_is_hiragana_only(stripped) and len(stripped) <= 3)


def _is_hiragana_only(text: str) -> bool:
    """Check if text consists only of hiragana characters."""
    return all(
        "\u3040" <= c <= "\u309f" or c == " "
        for c in text
    )
