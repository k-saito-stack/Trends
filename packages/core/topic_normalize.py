"""Normalization rules for topic-style candidates."""

from __future__ import annotations

import re
import unicodedata

STOP_PHRASES = {
    "おすすめ",
    "まとめ",
    "ランキング",
    "人気",
    "話題",
    "トレンド",
    "バズ",
    "新作",
    "公式",
    "rank",
    "hashtags",
    "posts",
    "trend",
    "creators",
    "actions",
    "songs",
    "videos",
    "industry",
    "view more",
    "see analytics",
    "no related creator",
}

TRAILING_MARKS_RE = re.compile(r"[!！?？…。．・〜~]+$")
EMOJIISH_RE = re.compile(r"[\U00010000-\U0010ffff]", flags=re.UNICODE)
NOISE_RE = re.compile(r"[\"'`“”‘’\[\]\(\)【】<>＜＞]")
WHITESPACE_RE = re.compile(r"\s+")

VERB_ENDINGS = (
    ("している", "する"),
    ("してる", "する"),
    ("した", "する"),
    ("して", "する"),
    ("つけてる", "つける"),
    ("つけた", "つける"),
    ("付けてる", "付ける"),
    ("付けた", "付ける"),
    ("持ってる", "持つ"),
    ("持った", "持つ"),
)


def normalize_topic_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    normalized = normalized.strip()
    normalized = EMOJIISH_RE.sub("", normalized)
    normalized = NOISE_RE.sub(" ", normalized)
    normalized = TRAILING_MARKS_RE.sub("", normalized)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip()
    return _lemmatize_surface(normalized)


def normalize_hashtag(text: str) -> str:
    normalized = normalize_topic_text(text)
    if normalized.startswith("#"):
        normalized = normalized[1:]
    return normalized.lower()


def topic_match_key(text: str) -> str:
    normalized = normalize_topic_text(text).lower()
    normalized = normalized.replace("#", "")
    normalized = re.sub(r"[\s_\-・:：/／]+", "", normalized)
    return normalized


def should_keep_topic(text: str) -> bool:
    normalized = normalize_topic_text(text)
    if not normalized:
        return False
    if normalize_hashtag(normalized) in STOP_PHRASES:
        return False
    if normalized.lower() in STOP_PHRASES:
        return False
    return len(topic_match_key(normalized)) > 1


def _lemmatize_surface(text: str) -> str:
    result = text
    for suffix, lemma in VERB_ENDINGS:
        if result.endswith(suffix):
            return result[: -len(suffix)] + lemma
    if result.endswith("して"):
        return result[:-2] + "する"
    return result
