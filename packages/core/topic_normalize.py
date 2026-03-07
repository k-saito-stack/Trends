"""Normalization rules for topic-style candidates."""

from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import cast

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
TOKEN_RE = re.compile(r"[ぁ-んァ-ン一-龥a-zA-Z0-9]+")

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "domain_filters.yaml"

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


@lru_cache(maxsize=1)
def _load_filters() -> dict[str, list[str]]:
    return cast(dict[str, list[str]], json.loads(CONFIG_PATH.read_text(encoding="utf-8")))


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
    if contains_generic_event_word(normalized) or contains_live_playbyplay_word(normalized):
        return False
    if contains_finance_print_word(normalized):
        return False
    return len(topic_match_key(normalized)) > 1


def topic_specificity(text: str) -> float:
    normalized = normalize_topic_text(text)
    if not normalized:
        return 0.0
    token_count = len(TOKEN_RE.findall(normalized))
    core_length = len(topic_match_key(normalized))
    if core_length <= 2:
        return 0.1
    specificity = min(1.0, 0.18 * token_count + 0.04 * core_length)
    if normalize_hashtag(normalized) in STOP_PHRASES:
        specificity *= 0.3
    if contains_generic_event_word(normalized):
        specificity *= 0.45
    if contains_live_playbyplay_word(normalized):
        specificity *= 0.35
    return round(max(0.0, min(1.0, specificity)), 4)


def behavior_objectness(text: str) -> float:
    normalized = normalize_topic_text(text)
    if not normalized:
        return 0.0
    filters = _load_filters()
    lowered = normalized.lower()
    if any(token.lower() in lowered for token in filters["behavior_object_blacklist"]):
        return 0.15
    if any(token.lower() in lowered for token in filters["behavior_object_whitelist"]):
        return 0.9
    if "を" in normalized or "の" in normalized:
        return 0.7
    if normalized.endswith(("活", "界隈", "チャレンジ", "メイク", "コーデ")):
        return 0.6
    return 0.35


def contains_generic_event_word(text: str) -> bool:
    lowered = normalize_topic_text(text).lower()
    filters = _load_filters()
    return any(token.lower() in lowered for token in filters["generic_event_keywords"])


def contains_live_playbyplay_word(text: str) -> bool:
    lowered = normalize_topic_text(text).lower()
    filters = _load_filters()
    return any(
        token.lower() in lowered
        for token in [
            *filters["sports_live_keywords"],
            *filters["play_by_play_keywords"],
            *filters["weather_disaster_keywords"],
        ]
    )


def contains_finance_print_word(text: str) -> bool:
    lowered = normalize_topic_text(text).lower()
    filters = _load_filters()
    return any(
        token.lower() in lowered
        for token in [
            *filters["finance_print_keywords"],
            *filters["politics_keywords"],
        ]
    )


def _lemmatize_surface(text: str) -> str:
    result = text
    for suffix, lemma in VERB_ENDINGS:
        if result.endswith(suffix):
            return result[: -len(suffix)] + lemma
    if result.endswith("して"):
        return result[:-2] + "する"
    return result
