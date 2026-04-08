from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher


@dataclass(frozen=True)
class TrackMatch:
    """Result from any provider search."""
    track_id: str
    matched_artist: str
    matched_title: str
    duration_ms: int
    explicit: bool | None
    album: str
    release_date: str
    genre: str | None
    source: str
    confidence: float  # 0.0–1.0 combined artist+title similarity


# Minimum confidence to accept a match
MIN_CONFIDENCE = 0.6


def normalize(text: str) -> str:
    """Normalize text for search: strip diacritics, feat/ft tags, extra whitespace."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\s*[\(\[](feat\.?|ft\.?|featuring)\b[^\)\]]*[\)\]]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(feat\.?|ft\.?|featuring)\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_parentheticals(text: str) -> str:
    """Strip all parenthetical/bracket suffixes for comparison."""
    return re.sub(r"\s*[\(\[][^\)\]]*[\)\]]", "", text).strip()


def similarity(a: str, b: str) -> float:
    """Normalized similarity between two strings (0.0–1.0).

    Strips diacritics, parentheticals, and compares case-insensitively.
    """
    a_clean = _strip_parentheticals(normalize(a)).lower()
    b_clean = _strip_parentheticals(normalize(b)).lower()
    if not a_clean or not b_clean:
        return 0.0
    return SequenceMatcher(None, a_clean, b_clean).ratio()


def match_confidence(
    query_artist: str,
    query_title: str,
    result_artist: str,
    result_title: str,
) -> float:
    """Combined confidence score: weighted average of artist and title similarity."""
    artist_sim = similarity(query_artist, result_artist)
    title_sim = similarity(query_title, result_title)
    # Title matters more — a wrong title is worse than a slightly different artist name
    return artist_sim * 0.4 + title_sim * 0.6
