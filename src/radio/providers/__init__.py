from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


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


def normalize(text: str) -> str:
    """Normalize text for search: strip diacritics, feat/ft tags, extra whitespace."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\s*[\(\[](feat\.?|ft\.?|featuring)\b[^\)\]]*[\)\]]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(feat\.?|ft\.?|featuring)\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text
