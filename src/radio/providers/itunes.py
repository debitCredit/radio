from __future__ import annotations

import logging
import time

import httpx

from radio.providers import TrackMatch, normalize

logger = logging.getLogger(__name__)

SEARCH_URL = "https://itunes.apple.com/search"

# Apple docs: ~20 requests/minute per IP
RATE_LIMIT = 20.0 / 60.0  # req/s


def search(artist: str, title: str) -> TrackMatch | None:
    """Search iTunes for a track. No auth required."""
    norm_artist = normalize(artist)
    norm_title = normalize(title)
    query = f"{norm_artist} {norm_title}"

    params = {
        "term": query,
        "media": "music",
        "entity": "song",
        "limit": "5",
    }

    resp = _request(params)
    if resp is None:
        return None

    results = resp.get("results", [])
    if not results:
        return None

    best = _pick_best(results, norm_artist, norm_title)
    if best is None:
        return None

    return TrackMatch(
        track_id=f"itunes:{best['trackId']}",
        matched_artist=best.get("artistName", ""),
        matched_title=best.get("trackName", ""),
        duration_ms=best.get("trackTimeMillis", 0),
        explicit=best.get("trackExplicitness") == "explicit",
        album=best.get("collectionName", ""),
        release_date=best.get("releaseDate", "")[:10],
        genre=best.get("primaryGenreName"),
        source="itunes",
    )


def lookup_genre(artist: str, title: str) -> str | None:
    """Lightweight search just for genre — used to backfill tracks from other providers."""
    norm_artist = normalize(artist)
    norm_title = normalize(title)

    params = {
        "term": f"{norm_artist} {norm_title}",
        "media": "music",
        "entity": "song",
        "limit": "3",
    }

    resp = _request(params)
    if resp is None:
        return None

    results = resp.get("results", [])
    best = _pick_best(results, norm_artist, norm_title) if results else None
    return best.get("primaryGenreName") if best else None


def _request(params: dict, retries: int = 3) -> dict | None:
    """Make an iTunes API request with retry and backoff."""
    for attempt in range(retries):
        try:
            resp = httpx.get(SEARCH_URL, params=params, timeout=10)

            if resp.status_code == 429:
                # iTunes doesn't send Retry-After — use exponential backoff
                wait = 2 ** attempt * 10  # 10s, 20s, 40s
                logger.warning("itunes 429 sleeping=%ds attempt=%d/%d", wait, attempt + 1, retries)
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                wait = 2 ** attempt * 15
                logger.warning("itunes 403 sleeping=%ds attempt=%d/%d", wait, attempt + 1, retries)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPError as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 2)
                continue
            logger.error("itunes request_failed error=%s", exc)
            return None

    logger.error("itunes exhausted retries=%d", retries)
    return None


def _pick_best(
    results: list[dict],
    norm_artist: str,
    norm_title: str,
) -> dict | None:
    """Pick the best match from iTunes results."""
    norm_artist_lower = norm_artist.lower()
    norm_title_lower = norm_title.lower()

    for result in results:
        r_artist = normalize(result.get("artistName", "")).lower()
        r_title = normalize(result.get("trackName", "")).lower()
        if norm_artist_lower in r_artist or r_artist in norm_artist_lower:
            if norm_title_lower in r_title or r_title in norm_title_lower:
                return result

    return results[0] if results else None
