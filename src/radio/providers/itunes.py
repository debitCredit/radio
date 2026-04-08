from __future__ import annotations

import logging
import time

import httpx

from radio.providers import MIN_CONFIDENCE, TrackMatch, match_confidence, normalize

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

    best, conf = _pick_best(results, artist, title)
    if best is None or conf < MIN_CONFIDENCE:
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
        confidence=conf,
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
    if not results:
        return None

    best, conf = _pick_best(results, artist, title)
    if best is None or conf < MIN_CONFIDENCE:
        return None

    return best.get("primaryGenreName")


def _request(params: dict, retries: int = 3) -> dict | None:
    """Make an iTunes API request with retry and backoff."""
    for attempt in range(retries):
        try:
            resp = httpx.get(SEARCH_URL, params=params, timeout=10)

            if resp.status_code == 429:
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
            body = resp.json()
            logger.debug("itunes status=%d results=%d q=%s", resp.status_code, len(body.get("results", [])), params.get("term", ""))
            return body

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
    query_artist: str,
    query_title: str,
) -> tuple[dict | None, float]:
    """Pick the best match by confidence score. Returns (result, confidence)."""
    best_result = None
    best_conf = 0.0

    for result in results:
        conf = match_confidence(
            query_artist, query_title,
            result.get("artistName", ""), result.get("trackName", ""),
        )
        if conf > best_conf:
            best_conf = conf
            best_result = result

    return best_result, best_conf
