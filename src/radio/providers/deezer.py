from __future__ import annotations

import logging
import time

import httpx

from radio.providers import MIN_CONFIDENCE, TrackMatch, match_confidence, normalize

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.deezer.com/search"

# Deezer docs: 50 requests per 5 seconds
RATE_LIMIT = 50.0 / 5.0  # req/s


def search(artist: str, title: str) -> TrackMatch | None:
    """Search Deezer for a track. No auth required."""
    norm_artist = normalize(artist)
    norm_title = normalize(title)

    # Try structured query first, fall back to free-text
    queries = (
        f'artist:"{norm_artist}" track:"{norm_title}"',
        f"{norm_artist} {norm_title}",
    )

    data: list[dict] = []
    for query in queries:
        resp = _request({"q": query, "limit": "5"})
        if resp is None:
            return None  # error, not just empty
        data = resp.get("data", [])
        if data:
            break

    if not data:
        return None

    best, conf = _pick_best(data, artist, title)
    if best is None or conf < MIN_CONFIDENCE:
        return None

    return TrackMatch(
        track_id=f"deezer:{best['id']}",
        matched_artist=best.get("artist", {}).get("name", ""),
        matched_title=best.get("title", ""),
        duration_ms=best.get("duration", 0) * 1000,  # Deezer returns seconds
        explicit=best.get("explicit_lyrics", False),
        album=best.get("album", {}).get("title", ""),
        release_date="",  # Not in search results
        genre=None,  # Not in search results
        source="deezer",
        confidence=conf,
    )


def _request(params: dict, retries: int = 3) -> dict | None:
    """Make a Deezer API request with retry and backoff."""
    for attempt in range(retries):
        try:
            resp = httpx.get(SEARCH_URL, params=params, timeout=10)
            resp.raise_for_status()
            body = resp.json()

            # Deezer signals rate limits as 200 with error object
            if "error" in body:
                code = body["error"].get("code")
                if code == 4:  # QuotaException
                    wait = 2 ** attempt * 5  # 5s, 10s, 20s
                    logger.warning("deezer quota_exceeded sleeping=%ds attempt=%d/%d", wait, attempt + 1, retries)
                    time.sleep(wait)
                    continue
                logger.error("deezer api_error code=%s message=%s", code, body["error"].get("message"))
                return None

            return body

        except httpx.HTTPError as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt * 2)
                continue
            logger.error("deezer request_failed error=%s", exc)
            return None

    logger.error("deezer exhausted retries=%d", retries)
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
            result.get("artist", {}).get("name", ""), result.get("title", ""),
        )
        if conf > best_conf:
            best_conf = conf
            best_result = result

    return best_result, best_conf
