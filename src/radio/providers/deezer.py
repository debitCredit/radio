from __future__ import annotations

import logging
import time

import httpx

from radio.providers import TrackMatch, normalize

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

    best = _pick_best(data, norm_artist, norm_title)
    if best is None:
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
    norm_artist: str,
    norm_title: str,
) -> dict | None:
    """Pick the best match from Deezer results."""
    norm_artist_lower = norm_artist.lower()
    norm_title_lower = norm_title.lower()

    for result in results:
        r_artist = normalize(result.get("artist", {}).get("name", "")).lower()
        r_title = normalize(result.get("title", "")).lower()
        if norm_artist_lower in r_artist or r_artist in norm_artist_lower:
            if norm_title_lower in r_title or r_title in norm_title_lower:
                return result

    return results[0] if results else None
