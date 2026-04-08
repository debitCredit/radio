from __future__ import annotations

import logging
import os
import time

import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

from radio.providers import TrackMatch, normalize

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Spotify docs: ~250 requests per 30 seconds for client credentials
RATE_LIMIT = 250.0 / 30.0  # req/s

# Bail if Spotify asks us to wait longer than this
MAX_RETRY_AFTER = 300


class RateBanError(Exception):
    """Raised when Spotify's Retry-After exceeds MAX_RETRY_AFTER."""


_client: spotipy.Spotify | None = None


def _get_client() -> spotipy.Spotify:
    global _client
    if _client is None:
        client_id = os.environ.get("SPOTIFY_CLIENT_ID")
        client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET required")
        auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        _client = spotipy.Spotify(auth_manager=auth, retries=0)
    return _client


def available() -> bool:
    """Check if Spotify credentials are configured."""
    return bool(os.environ.get("SPOTIFY_CLIENT_ID") and os.environ.get("SPOTIFY_CLIENT_SECRET"))


def search(artist: str, title: str) -> TrackMatch | None:
    """Search Spotify for a track."""
    sp = _get_client()
    norm_artist = normalize(artist)
    norm_title = normalize(title)

    normalized_query = f"track:{norm_title} artist:{norm_artist}"
    original_query = f"track:{title} artist:{artist}"
    queries = (normalized_query,) if normalized_query == original_query else (normalized_query, original_query)

    for query in queries:
        result = _search_with_retry(sp, query)
        if result is None:
            continue
        items = result.get("tracks", {}).get("items", [])
        if items:
            break
    else:
        return None

    track = items[0]

    return TrackMatch(
        track_id=f"spotify:{track['id']}",
        matched_artist=track["artists"][0]["name"] if track["artists"] else "",
        matched_title=track["name"],
        duration_ms=track["duration_ms"],
        explicit=track.get("explicit", False),
        album=track["album"]["name"],
        release_date=track["album"].get("release_date", ""),
        genre=None,  # Deprecated in Spotify API
        source="spotify",
    )


def _search_with_retry(
    sp: spotipy.Spotify,
    query: str,
    retries: int = 3,
) -> dict | None:
    for attempt in range(retries):
        try:
            return sp.search(q=query, type="track", limit=1)
        except SpotifyException as exc:
            if exc.http_status == 429:
                # spotipy may swallow the real Retry-After; parse from headers or msg
                retry_after = _parse_retry_after(exc)
                if retry_after > MAX_RETRY_AFTER:
                    logger.error("spotify rate_ban retry_after=%ds — stopping", retry_after)
                    raise RateBanError(f"Retry-After {retry_after}s")
                logger.warning("spotify rate_limited retry_after=%ds", retry_after)
                time.sleep(retry_after)
            elif exc.http_status == -1 or "Max Retries" in str(exc):
                # spotipy exhausted its internal retries — likely a ban
                logger.error("spotify max_retries_reached — treating as ban")
                raise RateBanError("Max retries reached")
            elif attempt < retries - 1:
                logger.warning("spotify search_error attempt=%d/%d error=%s", attempt + 1, retries, exc)
                time.sleep(2 ** attempt)
            else:
                logger.error("spotify search_failed query=%r error=%s", query, exc)
                return None
        except Exception as exc:
            if "Max Retries" in str(exc) or "rate limit" in str(exc).lower():
                logger.error("spotify connection_error — treating as ban: %s", exc)
                raise RateBanError(str(exc))
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            logger.error("spotify unexpected_error query=%r error=%s", query, exc)
            return None
    return None


def _parse_retry_after(exc: SpotifyException) -> int:
    """Extract Retry-After from SpotifyException headers or message."""
    if exc.headers:
        val = exc.headers.get("Retry-After") or exc.headers.get("retry-after")
        if val:
            try:
                return int(val)
            except ValueError:
                pass
    # Try to parse from error message
    msg = str(exc)
    import re
    match = re.search(r"(\d+)\s*s", msg)
    if match:
        return int(match.group(1))
    return 5
