from __future__ import annotations

import logging
import os
import time

import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

from radio.providers import MIN_CONFIDENCE, TrackMatch, match_confidence, normalize

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Spotify docs: ~250 requests per 30 seconds for client credentials
RATE_LIMIT = 250.0 / 30.0  # req/s


class RateBanError(Exception):
    """Raised when Spotify's Retry-After exceeds MAX_RETRY_AFTER."""


_client: spotipy.Spotify | None = None
_disabled = False  # Set True after a rate ban to stop all Spotify calls


def _get_client() -> spotipy.Spotify:
    global _client
    if _disabled:
        raise RateBanError("Spotify disabled due to rate ban")
    if _client is None:
        client_id = os.environ.get("SPOTIFY_CLIENT_ID")
        client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET required")
        auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        _client = spotipy.Spotify(auth_manager=auth, retries=0)
    return _client


def available() -> bool:
    """Check if Spotify credentials are configured and not banned."""
    if _disabled:
        return False
    return bool(os.environ.get("SPOTIFY_CLIENT_ID") and os.environ.get("SPOTIFY_CLIENT_SECRET"))


def search(artist: str, title: str) -> TrackMatch | None:
    """Search Spotify for a track."""
    sp = _get_client()
    norm_artist = normalize(artist)
    norm_title = normalize(title)

    normalized_query = f"track:{norm_title} artist:{norm_artist}"
    original_query = f"track:{title} artist:{artist}"
    queries = (normalized_query,) if normalized_query == original_query else (normalized_query, original_query)

    best_match: TrackMatch | None = None
    best_conf = 0.0

    for query in queries:
        result = _search_with_retry(sp, query)
        if result is None:
            continue
        for track in result.get("tracks", {}).get("items", []):
            matched_artist = track["artists"][0]["name"] if track["artists"] else ""
            conf = match_confidence(artist, title, matched_artist, track["name"])
            if conf > best_conf:
                best_conf = conf
                best_match = TrackMatch(
                    track_id=f"spotify:{track['id']}",
                    matched_artist=matched_artist,
                    matched_title=track["name"],
                    duration_ms=track["duration_ms"],
                    explicit=track.get("explicit", False),
                    album=track["album"]["name"],
                    release_date=track["album"].get("release_date", ""),
                    genre=None,
                    source="spotify",
                    confidence=conf,
                )

    if best_match is None or best_conf < MIN_CONFIDENCE:
        return None
    return best_match


def _search_with_retry(
    sp: spotipy.Spotify,
    query: str,
    retries: int = 3,
) -> dict | None:
    for attempt in range(retries):
        try:
            return sp.search(q=query, type="track", limit=1)
        except SpotifyException as exc:
            if exc.http_status == 429 or "Max Retries" in str(exc) or exc.http_status == -1:
                global _disabled
                _disabled = True
                logger.error("spotify rate_limited — disabling for this session")
                raise RateBanError("Spotify rate limited")
            elif attempt < retries - 1:
                logger.warning("spotify search_error attempt=%d/%d error=%s", attempt + 1, retries, exc)
                time.sleep(2 ** attempt)
            else:
                logger.error("spotify search_failed query=%r error=%s", query, exc)
                return None
        except Exception as exc:
            if "Max Retries" in str(exc) or "rate limit" in str(exc).lower():
                _disabled = True
                logger.error("spotify connection_error — disabling: %s", exc)
                raise RateBanError(str(exc))
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            logger.error("spotify unexpected_error query=%r error=%s", query, exc)
            return None
    return None
