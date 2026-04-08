from __future__ import annotations

import logging
import os
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from radio.storage import TRACKS_SCHEMA


def _get_client() -> spotipy.Spotify:
    client_id = os.environ["SPOTIFY_CLIENT_ID"]
    client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]
    auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(auth_manager=auth)


def get_unenriched_pairs(
    playlist_df: pl.DataFrame,
    tracks_df: pl.DataFrame,
) -> tuple[tuple[str, str], ...]:
    """Return unique (artist, title) pairs from playlist_df not already in tracks_df."""
    pairs = (
        playlist_df.select(["artist", "title"])
        .unique()
        .filter(pl.col("artist").is_not_null() & pl.col("title").is_not_null())
    )

    if tracks_df.is_empty():
        return tuple(
            (row["artist"], row["title"]) for row in pairs.iter_rows(named=True)
        )

    enriched = tracks_df.select(["artist", "title"]).unique()
    unenriched = pairs.join(enriched, on=["artist", "title"], how="anti")
    return tuple(
        (row["artist"], row["title"]) for row in unenriched.iter_rows(named=True)
    )


def enrich_tracks(
    pairs: tuple[tuple[str, str], ...],
    workers: int = 3,
    save_every: int = 500,
    on_batch: callable = None,
) -> pl.DataFrame:
    """Search Spotify for each (artist, title) pair concurrently.

    Args:
        pairs: (artist, title) tuples to look up.
        workers: concurrent threads.
        save_every: call on_batch every N completed tracks for incremental persistence.
        on_batch: callback receiving a pl.DataFrame of newly enriched rows to save.
    """
    sp = _get_client()
    rows: list[dict] = []
    lock = threading.Lock()
    counters = {"completed": 0, "matched": 0, "last_saved": 0}

    def _process(artist: str, title: str) -> dict | None:
        return _enrich_one(sp, artist, title)

    def _maybe_flush() -> None:
        pending = len(rows) - counters["last_saved"]
        if on_batch and pending >= save_every:
            batch = pl.DataFrame(rows[counters["last_saved"]:], schema=TRACKS_SCHEMA)
            counters["last_saved"] = len(rows)
            on_batch(batch)
            logger.info("batch_saved rows=%d total_saved=%d", len(batch), counters["last_saved"])

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, a, t): (a, t) for a, t in pairs}

        for future in as_completed(futures):
            artist, title = futures[future]
            row = future.result()

            with lock:
                if row is not None:
                    rows.append(row)
                    counters["matched"] += 1
                else:
                    logger.debug("no_match artist=%r title=%r", artist, title)

                counters["completed"] += 1
                if counters["completed"] % 500 == 0 or counters["completed"] == len(pairs):
                    logger.info(
                        "progress=%d/%d matched=%d miss=%d",
                        counters["completed"], len(pairs),
                        counters["matched"], counters["completed"] - counters["matched"],
                    )

                _maybe_flush()

    if not rows:
        return pl.DataFrame(schema=TRACKS_SCHEMA)

    # Return only unsaved rows if on_batch was used, otherwise all
    unsaved = rows[counters["last_saved"]:]
    if not unsaved:
        return pl.DataFrame(schema=TRACKS_SCHEMA)
    return pl.DataFrame(unsaved, schema=TRACKS_SCHEMA)


def _normalize(text: str) -> str:
    """Normalize text for Spotify search: strip diacritics, feat/ft tags, extra whitespace."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\s*[\(\[](feat\.?|ft\.?|featuring)\b[^\)\]]*[\)\]]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(feat\.?|ft\.?|featuring)\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _enrich_one(
    sp: spotipy.Spotify,
    artist: str,
    title: str,
) -> dict | None:
    normalized_query = f"track:{_normalize(title)} artist:{_normalize(artist)}"
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

    return {
        "spotify_track_id": track["id"],
        "artist": artist,
        "title": title,
        "spotify_artist": track["artists"][0]["name"] if track["artists"] else "",
        "spotify_title": track["name"],
        "duration_ms": track["duration_ms"],
        "explicit": track.get("explicit", False),
        "album": track["album"]["name"],
        "release_date": track["album"].get("release_date", ""),
    }


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
                retry_after = int(exc.headers.get("Retry-After", 5)) if exc.headers else 5
                logger.warning("rate_limited retry_after=%ds", retry_after)
                time.sleep(retry_after)
            elif attempt < retries - 1:
                logger.warning("search_error attempt=%d/%d query=%r error=%s", attempt + 1, retries, query, exc)
                time.sleep(2 ** attempt)
            else:
                logger.error("search_failed query=%r error=%s", query, exc)
                return None
    return None


def update_playlist_with_track_ids(
    playlist_df: pl.DataFrame,
    tracks_df: pl.DataFrame,
) -> pl.DataFrame:
    """Left join playlist on (artist, title) to populate spotify_track_id from tracks."""
    if tracks_df.is_empty():
        return playlist_df

    # Deduplicate tracks to prevent join fan-out
    id_map = tracks_df.select(["artist", "title", "spotify_track_id"]).unique(
        subset=["artist", "title"], keep="first"
    )

    if "spotify_track_id" in playlist_df.columns:
        base = playlist_df.drop("spotify_track_id")
    else:
        base = playlist_df

    updated = base.join(id_map, on=["artist", "title"], how="left")
    return updated.select(playlist_df.columns)
