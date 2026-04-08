from __future__ import annotations

import os
import time

import polars as pl
import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

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


def enrich_tracks(pairs: tuple[tuple[str, str], ...]) -> pl.DataFrame:
    """Search Spotify for each (artist, title) pair and return an enriched DataFrame."""
    sp = _get_client()
    rows: list[dict] = []

    for i, (artist, title) in enumerate(pairs):
        if i > 0 and i % 50 == 0:
            print(f"[spotify] enriched {i}/{len(pairs)} tracks...")

        row = _enrich_one(sp, artist, title)
        if row is not None:
            rows.append(row)

    print(f"[spotify] done — enriched {len(rows)}/{len(pairs)} tracks")

    if not rows:
        return pl.DataFrame(schema=TRACKS_SCHEMA)

    return pl.DataFrame(rows, schema=TRACKS_SCHEMA)


def _enrich_one(
    sp: spotipy.Spotify,
    artist: str,
    title: str,
) -> dict | None:
    query = f"track:{title} artist:{artist}"

    result = _search_with_retry(sp, query)
    if result is None:
        return None

    items = result.get("tracks", {}).get("items", [])
    if not items:
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
                print(f"[spotify] rate limited, sleeping {retry_after}s...")
                time.sleep(retry_after)
            elif attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"[spotify] search failed for query '{query}': {exc}")
                return None
    return None


def update_playlist_with_track_ids(
    playlist_df: pl.DataFrame,
    tracks_df: pl.DataFrame,
) -> pl.DataFrame:
    """Left join playlist on (artist, title) to populate spotify_track_id from tracks."""
    if tracks_df.is_empty():
        return playlist_df

    id_map = tracks_df.select(["artist", "title", "spotify_track_id"])

    if "spotify_track_id" in playlist_df.columns:
        base = playlist_df.drop("spotify_track_id")
    else:
        base = playlist_df

    updated = base.join(id_map, on=["artist", "title"], how="left")

    cols = [c for c in playlist_df.columns]
    return updated.select(cols)
