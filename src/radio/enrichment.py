from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable

import polars as pl

from radio.providers import TrackMatch
from radio.providers import itunes, deezer
from radio.providers.spotify_provider import RateBanError
from radio.storage import TRACKS_SCHEMA

logger = logging.getLogger(__name__)


@dataclass
class _Provider:
    name: str
    search: Callable[[str, str], TrackMatch | None]
    rate: float  # max requests per second
    workers: int  # concurrent threads


def _build_providers() -> tuple[_Provider, ...]:
    """Build list of available providers in priority order."""
    providers = [
        _Provider(name="itunes", search=itunes.search, rate=itunes.RATE_LIMIT, workers=1),
        _Provider(name="deezer", search=deezer.search, rate=deezer.RATE_LIMIT, workers=3),
    ]

    # Spotify is optional — needs credentials
    try:
        from radio.providers import spotify_provider
        if spotify_provider.available():
            providers.append(
                _Provider(name="spotify", search=spotify_provider.search, rate=spotify_provider.RATE_LIMIT, workers=3)
            )
    except Exception:
        logger.debug("spotify provider unavailable")

    return tuple(providers)


class _RateLimiter:
    """Token-bucket rate limiter: max `rate` calls per second."""

    def __init__(self, rate: float) -> None:
        self._interval = 1.0 / rate
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(now, self._next) + self._interval


def _match_to_row(artist: str, title: str, match: TrackMatch) -> dict:
    return {
        "track_id": match.track_id,
        "artist": artist,
        "title": title,
        "matched_artist": match.matched_artist,
        "matched_title": match.matched_title,
        "duration_ms": match.duration_ms,
        "explicit": match.explicit,
        "album": match.album,
        "release_date": match.release_date,
        "genre": match.genre,
        "source": match.source,
    }


def get_unenriched_pairs(
    playlist_df: pl.DataFrame,
    tracks_df: pl.DataFrame,
) -> tuple[tuple[str, str], ...]:
    """Return unique (artist, title) pairs from playlist not already in tracks."""
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
    save_every: int = 500,
    on_batch: Callable[[pl.DataFrame], None] | None = None,
) -> pl.DataFrame:
    """Enrich tracks using all available providers in parallel.

    Each provider runs its own thread pool with its own rate limiter.
    A shared set tracks which pairs are still needed. When any provider
    finds a match, that pair is removed so others skip it.
    """
    providers = _build_providers()
    logger.info("providers=%s", [p.name for p in providers])

    # Shared state
    remaining: set[tuple[str, str]] = set(pairs)
    remaining_lock = threading.Lock()
    rows: list[dict] = []
    rows_lock = threading.Lock()
    counters = {"completed": 0, "matched": 0, "last_saved": 0}

    def _maybe_flush() -> None:
        pending = len(rows) - counters["last_saved"]
        if on_batch and pending >= save_every:
            batch = pl.DataFrame(rows[counters["last_saved"]:], schema=TRACKS_SCHEMA)
            counters["last_saved"] = len(rows)
            on_batch(batch)
            logger.info("batch_saved rows=%d total_saved=%d", len(batch), counters["last_saved"])

    def _run_provider(provider: _Provider) -> None:
        limiter = _RateLimiter(provider.rate)
        plog = logging.getLogger(f"radio.enrichment.{provider.name}")

        def _process_one(artist: str, title: str) -> None:
            # Check if still needed
            with remaining_lock:
                if (artist, title) not in remaining:
                    return

            limiter.wait()

            try:
                match = provider.search(artist, title)
            except RateBanError:
                plog.warning("rate ban — stopping this provider")
                return
            except Exception as exc:
                plog.debug("error artist=%r title=%r error=%s", artist, title, exc)
                return

            with rows_lock:
                # Double-check — another provider may have found it
                with remaining_lock:
                    if (artist, title) not in remaining:
                        return
                    remaining.discard((artist, title))

                if match is not None:
                    rows.append(_match_to_row(artist, title, match))
                    counters["matched"] += 1

                counters["completed"] += 1
                if counters["completed"] % 500 == 0:
                    plog.info(
                        "progress=%d/%d matched=%d remaining=%d",
                        counters["completed"], len(pairs),
                        counters["matched"], len(remaining),
                    )
                _maybe_flush()

        # Each provider gets its own thread pool
        with ThreadPoolExecutor(max_workers=provider.workers) as pool:
            futures = []
            for artist, title in pairs:
                futures.append(pool.submit(_process_one, artist, title))

            for future in as_completed(futures):
                try:
                    future.result()
                except RateBanError:
                    plog.warning("rate ban — cancelling remaining work for %s", provider.name)
                    for f in futures:
                        f.cancel()
                    break
                except Exception:
                    pass  # Individual errors already logged

        plog.info("provider_done matched_so_far=%d remaining=%d", counters["matched"], len(remaining))

    # Run all providers concurrently — each in its own thread
    provider_threads: list[threading.Thread] = []
    for provider in providers:
        t = threading.Thread(target=_run_provider, args=(provider,), name=f"provider-{provider.name}")
        t.start()
        provider_threads.append(t)

    try:
        for t in provider_threads:
            t.join()
    except KeyboardInterrupt:
        logger.warning("interrupted — saving progress")

    # Final progress
    logger.info(
        "enrichment_done matched=%d/%d remaining=%d",
        counters["matched"], len(pairs), len(remaining),
    )

    # Return unsaved rows
    unsaved = rows[counters["last_saved"]:]
    if not unsaved:
        return pl.DataFrame(schema=TRACKS_SCHEMA)
    return pl.DataFrame(unsaved, schema=TRACKS_SCHEMA)


def backfill_genres(
    tracks_df: pl.DataFrame,
    on_save: Callable[[pl.DataFrame], None] | None = None,
) -> pl.DataFrame:
    """Backfill missing genres using iTunes for tracks found by other providers."""
    missing = tracks_df.filter(pl.col("genre").is_null() | (pl.col("genre") == ""))
    if missing.is_empty():
        logger.info("all tracks already have genres")
        return tracks_df

    logger.info("backfilling genres for %d tracks via iTunes", len(missing))
    limiter = _RateLimiter(itunes.RATE_LIMIT)
    updated = 0

    # Build a mutable genre map
    genre_map: dict[tuple[str, str], str] = {}

    for row in missing.iter_rows(named=True):
        artist, title = row["artist"], row["title"]
        limiter.wait()
        genre = itunes.lookup_genre(artist, title)
        if genre:
            genre_map[(artist, title)] = genre
            updated += 1

        if (updated + 1) % 100 == 0:
            logger.info("genre_backfill progress=%d/%d found=%d", updated, len(missing), len(genre_map))

    if not genre_map:
        logger.info("no new genres found")
        return tracks_df

    # Apply genres
    def _fill_genre(row: dict) -> str | None:
        key = (row["artist"], row["title"])
        return genre_map.get(key, row["genre"])

    result = tracks_df.with_columns(
        pl.struct(["artist", "title", "genre"])
        .map_elements(
            lambda s: genre_map.get((s["artist"], s["title"]), s["genre"]),
            return_dtype=pl.Utf8,
        )
        .alias("genre")
    )

    logger.info("genre_backfill_done filled=%d/%d", len(genre_map), len(missing))

    if on_save:
        on_save(result)

    return result


def update_playlist_with_track_ids(
    playlist_df: pl.DataFrame,
    tracks_df: pl.DataFrame,
) -> pl.DataFrame:
    """Left join playlist on (artist, title) to populate track_id from tracks."""
    if tracks_df.is_empty():
        return playlist_df

    # Deduplicate tracks to prevent join fan-out
    id_map = tracks_df.select(["artist", "title", "track_id"]).unique(
        subset=["artist", "title"], keep="first"
    )

    if "track_id" in playlist_df.columns:
        base = playlist_df.drop("track_id")
    else:
        base = playlist_df

    updated = base.join(id_map, on=["artist", "title"], how="left")
    return updated.select(playlist_df.columns)
