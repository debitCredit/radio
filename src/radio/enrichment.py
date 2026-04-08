from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable

import polars as pl

from radio.providers import TrackMatch
from radio.providers import itunes, deezer
from radio.providers.spotify_provider import RateBanError
from radio.storage import TRACKS_SCHEMA

logger = logging.getLogger(__name__)

# Sentinel to signal workers to stop
_STOP = ("__STOP__", "__STOP__")


@dataclass
class _Provider:
    name: str
    search: Callable[[str, str], TrackMatch | None]
    rate: float  # max requests per second
    workers: int  # concurrent threads


def _build_providers() -> tuple[_Provider, ...]:
    """Build list of available providers."""
    providers = [
        _Provider(name="deezer", search=deezer.search, rate=deezer.RATE_LIMIT, workers=3),
        _Provider(name="itunes", search=itunes.search, rate=itunes.RATE_LIMIT, workers=1),
    ]

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

    Uses a shared work queue: each pair is processed by one provider at a time.
    Misses go to a retry queue for other providers to attempt.
    """
    providers = _build_providers()
    logger.info("providers=%s", [p.name for p in providers])

    # Work queue — each item is (artist, title, tried_providers)
    work_q: queue.Queue[tuple[str, str, frozenset[str]]] = queue.Queue()
    for artist, title in pairs:
        work_q.put((artist, title, frozenset()))

    # Retry queue — pairs that missed, to be redistributed
    retry_q: queue.Queue[tuple[str, str, frozenset[str]]] = queue.Queue()

    # Results
    found: set[tuple[str, str]] = set()
    found_lock = threading.Lock()
    rows: list[dict] = []
    rows_lock = threading.Lock()
    counters = {"completed": 0, "matched": 0, "last_saved": 0}
    provider_names = frozenset(p.name for p in providers)

    def _maybe_flush() -> None:
        pending = len(rows) - counters["last_saved"]
        if on_batch and pending >= save_every:
            batch = pl.DataFrame(rows[counters["last_saved"]:], schema=TRACKS_SCHEMA)
            counters["last_saved"] = len(rows)
            on_batch(batch)
            logger.info("batch_saved rows=%d total_saved=%d", len(batch), counters["last_saved"])

    def _worker(provider: _Provider, limiter: _RateLimiter) -> None:
        plog = logging.getLogger(f"radio.enrichment.{provider.name}")

        while True:
            try:
                item = work_q.get(timeout=1)
            except queue.Empty:
                return

            artist, title = item[0], item[1]
            tried = item[2]

            if artist == _STOP[0]:
                work_q.put(item)  # re-add for other workers
                return

            # Skip if already found by another provider
            with found_lock:
                if (artist, title) in found:
                    work_q.task_done()
                    continue

            # Skip if this provider already tried
            if provider.name in tried:
                # Put back for other providers
                retry_q.put((artist, title, tried))
                work_q.task_done()
                continue

            limiter.wait()

            try:
                match = provider.search(artist, title)
            except RateBanError:
                plog.warning("rate ban — stopping provider")
                # Put item back for other providers
                retry_q.put((artist, title, tried | {provider.name}))
                work_q.task_done()
                # Drain remaining items back to retry queue
                while True:
                    try:
                        remaining_item = work_q.get_nowait()
                        if remaining_item[0] != _STOP[0]:
                            retry_q.put((remaining_item[0], remaining_item[1], remaining_item[2] | {provider.name}))
                        work_q.task_done()
                    except queue.Empty:
                        break
                return
            except Exception as exc:
                plog.debug("error artist=%r title=%r error=%s", artist, title, exc)
                retry_q.put((artist, title, tried | {provider.name}))
                work_q.task_done()
                continue

            with rows_lock:
                with found_lock:
                    if (artist, title) in found:
                        work_q.task_done()
                        continue
                    found.add((artist, title))

                if match is not None:
                    rows.append(_match_to_row(artist, title, match))
                    counters["matched"] += 1
                else:
                    # Search returned None — provider couldn't find it
                    found.discard((artist, title))
                    retry_q.put((artist, title, tried | {provider.name}))

                counters["completed"] += 1
                if counters["completed"] % 500 == 0:
                    logger.info(
                        "progress=%d/%d matched=%d queue=%d",
                        counters["completed"], len(pairs),
                        counters["matched"], work_q.qsize(),
                    )
                _maybe_flush()

            work_q.task_done()

    # Run rounds until no more progress
    max_rounds = len(providers)
    for round_num in range(max_rounds):
        if work_q.empty() and retry_q.empty():
            break

        # Move retries back to work queue (only pairs not yet tried by all providers)
        moved = 0
        while not retry_q.empty():
            try:
                item = retry_q.get_nowait()
                if item[2] < provider_names:  # still has untried providers
                    work_q.put(item)
                    moved += 1
            except queue.Empty:
                break

        if work_q.empty():
            break

        if round_num > 0:
            logger.info("round=%d retrying=%d pairs with remaining providers", round_num + 1, moved)

        # Start workers for all providers
        threads: list[threading.Thread] = []
        for provider in providers:
            limiter = _RateLimiter(provider.rate)
            for i in range(provider.workers):
                t = threading.Thread(
                    target=_worker,
                    args=(provider, limiter),
                    name=f"{provider.name}-{i}",
                )
                t.start()
                threads.append(t)

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            logger.warning("interrupted — saving progress")
            # Signal workers to stop
            for _ in threads:
                work_q.put((_STOP[0], _STOP[1], frozenset()))

    logger.info(
        "enrichment_done matched=%d/%d",
        counters["matched"], len(pairs),
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
    found = 0

    genre_map: dict[tuple[str, str], str] = {}

    for i, row in enumerate(missing.iter_rows(named=True)):
        artist, title = row["artist"], row["title"]
        limiter.wait()
        genre = itunes.lookup_genre(artist, title)
        if genre:
            genre_map[(artist, title)] = genre
            found += 1

        if (i + 1) % 100 == 0:
            logger.info("genre_backfill progress=%d/%d found=%d", i + 1, len(missing), found)

    if not genre_map:
        logger.info("no new genres found")
        return tracks_df

    result = tracks_df.with_columns(
        pl.struct(["artist", "title", "genre"])
        .map_elements(
            lambda s: genre_map.get((s["artist"], s["title"]), s["genre"]),
            return_dtype=pl.Utf8,
        )
        .alias("genre")
    )

    logger.info("genre_backfill_done filled=%d/%d", found, len(missing))

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

    id_map = tracks_df.select(["artist", "title", "track_id"]).unique(
        subset=["artist", "title"], keep="first"
    )

    if "track_id" in playlist_df.columns:
        base = playlist_df.drop("track_id")
    else:
        base = playlist_df

    updated = base.join(id_map, on=["artist", "title"], how="left")
    return updated.select(playlist_df.columns)
