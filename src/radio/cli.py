from __future__ import annotations

import asyncio
import datetime
import logging

import click
import httpx
import polars as pl

from radio import analytics, storage
from radio.enrichment import (
    backfill_genres,
    enrich_tracks,
    get_unenriched_pairs,
    update_playlist_with_track_ids,
)
from radio.scraper import SongPlay, find_earliest_date, scrape_range

log = logging.getLogger("radio.cli")


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    """Radio 357 playlist tools."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
        level=level,
    )


@cli.command()
@click.option("--from", "from_date", default=None, help="Start date YYYY-MM-DD")
@click.option("--to", "to_date", default=None, help="End date YYYY-MM-DD")
def scrape(from_date: str | None, to_date: str | None) -> None:
    """Scrape playlist data from radio357.pl."""
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    async def _resolve_earliest() -> datetime.date:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            return await find_earliest_date(client)

    if from_date is None and to_date is None:
        log.info("Finding earliest available date...")
        start = asyncio.run(_resolve_earliest())
        end = yesterday
    elif from_date is not None and to_date is None:
        start = datetime.date.fromisoformat(from_date)
        end = yesterday
    elif from_date is None and to_date is not None:
        log.info("Finding earliest available date...")
        start = asyncio.run(_resolve_earliest())
        end = datetime.date.fromisoformat(to_date)
    else:
        start = datetime.date.fromisoformat(from_date)
        end = datetime.date.fromisoformat(to_date)

    existing = storage.load_playlist()
    skip_dates = frozenset(existing["date"].unique().to_list()) if not existing.is_empty() else frozenset()

    plays: tuple[SongPlay, ...] = asyncio.run(
        scrape_range(start, end, skip_dates=skip_dates)
    )

    if not plays:
        log.info("No new songs scraped")
        return

    new_df = pl.DataFrame(
        [
            {
                "date": p.date,
                "time": p.time,
                "program": p.program,
                "artist": p.artist,
                "title": p.title,
                "track_id": None,
            }
            for p in plays
        ],
        schema=storage.PLAYLIST_SCHEMA,
    )

    combined = pl.concat([existing, new_df]) if not existing.is_empty() else new_df
    storage.save_playlist(combined)

    scraped_dates = sorted({p.date for p in plays})
    log.info(
        "saved songs=%d range=%s..%s",
        len(plays), scraped_dates[0], scraped_dates[-1],
    )


@cli.command()
def enrich() -> None:
    """Enrich tracks using iTunes, Deezer, and Spotify in parallel."""
    playlist_df = storage.load_playlist()
    tracks_df = storage.load_tracks()

    pairs = get_unenriched_pairs(playlist_df, tracks_df)

    if not pairs:
        log.info("All tracks already enriched")
        return

    log.info("enriching pairs=%d", len(pairs))

    def _save_batch(batch: pl.DataFrame) -> None:
        nonlocal tracks_df
        tracks_df = pl.concat([tracks_df, batch]) if not tracks_df.is_empty() else batch
        storage.save_tracks(tracks_df)

    remaining = enrich_tracks(pairs, on_batch=_save_batch)

    if not remaining.is_empty():
        tracks_df = pl.concat([tracks_df, remaining]) if not tracks_df.is_empty() else remaining
        storage.save_tracks(tracks_df)

    # Backfill genres for tracks found by providers that don't return genre
    tracks_df = backfill_genres(tracks_df, on_save=storage.save_tracks)

    # Update playlist with track IDs
    updated_playlist = update_playlist_with_track_ids(playlist_df, tracks_df)
    storage.save_playlist(updated_playlist)

    log.info("enrichment_complete tracks_total=%d", len(tracks_df))


@cli.command()
def analyze() -> None:
    """Compute analytics summaries."""
    analytics.compute_all()


@cli.command()
@click.argument("sql")
def query(sql: str) -> None:
    """Run a SQL query against playlist and tracks tables."""
    result = storage.query(sql)
    print(result)


@cli.command()
def stats() -> None:
    """Show summary statistics for the collected data."""
    playlist = storage.load_playlist()

    if playlist.is_empty():
        print("No playlist data found.")
        return

    total_songs = len(playlist)
    dates = playlist["date"].drop_nulls()
    date_min = dates.min()
    date_max = dates.max()
    unique_artists = playlist["artist"].n_unique()
    unique_songs = (
        playlist.select(["artist", "title"]).unique().height
    )

    print(f"Total songs:     {total_songs}")
    print(f"Date range:      {date_min} to {date_max}")
    print(f"Unique artists:  {unique_artists}")
    print(f"Unique songs:    {unique_songs}")

    if storage.TRACKS_PATH.exists():
        tracks = storage.load_tracks()
        enriched = playlist["track_id"].drop_nulls().len()
        coverage = enriched / total_songs * 100 if total_songs else 0.0
        print(f"Enrichment:      {enriched}/{total_songs} ({coverage:.1f}%)")

        if "source" in tracks.columns:
            source_counts = tracks.group_by("source").agg(pl.len().alias("count")).sort("count", descending=True)
            for row in source_counts.iter_rows(named=True):
                print(f"  {row['source']}: {row['count']}")

    daily_path = storage.ANALYTICS_DIR / "daily_summary.parquet"
    if daily_path.exists():
        daily = pl.read_parquet(daily_path)
        if "music_pct" in daily.columns:
            avg_music_pct = daily["music_pct"].drop_nulls().mean()
            if avg_music_pct is not None:
                print(f"Avg music %:     {avg_music_pct:.1f}%")
            else:
                print("Avg music %:     N/A")


@cli.command()
def report() -> None:
    """Generate a static HTML report."""
    from radio import report as report_module
    report_module.generate_report()
