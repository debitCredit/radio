from __future__ import annotations

import asyncio
import datetime

import click
import polars as pl

from radio import analytics, storage
from radio.scraper import SongPlay, find_earliest_date, scrape_range
from radio.spotify import enrich_tracks, get_unenriched_pairs, update_playlist_with_track_ids

import httpx


@click.group()
def cli() -> None:
    """Radio 357 playlist tools."""


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
        print("Finding earliest available date...")
        start = asyncio.run(_resolve_earliest())
        end = yesterday
    elif from_date is not None and to_date is None:
        start = datetime.date.fromisoformat(from_date)
        end = yesterday
    elif from_date is None and to_date is not None:
        print("Finding earliest available date...")
        start = asyncio.run(_resolve_earliest())
        end = datetime.date.fromisoformat(to_date)
    else:
        start = datetime.date.fromisoformat(from_date)
        end = datetime.date.fromisoformat(to_date)

    print(f"Scraping {start} to {end}...")

    skip_dates = storage.get_scraped_dates()
    if skip_dates:
        print(f"Skipping {len(skip_dates)} already-scraped dates")

    plays: tuple[SongPlay, ...] = asyncio.run(
        scrape_range(start, end, skip_dates=skip_dates)
    )

    if not plays:
        print("No new songs scraped.")
        return

    new_df = pl.DataFrame(
        [
            {
                "date": p.date,
                "time": p.time,
                "program": p.program,
                "artist": p.artist,
                "title": p.title,
                "spotify_track_id": None,
            }
            for p in plays
        ],
        schema=storage.PLAYLIST_SCHEMA,
    )

    existing = storage.load_playlist()
    combined = pl.concat([existing, new_df]) if not existing.is_empty() else new_df
    storage.save_playlist(combined)

    scraped_dates = sorted({p.date for p in plays})
    print(
        f"Scraped {len(plays)} new songs "
        f"({scraped_dates[0]} to {scraped_dates[-1]})"
    )


@cli.command()
def enrich() -> None:
    """Run Spotify enrichment on unenriched tracks."""
    playlist_df = storage.load_playlist()
    tracks_df = storage.load_tracks()

    pairs = get_unenriched_pairs(playlist_df, tracks_df)

    if not pairs:
        print("All tracks enriched.")
        return

    print(f"Enriching {len(pairs)} unenriched (artist, title) pairs...")
    new_tracks = enrich_tracks(pairs)

    if not new_tracks.is_empty():
        combined_tracks = (
            pl.concat([tracks_df, new_tracks]) if not tracks_df.is_empty() else new_tracks
        )
        storage.save_tracks(combined_tracks)

        updated_playlist = update_playlist_with_track_ids(playlist_df, combined_tracks)
        storage.save_playlist(updated_playlist)

        print(
            f"Enriched {len(new_tracks)}/{len(pairs)} pairs; "
            f"tracks total: {len(combined_tracks)}"
        )
    else:
        print(f"No tracks enriched (0/{len(pairs)} matched on Spotify).")


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
        enriched = playlist["spotify_track_id"].drop_nulls().len()
        coverage = enriched / total_songs * 100 if total_songs else 0.0
        print(f"Enrichment:      {enriched}/{total_songs} ({coverage:.1f}%)")

    daily_path = storage.ANALYTICS_DIR / "daily_summary.parquet"
    if daily_path.exists():
        daily = pl.read_parquet(daily_path)
        if "music_pct" in daily.columns:
            avg_music_pct = daily["music_pct"].drop_nulls().mean()
            print(f"Avg music %:     {avg_music_pct:.1f}%" if avg_music_pct is not None else "Avg music %:     N/A")


@cli.command()
def report() -> None:
    """Generate a static HTML report."""
    from radio import report as report_module
    report_module.generate_report()
