from __future__ import annotations

import logging

import polars as pl

from radio import storage

logger = logging.getLogger(__name__)

_DAILY_SQL = """
    SELECT
        p.date,
        COUNT(*) AS total_songs,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs,
        COUNT(DISTINCT p.artist) AS unique_artists,
        SUM(t.duration_ms) / 1000.0 / 60.0 AS music_minutes,
        1440.0 - SUM(t.duration_ms) / 1000.0 / 60.0 AS talk_minutes,
        (SUM(t.duration_ms) / 1000.0 / 60.0) / 1440.0 * 100 AS music_pct,
        SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS explicit_count
    FROM playlist p
    LEFT JOIN tracks t ON p.track_id = t.track_id
    GROUP BY p.date
    ORDER BY p.date
"""

_WEEKLY_SQL = """
    SELECT
        EXTRACT(isoyear FROM p.date) AS iso_year,
        EXTRACT(week FROM p.date) AS iso_week,
        COUNT(*) AS total_songs,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs,
        COUNT(DISTINCT p.artist) AS unique_artists,
        COUNT(DISTINCT p.date) AS days_in_week,
        SUM(t.duration_ms) / 1000.0 / 60.0 AS music_minutes,
        COUNT(DISTINCT p.date) * 1440.0 - SUM(t.duration_ms) / 1000.0 / 60.0 AS talk_minutes,
        (SUM(t.duration_ms) / 1000.0 / 60.0) / (COUNT(DISTINCT p.date) * 1440.0) * 100 AS music_pct,
        SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS explicit_count
    FROM playlist p
    LEFT JOIN tracks t ON p.track_id = t.track_id
    GROUP BY iso_year, iso_week
    ORDER BY iso_year, iso_week
"""

_PROGRAM_SQL = """
    SELECT
        p.program,
        COUNT(*) AS total_plays,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs,
        COUNT(DISTINCT p.artist) AS unique_artists,
        SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS explicit_count
    FROM playlist p
    LEFT JOIN tracks t ON p.track_id = t.track_id
    GROUP BY p.program
    ORDER BY p.program
"""

_DECADES_SQL = """
    SELECT
        CAST(SUBSTRING(t.release_date, 1, 3) || '0' AS VARCHAR) || 's' AS decade,
        COUNT(*) AS play_count,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs
    FROM playlist p
    JOIN tracks t ON p.track_id = t.track_id
    WHERE t.release_date IS NOT NULL AND LENGTH(t.release_date) >= 4
    GROUP BY decade
    ORDER BY decade
"""

_GENRE_SQL = """
    SELECT
        t.genre,
        COUNT(*) AS play_count,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs,
        COUNT(DISTINCT p.artist) AS unique_artists
    FROM playlist p
    JOIN tracks t ON p.track_id = t.track_id
    WHERE t.genre IS NOT NULL AND t.genre != ''
    GROUP BY t.genre
    ORDER BY play_count DESC
"""

_WEEKLY_HHI_SQL = """
    SELECT iso_year, iso_week, SUM(share * share) AS artist_hhi
    FROM (
        SELECT
            EXTRACT(isoyear FROM date) AS iso_year,
            EXTRACT(week FROM date) AS iso_week,
            artist,
            CAST(COUNT(*) AS DOUBLE) / SUM(COUNT(*)) OVER (
                PARTITION BY EXTRACT(isoyear FROM date), EXTRACT(week FROM date)
            ) AS share
        FROM playlist
        GROUP BY iso_year, iso_week, artist
    )
    GROUP BY iso_year, iso_week
    ORDER BY iso_year, iso_week
"""


def _compute_eclecticity(playlist_df: pl.DataFrame) -> pl.DataFrame:
    """Compute weekly eclecticity metrics from playlist data.

    Metrics:
    - unique_ratio: unique songs / total plays (1.0 = no repeats)
    - new_song_pct: % of plays that are songs not seen in the prior 90 days
    """
    p = playlist_df.sort("date")

    # Build a map of (artist, title) -> set of dates played
    song_dates: dict[tuple[str, str], list] = {}
    for row in p.iter_rows(named=True):
        key = (row["artist"], row["title"])
        song_dates.setdefault(key, []).append(row["date"])

    # For each play, check if the song appeared in the 90 days before this date
    import datetime

    lookback = datetime.timedelta(days=90)
    is_fresh: list[bool] = []
    for row in p.iter_rows(named=True):
        key = (row["artist"], row["title"])
        date = row["date"]
        cutoff = date - lookback
        # Check if any prior play exists within the lookback window
        prior = any(cutoff <= d < date for d in song_dates[key])
        is_fresh.append(not prior)

    p = p.with_columns(pl.Series("is_fresh", is_fresh))

    weekly = (
        p.with_columns(
            pl.col("date").dt.iso_year().alias("iso_year"),
            pl.col("date").dt.week().alias("iso_week"),
        )
        .group_by(["iso_year", "iso_week"])
        .agg(
            pl.len().alias("total_plays"),
            pl.struct(["artist", "title"]).n_unique().alias("unique_songs"),
            pl.col("is_fresh").sum().alias("fresh_songs"),
        )
        .with_columns(
            (pl.col("unique_songs").cast(pl.Float64) / pl.col("total_plays")).alias("unique_ratio"),
            (pl.col("fresh_songs").cast(pl.Float64) / pl.col("total_plays") * 100).alias("new_song_pct"),
        )
        .sort(["iso_year", "iso_week"])
    )

    return weekly


def compute_all() -> None:
    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    results = storage.query_many({
        "daily": _DAILY_SQL,
        "weekly": _WEEKLY_SQL,
        "program": _PROGRAM_SQL,
        "decades": _DECADES_SQL,
        "genre": _GENRE_SQL,
        "weekly_hhi": _WEEKLY_HHI_SQL,
    })

    # Compute eclecticity from raw playlist (needs Polars, not SQL)
    playlist_df = storage.load_playlist()
    eclecticity = _compute_eclecticity(playlist_df)

    # Merge HHI into eclecticity
    hhi = results["weekly_hhi"]
    eclecticity = eclecticity.join(
        hhi.select(["iso_year", "iso_week", "artist_hhi"]),
        on=["iso_year", "iso_week"],
        how="left",
    )

    outputs = {
        "daily": ("daily_summary.parquet", results["daily"]),
        "weekly": ("weekly_summary.parquet", results["weekly"]),
        "program": ("program_summary.parquet", results["program"]),
        "decades": ("release_decade_summary.parquet", results["decades"]),
        "genre": ("genre_summary.parquet", results["genre"]),
        "eclecticity": ("eclecticity.parquet", eclecticity),
    }

    for name, (filename, df) in outputs.items():
        path = storage.ANALYTICS_DIR / filename
        df.write_parquet(path)
        logger.info("%s rows=%d path=%s", name, len(df), path)
