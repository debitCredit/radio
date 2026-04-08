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
    LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
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
    LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
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
    LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
    GROUP BY p.program
    ORDER BY p.program
"""

_DECADES_SQL = """
    SELECT
        CAST(SUBSTRING(t.release_date, 1, 3) || '0' AS VARCHAR) || 's' AS decade,
        COUNT(*) AS play_count,
        COUNT(DISTINCT p.artist || ' - ' || p.title) AS unique_songs
    FROM playlist p
    JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
    WHERE t.release_date IS NOT NULL AND LENGTH(t.release_date) >= 4
    GROUP BY decade
    ORDER BY decade
"""


def compute_all() -> None:
    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    results = storage.query_many({
        "daily": _DAILY_SQL,
        "weekly": _WEEKLY_SQL,
        "program": _PROGRAM_SQL,
        "decades": _DECADES_SQL,
    })

    outputs = {
        "daily": ("daily_summary.parquet", results["daily"]),
        "weekly": ("weekly_summary.parquet", results["weekly"]),
        "program": ("program_summary.parquet", results["program"]),
        "decades": ("release_decade_summary.parquet", results["decades"]),
    }

    for name, (filename, df) in outputs.items():
        path = storage.ANALYTICS_DIR / filename
        df.write_parquet(path)
        logger.info("%s rows=%d path=%s", name, len(df), path)
