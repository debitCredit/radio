from __future__ import annotations

import polars as pl

from radio import storage


def compute_daily_summary() -> pl.DataFrame:
    sql = """
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
    df = storage.query(sql)

    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(storage.ANALYTICS_DIR / "daily_summary.parquet")
    return df


def compute_weekly_summary() -> pl.DataFrame:
    sql = """
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
    df = storage.query(sql)

    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(storage.ANALYTICS_DIR / "weekly_summary.parquet")
    return df


def compute_program_summary() -> pl.DataFrame:
    sql = """
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
    df = storage.query(sql)

    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(storage.ANALYTICS_DIR / "program_summary.parquet")
    return df


def compute_release_year_summary() -> pl.DataFrame:
    """How old is the music Radio 357 plays? Group tracks by release decade."""
    sql = """
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
    df = storage.query(sql)

    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(storage.ANALYTICS_DIR / "release_decade_summary.parquet")
    return df


def compute_all() -> None:
    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    daily = compute_daily_summary()
    print(f"daily_summary: {len(daily)} rows -> {storage.ANALYTICS_DIR / 'daily_summary.parquet'}")

    weekly = compute_weekly_summary()
    print(f"weekly_summary: {len(weekly)} rows -> {storage.ANALYTICS_DIR / 'weekly_summary.parquet'}")

    program = compute_program_summary()
    print(f"program_summary: {len(program)} rows -> {storage.ANALYTICS_DIR / 'program_summary.parquet'}")

    decades = compute_release_year_summary()
    print(f"release_decade_summary: {len(decades)} rows -> {storage.ANALYTICS_DIR / 'release_decade_summary.parquet'}")
