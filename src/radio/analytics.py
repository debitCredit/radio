from __future__ import annotations

import polars as pl

from radio import storage


def _top_genre_by_group(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    """Given a DataFrame with group_cols and a 'genres' column, return a DataFrame
    with group_cols + 'top_genre' where top_genre is the most frequent genre."""
    rows = []
    for key, group in df.group_by(group_cols):
        all_genres: list[str] = []
        for genres_val in group["genres"].to_list():
            if genres_val:
                all_genres.extend(g.strip() for g in genres_val.split(",") if g.strip())
        if all_genres:
            top = max(set(all_genres), key=all_genres.count)
        else:
            top = None
        row = dict(zip(group_cols, key))
        row["top_genre"] = top
        rows.append(row)

    if not rows:
        schema = {col: df.schema[col] for col in group_cols}
        schema["top_genre"] = pl.Utf8
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows)


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
            AVG(t.energy) AS avg_energy,
            AVG(t.danceability) AS avg_danceability,
            AVG(t.valence) AS avg_valence,
            AVG(t.tempo) AS avg_tempo,
            AVG(t.popularity) AS avg_popularity
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        GROUP BY p.date
        ORDER BY p.date
    """
    base = storage.query(sql)

    genres_sql = """
        SELECT p.date, t.genres
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        WHERE t.genres IS NOT NULL AND t.genres != ''
    """
    genres_df = storage.query(genres_sql)
    top_genre_df = _top_genre_by_group(genres_df, ["date"])

    df = base.join(top_genre_df, on="date", how="left")

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
            AVG(t.energy) AS avg_energy,
            AVG(t.danceability) AS avg_danceability,
            AVG(t.valence) AS avg_valence,
            AVG(t.tempo) AS avg_tempo,
            AVG(t.popularity) AS avg_popularity
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        GROUP BY iso_year, iso_week
        ORDER BY iso_year, iso_week
    """
    base = storage.query(sql)

    genres_sql = """
        SELECT
            EXTRACT(isoyear FROM p.date) AS iso_year,
            EXTRACT(week FROM p.date) AS iso_week,
            t.genres
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        WHERE t.genres IS NOT NULL AND t.genres != ''
    """
    genres_df = storage.query(genres_sql)
    top_genre_df = _top_genre_by_group(genres_df, ["iso_year", "iso_week"])

    df = base.join(top_genre_df, on=["iso_year", "iso_week"], how="left")

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
            AVG(t.energy) AS avg_energy,
            AVG(t.danceability) AS avg_danceability,
            AVG(t.valence) AS avg_valence
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        GROUP BY p.program
        ORDER BY p.program
    """
    base = storage.query(sql)

    genres_sql = """
        SELECT p.program, t.genres
        FROM playlist p
        LEFT JOIN tracks t ON p.spotify_track_id = t.spotify_track_id
        WHERE t.genres IS NOT NULL AND t.genres != ''
    """
    genres_df = storage.query(genres_sql)
    top_genre_df = _top_genre_by_group(genres_df, ["program"])

    df = base.join(top_genre_df, on="program", how="left")

    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(storage.ANALYTICS_DIR / "program_summary.parquet")
    return df


def compute_all() -> None:
    storage.ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    daily = compute_daily_summary()
    print(f"daily_summary: {len(daily)} rows -> {storage.ANALYTICS_DIR / 'daily_summary.parquet'}")

    weekly = compute_weekly_summary()
    print(f"weekly_summary: {len(weekly)} rows -> {storage.ANALYTICS_DIR / 'weekly_summary.parquet'}")

    program = compute_program_summary()
    print(f"program_summary: {len(program)} rows -> {storage.ANALYTICS_DIR / 'program_summary.parquet'}")
