from __future__ import annotations

import datetime
from pathlib import Path

import duckdb
import polars as pl

DATA_DIR = Path("data")
ANALYTICS_DIR = DATA_DIR / "analytics"
PLAYLIST_PATH = DATA_DIR / "playlist.parquet"
TRACKS_PATH = DATA_DIR / "tracks.parquet"

PLAYLIST_SCHEMA: dict[str, pl.PolarsDataType] = {
    "date": pl.Date,
    "time": pl.Utf8,
    "show": pl.Utf8,
    "artist": pl.Utf8,
    "title": pl.Utf8,
    "spotify_track_id": pl.Utf8,
}

TRACKS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "spotify_track_id": pl.Utf8,
    "artist": pl.Utf8,
    "title": pl.Utf8,
    "spotify_artist": pl.Utf8,
    "spotify_title": pl.Utf8,
    "duration_ms": pl.Int64,
    "popularity": pl.Int64,
    "album": pl.Utf8,
    "release_date": pl.Utf8,
    "genres": pl.Utf8,
    "energy": pl.Float64,
    "danceability": pl.Float64,
    "valence": pl.Float64,
    "tempo": pl.Float64,
    "acousticness": pl.Float64,
    "instrumentalness": pl.Float64,
}


def load_playlist() -> pl.DataFrame:
    if PLAYLIST_PATH.exists():
        return pl.read_parquet(PLAYLIST_PATH)
    return pl.DataFrame(schema=PLAYLIST_SCHEMA)


def save_playlist(df: pl.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(PLAYLIST_PATH)


def get_scraped_dates() -> frozenset[datetime.date]:
    if not PLAYLIST_PATH.exists():
        return frozenset()
    df = pl.read_parquet(PLAYLIST_PATH, columns=["date"])
    return frozenset(df["date"].unique().to_list())


def load_tracks() -> pl.DataFrame:
    if TRACKS_PATH.exists():
        return pl.read_parquet(TRACKS_PATH)
    return pl.DataFrame(schema=TRACKS_SCHEMA)


def save_tracks(df: pl.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(TRACKS_PATH)


def query(sql: str) -> pl.DataFrame:
    con = duckdb.connect()
    if PLAYLIST_PATH.exists():
        con.register("playlist", con.read_parquet(str(PLAYLIST_PATH)))
    if TRACKS_PATH.exists():
        con.register("tracks", con.read_parquet(str(TRACKS_PATH)))
    return con.execute(sql).pl()
