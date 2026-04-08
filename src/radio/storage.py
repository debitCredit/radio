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
    "program": pl.Utf8,
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
    "explicit": pl.Boolean,
    "album": pl.Utf8,
    "release_date": pl.Utf8,
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


def _make_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with playlist and tracks registered."""
    con = duckdb.connect()
    if PLAYLIST_PATH.exists():
        con.register("playlist", con.read_parquet(str(PLAYLIST_PATH)))
    if TRACKS_PATH.exists():
        con.register("tracks", con.read_parquet(str(TRACKS_PATH)))
    return con


def query(sql: str) -> pl.DataFrame:
    """Run a single SQL query."""
    con = _make_connection()
    return con.execute(sql).pl()


def query_many(queries: dict[str, str]) -> dict[str, pl.DataFrame]:
    """Run multiple SQL queries against a single connection, avoiding re-reads."""
    con = _make_connection()
    return {name: con.execute(sql).pl() for name, sql in queries.items()}
