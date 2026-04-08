# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Radio 357 playlist scraper, Spotify enrichment, and analytics. Scrapes playlist data from radio357.pl, enriches tracks with Spotify metadata (duration, genres, audio features), and generates a static analytics report for GitHub Pages.

Python 3.13+, managed with uv, src layout with hatchling.

## Commands

- `uv run radio scrape` — scrape all available dates incrementally
- `uv run radio scrape --from 2023-01-01 --to 2023-12-31` — scrape date range
- `uv run radio enrich` — enrich tracks with Spotify metadata (requires `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` in `.env`)
- `uv run radio analyze` — precompute daily/weekly/program summary parquets
- `uv run radio query "SELECT ..."` — run SQL against playlist and tracks tables
- `uv run radio stats` — program summary statistics
- `uv run radio report` — generate static HTML report to `docs/index.html`

## Architecture

- `src/radio/scraper.py` — async httpx scraper for radio357.pl playlists. Paywall is client-side JS only; raw HTTP gets all songs. Songs parsed from `.programGroup` / `.programGroupSong` HTML structure.
- `src/radio/storage.py` — Polars-based Parquet I/O. Two main tables: `playlist` (one row per song play) and `tracks` (deduplicated Spotify metadata). DuckDB query helper registers both.
- `src/radio/spotify.py` — Spotify enrichment via spotipy client credentials flow. Deduplicates (artist, title) pairs before querying. Audio features endpoint wrapped in try/except (may be deprecated).
- `src/radio/analytics.py` — DuckDB SQL joins playlist+tracks to precompute daily, weekly, and program summaries. Top genre computed via Polars post-processing (comma-separated genres field).
- `src/radio/report.py` — generates self-contained `docs/index.html` with Plotly charts and styled tables. Jinja2 template inline.
- `src/radio/cli.py` — Click CLI group wiring everything together.

## Data

All data lives in `data/` (gitignored):
- `data/playlist.parquet` — scraped song plays
- `data/tracks.parquet` — deduplicated Spotify track metadata
- `data/analytics/*.parquet` — precomputed summaries

Report output: `docs/index.html` (committed, served via GitHub Pages).
