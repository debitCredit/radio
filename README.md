# Radio 357 Playlist Analyzer

**[Live Report](https://debitcredit.github.io/radio/)**

Scrapes daily playlists from [Radio 357](https://radio357.pl), enriches tracks with metadata from multiple music providers, and generates a static analytics report.

## What it does

1. **Scrape** — fetches playlist history from radio357.pl (back to mid-2022, ~334k songs). The playlist paywall is client-side JS only; raw HTTP responses contain all data.
2. **Enrich** — matches each unique (artist, title) pair against Deezer, iTunes, and Spotify to collect duration, album, release date, genre, and explicit flags. Providers are tried sequentially — misses from one cascade to the next.
3. **Analyze** — joins playlist plays with track metadata via DuckDB to produce daily, weekly, program, and release decade summaries.
4. **Report** — generates a self-contained `docs/index.html` with interactive Plotly charts (music vs talk time trends, release decades, program breakdown, top artists/songs).

## Setup

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Spotify enrichment (optional) requires credentials in `.env`:

```
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
```

Deezer and iTunes require no authentication.

## Usage

```bash
# Scrape all available dates (incremental — skips already-scraped days)
uv run radio scrape

# Scrape a specific date range
uv run radio scrape --from 2023-01-01 --to 2023-12-31

# Enrich tracks with metadata (Deezer -> iTunes -> Spotify)
uv run radio enrich

# Compute analytics summaries
uv run radio analyze

# Generate static HTML report
uv run radio report

# Run ad-hoc SQL against playlist and tracks tables
uv run radio query "SELECT artist, COUNT(*) AS plays FROM playlist GROUP BY artist ORDER BY plays DESC LIMIT 10"

# Show summary statistics
uv run radio stats
```

Add `-v` for debug logging: `uv run radio -v enrich`

## Architecture

```
src/radio/
  cli.py              Click CLI entrypoint
  scraper.py           Async httpx scraper for radio357.pl playlists
  enrichment.py        Multi-provider enrichment orchestrator
  storage.py           Polars Parquet I/O + DuckDB query helpers
  analytics.py         SQL-based summary computation
  report.py            Static HTML report generation (Plotly + Jinja2)
  providers/
    __init__.py        TrackMatch dataclass, normalize(), match_confidence()
    deezer.py          Deezer API (10 req/s, no auth)
    itunes.py          iTunes Search API (0.33 req/s, no auth)
    spotify_provider.py  Spotify API via spotipy (client credentials)
```

## Data

All data lives in `data/` (gitignored):

| File | Description |
|------|-------------|
| `playlist.parquet` | One row per song play (date, time, program, artist, title, track_id) |
| `tracks.parquet` | One row per unique track (matched metadata from providers) |
| `analytics/*.parquet` | Precomputed daily, weekly, program, and decade summaries |

Report output: `docs/index.html` (committed, served via GitHub Pages).

## Enrichment pipeline

Providers are tried in order: **Deezer** (fastest, no auth) -> **iTunes** (has genre data) -> **Spotify** (optional, needs credentials). Each provider processes all remaining unmatched pairs before passing misses to the next.

Match quality is controlled by confidence scoring (40% artist similarity + 60% title similarity via `SequenceMatcher`). Tracks below 0.6 confidence are rejected. Name normalization strips diacritics and featuring tags before comparison.

After enrichment, a genre backfill pass uses iTunes to fill in genres for tracks found by Deezer or Spotify (which don't return genre in search results).
