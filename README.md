# Radio 357 Playlist Analyzer

**[Live Report](https://debitcredit.github.io/radio/)**

[Radio 357](https://radio357.pl) is a Polish independent radio station that I've been listening to for years. At some point I started feeling like there's less music and more talk than there used to be. Instead of just complaining about it, I decided to find out — scrape every playlist they've published since mid-2022, enrich the tracks with metadata from music APIs, and see what the data actually says.

This tool answers questions like: is the music-to-talk ratio really shrinking? What genres dominate the airwaves? Which artists and songs get the most play? How does programming differ across shows?

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

## Technical details

### Pipeline

Four incremental stages — each can be re-run independently, only processing what's new:

```
scrape ──> enrich ──> analyze ──> report
 HTML       APIs       SQL        HTML
```

- **Scrape**: async HTTP fetches of daily playlist pages, parsed with BeautifulSoup. Skips already-scraped dates.
- **Enrich**: deduplicates ~334k plays into ~84k unique (artist, title) pairs, then resolves each against providers in sequence (Deezer → iTunes → Spotify). Misses cascade to the next provider. Saves to disk every 500 tracks for crash resilience.
- **Analyze**: DuckDB SQL joins playlist with enriched track metadata, outputs summary Parquets.
- **Report**: Jinja2 template with embedded Plotly chart JSON. Fully static, served via GitHub Pages.

### Stack

| Layer | Tool | Why |
|-------|------|-----|
| HTTP | httpx + asyncio | Async scraping with semaphore concurrency |
| Parsing | BeautifulSoup + lxml | Fast HTML parsing |
| Storage | Polars + Parquet | Columnar format, fast aggregation, no server |
| Analytics | DuckDB | SQL joins over Parquet, in-process |
| Charts | Plotly | Interactive charts that work in a static HTML file |
| CLI | Click | Subcommands with shared flags |
