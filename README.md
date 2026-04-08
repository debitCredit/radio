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

The system runs as a four-stage pipeline, each stage incremental:

```
scrape ──> enrich ──> analyze ──> report
 HTML       APIs       SQL        HTML
```

**Scraping** is async (`httpx` + `asyncio`) with semaphore-limited concurrency. Radio 357's playlist paywall is client-side JavaScript only — the raw HTTP response contains all songs. Each day's page is parsed with BeautifulSoup, extracting program groups (`.programGroup` headers) and songs (`.programGroupSong` elements). The scraper finds the earliest available date via binary search and only fetches days not already in storage.

**Enrichment** deduplicates (artist, title) pairs from the playlist (334k plays → ~84k unique pairs) and resolves each against music provider APIs. Providers are tried in sequence — Deezer first (fastest, no auth, 10 req/s), then iTunes (has genre data, 0.33 req/s), then Spotify (optional, needs credentials). Each provider processes all remaining unmatched pairs before passing its misses to the next. This avoids wasting slow providers on tracks that faster ones already found.

**Analytics** joins playlist plays with track metadata in DuckDB to produce daily, weekly, program, genre, and release decade summaries — all written as Parquet files.

**Reporting** generates a single self-contained HTML file with embedded Plotly charts and inline data. No server, no API calls — the report works as a static file served via GitHub Pages.

### Data model

Two core tables, separated to avoid redundant API calls (the same song can appear hundreds of times in the playlist):

```
playlist.parquet                   tracks.parquet
┌──────────────────────┐           ┌──────────────────────────┐
│ date       (Date)    │           │ track_id      (Utf8)  PK │
│ time       (Utf8)    │     ┌────>│ artist        (Utf8)     │
│ program    (Utf8)    │     │     │ title         (Utf8)     │
│ artist     (Utf8)    │     │     │ matched_artist (Utf8)    │
│ title      (Utf8)    │     │     │ matched_title  (Utf8)    │
│ track_id   (Utf8) ───┼─────┘     │ duration_ms   (Int64)   │
└──────────────────────┘           │ explicit      (Boolean)  │
                                   │ album         (Utf8)     │
                                   │ release_date  (Utf8)     │
                                   │ genre         (Utf8)     │
                                   │ source        (Utf8)     │
                                   │ confidence    (Float64)  │
                                   └──────────────────────────┘
```

Both `artist`/`title` (as scraped from the radio) and `matched_artist`/`matched_title` (as returned by the provider) are stored, since radio names and provider names frequently differ (diacritics, featuring tags, spelling variations).

### Match quality

Providers return multiple candidates per search. Each is scored using `SequenceMatcher` with weighted similarity: 40% artist + 60% title. Before comparison, names are normalized — unicode diacritics are decomposed, `feat`/`ft`/`featuring` tags stripped, parenthetical suffixes removed, and whitespace collapsed. Matches below 0.6 confidence are rejected.

### Rate limiting and resilience

Each provider has a token-bucket rate limiter calibrated to its documented limits. Enrichment saves progress to disk every 500 tracks, so a crash or rate ban doesn't lose hours of work. Providers handle bans differently:

- **Deezer**: returns 200 with error code 4 on quota — backs off exponentially (5s, 10s, 20s)
- **iTunes**: returns 429/403 — backs off (10s, 20s, 40s)
- **Spotify**: on any 429, the provider disables itself globally for the rest of the session to avoid extended bans

### Stack

| Layer | Tool | Why |
|-------|------|-----|
| Scraping | httpx + asyncio + BeautifulSoup | Async HTTP with semaphore concurrency, lxml parser for speed |
| Storage | Polars + Parquet | Columnar format, fast aggregation, no database server needed |
| Analytics | DuckDB | SQL over Parquet files, single-process, joins playlist with tracks |
| Reporting | Plotly + Jinja2 | Interactive charts embedded as JSON in a static HTML file |
| CLI | Click | Subcommands with shared options (`-v` for debug logging) |
