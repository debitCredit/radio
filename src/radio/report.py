from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio
import polars as pl
from jinja2 import Template

from radio import storage

DOCS_DIR = Path("docs")

_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Radio 357 Analytics</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0f0f1a;
      color: #e0e0f0;
      font-family: 'Segoe UI', system-ui, sans-serif;
      font-size: 15px;
      line-height: 1.6;
    }

    .container {
      max-width: 1200px;
      margin: 0 auto;
      padding: 2rem 1.5rem;
    }

    header {
      text-align: center;
      padding: 3rem 0 2rem;
      border-bottom: 1px solid #2a2a4a;
      margin-bottom: 2.5rem;
    }

    header h1 {
      font-size: 2.4rem;
      font-weight: 700;
      color: #a78bfa;
      letter-spacing: 0.02em;
    }

    header p {
      color: #8888aa;
      margin-top: 0.5rem;
    }

    .section {
      background: #16162a;
      border: 1px solid #2a2a4a;
      border-radius: 12px;
      padding: 1.5rem 1.75rem;
      margin-bottom: 2rem;
    }

    .section h2 {
      font-size: 1.2rem;
      font-weight: 600;
      color: #c4b5fd;
      margin-bottom: 1.25rem;
      padding-bottom: 0.5rem;
      border-bottom: 1px solid #2a2a4a;
    }

    .chart-container {
      width: 100%;
      min-height: 380px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
    }

    thead th {
      background: #1e1e38;
      color: #a78bfa;
      text-align: left;
      padding: 0.6rem 0.9rem;
      font-weight: 600;
      border-bottom: 2px solid #2a2a4a;
    }

    tbody tr:nth-child(odd)  { background: #16162a; }
    tbody tr:nth-child(even) { background: #1a1a30; }

    tbody tr:hover { background: #22224a; }

    tbody td {
      padding: 0.55rem 0.9rem;
      border-bottom: 1px solid #22224a;
      color: #d0d0e8;
    }

    .rank {
      color: #6666aa;
      font-variant-numeric: tabular-nums;
      width: 3rem;
    }

    .count {
      text-align: right;
      color: #a78bfa;
      font-variant-numeric: tabular-nums;
    }

    footer {
      text-align: center;
      color: #44446a;
      font-size: 0.8rem;
      padding: 2rem 0 1rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Radio 357 Analytics</h1>
      <p>Auto-generated from playlist data</p>
    </header>

    <div class="section">
      <h2>Songs Per Day</h2>
      <div id="chart-songs-per-day" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Music vs Talk Time</h2>
      <div id="chart-music-pct" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Eclecticity — Unique Ratio</h2>
      <p style="color: #8888aa; font-size: 0.85rem; margin-bottom: 1rem;">Unique songs / total plays per week. Higher = less repetition.</p>
      <div id="chart-unique-ratio" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Eclecticity — Fresh Music Rate</h2>
      <p style="color: #8888aa; font-size: 0.85rem; margin-bottom: 1rem;">% of plays that are songs not heard in the prior 90 days.</p>
      <div id="chart-new-song" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Eclecticity — Artist Concentration</h2>
      <p style="color: #8888aa; font-size: 0.85rem; margin-bottom: 1rem;">Herfindahl-Hirschman Index of artist play share. Lower = more diverse.</p>
      <div id="chart-hhi" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Top Genres</h2>
      <div id="chart-genres" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Release Decades</h2>
      <div id="chart-decades" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Show Breakdown</h2>
      <div id="chart-shows" class="chart-container"></div>
    </div>

    <div class="section">
      <h2>Most Played Artists</h2>
      <table>
        <thead>
          <tr>
            <th class="rank">#</th>
            <th>Artist</th>
            <th class="count">Plays</th>
          </tr>
        </thead>
        <tbody>
          {% for row in top_artists %}
          <tr>
            <td class="rank">{{ loop.index }}</td>
            <td>{{ row.artist }}</td>
            <td class="count">{{ row.plays }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Most Played Songs</h2>
      <table>
        <thead>
          <tr>
            <th class="rank">#</th>
            <th>Artist</th>
            <th>Title</th>
            <th class="count">Plays</th>
          </tr>
        </thead>
        <tbody>
          {% for row in top_songs %}
          <tr>
            <td class="rank">{{ loop.index }}</td>
            <td>{{ row.artist }}</td>
            <td>{{ row.title }}</td>
            <td class="count">{{ row.plays }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <footer>Radio 357 Analytics &mdash; generated automatically</footer>
  </div>

  <script>
    var layout_defaults = {
      paper_bgcolor: '#16162a',
      plot_bgcolor: '#16162a',
      font: { color: '#e0e0f0', family: "'Segoe UI', system-ui, sans-serif" },
      margin: { t: 30, b: 60, l: 60, r: 30 },
    };

    function plot(id, fig) {
      var data = fig.data;
      var layout = Object.assign({}, layout_defaults, fig.layout);
      Plotly.newPlot(id, data, layout, { responsive: true, displayModeBar: false });
    }

    plot('chart-songs-per-day', {{ fig_songs_per_day | safe }});
    plot('chart-music-pct', {{ fig_music_pct | safe }});
    plot('chart-unique-ratio', {{ fig_unique_ratio | safe }});
    plot('chart-new-song', {{ fig_new_song | safe }});
    plot('chart-hhi', {{ fig_hhi | safe }});
    plot('chart-genres', {{ fig_genres | safe }});
    plot('chart-decades', {{ fig_decades | safe }});
    plot('chart-shows', {{ fig_shows | safe }});
  </script>
</body>
</html>
"""


def _load_parquet(path: Path) -> pl.DataFrame | None:
    if path.exists():
        return pl.read_parquet(path)
    return None


def _fig_to_json(fig: go.Figure) -> str:
    return pio.to_json(fig)


def _songs_per_day_figure(daily: pl.DataFrame) -> go.Figure:
    daily_sorted = daily.sort("date")
    dates = daily_sorted["date"].to_list()
    total = daily_sorted["total_songs"].to_list()

    rolling = (
        daily_sorted.with_columns(
            pl.col("total_songs").cast(pl.Float64)
            .rolling_mean(window_size=7, min_samples=1)
            .alias("rolling_7d")
        )["rolling_7d"]
        .to_list()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates, y=total, mode="lines", name="Daily",
            line={"color": "#6366f1", "width": 1}, opacity=0.5,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates, y=rolling, mode="lines", name="7-day avg",
            line={"color": "#a78bfa", "width": 2.5},
        )
    )
    fig.update_layout(
        template="plotly_dark",
        yaxis_title="Songs",
        xaxis_title="Date",
        legend={"orientation": "h", "y": -0.2},
    )
    return fig


def _music_pct_figure(daily: pl.DataFrame) -> go.Figure:
    daily_sorted = daily.sort("date")
    dates = daily_sorted["date"].to_list()
    music_pct = daily_sorted["music_pct"].to_list()

    rolling = (
        daily_sorted.with_columns(
            pl.col("music_pct")
            .rolling_mean(window_size=7, min_samples=1)
            .alias("rolling_7d")
        )["rolling_7d"]
        .to_list()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=music_pct,
            mode="lines",
            name="Daily",
            line={"color": "#6366f1", "width": 1},
            opacity=0.5,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=rolling,
            mode="lines",
            name="7-day avg",
            line={"color": "#a78bfa", "width": 2.5},
        )
    )
    fig.update_layout(
        template="plotly_dark",
        yaxis_title="Music %",
        xaxis_title="Date",
        legend={"orientation": "h", "y": -0.2},
    )
    return fig


def _weekly_line_figure(
    df: pl.DataFrame,
    value_col: str,
    *,
    color: str = "#6366f1",
    rolling_color: str = "#a78bfa",
    yaxis_title: str = "",
    rolling_window: int = 8,
) -> go.Figure:
    """Generic weekly line chart with rolling average."""
    # Build a pseudo-date from iso_year + iso_week for the x-axis
    import datetime

    dates = [
        datetime.date.fromisocalendar(int(row["iso_year"]), int(row["iso_week"]), 1)
        for row in df.iter_rows(named=True)
    ]
    values = df[value_col].to_list()

    rolling = (
        df.with_columns(
            pl.col(value_col)
            .rolling_mean(window_size=rolling_window, min_samples=1)
            .alias("rolling")
        )["rolling"]
        .to_list()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dates, y=values, mode="lines", name="Weekly",
            line={"color": color, "width": 1}, opacity=0.4,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates, y=rolling, mode="lines", name=f"{rolling_window}-week avg",
            line={"color": rolling_color, "width": 2.5},
        )
    )
    fig.update_layout(
        template="plotly_dark",
        yaxis_title=yaxis_title,
        xaxis_title="Date",
        legend={"orientation": "h", "y": -0.2},
    )
    return fig


def _decades_figure(decades: pl.DataFrame) -> go.Figure:
    decades_sorted = decades.sort("decade")
    labels = decades_sorted["decade"].to_list()
    counts = decades_sorted["play_count"].to_list()

    fig = go.Figure(
        go.Bar(
            x=labels,
            y=counts,
            marker_color="#7c3aed",
            marker_line_color="#a78bfa",
            marker_line_width=1,
        )
    )
    fig.update_layout(
        template="plotly_dark",
        yaxis_title="Plays",
        xaxis_title="Decade",
    )
    return fig


def _genres_figure(genre_summary: pl.DataFrame, n: int = 25) -> go.Figure:
    top = genre_summary.sort("play_count", descending=False).tail(n)
    genres = top["genre"].to_list()
    counts = top["play_count"].to_list()

    fig = go.Figure(
        go.Bar(
            x=counts,
            y=genres,
            orientation="h",
            marker_color="#10b981",
            marker_line_color="#34d399",
            marker_line_width=1,
        )
    )
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Plays",
        yaxis_title="Genre",
        height=max(350, n * 28 + 80),
    )
    return fig


def _shows_figure(show_summary: pl.DataFrame) -> go.Figure:
    shows_sorted = show_summary.sort("total_plays", descending=False)
    shows = shows_sorted["program"].to_list()
    plays = shows_sorted["total_plays"].to_list()

    fig = go.Figure(
        go.Bar(
            x=plays,
            y=shows,
            orientation="h",
            marker_color="#0ea5e9",
            marker_line_color="#38bdf8",
            marker_line_width=1,
        )
    )
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Total Plays",
        yaxis_title="Show",
        height=max(350, len(shows) * 28 + 80),
    )
    return fig


def _top_artists(playlist: pl.DataFrame, n: int = 20) -> list[dict]:
    result = (
        playlist.group_by("artist")
        .agg(pl.len().alias("plays"))
        .sort("plays", descending=True)
        .head(n)
    )
    return [{"artist": row["artist"], "plays": row["plays"]} for row in result.iter_rows(named=True)]


def _top_songs(playlist: pl.DataFrame, n: int = 20) -> list[dict]:
    result = (
        playlist.group_by(["artist", "title"])
        .agg(pl.len().alias("plays"))
        .sort("plays", descending=True)
        .head(n)
    )
    return [
        {"artist": row["artist"], "title": row["title"], "plays": row["plays"]}
        for row in result.iter_rows(named=True)
    ]


def generate_report() -> None:
    daily = _load_parquet(storage.ANALYTICS_DIR / "daily_summary.parquet")
    show_summary = _load_parquet(storage.ANALYTICS_DIR / "program_summary.parquet")
    decades = _load_parquet(storage.ANALYTICS_DIR / "release_decade_summary.parquet")
    genre_summary = _load_parquet(storage.ANALYTICS_DIR / "genre_summary.parquet")
    eclecticity = _load_parquet(storage.ANALYTICS_DIR / "eclecticity.parquet")
    playlist = _load_parquet(storage.PLAYLIST_PATH)

    if daily is None or daily.is_empty():
        print("No daily_summary data found — run analytics first.")
        return

    fig_songs_per_day = _songs_per_day_figure(daily)
    fig_music_pct = _music_pct_figure(daily)

    if eclecticity is not None and not eclecticity.is_empty():
        fig_unique_ratio = _weekly_line_figure(
            eclecticity, "unique_ratio",
            color="#22d3ee", rolling_color="#06b6d4", yaxis_title="Unique Ratio",
        )
        fig_new_song = _weekly_line_figure(
            eclecticity, "new_song_pct",
            color="#f472b6", rolling_color="#ec4899", yaxis_title="Fresh Music %",
        )
        fig_hhi = _weekly_line_figure(
            eclecticity.filter(pl.col("artist_hhi").is_not_null()),
            "artist_hhi",
            color="#fb923c", rolling_color="#f97316", yaxis_title="HHI (lower = more diverse)",
        )
    else:
        fig_unique_ratio = go.Figure()
        fig_new_song = go.Figure()
        fig_hhi = go.Figure()

    if genre_summary is not None and not genre_summary.is_empty():
        fig_genres = _genres_figure(genre_summary)
    else:
        fig_genres = go.Figure()

    if decades is not None and not decades.is_empty():
        fig_decades = _decades_figure(decades)
    else:
        fig_decades = go.Figure()

    if show_summary is not None and not show_summary.is_empty():
        fig_shows = _shows_figure(show_summary)
    else:
        fig_shows = go.Figure()

    if playlist is not None and not playlist.is_empty():
        top_artists = _top_artists(playlist)
        top_songs = _top_songs(playlist)
    else:
        top_artists = []
        top_songs = []

    template = Template(_TEMPLATE)
    html = template.render(
        fig_songs_per_day=_fig_to_json(fig_songs_per_day),
        fig_music_pct=_fig_to_json(fig_music_pct),
        fig_unique_ratio=_fig_to_json(fig_unique_ratio),
        fig_new_song=_fig_to_json(fig_new_song),
        fig_hhi=_fig_to_json(fig_hhi),
        fig_genres=_fig_to_json(fig_genres),
        fig_decades=_fig_to_json(fig_decades),
        fig_shows=_fig_to_json(fig_shows),
        top_artists=top_artists,
        top_songs=top_songs,
    )

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    out = DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"Report written to {out}")
