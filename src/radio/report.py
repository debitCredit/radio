from __future__ import annotations

import datetime
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

    .lang-toggle {
      display: inline-flex;
      gap: 0.5rem;
      margin-top: 1rem;
    }

    .lang-toggle button {
      background: #1e1e38;
      color: #8888aa;
      border: 1px solid #2a2a4a;
      border-radius: 6px;
      padding: 0.3rem 0.8rem;
      cursor: pointer;
      font-size: 0.85rem;
      transition: all 0.2s;
    }

    .lang-toggle button.active {
      background: #a78bfa;
      color: #0f0f1a;
      border-color: #a78bfa;
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

    .section-desc {
      color: #8888aa;
      font-size: 0.85rem;
      margin-bottom: 1rem;
    }

    .chart-container {
      width: 100%;
      min-height: 380px;
    }

    .insight-box {
      background: #1a1a35;
      border-left: 3px solid #a78bfa;
      border-radius: 0 8px 8px 0;
      padding: 0.9rem 1.2rem;
      margin-top: 1rem;
      font-size: 0.88rem;
      color: #c0c0e0;
      line-height: 1.65;
    }

    .insight-box strong {
      color: #c4b5fd;
    }

    .insight-box.warning {
      border-left-color: #ef4444;
    }

    .insight-box.warning strong {
      color: #fca5a5;
    }

    .insight-box.positive {
      border-left-color: #22c55e;
    }

    .insight-box.positive strong {
      color: #86efac;
    }

    .changepoints {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.75rem;
    }

    .changepoint-tag {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      background: #1e1e38;
      border-radius: 6px;
      padding: 0.3rem 0.7rem;
      font-size: 0.8rem;
    }

    .changepoint-tag.drop { color: #fca5a5; border: 1px solid #7f1d1d; }
    .changepoint-tag.increase { color: #86efac; border: 1px solid #14532d; }

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

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 1rem;
      margin-bottom: 1.5rem;
    }

    .stat-card {
      background: #1a1a35;
      border-radius: 8px;
      padding: 1rem;
      text-align: center;
    }

    .stat-card .value {
      font-size: 1.6rem;
      font-weight: 700;
      color: #a78bfa;
    }

    .stat-card .label {
      font-size: 0.78rem;
      color: #8888aa;
      margin-top: 0.25rem;
    }

    footer {
      text-align: center;
      color: #44446a;
      font-size: 0.8rem;
      padding: 2rem 0 1rem;
    }

    footer a { color: #6366f1; text-decoration: none; }
    footer a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Radio 357 Analytics</h1>
      <p data-en="Data-driven analysis of Radio 357 playlists since 2022" data-pl="Analiza danych playlist Radia 357 od 2022 roku"></p>
      <div class="lang-toggle">
        <button id="btn-en" onclick="setLang('en')">English</button>
        <button id="btn-pl" class="active" onclick="setLang('pl')">Polski</button>
      </div>
    </header>

    <!-- Summary stats -->
    <div class="section">
      <h2 data-en="Overview" data-pl="Podsumowanie"></h2>
      <div class="stats-grid">
        <div class="stat-card">
          <div class="value">{{ total_plays }}</div>
          <div class="label" data-en="Total Plays" data-pl="Odtworzeń"></div>
        </div>
        <div class="stat-card">
          <div class="value">{{ unique_songs }}</div>
          <div class="label" data-en="Unique Songs" data-pl="Unikalnych utworów"></div>
        </div>
        <div class="stat-card">
          <div class="value">{{ unique_artists }}</div>
          <div class="label" data-en="Unique Artists" data-pl="Unikalnych artystów"></div>
        </div>
        <div class="stat-card">
          <div class="value">{{ days_scraped }}</div>
          <div class="label" data-en="Days Analyzed" data-pl="Przeanalizowanych dni"></div>
        </div>
        <div class="stat-card">
          <div class="value">{{ avg_songs_per_day }}</div>
          <div class="label" data-en="Avg Songs/Day" data-pl="Śr. utworów/dzień"></div>
        </div>
        <div class="stat-card">
          <div class="value">{{ avg_music_pct }}%</div>
          <div class="label" data-en="Avg Music Time" data-pl="Śr. czas muzyki"></div>
        </div>
      </div>
    </div>

    <!-- Year over year -->
    <div class="section">
      <h2 data-en="Year over Year" data-pl="Porównanie roczne"></h2>
      <table>
        <thead>
          <tr>
            <th data-en="Year" data-pl="Rok"></th>
            <th class="count" data-en="Plays" data-pl="Odtworzeń"></th>
            <th class="count" data-en="Songs" data-pl="Utworów"></th>
            <th class="count" data-en="Artists" data-pl="Artystów"></th>
            <th class="count" data-en="Avg/Day" data-pl="Śr./dzień"></th>
            <th class="count" data-en="Music %" data-pl="Muzyka %"></th>
          </tr>
        </thead>
        <tbody>
          {% for row in yearly_stats %}
          <tr{% if row.is_current %} style="color: #fca5a5;"{% endif %}>
            <td>{{ row.year }}{% if row.partial %} *{% endif %}</td>
            <td class="count">{{ row.plays }}</td>
            <td class="count">{{ row.songs }}</td>
            <td class="count">{{ row.artists }}</td>
            <td class="count">{{ row.avg_per_day }}</td>
            <td class="count">{{ row.music_pct }}%</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% if yearly_stats and yearly_stats[-1].partial %}
      <p class="section-desc" style="margin-top: 0.75rem;">* <span data-en="Partial year" data-pl="Niepełny rok"></span></p>
      {% endif %}
    </div>

    <!-- Period comparison -->
    {% if period_comparison %}
    <div class="section">
      <h2 data-en="Recent Period vs Last Year" data-pl="Ostatni okres vs rok wcześniej"></h2>
      <p class="section-desc" data-en="Comparing the same calendar period year over year." data-pl="Porównanie tego samego okresu kalendarzowego rok do roku."></p>
      <table>
        <thead>
          <tr>
            <th data-en="Period" data-pl="Okres"></th>
            <th class="count" data-en="Plays" data-pl="Odtworzeń"></th>
            <th class="count" data-en="Avg/Day" data-pl="Śr./dzień"></th>
            <th class="count" data-en="Music %" data-pl="Muzyka %"></th>
            <th class="count" data-en="Artists" data-pl="Artystów"></th>
          </tr>
        </thead>
        <tbody>
          {% for row in period_comparison %}
          <tr{% if row.highlight %} style="color: #fca5a5;"{% endif %}>
            <td data-en="{{ row.label_en }}" data-pl="{{ row.label_pl }}"></td>
            <td class="count">{{ row.plays }}</td>
            <td class="count">{{ row.avg_per_day }}</td>
            <td class="count">{{ row.music_pct }}%</td>
            <td class="count">{{ row.artists }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% if period_delta_en %}
      <div class="insight-box warning">
        <strong data-en="Year-over-year change:" data-pl="Zmiana rok do roku:"></strong>
        <span data-en="{{ period_delta_en }}" data-pl="{{ period_delta_pl }}"></span>
      </div>
      {% endif %}
    </div>
    {% endif %}

    <!-- Songs per day -->
    <div class="section">
      <h2 data-en="Songs Per Day" data-pl="Utwory dziennie"></h2>
      <p class="section-desc" data-en="Number of songs played each day with 7-day rolling average." data-pl="Liczba odtworzonych utworów dziennie ze średnią kroczącą 7 dni."></p>
      <div id="chart-songs-per-day" class="chart-container"></div>
      <div class="insight-box warning">
        <strong data-en="Notable drop:" data-pl="Zauważalny spadek:"></strong>
        <span data-en="From ~300 songs/day in January 2026 to ~246 in March 2026 — an 18% decline in just two months." data-pl="Ze ~300 utworów/dzień w styczniu 2026 do ~246 w marcu 2026 — spadek o 18% w zaledwie dwa miesiące."></span>
      </div>
    </div>

    <!-- Music vs Talk -->
    <div class="section">
      <h2 data-en="Music vs Talk Time" data-pl="Muzyka vs audycje"></h2>
      <p class="section-desc" data-en="Percentage of broadcast time filled with music (based on track durations)." data-pl="Procent czasu antenowego wypełnionego muzyką (na podstawie długości utworów)."></p>
      <div id="chart-music-pct" class="chart-container"></div>
      <div class="insight-box">
        <strong data-en="How it's calculated:" data-pl="Jak to liczymy:"></strong>
        <span data-en="Sum of track durations per day divided by 24 hours. Days with lower coverage show more talk programming." data-pl="Suma długości utworów w ciągu dnia podzielona przez 24 godziny. Dni z niższym wynikiem to więcej audycji słownych."></span>
      </div>
    </div>

    <!-- Prophet: Songs trend -->
    <div class="section">
      <h2 data-en="Trend Analysis — Songs Per Day" data-pl="Analiza trendu — utwory dziennie"></h2>
      <p class="section-desc" data-en="Long-term trend extracted using Prophet. Dashed lines mark statistically significant change points." data-pl="Trend długoterminowy wyodrębniony za pomocą Prophet. Linie przerywane oznaczają statystycznie istotne punkty zmiany."></p>
      <div id="chart-songs-trend" class="chart-container"></div>
      {% if songs_changepoints %}
      <div class="insight-box warning">
        <strong data-en="Detected change points:" data-pl="Wykryte punkty zmiany:"></strong>
        <div class="changepoints">
          {% for cp in songs_changepoints %}
          <span class="changepoint-tag {{ cp.direction }}">
            {{ cp.date }} {{ "▼" if cp.direction == "drop" else "▲" }}
          </span>
          {% endfor %}
        </div>
      </div>
      {% endif %}
    </div>

    <!-- Prophet: Music % trend -->
    <div class="section">
      <h2 data-en="Trend Analysis — Music Share" data-pl="Analiza trendu — udział muzyki"></h2>
      <p class="section-desc" data-en="Long-term trend of music percentage with detected structural changes." data-pl="Trend długoterminowy udziału muzyki z wykrytymi zmianami strukturalnymi."></p>
      <div id="chart-music-pct-trend" class="chart-container"></div>
      {% if music_pct_changepoints %}
      <div class="insight-box warning">
        <strong data-en="Detected change points:" data-pl="Wykryte punkty zmiany:"></strong>
        <div class="changepoints">
          {% for cp in music_pct_changepoints %}
          <span class="changepoint-tag {{ cp.direction }}">
            {{ cp.date }} {{ "▼" if cp.direction == "drop" else "▲" }}
          </span>
          {% endfor %}
        </div>
      </div>
      {% endif %}
    </div>

    <!-- Prophet: Weekly seasonality -->
    <div class="section">
      <h2 data-en="Weekly Pattern" data-pl="Wzorzec tygodniowy"></h2>
      <p class="section-desc" data-en="How the number of songs varies by day of week (Prophet seasonal component)." data-pl="Jak zmienia się liczba utworów w zależności od dnia tygodnia (komponent sezonowy Prophet)."></p>
      <div id="chart-songs-weekly" class="chart-container"></div>
      <div class="insight-box">
        <strong data-en="Pattern:" data-pl="Wzorzec:"></strong>
        <span data-en="Weekdays consistently have more music than weekends, reflecting talk-heavy weekend programming." data-pl="W dni robocze konsekwentnie jest więcej muzyki niż w weekendy, co odzwierciedla audycje słowne w weekendy."></span>
      </div>
    </div>

    <!-- Eclecticity: Unique Ratio -->
    <div class="section">
      <h2 data-en="Eclecticity — Unique Ratio" data-pl="Eklektyczność — unikalność"></h2>
      <p class="section-desc" data-en="Unique songs / total plays per week. Higher = less repetition." data-pl="Unikalne utwory / wszystkie odtworzenia w tygodniu. Wyżej = mniej powtórek."></p>
      <div id="chart-unique-ratio" class="chart-container"></div>
      <div class="insight-box positive">
        <strong data-en="Good news:" data-pl="Dobra wiadomość:"></strong>
        <span data-en="Radio 357 barely repeats songs within a week — the ratio stays consistently above 0.93." data-pl="Radio 357 prawie nie powtarza utworów w ciągu tygodnia — wskaźnik utrzymuje się powyżej 0.93."></span>
      </div>
    </div>

    <!-- Eclecticity: Fresh Music Rate -->
    <div class="section">
      <h2 data-en="Eclecticity — Fresh Music Rate" data-pl="Eklektyczność — świeża muzyka"></h2>
      <p class="section-desc" data-en="% of plays that are songs not heard in the prior 90 days." data-pl="% odtworzeń utworów, które nie były grane w ciągu ostatnich 90 dni."></p>
      <div id="chart-new-song" class="chart-container"></div>
    </div>

    <!-- Eclecticity: Artist HHI -->
    <div class="section">
      <h2 data-en="Eclecticity — Artist Concentration" data-pl="Eklektyczność — koncentracja artystów"></h2>
      <p class="section-desc" data-en="Herfindahl-Hirschman Index of artist play share per week. Lower = more diverse." data-pl="Indeks Herfindahla-Hirschmana udziału artystów w tygodniu. Niżej = większa różnorodność."></p>
      <div id="chart-hhi" class="chart-container"></div>
    </div>

    {% if has_genres %}
    <!-- Top Genres -->
    <div class="section">
      <h2 data-en="Top Genres" data-pl="Najpopularniejsze gatunki"></h2>
      <div id="chart-genres" class="chart-container"></div>
    </div>
    {% endif %}

    <!-- Release Decades -->
    <div class="section">
      <h2 data-en="Release Decades" data-pl="Dekady wydania"></h2>
      <p class="section-desc" data-en="Distribution of played tracks by release decade." data-pl="Rozkład odtwarzanych utworów według dekady wydania."></p>
      <div id="chart-decades" class="chart-container"></div>
    </div>

    <!-- Top Artists -->
    <div class="section">
      <h2 data-en="Most Played Artists" data-pl="Najczęściej grani artyści"></h2>
      <table>
        <thead>
          <tr>
            <th class="rank">#</th>
            <th data-en="Artist" data-pl="Artysta"></th>
            <th class="count" data-en="Plays" data-pl="Odtworzeń"></th>
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

    <!-- Top Songs -->
    <div class="section">
      <h2 data-en="Most Played Songs" data-pl="Najczęściej grane utwory"></h2>
      <table>
        <thead>
          <tr>
            <th class="rank">#</th>
            <th data-en="Artist" data-pl="Artysta"></th>
            <th data-en="Title" data-pl="Tytuł"></th>
            <th class="count" data-en="Plays" data-pl="Odtworzeń"></th>
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

    <footer>
      <span data-en="Radio 357 Analytics — data from" data-pl="Radio 357 Analytics — dane od"></span>
      {{ date_min }} <span data-en="to" data-pl="do"></span> {{ date_max }} &mdash;
      <a href="https://github.com/debitCredit/radio">source code</a>
    </footer>
  </div>

  <script>
    // i18n
    var currentLang = 'pl';
    var traceLabels = {
      'Daily': 'Dziennie',
      '7-day avg': 'Śr. 7 dni',
      'Weekly': 'Tygodniowo',
      '8-week avg': 'Śr. 8 tyg.',
      'Actual': 'Dane',
      'Trend': 'Trend',
      'Mon': 'Pn', 'Tue': 'Wt', 'Wed': 'Śr', 'Thu': 'Cz', 'Fri': 'Pt', 'Sat': 'Sb', 'Sun': 'Nd',
    };
    var axisLabels = {
      'Songs': 'Utwory',
      'Music %': 'Muzyka %',
      'Date': 'Data',
      'Plays': 'Odtworzeń',
      'Decade': 'Dekada',
      'Genre': 'Gatunek',
      'Unique Ratio': 'Wsk. unikalności',
      'Fresh Music %': 'Świeża muzyka %',
      'HHI (lower = more diverse)': 'HHI (niżej = większa różnorodność)',
      'Songs/Day': 'Utwory/dzień',
      'Songs/Day (weekly effect)': 'Utwory/dzień (efekt tygodniowy)',
    };

    // Store original English labels per chart
    var chartOriginals = {};

    function setLang(lang) {
      currentLang = lang;
      document.querySelectorAll('[data-en]').forEach(function(el) {
        el.textContent = el.getAttribute('data-' + lang);
      });
      document.getElementById('btn-en').classList.toggle('active', lang === 'en');
      document.getElementById('btn-pl').classList.toggle('active', lang === 'pl');

      // Update chart labels
      document.querySelectorAll('.chart-container').forEach(function(el) {
        var gd = el;
        if (!gd.data) return;

        // Translate trace names
        gd.data.forEach(function(trace, i) {
          var orig = (chartOriginals[el.id] || {})[i];
          if (!orig) return;
          if (lang === 'pl') {
            trace.name = traceLabels[orig] || orig;
            // Translate bar x-axis categories (weekday names)
            if (trace.type === 'bar' && trace.x) {
              trace.x = trace.x.map(function(v) { return traceLabels[v] || v; });
            }
          } else {
            trace.name = orig;
            if (trace.type === 'bar' && (chartOriginals[el.id] || {}).x) {
              trace.x = (chartOriginals[el.id] || {}).x;
            }
          }
        });

        // Translate axis titles
        var update = {};
        var origLayout = chartOriginals[el.id] && chartOriginals[el.id].layout || {};
        if (lang === 'pl') {
          if (origLayout.xtitle) update['xaxis.title.text'] = axisLabels[origLayout.xtitle] || origLayout.xtitle;
          if (origLayout.ytitle) update['yaxis.title.text'] = axisLabels[origLayout.ytitle] || origLayout.ytitle;
        } else {
          if (origLayout.xtitle) update['xaxis.title.text'] = origLayout.xtitle;
          if (origLayout.ytitle) update['yaxis.title.text'] = origLayout.ytitle;
        }

        Plotly.react(el.id, gd.data, gd.layout);
        if (Object.keys(update).length) Plotly.relayout(el.id, update);
      });
    }

    // Charts
    var layout_defaults = {
      paper_bgcolor: '#16162a',
      plot_bgcolor: '#16162a',
      font: { color: '#e0e0f0', family: "'Segoe UI', system-ui, sans-serif" },
      margin: { t: 30, b: 60, l: 60, r: 30 },
    };

    function plot(id, fig) {
      var el = document.getElementById(id);
      if (!el) return;
      var data = fig.data;
      var layout = Object.assign({}, layout_defaults, fig.layout);
      Plotly.newPlot(id, data, layout, { responsive: true, displayModeBar: false });

      // Store original English labels for i18n
      var originals = {};
      data.forEach(function(trace, i) { originals[i] = trace.name; });
      // Store original bar x categories if present
      if (data[0] && data[0].type === 'bar' && data[0].x) {
        originals.x = data[0].x.slice();
      }
      originals.layout = {
        xtitle: (layout.xaxis && layout.xaxis.title && (layout.xaxis.title.text || layout.xaxis.title)) || '',
        ytitle: (layout.yaxis && layout.yaxis.title && (layout.yaxis.title.text || layout.yaxis.title)) || '',
      };
      chartOriginals[id] = originals;
    }

    plot('chart-songs-per-day', {{ fig_songs_per_day | safe }});
    plot('chart-music-pct', {{ fig_music_pct | safe }});
    plot('chart-songs-trend', {{ fig_songs_trend | safe }});
    plot('chart-music-pct-trend', {{ fig_music_pct_trend | safe }});
    plot('chart-songs-weekly', {{ fig_songs_weekly | safe }});
    plot('chart-unique-ratio', {{ fig_unique_ratio | safe }});
    plot('chart-new-song', {{ fig_new_song | safe }});
    plot('chart-hhi', {{ fig_hhi | safe }});
    plot('chart-genres', {{ fig_genres | safe }});
    plot('chart-decades', {{ fig_decades | safe }});

    // Initialize with Polish as default
    setLang('pl');
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
            x=dates, y=music_pct, mode="lines", name="Daily",
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
            x=labels, y=counts,
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
            x=counts, y=genres, orientation="h",
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


def _empty_fig_json() -> str:
    return _fig_to_json(go.Figure())


def generate_report() -> None:
    daily = _load_parquet(storage.ANALYTICS_DIR / "daily_summary.parquet")
    decades = _load_parquet(storage.ANALYTICS_DIR / "release_decade_summary.parquet")
    genre_summary = _load_parquet(storage.ANALYTICS_DIR / "genre_summary.parquet")
    eclecticity = _load_parquet(storage.ANALYTICS_DIR / "eclecticity.parquet")
    playlist = _load_parquet(storage.PLAYLIST_PATH)

    if daily is None or daily.is_empty():
        print("No daily_summary data found — run analytics first.")
        return

    # Summary stats
    if playlist is not None and not playlist.is_empty():
        total_plays = f"{len(playlist):,}"
        unique_songs = f"{playlist.select(['artist', 'title']).unique().height:,}"
        unique_artists = f"{playlist['artist'].n_unique():,}"
        days_scraped = str(playlist["date"].n_unique())
        avg_songs_per_day = str(round(len(playlist) / playlist["date"].n_unique()))
        date_min = str(playlist["date"].min())
        date_max = str(playlist["date"].max())
    else:
        total_plays = unique_songs = unique_artists = days_scraped = avg_songs_per_day = "—"
        date_min = date_max = "—"

    avg_music_pct = str(round(daily["music_pct"].drop_nulls().mean(), 1))

    # Year-over-year stats
    current_year = datetime.date.today().year
    yearly_stats = []
    if playlist is not None and not playlist.is_empty():
        for year in sorted(playlist["date"].dt.year().unique().to_list()):
            yp = playlist.filter(pl.col("date").dt.year() == year)
            yd = daily.filter(pl.col("date").dt.year() == year)
            if yp.is_empty():
                continue
            yearly_stats.append({
                "year": year,
                "plays": f"{len(yp):,}",
                "songs": f"{yp.select(['artist', 'title']).unique().height:,}",
                "artists": f"{yp['artist'].n_unique():,}",
                "avg_per_day": str(round(yd["total_songs"].mean())) if not yd.is_empty() else "—",
                "music_pct": str(round(yd["music_pct"].mean(), 1)) if not yd.is_empty() else "—",
                "partial": year == current_year or year == int(playlist["date"].min().year),
                "is_current": year == current_year,
            })

    # Period comparison: Mar-Apr current year vs previous year
    period_comparison = []
    period_delta_en = ""
    period_delta_pl = ""
    if playlist is not None and not playlist.is_empty():
        today = datetime.date.today()
        cur_year = today.year
        period_start_month = 3  # March
        period_start = datetime.date(cur_year, period_start_month, 1)
        period_end = today

        prev_start = datetime.date(cur_year - 1, period_start_month, 1)
        prev_end = datetime.date(cur_year - 1, today.month, today.day)

        for label_en, label_pl, start, end, highlight in [
            (f"Mar–Apr {cur_year - 1}", f"mar–kwi {cur_year - 1}", prev_start, prev_end, False),
            (f"Mar–Apr {cur_year}", f"mar–kwi {cur_year}", period_start, period_end, True),
        ]:
            pp = playlist.filter((pl.col("date") >= start) & (pl.col("date") <= end))
            pd_ = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end))
            if pp.is_empty():
                continue
            period_comparison.append({
                "label_en": label_en,
                "label_pl": label_pl,
                "plays": f"{len(pp):,}",
                "avg_per_day": str(round(pd_["total_songs"].mean())) if not pd_.is_empty() else "—",
                "music_pct": str(round(pd_["music_pct"].mean(), 1)) if not pd_.is_empty() else "—",
                "artists": f"{pp['artist'].n_unique():,}",
                "highlight": highlight,
            })

        if len(period_comparison) == 2:
            prev_avg = float(period_comparison[0]["avg_per_day"])
            cur_avg = float(period_comparison[1]["avg_per_day"])
            pct_change = (cur_avg - prev_avg) / prev_avg * 100
            pp_change = float(period_comparison[1]["music_pct"]) - float(period_comparison[0]["music_pct"])
            period_delta_en = f"{pct_change:+.1f}% songs/day, {pp_change:+.1f}pp music share"
            period_delta_pl = f"{pct_change:+.1f}% utworów/dzień, {pp_change:+.1f}pp udziału muzyki"

    fig_songs_per_day = _songs_per_day_figure(daily)
    fig_music_pct = _music_pct_figure(daily)

    # Prophet analysis
    from radio.forecast import run_analysis

    prophet_results = run_analysis(daily)

    # Eclecticity
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
        fig_unique_ratio = fig_new_song = fig_hhi = go.Figure()

    fig_genres = _genres_figure(genre_summary) if genre_summary is not None and not genre_summary.is_empty() else go.Figure()
    fig_decades = _decades_figure(decades) if decades is not None and not decades.is_empty() else go.Figure()

    if playlist is not None and not playlist.is_empty():
        top_artists = _top_artists(playlist)
        top_songs = _top_songs(playlist)
    else:
        top_artists = []
        top_songs = []

    template = Template(_TEMPLATE)
    html = template.render(
        # Year over year
        yearly_stats=yearly_stats,
        period_comparison=period_comparison,
        period_delta_en=period_delta_en if period_comparison else "",
        period_delta_pl=period_delta_pl if period_comparison else "",
        # Stats
        total_plays=total_plays,
        unique_songs=unique_songs,
        unique_artists=unique_artists,
        days_scraped=days_scraped,
        avg_songs_per_day=avg_songs_per_day,
        avg_music_pct=avg_music_pct,
        date_min=date_min,
        date_max=date_max,
        # Charts
        fig_songs_per_day=_fig_to_json(fig_songs_per_day),
        fig_music_pct=_fig_to_json(fig_music_pct),
        fig_songs_trend=prophet_results.get("fig_songs_trend", _empty_fig_json()),
        fig_music_pct_trend=prophet_results.get("fig_music_pct_trend", _empty_fig_json()),
        fig_songs_weekly=prophet_results.get("fig_songs_weekly", _empty_fig_json()),
        fig_unique_ratio=_fig_to_json(fig_unique_ratio),
        fig_new_song=_fig_to_json(fig_new_song),
        fig_hhi=_fig_to_json(fig_hhi),
        fig_genres=_fig_to_json(fig_genres),
        has_genres=genre_summary is not None and not genre_summary.is_empty() and genre_summary["play_count"].sum() >= 1000,
        fig_decades=_fig_to_json(fig_decades),
        # Tables
        top_artists=top_artists,
        top_songs=top_songs,
        # Prophet insights
        songs_changepoints=prophet_results.get("songs_changepoints", []),
        music_pct_changepoints=prophet_results.get("music_pct_changepoints", []),
    )

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    out = DOCS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"Report written to {out}")
