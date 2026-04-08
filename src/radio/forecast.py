"""Prophet-based time series analysis for Radio 357 playlist data."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl
import plotly.graph_objects as go
import plotly.io as pio
from prophet import Prophet

from radio import storage

logger = logging.getLogger(__name__)


def _daily_to_prophet(daily: pl.DataFrame, value_col: str) -> pl.DataFrame:
    """Convert daily summary to Prophet's expected (ds, y) format."""
    return (
        daily.sort("date")
        .select([
            pl.col("date").cast(pl.Utf8).alias("ds"),
            pl.col(value_col).alias("y"),
        ])
        .drop_nulls()
    )


def _fit_prophet(df: pl.DataFrame, changepoint_prior: float = 0.1) -> Prophet:
    """Fit a Prophet model. Returns the fitted model."""
    m = Prophet(
        changepoint_prior_scale=changepoint_prior,
        changepoint_range=1.0,  # analyze full history, not forecasting
        n_changepoints=50,  # default 25 is too sparse for 1400+ days
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
    )
    m.fit(df.to_pandas())
    return m


def _extract_changepoints(model: Prophet) -> list[dict]:
    """Extract significant changepoints with their magnitude."""
    deltas = model.params["delta"].flatten()
    changepoints = model.changepoints

    # Only keep changepoints with meaningful magnitude
    threshold = abs(deltas).mean() + abs(deltas).std()
    significant = []
    for cp, delta in zip(changepoints, deltas):
        if abs(delta) > threshold:
            significant.append({
                "date": str(cp.date()),
                "delta": round(float(delta), 4),
                "direction": "drop" if delta < 0 else "increase",
            })

    return sorted(significant, key=lambda x: abs(x["delta"]), reverse=True)


def _decomposition_figure(
    model: Prophet,
    forecast: object,
    title: str,
    color: str = "#a78bfa",
) -> go.Figure:
    """Create a decomposition figure showing trend + changepoints."""
    dates = forecast["ds"].tolist()
    trend = forecast["trend"].tolist()
    actual = forecast["y"].tolist() if "y" in forecast.columns else None

    fig = go.Figure()

    if actual is not None:
        fig.add_trace(go.Scatter(
            x=dates, y=actual, mode="lines", name="Actual",
            line={"color": color, "width": 1}, opacity=0.3,
        ))

    fig.add_trace(go.Scatter(
        x=dates, y=trend, mode="lines", name="Trend",
        line={"color": color, "width": 3},
    ))

    # Add changepoint markers
    changepoints = _extract_changepoints(model)
    for cp in changepoints[:5]:  # top 5
        fig.add_vline(
            x=cp["date"], line_dash="dash",
            line_color="#ef4444" if cp["direction"] == "drop" else "#22c55e",
            opacity=0.6,
        )

    fig.update_layout(
        template="plotly_dark",
        yaxis_title=title,
        xaxis_title="Date",
        legend={"orientation": "h", "y": -0.2},
    )
    return fig


def _seasonality_figure(
    model: Prophet,
    forecast: object,
    component: str,
    title: str,
    color: str = "#a78bfa",
) -> go.Figure:
    """Create a figure for a seasonality component."""
    if component == "weekly":
        from prophet.plot import plot_weekly
        import matplotlib
        matplotlib.use("Agg")
        # Extract weekly seasonality values from forecast
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        weekly = forecast.groupby(forecast["ds"].dt.dayofweek)["weekly"].mean()
        weekly = weekly.reindex(range(7))

        fig = go.Figure(go.Bar(
            x=days, y=weekly.tolist(),
            marker_color=color,
            marker_line_color=color,
            marker_line_width=1,
        ))
        fig.update_layout(
            template="plotly_dark",
            yaxis_title=title,
        )
        return fig

    elif component == "yearly":
        yearly = forecast.set_index("ds")["yearly"].resample("W").mean().reset_index()
        fig = go.Figure(go.Scatter(
            x=yearly["ds"].tolist(),
            y=yearly["yearly"].tolist(),
            mode="lines",
            line={"color": color, "width": 2},
        ))
        fig.update_layout(
            template="plotly_dark",
            yaxis_title=title,
            xaxis_title="Date",
        )
        return fig

    return go.Figure()


def run_analysis(daily: pl.DataFrame) -> dict:
    """Run Prophet analysis on songs_per_day and music_pct.

    Returns dict with figures (as JSON) and changepoints.
    """
    logger.info("running Prophet analysis...")
    results = {}

    for metric, col, title, color in [
        ("songs", "total_songs", "Songs/Day", "#a78bfa"),
        ("music_pct", "music_pct", "Music %", "#6366f1"),
    ]:
        logger.info("fitting %s...", metric)
        df = _daily_to_prophet(daily, col)
        model = _fit_prophet(df)
        forecast = model.predict(df.to_pandas())
        forecast["y"] = df["y"].to_list()

        # Trend + changepoints figure
        trend_fig = _decomposition_figure(model, forecast, title, color)
        results[f"fig_{metric}_trend"] = pio.to_json(trend_fig)

        # Weekly seasonality figure
        weekly_fig = _seasonality_figure(model, forecast, "weekly", f"{title} (weekly effect)", color)
        results[f"fig_{metric}_weekly"] = pio.to_json(weekly_fig)

        # Changepoints
        changepoints = _extract_changepoints(model)
        results[f"{metric}_changepoints"] = changepoints
        if changepoints:
            logger.info("%s changepoints: %s", metric, changepoints[:3])

    return results
