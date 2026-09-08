from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .ticks import write_parquet_atomic

BAR_COLUMNS = [
    "symbol",
    "timeframe",
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "tick_count",
    "range",
    "body",
    "upper_wick",
    "lower_wick",
]


def _write_json_atomic(value: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _validate_canonical_ticks(frame: pd.DataFrame, symbol: str) -> None:
    required = {"symbol", "epoch", "price", "timestamp_utc"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Canonical ticks are missing columns: {sorted(missing)}")
    if frame.empty:
        raise ValueError("Canonical ticks are empty")
    if set(frame["symbol"].unique()) != {symbol}:
        raise ValueError(f"Canonical ticks contain a symbol other than {symbol}")
    if frame[["epoch", "price"]].isna().any().any():
        raise ValueError("Canonical ticks contain missing epoch or price values")
    epochs = pd.to_numeric(frame["epoch"], errors="raise").astype("int64")
    if not epochs.is_monotonic_increasing:
        raise ValueError("Canonical ticks are not chronological")
    if epochs.duplicated().any():
        raise ValueError("Canonical ticks contain duplicate epochs")
    intervals = epochs.diff().dropna()
    if (intervals != 1).any():
        first_bad = int(epochs.iloc[intervals[intervals != 1].index[0]])
        raise ValueError(f"Canonical ticks are not one second apart near epoch {first_bad}")


def generate_bars(
    canonical_path: Path,
    output_path: Path,
    report_path: Path,
    symbol: str,
) -> dict:
    ticks = pd.read_parquet(canonical_path)
    _validate_canonical_ticks(ticks, symbol)
    ticks = ticks.copy()
    ticks["epoch"] = pd.to_numeric(ticks["epoch"], errors="raise").astype("int64")
    ticks["price"] = pd.to_numeric(ticks["price"], errors="raise").astype("float64")
    ticks["minute_epoch"] = ticks["epoch"] // 60 * 60

    grouped = ticks.groupby("minute_epoch", sort=True, observed=True)
    counts = grouped.size()
    incomplete = counts[counts != 60]
    complete = ticks[ticks["minute_epoch"].isin(incomplete[incomplete.index].index) == False]
    bars = (
        complete.groupby("minute_epoch", sort=True, observed=True)
        .agg(
            symbol=("symbol", "first"),
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            tick_count=("price", "size"),
        )
        .reset_index()
    )
    bars["timeframe"] = "1m"
    bars["timestamp_utc"] = pd.to_datetime(bars["minute_epoch"], unit="s", utc=True)
    bars["range"] = bars["high"] - bars["low"]
    bars["body"] = (bars["close"] - bars["open"]).abs()
    bars["upper_wick"] = bars["high"] - bars[["open", "close"]].max(axis=1)
    bars["lower_wick"] = bars[["open", "close"]].min(axis=1) - bars["low"]
    bars = bars[BAR_COLUMNS]

    write_parquet_atomic(bars, output_path)
    report = {
        "symbol": symbol,
        "timeframe": "1m",
        "source": canonical_path.name,
        "output": output_path.name,
        "summary": {
            "input_ticks": int(len(ticks)),
            "complete_minutes": int(len(bars)),
            "rejected_minutes": int(len(incomplete)),
            "rejected_incomplete_minutes": [int(epoch) for epoch in incomplete.index],
            "output_columns": BAR_COLUMNS,
        },
    }
    _write_json_atomic(report, report_path)
    return report