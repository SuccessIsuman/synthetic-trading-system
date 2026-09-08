from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

TICK_COLUMNS = ["symbol", "epoch", "price", "timestamp_utc"]
_SAFE_SYMBOL = re.compile(r"^[A-Za-z0-9_]+$")


def frame_from_history(symbol: str, history: dict[str, Any] | None) -> pd.DataFrame:
    """Convert an API history payload without masking data-quality problems."""
    if not _SAFE_SYMBOL.fullmatch(symbol):
        raise ValueError("symbol must contain only letters, numbers, and underscores")
    if not isinstance(history, dict):
        raise ValueError("history must be an object")
    times = history.get("times")
    prices = history.get("prices")
    if not isinstance(times, list) or not isinstance(prices, list):
        raise ValueError("history must contain times and prices arrays")
    if not times:
        raise ValueError("No ticks returned.")
    if len(times) != len(prices):
        raise ValueError(
            f"Mismatched history arrays: {len(times)} timestamps vs {len(prices)} prices."
        )
    frame = pd.DataFrame({"symbol": symbol, "epoch": times, "price": prices})
    try:
        frame["epoch"] = pd.to_numeric(frame["epoch"], errors="raise").astype("int64")
        frame["price"] = pd.to_numeric(frame["price"], errors="raise").astype("float64")
    except (TypeError, ValueError) as error:
        raise ValueError("History contains a non-numeric time or price.") from error
    frame["timestamp_utc"] = pd.to_datetime(frame["epoch"], unit="s", utc=True)
    validate_ticks(frame)
    return frame[TICK_COLUMNS]


def validate_ticks(frame: pd.DataFrame) -> dict[str, int | bool]:
    missing = set(TICK_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Tick data is missing columns: {sorted(missing)}")
    if frame.empty:
        raise ValueError("No ticks returned.")
    if frame[["symbol", "epoch", "price"]].isna().any().any():
        raise ValueError("Missing symbol, timestamp, or price detected.")
    if not frame["epoch"].is_monotonic_increasing:
        raise ValueError("Ticks are not ordered chronologically.")
    if (frame["epoch"] < 0).any():
        raise ValueError("Negative epoch detected.")
    nonpositive_prices = int((frame["price"] <= 0).sum())
    if nonpositive_prices:
        raise ValueError(f"Found {nonpositive_prices} non-positive prices.")
    duplicate_rows = int(frame.duplicated(subset=["symbol", "epoch", "price"]).sum())
    return {
        "rows": int(len(frame)),
        "duplicate_rows": duplicate_rows,
        "unique_timestamps": int(frame["epoch"].nunique()),
        "chronological": True,
    }


def deduplicate_ticks(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop_duplicates(subset=["symbol", "epoch", "price"], keep="first").reset_index(
        drop=True
    )


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return path
