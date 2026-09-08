from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from synthetic_trading.config import PROJECT_ROOT

LAGS = [1, 2, 5, 10, 20]
PERCENTILES = [1, 5, 25, 50, 75, 95, 99]
MINIMUM_EDGE_BARS = 43_200


def _json_number(value: float | int | np.floating | None) -> float | int | None:
    if value is None or not math.isfinite(float(value)):
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    return float(value)


def _run_lengths(returns: pd.Series, positive: bool) -> dict[str, int]:
    runs: dict[str, int] = {}
    current = 0
    for value in returns:
        matches = bool(value > 0) if positive else bool(value < 0)
        if matches:
            current += 1
        elif current:
            key = str(current)
            runs[key] = runs.get(key, 0) + 1
            current = 0
    if current:
        key = str(current)
        runs[key] = runs.get(key, 0) + 1
    return dict(sorted(runs.items(), key=lambda item: int(item[0])))


def _return_statistics(returns: pd.Series) -> dict:
    clean = returns.dropna().astype(float)
    percentiles = {
        f"p{percentile:02d}": _json_number(clean.quantile(percentile / 100))
        for percentile in PERCENTILES
    }
    autocorrelation = {
        str(lag): _json_number(clean.autocorr(lag=lag))
        if len(clean) > lag + 1 and clean.std() != 0
        else None
        for lag in LAGS
    }
    return {
        "count": int(len(clean)),
        "mean": _json_number(clean.mean()),
        "standard_deviation": _json_number(clean.std()),
        "skewness": _json_number(clean.skew()),
        "kurtosis": _json_number(clean.kurt()),
        "positive_return_rate": _json_number((clean > 0).mean()),
        "percentiles": percentiles,
        "autocorrelation": autocorrelation,
        "positive_run_lengths": _run_lengths(clean, positive=True),
        "negative_run_lengths": _run_lengths(clean, positive=False),
    }


def _baseline_comparison(returns: pd.Series) -> dict:
    clean = returns.dropna().astype(float)
    actual_up = clean > 0
    previous_direction = actual_up.shift(1).dropna()
    current_after_previous = actual_up.loc[previous_direction.index]
    return {
        "direction_definition": "up if return > 0, down otherwise",
        "evaluated_returns": int(len(clean)),
        "always_up": {"accuracy": _json_number(actual_up.mean())},
        "always_down": {"accuracy": _json_number((~actual_up).mean())},
        "random_50_50": {"expected_accuracy": 0.5},
        "previous_bar_direction": {
            "evaluated_returns": int(len(current_after_previous)),
            "accuracy": _json_number(
                (current_after_previous == previous_direction).mean()
                if len(current_after_previous)
                else None
            ),
        },
    }


def _partition_report(frame: pd.DataFrame) -> dict:
    simple_returns = frame["close"].pct_change()
    log_returns = np.log(frame["close"] / frame["close"].shift(1))
    return {
        "bar_count": int(len(frame)),
        "simple_returns": _return_statistics(simple_returns),
        "log_returns": _return_statistics(log_returns),
        "naive_baselines": _baseline_comparison(simple_returns),
    }


def _write_json_atomic(value: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def build_baseline_report(bars: pd.DataFrame, source_name: str) -> dict:
    required = {"symbol", "timeframe", "timestamp_utc", "close"}
    missing = required - set(bars.columns)
    if missing:
        raise ValueError(f"Bars are missing columns: {sorted(missing)}")
    if bars.empty:
        raise ValueError("Bar dataset is empty")
    bars = bars.copy()
    bars["timestamp_utc"] = pd.to_datetime(bars["timestamp_utc"], utc=True)
    if not bars["timestamp_utc"].is_monotonic_increasing:
        raise ValueError("Bars are not chronological")
    if bars["timestamp_utc"].duplicated().any():
        raise ValueError("Bars contain duplicate timestamps")
    if (bars["close"] <= 0).any() or bars["close"].isna().any():
        raise ValueError("Bars contain missing or non-positive close prices")

    bar_count = len(bars)
    start = bars["timestamp_utc"].iloc[0]
    end = bars["timestamp_utc"].iloc[-1]
    coverage = end - start
    split_train = int(bar_count * 0.70)
    split_validation = split_train + int(bar_count * 0.15)
    partitions = {
        "train": bars.iloc[:split_train],
        "validation": bars.iloc[split_train:split_validation],
        "test": bars.iloc[split_validation:],
    }
    return {
        "report_type": "read_only_statistical_baseline",
        "edge_claim_status": (
            "SUFFICIENT_FOR_EDGE_CLAIM_REVIEW"
            if bar_count >= MINIMUM_EDGE_BARS
            else "INSUFFICIENT_FOR_EDGE_CLAIM"
        ),
        "source": source_name,
        "symbol": str(bars["symbol"].iloc[0]),
        "timeframe": str(bars["timeframe"].iloc[0]),
        "dataset": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "bar_count": int(bar_count),
            "coverage_duration": str(coverage),
            "coverage_duration_minutes": _json_number(coverage.total_seconds() / 60),
        },
        "overall": _partition_report(bars),
        "partitions": {
            name: {
                "start": part["timestamp_utc"].iloc[0].isoformat() if len(part) else None,
                "end": part["timestamp_utc"].iloc[-1].isoformat() if len(part) else None,
                **_partition_report(part),
            }
            for name, part in partitions.items()
        },
    }


def generate_baseline_report(source_path: Path, output_path: Path) -> dict:
    bars = pd.read_parquet(source_path)
    report = build_baseline_report(bars, source_path.name)
    _write_json_atomic(report, output_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate read-only statistical bar baselines.")
    parser.add_argument("--symbol", default="1HZ100V")
    args = parser.parse_args()
    processed = PROJECT_ROOT / "data" / "processed"
    report = generate_baseline_report(
        processed / f"{args.symbol}_1m.parquet",
        processed / f"{args.symbol}_1m_baseline_report.json",
    )
    print(json.dumps({
        "edge_claim_status": report["edge_claim_status"],
        "dataset": report["dataset"],
    }, indent=2))


if __name__ == "__main__":
    main()