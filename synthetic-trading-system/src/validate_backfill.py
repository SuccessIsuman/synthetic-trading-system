from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from synthetic_trading.config import PROJECT_ROOT
from synthetic_trading.ticks import TICK_COLUMNS, write_parquet_atomic


def _write_json_atomic(value: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_chunk(path: Path, symbol: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing = set(TICK_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
    frame = frame[TICK_COLUMNS].copy()
    if frame.empty:
        raise ValueError(f"{path.name} is empty")
    if set(frame["symbol"].unique()) != {symbol}:
        raise ValueError(f"{path.name} contains a symbol other than {symbol}")
    frame["epoch"] = pd.to_numeric(frame["epoch"], errors="raise").astype("int64")
    frame["price"] = pd.to_numeric(frame["price"], errors="raise").astype("float64")
    return frame


def validate_backfill(symbol: str, raw_dir: Path, processed_dir: Path) -> dict:
    paths = sorted(raw_dir.glob("*.parquet"))
    if not paths:
        raise ValueError(f"No Parquet chunks found in {raw_dir}")
    chunks: list[tuple[Path, pd.DataFrame]] = []
    manifest = []
    for path in paths:
        frame = _read_chunk(path, symbol)
        chunks.append((path, frame))
        manifest.append({
            "filename": path.name,
            "row_count": len(frame),
            "earliest_epoch": int(frame["epoch"].min()),
            "latest_epoch": int(frame["epoch"].max()),
            "internal_duplicates": int(frame.duplicated(["symbol", "epoch", "price"]).sum()),
            "overlaps": [],
            "gaps": [],
        })

    for index, current in enumerate(manifest):
        for previous_index in range(index):
            previous = manifest[previous_index]
            if current["earliest_epoch"] <= previous["latest_epoch"] and current["latest_epoch"] >= previous["earliest_epoch"]:
                current["overlaps"].append(previous["filename"])

    combined = pd.concat([frame for _, frame in chunks], ignore_index=True)
    conflicts = combined.groupby(["symbol", "epoch"])["price"].nunique(dropna=False)
    conflicting_epochs = [int(epoch) for (_, epoch), count in conflicts.items() if count > 1]
    if conflicting_epochs:
        raise ValueError(
            f"Conflicting prices found for {len(conflicting_epochs)} epoch(s), including "
            f"{conflicting_epochs[:5]}"
        )

    exact_duplicates = int(combined.duplicated(["symbol", "epoch", "price"]).sum())
    canonical = combined.drop_duplicates(["symbol", "epoch", "price"], keep="first")
    canonical = canonical.sort_values("epoch", kind="stable").reset_index(drop=True)
    if canonical["epoch"].duplicated().any():
        raise ValueError("Canonical data still contains duplicate epochs")
    intervals = canonical["epoch"].diff().dropna()
    gaps = canonical.loc[intervals[intervals != 1].index, "epoch"].astype(int).tolist()
    for gap_epoch in gaps:
        prior = gap_epoch - 1
        matching = [item["filename"] for item in manifest if item["earliest_epoch"] <= gap_epoch and item["latest_epoch"] >= prior]
        if matching:
            manifest[0]["gaps"].append({"after_epoch": prior, "before_epoch": gap_epoch, "chunks": matching})

    processed_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = processed_dir / f"{symbol}_ticks_canonical.parquet"
    report_path = processed_dir / f"{symbol}_quality_report.json"
    write_parquet_atomic(canonical, canonical_path)
    report = {
        "symbol": symbol,
        "manifest": manifest,
        "summary": {
            "raw_files": len(paths),
            "raw_rows": len(combined),
            "canonical_rows": len(canonical),
            "exact_duplicates_removed": exact_duplicates,
            "overlapping_chunk_ranges": sum(bool(item["overlaps"]) for item in manifest),
            "gaps": len(gaps),
            "chronological": bool(canonical["epoch"].is_monotonic_increasing),
            "one_second_intervals": not gaps,
        },
        "canonical_file": canonical_path.name,
    }
    _write_json_atomic(report, report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and canonicalise raw tick backfill data.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--processed-dir", type=Path, default=PROJECT_ROOT / "data" / "processed")
    args = parser.parse_args()
    input_dir = args.input_dir or PROJECT_ROOT / "data" / "raw" / args.symbol
    report = validate_backfill(
        args.symbol,
        input_dir,
        args.processed_dir,
    )
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()