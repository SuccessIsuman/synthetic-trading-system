from __future__ import annotations

import argparse
import json
from pathlib import Path

from synthetic_trading.bars import generate_bars
from synthetic_trading.config import PROJECT_ROOT


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate complete UTC one-minute OHLC bars.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--input-path", type=Path)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args()
    processed = PROJECT_ROOT / "data" / "processed"
    report = generate_bars(
        args.input_path or processed / f"{args.symbol}_ticks_canonical.parquet",
        args.output_path or processed / f"{args.symbol}_1m.parquet",
        args.report_path or processed / f"{args.symbol}_1m_quality_report.json",
        args.symbol,
    )
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()