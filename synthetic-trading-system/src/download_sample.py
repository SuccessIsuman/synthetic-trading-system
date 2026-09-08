from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from synthetic_trading.config import RAW_DATA_DIR
from synthetic_trading.deriv_client import DerivPublicClient
from synthetic_trading.ticks import (
    deduplicate_ticks,
    frame_from_history,
    validate_ticks,
    write_parquet_atomic,
)


async def download(symbol: str, count: int, output_dir: Path) -> Path:
    response = await DerivPublicClient().ticks_history(symbol=symbol, count=count)
    df = frame_from_history(symbol, response.get("history"))
    source_stats = validate_ticks(df)
    df = deduplicate_ticks(df)
    stats = validate_ticks(df)
    path = write_parquet_atomic(df, output_dir / f"{symbol}_sample_{len(df)}.parquet")
    print("\nValidation")
    for key, value in source_stats.items():
        print(f"  source_{key}: {value}")
    for key, value in stats.items():
        print(f"  saved_{key}: {value}")
    print(f"\nSaved: {path}")

    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a small Deriv tick-history sample.")
    parser.add_argument("--symbol", required=True, help="Symbol returned by discover_symbols.py")
    parser.add_argument("--count", type=int, default=1000, help="Number of ticks to request")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.count <= 0:
        raise SystemExit("--count must be positive")
    asyncio.run(download(args.symbol, args.count, RAW_DATA_DIR))
