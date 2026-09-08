from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from synthetic_trading.backfill import backfill
from synthetic_trading.config import RAW_DATA_DIR
from synthetic_trading.deriv_client import DerivPublicClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restart-safe Deriv historical tick backfill.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--output-root", type=Path, default=RAW_DATA_DIR)
    parser.add_argument("--checkpoint-path", type=Path)
    parser.add_argument("--manifest-path", type=Path)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--target-ticks", type=int)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    checkpoint_path = args.checkpoint_path or args.output_root / f"{args.symbol}_backfill_checkpoint.json"
    manifest_path = args.manifest_path or args.output_root / f"{args.symbol}_backfill_manifest.json"
    await backfill(
        DerivPublicClient(),
        symbol=args.symbol,
        batches=args.batches if args.target_ticks is None else None,
        count=args.count,
        output_dir=args.output_root / args.symbol,
        checkpoint_path=checkpoint_path,
        target_ticks=args.target_ticks,
        delay_seconds=args.delay_seconds,
        manifest_path=manifest_path,
    )


if __name__ == "__main__":
    asyncio.run(main())
