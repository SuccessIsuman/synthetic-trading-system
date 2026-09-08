import tempfile
import unittest
from pathlib import Path

import pandas as pd

from synthetic_trading.bars import BAR_COLUMNS, generate_bars


def ticks(prices: list[float], start: int = 0) -> pd.DataFrame:
    epochs = list(range(start, start + len(prices)))
    return pd.DataFrame({
        "symbol": "TEST",
        "epoch": epochs,
        "price": prices,
        "timestamp_utc": pd.to_datetime(epochs, unit="s", utc=True),
    })


class BarTests(unittest.TestCase):
    def test_ohlc_wicks_and_body(self):
        prices = [10.0] + [10.0] * 58 + [12.0]
        prices[30] = 15.0
        prices[40] = 8.0
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ticks(prices).to_parquet(root / "ticks.parquet", index=False)
            report = generate_bars(root / "ticks.parquet", root / "bars.parquet", root / "report.json", "TEST")
            bar = pd.read_parquet(root / "bars.parquet").iloc[0]
            self.assertEqual(report["summary"]["complete_minutes"], 1)
            self.assertEqual(bar["open"], 10.0)
            self.assertEqual(bar["high"], 15.0)
            self.assertEqual(bar["low"], 8.0)
            self.assertEqual(bar["close"], 12.0)
            self.assertEqual(bar["range"], 7.0)
            self.assertEqual(bar["body"], 2.0)
            self.assertEqual(bar["upper_wick"], 3.0)
            self.assertEqual(bar["lower_wick"], 2.0)

    def test_incomplete_minutes_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ticks([1.0] * 59).to_parquet(root / "ticks.parquet", index=False)
            report = generate_bars(root / "ticks.parquet", root / "bars.parquet", root / "report.json", "TEST")
            self.assertEqual(report["summary"]["complete_minutes"], 0)
            self.assertEqual(report["summary"]["rejected_minutes"], 1)
            self.assertEqual(len(pd.read_parquet(root / "bars.parquet")), 0)

    def test_rejects_non_contiguous_canonical_ticks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            frame = ticks([1.0] * 60)
            frame.loc[30:, "epoch"] += 1
            frame.to_parquet(root / "ticks.parquet", index=False)
            with self.assertRaisesRegex(ValueError, "one second apart"):
                generate_bars(root / "ticks.parquet", root / "bars.parquet", root / "report.json", "TEST")

    def test_output_columns_and_utc_timestamp(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ticks([1.0] * 60).to_parquet(root / "ticks.parquet", index=False)
            generate_bars(root / "ticks.parquet", root / "bars.parquet", root / "report.json", "TEST")
            bars = pd.read_parquet(root / "bars.parquet")
            self.assertEqual(list(bars.columns), BAR_COLUMNS)
            self.assertEqual(str(bars["timestamp_utc"].dt.tz), "UTC")
            self.assertEqual(int(bars.iloc[0]["tick_count"]), 60)


if __name__ == "__main__":
    unittest.main()