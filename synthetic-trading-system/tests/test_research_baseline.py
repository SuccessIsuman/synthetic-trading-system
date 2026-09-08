import unittest

import pandas as pd

from research_baseline import build_baseline_report


def make_bars(closes: list[float]) -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=len(closes), freq="min", tz="UTC")
    return pd.DataFrame({
        "symbol": "TEST",
        "timeframe": "1m",
        "timestamp_utc": timestamps,
        "close": closes,
    })


class ResearchBaselineTests(unittest.TestCase):
    def test_returns_statistics_autocorrelation_and_run_lengths(self):
        report = build_baseline_report(make_bars([100, 101, 102, 101, 100, 101]), "bars.parquet")
        stats = report["partitions"]["train"]["simple_returns"]
        self.assertEqual(stats["count"], 3)
        self.assertAlmostEqual(stats["positive_return_rate"], 2 / 3)
        self.assertEqual(stats["positive_run_lengths"], {"2": 1})
        self.assertEqual(stats["negative_run_lengths"], {"1": 1})
        self.assertIn("1", stats["autocorrelation"])
        self.assertIn("p50", stats["percentiles"])

    def test_naive_baselines_and_chronological_splits(self):
        closes = [100 + index for index in range(20)]
        report = build_baseline_report(make_bars(closes), "bars.parquet")
        self.assertEqual(
            {name: item["bar_count"] for name, item in report["partitions"].items()},
            {"train": 14, "validation": 3, "test": 3},
        )
        baselines = report["partitions"]["test"]["naive_baselines"]
        self.assertEqual(baselines["always_up"]["accuracy"], 1.0)
        self.assertEqual(baselines["always_down"]["accuracy"], 0.0)
        self.assertEqual(baselines["random_50_50"]["expected_accuracy"], 0.5)
        self.assertEqual(baselines["previous_bar_direction"]["accuracy"], 1.0)

    def test_marks_short_dataset_insufficient(self):
        report = build_baseline_report(make_bars([100, 101, 100]), "bars.parquet")
        self.assertEqual(report["edge_claim_status"], "INSUFFICIENT_FOR_EDGE_CLAIM")

    def test_rejects_unordered_or_duplicate_bars(self):
        frame = make_bars([100, 101, 102])
        frame.loc[1, "timestamp_utc"] = frame.loc[0, "timestamp_utc"]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build_baseline_report(frame, "bars.parquet")


if __name__ == "__main__":
    unittest.main()