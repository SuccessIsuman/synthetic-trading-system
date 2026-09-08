import unittest
import asyncio
import tempfile
from pathlib import Path

import json
import pandas as pd

from discover_symbols import symbol_fields
from synthetic_trading.deriv_client import DerivPublicClient
from synthetic_trading.backfill import backfill, load_checkpoint
from synthetic_trading.ticks import deduplicate_ticks, frame_from_history, validate_ticks
from validate_backfill import validate_backfill


class FakeHistoryClient:
    def __init__(self) -> None:
        self.calls: list[str | int] = []

    async def ticks_history(self, symbol: str, count: int, end: str | int) -> dict:
        self.calls.append(end)
        if end == "latest":
            return {"history": {"times": [100, 101, 102], "prices": [10.0, 10.1, 10.2]}}
        return {"history": {"times": [97, 98, 99], "prices": [9.7, 9.8, 9.9]}}


class BoundaryHistoryClient:
    def __init__(self) -> None:
        self.calls: list[str | int] = []

    async def ticks_history(self, symbol: str, count: int, end: str | int) -> dict:
        self.calls.append(end)
        if len(self.calls) == 1:
            return {"history": {"times": list(range(100, 110)), "prices": [10.0] * 10}}
        if len(self.calls) == 2:
            return {"history": {"times": list(range(90, 93)), "prices": [9.0] * 3}}
        raise AssertionError("backfill requested another page after the history boundary")


class CoreTests(unittest.TestCase):
    def test_current_symbol_fields(self):
        item = {
            "underlying_symbol": "1HZ100V",
            "underlying_symbol_name": "Volatility 100 (1s) Index",
            "market": "synthetic_index",
            "submarket": "random_index",
        }
        self.assertEqual(
            symbol_fields(item),
            ("1HZ100V", "Volatility 100 (1s) Index", "synthetic_index", "random_index"),
        )

    def test_legacy_fallback_symbol_fields(self):
        item = {"symbol": "TEST", "display_name": "Test Index"}
        self.assertEqual(symbol_fields(item), ("TEST", "Test Index", "", ""))

    def test_history_conversion_and_validation(self):
        df = frame_from_history(
            "1HZ100V", {"times": [1, 2, 3], "prices": [100.0, 100.2, 100.1]}
        )
        stats = validate_ticks(df)
        self.assertEqual(stats["rows"], 3)
        self.assertTrue(stats["chronological"])
        self.assertEqual(stats["duplicate_rows"], 0)

    def test_rejects_unordered_history_without_sorting_it_away(self):
        with self.assertRaisesRegex(ValueError, "chronologically"):
            frame_from_history("1HZ100V", {"times": [2, 1], "prices": [100.0, 100.1]})

    def test_reports_then_removes_only_exact_duplicates(self):
        df = frame_from_history(
            "1HZ100V", {"times": [1, 2, 2], "prices": [100.0, 100.2, 100.2]}
        )
        self.assertEqual(validate_ticks(df)["duplicate_rows"], 1)
        self.assertEqual(len(deduplicate_ticks(df)), 2)

    def test_rejects_nonpositive_price(self):
        with self.assertRaisesRegex(ValueError, "non-positive"):
            frame_from_history("1HZ100V", {"times": [1], "prices": [0]})

    def test_restart_safe_backfill(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            client = FakeHistoryClient()
            paths = asyncio.run(
                backfill(client, "1HZ100V", 2, 3, root / "data", root / "checkpoint.json")
            )
            self.assertEqual(client.calls, ["latest", 99])
            self.assertEqual(len(paths), 2)
            self.assertTrue(all(path.exists() for path in paths))
            checkpoint = load_checkpoint(root / "checkpoint.json", "1HZ100V")
            self.assertEqual(checkpoint.next_end, 96)
            self.assertEqual(checkpoint.batches_completed, 2)

    def test_backfill_rejects_overlapping_partition(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            existing = frame_from_history("1HZ100V", {"times": [100, 101], "prices": [1, 1.1]})
            (root / "data").mkdir()
            existing.to_parquet(root / "data" / "1HZ100V_100_101.parquet", index=False)
            with self.assertRaisesRegex(ValueError, "overlaps existing raw partition"):
                asyncio.run(backfill(FakeHistoryClient(), "1HZ100V", 1, 3, root / "data", root / "checkpoint.json"))

    def test_backfill_target_stops_at_requested_valid_ticks_in_fresh_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            client = FakeHistoryClient()
            paths = asyncio.run(
                backfill(
                    client,
                    "1HZ100V",
                    None,
                    5,
                    root / "raw_clean" / "1HZ100V",
                    root / "checkpoints" / "checkpoint.json",
                    target_ticks=5,
                    manifest_path=root / "manifests" / "manifest.json",
                )
            )
            self.assertEqual(sum(len(pd.read_parquet(path)) for path in paths), 5)
            self.assertEqual(client.calls, ["latest", 99])
            self.assertTrue((root / "checkpoints" / "checkpoint.json").exists())
            self.assertTrue((root / "manifests" / "manifest.json").exists())

    def test_backfill_target_resumes_from_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / "raw_clean" / "1HZ100V"
            checkpoint = root / "checkpoint.json"
            first_client = FakeHistoryClient()
            asyncio.run(backfill(first_client, "1HZ100V", 1, 3, output, checkpoint, target_ticks=6))
            second_client = FakeHistoryClient()
            paths = asyncio.run(backfill(second_client, "1HZ100V", 2, 3, output, checkpoint, target_ticks=6))
            self.assertEqual(second_client.calls, [99])
            self.assertEqual(len(paths), 1)
            saved = load_checkpoint(checkpoint, "1HZ100V")
            self.assertEqual(saved.ticks_collected, 6)

    def test_backfill_stops_after_partial_non_latest_history_page(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            client = BoundaryHistoryClient()
            manifest_path = root / "manifest.json"
            paths = asyncio.run(
                backfill(client, "1HZ100V", 10, 10, root / "raw_clean" / "1HZ100V", root / "checkpoint.json", manifest_path=manifest_path)
            )
            self.assertEqual(len(paths), 2)
            self.assertEqual(client.calls, ["latest", 99])
            checkpoint = json.loads((root / "checkpoint.json").read_text())
            self.assertEqual(checkpoint["status"], "HISTORY_BOUNDARY_REACHED")
            self.assertEqual(checkpoint["ticks_collected"], 13)
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest[-1]["request_end_epoch"], 99)
            self.assertEqual(manifest[-1]["returned_start_epoch"], 90)
            self.assertEqual(manifest[-1]["returned_end_epoch"], 92)
            self.assertEqual(manifest[-1]["row_count"], 3)
            self.assertIsNone(manifest[-1]["requested_total"])
            self.assertEqual(manifest[-1]["collected_total"], 13)
            self.assertEqual(manifest[-1]["stop_reason"], "HISTORY_BOUNDARY_REACHED")

    def test_canonicalisation_removes_exact_duplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw" / "1HZ100V"
            raw.mkdir(parents=True)
            frame = frame_from_history("1HZ100V", {"times": [1, 2], "prices": [1, 2]})
            frame.to_parquet(raw / "a.parquet", index=False)
            frame.iloc[[1]].to_parquet(raw / "b.parquet", index=False)
            report = validate_backfill("1HZ100V", raw, root / "processed")
            self.assertEqual(report["summary"]["exact_duplicates_removed"], 1)
            self.assertEqual(report["summary"]["canonical_rows"], 2)

    def test_canonicalisation_rejects_conflicting_prices(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw" / "1HZ100V"
            raw.mkdir(parents=True)
            frame = frame_from_history("1HZ100V", {"times": [1], "prices": [1]})
            conflicting = frame_from_history("1HZ100V", {"times": [1], "prices": [2]})
            frame.to_parquet(raw / "a.parquet", index=False)
            conflicting.to_parquet(raw / "b.parquet", index=False)
            with self.assertRaisesRegex(ValueError, "Conflicting prices"):
                validate_backfill("1HZ100V", raw, root / "processed")

    def test_public_requests_use_current_compatible_payloads(self):
        async def request(payload, expected_msg_type):
            if expected_msg_type == "active_symbols":
                self.assertEqual(payload, {"active_symbols": "brief", "req_id": 1})
                return {"active_symbols": []}
            self.assertNotIn("subscribe", payload)
            self.assertEqual(payload["ticks_history"], "1HZ100V")
            return {"history": {"times": [1], "prices": [1.0]}}

        async def exercise():
            client = DerivPublicClient()
            client._request = request  # type: ignore[method-assign]
            self.assertEqual(await client.active_symbols(), [])
            await client.ticks_history("1HZ100V", count=1)

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
