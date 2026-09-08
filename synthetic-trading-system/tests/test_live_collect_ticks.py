import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from live_collect_ticks import LiveTickCollector


async def event_stream(events):
    for event in events:
        yield event


class LiveCollectorTests(unittest.TestCase):
    def test_rejects_malformed_duplicate_and_out_of_order_events(self):
        with tempfile.TemporaryDirectory() as temp:
            collector = LiveTickCollector("1HZ100V", Path(temp), flush_ticks=10, session_id="test")
            events = [
                {"tick": {"symbol": "1HZ100V", "epoch": 10, "quote": 100}},
                {"tick": {"symbol": "1HZ100V", "epoch": 10, "quote": 100}},
                {"tick": {"symbol": "1HZ100V", "epoch": 9, "quote": 99}},
                {"unexpected": True},
                {"tick": {"symbol": "OTHER", "epoch": 11, "quote": 101}},
            ]
            summary = asyncio.run(collector.collect_events(event_stream(events)))
            self.assertEqual(summary["accepted_ticks"], 1)
            self.assertEqual(summary["rejected_ticks"], {
                "exact_duplicate": 1,
                "out_of_order": 1,
                "malformed_tick_message": 1,
                "wrong_symbol": 1,
            })
            frame = pd.read_parquet(next((Path(temp) / "1HZ100V").glob("*.parquet")))
            self.assertEqual(list(frame.columns), [
                "symbol", "epoch", "price", "timestamp_utc", "received_at_utc", "session_sequence"
            ])

    def test_flushes_small_immutable_partitions_and_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            collector = LiveTickCollector("1HZ100V", root, flush_ticks=2, session_id="session")
            events = event_stream([
                {"tick": {"symbol": "1HZ100V", "epoch": 1, "quote": 1}},
                {"tick": {"symbol": "1HZ100V", "epoch": 2, "quote": 2}},
                {"tick": {"symbol": "1HZ100V", "epoch": 3, "quote": 3}},
            ])
            summary = asyncio.run(collector.collect_events(events, max_ticks=3))
            self.assertEqual(summary["status"], "MAX_TICKS_REACHED")
            self.assertEqual(len(list((root / "1HZ100V").glob("*.parquet"))), 2)
            checkpoint = json.loads((root / "1HZ100V_live_checkpoint.json").read_text())
            self.assertEqual(checkpoint["accepted_ticks"], 3)
            self.assertEqual(checkpoint["status"], "MAX_TICKS_REACHED")


if __name__ == "__main__":
    unittest.main()