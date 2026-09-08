from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterable, AsyncIterator

import pandas as pd
import websockets

from synthetic_trading.config import PROJECT_ROOT, WS_URL
from synthetic_trading.ticks import write_parquet_atomic

TICK_COLUMNS = [
    "symbol",
    "epoch",
    "price",
    "timestamp_utc",
    "received_at_utc",
    "session_sequence",
]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_atomic(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


@asynccontextmanager
async def collector_lock(path: Path) -> AsyncIterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"Another live collector is already running (lock: {path}).") from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class LiveTickCollector:
    def __init__(
        self,
        symbol: str,
        output_root: Path,
        flush_ticks: int = 100,
        session_id: str | None = None,
    ) -> None:
        if flush_ticks <= 0:
            raise ValueError("flush_ticks must be positive")
        self.symbol = symbol
        self.output_dir = output_root / symbol
        self.flush_ticks = flush_ticks
        self.session_id = session_id or uuid.uuid4().hex
        self.checkpoint_path = output_root / f"{symbol}_live_checkpoint.json"
        self.manifest_path = output_root / f"{symbol}_live_manifest.json"
        self.lock_path = output_root / f".{symbol}.live.lock"
        self.buffer: list[dict[str, Any]] = []
        self.session_sequence = 0
        self.last_epoch: int | None = None
        self.seen_ticks: set[tuple[int, float]] = set()
        self.accepted = 0
        self.rejected: dict[str, int] = {}
        self.partitions: list[dict[str, Any]] = []

    def _reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1

    def accept_event(self, event: dict[str, Any]) -> bool:
        tick = event.get("tick") if isinstance(event, dict) else None
        if not isinstance(tick, dict):
            self._reject("malformed_tick_message")
            return False
        if tick.get("symbol") != self.symbol:
            self._reject("wrong_symbol")
            return False
        try:
            epoch = int(tick["epoch"])
            price = float(tick["quote"])
        except (KeyError, TypeError, ValueError, OverflowError):
            self._reject("malformed_tick_fields")
            return False
        if epoch < 0 or price <= 0:
            self._reject("invalid_tick_values")
            return False
        if (epoch, price) in self.seen_ticks:
            self._reject("exact_duplicate")
            return False
        if self.last_epoch is not None and epoch <= self.last_epoch:
            self._reject("out_of_order")
            return False
        self.session_sequence += 1
        self.last_epoch = epoch
        self.seen_ticks.add((epoch, price))
        self.accepted += 1
        self.buffer.append({
            "symbol": self.symbol,
            "epoch": epoch,
            "price": price,
            "timestamp_utc": pd.Timestamp(epoch, unit="s", tz="UTC"),
            "received_at_utc": pd.Timestamp.now(tz="UTC"),
            "session_sequence": self.session_sequence,
        })
        return True

    def _load_manifest(self) -> list[dict[str, Any]]:
        if not self.manifest_path.exists():
            return []
        value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []

    def flush(self) -> Path | None:
        if not self.buffer:
            return None
        frame = pd.DataFrame(self.buffer, columns=TICK_COLUMNS)
        first_sequence = int(frame["session_sequence"].iloc[0])
        last_sequence = int(frame["session_sequence"].iloc[-1])
        first_epoch = int(frame["epoch"].iloc[0])
        last_epoch = int(frame["epoch"].iloc[-1])
        path = self.output_dir / (
            f"{self.symbol}_{self.session_id}_{first_sequence:012d}_{last_sequence:012d}.parquet"
        )
        if path.exists():
            raise FileExistsError(f"Immutable live partition already exists: {path}")
        write_parquet_atomic(frame, path)
        record = {
            "filename": path.name,
            "session_id": self.session_id,
            "first_sequence": first_sequence,
            "last_sequence": last_sequence,
            "first_epoch": first_epoch,
            "last_epoch": last_epoch,
            "row_count": len(frame),
            "written_at_utc": _now_utc(),
        }
        self.partitions.append(record)
        _write_json_atomic(self._load_manifest() + [record], self.manifest_path)
        self.buffer.clear()
        self._save_checkpoint("RUNNING")
        return path

    def _save_checkpoint(self, status: str) -> None:
        _write_json_atomic({
            "symbol": self.symbol,
            "session_id": self.session_id,
            "status": status,
            "accepted_ticks": self.accepted,
            "rejected_ticks": self.rejected,
            "last_epoch": self.last_epoch,
            "next_session_sequence": self.session_sequence + 1,
            "partition_count": len(self.partitions),
            "updated_at_utc": _now_utc(),
        }, self.checkpoint_path)

    async def collect_events(
        self,
        events: AsyncIterable[dict[str, Any]],
        duration_seconds: float | None = None,
        max_ticks: int | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            async for event in events:
                if duration_seconds is not None and time.monotonic() - started >= duration_seconds:
                    break
                if max_ticks is not None and self.accepted >= max_ticks:
                    break
                if self.accept_event(event) and len(self.buffer) >= self.flush_ticks:
                    self.flush()
        finally:
            self.flush()
        status = "MAX_TICKS_REACHED" if max_ticks is not None and self.accepted >= max_ticks else "COMPLETED"
        if duration_seconds is not None and time.monotonic() - started >= duration_seconds:
            status = "DURATION_REACHED"
        self._save_checkpoint(status)
        return self.summary(status)

    def summary(self, status: str) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "session_id": self.session_id,
            "status": status,
            "accepted_ticks": self.accepted,
            "rejected_ticks": self.rejected,
            "partitions": len(self.partitions),
        }

    async def run(
        self,
        duration_seconds: float | None = None,
        max_ticks: int | None = None,
        url: str = WS_URL,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
    ) -> dict[str, Any]:
        started = time.monotonic()
        reconnect_delay = reconnect_base_seconds
        async with collector_lock(self.lock_path):
            self._save_checkpoint("RUNNING")
            try:
                while (max_ticks is None or self.accepted < max_ticks) and (
                    duration_seconds is None or time.monotonic() - started < duration_seconds
                ):
                    try:
                        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
                            await websocket.send(json.dumps({"ticks": self.symbol, "subscribe": 1, "req_id": 1}))
                            reconnect_delay = reconnect_base_seconds
                            while True:
                                if duration_seconds is not None and time.monotonic() - started >= duration_seconds:
                                    break
                                if max_ticks is not None and self.accepted >= max_ticks:
                                    break
                                event = json.loads(await websocket.recv())
                                if isinstance(event, dict) and "error" in event:
                                    raise RuntimeError(f"Deriv subscription error: {event['error']}")
                                if self.accept_event(event) and len(self.buffer) >= self.flush_ticks:
                                    self.flush()
                            break
                    except (asyncio.CancelledError, KeyboardInterrupt):
                        raise
                    except (OSError, asyncio.TimeoutError, websockets.WebSocketException, json.JSONDecodeError) as error:
                        print(f"live stream disconnected: {error}; reconnecting in {reconnect_delay:.1f}s")
                        self.flush()
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, reconnect_max_seconds)
            finally:
                self.flush()
                status = "MAX_TICKS_REACHED" if max_ticks is not None and self.accepted >= max_ticks else "DURATION_REACHED"
                self._save_checkpoint(status)
        return self.summary(status)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only live Deriv tick collector.")
    parser.add_argument("--symbol", default="1HZ100V")
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--max-ticks", type=int)
    parser.add_argument("--flush-ticks", type=int, default=100)
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "data" / "live_raw")
    args = parser.parse_args()
    if args.duration_seconds is None and args.max_ticks is None:
        parser.error("one of --duration-seconds or --max-ticks is required")
    collector = LiveTickCollector(args.symbol, args.output_root, args.flush_ticks)
    summary = await collector.run(args.duration_seconds, args.max_ticks)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass