from __future__ import annotations

import json
import fcntl
import asyncio
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator, Protocol

import pandas as pd

from .ticks import deduplicate_ticks, frame_from_history, validate_ticks, write_parquet_atomic


class HistoryClient(Protocol):
    async def ticks_history(self, symbol: str, count: int, end: str | int) -> dict: ...


@dataclass(frozen=True)
class BackfillCheckpoint:
    symbol: str
    next_end: int
    batches_completed: int
    ticks_collected: int = 0
    target_ticks: int | None = None
    status: str = "RUNNING"
    requested_total: int | None = None


@contextmanager
def backfill_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"Another backfill is already running (lock: {path}).") from error
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def load_checkpoint(path: Path, symbol: str) -> BackfillCheckpoint | None:
    if not path.exists():
        return None
    values = json.loads(path.read_text(encoding="utf-8"))
    values.setdefault("ticks_collected", 0)
    values.setdefault("target_ticks", None)
    values.setdefault("status", "RUNNING")
    values.setdefault("requested_total", values.get("target_ticks"))
    checkpoint = BackfillCheckpoint(**values)
    if checkpoint.symbol != symbol:
        raise ValueError(f"Checkpoint {path} belongs to {checkpoint.symbol}, not {symbol}.")
    return checkpoint


def save_checkpoint(checkpoint: BackfillCheckpoint, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(asdict(checkpoint), indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _save_manifest(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError(f"Manifest {path} must contain a list.")
    return value


def _existing_ranges(output_dir: Path) -> list[tuple[Path, int, int]]:
    ranges = []
    for path in output_dir.glob("*.parquet"):
        frame = pd.read_parquet(path, columns=["epoch"])
        if not frame.empty:
            ranges.append((path, int(frame["epoch"].min()), int(frame["epoch"].max())))
    return ranges


def _check_batch_integrity(frame: pd.DataFrame, output_dir: Path) -> None:
    conflicts = frame.groupby(["symbol", "epoch"])["price"].nunique(dropna=False)
    if (conflicts > 1).any():
        raise ValueError("Downloaded batch contains conflicting prices for the same symbol and epoch.")
    earliest, latest = int(frame["epoch"].min()), int(frame["epoch"].max())
    for path, existing_earliest, existing_latest in _existing_ranges(output_dir):
        if earliest <= existing_latest and latest >= existing_earliest:
            raise ValueError(
                f"Downloaded batch range {earliest}..{latest} overlaps existing raw partition "
                f"{path.name} ({existing_earliest}..{existing_latest}); refusing to write."
            )


async def backfill(
    client: HistoryClient,
    symbol: str,
    batches: int | None,
    count: int,
    output_dir: Path,
    checkpoint_path: Path,
    target_ticks: int | None = None,
    delay_seconds: float = 0.0,
    manifest_path: Path | None = None,
) -> list[Path]:
    if batches is not None and batches <= 0 or count <= 0:
        raise ValueError("batches and count must be positive")
    if target_ticks is not None and target_ticks <= 0:
        raise ValueError("target_ticks must be positive")
    if delay_seconds < 0:
        raise ValueError("delay_seconds cannot be negative")
    if batches is None and target_ticks is None:
        raise ValueError("Either batches or target_ticks must be provided")
    manifest_path = manifest_path or checkpoint_path.with_name(f"{symbol}_backfill_manifest.json")
    with backfill_lock(output_dir.parent / f".{symbol}.backfill.lock"):
        checkpoint = load_checkpoint(checkpoint_path, symbol)
        end: str | int = checkpoint.next_end if checkpoint else "latest"
        completed = checkpoint.batches_completed if checkpoint else 0
        collected = checkpoint.ticks_collected if checkpoint else 0
        if checkpoint and target_ticks is not None and checkpoint.target_ticks not in (None, target_ticks):
            raise ValueError("Requested target_ticks does not match the existing checkpoint.")
        manifest = _load_manifest(manifest_path)
        paths: list[Path] = []
        if checkpoint and checkpoint.status == "HISTORY_BOUNDARY_REACHED":
            return paths
        if manifest and manifest[-1].get("rows", count) < count:
            last = manifest[-1]
            collected = checkpoint.ticks_collected if checkpoint else sum(item.get("rows", 0) for item in manifest)
            last.setdefault("request_end_epoch", last["latest_epoch"])
            last.setdefault("returned_start_epoch", last["earliest_epoch"])
            last.setdefault("returned_end_epoch", last["latest_epoch"])
            last.setdefault("row_count", last["rows"])
            last["requested_total"] = target_ticks if target_ticks is not None else checkpoint.target_ticks
            last["collected_total"] = collected
            last["stop_reason"] = "HISTORY_BOUNDARY_REACHED"
            _save_manifest(manifest, manifest_path)
            save_checkpoint(
                BackfillCheckpoint(
                    symbol=symbol,
                    next_end=checkpoint.next_end if checkpoint else int(last["earliest_epoch"]) - 1,
                    batches_completed=checkpoint.batches_completed if checkpoint else len(manifest),
                    ticks_collected=collected,
                    target_ticks=target_ticks if target_ticks is not None else checkpoint.target_ticks if checkpoint else None,
                    status="HISTORY_BOUNDARY_REACHED",
                    requested_total=target_ticks if target_ticks is not None else checkpoint.target_ticks if checkpoint else None,
                ),
                checkpoint_path,
            )
            print(
                f"history boundary already recorded: requested={last['requested_total']} "
                f"collected={collected}"
            )
            return paths
        stop_reason = "TARGET_REACHED" if target_ticks is not None and collected >= target_ticks else None
        while (
            (target_ticks is not None and collected < target_ticks)
            and (batches is None or completed < batches)
        ) or (target_ticks is None and completed < batches):
            request_count = min(count, target_ticks - collected) if target_ticks is not None else count
            if completed:
                await asyncio.sleep(delay_seconds)
            response = await client.ticks_history(symbol=symbol, count=request_count, end=end)
            frame = deduplicate_ticks(frame_from_history(symbol, response.get("history")))
            requested_end = end
            if target_ticks is not None:
                frame = frame.head(target_ticks - collected).reset_index(drop=True)
            stats = validate_ticks(frame)
            _check_batch_integrity(frame, output_dir)
            earliest, latest = int(frame["epoch"].min()), int(frame["epoch"].max())
            partial_history = requested_end != "latest" and stats["rows"] < request_count
            path = write_parquet_atomic(frame, output_dir / f"{symbol}_{earliest}_{latest}.parquet")
            paths.append(path)
            completed += 1
            collected += stats["rows"]
            end = earliest - 1
            manifest.append({
                "filename": path.name,
                "rows": stats["rows"],
                "earliest_epoch": earliest,
                "latest_epoch": latest,
                "request_end_epoch": requested_end,
                "returned_start_epoch": earliest,
                "returned_end_epoch": latest,
                "row_count": stats["rows"],
                "requested_total": target_ticks,
                "collected_total": collected,
                "stop_reason": "HISTORY_BOUNDARY_REACHED" if partial_history else None,
            })
            if partial_history:
                stop_reason = "HISTORY_BOUNDARY_REACHED"
            _save_manifest(manifest, manifest_path)
            save_checkpoint(
                BackfillCheckpoint(
                    symbol=symbol,
                    next_end=end,
                    batches_completed=completed,
                    ticks_collected=collected,
                    target_ticks=target_ticks,
                    status=stop_reason or "RUNNING",
                    requested_total=target_ticks,
                ),
                checkpoint_path,
            )
            print(
                f"batch {completed}: rows={stats['rows']} total={collected} duplicates={stats['duplicate_rows']} "
                f"range={earliest}..{latest} saved={path.name}"
            )
            if earliest == 0:
                break
            if partial_history:
                print(
                    f"history boundary reached: requested={target_ticks} collected={collected}"
                )
                break
        return paths
