# Synthetic Trading System - reliable data collector

This is a read-only market-data collector. It does not place trades. It can:

1. connect to Deriv's public Options WebSocket;
2. discover currently active synthetic-index symbols;
3. download a small historical tick sample;
4. validate and atomically save raw data as Parquet;
5. backfill batches with a restart-safe checkpoint.

## Requirements

- Python 3.11+ recommended

## Setup

```bash
python -m venv .venv
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## 1. Discover symbols

```bash
python src/discover_symbols.py
```

Copy one of the exact symbols printed by the script.

## 2. Download 1,000 ticks

Example only — replace `YOUR_SYMBOL` with a symbol returned above:

```bash
python src/download_sample.py --symbol YOUR_SYMBOL --count 1000
```

The output is written to `data/raw/` as Parquet.

## 3. Backfill safely

```bash
python src/backfill_ticks.py --symbol 1HZ100V --batches 10 --count 1000
```

Each completed batch is saved under `data/raw/<symbol>/`. A checkpoint is written after each
successful batch, so rerunning resumes from the preceding epoch.

## Integrity checks

The sample downloader verifies:

- a non-empty response;
- equal timestamp and price array lengths;
- numeric timestamps and prices;
- chronological ordering without silently sorting an unordered response;
- non-positive prices;
- duplicate rows;
- atomic Parquet and checkpoint persistence.

## Raw-data rule

Do not manually edit files in `data/raw/`. Indicators, candles, features and labels will be generated into downstream datasets.

## Next milestone

After this smoke test succeeds, build:

- paginated historical backfill;
- restart-safe manifests/checkpoints;
- live tick subscription;
- gap and duplicate detection;
- partitioned Parquet storage by symbol/date;
- reproducible data-quality reports.
