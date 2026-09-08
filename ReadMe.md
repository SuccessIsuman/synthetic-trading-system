# Synthetic Trading System

A Python-based research and trading project focused on synthetic indices.

The goal of this project is to collect high-quality historical and live tick data, analyse market behaviour, identify trends and volatility regimes, and test whether statistical or machine-learning models can find repeatable trading opportunities.

## Current Stage

The project is currently focused on data collection and validation.

The first version includes:

* Synthetic index symbol discovery
* Historical tick data collection
* Parquet data storage
* Duplicate and missing-data checks
* Timestamp validation
* Basic automated tests

## Planned Development

The next stages will include:

* Large-scale historical data collection
* Live tick streaming
* Multi-timeframe candle generation
* Trend and volatility analysis
* Market structure detection
* Feature engineering
* Statistical testing
* Machine-learning models
* Backtesting and walk-forward testing
* Risk management and trade execution logic

## Project Goal

The objective is not to predict every market movement with certainty. The system will aim to identify market conditions where the probability and expected reward of a trade are favourable.

## Technology

* Python
* Deriv WebSocket API
* Pandas
* PyArrow / Parquet
* Pytest
* GitHub Codespaces

## Disclaimer

This project is for research and educational purposes. Trading synthetic indices involves significant risk, and no trading model can guarantee profitable results.
