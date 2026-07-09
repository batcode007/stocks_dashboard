# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Interactive stock data explorer for Nifty 500 (Indian market). A two-stage system: a data pipeline that downloads OHLCV history into partitioned Parquet files, and a Streamlit dashboard that queries them via DuckDB.

## Commands

```bash
# Install dependencies
pip install yfinance pandas requests duckdb pyarrow lxml streamlit

# Download all markets (Nifty 500 + S&P 500) and build DuckDB view
python pipeline.py

# Rebuild only the DuckDB view (skips downloading)
python pipeline.py --view

# Run the dashboard
streamlit run app.py
```

## Architecture

**Pipeline (`pipeline.py`)**
- Iterates over `MARKETS` dict — currently `IN` (Nifty 500) and `US` (S&P 500)
- Each market has a `get_symbols` fn and a `ticker_fn` to convert symbol → Yahoo Finance ticker
  - IN: appends `.NS` suffix; symbol list fetched from NSE and cached to `nifty500_list.csv`
  - US: replaces `.` with `-` (e.g. `BRK.B` → `BRK-B`); symbol list scraped from Wikipedia, cached to `sp500_list.csv`
- Downloads 10 years of daily OHLCV via `yfinance`, writes Hive-partitioned Parquet: `data/market={MKT}/symbol={SYM}/year={YEAR}/data.parquet`
- Downloads are resumable — any symbol directory that already exists is skipped
- Creates/replaces a DuckDB view `prices` in `prices.duckdb` using `hive_partitioning=true`
- If `motherduck_token` env var is set, loads all parquet into a MotherDuck table `stocks.prices`

**Dashboard (`app.py`)**
- Opens `prices.duckdb` read-only locally, or connects to `md:stocks` if `motherduck_token` is set
- Sidebar: market → symbol multiselect → date range → price field (OHLCV)
- Queries the `prices` view with parameterized SQL, pivots on `symbol` for the line chart
- Exposes CSV download of filtered data

**Key design choices:**
- The `prices` view globs `data/**/*.parquet` — adding new markets means adding a new entry to `MARKETS` and running the pipeline; no schema changes needed
- Columns are lowercase (`date, open, high, low, close, volume, symbol, market`) due to normalization in `write_partitions()`
- Adding a new market: add entry to `MARKETS` dict with `get_symbols` and `ticker_fn`
