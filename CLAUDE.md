# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Interactive stock data explorer for Nifty 500 (Indian market). A two-stage system: a data pipeline that downloads OHLCV history into partitioned Parquet files, and a Streamlit dashboard that queries them via DuckDB.

## Commands

```bash
# Install dependencies
pip install yfinance pandas requests duckdb pyarrow streamlit

# Download Nifty 500 OHLCV data (10 years) and build DuckDB view
python nifty500_ohlcv.py

# Rebuild only the DuckDB view (skips downloading)
python nifty500_ohlcv.py --view

# Run the dashboard
streamlit run app.py
```

## Architecture

**Pipeline (`nifty500_ohlcv.py`)**
1. Fetches Nifty 500 symbol list from NSE, cached to `nifty500_list.csv`
2. Downloads 10 years of daily OHLCV via `yfinance` (appends `.NS` suffix for NSE tickers)
3. Writes Hive-partitioned Parquet: `data/market=IN/symbol={SYM}/year={YEAR}/data.parquet`
4. Downloads are resumable — any symbol directory that already exists is skipped
5. Creates/replaces a DuckDB view `prices` in `prices.duckdb` using `hive_partitioning=true`

**Dashboard (`app.py`)**
- Opens `prices.duckdb` read-only with `@st.cache_resource`
- Sidebar: market → symbol multiselect → date range → price field (OHLCV)
- Queries the `prices` view with parameterized SQL, pivots on `symbol` for the line chart
- Exposes CSV download of filtered data

**Key design choices:**
- The `prices` view globs `data/**/*.parquet` — adding new markets or symbols just means adding files, no schema migration
- `app.py` queries DuckDB directly from parquet at runtime; no separate ETL step needed for new data
- Columns are lowercase (`date, open, high, low, close, volume, symbol, market`) due to normalization in `write_partitions()`
