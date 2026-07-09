"""
Download 10-year daily OHLCV for Nifty 500 -> partitioned Parquet -> DuckDB.

Layout (Hive-partitioned, upgrade-friendly for more markets/symbols later):
    data/market=IN/symbol=RELIANCE/year=2024/data.parquet
    ...

DuckDB (prices.duckdb) exposes a view `prices` over ALL parquet files,
so you get one SQL database now and can just add more files later.

Usage:
    pip install yfinance pandas requests duckdb pyarrow
    python nifty500_ohlcv.py            # download + (re)build view
    python nifty500_ohlcv.py --view     # only rebuild the DuckDB view
"""

import os
import sys
import time
import requests
import pandas as pd
import yfinance as yf
import duckdb

# ---------- config ----------
MARKET = "IN"
YEARS = 10
START = (pd.Timestamp.today() - pd.DateOffset(years=YEARS)).strftime("%Y-%m-%d")
END = pd.Timestamp.today().strftime("%Y-%m-%d")
DATA_DIR = "data"
DB_PATH = "prices.duckdb"
LIST_CSV = "nifty500_list.csv"
SLEEP = 1.0
NSE_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
# ----------------------------


def get_nifty500_symbols():
    """Fetch (and cache) Nifty 500 constituents. Returns list of NSE symbols."""
    if not os.path.exists(LIST_CSV):
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US"})
        s.get("https://www.nseindia.com", timeout=15)      # set cookies
        r = s.get(NSE_URL, timeout=15)
        r.raise_for_status()
        with open(LIST_CSV, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(LIST_CSV)
    return [str(sym).strip() for sym in df["Symbol"]]


def write_partitions(symbol, df):
    """Write one symbol's OHLCV as year-partitioned parquet."""
    # newer yfinance returns MultiIndex columns like ('Close','SYM.NS'); flatten them
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df.columns = [str(c).lower() for c in df.columns]  # date, open, high, low, close, volume
    df["year"] = pd.to_datetime(df["date"]).dt.year
    for year, g in df.groupby("year"):
        out = os.path.join(
            DATA_DIR, f"market={MARKET}", f"symbol={symbol}", f"year={year}"
        )
        os.makedirs(out, exist_ok=True)
        g.drop(columns=["year"]).to_parquet(
            os.path.join(out, "data.parquet"), index=False
        )


def download_all(symbols):
    done, failed = [], []
    for i, sym in enumerate(symbols, 1):
        marker = os.path.join(DATA_DIR, f"market={MARKET}", f"symbol={sym}")
        if os.path.isdir(marker):                     # resume: skip finished symbols
            done.append(sym)
            continue
        try:
            df = yf.download(
                f"{sym}.NS", start=START, end=END,
                auto_adjust=True, progress=False,
            )
            if df.empty:
                failed.append(sym)
                print(f"[{i}/{len(symbols)}] {sym}: EMPTY")
            else:
                write_partitions(sym, df)
                done.append(sym)
                print(f"[{i}/{len(symbols)}] {sym}: {len(df)} rows")
        except Exception as e:
            failed.append(sym)
            print(f"[{i}/{len(symbols)}] {sym}: ERROR {e}")
        time.sleep(SLEEP)
    return done, failed


def build_view():
    """(Re)create a DuckDB view over every parquet file under DATA_DIR."""
    con = duckdb.connect(DB_PATH)
    glob = f"{DATA_DIR}/**/*.parquet"
    con.execute(f"""
        CREATE OR REPLACE VIEW prices AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning = true);
    """)
    n = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    mkts = con.execute("SELECT count(DISTINCT market) FROM prices").fetchone()[0]
    syms = con.execute("SELECT count(DISTINCT symbol) FROM prices").fetchone()[0]
    con.close()
    print(f"\nDuckDB '{DB_PATH}': view 'prices' -> {n:,} rows | "
          f"{syms} symbols | {mkts} market(s)")


def load_to_motherduck():
    """Replace the MotherDuck 'prices' table with current local parquet data."""
    token = os.environ.get("motherduck_token")
    if not token:
        return
    print("\nLoading to MotherDuck...")
    glob = f"{DATA_DIR}/**/*.parquet"
    con = duckdb.connect("md:stocks")
    con.execute(f"""
        CREATE OR REPLACE TABLE prices AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning = true)
    """)
    n = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    syms = con.execute("SELECT count(DISTINCT symbol) FROM prices").fetchone()[0]
    con.close()
    print(f"MotherDuck 'stocks.prices': {n:,} rows | {syms} symbols")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    if "--view" in sys.argv:
        build_view()
        sys.exit(0)

    symbols = get_nifty500_symbols()
    print(f"Universe: {len(symbols)} symbols | {START} -> {END}")
    done, failed = download_all(symbols)
    print(f"\nDownloaded/present: {len(done)}  Failed: {len(failed)}")
    if failed:
        print("Failed:", ", ".join(failed))
    build_view()
    load_to_motherduck()
