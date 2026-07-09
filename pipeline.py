"""
Multi-market OHLCV downloader → partitioned Parquet → DuckDB / MotherDuck.

Markets:
  IN  Nifty 500 (NSE)
  US  S&P 500 (NYSE / NASDAQ)

Layout:  data/market={MKT}/symbol={SYM}/year={YEAR}/data.parquet

Usage:
    pip install yfinance pandas requests duckdb pyarrow lxml
    python pipeline.py            # all markets
    python pipeline.py --view     # only rebuild DuckDB view
"""

import os
import sys
import time
import requests
import pandas as pd
import yfinance as yf
import duckdb

# ---------- config ----------
YEARS = 10
START = (pd.Timestamp.today() - pd.DateOffset(years=YEARS)).strftime("%Y-%m-%d")
END = pd.Timestamp.today().strftime("%Y-%m-%d")
DATA_DIR = "data"
DB_PATH = "prices.duckdb"
SLEEP = 1.0
# ----------------------------


def get_nifty500_symbols():
    list_csv = "nifty500_list.csv"
    nse_url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
    if not os.path.exists(list_csv):
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US"})
        s.get("https://www.nseindia.com", timeout=15)
        r = s.get(nse_url, timeout=15)
        r.raise_for_status()
        with open(list_csv, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(list_csv)
    return [str(sym).strip() for sym in df["Symbol"]]


def get_sp500_symbols():
    list_csv = "sp500_list.csv"
    if not os.path.exists(list_csv):
        df = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        )[0]
        df.to_csv(list_csv, index=False)
    df = pd.read_csv(list_csv)
    return [str(sym).strip() for sym in df["Symbol"]]


MARKETS = {
    "IN": {
        "get_symbols": get_nifty500_symbols,
        "ticker_fn": lambda sym: f"{sym}.NS",
    },
    "US": {
        "get_symbols": get_sp500_symbols,
        "ticker_fn": lambda sym: sym.replace(".", "-"),  # BRK.B → BRK-B on Yahoo Finance
    },
}


def write_partitions(market, symbol, df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df.columns = [str(c).lower() for c in df.columns]
    df["year"] = pd.to_datetime(df["date"]).dt.year
    for year, g in df.groupby("year"):
        out = os.path.join(DATA_DIR, f"market={market}", f"symbol={symbol}", f"year={year}")
        os.makedirs(out, exist_ok=True)
        g.drop(columns=["year"]).to_parquet(os.path.join(out, "data.parquet"), index=False)


def download_all(market, symbols, ticker_fn):
    done, failed = [], []
    for i, sym in enumerate(symbols, 1):
        marker = os.path.join(DATA_DIR, f"market={market}", f"symbol={sym}")
        if os.path.isdir(marker):
            done.append(sym)
            continue
        try:
            ticker = ticker_fn(sym)
            df = yf.download(ticker, start=START, end=END, auto_adjust=True, progress=False)
            if df.empty:
                failed.append(sym)
                print(f"[{i}/{len(symbols)}] {sym}: EMPTY")
            else:
                write_partitions(market, sym, df)
                done.append(sym)
                print(f"[{i}/{len(symbols)}] {sym}: {len(df)} rows")
        except Exception as e:
            failed.append(sym)
            print(f"[{i}/{len(symbols)}] {sym}: ERROR {e}")
        time.sleep(SLEEP)
    return done, failed


def build_view():
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
    print(f"\nDuckDB '{DB_PATH}': view 'prices' -> {n:,} rows | {syms} symbols | {mkts} market(s)")


def load_to_motherduck():
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

    for market, cfg in MARKETS.items():
        symbols = cfg["get_symbols"]()
        print(f"\n=== {market}: {len(symbols)} symbols | {START} -> {END} ===")
        done, failed = download_all(market, symbols, cfg["ticker_fn"])
        print(f"Downloaded/present: {len(done)}  Failed: {len(failed)}")
        if failed:
            print("Failed:", ", ".join(failed))

    build_view()
    load_to_motherduck()
