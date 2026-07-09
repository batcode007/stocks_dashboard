"""
Streamlit explorer for the OHLCV DuckDB.

Run:
    pip install streamlit duckdb pandas
    streamlit run app.py
"""

import os

import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "prices.duckdb"

st.set_page_config(page_title="Stock OHLCV Explorer", layout="wide")
st.title("📈 Stock OHLCV Explorer")


# read-only connection, cached across reruns
@st.cache_resource
def get_con():
    if os.environ.get("motherduck_token"):
        return duckdb.connect("md:stocks")
    return duckdb.connect(DB_PATH, read_only=True)


con = get_con()


@st.cache_data
def load_filters():
    markets = con.execute(
        "SELECT DISTINCT market FROM prices ORDER BY 1"
    ).df()["market"].tolist()
    bounds = con.execute("SELECT min(date), max(date) FROM prices").fetchone()
    return markets, bounds


@st.cache_data
def load_symbols(market):
    return con.execute(
        "SELECT DISTINCT symbol FROM prices WHERE market = ? ORDER BY 1",
        [market],
    ).df()["symbol"].tolist()


markets, (dmin, dmax) = load_filters()

# ---------- sidebar filters ----------
with st.sidebar:
    st.header("Filters")
    market = st.selectbox("Market", markets)
    symbols = load_symbols(market)
    picked = st.multiselect(
        "Symbols", symbols, default=symbols[:1] if symbols else []
    )
    date_range = st.date_input(
        "Date range",
        value=(pd.Timestamp(dmin).date(), pd.Timestamp(dmax).date()),
        min_value=pd.Timestamp(dmin).date(),
        max_value=pd.Timestamp(dmax).date(),
    )
    price_field = st.selectbox(
        "Chart field", ["close", "open", "high", "low", "volume"], index=0
    )

if not picked:
    st.info("Pick at least one symbol from the sidebar.")
    st.stop()

start, end = date_range if isinstance(date_range, tuple) else (dmin, dmax)

# ---------- query ----------
placeholders = ",".join("?" for _ in picked)
df = con.execute(
    f"""
    SELECT date, symbol, open, high, low, close, volume
    FROM prices
    WHERE market = ?
      AND symbol IN ({placeholders})
      AND date BETWEEN ? AND ?
    ORDER BY date, symbol
    """,
    [market, *picked, str(start), str(end)],
).df()

if df.empty:
    st.warning("No rows for that selection.")
    st.stop()

# ---------- chart ----------
st.subheader(f"{price_field.capitalize()} over time")
chart = df.pivot_table(index="date", columns="symbol", values=price_field)
st.line_chart(chart)

# ---------- metrics ----------
c1, c2, c3 = st.columns(3)
c1.metric("Rows", f"{len(df):,}")
c2.metric("Symbols", df["symbol"].nunique())
c3.metric("Date span", f"{df['date'].min().date()} → {df['date'].max().date()}")

# ---------- table + export ----------
st.subheader("Data")
st.dataframe(df, use_container_width=True, height=350)
st.download_button(
    "⬇️ Download CSV",
    df.to_csv(index=False).encode(),
    file_name=f"{market}_{'_'.join(picked)}.csv",
    mime="text/csv",
)
