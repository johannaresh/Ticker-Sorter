import os
import sys
import asyncio
import streamlit as st
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import date

# Enable importing from parent dir
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ticker_sorter.timeframe import main as fetch_metrics_async  # accepts tickers list

# Load environment
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["marble"]
collection = db["tickers"]

# --- Sidebar Filters ---
min_cap = st.sidebar.number_input("Min Market Cap ($)", value=500_000_000)
sort_field = st.sidebar.selectbox("Sort By", [
    "market_cap", "pe_ratio", "eps", "net_profit_margin", "revenue_quarterly", "stock_change_pct"
])
sort_order = st.sidebar.radio("Sort Order", ["Descending", "Ascending"])
limit = st.sidebar.slider("Results Limit", 10, 2700, 50)

# --- Date Range ---
start_date = st.sidebar.date_input("Start Date", date(2025, 3, 3))
end_date = st.sidebar.date_input("End Date", date(2025, 3, 7))

# --- Query Tickers from DB ---
query = {"market_cap": {"$gte": min_cap}}
direction = -1 if sort_order == "Descending" else 1
results = list(collection.find(query).sort(sort_field, direction).limit(limit))
tickers = [r["ticker"] for r in results]

# --- Fetch & Save ---
if st.sidebar.button("📥 Fetch & Save Metrics to MongoDB"):
    with st.spinner("Fetching and saving stock metrics..."):
        metrics = asyncio.run(fetch_metrics_async(start_date.isoformat(), end_date.isoformat(), tickers))
        for m in metrics:
            collection.update_one(
                {"ticker": m["ticker"]},
                {"$set": {
                    "stock_change_pct": m["stock_change_pct"],
                    "volume_custom": m["volume_custom"]
                }}
            )
        st.success("Metrics updated in MongoDB.")

# --- Fetch & Preview Only ---
if st.sidebar.button(" Preview Metrics (No Save)"):
    with st.spinner("Fetching metrics..."):
        metrics = asyncio.run(fetch_metrics_async(start_date.isoformat(), end_date.isoformat(), tickers))
        ticker_to_metrics = {m["ticker"]: m for m in metrics}
        for r in results:
            r.update(ticker_to_metrics.get(r["ticker"], {}))
        st.success("Preview updated.")

# --- Clean and Display Table ---
for r in results:
    r.pop("_id", None)

st.title("📊 Ticker Dashboard")
st.write(f"Showing top {limit} tickers sorted by `{sort_field}`")
st.dataframe(results, use_container_width=True)
