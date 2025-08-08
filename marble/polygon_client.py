import csv
import os
import asyncio
from datetime import datetime
import httpx
from dotenv import load_dotenv
from pymongo import MongoClient

# Load env vars
load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Connect to Mongo
client = MongoClient(MONGO_URI)
db = client["marble"]
collection = db["tickers"]

# Read tickers from CSV
def load_tickers_from_csv(filename="qualified_tickers.csv") -> list[str]:
    with open(filename, newline="") as f:
        return [row[0] for i, row in enumerate(csv.reader(f)) if i > 0]

# Fetch metrics per ticker
from datetime import datetime, timedelta

async def fetch_metrics(client: httpx.AsyncClient, ticker: str) -> dict | None:
    result = {
        "ticker": ticker,
        "timestamp": datetime.utcnow()
    }
    try:
        # Reference profile (industry, market cap)
        ref_url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
        ref_resp = await client.get(ref_url)
        ref = ref_resp.json().get("results", {})
        result["industry"] = ref.get("sic_description")
        result["market_cap"] = ref.get("market_cap")

        # Financials (Net Income, Revenue, EPS)
        fin_url = f"{BASE_URL}/vX/reference/financials"
        fin_params = {
            "ticker": ticker,
            "limit": 4,  # fetch 4 quarters for TTM
            "timeframe": "quarterly",
            "sort": "filing_date",
            "order": "desc",
            "apiKey": API_KEY
        }
        fin_resp = await client.get(fin_url, params=fin_params)
        reports = fin_resp.json().get("results", [])

        eps_list = []
        net_income = None
        revenue = None

        if reports:
            latest = reports[0].get("financials", {})
            income_stmt = latest.get("income_statement", {})
            net_income = income_stmt.get("net_income_loss", {}).get("value")
            revenue = income_stmt.get("revenues", {}).get("value")

            # Sum diluted EPS over 4 reports
            for report in reports:
                eps = (
                    report.get("financials", {})
                    .get("income_statement", {})
                    .get("diluted_earnings_per_share", {})
                    .get("value")
                )
                if eps is not None:
                    eps_list.append(eps)

        eps_ttm = round(sum(eps_list), 4) if eps_list else None
        result["net_income_quarterly"] = net_income
        result["revenue_quarterly"] = revenue
        result["eps"] = eps_ttm
        result["net_profit_margin"] = round(net_income / revenue, 4) if net_income and revenue else None

        # Close price from yesterday CHANGE LATER 
        yesterday = (datetime.utcnow().date() - timedelta(days=4)).strftime("%Y-%m-%d")
        close_url = f"{BASE_URL}/v1/open-close/{ticker}/{yesterday}"
        close_resp = await client.get(close_url, params={"adjusted": "true", "apiKey": API_KEY})
        close_price = close_resp.json().get("close")

        result["pe_ratio"] = round(close_price / eps_ttm, 2) if eps_ttm and close_price else None

        return result

    except Exception as e:
        print(f"❌ {ticker} failed: {e}")
        return None


# Main fetcher loop
async def process_all_tickers(ticker_list: list[str], max_concurrent=75):
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = []

    async with httpx.AsyncClient(headers=HEADERS, timeout=15.0) as client:
        async def worker(ticker: str):
            async with semaphore:
                data = await fetch_metrics(client, ticker)
                if data:
                    collection.update_one({"ticker": data["ticker"]}, {"$set": data}, upsert=True)
                    print(f"✅ Saved {ticker}")

        for ticker in ticker_list:
            tasks.append(worker(ticker))

        await asyncio.gather(*tasks)

# Entrypoint
tickers = load_tickers_from_csv()
asyncio.run(process_all_tickers(tickers))
