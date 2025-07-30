import os
import time
import csv
import asyncio
import httpx
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# Load env variables
load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def add_api_key_to_url(url: str, api_key: str) -> str:
    """Ensure API key is included in paginated URLs."""
    parts = list(urlparse(url))
    query = parse_qs(parts[4])
    query["apiKey"] = [api_key]
    parts[4] = urlencode(query, doseq=True)
    return urlunparse(parts)


def get_all_tickers(exchange: str) -> list[dict]:
    """Fetch all active stock tickers from a given exchange using pagination."""
    all_tickers = []
    url = f"{BASE_URL}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "exchange": exchange,
        "active": "true",
        "limit": 1000,
        "apiKey": API_KEY
    }

    while url:
        if url.endswith("tickers"):
            print(f"🔄 First page: {url}")
            res = httpx.get(url, params=params, timeout=10.0)
        else:
            full_url = add_api_key_to_url(url, API_KEY)
            print(f"🔄 Next page: {full_url}")
            res = httpx.get(full_url, timeout=10.0)

        res.raise_for_status()
        data = res.json()
        all_tickers.extend(data.get("results", []))
        url = data.get("next_url")
        time.sleep(0.1)

    return all_tickers


async def get_market_cap(client: httpx.AsyncClient, ticker: str) -> tuple[str, float | None]:
    url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
    try:
        res = await client.get(url)
        res.raise_for_status()
        data = res.json()
        cap = data.get("results", {}).get("market_cap")
        return ticker, cap
    except Exception as e:
        print(f"⚠️ {ticker}: {e}")
        return ticker, None


async def filter_by_market_cap_parallel(tickers: list[dict], threshold: float = 500_000_000, max_concurrent=50) -> dict:
    """Parallel fetches market cap and filters."""
    semaphore = asyncio.Semaphore(max_concurrent)
    passed = {}

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:
        async def fetch(t):
            async with semaphore:
                symbol, cap = await get_market_cap(client, t["ticker"])
                if cap and cap >= threshold:
                    passed[symbol] = cap
                    print(f"✅ {symbol}: ${cap:,.2f}")
                else:
                    print(f"❌ {symbol}: skipped (cap={cap})")

        await asyncio.gather(*(fetch(t) for t in tickers))

    return passed


def export_to_csv(data: dict, filename: str = "qualified_tickers.csv"):
    """Save to CSV for downstream metric enrichment."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Ticker", "Market Cap"])
        for ticker, cap in data.items():
            writer.writerow([ticker, cap])


def main():
    print("🔍 Fetching tickers from NASDAQ and NYSE...")
    nasdaq = get_all_tickers("XNAS")
    nyse = get_all_tickers("XNYS")
    all_tickers = nasdaq + nyse
    print(f"📦 Fetched: {len(all_tickers)} tickers")

    filtered = [t for t in all_tickers if t.get("type") == "CS" and t.get("active") is True]
    print(f"✅ Active common stocks: {len(filtered)}")

    print("\n🔎 Filtering by market cap ≥ $500M...\n")
    qualified = asyncio.run(filter_by_market_cap_parallel(filtered))

    export_to_csv(qualified)
    print(f"\n📁 Exported {len(qualified)} tickers to 'qualified_tickers.csv'")


if __name__ == "__main__":
    main()
