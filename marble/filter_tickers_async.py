import os
import time
import asyncio
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import httpx
from dotenv import load_dotenv
import certifi
from motor.motor_asyncio import AsyncIOMotorClient
import datetime as dt

# Load environment variables
load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

if not API_KEY or not MONGO_URI:
    raise ValueError("Missing required environment variables: POLYGON_API_KEY and/or MONGO_URI")

BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Mongo client
mongo = AsyncIOMotorClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where()
)
db = mongo["marble"]
col = db["tickers"]

MIN_MCAP = 500_000_000  # $500mm

def add_api_key_to_url(url: str, api_key: str) -> str:
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
        "apiKey": API_KEY,
    }

    while url:
        if url.endswith("tickers"):
            res = httpx.get(url, params=params, timeout=10.0)
        else:
            full_url = add_api_key_to_url(url, API_KEY)
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
        res = await client.get(url, headers=HEADERS)
        res.raise_for_status()
        data = res.json()
        cap = data.get("results", {}).get("market_cap")
        return ticker, cap
    except Exception as e:
        print(f"⚠️ {ticker}: {e}")
        return ticker, None

async def filter_by_market_cap_parallel(
    tickers: list[dict], threshold: float = MIN_MCAP, max_concurrent: int = 50
) -> dict[str, float]:
    """Parallel fetches market cap and filters."""
    semaphore = asyncio.Semaphore(max_concurrent)
    passed: dict[str, float] = {}

    async with httpx.AsyncClient(timeout=10.0) as client:
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

async def ensure_indexes():
    await col.create_index("ticker", unique=True)
    await col.create_index("universe_updated_at")

async def sync_to_mongo(qualified: dict[str, float]):
    """Upsert qualified tickers, keep existing, remove unqualified."""
    await ensure_indexes()
    now = dt.datetime.now(dt.UTC)

    # Upsert qualified
    upserts = []
    for ticker, mcap in qualified.items():
        upserts.append(
            col.update_one(
                {"ticker": ticker},
                {
                    "$set": {
                        "ticker": ticker,
                        "market_cap": mcap,
                        "universe_updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        )
    if upserts:
        await asyncio.gather(*upserts)

    # Remove unqualified
    await col.delete_many({"ticker": {"$nin": list(qualified.keys())}})
    print(f"🗄️ Mongo sync complete. Universe size: {len(qualified)}")

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

    asyncio.run(sync_to_mongo(qualified))

if __name__ == "__main__":
    main()
