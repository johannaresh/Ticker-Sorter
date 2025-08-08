import csv
import os
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
MAX_CONCURRENT = 75  # limit concurrent API calls

def load_tickers_from_csv(filename="qualified_tickers.csv") -> list[str]:
    with open(filename, newline="") as f:
        return [row[0] for i, row in enumerate(csv.reader(f)) if i >= 0]

async def get_close_price(client: httpx.AsyncClient, ticker: str, date: str):
    try:
        url = f"{BASE_URL}/v1/open-close/{ticker}/{date}"
        res = await client.get(url, params={"adjusted": "true", "apiKey": API_KEY})
        res.raise_for_status()
        return res.json().get("close")
    except:
        return None

async def get_volume_sum(client: httpx.AsyncClient, ticker: str, start_date: str, end_date: str):
    try:
        url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        res = await client.get(url, params={
            "adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": API_KEY
        })
        res.raise_for_status()
        data = res.json().get("results", [])
        return sum(d.get("v", 0) for d in data)
    except:
        return None

async def fetch_stock_metrics(client, semaphore, ticker, start_date, end_date):
    async with semaphore:
        try:
            start_price, end_price = await asyncio.gather(
                get_close_price(client, ticker, start_date),
                get_close_price(client, ticker, end_date)
            )
            volume = await get_volume_sum(client, ticker, start_date, end_date)

            if not start_price or not end_price:
                raise Exception("Missing price")

            change_pct = round(((end_price - start_price) / start_price) * 100, 2)
            return {
                "ticker": ticker,
                "stock_change_pct": change_pct,
                "volume_custom": volume
            }
        except:
            return {
                "ticker": ticker,
                "stock_change_pct": None,
                "volume_custom": None
            }

async def main(start_date: str, end_date: str, tickers: list[str]):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with httpx.AsyncClient(headers=HEADERS, timeout=15.0) as client:
        tasks = [
            fetch_stock_metrics(client, semaphore, ticker, start_date, end_date)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return results

if __name__ == "__main__":
    results = asyncio.run(main("2025-03-03", "2025-03-07"))
