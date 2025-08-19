# polygon_client.py

import os
import asyncio
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import certifi
import httpx
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# ── Env ─────────────────────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

if not API_KEY or not MONGO_URI:
    raise ValueError("Missing required environment variables: POLYGON_API_KEY and/or MONGO_URI")

BASE_URL = "https://api.polygon.io"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# ── Mongo (async) ───────────────────────────────────────────────────────────────
mongo = AsyncIOMotorClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
db = mongo["marble"]
collection = db["tickers"]  # expects docs with at least {"ticker": "AAPL", ...}

# ── Helpers ─────────────────────────────────────────────────────────────────────
def last_marketish_day_utc(days_back: int = 1) -> str:
    """
    Get a recent date string (YYYY-MM-DD) for /v1/open-close that is likely to exist.
    We step back up to 5 days to skip weekends/holidays.
    """
    d: date = datetime.utcnow().date() - timedelta(days=days_back)
    for _ in range(5):  # try a few days back if needed
        # Polygon's open-close often missing for today until after close;
        # start at 2 days back to be safer, then walk back if needed.
        if d.weekday() < 5:  # Mon-Fri
            return d.strftime("%Y-%m-%d")
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")

async def ensure_indexes():
    await collection.create_index("ticker", unique=True)
    await collection.create_index("metrics_updated_at")

async def load_tickers_from_mongo(limit: Optional[int] = None) -> List[str]:
    """
    Read the working universe from Mongo. Assumes docs look like:
      { _id, ticker: "AAPL", ... }
    """
    cursor = collection.find({"ticker": {"$exists": True}}, {"ticker": 1})
    if limit:
        cursor = cursor.limit(limit)
    symbols: List[str] = []
    async for doc in cursor:
        t = doc.get("ticker")
        if isinstance(t, str) and t:
            symbols.append(t.upper())
    # De-dup while preserving order
    seen = set()
    out: List[str] = []
    for t in symbols:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# ── Polygon fetch ───────────────────────────────────────────────────────────────
async def fetch_metrics(http: httpx.AsyncClient, ticker: str) -> Optional[Dict[str, Any]]:
    """
    Fetch non-timeframe dependent metrics:
      - industry (SIC description)
      - EPS (TTM computed from last 4 quarterly reports' diluted EPS)
      - PE ratio (close / EPS_TTM)
      - net_income_quarterly, revenue_quarterly, net_profit_margin (latest quarter)
    """
    out: Dict[str, Any] = {
        "ticker": ticker,
        "metrics_updated_at": datetime.utcnow(),
    }

    try:
        # 1) Reference profile (industry)
        ref_url = f"{BASE_URL}/v3/reference/tickers/{ticker}"
        ref_resp = await http.get(ref_url)
        ref_resp.raise_for_status()
        ref = ref_resp.json().get("results", {}) or {}
        out["industry"] = ref.get("sic_description")

        # 2) Financials (EPS TTM from last 4 quarters, latest NI/Revenue/Profit Margin)
        fin_url = f"{BASE_URL}/vX/reference/financials"
        fin_params = {
            "ticker": ticker,
            "limit": 4,             # last 4 quarters
            "timeframe": "quarterly",
            "sort": "filing_date",
            "order": "desc",
            "apiKey": API_KEY,      # required on vX endpoints via params
        }
        fin_resp = await http.get(fin_url, params=fin_params)
        fin_resp.raise_for_status()
        reports = fin_resp.json().get("results", []) or []

        eps_list: List[float] = []
        net_income = None
        revenue = None

        if reports:
            latest_fin = (reports[0].get("financials") or {})
            income_stmt = latest_fin.get("income_statement") or {}
            ni_field = income_stmt.get("net_income_loss") or {}
            rev_field = income_stmt.get("revenues") or {}
            net_income = ni_field.get("value")
            revenue = rev_field.get("value")

            # Sum diluted EPS across up to 4 quarters
            for r in reports:
                fin = r.get("financials") or {}
                inc = fin.get("income_statement") or {}
                eps_val = (inc.get("diluted_earnings_per_share") or {}).get("value")
                if isinstance(eps_val, (int, float)):
                    eps_list.append(float(eps_val))

        eps_ttm = round(sum(eps_list), 4) if eps_list else None
        out["eps"] = eps_ttm
        out["net_income_quarterly"] = net_income
        out["revenue_quarterly"] = revenue
        out["net_profit_margin"] = (
            round(net_income / revenue, 4) if isinstance(net_income, (int, float)) and isinstance(revenue, (int, float)) and revenue else None
        )

        # 3) Close price (recent past day, adjusted)
        #    Use a safe "recent business day" guess, then compute PE if EPS_TTM present
        ref_day = last_marketish_day_utc(days_back=2)  # a bit safer than "yesterday"
        oc_url = f"{BASE_URL}/v1/open-close/{ticker}/{ref_day}"
        oc_resp = await http.get(oc_url, params={"adjusted": "true", "apiKey": API_KEY})
        # open-close may 404 for holidays/illiquid tickers; handle gracefully
        close_price = None
        if oc_resp.status_code == 200:
            close_price = oc_resp.json().get("close")

        out["close_price"] = close_price
        out["pe_ratio"] = (round(close_price / eps_ttm, 2) if (isinstance(close_price, (int, float)) and isinstance(eps_ttm, (int, float)) and eps_ttm) else None)

        return out

    except Exception as e:
        print(f"❌ {ticker} failed: {e}")
        return None

# ── Orchestration ───────────────────────────────────────────────────────────────
async def process_all_tickers(max_concurrent: int = 75, limit: Optional[int] = None):
    await ensure_indexes()
    symbols = await load_tickers_from_mongo(limit=limit)
    if not symbols:
        print("No tickers found in MongoDB 'marble.tickers'.")
        return

    semaphore = asyncio.Semaphore(max_concurrent)
    results: List[asyncio.Task] = []

    async with httpx.AsyncClient(headers=HEADERS, timeout=20.0) as http:
        async def worker(sym: str):
            async with semaphore:
                data = await fetch_metrics(http, sym)
                if data:
                    # Upsert without touching market cap (explicitly excluded)
                    await collection.update_one(
                        {"ticker": sym},
                        {"$set": data, "$setOnInsert": {"created_at": datetime.utcnow()}},
                        upsert=True,
                    )
                    print(f"✅ Saved {sym}")

        for s in symbols:
            results.append(asyncio.create_task(worker(s)))

        await asyncio.gather(*results)

# ── Entrypoint ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(process_all_tickers())
