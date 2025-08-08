import os
from typing import Optional
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Connect to Mongo
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["marble"]
collection = db["tickers"]

def get_filtered_sorted_tickers(
    min_market_cap: float = 500_000_000,
    sort_field: Optional[str] = None,
    sort_order: int = -1,
    limit: int = 50
) -> list[dict]:
    """
    Fetch tickers filtered by market cap and sorted by another field.
    
    Args:
        min_market_cap: Minimum market cap to include.
        sort_field: Field to sort by (e.g. "pe_ratio", "eps").
        sort_order: 1 = ascending, -1 = descending.
        limit: Number of results to return.

    Returns:
        List of ticker dicts.
    """
    query = { "market_cap": { "$gte": min_market_cap } }

    if sort_field:
        cursor = collection.find(query).sort(sort_field, sort_order).limit(limit)
    else:
        cursor = collection.find(query).limit(limit)

    return list(cursor)
