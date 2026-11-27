"""Refresh the public market-data.json snapshot using the project's private .env."""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import re

import aiohttp
from dotenv import dotenv_values

BASE = "https://api.tgmrkt.io/api/v1"
COLLECTIONS = ("Plush Pepe", "Astral Shard", "Precious Peach", "Toy Bear", "Magic Potion", "Diamond Ring")


def row(gift, amount, date=None):
    if type(amount) is not int or amount <= 0:
        return None
    name = gift.get("collectionName") or gift.get("title") or gift.get("name")
    if not isinstance(name, str):
        return None
    number = gift.get("number")
    result = {"title": f"{name} #{number}" if type(number) is int else name, "amount": amount}
    gift_id = gift.get("id")
    if isinstance(gift_id, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", gift_id):
        result["link"] = f"https://t.me/mrkt/app?startapp={gift_id}"
    if date is not None:
        try:
            timestamp = datetime.fromisoformat(date)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            result["date"] = timestamp.isoformat()
        except (ValueError, TypeError):
            return None
    return result


async def fetch_market(token):
    collections = []
    headers = {"Authorization": token, "Referer": "https://cdn.tgmrkt.io/"}
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=12),
                                     connector=aiohttp.TCPConnector(limit=12)) as session:
        async def fetch(endpoint, body, key):
            async with session.post(f"{BASE}/{endpoint}", json=body, allow_redirects=False) as response:
                if response.status != 200:
                    raise ValueError(f"MRKT {endpoint}: HTTP {response.status}.")
                return (await response.json())[key]

        async def fetch_collection(name):
            filters = {"collectionNames": [name], "modelNames": [], "backdropNames": [], "count": 20}
            gifts, feed = await asyncio.gather(
                fetch("gifts/saling", dict(filters, cursor="", ordering="Price", lowToHigh=True), "gifts"),
                fetch("feed", dict(filters, cursor=None, ordering="Latest", lowToHigh=False, type=["sale"]), "items"),
            )
            listings = [item for gift in gifts if (item := row(gift, gift.get("salePrice")))]
            sales = [item for sale in feed if isinstance(sale.get("gift"), dict) and sale.get("date")
                     and (item := row(sale["gift"], sale.get("amount"), sale["date"]))]
            if (gifts and not listings) or (feed and not sales):
                raise ValueError("Unexpected MRKT data format. No snapshot was written.")
            return {"name": name, "listings": listings, "sales": sales}

        # The six collections are independent; fetching them together cuts
        # first-load latency from roughly 12 round trips to the slowest pair.
        collections = await asyncio.gather(*(fetch_collection(name) for name in COLLECTIONS))
    return {"updatedAt": datetime.now(timezone.utc).isoformat(), "collections": collections}


async def refresh(env_path):
    config = dotenv_values(env_path)
    token = os.getenv("MRKT_TOKEN") or config.get("MRKT_TOKEN") or config.get("MRKT_ACCESS_TOKEN")
    if not token:
        raise SystemExit("MRKT_TOKEN is missing. No snapshot was written.")
    data = await fetch_market(token)
    path = Path(__file__).with_name("market-data.json")
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


if __name__ == "__main__":
    asyncio.run(refresh(Path(sys.argv[1])))
