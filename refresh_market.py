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
    result = {"title": f"{name} #{number}" if type(number) is int else name, "amount": amount,
              "collection": gift.get("collectionName") or name}
    gift_id = gift.get("id")
    if gift_id is not None and re.fullmatch(r"[A-Za-z0-9_-]+", str(gift_id)):
        result["marketLink"] = f"https://t.me/mrkt/app?startapp={gift_id}"
    if isinstance(number, (int, str)):
        result["number"] = number
    # MRKT has used several names for these fields over time.  Keep the
    # normalised metadata in the snapshot so the Mini App does not need to
    # know the exact API response shape.
    for output, keys in {
        "model": ("modelName", "model", "model_name"),
        "backdrop": ("backdropName", "backgroundName", "backdrop", "background", "backdrop_name"),
        "pattern": ("patternName", "pattern", "pattern_name"),
    }.items():
        value = next((gift.get(key) for key in keys if isinstance(gift.get(key), str)), None)
        if value:
            result[output] = value
    image = next((gift.get(key) for key in
                  ("image", "imageUrl", "image_url", "imageURL", "thumbnail", "preview", "previewUrl")
                  if isinstance(gift.get(key), str)), None)
    if image and image.startswith(("https://", "http://")):
        result["image"] = image
    gift_link = next((gift.get(key) for key in
                      ("link", "url", "market_url", "marketUrl", "deep_link", "deepLink",
                       "gift_link", "giftLink", "marketLink", "market_link", "listingUrl",
                       "listing_url", "listingLink", "listing_link", "telegramUrl",
                       "telegram_url", "nftLink", "nft_link")
                      if isinstance(gift.get(key), str) and gift.get(key).startswith(("https://", "http://"))
                      and (not gift.get(key).startswith("https://t.me/nft/")
                           or re.fullmatch(r"https://t\.me/nft/[A-Za-z0-9_]+-[1-9][0-9]*", gift.get(key)))), None)
    nft_slug = next((gift.get(key) for key in
                     ("slug", "username", "nft", "nftSlug", "nft_slug")
                     if isinstance(gift.get(key), str) and gift.get(key).strip()), None)
    if gift_link and gift_link.startswith("https://t.me/nft/"):
        gift_link = gift_link if re.fullmatch(
            r"https://t\.me/nft/[A-Za-z0-9_]+-[1-9][0-9]*", gift_link) else None
    if gift_link:
        result["link"] = gift_link
    else:
        slug = nft_slug.strip().lstrip("/") if nft_slug else ""
        number_value = gift.get("number", gift.get("num"))
        if not re.fullmatch(r"[A-Za-z0-9_]+-[1-9][0-9]*", slug) and isinstance(number_value, int) and number_value > 0 and slug:
            slug = f"{re.sub(r'[^A-Za-z0-9_]', '', slug)}-{number_value}"
        if re.fullmatch(r"[A-Za-z0-9_]+-[1-9][0-9]*", slug):
            result["link"] = f"https://t.me/nft/{slug}"
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
