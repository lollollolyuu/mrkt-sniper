import asyncio
import contextlib
import os
import sqlite3
import time
import json
from urllib.parse import parse_qs
from pathlib import Path
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from refresh_market import fetch_market
from see_guest import run_guest_feed

ROOT = Path(__file__).parent
market_slots = asyncio.Semaphore(3)
market_cache = {"token": "", "snapshot": None, "expires": 0.0}
ACCOUNT_DB = ROOT / "mrkt_accounts.sqlite3"
DEFAULT_COLLECTIONS = (
    "Plush Pepe", "Astral Shard", "Precious Peach",
    "Toy Bear", "Magic Potion", "Diamond Ring",
)


def guest_snapshot(events):
    """Convert the live guest feed into the UI's collection snapshot shape."""
    grouped = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        market = event.get("market")
        if isinstance(market, str) and "mrkt" not in market.lower():
            continue
        gift = event.get("gift") if isinstance(event.get("gift"), dict) else {}
        name = gift.get("title") or gift.get("collection") or gift.get("slug")
        if not isinstance(name, str) or not name.strip():
            continue
        bucket = grouped.setdefault(name, {"name": name, "listings": [], "sales": []})
        price = event.get("price") if isinstance(event.get("price"), dict) else {}
        amount = price.get("amount")
        if not isinstance(amount, int) or amount <= 0:
            continue
        currency = price.get("currency")
        # The existing UI formats amounts as nanoTON; retain Stars as-is and
        # convert gram values to nanoTON-compatible integers.
        if currency == "gram":
            amount = amount
        row = {"title": gift.get("name") or name, "amount": amount,
               "collection": gift.get("collection") or gift.get("collectionName") or name}
        for output, keys in {
            "model": ("model", "modelName", "model_name"),
            "backdrop": ("backdrop", "backdropName", "background", "backgroundName"),
            "pattern": ("pattern", "patternName", "pattern_name"),
        }.items():
            value = next((gift.get(key) for key in keys if isinstance(gift.get(key), str)), None)
            if value:
                row[output] = value
        row["status"] = "Продан" if str(event.get("event") or "").lower() in {"sale", "sold"} else "Выставлен"
        if isinstance(gift.get("number"), (int, str)):
            row["number"] = gift["number"]
        image = gift.get("image") or gift.get("image_url") or gift.get("thumbnail")
        if isinstance(image, str) and image.startswith(("https://", "http://")):
            row["image"] = image
        at = event.get("at") or event.get("date")
        if isinstance(at, str):
            row["date"] = at
        gift_id = gift.get("id")
        gift_link = next((gift.get(key) for key in
                          ("link", "url", "market_url", "marketUrl", "deep_link", "deepLink")
                          if isinstance(gift.get(key), str) and gift.get(key).startswith(("https://", "http://"))), None)
        if gift_link:
            row["link"] = gift_link
        elif isinstance(gift_id, (str, int)):
            row["link"] = f"https://t.me/mrkt/app?startapp={gift_id}"
        kind = str(event.get("event") or event.get("type") or "").lower()
        if kind in {"sale", "sold"}:
            bucket["sales"].append(row)
        elif kind not in {"delisting", "delisted"}:
            bucket["listings"].append(row)
    # Keep the complete catalog in the selector even when the public feed has
    # no recent event for one of the collections.
    collections = [grouped.pop(name, {"name": name, "listings": [], "sales": []})
                   for name in DEFAULT_COLLECTIONS]
    collections.extend(grouped.values())
    return {"source": "see.tg", "updatedAt": datetime.now(timezone.utc).isoformat(),
            "collections": collections, "events": list(events)}


def init_account_db():
    with sqlite3.connect(ACCOUNT_DB) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS mrkt_accounts (
            telegram_user_id INTEGER PRIMARY KEY,
            mrkt_token TEXT NOT NULL,
            connected_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")


def save_account(user_id, token):
    init_account_db()
    with sqlite3.connect(ACCOUNT_DB) as db:
        now = int(time.time())
        db.execute("""INSERT INTO mrkt_accounts
            (telegram_user_id, mrkt_token, connected_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(telegram_user_id) DO UPDATE SET
              mrkt_token=excluded.mrkt_token, updated_at=excluded.updated_at""",
            (user_id, token, now, now))


async def market(request):
    headers = {"Cache-Control": "no-store"}
    if request.content_type != "application/json":
        return web.Response(status=415, headers=headers)
    try:
        body = await request.json()
        token = body.get("token") if isinstance(body, dict) else None
    except ValueError:
        body = {}
        token = None
    if not isinstance(body, dict):
        return web.Response(status=400, headers=headers)
    user_id = body.get("telegram_user_id")
    init_data = body.get("telegram_init_data")
    if isinstance(init_data, str) and init_data:
        try:
            parsed = parse_qs(init_data)
            user = json.loads(parsed["user"][0])
            if isinstance(user.get("id"), int):
                user_id = user["id"]
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return web.json_response({"error": "Некорректные данные Telegram Mini App."}, status=400, headers=headers)
    if not isinstance(user_id, int) or user_id <= 0:
        return web.Response(status=400, headers=headers)
    if not isinstance(token, str):
        token = ""
    # Read-only shared catalog mode: keep the MRKT token only on the server.
    shared_token = os.getenv("MRKT_TOKEN", "").strip()
    if shared_token:
        token = shared_token
    if token:
        if not 1 <= len(token) <= 4096 or not all(33 <= ord(char) <= 126 for char in token):
            return web.Response(status=400, headers=headers)
    else:
        with sqlite3.connect(ACCOUNT_DB) as db:
            row = db.execute("SELECT mrkt_token FROM mrkt_accounts WHERE telegram_user_id = ?", (user_id,)).fetchone()
        token = row[0] if row else ""
    if not token:
        # Guest mode: no MRKT token is required. The public see.tg feed is
        # exposed separately and may be empty while that upstream is offline.
        events = list(request.app.get("see_events", ()))
        snapshot = guest_snapshot(events)
        if not snapshot["collections"]:
            snapshot["error"] = "Публичная лента see.tg пока недоступна или не содержит событий."
        return web.json_response(snapshot, status=200, headers=headers)
    # Reuse a fresh server-side snapshot so simultaneous users do not all
    # wait for the same MRKT requests.  A short TTL keeps prices current.
    now = time.monotonic()
    if (market_cache["token"] == token and market_cache["snapshot"] is not None
            and now < market_cache["expires"]):
        return web.json_response(market_cache["snapshot"], headers=headers)
    if body.get("token") and not shared_token:
        save_account(user_id, token)
    if market_slots.locked():
        return web.Response(status=503, headers=headers)
    async with market_slots:
        try:
            async with asyncio.timeout(15):
                snapshot = await fetch_market(token)
                market_cache.update(token=token, snapshot=snapshot, expires=time.monotonic() + 10)
                return web.json_response(snapshot, headers=headers)
        except ValueError as error:
            if str(error).endswith(("HTTP 401.", "HTTP 403.")):
                return web.Response(status=401, headers=headers)
        except (TimeoutError, aiohttp.ClientError, KeyError, TypeError):
            pass
    return web.Response(status=502, headers=headers)


async def index(request):
    return web.FileResponse(ROOT / "index.html")


async def snapshot(request):
    path = ROOT / "market-data.json"
    if not path.is_file():
        raise web.HTTPNotFound()
    return web.FileResponse(path)


async def see_events(request):
    return web.json_response({
        "source": "see.tg",
        "events": list(request.app.get("see_events", ())),
    }, headers={"Cache-Control": "no-store"})


app = web.Application(client_max_size=16 * 1024)
app.router.add_get("/", index)
app.router.add_get("/market-data.json", snapshot)
app.router.add_get("/api/see-events", see_events)
app.router.add_post("/api/market", market)
init_account_db()


async def start_bot(app):
    """Run the Telegram polling loop alongside the free web service."""
    try:
        from bot import main as bot_main
    except Exception as error:
        print(f"Не удалось загрузить Telegram-бота: {error}")
        return
    # The Mini App already owns Render's HTTP port.  The bot's standalone
    # health server must be disabled when polling is embedded in this process.
    task = asyncio.create_task(bot_main(start_health=False), name="telegram-bot")

    def report_bot_failure(done_task):
        # Never let a failed polling task look like a healthy web-only deploy.
        try:
            error = done_task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            print(f"Telegram polling stopped: {type(error).__name__}: {error}", flush=True)

    task.add_done_callback(report_bot_failure)
    app["bot_task"] = task


async def stop_bot(app):
    task = app.get("bot_task")
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app.on_startup.append(start_bot)
app.on_cleanup.append(stop_bot)


async def start_guest_feed(app):
    app["see_task"] = asyncio.create_task(run_guest_feed(app), name="see-tg-guest-feed")


async def stop_guest_feed(app):
    task = app.get("see_task")
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app.on_startup.append(start_guest_feed)
app.on_cleanup.append(stop_guest_feed)

if __name__ == "__main__":
    web.run_app(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")), access_log=None)

