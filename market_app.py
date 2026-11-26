import asyncio
import contextlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs

import aiohttp
from aiohttp import web

from refresh_market import fetch_market
from see_guest import run_guest_feed

ROOT = Path(__file__).parent
market_slots = asyncio.Semaphore(3)
ACCOUNT_DB = ROOT / "mrkt_accounts.sqlite3"


def guest_snapshot(events):
    grouped = {}
    for event in events:
        if not isinstance(event, dict):
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
        row = {"title": gift.get("name") or name, "amount": amount}
        at = event.get("at") or event.get("date")
        if isinstance(at, str):
            row["date"] = at
        gift_id = gift.get("id")
        if isinstance(gift_id, (str, int)):
            row["link"] = f"https://t.me/mrkt/app?startapp={gift_id}"
        kind = str(event.get("event") or event.get("type") or "").lower()
        if kind in {"sale", "sold"}:
            bucket["sales"].append(row)
        elif kind not in {"delisting", "delisted"}:
            bucket["listings"].append(row)
    return {"source": "see.tg", "updatedAt": datetime.now(timezone.utc).isoformat(),
            "collections": list(grouped.values()), "events": list(events)}


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
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        return web.Response(status=400, headers=headers)
    token = body.get("token") if isinstance(body.get("token"), str) else ""
    user_id = body.get("telegram_user_id")
    init_data = body.get("telegram_init_data")
    if isinstance(init_data, str) and init_data:
        try:
            user = json.loads(parse_qs(init_data)["user"][0])
            if isinstance(user.get("id"), int):
                user_id = user["id"]
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return web.json_response({"error": "Некорректные данные Telegram Mini App."}, status=400, headers=headers)
    if not isinstance(user_id, int) or user_id <= 0:
        return web.Response(status=400, headers=headers)
    shared_token = os.getenv("MRKT_TOKEN", "").strip()
    if shared_token:
        token = shared_token
    if not token:
        events = list(request.app.get("see_events", ()))
        snapshot = guest_snapshot(events)
        if not snapshot["collections"]:
            snapshot["error"] = "Публичная лента see.tg пока недоступна или не содержит событий."
        return web.json_response(snapshot, headers=headers)
    if not 1 <= len(token) <= 4096 or not all(33 <= ord(char) <= 126 for char in token):
        return web.Response(status=400, headers=headers)
    if body.get("token") and not shared_token:
        save_account(user_id, token)
    if market_slots.locked():
        return web.Response(status=503, headers=headers)
    async with market_slots:
        try:
            async with asyncio.timeout(60):
                return web.json_response(await fetch_market(token), headers=headers)
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
    return web.json_response({"source": "see.tg", "events": list(request.app.get("see_events", ()))},
                             headers={"Cache-Control": "no-store"})


app = web.Application(client_max_size=16 * 1024)
app.router.add_get("/", index)
app.router.add_get("/market-data.json", snapshot)
app.router.add_get("/api/see-events", see_events)
app.router.add_post("/api/market", market)
init_account_db()


async def start_bot(app):
    try:
        from bot import main as bot_main
    except Exception as error:
        print(f"Не удалось загрузить Telegram-бота: {error}")
        return
    task = asyncio.create_task(bot_main(), name="telegram-bot")
    app["bot_task"] = task


async def stop_bot(app):
    task = app.get("bot_task")
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def start_guest_feed(app):
    app["see_task"] = asyncio.create_task(run_guest_feed(app), name="see-tg-guest-feed")


async def stop_guest_feed(app):
    task = app.get("see_task")
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app.on_startup.append(start_bot)
app.on_cleanup.append(stop_bot)
app.on_startup.append(start_guest_feed)
app.on_cleanup.append(stop_guest_feed)

if __name__ == "__main__":
    web.run_app(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")), access_log=None)
