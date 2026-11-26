import asyncio
import contextlib
import os
import sqlite3
import time
import json
from urllib.parse import parse_qs
from pathlib import Path

import aiohttp
from aiohttp import web

from refresh_market import fetch_market

ROOT = Path(__file__).parent
market_slots = asyncio.Semaphore(3)
ACCOUNT_DB = ROOT / "mrkt_accounts.sqlite3"


def save_account(user_id, token):
    with sqlite3.connect(ACCOUNT_DB) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS mrkt_accounts (
            telegram_user_id INTEGER PRIMARY KEY,
            mrkt_token TEXT NOT NULL,
            connected_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
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
        token = None
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
    if not token and isinstance(init_data, str) and init_data:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post("https://api.tgmrkt.io/api/v1/auth", json={"data": init_data}) as response:
                    auth = await response.json()
            token = auth.get("token") if isinstance(auth, dict) else None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
            token = None
        if not isinstance(token, str) or not token:
            return web.json_response({"error": "MRKT не подтвердил Telegram-сессию."}, status=401, headers=headers)
        save_account(user_id, token)
    if not token:
            return web.json_response({"error": "Сначала один раз введите личный токен MRKT."}, status=400, headers=headers)
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


app = web.Application(client_max_size=16 * 1024)
app.router.add_get("/", index)
app.router.add_get("/market-data.json", snapshot)
app.router.add_post("/api/market", market)


async def start_bot(app):
    """Run the Telegram polling loop alongside the free web service."""
    try:
        from bot import main as bot_main
    except Exception as error:
        print(f"Не удалось загрузить Telegram-бота: {error}")
        return
    app["bot_task"] = asyncio.create_task(bot_main(), name="telegram-bot")


async def stop_bot(app):
    task = app.get("bot_task")
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app.on_startup.append(start_bot)
app.on_cleanup.append(stop_bot)

if __name__ == "__main__":
    web.run_app(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")), access_log=None)

