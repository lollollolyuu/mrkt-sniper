import asyncio
import os
import sqlite3
import time
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
    if not isinstance(token, str) or not 1 <= len(token) <= 4096 or not all(
        33 <= ord(char) <= 126 for char in token
    ):
        return web.Response(status=400, headers=headers)
    user_id = body.get("telegram_user_id")
    if not isinstance(user_id, int) or user_id <= 0:
        return web.Response(status=400, headers=headers)
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

if __name__ == "__main__":
    web.run_app(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")), access_log=None)

