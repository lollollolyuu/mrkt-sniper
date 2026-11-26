"""Public, unauthenticated see.tg event feed for MRKT (best effort)."""
import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

WS_URL = "wss://live.see.tg/v1/ws"
EVENTS_FILE = Path(__file__).with_name("see-events.json")


async def run_guest_feed(app):
    """Keep a small deduplicated event buffer; never affects bot startup."""
    events = deque(maxlen=200)
    seen = set()
    app["see_events"] = events
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                    await ws.send_json({"type": "subscribe", "events": ["all"], "markets": ["mrkt"]})
                    async for message in ws:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            item = json.loads(message.data)
                        except (TypeError, json.JSONDecodeError):
                            continue
                        if not isinstance(item, dict):
                            continue
                        if item.get("market") not in (None, "mrkt", "MRKT"):
                            continue
                        key = str(item.get("id") or item.get("event_id") or json.dumps(item, sort_keys=True))
                        if key in seen:
                            continue
                        seen.add(key)
                        item["source"] = "see.tg"
                        item["receivedAt"] = datetime.now(timezone.utc).isoformat()
                        events.appendleft(item)
                        if len(seen) > 500:
                            seen.clear()
                        try:
                            EVENTS_FILE.write_text(json.dumps(list(events), ensure_ascii=False), encoding="utf-8")
                        except OSError:
                            pass
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            print(f"see.tg feed unavailable: {error}", flush=True)
        await asyncio.sleep(5)
