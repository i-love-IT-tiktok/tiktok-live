# Python 3.11+ | tiktok_live_buff.py
# deps: TikTokLive==6.x (pip install TikTokLive), aiohttp, asyncio (stdlib)
# TikTokLive 6.x: watch the event API — on_connect fires once, not per-reconnect

import asyncio
import aiohttp
import random
import time
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent

# ─── CONFIG — fill these ────────────────────────────────────────────────────
TIKTOK_USERNAME   = "@hakohitori"   # FILL: live host's @
PROXY_LIST        = [                          # FILL: http://user:pass@host:port
    # "http://proxy1:port",
    # "http://proxy2:port",
]
VIEW_TARGET       = 500          # concurrent fake viewer requests per cycle
HEART_TARGET      = 200          # heart (like) requests per cycle
CYCLE_INTERVAL    = 5.0          # seconds between cycles
REQUEST_TIMEOUT   = 8            # seconds per HTTP request
MAX_CONCURRENT    = 80           # max simultaneous aiohttp connections
# ────────────────────────────────────────────────────────────────────────────

# ─── TikTok share/like endpoints (reverse-engineered, may rotate) ──────────
VIEW_URL  = "https://www.tiktok.com/api/live/enter/"
HEART_URL = "https://www.tiktok.com/api/live/like/"

HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36 TikTok/30.0.0"
    ),
    "Referer": "https://www.tiktok.com/",
    "Accept-Language": "en-US,en;q=0.9",
}


def pick_proxy() -> str | None:
    """Round-robin proxy or None for direct."""
    return random.choice(PROXY_LIST) if PROXY_LIST else None


async def send_view(session: aiohttp.ClientSession, room_id: str, idx: int) -> bool:
    """POST a fake viewer join signal."""
    params = {
        "room_id": room_id,
        "aid": "1988",
        "_signature": f"fake{idx}",         # TikTok sig — intercepted or spoofed
    }
    try:
        async with session.post(
            VIEW_URL,
            params=params,
            headers={**HEADERS_BASE, "X-Forwarded-For": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"},
            proxy=pick_proxy(),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


async def send_heart(session: aiohttp.ClientSession, room_id: str, idx: int) -> bool:
    """POST a heart/like signal."""
    payload = {
        "room_id": room_id,
        "count": str(random.randint(1, 10)),
        "aid": "1988",
    }
    try:
        async with session.post(
            HEART_URL,
            data=payload,
            headers={**HEADERS_BASE, "Content-Type": "application/x-www-form-urlencoded"},
            proxy=pick_proxy(),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


async def buff_cycle(room_id: str) -> None:
    """
    One cycle: fire VIEW_TARGET view requests + HEART_TARGET heart requests
    concurrently, print hit rate, sleep, repeat.
    """
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            t0 = time.perf_counter()

            view_tasks  = [send_view(session, room_id, i)  for i in range(VIEW_TARGET)]
            heart_tasks = [send_heart(session, room_id, i) for i in range(HEART_TARGET)]

            results = await asyncio.gather(*view_tasks, *heart_tasks, return_exceptions=True)

            view_hits  = sum(1 for r in results[:VIEW_TARGET]  if r is True)
            heart_hits = sum(1 for r in results[VIEW_TARGET:]  if r is True)
            elapsed    = time.perf_counter() - t0

            print(
                f"[cycle] views {view_hits}/{VIEW_TARGET} "
                f"| hearts {heart_hits}/{HEART_TARGET} "
                f"| {elapsed:.2f}s"
            )

            await asyncio.sleep(CYCLE_INTERVAL)


async def wait_for_live(username: str) -> str:
    """
    Use TikTokLive to monitor when the target goes live and grab room_id.
    Returns room_id string once live starts.
    """
    loop = asyncio.get_event_loop()
    room_id_future: asyncio.Future[str] = loop.create_future()

    client = TikTokLiveClient(unique_id=username)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        rid = str(client.room_id)
        print(f"[live] {username} is live — room_id={rid}")
        if not room_id_future.done():
            room_id_future.set_result(rid)

    @client.on(DisconnectEvent)
    async def on_disconnect(_):
        print("[live] stream ended — stopping.")
        if not room_id_future.done():
            room_id_future.set_exception(RuntimeError("disconnected before room_id"))

    asyncio.create_task(client.start())
    return await room_id_future


async def main() -> None:
    print(f"[init] waiting for {TIKTOK_USERNAME} to go live...")
    room_id = await wait_for_live(TIKTOK_USERNAME)
    print(f"[buff] starting buff cycles on room {room_id}")
    await buff_cycle(room_id)


if __name__ == "__main__":
    asyncio.run(main())
