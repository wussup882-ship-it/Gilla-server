"""Tiny relay server for Gilla Remake rooms.

Everyone who joins the same room code sees each other. The server only relays
messages (positions, hits, kicks) - it holds no game logic.
Env vars: PORT (default 8765), MOD_TOKEN (optional, enables real kicks),
MAX_PER_ROOM (default 10).
"""
import asyncio
import json
import os
import itertools
from http import HTTPStatus

import websockets

PORT = int(os.environ.get("PORT", "8765"))
MOD_TOKEN = os.environ.get("MOD_TOKEN", "")
MAX_PER_ROOM = int(os.environ.get("MAX_PER_ROOM", "10"))

rooms = {}
_ids = itertools.count(1)


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj, separators=(",", ":")))
    except Exception:
        pass


async def broadcast(room, obj, skip=None):
    for pid, c in list(rooms.get(room, {}).items()):
        if pid != skip:
            await send(c["ws"], obj)


def vec(v, n):
    if isinstance(v, list) and len(v) >= n and all(isinstance(x, (int, float)) for x in v[:n]):
        return [float(x) for x in v[:n]]
    return None


async def handler(ws):
    pid = next(_ids)
    me = {"ws": ws, "name": "ape", "room": None, "mod": False}
    try:
        async for raw in ws:
            if isinstance(raw, bytes) or len(raw) > 2000:
                continue
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(m, dict):
                continue
            t = m.get("t")
            if t == "join" and me["room"] is None:
                room = str(m.get("room", "GILLA-1"))[:24]
                members = rooms.setdefault(room, {})
                if len(members) >= MAX_PER_ROOM:
                    await send(ws, {"t": "full"})
                    await ws.close()
                    return
                me["name"] = str(m.get("name", "ape"))[:16]
                me["room"] = room
                me["mod"] = bool(MOD_TOKEN) and m.get("tok") == MOD_TOKEN
                peers = [{"id": i, "name": c["name"]} for i, c in members.items()]
                members[pid] = me
                await send(ws, {"t": "welcome", "id": pid, "mod": me["mod"], "peers": peers})
                await broadcast(room, {"t": "joined", "id": pid, "name": me["name"]}, skip=pid)
            elif me["room"] is None:
                continue
            elif t == "st":
                p, l, r = vec(m.get("p"), 4), vec(m.get("l"), 3), vec(m.get("r"), 3)
                if p and l and r:
                    out = {"t": "st", "id": pid, "p": p, "l": l, "r": r,
                           "m": str(m.get("m", "lobby"))[:8], "f": int(m.get("f", 0))}
                    await broadcast(me["room"], out, skip=pid)
            elif t == "hit":
                v = vec(m.get("v"), 3)
                target = rooms.get(me["room"], {}).get(int(m.get("to", 0)))
                if v and target:
                    mag = sum(x * x for x in v) ** 0.5
                    if mag > 14.0:
                        v = [x * 14.0 / mag for x in v]
                    await send(target["ws"], {"t": "hit", "v": v})
            elif t == "kick" and me["mod"]:
                target = rooms.get(me["room"], {}).get(int(m.get("to", 0)))
                if target:
                    await send(target["ws"], {"t": "kicked"})
                    await target["ws"].close()
    except websockets.ConnectionClosed:
        pass
    finally:
        room = me["room"]
        if room and pid in rooms.get(room, {}):
            del rooms[room][pid]
            await broadcast(room, {"t": "left", "id": pid})
            if not rooms[room]:
                del rooms[room]


async def health(path, request_headers):
    if path == "/healthz":
        return HTTPStatus.OK, [], b"ok\n"
    return None


async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT, max_size=4096,
                                ping_interval=20, process_request=health):
        print("Gilla relay listening on", PORT, flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
