"""
AMC Quest - online PvP relay server (WebSocket edition).

This is a small, dumb matchmaking relay: it pairs two connected clients
into a room by a 4-character code, then forwards whatever either one
sends straight to the other. It doesn't understand or validate any
battle logic - each game client is trusted to report its own HP and
actions honestly, same "no heavy security, this is a casual kids' game"
philosophy as the rest of the project.

Why WebSockets instead of a plain TCP socket server (the previous
version of this file): browsers can't open raw TCP sockets at all, and
this game can now be built for the browser with pygbag. WebSockets are
the only real option for a browser client to talk to a server, so both
this file and prodigy_net.py were rewritten to speak WebSocket instead
of raw newline-delimited JSON over TCP. The wire *content* (the JSON
message dicts and their "type" fields) is unchanged - each WebSocket
message carries exactly one JSON object, same shapes as before.

Requires: pip install websockets

Run it somewhere both players' games can reach:
    python prodigy_server.py                  # listens on ws://0.0.0.0:8765
    python prodigy_server.py --port 9000
    python prodigy_server.py --host 127.0.0.1  # local testing only

IMPORTANT for the itch.io / browser build specifically: a page served
over https:// (which itch.io always uses) is only allowed to open
*secure* WebSocket connections (wss://), not plain ws:// - browsers
block that as mixed content. Plain ws:// is fine for the desktop pygame
build, and fine for testing the browser build on localhost, but for a
real itch.io deployment you need this server reachable over wss://.
Two ways to get there:
  1. (Simplest) Put a reverse proxy that terminates TLS in front of this
     script (nginx, Caddy, or a host like Render/Fly.io that provides
     HTTPS/WSS for you already) and point players at that address.
  2. Run this script with --cert/--key to have it terminate TLS itself:
         python prodigy_server.py --cert fullchain.pem --key privkey.pem
     Players (and prodigy_net.py's connect field) then use
     "wss://your-domain:port" instead of a bare host.

Wire format: one JSON object per WebSocket message. See prodigy_net.py
for the client side and prodigy_pygame.py's PvP methods for the message
types in play (create_room / join_room / match_found / apply_damage /
heal / hp_sync / miss / spell_cast / game_over / leave).
"""
from __future__ import annotations

import argparse
import asyncio
import http
import json
import os
import random
import ssl

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

DEFAULT_PORT = 8765
ROOM_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O/1/I


class Client:
    def __init__(self, ws):
        self.ws = ws
        self.room_code: str | None = None
        self.username = "Player"
        self.alive = True

    async def send(self, message: dict):
        if not self.alive:
            return
        try:
            await self.ws.send(json.dumps(message))
        except ConnectionClosed:
            self.alive = False


class Room:
    def __init__(self, code: str, host: Client):
        self.code = code
        self.members: list[Client] = [host]

    def other(self, client: Client) -> Client | None:
        for m in self.members:
            if m is not client:
                return m
        return None


class Server:
    def __init__(self):
        # asyncio.serve() runs everything cooperatively on a single thread/
        # event loop, so - unlike the old threaded version - there's no
        # need for a lock around this shared dict.
        self.rooms: dict[str, Room] = {}

    def new_room_code(self) -> str:
        while True:
            code = "".join(random.choice(ROOM_CODE_ALPHABET) for _ in range(4))
            if code not in self.rooms:
                return code

    def create_room(self, client: Client) -> str:
        code = self.new_room_code()
        self.rooms[code] = Room(code, client)
        client.room_code = code
        return code

    def join_room(self, client: Client, code: str) -> tuple[bool, str, Client | None]:
        code = code.strip().upper()
        room = self.rooms.get(code)
        if room is None:
            return False, "No room with that code.", None
        if len(room.members) >= 2:
            return False, "That room is already full.", None
        room.members.append(client)
        client.room_code = code
        host = room.members[0]
        return True, "", host

    async def leave(self, client: Client):
        code = client.room_code
        if not code:
            return
        room = self.rooms.get(code)
        if room is None:
            return
        other = room.other(client)
        room.members = [m for m in room.members if m is not client]
        if not room.members:
            del self.rooms[code]
        if other is not None and other.alive:
            await other.send({"type": "opponent_left"})


async def handle_message(server: Server, client: Client, msg: dict):
    msg_type = msg.get("type")

    if msg_type == "create_room":
        client.username = str(msg.get("username", "Player"))[:24]
        code = server.create_room(client)
        await client.send({"type": "room_created", "code": code})
        return

    if msg_type == "join_room":
        client.username = str(msg.get("username", "Player"))[:24]
        code = str(msg.get("code", ""))
        ok, error, host = server.join_room(client, code)
        if not ok:
            await client.send({"type": "error", "message": error})
            return
        seed = random.randint(0, 2**31 - 1)
        host_name = host.username if host is not None else "Player"
        await client.send({"type": "match_found", "opponent": host_name, "seed": seed,
                            "as_host": False})
        if host is not None:
            await host.send({"type": "match_found", "opponent": client.username,
                              "seed": seed, "as_host": True})
        return

    if msg_type == "leave":
        await server.leave(client)
        client.room_code = None
        return

    # Everything else is pure relay to whoever shares this client's room -
    # damage, heals, hp syncs, spell-cast toasts, game_over, etc. The server
    # doesn't need to understand any of it.
    code = client.room_code
    if not code:
        return
    room = server.rooms.get(code)
    other = room.other(client) if room else None
    if other is not None:
        await other.send(msg)


async def handle_connection(server: Server, ws):
    client = Client(ws)
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                continue
            await handle_message(server, client, msg)
    except ConnectionClosed:
        pass
    finally:
        client.alive = False
        await server.leave(client)


async def health_check(connection, request):
    """Answer plain HTTP requests (e.g. Render's health-check probe, or
    anyone opening the URL in a browser tab) with a normal HTTP response
    instead of letting them fail as a broken WebSocket handshake. A real
    WebSocket connection always carries an "Upgrade: websocket" header,
    so only requests missing that get intercepted here - everything else
    proceeds to the normal handshake untouched."""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(http.HTTPStatus.OK, "AMC Quest relay is running.\n")
    return None


async def run_server(host: str, port: int, ssl_context: ssl.SSLContext | None):
    server = Server()
    scheme = "wss" if ssl_context else "ws"
    async with serve(lambda ws: handle_connection(server, ws), host, port, ssl=ssl_context,
                      process_request=health_check) as ws_server:
        print(f"AMC Quest PvP relay listening on {scheme}://{host}:{port}")
        print("Share your reachable address:port with the other player to battle.")
        await ws_server.wait_closed()


def main():
    parser = argparse.ArgumentParser(description="AMC Quest online PvP relay server")
    parser.add_argument("--host", default="0.0.0.0", help="Address to listen on (default 0.0.0.0)")
    # Render (and most PaaS hosts) assign a port dynamically via the PORT
    # env var - the server must bind to that, not a fixed port, or the
    # platform's edge proxy won't be able to reach it. Falls back to
    # DEFAULT_PORT for plain local runs where PORT isn't set.
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", DEFAULT_PORT)),
                         help=f"Port to listen on (default {DEFAULT_PORT}, or $PORT if set)")
    parser.add_argument("--cert", default=None, help="TLS certificate file (enables wss://, e.g. fullchain.pem)")
    parser.add_argument("--key", default=None, help="TLS private key file (required if --cert is given)")
    args = parser.parse_args()

    ssl_context = None
    if args.cert:
        if not args.key:
            parser.error("--cert requires --key")
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(args.cert, args.key)

    try:
        asyncio.run(run_server(args.host, args.port, ssl_context))
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()
