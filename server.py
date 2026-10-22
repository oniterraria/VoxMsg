import asyncio
import json
import os
from datetime import datetime

from websockets.asyncio.server import serve


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8765"))

clients = {}
history = []


def make_packet(packet_type, **data):
    return {
        "type": packet_type,
        **data
    }


async def send_json(websocket, packet):
    await websocket.send(
        json.dumps(packet, ensure_ascii=False)
    )


async def broadcast(packet):
    if not clients:
        return

    dead = []

    for websocket in list(clients):
        try:
            await send_json(websocket, packet)
        except Exception:
            dead.append(websocket)

    for websocket in dead:
        clients.pop(websocket, None)


async def send_history(websocket):
    for message in history[-100:]:
        try:
            await send_json(websocket, message)
        except Exception:
            break


async def handler(websocket):
    username = None

    try:
        # --------------------------------------------------
        # LOGIN
        # --------------------------------------------------

        raw = await websocket.recv()
        packet = json.loads(raw)

        if packet.get("type") != "login":
            await send_json(
                websocket,
                make_packet(
                    "error",
                    text="First packet must be login."
                )
            )
            return

        username = str(packet.get("username", "")).strip()

        if not username:
            await send_json(
                websocket,
                make_packet(
                    "error",
                    text="Username cannot be empty."
                )
            )
            return

        if len(username) > 32:
            await send_json(
                websocket,
                make_packet(
                    "error",
                    text="Username is too long."
                )
            )
            return

        # Проверяем занятость имени
        existing_names = {
            name.lower()
            for name in clients.values()
        }

        if username.lower() in existing_names:
            await send_json(
                websocket,
                make_packet(
                    "error",
                    text="Username already in use."
                )
            )
            return

        clients[websocket] = username

        print(
            f"[+] {username} connected "
            f"({len(clients)} online)"
        )

        await send_json(
            websocket,
            make_packet(
                "login_ok",
                username=username
            )
        )

        # История
        await send_history(websocket)

        # Сообщение о входе
        join_packet = make_packet(
            "system",
            text=f"{username} joined the chat."
        )

        history.append(join_packet)
        await broadcast(join_packet)

        # --------------------------------------------------
        # MAIN LOOP
        # --------------------------------------------------

        async for raw in websocket:

            try:
                packet = json.loads(raw)
            except json.JSONDecodeError:
                await send_json(
                    websocket,
                    make_packet(
                        "error",
                        text="Invalid JSON."
                    )
                )
                continue

            packet_type = packet.get("type")

            # ----------------------------------------------
            # CHAT MESSAGE
            # ----------------------------------------------

            if packet_type == "message":

                text = str(packet.get("text", "")).strip()

                if not text:
                    continue

                if len(text) > 2000:
                    await send_json(
                        websocket,
                        make_packet(
                            "error",
                            text="Message is too long."
                        )
                    )
                    continue

                message = make_packet(
                    "message",
                    username=username,
                    text=text,
                    time=datetime.now().strftime("%H:%M:%S")
                )

                history.append(message)

                # Ограничиваем память сервера
                if len(history) > 1000:
                    del history[:-1000]

                await broadcast(message)

            # ----------------------------------------------
            # USERS
            # ----------------------------------------------

            elif packet_type == "users":

                users = list(clients.values())

                await send_json(
                    websocket,
                    make_packet(
                        "users",
                        users=users
                    )
                )

            # ----------------------------------------------
            # PING
            # ----------------------------------------------

            elif packet_type == "ping":

                await send_json(
                    websocket,
                    make_packet("pong")
                )

            else:

                await send_json(
                    websocket,
                    make_packet(
                        "error",
                        text="Unknown command."
                    )
                )

    except Exception as e:

        print(
            f"[!] Connection error "
            f"({username}): {e}"
        )

    finally:

        if websocket in clients:
            clients.pop(websocket, None)

        if username:

            print(
                f"[-] {username} disconnected "
                f"({len(clients)} online)"
            )

            leave_packet = make_packet(
                "system",
                text=f"{username} left the chat."
            )

            history.append(leave_packet)

            if len(history) > 1000:
                del history[:-1000]

            await broadcast(leave_packet)


async def main():

    print("=" * 60)
    print(" VoxMsg Server")
    print("=" * 60)
    print()
    print(f"Listening on {HOST}:{PORT}")
    print()
    print("Press CTRL+C to stop.")
    print()

    async with serve(
        handler,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print()
        print("Server stopped.")