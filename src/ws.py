import asyncio
import websockets
import json
import threading
import os

class WSBroadcaster:
    def __init__(self, host="0.0.0.0", port=25857):
        self.host = host
        self.port = port
        self.connected_clients = set()
        self.clients_lock = threading.Lock()
        self.loop = asyncio.new_event_loop()
        self.loop.set_exception_handler(self.handle_ws_exceptions)
        self._server_thread = threading.Thread(target=self._start_server_loop, daemon=True)

    def start(self):
        self._server_thread.start()

    async def ignore_invalid_requests(self, path, request_headers):
        return (400, [], b'Bad request')

    def handle_ws_exceptions(self, loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")

        # --- exceptions that are safe to ignore ---
        if isinstance(exc, (
            websockets.exceptions.InvalidMessage,
            websockets.exceptions.InvalidUpgrade,
            ValueError,
            EOFError,
            ConnectionResetError
        )):
            return

        # --- messages that indicate random scanners, bots, or invalid handshake noise ---
        if isinstance(msg, str) and any(
            s in msg for s in (
                "opening handshake failed",
                "did not receive a valid HTTP request",
                "unsupported HTTP method",
                "unsupported protocol",
                "invalid HTTP request line",
                "missing Connection header",
                "connection closed while reading HTTP request line",
            )
        ):
            return

        # let anything else go through normally
        loop.default_exception_handler(context)

        # Sometimes there is no exception object, just a message string.
        msg = context.get("message", "")
        if isinstance(msg, str) and (
            "did not receive a valid HTTP request" in msg
            or "unsupported HTTP method" in msg
            or "missing Connection header" in msg
        ):
            return

        # Otherwise treat as a real/unexpected exception
        loop.default_exception_handler(context)

    async def _safe_serve(self):
        while True:
            try:
                server = await websockets.serve(self._handler, self.host, self.port, process_request=self.ignore_invalid_requests)
                print(f"[WS] Server running at ws://{self.host}:{self.port}")
                await server.wait_closed()
            except (websockets.exceptions.InvalidMessage, websockets.exceptions.InvalidUpgrade, ValueError):
                pass # ignore
            except Exception as e:
                print("[WS] Server error:", e)
                await asyncio.sleep(1)  # avoid tight crash loop

    def _start_server_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._safe_serve())
        print(f"[WebSocket] Server running at ws://{self.host}:{self.port}")
        self.loop.run_forever()

    async def _handler(self, websocket):
        """Handle incoming WebSocket connections and subscriptions."""
        # register client
        with self.clients_lock:
            self.connected_clients.add(websocket)
        print(f"[WS] Client connected: {websocket.remote_address}")

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    # ignore bad messages
                    continue

                # handle eth_subscribe
                if data.get("method") == "eth_subscribe" and "id" in data:
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": data["id"],
                        "result": "sub0"
                    }))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            # unregister client
            with self.clients_lock:
                self.connected_clients.discard(websocket)
            print(f"[WS] Client disconnected: {websocket.remote_address}")


    def broadcast_tx(self, tx):
        """Public method to send a payload to all connected clients."""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "sub0",
                "result": tx if isinstance(tx, dict) else tx.__json__()
            }
        }
        asyncio.run_coroutine_threadsafe(self._broadcast(payload), self.loop)

    async def _send_one(self, client, message, to_remove):
        try:
            await client.send(message)
        except:
            to_remove.append(client)

    async def _broadcast(self, payload: dict):
        """Async version of the broadcast logic."""
        message = json.dumps(payload)

        with self.clients_lock:
            clients = list(self.connected_clients)

        coros = []
        to_remove = []

        for client in clients:
            coros.append(self._send_one(client, message, to_remove))
        await asyncio.gather(*coros, return_exceptions=True)

        with self.clients_lock:
            for client in to_remove:
                self.connected_clients.discard(client)
