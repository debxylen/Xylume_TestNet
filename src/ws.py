import asyncio
import websockets
import json
import threading
import os

class WSBroadcaster:
    def __init__(self, host="0.0.0.0", port=int(os.environ.get("PORT", 3000))):
        self.host = host
        self.port = port
        self.connected_clients = set()
        self.clients_lock = threading.Lock()
        self.loop = asyncio.new_event_loop()
        self._server_thread = threading.Thread(target=self._start_server_loop, daemon=True)

    def start(self):
        self._server_thread.start()

    async def _safe_serve(self):
        start_server = await websockets.serve(self._handler, self.host, self.port)
        print(f"[WebSocket] Server running at ws://{self.host}:{self.port}")
        await start_server.wait_closed()

    def _start_server_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._safe_serve())
        print(f"[WebSocket] Server running at ws://{self.host}:{self.port}")
        self.loop.run_forever()

    async def _handler(self, websocket):
        """Handle incoming WebSocket connections and subscriptions."""
        with self.clients_lock:
            self.connected_clients.add(websocket)

        try:
            async for message in websocket:
                data = json.loads(message)
                if data.get("method") == "eth_subscribe":
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": data["id"],
                        "result": "sub0"
                    }))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            with self.clients_lock:
                self.connected_clients.discard(websocket)

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

    async def _broadcast(self, payload: dict):
        """Async version of the broadcast logic."""
        message = json.dumps(payload)
        to_remove = []

        with self.clients_lock:
            clients = list(self.connected_clients)

        for client in clients:
            try:
                await client.send(message)
            except:
                to_remove.append(client)

        with self.clients_lock:
            for client in to_remove:
                self.connected_clients.discard(client)
