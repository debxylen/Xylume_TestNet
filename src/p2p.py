import asyncio
import threading
import socket
import json
import uuid
import random
import inspect
from typing import Tuple, Dict, Optional, Any, List
from concurrent.futures import TimeoutError as FutureTimeoutError

PeerKey = Tuple[str, int]  # (ip, listening_port)


class P2PNode:
    def __init__(self, listen_ip: str = "0.0.0.0", listen_port: Optional[int] = None,
                 connect_timeout: float = 3.0, recv_buffer_limit: int = 1 << 20):
        """
        Asyncio-backed P2PNode.
        - Override process_received(self, msg) in user code (can be sync or async).
        - Call broadcast(message, per_peer_timeout=3.0) from external code (sync).
        """
        self.listen_ip = listen_ip
        self.listen_port = listen_port or self._get_random_port()
        self.connect_timeout = float(connect_timeout)
        self.recv_buffer_limit = recv_buffer_limit

        # All of these are managed *inside* the event loop thread. Access from other threads
        # must be done via run_coroutine_threadsafe calls.
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

        # mapping: PeerKey -> (reader, writer)
        self._peers: Dict[PeerKey, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}

        # pending responses keyed by (peerkey, msg_id) -> asyncio.Future
        self._pending: Dict[Tuple[PeerKey, str], asyncio.Future] = {}

        # start event loop thread and server
        self._start_loop_and_server()

    def _get_random_port(self) -> int:
        return random.randint(10000, 60000)

    # ---------------------------
    # User-overridable API
    # ---------------------------
    def process_received(self, data: dict) -> Any:
        """
        Override this. Can be a normal function or an `async def`.
        If returns:
          - bool -> that becomes {"id": id, "reply": bool}
          - dict -> will be sent as the response (id added if original message had id)
          - None -> no response sent
        """
        print("process_received stub called; override me.")
        return None

    # ---------------------------
    # Event-loop & server startup
    # ---------------------------
    def _start_loop_and_server(self):
        def _run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            # schedule the server start coroutine
            self._loop.create_task(self._start_server())
            self._loop.run_forever()

        t = threading.Thread(target=_run_loop, daemon=True)
        t.start()
        self._loop_thread = t

        # wait for loop to be available
        while self._loop is None:
            pass

    async def _start_server(self):
        # start asyncio server
        server = await asyncio.start_server(self._handle_client,
                                            host=self.listen_ip,
                                            port=self.listen_port,
                                            limit=self.recv_buffer_limit)
        print(f"P2PNode asyncio server listening on {self.listen_ip}:{self.listen_port}")
        # server runs as long as loop runs

    # ---------------------------
    # Connection / handshake / reader
    # ---------------------------
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peername = writer.get_extra_info("peername") or ("unknown", 0)
        peer_ip = peername[0]
        remote_ephemeral_port = peername[1] if len(peername) > 1 else 0
        # provisional key until handshake says the listening port
        provisional_key: PeerKey = (peer_ip, remote_ephemeral_port)
        self._register_peer(provisional_key, reader, writer)
        try:
            buf = bytearray()
            while True:
                data = await reader.readuntil(separator=b"\n")
                if not data:
                    break
                # decode robustly
                try:
                    text = data.rstrip(b"\n").decode("utf-8", errors="replace")
                except Exception:
                    # fallback, but errors="replace" should avoid exceptions
                    text = data.rstrip(b"\n").decode("utf-8", errors="ignore")

                if not text.strip():
                    continue

                # parse json safely
                try:
                    msg = json.loads(text)
                except json.JSONDecodeError:
                    # ignore malformed JSON; log and continue
                    # (do not propagate)
                    # print(f"Malformed JSON from {peername}: {text!r}")
                    continue

                # handshake
                if msg.get("type") == "hello":
                    declared_port = msg.get("port")
                    if isinstance(declared_port, int):
                        new_key = (peer_ip, declared_port)
                        await self._update_peer_key(provisional_key, new_key, reader, writer)
                        provisional_key = new_key
                    continue

                # reply handling
                if "reply" in msg and "id" in msg:
                    msg_id = msg.get("id")
                    key = provisional_key
                    fut = self._pending.get((key, msg_id))
                    if fut and not fut.done():
                        fut.set_result(msg.get("reply", None))
                    continue

                # normal inbound message -> call process_received (sync or async)
                # run in executor if user provided sync function to avoid blocking loop
                proc = self.process_received
                try:
                    if inspect.iscoroutinefunction(proc):
                        resp = await proc(msg)
                    else:
                        # run in default executor so user code can't block the loop
                        resp = await self._loop.run_in_executor(None, proc, msg)
                except Exception as e:
                    # user code errors shouldn't kill the connection
                    print("process_received raised:", e)
                    resp = None

                if resp is None:
                    continue

                # normalize response
                if isinstance(resp, bool):
                    resp_obj = {"id": msg.get("id", 0), "reply": resp}
                elif isinstance(resp, dict):
                    if "id" not in resp and "id" in msg:
                        resp["id"] = msg["id"]
                    resp_obj = resp
                else:
                    resp_obj = {"id": msg.get("id", 0), "reply": resp}

                # send response best-effort
                try:
                    writer.write((json.dumps(resp_obj) + "\n").encode())
                    await writer.drain()
                except Exception:
                    # writer may be closed; just ignore and allow cleanup
                    break

        except (asyncio.IncompleteReadError, ConnectionResetError):
            # remote closed abruptly
            pass
        except asyncio.LimitOverrunError:
            # message too large; close connection
            pass
        except Exception as e:
            # catch-all to keep loop alive
            # print("Unhandled per-connection exception:", e)
            pass
        finally:
            await self._unregister_writer(writer)

    def _register_peer(self, key: PeerKey, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # register provisional mapping; runs in loop context
        self._peers[key] = (reader, writer)

    async def _update_peer_key(self, old_key: PeerKey, new_key: PeerKey,
                              reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Update peer mapping when a handshake message declares the peer's listening port.
        If there is an existing connection for new_key, prefer the existing one and close the new writer.
        """
        existing = self._peers.get(new_key)
        if existing:
            # prefer existing; close this writer
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            # remove old mapping if present
            if old_key in self._peers and self._peers[old_key][1] is writer:
                self._peers.pop(old_key, None)
            return

        # move provisional to new key
        if old_key in self._peers and self._peers[old_key][1] is writer:
            self._peers.pop(old_key, None)
        self._peers[new_key] = (reader, writer)

    async def _unregister_writer(self, writer: asyncio.StreamWriter):
        # remove any peer mapping that references this writer and cancel pending futures
        to_remove = []
        for k, (r, w) in list(self._peers.items()):
            if w is writer:
                to_remove.append(k)
        for k in to_remove:
            self._peers.pop(k, None)
            # cancel pending futures associated with this peer
            for pkey in list(self._pending.keys()):
                if pkey[0] == k:
                    fut = self._pending.pop(pkey, None)
                    if fut and not fut.done():
                        fut.set_result(None)  # sentinel: closed / no reply
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    # ---------------------------
    # Outbound connect
    # ---------------------------
    def connect_to_peer(self, peer_ip: str, peer_port: int, retries: int = 3, retry_delay: float = 1.0):
        """
        Sync wrapper that schedules the async connection on the node's loop.
        """
        coro = self._connect_to_peer_coro(peer_ip, peer_port, retries, retry_delay)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=self.connect_timeout * (retries + 1) + 1.0)
        except FutureTimeoutError:
            return

    async def _connect_to_peer_coro(self, peer_ip: str, peer_port: int, retries: int, retry_delay: float):
        target = (peer_ip, peer_port)
        if target in self._peers:
            return
        attempt = 0
        while attempt <= retries:
            attempt += 1
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(peer_ip, peer_port),
                                                        timeout=self.connect_timeout)
                # immediately register using declared listening port (we assume peer_port is their listening port)
                self._peers[target] = (reader, writer)
                # send hello handshake
                hello = {"type": "hello", "port": self.listen_port}
                writer.write((json.dumps(hello) + "\n").encode())
                await writer.drain()
                # start reader is already handled by server for inbound sides; for outbound sides we must
                # ensure we have a task that processes incoming messages on this reader/writer pair.
                # We'll create a background task that uses same handler logic:
                self._loop.create_task(self._outbound_reader_task(reader, writer, peer_ip, peer_port))
                return
            except Exception:
                try:
                    await asyncio.sleep(retry_delay)
                except asyncio.CancelledError:
                    return
        return

    async def _outbound_reader_task(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                    peer_ip: str, peer_port: int):
        # reuse the same message processing used for server-accepted connections
        await self._handle_client(reader, writer)

    # ---------------------------
    # Broadcast implementation (async + sync wrapper)
    # ---------------------------
    def broadcast(self, message: dict, per_peer_timeout: float = 3.0) -> List[Any]:
        """
        Synchronous API (keeps same API for external callers).
        Internally schedules the async broadcast and waits for the result.
        """
        if not isinstance(message, dict):
            raise TypeError("message must be a dict")
        coro = self._broadcast_coro(message, float(per_peer_timeout))
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        # wait slightly longer than per_peer_timeout for safety
        try:
            return fut.result(timeout=float(per_peer_timeout) + 2.0)
        except FutureTimeoutError:
            # if the loop couldn't finish in that time, try to get partial result if available
            try:
                return fut.result(timeout=0.1)
            except Exception:
                return []

    async def _broadcast_coro(self, message: dict, per_peer_timeout: float) -> List[Any]:
        msg_id = str(uuid.uuid4())
        message["id"] = msg_id
        payload = (json.dumps(message) + "\n").encode()

        # snapshot peers in loop context
        peers_snapshot = list(self._peers.items())  # [(PeerKey, (reader, writer)), ...]

        # register futures
        futures = []
        for peerkey, (r, w) in peers_snapshot:
            fut = self._loop.create_future()
            self._pending[(peerkey, msg_id)] = fut
            futures.append((peerkey, w, fut))

        # send to all peers (best-effort). If send fails, set pending to None sentinel
        for peerkey, writer, fut in futures:
            try:
                writer.write(payload)
                # don't await drain for each; we gather drains in batches (but calling drain helps flush)
                await writer.drain()
            except Exception:
                # fail fast: set result to None so waiting doesn't hang on this peer
                if not fut.done():
                    fut.set_result(None)

        # gather replies with timeout
        replies = []
        if futures:
            wait_futures = [fut for (_, _, fut) in futures]
            done, pending = await asyncio.wait(wait_futures, timeout=per_peer_timeout)
            for d in done:
                try:
                    val = d.result()
                except Exception:
                    val = None
                if val is not None:
                    replies.append(val)
            # pending futures — convert to None / cancel
            for p in pending:
                if not p.done():
                    p.cancel()
        # cleanup pending map entries for this msg_id
        for peerkey, _, fut in futures:
            self._pending.pop((peerkey, msg_id), None)

        return replies

    # ---------------------------
    # Utility: discovery stub
    # ---------------------------
    def discover_and_connect(self):
        """
        Placeholder. User can call this which should schedule their discovery logic and call connect_to_peer.
        """
        pass