import socket
import threading
import uuid
import json
import time


class P2PNode:
    def __init__(self, listen_ip='0.0.0.0', listen_port=None):
        self.listen_ip = listen_ip
        self.listen_port = listen_port or self.get_random_port()
        self.server_socket = None
        self.peer_sockets = []
        self.lock = threading.Lock()
        self.connected_addrs = set()  # (ip, port) tuples

        threading.Thread(target=self.start_server, daemon=True).start()
        print(f"Node started on {self.listen_ip}:{self.listen_port}")

    def get_random_port(self):
        import random
        return random.randint(5000, 5010)

    def process_received(self, data):  # override this externally
        print('The process_received function stub has been called, meaning that the function hasn\'t been overwritten properly.')

    def handle_data(self, conn, addr):
        buffer = ""
        peer_ip, _ = addr
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data.decode()

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue

                    received_msg = json.loads(line)

                    # handshake message to allow reverse connection
                    if received_msg.get("type") == "hello":
                        print('received hello')
                        peer_port = received_msg.get("port")
                        if peer_port is not None:
                            if peer_ip not in [ip for (ip, _) in self.connected_addrs]:
                                threading.Thread(
                                    target=self.connect_to_peer,
                                    args=(peer_ip, peer_port),
                                    daemon=True
                                ).start()
                        continue

                    if "reply" in received_msg:
                        continue  # ignore replies

                    print(f"Received from {addr}: {received_msg}")
                    response = self.process_received(received_msg)
                    print('Response to send:', response)
                    if isinstance(response, bool):
                        response = {"id": received_msg.get("id", 0), "reply": response}
                    conn.send((json.dumps(response) + "\n").encode())
                    print('Sent response.')
        except Exception as e:
            print(f"Error with {addr}: {e}")
        finally:
            conn.close()

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.listen_ip, self.listen_port))
        self.server_socket.listen(5)
        print(f"Listening on {self.listen_ip}:{self.listen_port}...")

        #threading.Thread(target=self.connect_to_peer, args=("212.192.28.4", 25795), daemon=True).start()

        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"Incoming connection from {addr}")
            with self.lock:
                self.peer_sockets.append(client_socket)
                self.connected_addrs.add((addr[0], addr[1]))
            threading.Thread(target=self.handle_data, args=(client_socket, addr), daemon=True).start()

    def connect_to_peer(self, peer_ip, peer_port):
        if (peer_ip, peer_port) in self.connected_addrs:
            print(f"Already connected to {peer_ip}:{peer_port}")
            return

        retries = 0
        while retries < 10:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect((peer_ip, peer_port))
                print(f"Connected to {peer_ip}:{peer_port}")
                with self.lock:
                    self.peer_sockets.append(peer_socket)
                    self.connected_addrs.add((peer_ip, peer_port))

                # send handshake with our listening port
                hello_msg = json.dumps({
                    "type": "hello",
                    "port": self.listen_port
                }) + "\n"
                peer_socket.send(hello_msg.encode())

                threading.Thread(target=self.handle_data, args=(peer_socket, (peer_ip, peer_port)), daemon=True).start()
                break
            except Exception as e:
                print(f"Could not connect to {peer_ip}:{peer_port} -> {e}")
                retries += 1
                time.sleep(1)

    def broadcast(self, message):
        msg_id = str(uuid.uuid4())
        message["id"] = msg_id
        msg = json.dumps(message) + "\n"
        msg_bytes = msg.encode()

        responses = []
        threads = []
        lock = threading.Lock()

        def send_and_collect(peer):
            try:
                peer.send(msg_bytes)
                response_data = b""
                while b"\n" not in response_data:
                    chunk = peer.recv(1024)
                    if not chunk:
                        return
                    response_data += chunk

                response = json.loads(response_data.decode().strip())
                if response.get("id") == msg_id:
                    with lock:
                        responses.append(response.get("reply", None))
            except Exception as e:
                print(f"Error with {peer.getpeername()}: {e}")

        with self.lock:
            for peer in self.peer_sockets:
                t = threading.Thread(target=send_and_collect, args=(peer,))
                t.start()
                threads.append(t)

        for t in threads:
            t.join(timeout=3)  # timeout so we don't hang forever

        return responses

    def discover_and_connect(self):
        pass  # still empty for now
