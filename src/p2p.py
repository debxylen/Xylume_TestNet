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
        self.peer_sockets = []  # List of connected peers
        self.lock = threading.Lock()  # To safely modify peers list

        threading.Thread(target=self.start_server, daemon=True).start()
        print(f"Node started on {self.listen_ip}:{self.listen_port}")

    # Get a random port between 5000 and 5010
    def get_random_port(self):
        import random
        return random.randint(5000, 5010)

    def process_received(self, data): # to be overrided by real function
        pass

    # Handle incoming data from peers
    def handle_data(self, conn, addr):
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                received_msg = json.loads(data.decode())
                response = self.process_received(received_msg)
                if isinstance(response, bool):
                    response = {"id": received_msg.get("id", 0), "reply": response}
                conn.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error with {addr}: {e}")
        finally:
            conn.close()

    # Start the server to listen for incoming connections
    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.listen_ip, self.listen_port))
        self.server_socket.listen(5)
        print(f"Listening on {self.listen_ip}:{self.listen_port}...")

        # Continuously accept incoming connections
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"Connected to {addr}")
            threading.Thread(target=self.handle_data, args=(client_socket, addr), daemon=True).start()

    # Establish a two-way connection with another node
    def connect_to_peer(self, peer_ip, peer_port):
        retries = 0
        while retries < 10:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect((peer_ip, peer_port))
                print(f"Connected to {peer_ip}:{peer_port}")

                threading.Thread(target=self.handle_data, args=(peer_socket, (peer_ip, peer_port)), daemon=True).start()

                with self.lock:
                    self.peer_sockets.append(peer_socket)
                break
            except Exception as e:
                retries += 1
                time.sleep(1)

    # Broadcast data to all connected peers
    def broadcast(self, message):
        msg_id = str(uuid.uuid4())
        message["id"] = msg_id
        msg = json.dumps(message).encode()
        responses = []
        with self.lock:
            for peer in self.peer_sockets:
                try:
                    peer.send(msg)
                    response = json.loads(peer.recv(1024).decode())
                    if response.get("id") == msg_id:
                        responses.append(response_data.get("reply", None))
                except Exception as e:
                    print(f"Failed to send to {peer.getpeername()}: {e}")
        return responses

    def discover_and_connect(self):
        # scrape list for other nodes and notify list of our presence so we get added to the list too
        pass # TBD
