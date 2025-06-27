import socket
import struct
import threading
import time
from logger import Logger
from peer import Peer
from Messages import interested

class PeerConnection:
    def __init__(self, peer: Peer, piece_manager, number_of_pieces):
        self.peer = peer
        self.piece_manager = piece_manager
        self.number_of_pieces = number_of_pieces
        self.sock = None
        self.running = True
        self.last_transmission = time.time()
        self.connection_timeout = 10  # Increased from 15 to 20 seconds
        self.message_timeout = 10    # 30 seconds for message reading
        self._lock = threading.Lock()
        self.max_retries = 3
        self.retry_delay = 1
        self.buffer_size = 131072  # Increased from 65536 to 131072
        self.keepalive_interval = 60  # 60 seconds between keepalives

    def connect(self):
        with self._lock:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.buffer_size)
                self.sock.settimeout(self.connection_timeout)
                self.sock.connect((self.peer.ip, self.peer.port))
                self.sock.settimeout(self.message_timeout)
                return True
            except socket.timeout:
                Logger.error(f"Connection timeout for peer {self.peer.ip_port}")
                return False
            except Exception as e:
                Logger.error(f"Failed to connect to peer {self.peer.ip_port}", e)
                return False

    def send_message(self, message):
        with self._lock:
            if not self.sock or not self.peer.is_socket_valid():
                raise ConnectionError("Socket is not valid")

            for attempt in range(self.max_retries):
                try:
                    self.sock.send(message)
                    self.last_transmission = time.time()
                    self.peer.update_activity()
                    return
                except socket.timeout:
                    if attempt == self.max_retries - 1:
                        Logger.error(f"Send timeout for peer {self.peer.ip_port}")
                        raise
                    time.sleep(self.retry_delay * (attempt + 1))
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        Logger.error(f"Failed to send message to {self.peer.ip_port}", e)
                        raise
                    time.sleep(self.retry_delay * (attempt + 1))

    def read_message(self):
        with self._lock:
            if not self.sock or not self.peer.is_socket_valid():
                return None

            for attempt in range(self.max_retries):
                try:
                    message_length = self.sock.recv(4)
                    if not message_length or len(message_length) < 4:
                        Logger.error(f"Invalid message length from {self.peer.ip_port}")
                        return None

                    message_length = struct.unpack(">I", message_length)[0]
                    if message_length == 0:  # Keep-alive
                        self.peer.update_activity()
                        return None

                    message_id = self.sock.recv(1)
                    if not message_id:
                        Logger.error(f"Invalid message ID from {self.peer.ip_port}")
                        return None

                    self.peer.update_activity()
                    return struct.unpack(">B", message_id)[0], message_length - 1
                except socket.timeout:
                    if attempt == self.max_retries - 1:
                        Logger.error(f"Read timeout from {self.peer.ip_port}")
                        return None
                    time.sleep(self.retry_delay * (attempt + 1))
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        Logger.error(f"Error reading message from {self.peer.ip_port}", e)
                        return None
                    time.sleep(self.retry_delay * (attempt + 1))

    def handle_unchoke(self):
        self.peer.peer_choking = 0
        try:
            interested_message = interested()
            self.send_message(interested_message.byteStringForInterested())
            self.peer.am_interested = 1
            self.peer.update_activity()
        except Exception as e:
            Logger.error(f"Error sending interested message to {self.peer.ip_port}", e)

    def handle_choke(self):
        self.peer.peer_choking = 1
        self.peer.update_activity()

    def handle_interested(self):
        self.peer.peer_interested = 1
        self.peer.update_activity()
        Logger.info(f"Peer {self.peer.ip_port} is interested")

    def handle_not_interested(self):
        self.peer.peer_interested = 0
        self.peer.update_activity()
        Logger.info(f"Peer {self.peer.ip_port} is not interested")

    def send_keepalive(self):
        """Send keepalive message if needed"""
        if not self.running or not self.sock or not self.peer.is_socket_valid():
            return False
            
        current_time = time.time()
        if current_time - self.last_transmission >= self.keepalive_interval:
            try:
                self.sock.send(b'\x00\x00\x00\x00')  # Keepalive message
                self.last_transmission = current_time
                self.peer.update_activity()
                return True
            except:
                self.running = False
                return False
        return True

    def close(self):
        with self._lock:
            self.running = False
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.sock.close()
                except:
                    pass
                self.sock = None