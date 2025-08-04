import socket
from ipaddress import ip_address, IPv4Address
import bitstring
import time
from datetime import datetime
import os
from rwlock import RWLock
import traceback
import threading
import queue
from collections import deque

MAX_CONNECTION_ATTEMPTS = 3
MAX_PENDING_REQUESTS = 128
CONNECTION_TIMEOUT = 10
INACTIVITY_TIMEOUT = 180

def RoundUp(x):
    return ((x + 7) & (-8))

def validIPAddress(IP: str) -> str:
    try:
        return "IPv4" if type(ip_address(IP)) is IPv4Address else "IPv6"
    except ValueError:
        return "Invalid"
    
def log_error(msg, exc=None, flags = None, name = ''):
    if flags is None:
        flags = []
    flags.insert(0, 'ERROR')
    now = datetime.now()
    flags.insert(0, f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{now.microsecond // 1000:03d}")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f'[{timestamp}] {msg}\n'
    if exc is not None:
        if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
            flags.append('Network')
            log_entry += f'Пир разорвал соединение (обычно для BitTorrent): {msg} — {exc}'
        else:
            log_entry = f'{msg}: {exc}'
    else:
        log_entry = f'{msg}'
    log_entry += '\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    if name:
        path = os.path.join('logs', 'peermanager')
    else:
        name = 'peer.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

def log_info(msg, flags = None, name = ''):
    if flags is None:
        flags = []
    now = datetime.now()
    flags.insert(0, f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{now.microsecond // 1000:03d}")
    log_entry = f'{msg}\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    if name:
        path = os.path.join('logs', 'peermanager')
    else:
        name = 'peer.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class Peer:
    def __init__(self, ip_port: tuple[str, int], number_of_pieces, send_cancel):
        self.sock: socket.socket = None
        self._bit_field = bitstring.BitArray(RoundUp(number_of_pieces))
        self._bit_field_len = 0
        self._have_pieces = 0
        self._bit_field_lock = RWLock("_bit_field_lock")
        self._got_bit_field = False
        self.ip_port: tuple[str, int] = ip_port
        self.ip = ip_port[0]
        self.port = ip_port[1]
        self.bit_field_sent = False
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        self.connection_time = None
        self.blocks_sent = 0
        self.connection_attempts = 0
        self.connected = False
        self.handshake_sent = False
        self.handshake_received = False
        self.blocks_down = 0
        self.blocks_up = 0
        self.downloaded_bytes = 0
        self.uploaded_bytes = 0
        self._requests_sent_down = 0
        self._requests_sent_down_lock = RWLock("_requests_sent_lock")
        self._requests_sent_up = 0
        self._requests_sent_up_lock = RWLock("_requests_sent_lock")
        self._pending_requests_down = 0
        self._pending_requests_down_lock = RWLock("_pending_requests_lock")
        self._pending_requests_up = 0
        self._pending_requests_up_lock = RWLock("_pending_requests_lock")
        self._requested_queue: deque[tuple[int, int, int]] = deque()
        self._requested_queue_lock = threading.Lock()
        self._last_request_time = time.time()
        self.bad_peer = False
        self.connecting = False
        self.peer_needs = True
        self.peer_can_send = True
        self._last_update_time = time.time() - 1
        self._last_bytes_down = 0
        self._last_bytes_up = 0
        self._download_speed = 0
        self._upload_speed = 0
        self._download_speed_history = []
        self._upload_speed_history = []
        self.canceled_requests_down = 0
        self.canceled_requests_up = 0
        self.requested_blocks_per_piece: list[set[int]] = [set() for _ in range(number_of_pieces)]
        self.requested_blocks_per_piece_lock = threading.Lock()
        self.initial_have: queue.Queue[int] | None = None
        self.initial_have_put_time: float | None = None
        self.send_cancel = send_cancel
        
    @property
    def requests_sent_down(self):
        with self._requests_sent_down_lock.read_access:
            return self._requests_sent_down
    def _inc_requests_sent_down(self):
        with self._requests_sent_down_lock.write_access:
            self._requests_sent_down += 1
            
    @property
    def requests_sent_up(self):
        with self._requests_sent_up_lock.read_access:
            return self._requests_sent_up
    def _inc_requests_sent_up(self):
        with self._requests_sent_up_lock.write_access:
            self._requests_sent_up += 1

    @property
    def pending_requests_down(self):
        with self._pending_requests_down_lock.read_access:
            return self._pending_requests_down
    def _inc_pending_requests_down(self):
        with self._pending_requests_down_lock.write_access:
            self._pending_requests_down += 1
    def _dec_pending_requests_down(self):
        with self._pending_requests_down_lock.write_access:
            self._pending_requests_down -= 1

    @property
    def pending_requests_up(self):
        with self._pending_requests_up_lock.read_access:
            return self._pending_requests_up
    def _inc_pending_requests_up(self):
        with self._pending_requests_up_lock.write_access:
            self._pending_requests_up += 1
    def _dec_pending_requests_up(self):
        with self._pending_requests_up_lock.write_access:
            self._pending_requests_up -= 1        
    
    @property
    def last_request_time(self):
        return self._last_request_time
    @last_request_time.setter
    def last_request_time(self, value):
        mem = self.last_request_time
        if mem is None or value is None or value - mem >= 1:
            self._last_request_time = value

    def set_bit_field(self, data):
        with self._bit_field_lock.write_access:
            self._bit_field = bitstring.BitArray(data)
            self._have_pieces = self._bit_field.count(1)
            self._bit_field_len = len(self._bit_field)
            self._got_bit_field = self._bit_field_len != 0
            
    def is_bit_set_in_bit_field(self, piece_index) -> bool:
        if not self._got_bit_field or self._bit_field_len <= piece_index:
            return False
        with self._bit_field_lock.read_access:
            return self._bit_field[piece_index]
    
    def set_bit_in_bit_field(self, piece_index) -> bool:
        if not self.is_bit_set_in_bit_field(piece_index) and self._bit_field_len > piece_index:
            with self._bit_field_lock.write_access:
                self._bit_field[piece_index] = 1
                self._have_pieces += 1
                self._got_bit_field = True
                return True
        return False

    def get_abilities(self, client_bf: bitstring.BitArray) -> tuple[bool, bool]:
        with self._bit_field_lock.read_access:
            peer_bf = self._bit_field.copy()
        self.peer_needs = (client_bf & ~peer_bf).any(True)
        self.peer_can_send = (peer_bf & ~client_bf).any(True)
        return self.peer_needs, self.peer_can_send

    def connect_to_peer(self, sock: socket.socket | None = None):
        needs_to_connect = not bool(sock)
        
        if not (0 < self.port < 65536):
            log_info(f"Invalid port number {self.port} for {self.ip}")
            return False

        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError, AttributeError):
                pass
            try:
                self.sock.close()
            except (socket.error, OSError, AttributeError):
                pass
            self.sock = None

        try:
            if validIPAddress(self.ip) == "IPv6":
                log_info(f"{self.ip} is v6")
                if needs_to_connect:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                ip_port = (self.ip, self.port, 0, 0)
            else:
                log_info(f"{self.ip} is v4")
                if needs_to_connect:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024) 
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)  
                ip_port = (self.ip, self.port)

            if needs_to_connect:
                log_info(f"Attempting connection to {ip_port}")
                sock.settimeout(CONNECTION_TIMEOUT)
                sock.connect(ip_port)
            
            try:
                sock.getpeername()
            except (socket.error, OSError) as e:
                log_info(f"Connection verification failed for {ip_port}: {e}")
                self.close2(sock)
                return False
                
            sock.setblocking(False)
            current_time = time.time()
            self.sock = sock
            self.ip_port = ip_port
            self.connection_attempts = 0
            self.connected = True
            self.connection_time = current_time
            self.handshake_sent = False
            self.handshake_received = False
            self.bit_field_sent = False
            self.am_choking = True
            self.am_interested = False
            self.peer_choking = True
            self.peer_interested = False
            
            log_info(f"Successfull connection to {ip_port}")
            return True
            
        except socket.timeout:
            log_info(f"Connection timeout for {ip_port}")
            self.close2(sock)
            return False
        except ConnectionRefusedError:
            log_info(f"Connection refused by {ip_port}")
            self.close2(sock)
            return False
        except Exception as e:
            log_info(f"Connection error for {ip_port}: {str(e)}, \nTraceback:\n{traceback.format_exc()}")
            self.close2(sock)
            return False

    def close(self):
        print(f"{self.ip_port}: close")
        sock_to_close = None
        if self.sock:
            sock_to_close = self.sock
            try:
                sock_to_close.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                sock_to_close.close()
            except:
                pass
        
        self.connecting = False
        self.connected = False
        self.handshake_sent = False
        self.handshake_received = False
        self.sock = None
        self.bit_field_sent = False
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False

    def close2(self, sock):
        self.connection_attempts += 1
        self.connected = False
        if 'sock' in locals():
            try:
                sock.close()
            except:
                pass

    def peer_score(self):
        if self.peer_choking or not self.connected or self.pending_requests_down > 1024 or self.pending_requests_down > max(32, (self._download_speed / (1024 * 1024)) * MAX_PENDING_REQUESTS):
            return float('-inf')

        if self.requests_sent_down > 0:
            if self.blocks_down == self.requests_sent_down - self.canceled_requests_down:
                score = 1
            else:
                score = (self.blocks_down / (self.requests_sent_down - self.canceled_requests_down)) * (self._download_speed / (20 * 1024 * 1024))
        else:
            score = 0.99
        
        return score
    
    def am_request_block(self, piece_index: int, block_index: int):
        with self.requested_blocks_per_piece_lock:
            self.requested_blocks_per_piece[piece_index].add(block_index)
        
        self._inc_pending_requests_down()
        self._inc_requests_sent_down()
    
    def am_cancel_block(self, piece_index: int, block_index: int, skip_notify: bool):
        if not skip_notify:
            with self.requested_blocks_per_piece_lock:
                if block_index in self.requested_blocks_per_piece[piece_index]:
                    self.requested_blocks_per_piece[piece_index].remove(block_index)
                    self.send_cancel(self, piece_index, block_index)
                else:
                    log_error(f"cancel block that not in list {piece_index}:{block_index}")
                    return
        self._dec_pending_requests_down()
        self.canceled_requests_down += 1
    
    def am_got_block(self, piece_index: int, block_index: int, block_length: int):
        with self.requested_blocks_per_piece_lock:
            if block_index in self.requested_blocks_per_piece[piece_index]:
                self.requested_blocks_per_piece[piece_index].remove(block_index)
            else:
                log_error(f"got block that not in list {piece_index}:{block_index}")
        self._dec_pending_requests_down()
        self.blocks_down += 1
        self.downloaded_bytes += block_length
        
    def request_block(self, piece_index: int, block_index: int, block_length: int):
        with self._requested_queue_lock:
            self._requested_queue.append((piece_index, block_index, block_length))
        
        self._inc_pending_requests_up()
        self._inc_requests_sent_up()
    
    def cancel_block(self, piece_index: int, block_index: int, block_length: int):
        try:
            with self._requested_queue_lock:
                self._requested_queue.remove((piece_index, block_index, block_length))
        except:
            return
        self._dec_pending_requests_up()
        self.canceled_requests_up += 1
    
    def send_block(self) -> tuple[int, int, int] | None:
        try:
            with self._requested_queue_lock:
                res = self._requested_queue.popleft()
        except:
            return None
        self._dec_pending_requests_up()
        self.blocks_up += 1
        self.uploaded_bytes += res[2]
        return res