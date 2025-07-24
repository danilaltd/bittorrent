import socket
from ipaddress import ip_address, IPv4Address
import bitstring
import time
from datetime import datetime
import os
from logger import timed_lock
from rwlock import RWLock
import traceback
import threading
import errno
import struct
import traceback
import selectors

MAX_CONNECTION_ATTEMPTS = 3
MAX_PENDING_REQUESTS = 128
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
    def __init__(self, ip_port: tuple[str, int], number_of_pieces):
        self._sock: socket.socket = None
        self._sock_lock = RWLock("_sock_lock")
        self._bit_field = bitstring.BitArray(RoundUp(number_of_pieces))
        self._bit_field_len = 0
        self._bit_field_lock = RWLock("_bit_field_lock")
        self._got_bit_field = False
        self._ip_port: tuple[str, int] = ip_port
        self._ip_port_lock = RWLock("_ip_port_lock")
        self.ip = ip_port[0]
        self.port = ip_port[1]
        self._am_choking = 1
        self._am_choking_lock = RWLock("_am_choking_lock")
        self._am_interested = 0
        self._am_interested_lock = RWLock("_am_interested_lock")
        self._peer_choking = True
        self._peer_choking_lock = RWLock("_peer_choking_lock")
        self._peer_interested = 0
        self._peer_interested_lock = RWLock("_peer_interested_lock")
        self._connection_time = None
        self._connection_time_lock = RWLock("_connection_time_lock")
        self._rate = None
        self._rate_lock = RWLock("_rate_lock")
        self._uploaded = 0
        self._uploaded_lock = RWLock("_uploaded_lock")
        self._blocks_recieved = 0
        self._blocks_recieved_lock = RWLock("_blocks_recieved_lock")
        self._connection_attempts = 0
        self._connection_attempts_lock = RWLock("_connection_attempts_lock")
        self._connection_timeout = 10
        self._connection_timeout_lock = RWLock("_connection_timeout_lock")
        self._last_connection_attempt = 0
        self._last_connection_attempt_lock = RWLock("_last_connection_attempt_lock")
        self._connection_cooldown = 5
        self._connection_cooldown_lock = RWLock("_connection_cooldown_lock")
        self._connected = False
        self._connected_lock = RWLock("_connected_lock")
        self._handshake_sent = False
        self._handshake_sent_lock = RWLock("_handshake_sent_lock")
        self._handshake_received = False
        self._handshake_received_lock = RWLock("_handshake_received_lock")
        self._last_keepalive = time.time()
        self._last_keepalive_lock = RWLock("_last_keepalive_lock")
        self._keepalive_interval = 60
        self._keepalive_interval_lock = RWLock("_keepalive_interval_lock")
        self._last_activity = time.time()
        self._last_activity_lock = RWLock("_last_activity_lock")
        self._inactivity_timeout = 180
        self._inactivity_timeout_lock = RWLock("_inactivity_timeout_lock")
        self._requests_sent = 0
        self._requests_sent_lock = RWLock("_requests_sent_lock")
        self._pending_requests = 0
        self._pending_requests_lock = RWLock("_pending_requests_lock")
        self._last_request_time = time.time()
        self._last_request_time_lock = RWLock("_last_request_time_lock")
        self._bad_peer = False
        self._bad_peer_lock = RWLock("_bad_peer_lock")
        self._connecting = False
        self._connecting_lock = RWLock("_connecting_lock")
        
        self._last_update_time = time.time()
        self._last_blocks_done = 0
        self._last_bytes_done = 0
        self._download_speed = 0
        self._speed_history = []
        self.canceled_requests = 0
        self.requested_blocks_per_piece: list[set[int]] = [set() for _ in range(number_of_pieces)]
        
    @property
    def sock(self):
        with self._sock_lock.read_access:
            return self._sock
    @sock.setter
    def sock(self, value):
        with self._sock_lock.write_access:
            self._sock = value

    @property
    def ip_port(self):
        with self._ip_port_lock.read_access:
            return self._ip_port
    @ip_port.setter
    def ip_port(self, value):
        with self._ip_port_lock.write_access:
            self._ip_port = value

    @property
    def am_choking(self):
        with self._am_choking_lock.read_access:
            return self._am_choking
    @am_choking.setter
    def am_choking(self, value):
        with self._am_choking_lock.write_access:
            self._am_choking = value

    @property
    def am_interested(self):
        with self._am_interested_lock.read_access:
            return self._am_interested
    @am_interested.setter
    def am_interested(self, value):
        with self._am_interested_lock.write_access:
            self._am_interested = value

    @property
    def peer_choking(self):
        with self._peer_choking_lock.read_access:
            return self._peer_choking
    @peer_choking.setter
    def peer_choking(self, value):
        with self._peer_choking_lock.write_access:
            self._peer_choking = value

    @property
    def peer_interested(self):
        with self._peer_interested_lock.read_access:
            return self._peer_interested
    @peer_interested.setter
    def peer_interested(self, value):
        with self._peer_interested_lock.write_access:
            self._peer_interested = value

    @property
    def connection_time(self):
        with self._connection_time_lock.read_access:
            return self._connection_time
    @connection_time.setter
    def connection_time(self, value):
        with self._connection_time_lock.write_access:
            self._connection_time = value

    @property
    def rate(self):
        with self._rate_lock.read_access:
            return self._rate
    @rate.setter
    def rate(self, value):
        with self._rate_lock.write_access:
            self._rate = value

    @property
    def uploaded(self):
        with self._uploaded_lock.read_access:
            return self._uploaded
    @uploaded.setter
    def uploaded(self, value):
        with self._uploaded_lock.write_access:
            self._uploaded = value

    @property
    def blocks_recieved(self):
        with self._blocks_recieved_lock.read_access:
            return self._blocks_recieved
    @blocks_recieved.setter
    def blocks_recieved(self, value):
        with self._blocks_recieved_lock.write_access:
            self._blocks_recieved = value

    @property
    def connection_attempts(self):
        with self._connection_attempts_lock.read_access:
            return self._connection_attempts
    @connection_attempts.setter
    def connection_attempts(self, value):
        with self._connection_attempts_lock.write_access:
            self._connection_attempts = value

    @property
    def connection_timeout(self):
        with self._connection_timeout_lock.read_access:
            return self._connection_timeout
    @connection_timeout.setter
    def connection_timeout(self, value):
        with self._connection_timeout_lock.write_access:
            self._connection_timeout = value

    @property
    def last_connection_attempt(self):
        with self._last_connection_attempt_lock.read_access:
            return self._last_connection_attempt
    @last_connection_attempt.setter
    def last_connection_attempt(self, value):
        with self._last_connection_attempt_lock.write_access:
            self._last_connection_attempt = value

    @property
    def connection_cooldown(self):
        with self._connection_cooldown_lock.read_access:
            return self._connection_cooldown
    @connection_cooldown.setter
    def connection_cooldown(self, value):
        with self._connection_cooldown_lock.write_access:
            self._connection_cooldown = value

    @property
    def connected(self):
        with self._connected_lock.read_access:
            return self._connected
    @connected.setter
    def connected(self, value):
        with self._connected_lock.write_access:
            self._connected = value

    @property
    def handshake_sent(self):
        with self._handshake_sent_lock.read_access:
            return self._handshake_sent
    @handshake_sent.setter
    def handshake_sent(self, value):
        with self._handshake_sent_lock.write_access:
            self._handshake_sent = value

    @property
    def handshake_received(self):
        with self._handshake_received_lock.read_access:
            return self._handshake_received
    @handshake_received.setter
    def handshake_received(self, value):
        with self._handshake_received_lock.write_access:
            self._handshake_received = value

    @property
    def last_keepalive(self):
        with self._last_keepalive_lock.read_access:
            return self._last_keepalive
    @last_keepalive.setter
    def last_keepalive(self, value):
        with self._last_keepalive_lock.write_access:
            self._last_keepalive = value

    @property
    def keepalive_interval(self):
        with self._keepalive_interval_lock.read_access:
            return self._keepalive_interval
    @keepalive_interval.setter
    def keepalive_interval(self, value):
        with self._keepalive_interval_lock.write_access:
            self._keepalive_interval = value

    @property
    def last_activity(self):
        with self._last_activity_lock.read_access:
            return self._last_activity
    @last_activity.setter
    def last_activity(self, value):
        mem = self.last_activity
        if mem is None or value is None or value - mem >= 1:
            with self._last_activity_lock.write_access:
                self._last_activity = value

    @property
    def inactivity_timeout(self):
        with self._inactivity_timeout_lock.read_access:
            return self._inactivity_timeout
    @inactivity_timeout.setter
    def inactivity_timeout(self, value):
        with self._inactivity_timeout_lock.write_access:
            self._inactivity_timeout = value

    @property
    def requests_sent(self):
        with self._requests_sent_lock.read_access:
            return self._requests_sent
    @requests_sent.setter
    def requests_sent(self, value):
        with self._requests_sent_lock.write_access:
            self._requests_sent = value

    @property
    def pending_requests(self):
        with self._pending_requests_lock.read_access:
            return self._pending_requests
    @pending_requests.setter
    def pending_requests(self, value):
        if value >= 0:
            with self._pending_requests_lock.write_access:
                self._pending_requests = value

    @property
    def last_request_time(self):
        with self._last_request_time_lock.read_access:
            return self._last_request_time
    @last_request_time.setter
    def last_request_time(self, value):
        mem = self.last_request_time
        if mem is None or value is None or value - mem >= 1:
            with self._last_request_time_lock.write_access:
                self._last_request_time = value

    @property
    def bad_peer(self):
        with self._bad_peer_lock.read_access:
            return self._bad_peer
    @bad_peer.setter
    def bad_peer(self, value):
        with self._bad_peer_lock.write_access:
            self._bad_peer = value
    
    @property
    def connecting(self):
        with self._connecting_lock.read_access:
            return self._connecting
    @connecting.setter
    def connecting(self, value):
        with self._connecting_lock.write_access:
            self._connecting = value

    def set_bit_field(self, data):
        with self._bit_field_lock.write_access:
            self._bit_field = bitstring.BitArray(data)
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
                self._got_bit_field = True
                return True
        return False

    def connect_to_peer(self):
        current_time = time.time()
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            log_info(f"{self.ip}: current_time - self.last_connection_attempt < self.connection_cooldown")
            return None
        
        if not (0 < self.port < 65536):
            log_info(f"Invalid port number {self.port} for {self.ip}")
            return None

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
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                ip_port = (self.ip, self.port, 0, 0)
            else:
                log_info(f"{self.ip} is v4")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                # sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                # sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                # sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5) 
                # sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)  
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024) 
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)  
                ip_port = (self.ip, self.port)

            log_info(f"Attempting connection to {ip_port}")
            sock.settimeout(self.connection_timeout)
                
            sock.connect(ip_port)
            
            # Проверяем, что соединение действительно установлено
            try:
                sock.getpeername()
            except (socket.error, OSError) as e:
                log_info(f"Connection verification failed for {ip_port}: {e}")
                self.close2(sock)
                return None
            
            # Обновляем состояние под блокировкой (быстрая операция)
            sock.setblocking(False)
            current_time = time.time()
            self.sock = sock
            self.ip_port = ip_port
            self.last_keepalive = current_time
            self.update_activity(current_time)
            self.connection_attempts = 0
            self.connected = True
            self.connection_time = current_time
            self.handshake_sent = False
            self.handshake_received = False
            
            log_info(f"Successfull connection to {ip_port}")
            return sock
            
        except socket.timeout:
            log_info(f"Connection timeout for {ip_port}")
            self.close2(sock)
            return None
        except ConnectionRefusedError:
            log_info(f"Connection refused by {ip_port}")
            self.close2(sock)
            return None
        except Exception as e:
            log_info(f"Connection error for {ip_port}: {str(e)}, \nTraceback:\n{traceback.format_exc()}")
            self.close2(sock)
            return None

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
        
        self.connected = False
        self.handshake_sent = False
        self.handshake_received = False
        self.sock = None

    def close2(self, sock):
        # print(f"{self.ip_port}: close2")
        self.connection_attempts += 1
        self.connected = False
        if 'sock' in locals():
            try:
                sock.close()
            except:
                pass

    # def send_keepalive(self):
    #     """Send keepalive message if needed"""
    #     if not self.connected or not self.sock:
    #         return False
            
    #     current_time = time.time()
    #     if current_time - self.last_keepalive >= self.keepalive_interval:
    #         try:
    #             # self._send_data(b'\x00\x00\x00\x00')  # Keepalive message
    #             self.last_keepalive = current_time
    #             return True
    #         except Exception as e:
    #             log_info(f"Keepalive send failed for {self.ip_port}: {e}")
    #             self.connected = False
    #             return False
    #     return True

    # def check_inactivity(self):
    #     """Check if peer has been inactive for too long"""
    #     if not self.connected:
    #         return True
    #     current_time = time.time()
    #     if current_time - self.last_activity >= self.inactivity_timeout:
    #         self.connected = False
    #         return True
    #     return False

    def update_activity(self, current_time = None):
        if current_time is None:
            current_time = time.time()
        self.last_activity = current_time
        
    def peer_score(self):
        # if self.ip == "5.79.98.162":
            # print("ok")
            # return 1000
        # else:
            # print(self.ip)
            # return -1000
        
        if self.peer_choking or not self.connected or self.pending_requests > 1024 or self.pending_requests > max(32, (self._download_speed / (1024 * 1024)) * MAX_PENDING_REQUESTS):
            # print(f"ret -inf: {self.ip} {self.peer_choking} {not self.connected} {self.pending_requests > max(32, (self._download_speed / (1024 * 1024)) * MAX_PENDING_REQUESTS)}")
            return float('-inf')

        if self.requests_sent > 0:
            if self.blocks_recieved == self.requests_sent:
                score = 1
            else:
                score = (self.blocks_recieved / self.requests_sent) * (self._download_speed / (20 * 1024 * 1024))
        else:
            score = 0.99
        
        # print(f"{self.ip}: {score}")
        return score
    
    def is_active(self, needed_pieces: set):
        bitfield = self.bit_field.copy()
        length = len(self.bit_field)
        return any(bitfield[i] for i in needed_pieces if i < length)
    
    def avaliable_pieces(self, needed_pieces: set):
        bitfield = self.bit_field.copy()
        length = len(self.bit_field)
        return sum(1 for i in needed_pieces if i < length and bitfield[i])

    def cancel_block(self, piece_index: int, block_index: int):
        self.requested_blocks_per_piece[piece_index].discard(block_index)
        self.pending_requests -= 1
        self.canceled_requests += 1
    
    def got_block(self, piece_index: int, block_index: int):
        self.requested_blocks_per_piece[piece_index].discard(block_index)
        self.pending_requests -= 1
        self.blocks_recieved += 1

    
    def request_block(self, piece_index: int, block_index: int, current_time: float):
        self.requested_blocks_per_piece[piece_index].add(block_index)
        
        self.pending_requests += 1
        self.requests_sent += 1
        
        if not current_time:
            current_time = time.time()
        self.last_request_time = current_time




            


