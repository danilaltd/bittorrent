import socket
from ipaddress import ip_address, IPv4Address
import bitstring
import time
from datetime import datetime
import os
from logger import timed_lock
from rwlock import RWLock
import traceback

def RoundUp(x):
    return ((x + 7) & (-8))

def validIPAddress(IP: str) -> str:
    try:
        return "IPv4" if type(ip_address(IP)) is IPv4Address else "IPv6"
    except ValueError:
        return "Invalid"
    
def log_info(msg, flags = None):
    if flags is None:
        flags = []
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_entry = f'{msg}\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    with open(os.path.join('logs', "peer.log"), 'a', encoding='utf-8') as f:
        f.write(res)
        
def log_error(msg, exc=None, flags = None):
    if flags is None:
        flags = []
    flags.insert(0, 'ERROR')
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    with open(os.path.join('logs', "peer.log"), 'a', encoding='utf-8') as f:
        f.write(res)

class Peer:
    def __init__(self, ip_port, number_of_pieces):
        self._sock = None
        self._sock_lock = RWLock("_sock_lock")
        self._bit_field = bitstring.BitArray(RoundUp(number_of_pieces))
        self._bit_field_lock = RWLock("_bit_field_lock")
        self._got_bit_field = False
        self._got_bit_field_lock = RWLock("_got_bit_field_lock")
        self._ip_port = ip_port
        self._ip_port_lock = RWLock("_ip_port_lock")
        self._ip = ip_port[0]
        self._ip_lock = RWLock("_ip_lock")
        self._port = ip_port[1]
        self._port_lock = RWLock("_port_lock")
        self._am_choking = 1
        self._am_choking_lock = RWLock("_am_choking_lock")
        self._am_interested = 0
        self._am_interested_lock = RWLock("_am_interested_lock")
        self._peer_choking = 1
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
        self._max_connection_attempts = 3
        self._max_connection_attempts_lock = RWLock("_max_connection_attempts_lock")
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

    @property
    def sock(self):
        with timed_lock(self._sock_lock.read_access, "_sock_lock.read_access"):
            return self._sock
    @sock.setter
    def sock(self, value):
        with timed_lock(self._sock_lock.write_access, "_sock_lock.write_access"):
            self._sock = value

    @property
    def bit_field(self):
        with timed_lock(self._bit_field_lock.read_access, "_bit_field_lock.read_access"):
            return self._bit_field
    @bit_field.setter
    def bit_field(self, value):
        with timed_lock(self._bit_field_lock.write_access, "_bit_field_lock.write_access"):
            self._bit_field = value

    @property
    def got_bit_field(self):
        with timed_lock(self._got_bit_field_lock.read_access, "_got_bit_field_lock.read_access"):
            return self._got_bit_field
    @got_bit_field.setter
    def got_bit_field(self, value):
        with timed_lock(self._got_bit_field_lock.write_access, "_got_bit_field_lock.write_access"):
            self._got_bit_field = value

    @property
    def ip_port(self):
        with timed_lock(self._ip_port_lock.read_access, "_ip_port_lock.read_access"):
            return self._ip_port
    @ip_port.setter
    def ip_port(self, value):
        with timed_lock(self._ip_port_lock.write_access, "_ip_port_lock.write_access"):
            self._ip_port = value

    @property
    def ip(self):
        with timed_lock(self._ip_lock.read_access, "_ip_lock.read_access"):
            return self._ip
    @ip.setter
    def ip(self, value):
        with timed_lock(self._ip_lock.write_access, "_ip_lock.write_access"):
            self._ip = value

    @property
    def port(self):
        with timed_lock(self._port_lock.read_access, "_port_lock.read_access"):
            return self._port
    @port.setter
    def port(self, value):
        with timed_lock(self._port_lock.write_access, "_port_lock.write_access"):
            self._port = value

    @property
    def am_choking(self):
        with timed_lock(self._am_choking_lock.read_access, "_am_choking_lock.read_access"):
            return self._am_choking
    @am_choking.setter
    def am_choking(self, value):
        with timed_lock(self._am_choking_lock.write_access, "_am_choking_lock.write_access"):
            self._am_choking = value

    @property
    def am_interested(self):
        with timed_lock(self._am_interested_lock.read_access, "_am_interested_lock.read_access"):
            return self._am_interested
    @am_interested.setter
    def am_interested(self, value):
        with timed_lock(self._am_interested_lock.write_access, "_am_interested_lock.write_access"):
            self._am_interested = value

    @property
    def peer_choking(self):
        with timed_lock(self._peer_choking_lock.read_access, "_peer_choking_lock.read_access"):
            return self._peer_choking
    @peer_choking.setter
    def peer_choking(self, value):
        with timed_lock(self._peer_choking_lock.write_access, "_peer_choking_lock.write_access"):
            self._peer_choking = value

    @property
    def peer_interested(self):
        with timed_lock(self._peer_interested_lock.read_access, "_peer_interested_lock.read_access"):
            return self._peer_interested
    @peer_interested.setter
    def peer_interested(self, value):
        with timed_lock(self._peer_interested_lock.write_access, "_peer_interested_lock.write_access"):
            self._peer_interested = value

    @property
    def connection_time(self):
        with timed_lock(self._connection_time_lock.read_access, "_connection_time_lock.read_access"):
            return self._connection_time
    @connection_time.setter
    def connection_time(self, value):
        with timed_lock(self._connection_time_lock.write_access, "_connection_time_lock.write_access"):
            self._connection_time = value

    @property
    def rate(self):
        with timed_lock(self._rate_lock.read_access, "_rate_lock.read_access"):
            return self._rate
    @rate.setter
    def rate(self, value):
        with timed_lock(self._rate_lock.write_access, "_rate_lock.write_access"):
            self._rate = value

    @property
    def uploaded(self):
        with timed_lock(self._uploaded_lock.read_access, "_uploaded_lock.read_access"):
            return self._uploaded
    @uploaded.setter
    def uploaded(self, value):
        with timed_lock(self._uploaded_lock.write_access, "_uploaded_lock.write_access"):
            self._uploaded = value

    @property
    def blocks_recieved(self):
        with timed_lock(self._blocks_recieved_lock.read_access, "_blocks_recieved_lock.read_access"):
            return self._blocks_recieved
    @blocks_recieved.setter
    def blocks_recieved(self, value):
        with timed_lock(self._blocks_recieved_lock.write_access, "_blocks_recieved_lock.write_access"):
            self._blocks_recieved = value

    @property
    def connection_attempts(self):
        with timed_lock(self._connection_attempts_lock.read_access, "_connection_attempts_lock.read_access"):
            return self._connection_attempts
    @connection_attempts.setter
    def connection_attempts(self, value):
        with timed_lock(self._connection_attempts_lock.write_access, "_connection_attempts_lock.write_access"):
            self._connection_attempts = value

    @property
    def max_connection_attempts(self):
        with timed_lock(self._max_connection_attempts_lock.read_access, "_max_connection_attempts_lock.read_access"):
            return self._max_connection_attempts
    @max_connection_attempts.setter
    def max_connection_attempts(self, value):
        with timed_lock(self._max_connection_attempts_lock.write_access, "_max_connection_attempts_lock.write_access"):
            self._max_connection_attempts = value

    @property
    def connection_timeout(self):
        with timed_lock(self._connection_timeout_lock.read_access, "_connection_timeout_lock.read_access"):
            return self._connection_timeout
    @connection_timeout.setter
    def connection_timeout(self, value):
        with timed_lock(self._connection_timeout_lock.write_access, "_connection_timeout_lock.write_access"):
            self._connection_timeout = value

    @property
    def last_connection_attempt(self):
        with timed_lock(self._last_connection_attempt_lock.read_access, "_last_connection_attempt_lock.read_access"):
            return self._last_connection_attempt
    @last_connection_attempt.setter
    def last_connection_attempt(self, value):
        with timed_lock(self._last_connection_attempt_lock.write_access, "_last_connection_attempt_lock.write_access"):
            self._last_connection_attempt = value

    @property
    def connection_cooldown(self):
        with timed_lock(self._connection_cooldown_lock.read_access, "_connection_cooldown_lock.read_access"):
            return self._connection_cooldown
    @connection_cooldown.setter
    def connection_cooldown(self, value):
        with timed_lock(self._connection_cooldown_lock.write_access, "_connection_cooldown_lock.write_access"):
            self._connection_cooldown = value

    @property
    def connected(self):
        with timed_lock(self._connected_lock.read_access, "_connected_lock.read_access"):
            return self._connected
    @connected.setter
    def connected(self, value):
        with timed_lock(self._connected_lock.write_access, "_connected_lock.write_access"):
            self._connected = value

    @property
    def handshake_sent(self):
        with timed_lock(self._handshake_sent_lock.read_access, "_handshake_sent_lock.read_access"):
            return self._handshake_sent
    @handshake_sent.setter
    def handshake_sent(self, value):
        with timed_lock(self._handshake_sent_lock.write_access, "_handshake_sent_lock.write_access"):
            self._handshake_sent = value

    @property
    def handshake_received(self):
        with timed_lock(self._handshake_received_lock.read_access, "_handshake_received_lock.read_access"):
            return self._handshake_received
    @handshake_received.setter
    def handshake_received(self, value):
        with timed_lock(self._handshake_received_lock.write_access, "_handshake_received_lock.write_access"):
            self._handshake_received = value

    @property
    def last_keepalive(self):
        with timed_lock(self._last_keepalive_lock.read_access, "_last_keepalive_lock.read_access"):
            return self._last_keepalive
    @last_keepalive.setter
    def last_keepalive(self, value):
        with timed_lock(self._last_keepalive_lock.write_access, "_last_keepalive_lock.write_access"):
            self._last_keepalive = value

    @property
    def keepalive_interval(self):
        with timed_lock(self._keepalive_interval_lock.read_access, "_keepalive_interval_lock.read_access"):
            return self._keepalive_interval
    @keepalive_interval.setter
    def keepalive_interval(self, value):
        with timed_lock(self._keepalive_interval_lock.write_access, "_keepalive_interval_lock.write_access"):
            self._keepalive_interval = value

    @property
    def last_activity(self):
        with timed_lock(self._last_activity_lock.read_access, "_last_activity_lock.read_access"):
            return self._last_activity
    @last_activity.setter
    def last_activity(self, value):
        mem = self.last_activity
        if mem is None or value is None or value - mem >= 1:
            with timed_lock(self._last_activity_lock.write_access, "_last_activity_lock.write_access"):
                self._last_activity = value

    @property
    def inactivity_timeout(self):
        with timed_lock(self._inactivity_timeout_lock.read_access, "_inactivity_timeout_lock.read_access"):
            return self._inactivity_timeout
    @inactivity_timeout.setter
    def inactivity_timeout(self, value):
        with timed_lock(self._inactivity_timeout_lock.write_access, "_inactivity_timeout_lock.write_access"):
            self._inactivity_timeout = value

    @property
    def requests_sent(self):
        with timed_lock(self._requests_sent_lock.read_access, "_requests_sent_lock.read_access"):
            return self._requests_sent
    @requests_sent.setter
    def requests_sent(self, value):
        with timed_lock(self._requests_sent_lock.write_access, "_requests_sent_lock.write_access"):
            self._requests_sent = value

    @property
    def pending_requests(self):
        with timed_lock(self._pending_requests_lock.read_access, "_pending_requests_lock.read_access"):
            return self._pending_requests
    @pending_requests.setter
    def pending_requests(self, value):
        if value >= 0:
            with timed_lock(self._pending_requests_lock.write_access, "_pending_requests_lock.write_access"):
                self._pending_requests = value

    @property
    def last_request_time(self):
        with timed_lock(self._last_request_time_lock.read_access, "_last_request_time_lock.read_access"):
            return self._last_request_time
    @last_request_time.setter
    def last_request_time(self, value):
        mem = self.last_request_time
        if mem is None or value is None or value - mem >= 1:
            with timed_lock(self._last_request_time_lock.write_access, "_last_request_time_lock.write_access"):
                self._last_request_time = value

    @property
    def bad_peer(self):
        with timed_lock(self._bad_peer_lock.read_access, "_bad_peer_lock.read_access"):
            return self._bad_peer
    @bad_peer.setter
    def bad_peer(self, value):
        with timed_lock(self._bad_peer_lock.write_access, "_bad_peer_lock.write_access"):
            self._bad_peer = value
    
    @property
    def connecting(self):
        with timed_lock(self._connecting_lock.read_access, "_connecting_lock.read_access"):
            return self._connecting
    @connecting.setter
    def connecting(self, value):
        with timed_lock(self._connecting_lock.write_access, "_connecting_lock.write_access"):
            self._connecting = value

    def is_socket_valid(self):
        """Check if socket is valid and connected"""
        if not self.sock:
            print("!!!socket not exists")
            return False
        
        # Проверяем, что сокет не закрыт
        if self.sock.fileno() == -1:
            print("!!!socket is closed (fileno == -1)")
            return False
            
        try:
            # Проверяем, что сокет все еще подключен
            self.sock.getpeername()
            
            # Проверяем, что можем отправить данные
            try:
                # Используем MSG_DONTWAIT для неблокирующей проверки
                self.send_data(b'\x00\x00\x00\x00')
                return True
            except (socket.error, OSError) as e:
                log_error(f"!!!send test failed: {e}")
                return False
            except Exception as e:
                log_error(f"!!!send test failed2: {e}")
                return False
                
        except (socket.error, OSError, AttributeError) as e:
            log_error(f"!!!getpeername failed: {e}")
            return False
        except Exception as e:
                log_error(f"!!!getpeername failed2: {e}")
                return False

    def connect_to_peer(self):
        # Проверяем cooldown без блокировки
        current_time = time.time()
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            return None

        # Получаем блокировку только для обновления состояния
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            return None
        self.last_connection_attempt = current_time

        # Закрываем существующий сокет без блокировки
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

        # Создаем новый сокет и подключаемся (сетевые операции без блокировки)
        try:
            if validIPAddress(self.ip) == "IPv6":
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                ip_port = (self.ip, self.port, 0, 0)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5) 
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)  
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072) 
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)  
                ip_port = (self.ip, self.port)

            log_info(f"Attempting connection to {ip_port}")
            sock.settimeout(self.connection_timeout)
            
            if not (0 < self.port < 65536):
                log_info(f"Invalid port number {self.port} for {self.ip}")
                sock.close()
                return None
                
            sock.connect(ip_port)
            
            # Проверяем, что соединение действительно установлено
            try:
                sock.getpeername()
            except (socket.error, OSError) as e:
                log_info(f"Connection verification failed for {ip_port}: {e}")
                sock.close()
                self.connection_attempts += 1
                self.connected = False
                return None
            
            # Обновляем состояние под блокировкой (быстрая операция)
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
            
            log_info(f"Success {ip_port}")
            return sock
            
        except socket.timeout:
            log_info(f"Connection timeout for {ip_port}")
            self.connection_attempts += 1
            self.connected = False
            if 'sock' in locals():
                try:
                    sock.close()
                except:
                    pass
            return None
        except ConnectionRefusedError:
            log_info(f"Connection refused by {ip_port}")
            self.connection_attempts += 1
            self.connected = False
            if 'sock' in locals():
                try:
                    sock.close()
                except:
                    pass
            return None
        except Exception as e:
            log_info(f"Connection error for {ip_port}: {str(e)}, \nTraceback:\n{traceback.format_exc()}")
            self.connection_attempts += 1
            self.connected = False
            if 'sock' in locals():
                try:
                    sock.close()
                except:
                    pass
            return None

    def close(self):
        # Закрываем сокет без блокировки
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
        
        # Обновляем состояние под блокировкой (быстрая операция)
        self.connected = False
        self.handshake_sent = False
        self.handshake_received = False
        self.sock = None

    def send_keepalive(self):
        """Send keepalive message if needed"""
        if not self.connected or not self.sock:
            return False
            
        current_time = time.time()
        if current_time - self.last_keepalive >= self.keepalive_interval:
            try:
                # Проверяем валидность сокета перед отправкой
                if not self.is_socket_valid():
                    self.connected = False
                    return False
                    
                self.send_data(b'\x00\x00\x00\x00')  # Keepalive message
                self.last_keepalive = current_time
                return True
            except Exception as e:
                log_info(f"Keepalive send failed for {self.ip_port}: {e}")
                self.connected = False
                return False
        return True

    def check_inactivity(self):
        """Check if peer has been inactive for too long"""
        if not self.connected:
            return True
        current_time = time.time()
        if current_time - self.last_activity >= self.inactivity_timeout:
            self.connected = False
            return True
        return False

    def update_activity(self, current_time = None):
        if current_time is None:
            current_time = time.time()
        self.last_activity = current_time
        
    def send_data(self, data):
        try:
            if not self.connected or not self.sock:
                raise Exception(f"not self.connected or not self.sock: {self.connected} {not not self.sock}")
            self.sock.sendall(data)
            self.update_activity()
            log_info(f"Send data was ok {self.ip_port}")
            return True
        except Exception as e:
            log_error(f"Send data failed for {self.ip_port}: {e}")
            self.connected = False
            raise
            # return False
        
    def peer_score(self):
        # Base score on download rate
        rate_score = self.rate if self.rate else 0
        
        # Bonus for unchoked peers
        unchoke_bonus = 1000 if not self.peer_choking else 0
        
        # Bonus for peers with more pieces we need
        # needed_pieces = sum(1 for i, piece in enumerate(self.piece_manager.pieces) 
                            # if not piece.is_complete() and self.bit_field[i])
        # piece_bonus = needed_pieces * 100
        
        # Penalty for peers with connection issues
        connection_penalty = self.connection_attempts * 500
        
        return rate_score + unchoke_bonus - connection_penalty #+ piece_bonus
    
    def is_active(self, needed_pieces: set):
        bitfield = self.bit_field.copy()
        length = len(self.bit_field)
        return any(bitfield[i] for i in needed_pieces if i < length)
    
    def avaliable_pieces(self, needed_pieces: set):
        bitfield = self.bit_field.copy()
        length = len(self.bit_field)
        return sum(1 for i in needed_pieces if i < length and bitfield[i])

    



            


