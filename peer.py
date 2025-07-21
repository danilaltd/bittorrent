import socket
from ipaddress import ip_address, IPv4Address
import bitstring
import time
from datetime import datetime
import os
from logger import timed_lock
from rwlock import RWLock
import traceback
import queue
import threading
import errno
import struct
import traceback
import selectors

MAX_CONNECTION_ATTEMPTS = 3
MAX_PENDING_REQUESTS = 100
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
    def __init__(self, ip_port, number_of_pieces):
        self._sock: socket.socket = None
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
        
        self.send_queue = queue.Queue()
        self.receive_queue = queue.Queue()

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

    def getMessage(self):
        try:
            data = self.receive_queue.get_nowait()   
            return data
        except: 
            return None
    
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
                self._send_data(b'\x00\x00\x00\x00')
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
                sock.close()
                self.connection_attempts += 1
                self.connected = False
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
        print("close")
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
                    
                self._send_data(b'\x00\x00\x00\x00')  # Keepalive message
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
        self.send_queue.put(data)
        # mask = selectors.EVENT_READ | selectors.EVENT_WRITE
        # self.selector.modify(self.sock, mask)
        return True
            
    def _send_data(self, data):
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
        
    def _read_message(self):
        try:
            data = self._read_bytes_from_sock(4, empty_ok=True)
            if not data: 
                raise EOFError("no data")
            message_length = struct.unpack(">I", data)[0]
            if message_length:
                data_1 = self._read_bytes_from_sock(message_length)
                if data_1:
                    data += data_1
                else:
                    raise Exception(f"req len: {message_length}, got none")
            return data
        except EOFError:
            return None
        except Exception as e:
            log_error(f"{self.ip}: err in _read_message: {e}, \nTraceback:\n{traceback.format_exc()}")
            return None
    
        
    def _read_bytes_from_sock(self, length: int, empty_ok = False):
        self.update_activity()
        if length < 0:
            log_error(f"Invalid piece data length {length} from {self.ip_port}", flags=['piece Reading'], name=f'{self.ip_port}({threading.current_thread().name}).log')
            return None
            
        data = b''
        required = length
        timeout = 10  # 1 seconds timeout for reading piece data
        start_time = time.time()
            
        while required > 0:
            try:
                if time.time() - start_time > timeout:
                    log_error(f"Timeout reading piece data from {self.ip_port}", flags=['piece Reading'], name=f'{self.ip_port}({threading.current_thread().name}).log')
                    return None
                    
                # start = time.time()
                buff = self.sock.recv(min(required, 65536))  # Increased buffer size to 64KB
                # end = time.time()
                
                if buff and len(buff) > 0:
                    # self.rate = len(buff) // 125
                    # self.rate = self.rate // (end - start) if (end - start) > 0 else 0
                    data += buff
                    required = length - len(data)
                    
            except socket.error as e:
                err = e.args[0]
                if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                    log_error(f"Socket error reading piece data from {self.ip_port}, \nTraceback:\n{traceback.format_exc()}", e, flags=['piece Reading'], name=f'{self.ip_port}({threading.current_thread().name}).log')
                    if len(data) == 0:
                        return None
                    else:
                        raise
                if empty_ok and len(data) == 0:
                    return None
                
                log_error(f"wait", name=f'{self.ip_port}({threading.current_thread().name}).log')
                continue
            except Exception as e:
                log_error(f"Error reading piece data from {self.ip_port}", e, flags=['piece Reading'], name=f'{self.ip_port}({threading.current_thread().name}).log')
                if len(data) == 0:
                    return None
                else:
                    raise
                
        if len(data) != length:
            log_error(f"Received incomplete piece data from {self.ip_port}: got {len(data)} bytes, expected {length}", flags=['piece Reading'], name=f'{self.ip_port}({threading.current_thread().name}).log')
            return None
            
        return data
        
    def peer_score(self):
        # if self.ip == "5.79.98.162":
            # print("ok")
            # return 1000
        # else:
            # print(self.ip)
            # return -1000
        
        if self.peer_choking or not self.connected or self.pending_requests > (self._download_speed / (1024 * 1024)) * MAX_PENDING_REQUESTS:
            # print(f"ret -inf: {self.ip} {self.peer_choking} {not self.connected} {self.pending_requests > MAX_PENDING_REQUESTS}")
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

    



            


