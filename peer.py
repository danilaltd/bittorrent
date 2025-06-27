import socket
import struct
from ipaddress import ip_address, IPv4Address
import bitstring
import time
import threading
from datetime import datetime
import os

def RoundUp(x):
    return ((x + 7) & (-8))

def validIPAddress(IP: str) -> str:
    try:
        return "IPv4" if type(ip_address(IP)) is IPv4Address else "IPv6"
    except ValueError:
        return "Invalid"
    
def peer_log(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f'[{timestamp}] {msg}\n'
        with open(os.path.join('logs', "peer.log"), 'a', encoding='utf-8') as f:
            f.write(log_entry)

class Peer:
    def __init__(self, ip_port, number_of_pieces):
        self.sock = None
        self.bit_field = bitstring.BitArray(RoundUp(number_of_pieces))
        self.ip_port = ip_port
        self.ip = ip_port[0]
        self.port = ip_port[1]
        self.am_choking = 1
        self.am_interested = 0
        self.peer_choking = 1
        self.peer_interested = 0
        self.last_transmission = None
        self.rate = None
        self.uploaded = 0
        self.blocks_recieved = 0
        self.connection_attempts = 0
        self.max_connection_attempts = 3  # Increased from 3 to 5
        self.connection_timeout = 10  # Increased from 15 to 20 seconds
        self.last_connection_attempt = 0
        self.connection_cooldown = 5
        self.connected = False
        self.handshake_sent = False
        self.handshake_received = False
        self.last_keepalive = time.time()
        self.keepalive_interval = 60  # Reduced from 120 to 60 seconds
        self._lock = threading.Lock()
        self.last_activity = time.time()
        self.inactivity_timeout = 180  # 3 minutes timeout for inactivity
        self.requests_sent = 0
        self.pending_requests = 0   
        self.last_request_time = time.time()

    def is_socket_valid(self):
        """Check if socket is valid and connected"""
        if not self.sock:
            return False
        try:
            # Try to get socket state
            self.sock.getpeername()
            # Check if socket is still responsive
            self.sock.send(b'\x00\x00\x00\x00')  # Send keepalive
            return True
        except (socket.error, OSError):
            return False

    def connect_to_peer(self):
        with self._lock:
            current_time = time.time()
            if current_time - self.last_connection_attempt < self.connection_cooldown:
                return None

            self.last_connection_attempt = current_time

            # Properly close existing socket
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


            try:
                if validIPAddress(self.ip) == "IPv6":
                    self.sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                    self.ip_port = (self.ip, self.port, 0, 0)
                else:
                    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5) 
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)  
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072) 
                    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)  

                peer_log(f"Attempting connection to {self.ip_port}")
                self.sock.settimeout(self.connection_timeout)
                
                if not (0 < self.port < 65536):
                    peer_log(f"Invalid port number {self.port} for {self.ip}")
                    return None
                    
                self.sock.connect(self.ip_port)
                peer_log("Success")
                self.last_transmission = time.time()
                self.last_keepalive = time.time()
                self.last_activity = time.time()
                self.connection_attempts = 0
                self.connected = True
                self.handshake_sent = False
                self.handshake_received = False
                return self.sock
            except socket.timeout:
                peer_log(f"Connection timeout for {self.ip_port}")
                self.connection_attempts += 1
                self.connected = False
                return None
            except ConnectionRefusedError:
                peer_log(f"Connection refused by {self.ip_port}")
                self.connection_attempts += 1
                self.connected = False
                return None
            except Exception as e:
                peer_log(f"Connection error for {self.ip_port}: {str(e)}")
                self.connection_attempts += 1
                self.connected = False
                return None

    def close(self):
        with self._lock:
            self.connected = False
            self.handshake_sent = False
            self.handshake_received = False
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

    def send_keepalive(self):
        """Send keepalive message if needed"""
        if not self.connected or not self.sock:
            return False
            
        current_time = time.time()
        if current_time - self.last_keepalive >= self.keepalive_interval:
            try:
                self.sock.send(b'\x00\x00\x00\x00')  # Keepalive message
                self.last_keepalive = current_time
                self.last_activity = current_time
                return True
            except:
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

    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()
        
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
    
class Handshake():
    def __init__(self, peer_id, info_hash):
        pstr = b"BitTorrent protocol"
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.pstr = pstr
        self.pstrlen = len(pstr)
        self.reserved = b'x\00' * 8
        self.handshake = struct.pack(">B{}s8s20s20s".format(self.pstrlen),
                            self.pstrlen,
                            self.pstr,
                            self.reserved,
                            self.info_hash,
                            self.peer_id)
    def getHandshakeBytes(self):
        return self.handshake


    



            


